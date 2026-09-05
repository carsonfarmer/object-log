use super::{
    Bytes, Catalog, Error, FetchOptions, ObjectId, Operation, Repository, Reservation,
    UploadRequest, durable, io, size_of, wire,
};
use futures::{Sink, SinkExt};

pub(super) enum FetchPlan {
    Bytes(Bytes),
    Pack {
        catalog: Box<Catalog>,
        _catalog_memory: Reservation,
        selected: crate::selection::Selection,
        prefix: Option<Bytes>,
    },
}

/// A validated upload selection bound to one exact repository view.
/// Dropping it releases admission. Writing consumes it and never retries after
/// output starts; a late error requires aborting the response.
#[must_use = "write the response or drop it to release admission"]
pub struct PreparedUpload {
    repository: Repository,
    plan: FetchPlan,
}

impl Repository {
    /// Prepares protocol-v2 controls and verifies selected objects before HTTP
    /// headers are sent. The pack body is emitted by `PreparedUpload::write_to`.
    ///
    /// # Errors
    /// Invalid controls, graph data, quotas or storage failures reject the
    /// request. One expired-view retry shares the original operation counters.
    pub async fn prepare_upload(
        self,
        input: Bytes,
        uris: Option<&crate::PackfileUris>,
    ) -> Result<PreparedUpload, Error> {
        if input.len() > wire::MAX_UPLOAD_BYTES {
            return Err(Error::InvalidProtocol("upload control bytes"));
        }
        let operation = self.operation.clone();
        let _input_memory = operation.reserve(input.len())?;
        let maximum =
            3 * ((1024 + 2 * 32768) * size_of::<ObjectId>() + 2 * 1024 * size_of::<&[u8]>());
        let _parse_memory = operation.reserve((input.len() * 4 + 128).min(maximum))?;
        operation.work(input.len())?;
        let request = wire::parse_upload(&input, self.format)?;
        let mut repository = self;
        let plan = match repository.prepare_upload_attempt(&request, uris).await {
            Err(Error::ObjectLog(object_log::Error::ViewExpired)) => {
                let (log, format) = (repository.log.clone(), repository.format);
                drop(repository);
                operation.retry()?;
                repository = Self::open_attempt(&log, format, &operation).await?;
                repository.prepare_upload_attempt(&request, uris).await?
            }
            result => result?,
        };
        Ok(PreparedUpload { repository, plan })
    }

    async fn prepare_upload_attempt(
        &self,
        request: &UploadRequest<'_>,
        uris: Option<&crate::PackfileUris>,
    ) -> Result<FetchPlan, Error> {
        match request {
            UploadRequest::LsRefs {
                peel,
                symrefs,
                unborn,
                prefixes,
            } => self
                .ls_refs(*peel, *symrefs, *unborn, prefixes)
                .await
                .map(FetchPlan::Bytes),
            UploadRequest::Fetch {
                wants,
                haves,
                done,
                include_tag,
                shallow,
                filter,
                uri_protocols,
                ..
            } => {
                if uri_protocols.is_some() && uris.is_none() {
                    return Err(Error::InvalidProtocol("packfile URIs not enabled"));
                }
                let enabled = uris
                    .filter(|base| uri_protocols.is_some_and(|protocols| base.accepts(protocols)));
                self.prepare_fetch(
                    wants,
                    haves,
                    FetchOptions {
                        include_tag: *include_tag,
                        done: *done,
                        shallow: Some(shallow),
                        filter: *filter,
                        uris: enabled,
                    },
                )
                .await
            }
        }
    }
}

async fn send<S: Sink<Bytes, Error = io::Error> + Unpin>(
    sink: &mut S,
    operation: &Operation,
    total: &mut usize,
    bytes: &[u8],
) -> io::Result<()> {
    let length = total
        .checked_add(bytes.len())
        .filter(|length| *length <= wire::MAX_FETCH_RESPONSE_BYTES)
        .ok_or_else(|| io::Error::other(Error::InvalidProtocol("upload response bytes")))?;
    operation.work(bytes.len()).map_err(io::Error::other)?;
    for chunk in bytes.chunks(65536) {
        let memory = operation.reserve(chunk.len()).map_err(io::Error::other)?;
        sink.send(crate::pack::budget::hold(
            Bytes::copy_from_slice(chunk),
            memory,
        ))
        .await?;
    }
    *total = length;
    Ok(())
}

impl PreparedUpload {
    pub(super) async fn buffered(self) -> Result<Bytes, Error> {
        self.repository.buffer_fetch(self.plan).await
    }

    /// Writes bounded frames with backpressure, retaining the exact view and
    /// operation through completion or cancellation. Consumers must not collect
    /// frames without their own admission and must abort on error.
    ///
    /// # Errors
    /// A sink, quota, integrity or expired-view error can follow partial output.
    /// No final pack digest or protocol flush is written after such a failure.
    pub async fn write_to<S>(self, sink: &mut S) -> Result<(), Error>
    where
        S: Sink<Bytes, Error = io::Error> + Unpin,
    {
        let operation = &self.repository.operation;
        let mut total = 0;
        let result = match self.plan {
            FetchPlan::Bytes(bytes) => send(sink, operation, &mut total, &bytes).await,
            FetchPlan::Pack {
                catalog,
                selected,
                prefix,
                ..
            } => {
                let mut reader =
                    durable::Reader::new(&self.repository.log, &self.repository.view, &catalog);
                if let Some(prefix) = prefix {
                    send(sink, operation, &mut total, &prefix)
                        .await
                        .map_err(durable::output_error)?;
                    let _memory = operation.reserve(wire::MAX_PACKET_PAYLOAD + 65536)?;
                    let mut pending = Vec::with_capacity(wire::MAX_PACKET_PAYLOAD);
                    {
                        let framed = futures::sink::unfold(
                            (&mut *sink, &mut pending, &mut total),
                            |(sink, pending, total), bytes: Bytes| async move {
                                // Copy into the sideband buffer and then encode
                                // its packet; send charges the final frame copy.
                                operation.work(bytes.len() * 2).map_err(io::Error::other)?;
                                let mut remaining = bytes.as_ref();
                                while !remaining.is_empty() {
                                    let count = remaining
                                        .len()
                                        .min(wire::MAX_PACKET_PAYLOAD - pending.len());
                                    pending.extend_from_slice(&remaining[..count]);
                                    remaining = &remaining[count..];
                                    if pending.len() == wire::MAX_PACKET_PAYLOAD {
                                        let mut packet = Vec::with_capacity(65536);
                                        wire::write_pack_data(&mut packet, pending)
                                            .map_err(io::Error::other)?;
                                        send(sink, operation, total, &packet).await?;
                                        pending.clear();
                                    }
                                }
                                Ok::<_, io::Error>((sink, pending, total))
                            },
                        );
                        reader
                            .write_fetch(&selected.ids, &mut Box::pin(framed))
                            .await?;
                    }
                    if !pending.is_empty() {
                        let mut packet = Vec::with_capacity(65536);
                        wire::write_pack_data(&mut packet, &pending)?;
                        send(sink, operation, &mut total, &packet)
                            .await
                            .map_err(durable::output_error)?;
                    }
                    send(sink, operation, &mut total, b"0000").await
                } else {
                    reader.write_fetch(&selected.ids, sink).await?;
                    Ok(())
                }
            }
        };
        result.map_err(durable::output_error)
    }
}
