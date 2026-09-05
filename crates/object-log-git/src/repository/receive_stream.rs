//! Frame-bounded receive input sharing buffered validation and publication.

use bytes::Bytes;
use futures::{Stream, StreamExt};
use object_log::TransactionId;

use super::{Repository, reject_receive};
use crate::{
    Error, ObjectId, ReceivePolicy,
    durable::{self, Catalog, SelectedIndex},
    pack::ingest::{BaseProvider, Decoded, Input},
    repository::PreparedPush,
    wire,
};

impl Repository {
    /// Stages a receive-pack request from frames with at most 1 MiB of backing
    /// allocation each. The producer accounts for frames before yielding them;
    /// this method owns their admission and memory accounting after yield.
    ///
    /// Commands and packs retain the limits of [`Self::prepare_receive`]. The
    /// complete body is never collected. A completed immutable input can retry
    /// once after its view expires; an incomplete client stream cannot replay.
    ///
    /// # Errors
    /// Returns malformed-control, producer, storage, or receive rejection errors.
    /// Failed preparation never publishes refs and may leave collectible staging.
    pub async fn prepare_receive_stream<S>(
        self,
        transaction_id: TransactionId,
        frames: S,
    ) -> Result<PreparedPush, Error>
    where
        S: Stream<Item = Result<Bytes, Error>> + Unpin,
    {
        self.prepare_receive_stream_with_policy(transaction_id, frames, ReceivePolicy::default())
            .await
    }

    /// Prepares bounded streaming input with an explicit branch policy.
    ///
    /// # Errors
    /// Returns the errors documented by [`Self::prepare_receive_stream`].
    pub async fn prepare_receive_stream_with_policy<S>(
        mut self,
        transaction_id: TransactionId,
        frames: S,
        policy: ReceivePolicy,
    ) -> Result<PreparedPush, Error>
    where
        S: Stream<Item = Result<Bytes, Error>> + Unpin,
    {
        let operation = self.operation.clone();
        let split = crate::pack::ingest::controls::split(&operation, frames).await?;
        let _commands = operation.reserve_state(split.bytes.len() * 4 + 1024)?;
        operation.work(split.bytes.len())?;
        let (request, rest) = wire::parse_receive_controls(&split.bytes, self.format)?;
        if !rest.is_empty() {
            return Err(Error::InvalidProtocol("control splitter left a tail"));
        }
        drop(split.bytes);
        if let Err(error) = self.validate_receive_controls(transaction_id, &request) {
            return reject_receive(&operation, &request, error);
        }
        let mut frames = split.pack;
        let mut replay = if request.needs_pack() {
            match Input::receive(&operation, &self.log, &self.view, frames).await {
                Ok(input) => Some(input.into_replay()),
                Err(error) => return reject_receive(&operation, &request, error),
            }
        } else {
            if let Some(frame) = frames.next().await {
                frame?;
                return Err(Error::InvalidProtocol("delete-only request has a Git pack"));
            }
            None
        };
        loop {
            let result = async {
                self.validate_receive_controls(transaction_id, &request)?;
                let (staged, certificate) = if let Some(replay) = &mut replay {
                    let input = replay.bind(&operation, &self.log, &self.view).await?;
                    let scanned = input.scan(self.format).await?;
                    if scanned.is_empty() {
                        (None, None)
                    } else {
                        let catalog = self.catalog().await?;
                        let mut bases = StoredBases {
                            repository: &self,
                            catalog: &catalog,
                        };
                        let (staged, certificate) =
                            scanned.normalize_for_receive(&mut bases).await?;
                        (Some(staged), certificate)
                    }
                } else {
                    (None, None)
                };
                self.prepare_staged_receive(
                    transaction_id,
                    &request,
                    staged,
                    policy,
                    certificate.as_ref(),
                )
                .await
            }
            .await;
            match result {
                Ok((prepared, memory)) => {
                    drop(replay);
                    return self.finish_receive(&request, prepared, memory);
                }
                Err(Error::ObjectLog(object_log::Error::ViewExpired)) => {
                    operation.retry()?;
                    let log = self.log.clone();
                    let format = self.format;
                    drop(self);
                    self = Self::open_attempt(&log, format, &operation).await?;
                }
                Err(error) => return reject_receive(&operation, &request, error),
            }
        }
    }
}

struct StoredBases<'a> {
    repository: &'a Repository,
    catalog: &'a Catalog,
}

impl BaseProvider for StoredBases<'_> {
    async fn provide<'source>(
        &mut self,
        source: &Input<'source>,
        id: ObjectId,
    ) -> Result<Option<Decoded<'source>>, Error> {
        let (descriptor, root, position) = if self
            .repository
            .state
            .catalog_tree(self.repository.format)
            .is_some()
        {
            let mut reader = durable::Reader::new(source.log(), source.view(), self.catalog);
            let Some(location) = reader.selected_location(id).await? else {
                return Ok(None);
            };
            (location.descriptor, location.root, Some(location.index))
        } else {
            let Some(pack) = self.catalog.containing_pack(id) else {
                return Ok(None);
            };
            let (bytes, root) = self
                .repository
                .state
                .packs
                .get(&pack)
                .ok_or(Error::InvalidReference)?;
            (
                crate::format::PackDescriptor {
                    id: pack,
                    bytes: *bytes,
                },
                root.clone(),
                None,
            )
        };
        let selected = SelectedIndex::load(
            source.operation(),
            source.log(),
            source.view(),
            &descriptor,
            &root,
        )
        .await?;
        let position = match position {
            Some(position) => position,
            None => selected.position_of(id)?.ok_or(Error::InvalidReference)?,
        };
        selected.stage_base(source, id, position).await.map(Some)
    }
}
