//! Bounded dependency resolution over immutable decoded scratch.

#[cfg(test)]
use super::scratch::ObjectSink;
use super::{
    Input,
    scan::Scanned,
    scratch::{self, Decoded},
};
use crate::{
    Error, ObjectId,
    pack::{MAX_OBJECTS, budget::Reservation, invalid, pack_error},
};
#[cfg(test)]
use bytes::Bytes;
#[cfg(test)]
use futures::{Stream, StreamExt};
use std::{mem::size_of, sync::Arc};

/// Implementations return repository content verified into bounded scratch.
/// They must not collect a whole object as an adapter for this interface.
pub(crate) trait BaseProvider {
    fn provide<'a>(
        &mut self,
        source: &Input<'a>,
        id: ObjectId,
    ) -> impl std::future::Future<Output = Result<Option<Decoded<'a>>, Error>> + Send;
}

#[cfg(test)]
pub(super) struct NoBases;
#[cfg(test)]
impl BaseProvider for NoBases {
    async fn provide<'a>(
        &mut self,
        _: &Input<'a>,
        _: ObjectId,
    ) -> Result<Option<Decoded<'a>>, Error> {
        Ok(None)
    }
}

#[cfg(test)]
impl<'a> Input<'a> {
    /// A thin-base capability is created only after exact size and OID verification.
    pub(crate) async fn stage_base(
        &self,
        id: ObjectId,
        kind: gix_object::Kind,
        size: usize,
        mut frames: impl Stream<Item = Result<Bytes, Error>> + Unpin,
    ) -> Result<Decoded<'a>, Error> {
        let mut sink = ObjectSink::new(self, kind, size, id.format())?;
        while let Some(frame) = frames.next().await {
            let frame = frame?;
            if frame.is_empty() || frame.len() > super::FRAME_BYTES {
                return invalid("invalid thin-base frame");
            }
            let _memory = self.operation.reserve(super::FRAME_BYTES)?;
            sink.write(&frame).await?;
        }
        sink.finish(Some(id), 0).await
    }
}

pub(super) struct Resolved<'a> {
    pub(super) objects: Vec<Option<Decoded<'a>>>,
    pub(super) external: Vec<Decoded<'a>>,
    pub(super) bases: Vec<Option<ObjectId>>,
    _memory: Reservation,
    external_memory: Reservation,
}

impl<'a> Scanned<'_, 'a> {
    pub(super) async fn resolve(
        &self,
        provider: &mut impl BaseProvider,
    ) -> Result<Resolved<'a>, Error> {
        let count = self.entries.len();
        let memory = self
            .input
            .operation
            .reserve(count * (size_of::<Option<Decoded<'a>>>() + size_of::<Option<ObjectId>>()))?;
        let mut result = Resolved {
            objects: (0..count).map(|_| None).collect(),
            external: Vec::new(),
            bases: vec![None; count],
            _memory: memory,
            external_memory: self.input.operation.reserve(0)?,
        };
        let mut remaining = count;
        while remaining > 0 {
            let mut progress = false;
            for (index, entry) in self.entries.iter().enumerate() {
                if result.objects[index].is_some() {
                    continue;
                }
                self.input.operation.work(1)?;
                let base = match entry.header.header {
                    gix_pack::data::entry::Header::OfsDelta { base_distance } => {
                        self.input
                            .operation
                            .work(count.max(1).ilog2() as usize + 1)?;
                        let offset = entry
                            .header
                            .checked_base_pack_offset(base_distance)
                            .ok_or_else(|| pack_error("invalid delta base offset"))?;
                        let position = self
                            .entries
                            .binary_search_by_key(&offset, |entry| entry.header.pack_offset())
                            .map_err(pack_error)?;
                        result.objects[position].as_ref()
                    }
                    gix_pack::data::entry::Header::RefDelta { base_id } => {
                        self.input
                            .operation
                            .work((count + result.external.len()) * size_of::<ObjectId>())?;
                        let id = ObjectId::from_bytes(self.id.format(), base_id.as_slice())?;
                        result
                            .objects
                            .iter()
                            .flatten()
                            .chain(result.external.iter())
                            .find(|object| object.id == id)
                    }
                    _ => None,
                };
                if entry.header.header.is_delta() && base.is_none() {
                    continue;
                }
                result.bases[index] = base.map(|object| object.id);
                let decoded = scratch::decode(self.input, entry, self.id.format(), base).await?;
                result.objects[index] = Some(decoded);
                remaining -= 1;
                progress = true;
            }
            if progress {
                continue;
            }
            // The known graph reached a fixed point. Request one still-missing
            // REF base, then resolve in-pack dependencies before another request.
            let missing = self
                .entries
                .iter()
                .enumerate()
                .find_map(|(index, entry)| {
                    if result.objects[index].is_some() {
                        return None;
                    }
                    if let gix_pack::data::entry::Header::RefDelta { base_id } = entry.header.header
                    {
                        Some(ObjectId::from_bytes(self.id.format(), base_id.as_slice()))
                    } else {
                        None
                    }
                })
                .transpose()?
                .ok_or_else(|| pack_error("delta graph cannot make progress"))?;
            self.input.operation.thin_round()?;
            if count + result.external.len() >= MAX_OBJECTS as usize {
                return invalid("thin pack object count exceeds limit");
            }
            let base = provider
                .provide(self.input, missing)
                .await?
                .ok_or_else(|| pack_error("thin base is missing or delta graph cycles"))?;
            if base.id != missing || !Arc::ptr_eq(&base.context, &self.input.context) {
                return invalid("thin base belongs to another source or OID");
            }
            if result.external.iter().any(|prior| prior.id == missing) {
                return invalid("thin resolution made no progress");
            }
            result.push_external(base, count)?;
        }
        Ok(result)
    }
}

impl<'a> Resolved<'a> {
    fn push_external(&mut self, base: Decoded<'a>, count: usize) -> Result<(), Error> {
        // Reserve allocator growth before adding another retained scratch object.
        let old = self.external.capacity();
        if self.external.len() == old {
            let new = (old.max(1) * 2).min(MAX_OBJECTS as usize - count);
            self.external_memory
                .grow((new - old) * size_of::<Decoded<'a>>())?;
            self.external.reserve_exact(new - old);
        }
        self.external.push(base);
        Ok(())
    }
}
