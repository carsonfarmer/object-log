//! Replayable immutable input for bounded streaming receive.

use std::{mem::size_of, sync::Arc};

use bytes::{Bytes, BytesMut};
use futures::{Stream, StreamExt};
use object_log::{Log, ObjectRef, StagedObject, View};

use super::{
    MAX_RECEIVE_PACK_BYTES,
    budget::{Operation, Reservation, hold},
    invalid, pack_error,
};
use crate::{Error, ObjectFormat, durable::publication_plan};

#[path = "controls.rs"]
pub(crate) mod controls;
#[path = "normalize.rs"]
mod normalize;
#[path = "replay.rs"]
mod replay;
#[path = "resolve.rs"]
mod resolve;
#[path = "scan.rs"]
mod scan;
#[path = "scratch.rs"]
mod scratch;
#[path = "stored.rs"]
mod stored;
pub(crate) use resolve::BaseProvider;
pub(crate) use scan::{Entry as IndexedEntry, Scanned};
pub(crate) use scratch::Decoded;

pub(super) const FRAME_BYTES: usize = 1024 * 1024;

/// Every frame must have backing allocation at most `FRAME_BYTES`, not a slice
/// pinning a larger request. The source reserves that entire bound after yield;
/// the producer owns and accounts for allocations before yield.
pub(crate) struct Input<'a> {
    log: &'a Log,
    view: &'a View,
    operation: Operation,
    context: Arc<()>,
    chunks: Vec<StagedObject>,
    inline: Option<Bytes>,
    cache: std::sync::Mutex<Option<(usize, Bytes)>>,
    bytes: u64,
    width: usize,
    maximum: usize,
    memory: Reservation,
}

impl<'a> Input<'a> {
    fn empty(
        operation: &Operation,
        log: &'a Log,
        view: &'a View,
        limit: usize,
    ) -> Result<Self, Error> {
        let width = FRAME_BYTES.min(log.options().max_object_bytes);
        if width == 0 {
            return invalid("input chunk width is zero");
        }
        let maximum = limit.div_ceil(width).min(log.options().max_object_refs);
        let memory = operation.reserve(
            maximum * (size_of::<StagedObject>() + size_of::<ObjectRef>()) + 2 * size_of::<usize>(),
        )?;
        Ok(Self {
            log,
            view,
            operation: operation.clone(),
            context: Arc::new(()),
            chunks: Vec::with_capacity(maximum),
            inline: None,
            cache: std::sync::Mutex::new(None),
            bytes: 0,
            width,
            maximum,
            memory,
        })
    }

    pub(crate) async fn receive(
        operation: &Operation,
        log: &'a Log,
        view: &'a View,
        mut frames: impl Stream<Item = Result<Bytes, Error>> + Unpin,
    ) -> Result<Self, Error> {
        let mut input = Self::empty(operation, log, view, MAX_RECEIVE_PACK_BYTES)?;
        let width = input.width;
        let mut pending_memory = operation.reserve(width)?;
        let mut pending = BytesMut::with_capacity(width);
        while let Some(frame) = frames.next().await {
            let frame = frame?;
            if frame.is_empty() {
                return invalid("empty input frame");
            }
            if frame.len() > FRAME_BYTES {
                return invalid("input frame exceeds byte limit");
            }
            let _frame_memory = operation.reserve(FRAME_BYTES)?;
            if frame.len() as u64 > MAX_RECEIVE_PACK_BYTES as u64 - input.bytes {
                return invalid("input exceeds byte limit");
            }
            operation.work(frame.len())?;
            input.bytes += frame.len() as u64;
            let mut remaining = &frame[..];
            while !remaining.is_empty() {
                let count = remaining.len().min(width - pending.len());
                pending.extend_from_slice(&remaining[..count]);
                remaining = &remaining[count..];
                if pending.len() == width {
                    input.put(pending.freeze(), pending_memory).await?;
                    pending_memory = operation.reserve(width)?;
                    pending = BytesMut::with_capacity(width);
                }
            }
        }
        if !pending.is_empty() {
            input.put(pending.freeze(), pending_memory).await?;
        }
        Ok(input)
    }

    async fn put(&mut self, bytes: Bytes, _memory: Reservation) -> Result<(), Error> {
        if self.chunks.len() >= self.maximum {
            return invalid("input needs too many chunks");
        }
        let _plan_memory = publication_plan(&self.operation, self.view)?;

        self.operation.work(bytes.len())?;
        self.chunks
            .push(self.log.put_object(self.view, bytes).await?);
        Ok(())
    }

    pub(crate) async fn scan(&self, format: ObjectFormat) -> Result<Scanned<'_, 'a>, Error> {
        scan::scan(self, format).await
    }
}

struct Cursor<'a, 'log> {
    input: &'a Input<'log>,
    position: u64,
    cache: Option<(usize, Bytes)>,
}

impl<'a, 'log> Cursor<'a, 'log> {
    fn new(input: &'a Input<'log>) -> Self {
        Self {
            input,
            position: 0,
            cache: None,
        }
    }

    async fn window(&mut self) -> Result<Bytes, Error> {
        if self.position == self.input.bytes {
            return Ok(Bytes::new());
        }
        let position = usize::try_from(self.position).map_err(pack_error)?;
        if let Some(bytes) = &self.input.inline {
            return Ok(bytes.slice(position..));
        }
        let index = position / self.input.width;
        if self
            .cache
            .as_ref()
            .is_none_or(|(cached, _)| *cached != index)
        {
            self.cache = None;
            let shared = self
                .input
                .cache
                .lock()
                .map_err(|_| pack_error("input cache lock poisoned"))?
                .as_ref()
                .filter(|(cached, _)| *cached == index)
                .cloned();
            if let Some(cached) = shared {
                self.cache = Some(cached);
            } else {
                // Remove the previous shared window before admitting its replacement.
                *self
                    .input
                    .cache
                    .lock()
                    .map_err(|_| pack_error("input cache lock poisoned"))? = None;
                let object = self
                    .input
                    .chunks
                    .get(index)
                    .ok_or_else(|| pack_error("input chunk is missing"))?
                    .reference();
                let size = usize::try_from(object.len()).map_err(pack_error)?;

                self.input.operation.work(size)?;
                let memory = self.input.operation.reserve(size)?;
                let bytes = self.input.log.read_object(self.input.view, object).await?;
                self.cache = Some((index, hold(bytes, memory)));
                self.input
                    .cache
                    .lock()
                    .map_err(|_| pack_error("input cache lock poisoned"))?
                    .clone_from(&self.cache);
            }
        }
        let (_, bytes) = self
            .cache
            .as_ref()
            .ok_or_else(|| pack_error("input cache is missing"))?;
        Ok(bytes.slice(position % self.input.width..))
    }

    async fn read_exact(&mut self, mut output: &mut [u8]) -> Result<(), Error> {
        while !output.is_empty() {
            let bytes = self.window().await?;
            if bytes.is_empty() {
                return invalid("input pack is truncated");
            }
            let count = output.len().min(bytes.len());
            output[..count].copy_from_slice(&bytes[..count]);
            output = &mut output[count..];
            self.position += count as u64;
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "ingest_tests.rs"]
mod tests;
