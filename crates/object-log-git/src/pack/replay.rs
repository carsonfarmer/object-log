//! Completed immutable input retained across a repository-view retry.

use super::{FRAME_BYTES, Input};
use crate::{
    Error,
    pack::{
        MAX_RECEIVE_PACK_BYTES,
        budget::{Operation, Reservation},
        invalid,
    },
};
use object_log::{Log, StagedObject, View};

pub(crate) struct Replay {
    chunks: Vec<StagedObject>,
    bytes: u64,
    epoch: u64,
    width: usize,
    operation: Operation,
    _memory: Reservation,
}

impl Input<'_> {
    pub(crate) fn into_replay(self) -> Replay {
        Replay {
            chunks: self.chunks,
            bytes: self.bytes,
            epoch: self.view.collection_epoch(),
            width: self.width,
            operation: self.operation,
            _memory: self.memory,
        }
    }
}

impl Replay {
    // Private caller retains the originating guarded Log clone family across
    // repository reopen; this does not authorize copying input into another log.
    pub(crate) async fn bind<'a>(
        &mut self,
        operation: &Operation,
        log: &'a Log,
        view: &'a View,
    ) -> Result<Input<'a>, Error> {
        if !operation.same_as(&self.operation) {
            return invalid("replayed input belongs to another operation");
        }
        let mut input = Input::empty(operation, log, view, MAX_RECEIVE_PACK_BYTES)?;
        if input.width != self.width || self.chunks.len() > input.maximum {
            return invalid("replayed input chunk geometry differs");
        }
        if view.collection_epoch() != self.epoch {
            // Prove one blob at a time: core traversal retains one verified body
            // plus a one-entry visited set. An active plan is reserved separately.
            let _memory = operation.reserve(FRAME_BYTES + 1024)?;
            for chunk in &mut self.chunks {
                let _plan = crate::durable::publication_plan(operation, view)?;
                operation.work(
                    usize::try_from(chunk.reference().len()).map_err(crate::pack::pack_error)?,
                )?;
                let mut proofs = log
                    .stage_objects(view, vec![chunk.reference().clone()])
                    .await?;
                *chunk = proofs
                    .pop()
                    .ok_or_else(|| Error::InvalidPack("input proof is missing".into()))?;
            }
            self.epoch = view.collection_epoch();
        }
        input.bytes = self.bytes;
        input.chunks.extend(self.chunks.iter().cloned());
        Ok(input)
    }
}
