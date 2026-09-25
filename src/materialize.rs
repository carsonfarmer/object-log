//! Typed recovery of state from one durable base and ordered WAL tail.

use std::error::Error as StdError;

use futures::TryStreamExt;

use crate::{CheckpointRecord, CommitRecord, Error, Log, ObjectRef, StagedObject, View};

/// One decoded durable record and publication proofs for its object references.
#[derive(Clone, Debug)]
pub struct Authenticated<R> {
    record: R,
    proofs: Vec<StagedObject>,
}

impl<R> Authenticated<R> {
    /// Returns the decoded record.
    #[must_use]
    pub const fn record(&self) -> &R {
        &self.record
    }

    /// Returns publication proofs for the record's ordered object references.
    #[must_use]
    pub fn proofs(&self) -> &[StagedObject] {
        &self.proofs
    }

    /// Separates the decoded record and its publication proofs.
    #[must_use]
    pub fn into_parts(self) -> (R, Vec<StagedObject>) {
        (self.record, self.proofs)
    }
}

/// The next authenticated item in a log's recovery history.
#[derive(Clone, Debug)]
pub enum HistoryItem {
    /// The optional durable base, always returned before commits.
    Checkpoint(Authenticated<CheckpointRecord>),
    /// One active-tail commit in sequence order.
    Commit(Authenticated<CommitRecord>),
}

/// A bounded cursor over one exact view's checkpoint and active tail.
#[derive(Clone, Debug)]
pub struct HistoryCursor {
    log: Log,
    view: View,
    checkpoint_read: bool,
    next_commit: usize,
    complete: bool,
}

impl HistoryCursor {
    /// Returns the exact durable view represented by this cursor.
    #[must_use]
    pub const fn view(&self) -> &View {
        &self.view
    }

    /// Reports whether [`Self::next`] has authenticated the complete history.
    #[must_use]
    pub const fn is_complete(&self) -> bool {
        self.complete
    }

    /// Reads the next authenticated checkpoint or commit without retaining
    /// earlier records. The checkpoint, when present, is returned first.
    ///
    /// # Errors
    ///
    /// Returns a log error when durable history is missing, corrupt, expired,
    /// or exceeds its authenticated limits.
    pub async fn next(&mut self) -> Result<Option<HistoryItem>, Error> {
        if self.complete {
            return Ok(None);
        }
        if !self.checkpoint_read {
            let checkpoint = self.log.read_checkpoint(&self.view).await?;
            self.checkpoint_read = true;
            if let Some(record) = checkpoint {
                let proofs = record_proofs(&self.log, &self.view, record.objects());
                return Ok(Some(HistoryItem::Checkpoint(Authenticated {
                    record,
                    proofs,
                })));
            }
        }
        if self.next_commit < self.view.tail().len() {
            let record = self
                .log
                .read_tail_record(&self.view, self.next_commit)
                .await?;
            self.next_commit += 1;
            let proofs = record_proofs(&self.log, &self.view, record.objects());
            return Ok(Some(HistoryItem::Commit(Authenticated { record, proofs })));
        }
        self.log.remember_tail(&self.view);
        self.complete = true;
        Ok(None)
    }
}

/// Creates a bounded authenticated-history cursor for one exact view.
///
/// At most one decoded checkpoint or commit is returned per call. Consumers
/// choose how much reconstructed application state to retain.
///
/// # Errors
///
/// Returns a log error when the view belongs to another log or its authenticated
/// lengths cannot be represented on this target.
pub fn history(log: &Log, view: View) -> Result<HistoryCursor, Error> {
    log.validate_view(&view)?;
    Ok(HistoryCursor {
        log: log.clone(),
        view,
        checkpoint_read: false,
        next_commit: 0,
        complete: false,
    })
}

/// Reads one commit from an exact view's active tail with publication proofs
/// for its declared objects. This does not verify the other tail records or
/// certify the complete history for checkpoint publication.
///
/// # Errors
///
/// Returns an error for a foreign view, an out-of-range index, a missing,
/// corrupt, or expired commit, or a broken link to its declared predecessor.
pub async fn tail_record(
    log: &Log,
    view: &View,
    index: usize,
) -> Result<Authenticated<CommitRecord>, Error> {
    let record = log.read_tail_record(view, index).await?;
    let proofs = record_proofs(log, view, record.objects());
    Ok(Authenticated { record, proofs })
}

/// Applies opaque log data to one application state type.
///
/// Callbacks may run before a later storage or state error is discovered.
/// Failed materialization drops its partial state; it does not roll back any
/// external effects performed by callbacks.
pub trait Materializer {
    /// The reconstructed application state.
    type State;
    /// A domain-specific decode or state-transition error.
    type Error: StdError + 'static;

    /// Creates the state before the first committed operation.
    fn empty(&self) -> Self::State;

    /// Restores one application snapshot.
    ///
    /// `objects` contains publication proofs for the snapshot's ordered object
    /// references. State can retain these proofs for a checkpoint against the
    /// returned [`Materialized::view`].
    ///
    /// # Errors
    ///
    /// Returns a domain error when the snapshot is invalid.
    fn restore(
        &self,
        checkpoint: &[u8],
        objects: &[StagedObject],
    ) -> Result<Self::State, Self::Error>;

    /// Applies one committed operation in sequence order.
    ///
    /// `objects` contains publication proofs for the operation's ordered
    /// object references.
    ///
    /// # Errors
    ///
    /// Returns a domain error when the operation is invalid for `state`.
    fn apply(
        &self,
        state: &mut Self::State,
        operation: &[u8],
        objects: &[StagedObject],
    ) -> Result<(), Self::Error>;
}

/// One state value reconstructed from one exact durable view.
#[derive(Clone, Debug)]
pub struct Materialized<S> {
    view: View,
    state: S,
}

impl<S> Materialized<S> {
    /// Returns the exact durable view used for reconstruction.
    #[must_use]
    pub const fn view(&self) -> &View {
        &self.view
    }

    /// Returns the reconstructed state.
    #[must_use]
    pub const fn state(&self) -> &S {
        &self.state
    }

    /// Separates the durable view and reconstructed state.
    #[must_use]
    pub fn into_parts(self) -> (View, S) {
        (self.view, self.state)
    }
}

/// Failure while reconstructing one application state.
#[derive(Debug, thiserror::Error)]
pub enum MaterializeError<E: StdError + 'static> {
    /// Durable log loading or verification failed.
    #[error(transparent)]
    Log(#[from] Error),
    /// Application snapshot or operation processing failed.
    #[error("state materialization failed: {0}")]
    State(E),
}

/// Reconstructs state from one exact observed view.
///
/// The returned [`Materialized`] contains the supplied view.
///
/// # Errors
///
/// Returns a log error for invalid durable storage or a domain error for an
/// invalid application snapshot or operation.
pub async fn materialize<M>(
    log: &Log,
    view: View,
    materializer: &M,
) -> Result<Materialized<M::State>, MaterializeError<M::Error>>
where
    M: Materializer,
{
    let mut state = match log.read_checkpoint(&view).await? {
        Some(checkpoint) => {
            let objects = record_proofs(log, &view, checkpoint.objects());
            materializer
                .restore(checkpoint.snapshot(), &objects)
                .map_err(MaterializeError::State)?
        }
        None => materializer.empty(),
    };
    {
        let records = log.tail_records(&view)?;
        futures::pin_mut!(records);
        while let Some(record) = records.try_next().await? {
            let objects = record_proofs(log, &view, record.objects());
            materializer
                .apply(&mut state, record.operation(), &objects)
                .map_err(MaterializeError::State)?;
        }
    }
    log.remember_tail(&view);
    Ok(Materialized { view, state })
}

fn record_proofs(log: &Log, view: &View, objects: &[ObjectRef]) -> Vec<StagedObject> {
    objects
        .iter()
        .cloned()
        .map(|object| log.staged_object(view, object))
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use object_store::{memory::InMemory, path::Path};

    use super::*;
    use crate::{CommitStatus, LogId, Options, TransactionId, ValidatedBackend};

    #[tokio::test]
    async fn history_cursor_preserves_metadata_proofs_and_order() -> Result<(), Error> {
        let backend =
            ValidatedBackend::new(Arc::new(InMemory::new()), Path::from("history-test")).await?;
        let log = Log::open(&backend, &LogId::new("history")?, Options::default()).await?;
        let view = log.load().await?;
        let object = log.put_object(&view, Bytes::from_static(b"value")).await?;
        let transaction_id = TransactionId::from_uuid(uuid::Uuid::from_u128(7));
        let prepared = log.prepare(
            &view,
            transaction_id,
            Bytes::from_static(b"operation"),
            Bytes::from_static(b"result"),
            vec![object.clone()],
        )?;
        let CommitStatus::Committed(view) = log.commit(prepared).await? else {
            return Err(Error::InvalidFormat("test commit did not publish".into()));
        };

        let mut history = history(&log, view)?;
        let Some(HistoryItem::Commit(authenticated)) = history.next().await? else {
            return Err(Error::InvalidFormat("test history has wrong item".into()));
        };
        let (record, proofs) = authenticated.into_parts();
        assert_eq!(
            record.reference().sequence(),
            history.view().tail()[0].sequence()
        );
        assert_eq!(record.reference().transaction_id(), transaction_id);
        assert_eq!(record.operation(), b"operation".as_slice());
        assert_eq!(record.result(), b"result".as_slice());
        assert_eq!(proofs.len(), 1);
        assert_eq!(proofs[0].reference(), object.reference());
        let latest = tail_record(&log, history.view(), 0).await?;
        assert_eq!(latest.record(), &record);
        assert_eq!(latest.proofs()[0].reference(), object.reference());
        assert!(history.next().await?.is_none());
        assert!(history.is_complete());
        Ok(())
    }

    #[tokio::test]
    async fn history_cursor_yields_a_full_tail_one_record_at_a_time() -> Result<(), Error> {
        const ENTRIES: usize = 32;
        let backend =
            ValidatedBackend::new(Arc::new(InMemory::new()), Path::from("cursor-bound-test"))
                .await?;
        let log = Log::open(
            &backend,
            &LogId::new("history")?,
            Options {
                max_tail_entries: ENTRIES,
                ..Options::default()
            },
        )
        .await?;
        let mut view = log.load().await?;
        for value in 0..ENTRIES {
            let prepared = log.prepare(
                &view,
                TransactionId::new(),
                Bytes::copy_from_slice(&value.to_le_bytes()),
                Bytes::new(),
                Vec::new(),
            )?;
            let CommitStatus::Committed(next) = log.commit(prepared).await? else {
                return Err(Error::InvalidFormat("test commit did not publish".into()));
            };
            view = next;
        }

        let mut cursor = history(&log, view)?;
        for sequence in 0..ENTRIES {
            let Some(HistoryItem::Commit(record)) = cursor.next().await? else {
                return Err(Error::InvalidFormat("cursor ended early".into()));
            };
            assert_eq!(record.record().reference().sequence(), sequence as u64);
            drop(record);
        }
        assert!(cursor.next().await?.is_none());
        assert!(cursor.is_complete());
        Ok(())
    }
}
