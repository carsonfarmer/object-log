//! Authenticated recovery from one durable base and ordered WAL tail.

use crate::{CheckpointRecord, CommitRecord, Error, Log, ObjectRef, StagedObject, View};
use std::collections::VecDeque;

const READ_AHEAD: usize = 8;

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
/// Reads ahead by at most eight commits under the log's byte budget.
#[derive(Debug)]
pub struct HistoryCursor {
    log: Log,
    view: View,
    checkpoint_read: bool,
    next_commit: usize,
    ready: VecDeque<Result<Option<CommitRecord>, Error>>,
    complete: bool,
}

impl Clone for HistoryCursor {
    fn clone(&self) -> Self {
        Self {
            log: self.log.clone(),
            view: self.view.clone(),
            checkpoint_read: self.checkpoint_read,
            next_commit: self.next_commit,
            ready: VecDeque::new(),
            complete: self.complete,
        }
    }
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
            if self.ready.is_empty() {
                let window = self.log.commit_read_window(READ_AHEAD);
                let end = self.view.tail().len().min(self.next_commit + window);
                self.ready = futures::future::join_all(
                    (self.next_commit..end)
                        .map(|index| self.log.read_tail_record_optional(&self.view, index)),
                )
                .await
                .into();
            }
            let record = match self.ready.pop_front().ok_or(Error::CorruptObject)? {
                Ok(Some(record)) => record,
                Ok(None) => {
                    self.ready.clear();
                    return Err(self.log.missing_read_error(&self.view).await?);
                }
                Err(error) => {
                    self.ready.clear();
                    return Err(error);
                }
            };
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
/// Returns a log error when the view belongs to another log.
pub fn history(log: &Log, view: View) -> Result<HistoryCursor, Error> {
    log.validate_view(&view)?;
    Ok(HistoryCursor {
        log: log.clone(),
        view,
        checkpoint_read: false,
        next_commit: 0,
        ready: VecDeque::new(),
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
    use crate::sim::{Failure, FailurePhase, FaultStore, Operation};
    use crate::{CommitStatus, LogId, Options, TransactionId, ValidatedBackend};

    #[tokio::test]
    async fn history_cursor_preserves_metadata_proofs_and_order() -> Result<(), Error> {
        let faults = FaultStore::new(InMemory::new());
        let backend =
            ValidatedBackend::new(Arc::new(faults.clone()), Path::from("history-test")).await?;
        let log = Log::open(
            &backend,
            &LogId::new("history")?,
            Options {
                max_object_bytes: 128 * 1024,
                ..Options::default()
            },
        )
        .await?;
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
        faults.reset();
        for occurrence in 1..=3 {
            faults.schedule(Failure {
                operation: Operation::Get,
                occurrence,
                phase: FailurePhase::Before,
            });
        }
        assert!(history.next().await.is_err());
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
            if sequence == 0 {
                let mut cloned = cursor.clone();
                let Some(HistoryItem::Commit(next)) = cloned.next().await? else {
                    return Err(Error::CorruptObject);
                };
                assert_eq!(next.record().reference().sequence(), 1);
            }
        }
        assert!(cursor.next().await?.is_none());
        assert!(cursor.is_complete());
        Ok(())
    }
}
