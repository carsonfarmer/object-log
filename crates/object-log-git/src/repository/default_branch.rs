use bytes::Bytes;
use object_log::{CommitStatus, TransactionId};

use super::{HEAD_DECODE_FACTOR, Repository, memory_bound};
use crate::{
    Error, durable,
    format::{Record, validate_default_branch},
};

impl Repository {
    /// Returns the persisted symbolic HEAD target, including an unborn target.
    /// Legacy repositories default to `refs/heads/main` until explicitly changed.
    #[must_use]
    pub fn default_branch(&self) -> &[u8] {
        self.state.default_branch()
    }

    /// Publishes one explicit symbolic HEAD update through the ordinary WAL CAS.
    ///
    /// Both names must be valid full branch refs. The target may be unborn;
    /// deleting it later preserves this symbolic HEAD. `expected` must match
    /// the observed default, and any concurrent publication can reject the CAS.
    /// No different candidate is retried. Pending results carry the existing
    /// core recovery evidence; callers must retain it for exact recovery.
    ///
    /// # Errors
    /// Returns an error for invalid names, a stale expected target, exhausted
    /// limits, or storage failure. Ref OIDs and pack roots are unchanged.
    pub async fn set_default_branch(
        self,
        transaction_id: TransactionId,
        expected: &[u8],
        target: &[u8],
    ) -> Result<CommitStatus, Error> {
        let options = self.log.options();
        let name_bytes = expected
            .len()
            .checked_add(target.len())
            .filter(|bytes| *bytes <= options.max_inline_operation_bytes)
            .ok_or(Error::InvalidRefName)?;
        self.operation.work(name_bytes)?;
        validate_default_branch(expected)?;
        validate_default_branch(target)?;
        if expected != self.default_branch() {
            return Err(Error::StaleReference);
        }
        let bytes = memory_bound(options.max_commit_bytes, 4)?
            .checked_add(memory_bound(options.max_head_bytes, HEAD_DECODE_FACTOR)?)
            .ok_or_else(|| Error::InvalidPack("Git publication exceeds memory".into()))?;
        let _memory = self.operation.reserve(bytes)?;
        let record =
            Record::metadata_update(self.format, expected.to_vec(), target.to_vec())?.encode()?;
        self.operation.work(record.len())?;
        let prepared =
            self.log
                .prepare(&self.view, transaction_id, record, Bytes::new(), Vec::new())?;
        let _plan = durable::publication_plan(&self.operation, &self.view)?;
        self.operation
            .work(options.max_commit_bytes + options.max_head_bytes * 2)?;
        Ok(self.log.commit(prepared).await?)
    }
}
