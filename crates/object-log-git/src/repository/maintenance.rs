use std::mem::size_of;

use object_log::{CheckpointStatus, Log, StagedObject};

use super::{Repository, memory_bound, preflight_view};
use crate::{
    Error, ObjectFormat, ObjectId, durable,
    format::{Metadata, PackDescriptor, Record},
    pack::budget::{Operation, Pool},
};

impl Repository {
    /// Checkpoints authenticated Git metadata while retaining every pack proof.
    ///
    /// This conservative maintenance path does not read indexes, decode packs,
    /// traverse objects, or prune data. It can recover a bounded accumulated WAL
    /// tail that exceeds ordinary serving calls. It cannot reduce catalog size.
    ///
    /// Admission uses the same 88 MiB live and 24 MiB retained-state limits as
    /// serving, with 8,192 calls instead of 512. Transfer (96 MiB), work (256 MiB),
    /// and the single cumulative expired-view retry remain bounded. Returned
    /// pending evidence has the exact core checkpoint semantics; no resolution
    /// or collection is performed automatically.
    ///
    /// # Errors
    /// Returns an error for invalid authenticated metadata, exhausted limits,
    /// another active operation, or storage failure. No snapshot is published
    /// from a partially materialized or invalid tail.
    pub async fn checkpoint_retaining_packs(
        log: &Log,
        format: ObjectFormat,
    ) -> Result<CheckpointStatus, Error> {
        let operation = Pool::shared().admit_maintenance()?;
        Self::retain_packs(log, format, &operation).await
    }

    pub(super) async fn retain_packs(
        log: &Log,
        format: ObjectFormat,
        operation: &Operation,
    ) -> Result<CheckpointStatus, Error> {
        let guarded = log.with_request_guard(std::sync::Arc::new(operation.clone()));
        let log = &guarded;
        loop {
            let result = async {
                Self::open_attempt(log, format, operation)
                    .await?
                    .checkpoint_snapshot(|_| true)
                    .await
            }
            .await;
            match result {
                Err(Error::ObjectLog(object_log::Error::ViewExpired)) => operation.retry()?,
                other => return other,
            }
        }
    }

    // Both conservative maintenance and the graph-pruning checkpoint publish the
    // same record format, core tail validation, collection fencing, and head CAS.
    pub(super) async fn checkpoint_snapshot(
        self,
        keep: impl Fn(&ObjectId) -> bool,
    ) -> Result<CheckpointStatus, Error> {
        let Some(through) = self.view.tail().last().cloned() else {
            return Ok(CheckpointStatus::Published(self.view));
        };
        let metadata_bytes = self.state.default_branch().len();
        let tree_root = match self.state.catalog {
            crate::state::CatalogState::Legacy => None,
            crate::state::CatalogState::Tree(root) => Some(root),
        };
        let count =
            self.state.packs.len() + usize::from(tree_root.as_ref().is_some_and(Option::is_some));
        let _vectors_memory = self.operation.reserve(memory_bound(
            count,
            size_of::<PackDescriptor>() + size_of::<StagedObject>(),
        )?)?;
        let mut objects = Vec::with_capacity(count);
        let mut packs = Vec::with_capacity(count);
        for (id, (bytes, root)) in self.state.packs {
            if keep(&id) {
                packs.push(PackDescriptor { id, bytes });
                objects.push(root);
            }
        }
        objects.extend(tree_root.as_ref().and_then(Option::as_ref).cloned());
        let options = self.log.options();
        let snapshot_bound = self
            .state
            .refs
            .keys()
            .try_fold(128_usize + metadata_bytes, |sum, name| {
                sum.checked_add(name.len())?.checked_add(128)
            })
            .and_then(|sum| sum.checked_add(packs.len().checked_mul(128)?))
            .ok_or_else(|| Error::InvalidPack("Git snapshot exceeds memory".into()))?;
        // Tail preflight below reserves the possible classification/candidate
        // head alongside its decoder window; this reservation covers snapshot
        // construction and the core checkpoint envelope.
        let _publication_memory = self.operation.reserve(memory_bound(snapshot_bound, 4)?)?;
        let snapshot = Record::snapshot(self.format, self.state.refs, packs)?;
        let snapshot = if let Some(target) = self.state.default_branch {
            snapshot.with_metadata(Metadata::Snapshot(target))?
        } else {
            snapshot
        };
        let snapshot = if tree_root.is_some() {
            snapshot.with_catalog(crate::format::CatalogOperation::TreeSnapshot)?
        } else {
            snapshot
        }
        .encode()?;
        self.operation.work(snapshot.len())?;
        // publish_checkpoint validates the tail again before its first PUT.
        let _tail_memory = preflight_view(&self.operation, &self.log, &self.view)?;
        let _plan_memory = durable::publication_plan(&self.operation, &self.view)?;
        // Bound inner encoding/hash and outer encoding/hash once. Each core
        // reference fits 128 encoded bytes; the head allowance covers the
        // shared log identity and fixed envelope. Identity-collision retries
        // reuse the bytes and digest; request guards charge their actual I/O.
        let checkpoint_work = snapshot_bound
            .checked_add(memory_bound(objects.len(), 128)?)
            .and_then(|bytes| bytes.checked_add(options.max_head_bytes))
            .ok_or_else(|| Error::InvalidPack("Git checkpoint work exceeds limit".into()))?;
        self.operation.work(memory_bound(checkpoint_work, 4)?)?;
        for _ in 0..2 {
            self.operation.work(options.max_head_bytes)?;
        }
        Ok(self
            .log
            .publish_checkpoint(&self.view, &through, snapshot, objects)
            .await?)
    }
}
