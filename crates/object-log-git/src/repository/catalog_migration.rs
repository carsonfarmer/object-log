//! Explicit legacy-to-tree migration through one conditional publication.

use object_log::{CommitStatus, Log, TransactionId};

use super::{HEAD_DECODE_FACTOR, Repository, memory_bound};
use crate::{
    Error, ObjectFormat, ObjectId,
    catalog_tree::CatalogTree,
    durable::{self, SelectedIndex},
    format::{PackDescriptor, Record},
    pack::budget::{Operation, Pool},
    state::CatalogState,
};

impl Repository {
    /// Migrates a legacy pack catalog using one conditional WAL publication.
    ///
    /// Returns `None` when the observed state already uses a tree. Conflicts are
    /// returned without rebasing the candidate. Pending results carry the core
    /// recovery evidence. Maintenance admission bounds this one-shot operation;
    /// an oversized history fails before publication and is not partly migrated.
    ///
    /// # Errors
    /// Returns validation, admission, or storage errors. One expired-view retry
    /// retains cumulative counters; it never retries a pending publication.
    pub async fn migrate_catalog(
        log: &Log,
        format: ObjectFormat,
        transaction_id: TransactionId,
    ) -> Result<Option<CommitStatus>, Error> {
        let operation = Pool::shared().admit_maintenance()?;
        Self::migrate_catalog_admitted(log, format, transaction_id, &operation).await
    }

    pub(super) async fn migrate_catalog_admitted(
        log: &Log,
        format: ObjectFormat,
        transaction_id: TransactionId,
        operation: &Operation,
    ) -> Result<Option<CommitStatus>, Error> {
        let guarded = log.with_request_guard(std::sync::Arc::new(operation.clone()));
        let log = &guarded;
        loop {
            let result = async {
                Self::open_attempt(log, format, operation)
                    .await?
                    .migrate_catalog_attempt(transaction_id)
                    .await
            }
            .await;
            match result {
                Err(Error::ObjectLog(object_log::Error::ViewExpired)) => operation.retry()?,
                result => return result,
            }
        }
    }

    pub(super) async fn migrate_catalog_attempt(
        self,
        transaction_id: TransactionId,
    ) -> Result<Option<CommitStatus>, Error> {
        if matches!(self.state.catalog, CatalogState::Tree(_)) {
            return Ok(None);
        }
        self.log.preflight(&self.view, transaction_id)?;
        let mut tree = CatalogTree::empty(self.format);
        for (id, (bytes, root)) in &self.state.packs {
            let descriptor = PackDescriptor {
                id: *id,
                bytes: *bytes,
            };
            tree = insert_pack(
                &tree,
                &self.log,
                &self.view,
                &self.operation,
                descriptor,
                root.clone(),
            )
            .await?;
        }
        if tree.root().is_none() && !self.state.refs.is_empty() {
            return Err(Error::InvalidRecord("empty catalog has refs"));
        }
        let options = self.log.options();
        let bytes = memory_bound(options.max_commit_bytes, 4)?
            .checked_add(memory_bound(options.max_head_bytes, HEAD_DECODE_FACTOR)?)
            .ok_or_else(|| Error::InvalidPack("Git publication exceeds memory".into()))?;
        let _memory = self.operation.reserve(bytes)?;
        let record =
            Record::migration(self.format, self.state.default_branch().to_vec())?.encode()?;
        self.operation.work(record.len())?;
        let prepared = self.log.prepare(
            &self.view,
            transaction_id,
            record,
            bytes::Bytes::new(),
            tree.root().cloned().into_iter().collect(),
        )?;
        let _plan = durable::publication_plan(&self.operation, &self.view)?;
        self.operation
            .work(options.max_commit_bytes + options.max_head_bytes * 2)?;
        Ok(Some(self.log.commit(prepared).await?))
    }
}

pub(super) async fn insert_pack(
    tree: &CatalogTree,
    log: &Log,
    view: &object_log::View,
    operation: &Operation,
    descriptor: PackDescriptor,
    root: object_log::StagedObject,
) -> Result<CatalogTree, Error> {
    let selected = SelectedIndex::load(operation, log, view, &descriptor, &root).await?;
    let count = selected.num_objects() as usize;
    let _entries_memory =
        operation.reserve_state(memory_bound(count, size_of::<(ObjectId, u32)>())?)?;
    let mut entries = Vec::with_capacity(count);
    for entry in selected.entries() {
        entries.push(entry?);
    }
    tree.insert_pack(log, view, operation, descriptor, root, &entries)
        .await
}
