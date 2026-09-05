//! Bounded live-object repacking with one replacement-root publication.

use object_log::{CommitStatus, Log, PreparedCommit, TransactionId};

use super::{HEAD_DECODE_FACTOR, Repository, memory_bound};
use crate::{
    Error, ObjectFormat, ObjectId,
    catalog_tree::CatalogTree,
    durable,
    format::{CatalogOperation, Record},
    graph::Graph,
    pack::{
        self,
        budget::{Pool, Reservation},
    },
    state::CatalogState,
};

impl Repository {
    /// Repackages reachable objects and atomically replaces the catalog root.
    ///
    /// Requires an explicitly migrated tree catalog. Ref OIDs and symbolic HEAD
    /// remain unchanged. Output is split into bounded packs; the whole operation
    /// retains the existing maintenance work, transfer, and memory limits.
    /// A conflict is returned without rebasing. Pending results carry the exact
    /// core recovery token; callers must retain it and resolve before retrying.
    ///
    /// Old roots remain in retained WAL history until a successful checkpoint.
    /// Ordinary garbage collection can reclaim them only after that retention
    /// advances. This method does not run checkpointing or collection itself.
    ///
    /// # Errors
    /// Returns validation, admission, or storage errors. An oversized live set
    /// fails without a partial root publication. One expired-view retry keeps
    /// the same cumulative operation counters.
    pub async fn compact_packs(
        log: &Log,
        format: ObjectFormat,
        transaction_id: TransactionId,
    ) -> Result<CommitStatus, Error> {
        let operation = Pool::shared().admit_maintenance()?;
        let log = log.with_request_guard(std::sync::Arc::new(operation.clone()));
        loop {
            let result = async {
                let repository = Self::open_attempt(&log, format, &operation).await?;
                let (prepared, _memory) =
                    repository.prepare_pack_compaction(transaction_id).await?;
                let _plan = durable::publication_plan(&operation, &repository.view)?;
                Ok(repository.log.commit(prepared).await?)
            }
            .await;
            match result {
                Err(Error::ObjectLog(object_log::Error::ViewExpired)) => operation.retry()?,
                result => return result,
            }
        }
    }

    pub(super) async fn prepare_pack_compaction(
        &self,
        transaction_id: TransactionId,
    ) -> Result<(PreparedCommit, Reservation), Error> {
        if !matches!(self.state.catalog, CatalogState::Tree(_)) {
            return Err(Error::InvalidRecord(
                "pack compaction requires a tree catalog",
            ));
        }
        self.log.preflight(&self.view, transaction_id)?;
        let catalog = self.catalog().await?;
        let mut reader = durable::Reader::new(&self.log, &self.view, &catalog);
        let _roots_memory = self
            .operation
            .reserve_state(memory_bound(self.state.refs.len(), size_of::<ObjectId>())?)?;
        let roots = self.state.refs.values().copied().collect::<Vec<_>>();
        let graph = Graph::load(&self.operation, &mut reader, &roots).await?;
        for (name, id) in &self.state.refs {
            if name.starts_with(b"refs/heads/")
                && graph.location(*id).is_none_or(|index| {
                    graph.nodes[index as usize].kind != Some(gix_object::Kind::Commit)
                })
            {
                return Err(Error::InvalidReference);
            }
        }
        for node in &graph.nodes {
            if !node.verified && reader.verify(node.id).await? != node.kind {
                return Err(Error::InvalidReference);
            }
        }
        let _ids_memory = self
            .operation
            .reserve_state(memory_bound(graph.nodes.len(), size_of::<ObjectId>())?)?;
        let mut ids = graph.nodes.iter().map(|node| node.id).collect::<Vec<_>>();
        drop(graph);
        self.operation.work(memory_bound(
            ids.len(),
            (ids.len().max(1).ilog2() as usize + 1) * size_of::<ObjectId>(),
        )?)?;
        ids.sort_unstable();
        let mut tree = CatalogTree::empty(self.format);
        let limit = pack::MAX_FETCH_PACK_BYTES
            .min(pack::MAX_RECEIVE_PACK_BYTES)
            .min(pack::MAX_PACK_BYTES);
        let envelope = 12 + self.format.digest_len();
        let mut start = 0;
        let mut used = envelope;
        for (position, id) in ids.iter().enumerate() {
            let bound = entry_bound(&mut reader, *id).await?;
            if bound > limit - envelope {
                return Err(Error::InvalidPack(
                    "object exceeds bounded compaction pack".into(),
                ));
            }
            if bound > limit - used {
                tree = self
                    .compact_group(&mut reader, &tree, &ids[start..position])
                    .await?;
                start = position;
                used = envelope;
            }
            used += bound;
        }
        if start < ids.len() {
            tree = self
                .compact_group(&mut reader, &tree, &ids[start..])
                .await?;
        }
        let options = self.log.options();
        let bytes = memory_bound(options.max_commit_bytes, 4)?
            .checked_add(memory_bound(options.max_head_bytes, HEAD_DECODE_FACTOR)?)
            .ok_or_else(|| {
                Error::InvalidPack("Git compaction publication exceeds memory".into())
            })?;
        let memory = self.operation.reserve(bytes)?;
        let branch = self.state.default_branch().to_vec();
        let record = Record::metadata_update(self.format, branch.clone(), branch)?
            .with_catalog(CatalogOperation::Replace)?
            .encode()?;
        self.operation
            .work(record.len() + options.max_commit_bytes + options.max_head_bytes * 2)?;
        let prepared = self.log.prepare(
            &self.view,
            transaction_id,
            record,
            bytes::Bytes::new(),
            tree.root().cloned().into_iter().collect(),
        )?;
        Ok((prepared, memory))
    }

    async fn compact_group(
        &self,
        reader: &mut durable::Reader<'_>,
        tree: &CatalogTree,
        ids: &[ObjectId],
    ) -> Result<CatalogTree, Error> {
        let bytes = reader.fetch_pack(ids).await?;
        // Normalization validates the complete emitted pack and every object,
        // including reused delta streams, before immutable staging begins.
        let normalized = pack::normalize_attempt(&self.operation, self.format, &bytes, &[])
            .map_err(|error| match error {
                pack::NormalizeError::Invalid(error) => error,
                pack::NormalizeError::MissingBase { message, .. } => Error::InvalidPack(message),
                pack::NormalizeError::DuplicateObject(_) => {
                    Error::InvalidPack("duplicate compaction object".into())
                }
            })?;
        let index = gix_pack::index::File::from_data(
            normalized.index.as_slice(),
            std::path::PathBuf::new(),
            pack::object_hash(self.format),
        )
        .map_err(pack::pack_error)?;
        self.operation
            .work(memory_bound(ids.len(), self.format.digest_len())?)?;
        if index.num_objects() as usize != ids.len()
            || ids
                .iter()
                .zip(0..index.num_objects())
                .any(|(id, position)| index.oid_at_index(position).as_bytes() != id.as_bytes())
        {
            return Err(Error::InvalidPack(
                "compaction changed selected object IDs".into(),
            ));
        }
        drop(index);
        drop(bytes);
        let (descriptor, root) =
            durable::stage(&self.operation, &self.log, &self.view, normalized).await?;
        super::catalog_migration::insert_pack(
            tree,
            &self.log,
            &self.view,
            &self.operation,
            descriptor,
            root,
        )
        .await
    }
}

async fn entry_bound(reader: &mut durable::Reader<'_>, id: ObjectId) -> Result<usize, Error> {
    let stored = reader
        .packed_entry_bytes(id)
        .await?
        .ok_or(Error::InvalidReference)?;
    let decoded = reader
        .object_size(id)
        .await?
        .ok_or(Error::InvalidReference)?;
    // The current decoded-object limit is 8 MiB. One percent plus 64 bytes
    // conservatively covers zlib expansion and Git entry framing. Retained delta
    // instructions may exceed their result size, so also bound the stored extent
    // plus the maximum extra base-ID/header representation.
    decoded
        .checked_add(decoded.div_ceil(100))
        .and_then(|size| size.checked_add(64))
        .zip(stored.checked_add(32))
        .map(|(full, delta)| full.max(delta))
        .ok_or_else(|| Error::InvalidPack("compaction entry size overflow".into()))
}
