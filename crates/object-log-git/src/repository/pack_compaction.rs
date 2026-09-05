//! Bounded live-object repacking with one replacement-root publication.

use futures::{SinkExt, StreamExt};
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
        let limit = crate::MAX_STREAM_PACK_BYTES.min(pack::MAX_STORED_PACK_BYTES);
        // Keep ordinary groups small; one larger admitted object gets its own
        // pack instead of making every compaction output approach the wire cap.
        let preferred = pack::MAX_FETCH_PACK_BYTES.min(limit);
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
            if used > envelope && bound > preferred.saturating_sub(used) {
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

    pub(super) async fn compact_group(
        &self,
        reader: &mut durable::Reader<'_>,
        tree: &CatalogTree,
        ids: &[ObjectId],
    ) -> Result<CatalogTree, Error> {
        // A single sender admits at most one queued frame. Both futures share
        // the operation; either error drops its peer and leaves only unpublished
        // immutable staging. EOF is sent only after the writer's final digest.
        let (sender, receiver) = futures::channel::mpsc::channel(0);
        let produce = async {
            let mut sink = sender.sink_map_err(std::io::Error::other);
            reader.write_fetch(ids, &mut sink).await?;
            sink.close().await.map_err(pack::pack_error)
        };
        let receive =
            pack::ingest::Input::receive(&self.operation, &self.log, &self.view, receiver.map(Ok));
        let ((), input) = futures::try_join!(produce, receive)?;
        let scanned = input.scan(self.format).await?;
        let (descriptor, root) = scanned.normalize(&mut pack::ingest::NoBases).await?;
        drop(input);
        let index = durable::SelectedIndex::load(
            &self.operation,
            &self.log,
            &self.view,
            &descriptor,
            &root,
        )
        .await?;
        if index.num_objects() as usize != ids.len() {
            return Err(Error::InvalidPack(
                "compaction changed selected object IDs".into(),
            ));
        }
        let _entries_memory = self
            .operation
            .reserve_state(memory_bound(ids.len(), size_of::<(ObjectId, u32)>())?)?;
        // SelectedIndex charges OID decoding; charge comparison and tuple copies.
        self.operation.work(memory_bound(
            ids.len(),
            size_of::<(ObjectId, u32)>() + self.format.digest_len(),
        )?)?;
        let mut entries = Vec::with_capacity(ids.len());
        for (expected, entry) in ids.iter().zip(index.entries()) {
            let (id, position) = entry?;
            if id != *expected {
                return Err(Error::InvalidPack(
                    "compaction changed selected object IDs".into(),
                ));
            }
            entries.push((id, position));
        }
        drop(index);
        tree.insert_pack(
            &self.log,
            &self.view,
            &self.operation,
            descriptor,
            root,
            &entries,
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
    // One percent plus 64 bytes conservatively covers zlib expansion and Git
    // entry framing for admitted objects. Retained delta
    // instructions may exceed their result size, so also bound the stored extent
    // plus the maximum extra base-ID/header representation.
    decoded
        .checked_add(decoded.div_ceil(100))
        .and_then(|size| size.checked_add(64))
        .zip(stored.checked_add(32))
        .map(|(full, delta)| full.max(delta))
        .ok_or_else(|| Error::InvalidPack("compaction entry size overflow".into()))
}
