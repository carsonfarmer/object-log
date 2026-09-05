//! Reachability pruning for the authenticated catalog representation.

use gix_object::Kind;

use super::{Repository, memory_bound};
use crate::{
    Error, ObjectId,
    catalog_tree::{CatalogTree, PackLocation},
    durable,
    graph::Graph,
    state::CatalogState,
};

impl Repository {
    pub(super) async fn checkpoint_tree(mut self) -> Result<object_log::CheckpointStatus, Error> {
        if self.view.tail().is_empty() {
            return Ok(object_log::CheckpointStatus::Published(self.view));
        }
        let _roots_memory = self
            .operation
            .reserve_state(memory_bound(self.state.refs.len(), size_of::<ObjectId>())?)?;
        let roots = self.state.refs.values().copied().collect::<Vec<_>>();
        let catalog = self.catalog().await?;
        let mut reader = durable::Reader::new(&self.log, &self.view, &catalog);
        let graph = Graph::load(&self.operation, &mut reader, &roots).await?;
        for (name, id) in &self.state.refs {
            if name.starts_with(b"refs/heads/")
                && graph
                    .location(*id)
                    .is_none_or(|index| graph.nodes[index as usize].kind != Some(Kind::Commit))
            {
                return Err(Error::InvalidReference);
            }
        }
        let _locations_memory = self.operation.reserve_state(memory_bound(
            graph.nodes.len(),
            size_of::<(ObjectId, PackLocation)>(),
        )?)?;
        let mut locations = Vec::with_capacity(graph.nodes.len());
        for node in &graph.nodes {
            if !node.verified && reader.verify(node.id).await? != node.kind {
                return Err(Error::InvalidReference);
            }
            locations.push((
                node.id,
                reader
                    .selected_location(node.id)
                    .await?
                    .ok_or(Error::InvalidReference)?,
            ));
        }
        drop(graph);
        drop(reader);
        drop(catalog);
        self.operation.work(memory_bound(
            locations.len(),
            (locations.len().max(1).ilog2() as usize + 1) * size_of::<(ObjectId, PackLocation)>(),
        )?)?;
        locations.sort_unstable_by_key(|(id, location)| (location.descriptor.id, *id));
        let mut tree = CatalogTree::empty(self.format);
        let mut start = 0;
        while start < locations.len() {
            let first = &locations[start].1;
            let end = start
                + locations[start..]
                    .partition_point(|(_, location)| location.descriptor.id == first.descriptor.id);
            let _entries_memory = self
                .operation
                .reserve_state(memory_bound(end - start, size_of::<(ObjectId, u32)>())?)?;
            let mut entries = Vec::with_capacity(end - start);
            for (id, location) in &locations[start..end] {
                if location.descriptor != first.descriptor
                    || location.root.reference() != first.root.reference()
                {
                    return Err(Error::InvalidPack(
                        "catalog pack identity differs across leaves".into(),
                    ));
                }
                entries.push((*id, location.index));
            }
            tree = tree
                .insert_pack(
                    &self.log,
                    &self.view,
                    &self.operation,
                    first.descriptor.clone(),
                    first.root.clone(),
                    &entries,
                )
                .await?;
            start = end;
        }
        self.state.catalog = CatalogState::Tree(tree.root().cloned());
        self.checkpoint_snapshot(|_| true).await
    }
}
