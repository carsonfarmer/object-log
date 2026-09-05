//! Reachability pruning for the authenticated catalog representation.

use gix_object::Kind;

use super::{Repository, memory_bound};
use crate::{
    Error, ObjectId,
    catalog_tree::{CatalogTree, PackLocation},
    closure::{CONNECTED, Closure, Edges},
    durable,
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
        let mut closure = Closure::new(&self.operation)?;
        closure
            .walk(&mut reader, &roots, CONNECTED, Edges::All)
            .await?;
        for (name, &id) in &self.state.refs {
            if name.starts_with(b"refs/heads/") && closure.kind(id) != Some(Kind::Commit) {
                return Err(Error::InvalidReference);
            }
        }
        closure.verify_all(&mut reader).await?;
        let _locations_memory = self.operation.reserve_state(memory_bound(
            closure.nodes.len(),
            size_of::<(ObjectId, PackLocation)>(),
        )?)?;
        let mut locations = Vec::with_capacity(closure.nodes.len());
        for &id in closure.nodes.keys() {
            locations.push((
                id,
                reader
                    .selected_location(id)
                    .await?
                    .ok_or(Error::InvalidReference)?,
            ));
        }
        drop(closure);
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
            let selected = durable::SelectedIndex::load(
                &self.operation,
                &self.log,
                &self.view,
                &first.descriptor,
                &first.root,
            )
            .await?;
            for (id, location) in &locations[start..end] {
                if location.descriptor != first.descriptor {
                    return Err(Error::InvalidPack(
                        "catalog pack identity differs across leaves".into(),
                    ));
                }
                selected.verify_position(*id, location.index)?;
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
        self.verify_changed_sources(&tree, &locations).await?;
        self.state.catalog = CatalogState::Tree(tree.root().cloned());
        self.checkpoint_snapshot(|_| true).await
    }

    pub(super) async fn verify_changed_sources(
        &self,
        tree: &CatalogTree,
        locations: &[(ObjectId, PackLocation)],
    ) -> Result<(), Error> {
        let candidate =
            durable::Catalog::from_tree(&self.operation, self.format, tree.root().cloned())?;
        let mut reader = durable::Reader::new(&self.log, &self.view, &candidate);
        for (id, original) in locations {
            let chosen = reader
                .selected_location(*id)
                .await?
                .ok_or(Error::InvalidReference)?;
            // Equal logical pack IDs can name distinct immutable copies. Verify
            // any changed source before discarding its previously verified copy.
            if chosen.root.reference() != original.root.reference()
                && reader.verify(*id).await?.is_none()
            {
                return Err(Error::InvalidReference);
            }
        }
        drop(reader);
        drop(candidate);
        Ok(())
    }
}
