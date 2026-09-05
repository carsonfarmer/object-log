//! Lazy authenticated catalog lookup and a bounded selected-index cache.

use std::{ops::Deref, sync::Arc};

use std::mem::size_of;

use object_log::{ObjectRef, StagedObject};

use super::{Catalog, Location, Pack, Reader, SelectedIndex, selected_index_bytes};
use crate::catalog_tree::{CatalogCache, CatalogTree, PackLocation};
use crate::{
    Error, ObjectFormat, ObjectId,
    pack::{
        budget::{Operation, Reservation},
        invalid, pack_error,
    },
};

const SELECTED_PACKS: usize = 8;

type Selected<'a> = Arc<SelectedIndex<'a>>;

pub(super) struct SelectedPacks<'a> {
    slots: Vec<Option<Selected<'a>>>,
    next: usize,
    _memory: Reservation,
}

impl<'a> SelectedPacks<'a> {
    fn new(operation: &Operation) -> Result<Self, Error> {
        let memory = operation.reserve_state(SELECTED_PACKS * size_of::<Option<Selected<'a>>>())?;
        Ok(Self {
            slots: (0..SELECTED_PACKS).map(|_| None).collect(),
            next: 0,
            _memory: memory,
        })
    }
}

pub(super) enum PackRef<'a> {
    Legacy(&'a Pack),
    Selected(Selected<'a>),
}

impl Deref for PackRef<'_> {
    type Target = Pack;

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Legacy(pack) => pack,
            Self::Selected(index) => &index.pack,
        }
    }
}

impl Catalog {
    /// The exact proof is supplied by replayed repository state.
    pub(crate) fn from_tree(
        operation: &Operation,
        format: ObjectFormat,
        root: Option<StagedObject>,
    ) -> Result<Self, Error> {
        let memory = operation.reserve_state(size_of::<CatalogTree>())?;
        Ok(Self {
            tree: Some(root.map_or_else(
                || CatalogTree::empty(format),
                |root| CatalogTree::from_root(format, root),
            )),
            format,
            packs: Box::new([]),
            directory: Vec::new(),
            operation: operation.clone(),
            _memory: memory,
        })
    }
}

impl<'a> Reader<'a> {
    /// Tree-only: legacy callers retain their existing state proof route.
    pub(crate) async fn selected_location(
        &mut self,
        id: ObjectId,
    ) -> Result<Option<PackLocation>, Error> {
        let tree = self.catalog.tree.as_ref().ok_or_else(|| {
            Error::InvalidPack("selected location requires a tree catalog".into())
        })?;
        if self.tree_cache.is_none() {
            self.tree_cache = Some(CatalogCache::new(
                tree,
                self.log,
                self.view,
                &self.catalog.operation,
            )?);
        }
        let cache = self
            .tree_cache
            .as_mut()
            .ok_or_else(|| Error::InvalidPack("catalog cache was not initialized".into()))?;
        let Some(location) = cache.lookup(id).await? else {
            return Ok(None);
        };
        self.select_pack(id, &location).await?;
        Ok(Some(location))
    }

    pub(super) async fn location(&mut self, id: ObjectId) -> Result<Option<Location>, Error> {
        if id.format() != self.catalog.format {
            return invalid("catalog object format differs");
        }
        if self.catalog.tree.is_none() {
            return Ok(self.catalog.location(id));
        }
        let Some(selected) = self.selected_location(id).await? else {
            return Ok(None);
        };
        // select_pack finds the authenticated entry just loaded above without I/O.
        let pack = self.select_pack(id, &selected).await?;
        Ok(Some(Location {
            pack,
            index: selected.index,
        }))
    }

    #[allow(
        clippy::expect_used,
        reason = "private locations refer to resident selected indexes"
    )]
    pub(super) fn pack(&self, slot: u16) -> PackRef<'a> {
        if self.catalog.tree.is_none() {
            return PackRef::Legacy(&self.catalog.packs[usize::from(slot)]);
        }
        PackRef::Selected(Arc::clone(
            self.selected_packs
                .as_ref()
                .expect("selected cache initialized")
                .slots[usize::from(slot)]
            .as_ref()
            .expect("selected slot resident"),
        ))
    }

    async fn select_pack(&mut self, id: ObjectId, location: &PackLocation) -> Result<u16, Error> {
        if self.selected_packs.is_none() {
            self.selected_packs = Some(SelectedPacks::new(&self.catalog.operation)?);
        }
        let operation = &self.catalog.operation;
        let packs = self
            .selected_packs
            .as_mut()
            .ok_or_else(|| Error::InvalidPack("selected cache was not initialized".into()))?;
        operation.work(SELECTED_PACKS * size_of::<ObjectRef>())?;
        for (slot, selected) in packs.slots.iter().enumerate() {
            if let Some(selected) = selected
                && selected.root.reference() == location.root.reference()
                && selected.pack.id == location.descriptor.id
                && u64::from(selected.pack.bytes) == location.descriptor.bytes
            {
                selected.verify_position(id, location.index)?;
                return u16::try_from(slot).map_err(pack_error);
            }
        }
        // Reserve before reading; evict only our own cache, never an active reader handle.
        let bound = selected_index_bytes(&location.descriptor, &location.root)?;
        loop {
            let memory = operation.reserve_state(bound);
            let empty = packs.slots.iter().position(Option::is_none);
            if memory.is_ok() && empty.is_some() {
                drop(memory);
                // The preflight reservation is released immediately before the load.
                // No await or other allocation occurs between them.
                break;
            }
            let candidate = (0..SELECTED_PACKS)
                .map(|offset| (packs.next + offset) % SELECTED_PACKS)
                .find(|slot| {
                    packs.slots[*slot]
                        .as_ref()
                        .is_some_and(|index| Arc::strong_count(index) == 1)
                });
            let Some(slot) = candidate else {
                return Err(memory.err().unwrap_or_else(|| {
                    Error::InvalidPack("selected index cache is pinned".into())
                }));
            };
            packs.slots[slot] = None;
            packs.next = (slot + 1) % SELECTED_PACKS;
            // Encoded chunks are keyed by immutable ObjectRef, not this reusable
            // selected-pack slot, and remain within their independent byte bound.
        }
        let slot = packs
            .slots
            .iter()
            .position(Option::is_none)
            .ok_or_else(|| Error::InvalidPack("selected cache has no free slot".into()))?;
        let selected = SelectedIndex::load(
            operation,
            self.log,
            self.view,
            &location.descriptor,
            &location.root,
        )
        .await?;
        selected.verify_position(id, location.index)?;
        // Account Arc counters in addition to SelectedIndex's retained data.
        // This reservation shares the selected object's lifetime.
        packs.slots[slot] = Some(Arc::new(selected));
        u16::try_from(slot).map_err(pack_error)
    }
}
