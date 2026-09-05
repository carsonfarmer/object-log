use std::{collections::BTreeMap, mem::size_of, sync::Mutex};

use bytes::Bytes;
use object_log::{Materializer, ObjectRef, StagedObject};

use crate::pack::budget::{Operation, Reservation};

use crate::RefUpdate;
use crate::format::{CatalogOperation, Metadata, PackDescriptor, Record};
use crate::{Error, ObjectFormat, ObjectId, RefSnapshot};

#[derive(Clone, Debug, Default)]
pub(crate) enum CatalogState {
    #[default]
    Legacy,
    Tree(Option<StagedObject>),
}

#[derive(Clone, Debug, Default)]
pub(crate) struct State {
    pub(crate) refs: RefSnapshot,
    pub(crate) default_branch: Option<Vec<u8>>,
    pub(crate) packs: BTreeMap<ObjectId, (u64, StagedObject)>,
    pub(crate) catalog: CatalogState,
}

impl State {
    pub(crate) fn catalog_tree(
        &self,
        format: ObjectFormat,
    ) -> Option<crate::catalog_tree::CatalogTree> {
        match &self.catalog {
            CatalogState::Legacy => None,
            CatalogState::Tree(root) => Some(root.clone().map_or_else(
                || crate::catalog_tree::CatalogTree::empty(format),
                |root| crate::catalog_tree::CatalogTree::from_root(format, root),
            )),
        }
    }

    pub(crate) fn default_branch(&self) -> &[u8] {
        self.default_branch.as_deref().unwrap_or(b"refs/heads/main")
    }

    pub(crate) fn transaction(
        &self,
        format: ObjectFormat,
        updates: Vec<RefUpdate>,
        packs: Vec<PackDescriptor>,
    ) -> Result<Bytes, Error> {
        let record = Record::transaction(format, updates, packs)?;
        let record = if self.default_branch.is_some() {
            record.with_metadata(Metadata::Unchanged)?
        } else {
            record
        };
        let record = if matches!(self.catalog, CatalogState::Tree(_)) {
            record.with_catalog(CatalogOperation::Replace)?
        } else {
            record
        };
        record.encode()
    }
}

#[derive(Clone, Copy)]
pub(crate) struct Machine<'a>(ObjectFormat, Option<&'a StateBudget>);

impl<'a> Machine<'a> {
    #[cfg(test)]
    pub(crate) const fn new(format: ObjectFormat) -> Self {
        Self(format, None)
    }

    pub(crate) const fn budgeted(format: ObjectFormat, budget: &'a StateBudget) -> Self {
        Self(format, Some(budget))
    }

    #[cfg(test)]
    pub(crate) fn transaction(
        self,
        updates: Vec<RefUpdate>,
        packs: Vec<PackDescriptor>,
    ) -> Result<Bytes, Error> {
        Record::transaction(self.0, updates, packs)?.encode()
    }
}

impl Materializer for Machine<'_> {
    type State = State;
    type Error = Error;

    fn empty(&self) -> Self::State {
        State::default()
    }

    fn restore(
        &self,
        checkpoint: &[u8],
        objects: &[StagedObject],
    ) -> Result<Self::State, Self::Error> {
        let mut record = Record::decode(checkpoint, self.0, objects.len())?;
        if !record.checkpoint {
            return Err(Error::InvalidRecord("checkpoint is a transaction"));
        }
        let retained = self
            .1
            .map(|budget| budget.prepare(&State::default(), &record))
            .transpose()?;
        let default_branch = match record.metadata.take() {
            Some(Metadata::Snapshot(target)) => Some(target),
            _ => None,
        };
        let catalog = catalog_state(&record, objects)?;
        let (refs, packs) = record.into_snapshot()?;
        let state = State {
            refs,
            default_branch,
            packs: zip(packs, objects),
            catalog,
        };
        validate_empty_tree(&state)?;
        if let Some((budget, retained)) = self.1.zip(retained) {
            budget.finish(retained)?;
        }
        Ok(state)
    }

    fn apply(
        &self,
        state: &mut Self::State,
        operation: &[u8],
        objects: &[StagedObject],
    ) -> Result<(), Self::Error> {
        let record = Record::decode(operation, self.0, objects.len())?;
        if record.checkpoint {
            return Err(Error::InvalidRecord("operation is a checkpoint"));
        }
        match (&state.catalog, record.catalog) {
            (CatalogState::Tree(_), Some(CatalogOperation::Unchanged))
                if record.packs.is_empty() => {}
            (CatalogState::Tree(_), Some(CatalogOperation::Replace)) => {}
            (CatalogState::Legacy, Some(CatalogOperation::Replace))
            | (CatalogState::Tree(_), _) => {
                return Err(Error::InvalidRecord("invalid catalog mode transition"));
            }
            _ => {}
        }
        let next_catalog = record
            .tree_root_operation()
            .then(|| catalog_state(&record, objects))
            .transpose()?;
        if matches!(
            next_catalog.as_ref().unwrap_or(&state.catalog),
            CatalogState::Tree(None)
        ) {
            let survivors = state.refs.keys().any(|name| {
                !record
                    .refs
                    .iter()
                    .any(|update| &update.name == name && update.target.is_none())
            });
            if survivors || record.refs.iter().any(|update| update.target.is_some()) {
                return Err(Error::InvalidRecord("empty catalog has refs"));
            }
        }
        if state.default_branch.is_some() && record.metadata.is_none() {
            return Err(Error::InvalidRecord("Git state version downgrade"));
        }
        if let Some(Metadata::Update { expected, .. }) = &record.metadata {
            if expected.as_slice() != state.default_branch() {
                return Err(Error::StateDiverged);
            }
        } else if state.default_branch.is_none() && record.metadata.is_some() {
            return Err(Error::InvalidRecord("Git upgrade lacks complete metadata"));
        }
        if record
            .refs
            .iter()
            .any(|update| state.refs.get(update.name.as_slice()).copied() != update.expected)
        {
            return Err(Error::StateDiverged);
        }
        if record
            .packs
            .iter()
            .any(|pack| state.packs.contains_key(&pack.id))
        {
            return Err(Error::InvalidRecord("pack is already present"));
        }
        let retained = self
            .1
            .map(|budget| budget.prepare(state, &record))
            .transpose()?;
        for update in record.refs {
            if let Some(target) = update.target {
                state.refs.insert(update.name, target);
            } else {
                state.refs.remove(&update.name);
            }
        }
        // Removing the final key retains an allocated BTree root. Drop it
        // before releasing the empty map's leaf allowance.
        if state.refs.is_empty() {
            state.refs = BTreeMap::new();
        }
        if let Some(Metadata::Update { target, .. }) = record.metadata {
            state.default_branch = Some(target);
        }
        if let Some(catalog) = next_catalog {
            state.catalog = catalog;
            state.packs = BTreeMap::new();
        } else {
            state.packs.extend(zip(record.packs, objects));
        }
        if let Some((budget, retained)) = self.1.zip(retained) {
            budget.finish(retained)?;
        }
        Ok(())
    }
}

fn catalog_state(record: &Record, objects: &[StagedObject]) -> Result<CatalogState, Error> {
    if !record.tree_root_operation() {
        return Ok(CatalogState::Legacy);
    }
    if objects
        .first()
        .is_some_and(|root| root.reference().kind() != object_log::ObjectKind::Node)
    {
        return Err(Error::InvalidRecord("catalog root is not a node"));
    }
    Ok(CatalogState::Tree(objects.first().cloned()))
}

fn validate_empty_tree(state: &State) -> Result<(), Error> {
    if matches!(state.catalog, CatalogState::Tree(None)) && !state.refs.is_empty() {
        return Err(Error::InvalidRecord("empty catalog has refs"));
    }
    Ok(())
}

// Fourfold entry/name storage bounds BTree node occupancy, decoded Vec
// capacity and the descriptor/proof vectors needed to publish a snapshot.
const REF_MEMORY: usize = 4 * size_of::<(Vec<u8>, ObjectId)>();
const PACK_MEMORY: usize =
    4 * (size_of::<(ObjectId, (u64, StagedObject))>() + size_of::<(PackDescriptor, ObjectRef)>());
const REF_LEAF: usize = 12 * size_of::<(Vec<u8>, ObjectId)>();
const PACK_LEAF: usize = 12 * size_of::<(ObjectId, (u64, StagedObject))>();

/// Tracks retained maps independently from the bounded transient decoder window.
/// The mutex keeps borrowed Machine materialization futures Send; no lock spans I/O.
pub(crate) struct StateBudget(Mutex<(Reservation, usize)>);

impl StateBudget {
    pub(crate) fn new(operation: &Operation) -> Result<Self, Error> {
        Ok(Self(Mutex::new((operation.reserve_state(0)?, 0))))
    }

    // Reserve all insertions before applying any deletion. Sorted mixed batches
    // can temporarily hold both sets. Updates to existing names do not accumulate.
    fn prepare(&self, state: &State, record: &Record) -> Result<usize, Error> {
        let mut added = record
            .packs
            .len()
            .checked_mul(PACK_MEMORY)
            .ok_or_else(memory_error)?;
        let mut removed = 0_usize;
        if let Some(target) = record.metadata.as_ref().and_then(Metadata::target) {
            added = added
                .checked_add(target.len().checked_mul(4).ok_or_else(memory_error)?)
                .ok_or_else(memory_error)?;
            removed = state
                .default_branch
                .as_ref()
                .map_or(0, Vec::len)
                .checked_mul(4)
                .ok_or_else(memory_error)?;
        }
        let (mut creates, mut deletes) = (0, 0);
        for update in &record.refs {
            let present = state.refs.contains_key(&update.name);
            let bytes = update
                .name
                .len()
                .checked_mul(4)
                .and_then(|n| n.checked_add(REF_MEMORY))
                .ok_or_else(memory_error)?;
            if !present && update.target.is_some() {
                creates += 1;
                added = added.checked_add(bytes).ok_or_else(memory_error)?;
            } else if present && update.target.is_none() {
                deletes += 1;
                removed = removed.checked_add(bytes).ok_or_else(memory_error)?;
            }
        }
        if state.refs.is_empty() && creates != 0 {
            added = added.checked_add(REF_LEAF).ok_or_else(memory_error)?;
        }
        if state.packs.is_empty() && !record.packs.is_empty() {
            added = added.checked_add(PACK_LEAF).ok_or_else(memory_error)?;
        }
        if !state.refs.is_empty() && state.refs.len() + creates == deletes {
            removed = removed.checked_add(REF_LEAF).ok_or_else(memory_error)?;
        }
        if record.catalog == Some(CatalogOperation::Migrate) {
            removed = removed
                .checked_add(
                    state
                        .packs
                        .len()
                        .checked_mul(PACK_MEMORY)
                        .ok_or_else(memory_error)?,
                )
                .and_then(|bytes| {
                    bytes.checked_add(if state.packs.is_empty() { 0 } else { PACK_LEAF })
                })
                .ok_or_else(memory_error)?;
        }
        let mut held = self.0.lock().map_err(|_| memory_error())?;
        let next = held.1.checked_add(added).ok_or_else(memory_error)?;
        let retained = next.checked_sub(removed).ok_or_else(memory_error)?;
        held.0.grow(added)?;
        held.1 = next;
        Ok(retained)
    }

    fn finish(&self, retained: usize) -> Result<(), Error> {
        let mut held = self.0.lock().map_err(|_| memory_error())?;
        let released = held.1.checked_sub(retained).ok_or_else(memory_error)?;
        held.0.shrink(released)?;
        held.1 = retained;
        Ok(())
    }

    pub(crate) fn into_reservation(self) -> Result<Reservation, Error> {
        Ok(self.0.into_inner().map_err(|_| memory_error())?.0)
    }
}

fn memory_error() -> Error {
    Error::InvalidPack("Git state exceeds memory".into())
}

fn zip(
    descriptors: Vec<PackDescriptor>,
    objects: &[StagedObject],
) -> BTreeMap<ObjectId, (u64, StagedObject)> {
    descriptors
        .into_iter()
        .zip(objects.iter().cloned())
        .map(|(descriptor, object)| (descriptor.id, (descriptor.bytes, object)))
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use object_log::{Log, LogId, Options, ValidatedBackend};
    use object_store::memory::InMemory;
    use object_store::path::Path;

    use super::*;

    fn id(byte: u8) -> ObjectId {
        ObjectId(crate::Digest::Sha1([byte; 20]))
    }

    fn pack(byte: u8) -> PackDescriptor {
        PackDescriptor {
            id: id(byte),
            bytes: 4,
        }
    }

    fn checkpoint(machine: Machine, state: &State) -> Result<Bytes, Error> {
        let packs = state
            .packs
            .iter()
            .map(|(&id, (bytes, _))| PackDescriptor { id, bytes: *bytes })
            .collect();
        Record::snapshot(machine.0, state.refs.clone(), packs)?.encode()
    }

    #[test]
    fn metadata_upgrades_preserve_refs_and_reject_downgrades() -> Result<(), Error> {
        let machine = Machine::new(ObjectFormat::Sha1);
        let mut state = machine.empty();
        let create = machine.transaction(
            vec![RefUpdate::new("refs/heads/master", None, Some(id(1)))?],
            vec![],
        )?;
        machine.apply(&mut state, &create, &[])?;
        let upgrade = Record::metadata_update(
            ObjectFormat::Sha1,
            b"refs/heads/main".to_vec(),
            b"refs/heads/master".to_vec(),
        )?
        .encode()?;
        machine.apply(&mut state, &upgrade, &[])?;
        assert_eq!(state.default_branch(), b"refs/heads/master");
        assert_eq!(state.refs.len(), 1);
        assert!(matches!(
            machine.apply(&mut state, &upgrade, &[]),
            Err(Error::StateDiverged)
        ));
        let delete = machine.transaction(
            vec![RefUpdate::new("refs/heads/master", Some(id(1)), None)?],
            vec![],
        )?;
        assert!(machine.apply(&mut state, &delete, &[]).is_err());
        assert_eq!(state.refs.len(), 1);
        let delete = state.transaction(
            ObjectFormat::Sha1,
            vec![RefUpdate::new("refs/heads/master", Some(id(1)), None)?],
            vec![],
        )?;
        machine.apply(&mut state, &delete, &[])?;
        assert!(state.refs.is_empty());
        assert_eq!(state.default_branch(), b"refs/heads/master");
        let snapshot = Record::snapshot(ObjectFormat::Sha1, state.refs, vec![])?
            .with_metadata(Metadata::Snapshot(b"refs/heads/master".to_vec()))?
            .encode()?;
        let restored = machine.restore(&snapshot, &[])?;
        assert_eq!(restored.default_branch(), b"refs/heads/master");
        assert!(restored.refs.is_empty());
        Ok(())
    }

    #[test]
    fn metadata_retention_is_precharged_before_upgrade() -> Result<(), Error> {
        use crate::pack::budget::Pool;
        let target = b"refs/heads/trunk";
        let update = Record::metadata_update(
            ObjectFormat::Sha1,
            b"refs/heads/main".to_vec(),
            target.to_vec(),
        )?
        .encode()?;
        let operation = Pool::new(target.len() * 4 - 1).admit()?;
        let budget = StateBudget::new(&operation)?;
        let machine = Machine::budgeted(ObjectFormat::Sha1, &budget);
        let mut state = machine.empty();
        assert!(machine.apply(&mut state, &update, &[]).is_err());
        assert!(state.default_branch.is_none());
        assert_eq!(operation.live_bytes(), 0);
        Ok(())
    }

    #[test]
    fn retained_state_is_reserved_before_mutation_and_released_after_deletion() -> Result<(), Error>
    {
        use crate::pack::budget::Pool;
        let name = "refs/tags/a";
        let needed = REF_MEMORY + name.len() * 4 + REF_LEAF;
        let operation = Pool::new(needed - 1).admit()?;
        let budget = StateBudget::new(&operation)?;
        let machine = Machine::budgeted(ObjectFormat::Sha1, &budget);
        let mut state = machine.empty();
        let create = machine.transaction(vec![RefUpdate::new(name, None, Some(id(1)))?], vec![])?;
        assert!(machine.apply(&mut state, &create, &[]).is_err());
        assert!(state.refs.is_empty());
        assert_eq!(operation.live_bytes(), 0);
        let operation = Pool::new(needed).admit()?;
        let budget = StateBudget::new(&operation)?;
        let machine = Machine::budgeted(ObjectFormat::Sha1, &budget);
        machine.apply(&mut state, &create, &[])?;
        assert_eq!(operation.live_bytes(), needed);
        for (old, new) in [(1, 2), (2, 1)] {
            let update = machine.transaction(
                vec![RefUpdate::new(name, Some(id(old)), Some(id(new)))?],
                vec![],
            )?;
            machine.apply(&mut state, &update, &[])?;
            assert_eq!(operation.live_bytes(), needed);
        }
        let delete = machine.transaction(vec![RefUpdate::new(name, Some(id(1)), None)?], vec![])?;
        for round in 0..4 {
            if round != 0 {
                machine.apply(&mut state, &create, &[])?;
                assert_eq!(operation.live_bytes(), needed);
            }
            machine.apply(&mut state, &delete, &[])?;
            assert!(state.refs.is_empty());
            assert_eq!(operation.live_bytes(), 0);
        }
        Ok(())
    }

    #[test]
    fn transaction_is_sorted_and_applied_atomically() -> Result<(), Error> {
        let machine = Machine::new(ObjectFormat::Sha1);
        let mut state = machine.empty();
        let operation = machine.transaction(
            vec![
                RefUpdate::new("refs/tags/v1", None, Some(id(2)))?,
                RefUpdate::new("refs/heads/main", None, Some(id(1)))?,
            ],
            vec![],
        )?;
        machine.apply(&mut state, &operation, &[])?;
        assert_eq!(
            state.refs.get(&b"refs/heads/main"[..]).copied(),
            Some(id(1))
        );
        assert_eq!(state.refs.get(&b"refs/tags/v1"[..]).copied(), Some(id(2)));

        let before = state.refs.clone();
        let invalid = machine.transaction(
            vec![
                RefUpdate::new("refs/heads/main", Some(id(1)), Some(id(3)))?,
                RefUpdate::new("refs/tags/v1", Some(id(9)), None)?,
            ],
            vec![],
        )?;
        assert!(matches!(
            machine.apply(&mut state, &invalid, &[]),
            Err(Error::StateDiverged)
        ));
        assert_eq!(state.refs, before);
        Ok(())
    }

    #[test]
    fn checkpoint_round_trips_empty_pack_set() -> Result<(), Error> {
        let machine = Machine::new(ObjectFormat::Sha256);
        let mut state = machine.empty();
        let operation = machine.transaction(
            vec![RefUpdate::new(
                "refs/heads/main",
                None,
                Some(ObjectId::from_bytes(ObjectFormat::Sha256, &[7; 32])?),
            )?],
            vec![],
        )?;
        machine.apply(&mut state, &operation, &[])?;
        let restored = machine.restore(&checkpoint(machine, &state)?, &[])?;
        assert_eq!(restored.refs, state.refs);
        assert!(restored.packs.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn pack_descriptors_stay_aligned_with_roots() -> Result<(), Box<dyn std::error::Error>> {
        let backend =
            ValidatedBackend::new(Arc::new(InMemory::new()), Path::from("git-format-tests"))
                .await?;
        let log = Log::open(&backend, &LogId::new("pack-alignment")?, Options::default()).await?;
        let view = log.load().await?;
        let object = log.put_object(&view, Bytes::from_static(b"PACK")).await?;
        let descriptor = pack(4);
        let machine = Machine::new(ObjectFormat::Sha1);
        let mut state = machine.empty();
        let operation = machine.transaction(
            vec![RefUpdate::new("refs/heads/main", None, Some(id(1)))?],
            vec![descriptor.clone()],
        )?;
        assert!(machine.apply(&mut state, &operation, &[]).is_err());
        machine.apply(&mut state, &operation, std::slice::from_ref(&object))?;
        assert_eq!(state.packs[&descriptor.id].0, descriptor.bytes);
        assert_eq!(
            state.packs[&descriptor.id].1.reference(),
            object.reference()
        );

        let restored =
            machine.restore(&checkpoint(machine, &state)?, std::slice::from_ref(&object))?;
        assert_eq!(restored.packs[&descriptor.id].0, descriptor.bytes);
        assert_eq!(
            restored.packs[&descriptor.id].1.reference(),
            object.reference()
        );
        Ok(())
    }
}
