use std::collections::BTreeMap;

use bytes::Bytes;
use object_log::{Materializer, StagedObject};

use crate::RefUpdate;
use crate::format::{PackDescriptor, Record};
use crate::{Error, ObjectFormat, ObjectId, RefSnapshot};

#[derive(Clone, Debug, Default)]
pub(crate) struct State {
    pub(crate) refs: RefSnapshot,
    pub(crate) packs: BTreeMap<ObjectId, (u64, StagedObject)>,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct Machine(ObjectFormat);

impl Machine {
    pub(crate) const fn new(format: ObjectFormat) -> Self {
        Self(format)
    }

    pub(crate) fn transaction(
        self,
        updates: Vec<RefUpdate>,
        packs: Vec<PackDescriptor>,
    ) -> Result<Bytes, Error> {
        Record::transaction(self.0, updates, packs)?.encode()
    }
}

impl Materializer for Machine {
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
        let record = Record::decode(checkpoint, self.0, objects.len())?;
        if !record.checkpoint {
            return Err(Error::InvalidRecord("checkpoint is a transaction"));
        }
        let (refs, packs) = record.into_snapshot()?;
        Ok(State {
            refs,
            packs: zip(packs, objects),
        })
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
        for update in record.refs {
            if let Some(target) = update.target {
                state.refs.insert(update.name, target);
            } else {
                state.refs.remove(&update.name);
            }
        }
        state.packs.extend(zip(record.packs, objects));
        Ok(())
    }
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
