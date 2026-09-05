//! Private authenticated catalog foundation; not yet used by repository state.

use bytes::Bytes;
use minicbor::{Decoder, Encode};
use object_log::{Log, ObjectKind, StagedObject, View};

use crate::{
    Error, ObjectFormat, ObjectId,
    format::PackDescriptor,
    pack::budget::{Operation, Reservation},
};

#[path = "catalog_cache.rs"]
mod cache;
pub(crate) use cache::CatalogCache;

const VERSION: u8 = 1;
const FANOUT: usize = 64;
const MAX_HEIGHT: u8 = 8;
const NODE_BYTES: usize = 16 * 1024;

/// A tree root is a publication proof, never an independently mutable authority.
#[derive(Clone)]
pub(crate) struct CatalogTree {
    format: ObjectFormat,
    root: Option<StagedObject>,
}

#[derive(Clone)]
pub(crate) struct PackLocation {
    pub(crate) descriptor: PackDescriptor,
    pub(crate) root: StagedObject,
    pub(crate) index: u32,
}

#[derive(Encode)]
#[cbor(array)]
struct Payload {
    #[n(0)]
    version: u8,
    #[n(1)]
    format: ObjectFormat,
    #[n(2)]
    level: u8,
    #[n(3)]
    keys: Vec<ObjectId>,
    #[n(4)]
    packs: Vec<PackDescriptor>,
    #[n(5)]
    slots: Vec<u16>,
    #[n(6)]
    indexes: Vec<u32>,
}

struct Loaded {
    payload: Payload,
    children: Vec<StagedObject>,
    memory: Reservation,
    upper: Option<ObjectId>,
}

#[derive(Clone)]
struct Child {
    lower: ObjectId,
    level: u8,
    proof: StagedObject,
}

#[derive(Clone, Copy)]
struct Entry {
    id: ObjectId,
    slot: usize,
    index: u32,
}

fn invalid(message: &'static str) -> Error {
    Error::InvalidPack(message.into())
}

impl CatalogTree {
    pub(crate) const fn empty(format: ObjectFormat) -> Self {
        Self { format, root: None }
    }

    pub(crate) const fn from_root(format: ObjectFormat, root: StagedObject) -> Self {
        Self {
            format,
            root: Some(root),
        }
    }

    pub(crate) fn root(&self) -> Option<&StagedObject> {
        self.root.as_ref()
    }

    pub(crate) async fn lookup(
        &self,
        log: &Log,
        view: &View,
        operation: &Operation,
        id: ObjectId,
    ) -> Result<Option<PackLocation>, Error> {
        if id.format() != self.format {
            return Err(invalid("catalog object format differs"));
        }
        let Some(root) = &self.root else {
            return Ok(None);
        };
        let mut node = load(log, view, operation, self.format, root, None, None).await?;
        loop {
            let payload = &node.payload;
            if payload.level == 0 {
                let Ok(position) = payload.keys.binary_search(&id) else {
                    return Ok(None);
                };
                let slot = usize::from(payload.slots[position]);
                return Ok(Some(PackLocation {
                    descriptor: payload.packs[slot].clone(),
                    root: node.children[slot].clone(),
                    index: payload.indexes[position],
                }));
            }
            let end = payload.keys.partition_point(|key| *key <= id);
            if end == 0 {
                return Ok(None);
            }
            let position = end - 1;
            let child = Child {
                lower: payload.keys[position],
                level: payload.level - 1,
                proof: node.children[position].clone(),
            };
            let upper = payload.keys.get(position + 1).copied().or(node.upper);
            drop(node);
            node = load(
                log,
                view,
                operation,
                self.format,
                &child.proof,
                Some((child.level, child.lower)),
                upper,
            )
            .await?;
        }
    }

    /// Inserts a validated pack index's ordered OIDs. Duplicate OIDs select the
    /// lowest pack ID, matching the existing catalog. Reader integration must
    /// still cross-check the selected standard index's OID and position.
    pub(crate) async fn insert_pack(
        &self,
        log: &Log,
        view: &View,
        operation: &Operation,
        descriptor: PackDescriptor,
        root: StagedObject,
        entries: &[(ObjectId, u32)],
    ) -> Result<Self, Error> {
        if descriptor.id.format() != self.format
            || descriptor.bytes == 0
            || root.reference().kind() != ObjectKind::Node
        {
            return Err(invalid("catalog pack descriptor is invalid"));
        }
        if entries
            .iter()
            .any(|(id, index)| id.format() != self.format || *index >= crate::pack::MAX_OBJECTS)
            || entries.windows(2).any(|pair| pair[0].0 >= pair[1].0)
            || entries.len() > crate::pack::MAX_OBJECTS as usize
        {
            return Err(invalid("catalog insertion IDs are invalid"));
        }
        if entries.is_empty() {
            return Ok(self.clone());
        }
        // Merged entries, split outputs, local pack tables, and recursive
        // construction scratch; authenticated reads reserve their own buffers.
        let _scratch = operation.reserve_state(entries.len() * 256 + 64 * 1024)?;
        operation.work(entries.len() * 128)?;
        let context = Builder {
            log,
            view,
            operation,
            format: self.format,
            descriptor,
            root,
        };
        let mut roots = if let Some(root) = &self.root {
            let node = load(log, view, operation, self.format, root, None, None).await?;
            context.update(node, entries).await?
        } else {
            let merged = entries
                .iter()
                .map(|&(id, index)| Entry { id, slot: 0, index })
                .collect::<Vec<_>>();
            context
                .leaves(
                    &merged,
                    std::slice::from_ref(&context.descriptor),
                    std::slice::from_ref(&context.root),
                )
                .await?
        };
        while roots.len() > 1 {
            if roots[0].level == MAX_HEIGHT {
                return Err(invalid("catalog height limit exceeded"));
            }
            let next = context.branches(&roots).await?;
            if next.len() >= roots.len() {
                return Err(invalid("catalog node cannot hold two children"));
            }
            roots = next;
        }
        Ok(Self {
            format: self.format,
            root: roots.pop().map(|child| child.proof),
        })
    }
}

struct Builder<'a> {
    log: &'a Log,
    view: &'a View,
    operation: &'a Operation,
    format: ObjectFormat,
    descriptor: PackDescriptor,
    root: StagedObject,
}

impl Builder<'_> {
    async fn update(&self, node: Loaded, entries: &[(ObjectId, u32)]) -> Result<Vec<Child>, Error> {
        let Loaded {
            payload,
            mut children,
            memory: _memory,
            upper: inherited_upper,
        } = node;
        if payload.level == 0 {
            let mut packs = payload.packs;
            let incoming = packs
                .iter()
                .position(|pack| pack.id == self.descriptor.id)
                .unwrap_or(packs.len());
            if incoming == packs.len() {
                packs.push(self.descriptor.clone());
                children.push(self.root.clone());
            }
            let mut merged = Vec::with_capacity(payload.keys.len() + entries.len());
            let mut existing = 0;
            for &(id, index) in entries {
                while existing < payload.keys.len() && payload.keys[existing] < id {
                    merged.push(Entry {
                        id: payload.keys[existing],
                        slot: usize::from(payload.slots[existing]),
                        index: payload.indexes[existing],
                    });
                    existing += 1;
                }
                if existing < payload.keys.len() && payload.keys[existing] == id {
                    let slot = usize::from(payload.slots[existing]);
                    if packs[slot].id <= self.descriptor.id {
                        merged.push(Entry {
                            id,
                            slot,
                            index: payload.indexes[existing],
                        });
                    } else {
                        merged.push(Entry {
                            id,
                            slot: incoming,
                            index,
                        });
                    }
                    existing += 1;
                } else {
                    merged.push(Entry {
                        id,
                        slot: incoming,
                        index,
                    });
                }
            }
            while existing < payload.keys.len() {
                merged.push(Entry {
                    id: payload.keys[existing],
                    slot: usize::from(payload.slots[existing]),
                    index: payload.indexes[existing],
                });
                existing += 1;
            }
            return self.leaves(&merged, &packs, &children).await;
        }
        let mut updated = Vec::with_capacity(children.len() + entries.len().div_ceil(FANOUT));
        let mut start = 0;
        for (position, proof) in children.into_iter().enumerate() {
            let upper = payload.keys.get(position + 1).copied().or(inherited_upper);
            let end = upper.map_or(entries.len(), |upper| {
                entries.partition_point(|(id, _)| *id < upper)
            });
            let child = Child {
                lower: payload.keys[position],
                level: payload.level - 1,
                proof,
            };
            if start == end {
                updated.push(child);
            } else {
                let loaded = load(
                    self.log,
                    self.view,
                    self.operation,
                    self.format,
                    &child.proof,
                    Some((child.level, child.lower)),
                    upper,
                )
                .await?;
                updated.extend(Box::pin(self.update(loaded, &entries[start..end])).await?);
            }
            start = end;
        }
        self.branches(&updated).await
    }

    async fn leaves(
        &self,
        entries: &[Entry],
        packs: &[PackDescriptor],
        roots: &[StagedObject],
    ) -> Result<Vec<Child>, Error> {
        let mut pending = vec![entries];
        let mut output = Vec::new();
        while let Some(entries) = pending.pop() {
            if entries.len() > FANOUT {
                let middle = entries.len() / 2;
                pending.push(&entries[middle..]);
                pending.push(&entries[..middle]);
                continue;
            }
            let mut slots = entries.iter().map(|entry| entry.slot).collect::<Vec<_>>();
            slots.sort_unstable_by_key(|slot| packs[*slot].id);
            slots.dedup();
            let payload = Payload {
                version: VERSION,
                format: self.format,
                level: 0,
                keys: entries.iter().map(|entry| entry.id).collect(),
                packs: slots.iter().map(|slot| packs[*slot].clone()).collect(),
                slots: entries
                    .iter()
                    .map(|entry| {
                        slots
                            .iter()
                            .position(|slot| *slot == entry.slot)
                            .and_then(|slot| u16::try_from(slot).ok())
                            .ok_or_else(|| invalid("catalog slot overflow"))
                    })
                    .collect::<Result<_, _>>()?,
                indexes: entries.iter().map(|entry| entry.index).collect(),
            };
            let children = slots.iter().map(|slot| roots[*slot].clone()).collect();
            if let Some(child) = self.write(payload, children).await? {
                output.push(child);
            } else {
                if entries.len() == 1 {
                    return Err(invalid("catalog leaf exceeds object limit"));
                }
                let middle = entries.len() / 2;
                pending.push(&entries[middle..]);
                pending.push(&entries[..middle]);
            }
        }
        Ok(output)
    }

    async fn branches(&self, children: &[Child]) -> Result<Vec<Child>, Error> {
        let mut pending = vec![children];
        let mut output = Vec::new();
        while let Some(children) = pending.pop() {
            if children.len() > FANOUT {
                let middle = children.len() / 2;
                pending.push(&children[middle..]);
                pending.push(&children[..middle]);
                continue;
            }
            let payload = Payload {
                version: VERSION,
                format: self.format,
                level: children[0].level + 1,
                keys: children.iter().map(|child| child.lower).collect(),
                packs: Vec::new(),
                slots: Vec::new(),
                indexes: Vec::new(),
            };
            let proofs = children.iter().map(|child| child.proof.clone()).collect();
            if let Some(child) = self.write(payload, proofs).await? {
                output.push(child);
            } else {
                if children.len() == 1 {
                    return Err(invalid("catalog branch exceeds object limit"));
                }
                let middle = children.len() / 2;
                pending.push(&children[middle..]);
                pending.push(&children[..middle]);
            }
        }
        Ok(output)
    }

    async fn write(
        &self,
        payload: Payload,
        children: Vec<StagedObject>,
    ) -> Result<Option<Child>, Error> {
        if payload.level > MAX_HEIGHT {
            return Err(invalid("catalog height limit exceeded"));
        }
        let bytes = minicbor::to_vec(&payload).map_err(|_| invalid("catalog encoding failed"))?;
        let size = match self.log.node_size(
            bytes.len(),
            children.iter().map(|child| child.reference().len()),
        ) {
            Ok(size) if size <= NODE_BYTES => size,
            Ok(_) | Err(object_log::Error::LimitExceeded("object bytes" | "object references")) => {
                return Ok(None);
            }
            Err(error) => return Err(error.into()),
        };
        let _memory = self.operation.reserve(size)?;
        self.operation.work(size)?;
        self.operation.io(size)?;
        let _plan = crate::durable::publication_plan(self.operation, self.view)?;
        let proof = self.log.put_node(self.view, bytes.into(), children).await?;
        Ok(Some(Child {
            lower: payload.keys[0],
            level: payload.level,
            proof,
        }))
    }
}

async fn load(
    log: &Log,
    view: &View,
    operation: &Operation,
    format: ObjectFormat,
    proof: &StagedObject,
    expected: Option<(u8, ObjectId)>,
    upper: Option<ObjectId>,
) -> Result<Loaded, Error> {
    let memory = operation.reserve_state(read_memory(proof)?)?;
    let mut node = read_reserved(log, view, operation, format, proof, memory).await?;
    check_bounds(&node, expected, upper)?;
    node.upper = upper;
    Ok(node)
}

fn read_memory(proof: &StagedObject) -> Result<usize, Error> {
    let size =
        usize::try_from(proof.reference().len()).map_err(|_| invalid("catalog size overflow"))?;
    if size > NODE_BYTES {
        return Err(invalid("catalog node exceeds byte limit"));
    }
    Ok(size * 128 + 4096)
}

fn check_bounds(
    node: &Loaded,
    expected: Option<(u8, ObjectId)>,
    upper: Option<ObjectId>,
) -> Result<(), Error> {
    let payload = &node.payload;
    if expected.is_some_and(|(level, lower)| payload.level != level || payload.keys[0] != lower)
        || upper.is_some_and(|upper| payload.keys.last().is_some_and(|key| *key >= upper))
    {
        return Err(invalid("catalog child bounds differ"));
    }
    Ok(())
}

async fn read_reserved(
    log: &Log,
    view: &View,
    operation: &Operation,
    format: ObjectFormat,
    proof: &StagedObject,
    memory: Reservation,
) -> Result<Loaded, Error> {
    let size =
        usize::try_from(proof.reference().len()).map_err(|_| invalid("catalog size overflow"))?;
    operation.io(size)?;
    operation.work(size * 2)?;
    let (bytes, children) = log.read_staged_node(view, proof).await?;
    let payload = decode(&bytes, format, &children)?;
    Ok(Loaded {
        payload,
        children,
        memory,
        upper: None,
    })
}

fn decode(bytes: &[u8], format: ObjectFormat, children: &[StagedObject]) -> Result<Payload, Error> {
    fn array<'b, T>(
        d: &mut Decoder<'b>,
        mut item: impl FnMut(&mut Decoder<'b>) -> Result<T, minicbor::decode::Error>,
    ) -> Result<Vec<T>, Error> {
        let count = d
            .array()
            .map_err(|_| invalid("catalog array is invalid"))?
            .ok_or_else(|| invalid("catalog array is indefinite"))?;
        if count > FANOUT as u64 {
            return Err(invalid("catalog fanout exceeds limit"));
        }
        (0..count)
            .map(|_| item(d).map_err(|_| invalid("catalog item is invalid")))
            .collect()
    }
    let mut d = Decoder::new(bytes);
    if d.array().ok() != Some(Some(7)) {
        return Err(invalid("catalog shape is invalid"));
    }
    let version = d.u8().map_err(|_| invalid("catalog version is invalid"))?;
    let stored_format = d
        .decode()
        .map_err(|_| invalid("catalog format is invalid"))?;
    let level = d.u8().map_err(|_| invalid("catalog level is invalid"))?;
    let payload = Payload {
        version,
        format: stored_format,
        level,
        keys: array(&mut d, Decoder::decode)?,
        packs: array(&mut d, Decoder::decode)?,
        slots: array(&mut d, Decoder::u16)?,
        indexes: array(&mut d, Decoder::u32)?,
    };
    if d.position() != bytes.len()
        || minicbor::to_vec(&payload).ok().as_deref() != Some(bytes)
        || version != VERSION
        || stored_format != format
        || level > MAX_HEIGHT
        || payload.keys.is_empty()
        || payload.keys.iter().any(|id| id.format() != format)
        || payload.keys.windows(2).any(|pair| pair[0] >= pair[1])
        || children
            .iter()
            .any(|child| child.reference().kind() != ObjectKind::Node)
    {
        return Err(invalid("catalog node is invalid"));
    }
    if level == 0 {
        if payload.packs.len() != children.len()
            || payload.slots.len() != payload.keys.len()
            || payload.indexes.len() != payload.keys.len()
            || payload
                .packs
                .iter()
                .any(|pack| pack.id.format() != format || pack.bytes == 0)
            || payload
                .packs
                .windows(2)
                .any(|pair| pair[0].id >= pair[1].id)
            || payload
                .slots
                .iter()
                .any(|slot| usize::from(*slot) >= children.len())
        {
            return Err(invalid("catalog leaf is invalid"));
        }
        if payload
            .indexes
            .iter()
            .any(|index| *index >= crate::pack::MAX_OBJECTS)
            || (0..children.len()).any(|slot| {
                !payload
                    .slots
                    .iter()
                    .any(|stored| usize::from(*stored) == slot)
            })
        {
            return Err(invalid("catalog leaf positions are invalid"));
        }
    } else if children.len() != payload.keys.len()
        || !payload.packs.is_empty()
        || !payload.slots.is_empty()
        || !payload.indexes.is_empty()
    {
        return Err(invalid("catalog branch is invalid"));
    }
    Ok(payload)
}

#[cfg(test)]
#[path = "catalog_tree_tests.rs"]
mod tests;
