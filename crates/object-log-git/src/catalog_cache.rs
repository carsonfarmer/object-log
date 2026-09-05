//! A bounded decoded-node cache tied to one log view and admitted operation.

use super::{CatalogTree, Loaded, PackLocation, check_bounds, invalid, read_memory, read_reserved};
use crate::{
    Error, ObjectId,
    format::PackDescriptor,
    pack::budget::{Operation, Reservation},
};
use object_log::{Digest, ObjectRef};
use object_log::{Log, StagedObject, View};

const RETAINED_BYTES: usize = 2 * 1024 * 1024;
type Key = (Digest, u64);

struct Cached {
    key: Key,
    reference: ObjectRef,
    node: Loaded,
    bytes: usize,
    used: u64,
}

pub(crate) struct CatalogCache<'a> {
    log: &'a Log,
    view: &'a View,
    operation: &'a Operation,
    tree: CatalogTree,
    // Sorted, adaptively admitted storage includes headers in the byte bound.
    nodes: Vec<Cached>,
    memory: Reservation,
    bytes: usize,
    clock: u64,
}

impl<'a> CatalogCache<'a> {
    pub(crate) fn new(
        tree: &CatalogTree,
        log: &'a Log,
        view: &'a View,
        operation: &'a Operation,
    ) -> Result<Self, Error> {
        let memory = operation.reserve_state(0)?;
        Ok(Self {
            log,
            view,
            operation,
            tree: tree.clone(),
            nodes: Vec::new(),
            memory,
            bytes: 0,
            clock: 0,
        })
    }

    pub(crate) async fn lookup(&mut self, id: ObjectId) -> Result<Option<PackLocation>, Error> {
        if id.format() != self.tree.format {
            return Err(invalid("catalog object format differs"));
        }
        let Some(mut proof) = self.tree.root.clone() else {
            return Ok(None);
        };
        let mut expected = None;
        let mut upper = None;
        loop {
            let index = self.node(&proof).await?;
            let node = &self.nodes[index].node;
            // Bounds belong to this path, including on cache hits. A node may
            // not borrow a less restrictive bound from an earlier traversal.
            check_bounds(node, expected, upper)?;
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
            expected = Some((payload.level - 1, payload.keys[position]));
            upper = payload.keys.get(position + 1).copied().or(upper);
            proof = node.children[position].clone();
        }
    }

    async fn node(&mut self, proof: &StagedObject) -> Result<usize, Error> {
        self.clock = self
            .clock
            .checked_add(1)
            .ok_or_else(|| invalid("catalog cache clock overflow"))?;
        self.operation
            .work((self.nodes.len().max(1).ilog2() as usize + 1) * size_of::<Key>())?;
        let reference = proof.reference();
        let key = (reference.digest(), reference.len());
        if let Ok(index) = self.nodes.binary_search_by_key(&key, |entry| entry.key) {
            if self.nodes[index].reference == *reference {
                self.nodes[index].used = self.clock;
                return Ok(index);
            }
            // Equal content hashes do not authorize a different physical ref.
            self.remove(index);
        }
        let bound = read_memory(proof)?;
        let memory = loop {
            match self.operation.reserve_state(bound) {
                Ok(memory) => break memory,
                Err(error) if self.nodes.is_empty() => return Err(error),
                Err(_) => self.evict()?,
            }
        };
        let mut node = read_reserved(
            self.log,
            self.view,
            self.operation,
            self.tree.format,
            proof,
            memory,
        )
        .await?;
        let payload = &node.payload;
        let bytes = payload.keys.capacity() * size_of::<ObjectId>()
            + payload.packs.capacity() * size_of::<PackDescriptor>()
            + payload.slots.capacity() * size_of::<u16>()
            + payload.indexes.capacity() * size_of::<u32>()
            + node.children.capacity() * size_of::<StagedObject>();
        if bytes > RETAINED_BYTES || bytes > bound {
            return Err(invalid("catalog decoded node exceeds memory bound"));
        }
        node.memory.shrink(bound - bytes)?;
        self.make_room(bytes)?;
        self.operation
            .work(self.nodes.len() * size_of::<Cached>())?;
        let index = self.nodes.partition_point(|entry| entry.key < key);
        self.nodes.insert(
            index,
            Cached {
                key,
                reference: reference.clone(),
                node,
                bytes,
                used: self.clock,
            },
        );
        self.bytes += bytes;
        Ok(index)
    }

    fn make_room(&mut self, bytes: usize) -> Result<(), Error> {
        loop {
            let capacity = if self.nodes.len() == self.nodes.capacity() {
                self.nodes
                    .capacity()
                    .max(16)
                    .checked_mul(2)
                    .ok_or_else(|| invalid("catalog cache capacity overflow"))?
            } else {
                self.nodes.capacity()
            };
            let headers = capacity
                .checked_mul(size_of::<Cached>())
                .ok_or_else(|| invalid("catalog cache size overflow"))?;
            if self.bytes + bytes + headers > RETAINED_BYTES {
                self.evict()?;
                continue;
            }
            if capacity != self.nodes.capacity() {
                // Old and replacement allocations coexist until the move. Both
                // are admitted; retained payload plus headers stays below 2 MiB.
                let memory = match self.operation.reserve_state(headers) {
                    Ok(memory) => memory,
                    Err(error) if self.nodes.is_empty() => return Err(error),
                    Err(_) => {
                        self.evict()?;
                        continue;
                    }
                };
                self.operation
                    .work(self.nodes.len() * size_of::<Cached>())?;
                let mut nodes = Vec::with_capacity(capacity);
                nodes.append(&mut self.nodes);
                self.nodes = nodes;
                self.memory = memory;
            }
            return Ok(());
        }
    }

    fn remove(&mut self, index: usize) {
        self.bytes -= self.nodes.remove(index).bytes;
    }

    fn evict(&mut self) -> Result<(), Error> {
        self.operation
            .work(self.nodes.len() * size_of::<Cached>())?;
        let index = self
            .nodes
            .iter()
            .enumerate()
            .min_by_key(|(_, entry)| entry.used)
            .map(|(index, _)| index)
            .ok_or_else(|| invalid("catalog cache cannot evict"))?;
        self.remove(index);
        Ok(())
    }
}
