//! Charged reachability state without retained edges or a stored-pack count cap.
use crate::{
    Error, ObjectId,
    durable::Reader,
    pack::{
        budget::{Operation, Reservation},
        invalid,
    },
};
use gix_object::Kind;
use std::{collections::HashMap, mem::size_of};

pub(crate) const PUBLISHED: u8 = 1;
pub(crate) const WANTED: u8 = 2;
pub(crate) const PRESENT: u8 = 4;
pub(crate) const REQUESTED: u8 = 8;
pub(crate) const KNOWN: u8 = 16;
pub(crate) const ANCESTRY: u8 = 32;
pub(crate) const CONNECTED: u8 = 64;

#[derive(Clone, Copy)]
pub(crate) enum Edges {
    All,
    Commits,
    Objects,
}

pub(crate) struct Node {
    pub(crate) kind: Option<Kind>,
    pub(crate) verified: bool,
    marks: u8,
    scheduled: u8,
}

// A failed/cancelled walk has partial marks. Callers discard the entire
// request-local closure; only successful walks may share marks with a next pass.
pub(crate) struct Closure {
    pub(crate) nodes: HashMap<ObjectId, Node>,
    frontier: Vec<ObjectId>,
    table_limit: usize,
    operation: Operation,
    table_memory: Reservation,
    frontier_memory: Reservation,
}

impl Closure {
    pub(crate) fn new(operation: &Operation) -> Result<Self, Error> {
        Ok(Self {
            nodes: HashMap::new(),
            frontier: Vec::new(),
            table_limit: 0,
            operation: operation.clone(),
            table_memory: operation.reserve_state(0)?,
            frontier_memory: operation.reserve_state(0)?,
        })
    }

    pub(crate) fn kind(&self, id: ObjectId) -> Option<Kind> {
        self.nodes.get(&id).and_then(|node| node.kind)
    }
    pub(crate) fn marked(&self, id: ObjectId, mark: u8) -> bool {
        self.nodes
            .get(&id)
            .is_some_and(|node| node.marks & mark != 0)
    }

    pub(crate) fn clear_mark(&mut self, mark: u8) -> Result<(), Error> {
        self.operation.work(self.nodes.len() * size_of::<Node>())?;
        for node in self.nodes.values_mut() {
            node.marks &= !mark;
            node.scheduled &= !mark;
        }
        Ok(())
    }

    pub(crate) async fn walk(
        &mut self,
        reader: &mut Reader<'_>,
        roots: &[ObjectId],
        mark: u8,
        edges: Edges,
    ) -> Result<(), Error> {
        self.walk_until(reader, roots, mark, edges, None)
            .await
            .map(|_| ())
    }

    pub(crate) async fn reaches_commit(
        &mut self,
        reader: &mut Reader<'_>,
        target: ObjectId,
        expected: ObjectId,
    ) -> Result<bool, Error> {
        self.clear_mark(ANCESTRY)?;
        if self.kind(target) != Some(Kind::Commit) || self.kind(expected) != Some(Kind::Commit) {
            return Ok(false);
        }
        self.walk_until(reader, &[target], ANCESTRY, Edges::Commits, Some(expected))
            .await
    }

    async fn walk_until(
        &mut self,
        reader: &mut Reader<'_>,
        roots: &[ObjectId],
        mark: u8,
        edges: Edges,
        stop: Option<ObjectId>,
    ) -> Result<bool, Error> {
        for &id in roots {
            self.schedule(reader, id, None, true, mark).await?;
        }
        while let Some(id) = self.frontier.pop() {
            if stop == Some(id) {
                self.frontier.clear();
                return Ok(true);
            }
            let expected = self.kind(id);
            let kind = if let Some(kind) = expected {
                kind
            } else {
                reader
                    .object_kind(id)
                    .await?
                    .ok_or(Error::InvalidReference)?
            };
            if kind == Kind::Blob {
                // Content verification survives mark passes in this request/view.
                // Structural objects still need parsing to propagate each mark.
                if self.nodes.get(&id).is_some_and(|node| node.verified) {
                    continue;
                }
                let actual = reader.verify(id).await?.ok_or(Error::InvalidReference)?;
                self.verified(id, actual)?;
                continue;
            }
            let object = reader.find(id).await?.ok_or(Error::InvalidReference)?;
            self.verified(id, object.kind)?;
            let operation = self.operation.clone();
            crate::structure::visit(
                &operation,
                id,
                object.kind,
                &object.data,
                WalkerLinks {
                    closure: self,
                    reader,
                    mark,
                    edges,
                },
            )
            .await?;
        }
        Ok(false)
    }

    fn verified(&mut self, id: ObjectId, kind: Kind) -> Result<(), Error> {
        let node = self.nodes.get_mut(&id).ok_or(Error::InvalidReference)?;
        if node.kind.is_some_and(|expected| expected != kind) {
            return invalid("graph object kind does not match its reference");
        }
        node.kind = Some(kind);
        node.verified = true;
        Ok(())
    }

    pub(crate) async fn verify_all(&mut self, reader: &mut Reader<'_>) -> Result<(), Error> {
        for (&id, node) in &mut self.nodes {
            if !node.verified {
                let actual = reader.verify(id).await?.ok_or(Error::InvalidReference)?;
                if node.kind != Some(actual) {
                    return Err(Error::InvalidReference);
                }
                node.verified = true;
            }
        }
        Ok(())
    }

    async fn schedule(
        &mut self,
        reader: &mut Reader<'_>,
        id: ObjectId,
        kind: Option<Kind>,
        verify: bool,
        mark: u8,
    ) -> Result<(), Error> {
        self.operation
            .work(size_of::<ObjectId>() + size_of::<Node>())?;
        if !reader.contains(id).await? {
            return invalid("graph references a missing object");
        }
        if !self.nodes.contains_key(&id) {
            if self.nodes.len() == self.table_limit {
                self.grow_table()?;
            }
            self.nodes.insert(
                id,
                Node {
                    kind,
                    verified: false,
                    marks: 0,
                    scheduled: 0,
                },
            );
        }
        let node = self.nodes.get_mut(&id).ok_or(Error::InvalidReference)?;
        if let Some(kind) = kind {
            if node.kind.is_some_and(|expected| expected != kind) {
                return invalid("graph object kind does not match its reference");
            }
            node.kind = Some(kind);
        }
        node.marks |= mark;
        if verify && node.scheduled & mark == 0 {
            node.scheduled |= mark;
            if self.frontier.len() == self.frontier.capacity() {
                self.grow_frontier()?;
            }
            self.frontier.push(id);
        }
        Ok(())
    }

    fn grow_table(&mut self) -> Result<(), Error> {
        let limit = self
            .table_limit
            .max(64)
            .checked_mul(2)
            .ok_or(Error::InvalidReference)?;
        // At most two buckets per requested entry plus trailing controls.
        // Keep the old reservation until the replacement table is populated.
        let bytes = limit
            .checked_mul(2 * (size_of::<(ObjectId, Node)>() + 1))
            .and_then(|bytes| bytes.checked_add(16))
            .ok_or(Error::InvalidReference)?;
        let memory = self.operation.reserve_state(bytes)?;
        self.operation
            .work(self.nodes.len() * size_of::<(ObjectId, Node)>())?;
        let mut table = HashMap::with_capacity(limit);
        table.extend(self.nodes.drain());
        self.nodes = table;
        self.table_memory = memory;
        self.table_limit = limit;
        Ok(())
    }

    fn grow_frontier(&mut self) -> Result<(), Error> {
        let capacity = self
            .frontier
            .capacity()
            .max(64)
            .checked_mul(2)
            .ok_or(Error::InvalidReference)?;
        let memory = self.operation.reserve_state(
            capacity
                .checked_mul(size_of::<ObjectId>())
                .ok_or(Error::InvalidReference)?,
        )?;
        self.operation
            .work(self.frontier.len() * size_of::<ObjectId>())?;
        let mut frontier = Vec::with_capacity(capacity);
        frontier.append(&mut self.frontier);
        self.frontier = frontier;
        self.frontier_memory = memory;
        Ok(())
    }
}

struct WalkerLinks<'a, 'store> {
    closure: &'a mut Closure,
    reader: &'a mut Reader<'store>,
    mark: u8,
    edges: Edges,
}
impl crate::structure::Links for WalkerLinks<'_, '_> {
    async fn link(&mut self, id: ObjectId, kind: Kind, verify: bool) -> Result<(), Error> {
        let follow = match self.edges {
            Edges::All => true,
            Edges::Commits => kind == Kind::Commit,
            Edges::Objects => kind != Kind::Commit,
        };
        if follow {
            self.closure
                .schedule(self.reader, id, Some(kind), verify, self.mark)
                .await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pack::budget::Pool;
    type TestResult = Result<(), Box<dyn std::error::Error>>;

    #[test]
    fn adaptive_growth_charges_old_and_new_tables_before_allocation() -> TestResult {
        let pool = Pool::new(24 * 1024 * 1024);
        let operation = pool.admit()?;
        let mut closure = Closure::new(&operation)?;
        assert_eq!(operation.live_bytes(), 0);
        closure.grow_table()?;
        let before = operation.live_bytes();
        let limit = closure.table_limit;
        let capacity = closure.nodes.capacity();
        let pressure = operation.reserve(24 * 1024 * 1024 - before - 1)?;
        assert!(closure.grow_table().is_err());
        assert_eq!(closure.table_limit, limit);
        assert_eq!(closure.nodes.capacity(), capacity);
        drop(pressure);
        assert_eq!(operation.live_bytes(), before);
        closure.grow_table()?;
        assert!(closure.table_limit > limit);
        drop(closure);
        assert_eq!(operation.live_bytes(), 0);
        Ok(())
    }
}
