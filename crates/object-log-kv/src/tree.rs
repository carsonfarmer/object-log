use bytes::Bytes;
use minicbor::{CborLen, Decode, Encode};
use object_log::{Log, StagedObject, View};

use crate::{Budget, KvCommand, KvError, KvPage, KvResult, Limits, decode, encode, ensure};

pub(crate) struct Tree<'a> {
    pub log: &'a Log,
    pub view: &'a View,
    pub limits: Limits,
    pub budget: Budget,
}

pub(super) enum Link {
    Stored(StagedObject),
    Dirty(Box<Node>),
}

pub(super) struct Node {
    prefix: Bytes,
    value: Option<Bytes>,
    edges: Vec<u8>,
    children: Vec<Link>,
    source: Option<StagedObject>,
}

impl Node {
    fn into_link(mut self) -> Link {
        self.source
            .take()
            .map_or_else(|| Link::Dirty(Box::new(self)), Link::Stored)
    }

    fn transient_bytes(&self) -> usize {
        size_of::<Self>()
            .saturating_add(self.prefix.len())
            .saturating_add(self.value.as_ref().map_or(0, Bytes::len))
            .saturating_add(self.edges.capacity())
            .saturating_add(self.children.capacity().saturating_mul(size_of::<Link>()))
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        let mut pending = std::mem::take(&mut self.children);
        while let Some(mut link) = pending.pop() {
            if let Link::Dirty(node) = &mut link {
                pending.append(&mut node.children);
            }
        }
    }
}

struct FlushFrame {
    node: Node,
    children: std::vec::IntoIter<Link>,
    staged: Vec<StagedObject>,
}

#[derive(CborLen, Decode, Encode)]
#[cbor(array)]
struct Wire<'a> {
    #[cbor(n(0), with = "minicbor::bytes")]
    prefix: &'a [u8],
    #[cbor(n(1), with = "minicbor::bytes")]
    value: Option<&'a [u8]>,
    #[cbor(n(2), with = "minicbor::bytes")]
    edges: &'a [u8],
}

impl Tree<'_> {
    async fn read(&mut self, link: Link) -> Result<Node, KvError> {
        let object = match link {
            Link::Dirty(node) => {
                self.budget.charge(node.transient_bytes())?;
                return Ok(*node);
            }
            Link::Stored(object) => object,
        };
        let len =
            usize::try_from(object.reference().len()).map_err(|_| KvError::Limit("node bytes"))?;
        // Bound the core decoder before allocating child references or payloads.
        let max = self
            .limits
            .key_bytes
            .saturating_add(self.limits.value_bytes)
            .saturating_add(32 * 1024);
        ensure(len <= max, "node bytes")?;
        self.budget.charge(len)?;
        let (payload, children) = self.log.read_staged_node(self.view, &object).await?;
        let wire: Wire<'_> = decode(&payload)?;
        if wire.edges.len() != children.len()
            || wire.edges.windows(2).any(|edges| edges[0] >= edges[1])
            || (wire.value.is_none() && children.len() < 2)
        {
            return Err(KvError::InvalidEncoding);
        }
        ensure(wire.prefix.len() <= self.limits.key_bytes, "key bytes")?;
        ensure(
            wire.value.map_or(0, <[u8]>::len) <= self.limits.value_bytes,
            "value bytes",
        )?;
        Ok(Node {
            prefix: Bytes::copy_from_slice(wire.prefix),
            value: wire.value.map(Bytes::copy_from_slice),
            edges: wire.edges.to_vec(),
            children: children.into_iter().map(Link::Stored).collect(),
            source: Some(object),
        })
    }

    async fn dirty(&mut self, mut node: Node) -> Result<Option<Link>, KvError> {
        node.source = None;
        if node.value.is_none() {
            match node.children.len() {
                0 => return Ok(None),
                1 => {
                    let child = node.children.pop().ok_or(KvError::InvalidEncoding)?;
                    let mut child = self.read(child).await?;
                    let mut prefix = node.prefix.to_vec();
                    prefix.push(node.edges[0]);
                    prefix.extend_from_slice(&child.prefix);
                    child.prefix = Bytes::from(prefix);
                    child.source = None;
                    node = child;
                }
                _ => {}
            }
        }
        ensure(node.prefix.len() <= self.limits.key_bytes, "key bytes")?;
        ensure(
            node.value.as_ref().map_or(0, Bytes::len) <= self.limits.value_bytes,
            "value bytes",
        )?;
        self.budget.charge(node.transient_bytes())?;
        Ok(Some(Link::Dirty(Box::new(node))))
    }

    async fn stage(
        &mut self,
        node: Node,
        children: Vec<StagedObject>,
    ) -> Result<StagedObject, KvError> {
        ensure(node.edges.len() == children.len(), "node children")?;
        let wire = Wire {
            prefix: &node.prefix,
            value: node.value.as_deref(),
            edges: &node.edges,
        };
        let payload_len = minicbor::len(&wire);
        let len = self
            .log
            .node_size(payload_len, children.iter().map(StagedObject::reference))?;
        self.budget.charge(len)?;
        Ok(self
            .log
            .put_node(self.view, encode(&wire)?, children)
            .await?)
    }

    pub async fn finish(&mut self, root: Option<Link>) -> Result<Option<StagedObject>, KvError> {
        let Some(mut current) = root else {
            return Ok(None);
        };
        let mut parents = Vec::<FlushFrame>::new();
        loop {
            let mut staged = match current {
                Link::Stored(object) => object,
                Link::Dirty(node) => {
                    let mut node = *node;
                    let mut children = std::mem::take(&mut node.children).into_iter();
                    if let Some(first) = children.next() {
                        let capacity = children.len() + 1;
                        parents.push(FlushFrame {
                            node,
                            children,
                            staged: Vec::with_capacity(capacity),
                        });
                        current = first;
                        continue;
                    }
                    self.stage(node, Vec::new()).await?
                }
            };
            loop {
                let Some(parent) = parents.last_mut() else {
                    return Ok(Some(staged));
                };
                parent.staged.push(staged);
                if let Some(next) = parent.children.next() {
                    current = next;
                    break;
                }
                let parent = parents.pop().ok_or(KvError::InvalidEncoding)?;
                staged = self.stage(parent.node, parent.staged).await?;
            }
        }
    }

    pub async fn get(
        &mut self,
        root: Option<StagedObject>,
        mut key: &[u8],
    ) -> Result<Option<Bytes>, KvError> {
        let mut root = root.map(Link::Stored);
        while let Some(link) = root {
            let mut node = self.read(link).await?;
            let Some(suffix) = key.strip_prefix(node.prefix.as_ref()) else {
                return Ok(None);
            };
            if suffix.is_empty() {
                return Ok(node.value.take());
            }
            let Ok(index) = node.edges.binary_search(&suffix[0]) else {
                return Ok(None);
            };
            root = Some(node.children.remove(index));
            key = &suffix[1..];
        }
        Ok(None)
    }

    pub async fn apply(
        &mut self,
        mut root: Option<Link>,
        command: &KvCommand,
    ) -> Result<(Option<Link>, KvResult), KvError> {
        let mut key = command.key().as_ref();
        let mut parents = Vec::new();
        // Keep the decoded path through evaluation so a mutation reads it once.
        let (node, common) = loop {
            let Some(link) = root else {
                break (None, 0);
            };
            let mut node = self.read(link).await?;
            let common = key
                .iter()
                .zip(&node.prefix)
                .take_while(|(left, right)| left == right)
                .count();
            if common < node.prefix.len() || common == key.len() {
                break (Some(node), common);
            }
            let Ok(index) = node.edges.binary_search(&key[common]) else {
                break (Some(node), common);
            };
            root = Some(node.children.remove(index));
            parents.push((node, index));
            key = &key[common + 1..];
        };
        let previous = node
            .as_ref()
            .filter(|node| common == node.prefix.len() && common == key.len())
            .and_then(|node| node.value.clone());
        let (value, result) = command.evaluate(previous.clone())?;
        let changed = value != previous;
        let mut replacement = if changed {
            self.replace(node, key, common, value).await?
        } else {
            node.map(Node::into_link)
        };
        while let Some((mut node, index)) = parents.pop() {
            if let Some(link) = replacement {
                node.children.insert(index, link);
            } else {
                node.edges.remove(index);
            }
            replacement = if changed {
                self.dirty(node).await?
            } else {
                Some(node.into_link())
            };
        }
        Ok((replacement, result))
    }

    async fn replace(
        &mut self,
        node: Option<Node>,
        key: &[u8],
        common: usize,
        value: Option<Bytes>,
    ) -> Result<Option<Link>, KvError> {
        let Some(mut node) = node else {
            return self
                .dirty(Node {
                    prefix: Bytes::copy_from_slice(key),
                    value,
                    edges: Vec::new(),
                    children: Vec::new(),
                    source: None,
                })
                .await;
        };
        if common < node.prefix.len() {
            // Only insertion reaches a divergent prefix; absent no-ops return above.
            let mut parent = Node {
                prefix: Bytes::copy_from_slice(&key[..common]),
                value: None,
                edges: vec![node.prefix[common]],
                children: Vec::new(),
                source: None,
            };
            node.prefix = Bytes::copy_from_slice(&node.prefix[common + 1..]);
            let old = self.dirty(node).await?.ok_or(KvError::InvalidEncoding)?;
            parent.children.push(old);
            if common == key.len() {
                parent.value = value;
            } else {
                let leaf = self
                    .dirty(Node {
                        prefix: Bytes::copy_from_slice(&key[common + 1..]),
                        value,
                        edges: Vec::new(),
                        children: Vec::new(),
                        source: None,
                    })
                    .await?
                    .ok_or(KvError::InvalidEncoding)?;
                let at = usize::from(parent.edges[0] < key[common]);
                parent.edges.insert(at, key[common]);
                parent.children.insert(at, leaf);
            }
            return self.dirty(parent).await;
        }
        if common == key.len() {
            node.value = value;
        } else {
            let leaf = self
                .dirty(Node {
                    prefix: Bytes::copy_from_slice(&key[common + 1..]),
                    value,
                    edges: Vec::new(),
                    children: Vec::new(),
                    source: None,
                })
                .await?
                .ok_or(KvError::InvalidEncoding)?;
            let index = node.edges.partition_point(|edge| *edge < key[common]);
            node.edges.insert(index, key[common]);
            node.children.insert(index, leaf);
        }
        self.dirty(node).await
    }

    pub async fn scan(
        &mut self,
        root: Option<StagedObject>,
        start: &[u8],
        end: Option<&[u8]>,
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<KvPage, KvError> {
        let mut stack: Vec<_> = root
            .into_iter()
            .map(|root| (Vec::new(), Link::Stored(root)))
            .collect();
        let mut entries: Vec<(Bytes, Bytes)> = Vec::new();
        let mut remaining = self.limits.response_bytes;
        while let Some((mut key, object)) = stack.pop() {
            if outside(&key, start, end, after) {
                continue;
            }
            let mut node = self.read(object).await?;
            ensure(
                key.len().saturating_add(node.prefix.len()) <= self.limits.key_bytes,
                "key bytes",
            )?;
            key.extend_from_slice(&node.prefix);
            if outside(&key, start, end, after) {
                continue;
            }
            if let Some(value) = node.value.take()
                && key.as_slice() >= start
                && after.is_none_or(|after| key.as_slice() > after)
            {
                let size = key
                    .len()
                    .checked_add(value.len())
                    .ok_or(KvError::Limit("response bytes"))?;
                if size > remaining {
                    ensure(!entries.is_empty(), "response bytes")?;
                    return Ok(KvPage {
                        after: entries.last().map(|(key, _)| key.clone()),
                        entries,
                    });
                }
                remaining -= size;
                entries.push((Bytes::copy_from_slice(&key), value));
                if entries.len() == limit {
                    return Ok(KvPage {
                        after: entries.last().map(|(key, _)| key.clone()),
                        entries,
                    });
                }
            }
            for (edge, child) in std::mem::take(&mut node.edges)
                .into_iter()
                .zip(std::mem::take(&mut node.children))
                .rev()
            {
                self.budget.charge(key.len().saturating_add(1))?;
                let mut prefix = key.clone();
                prefix.push(edge);
                if !outside(&prefix, start, end, after) {
                    stack.push((prefix, child));
                }
            }
        }
        Ok(KvPage {
            entries,
            after: None,
        })
    }
}

pub(crate) fn prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let last = prefix.iter().rposition(|byte| *byte != u8::MAX)?;
    let mut end = prefix[..=last].to_vec();
    end[last] += 1;
    Some(end)
}

fn outside(prefix: &[u8], start: &[u8], end: Option<&[u8]>, after: Option<&[u8]>) -> bool {
    end.is_some_and(|end| prefix >= end)
        || (prefix < start && !start.starts_with(prefix))
        || after.is_some_and(|after| prefix < after && !after.starts_with(prefix))
}

#[cfg(test)]
mod tests {
    use std::{error::Error, sync::Arc, thread};

    use bytes::Bytes;
    use object_log::{LogId, Options, ValidatedBackend, sim::FaultStore};
    use object_store::{memory::InMemory, path::Path};

    use super::{Link, Node, Tree, Wire};
    use crate::{Budget, KvError, Limits};

    #[test]
    fn deep_dirty_tree_drops_on_a_small_stack() -> Result<(), Box<dyn Error>> {
        let handle = thread::Builder::new().stack_size(64 * 1024).spawn(|| {
            let mut link = Link::Dirty(Box::new(Node {
                prefix: Bytes::new(),
                value: Some(Bytes::new()),
                edges: Vec::new(),
                children: Vec::new(),
                source: None,
            }));
            for _ in 0..10_000 {
                link = Link::Dirty(Box::new(Node {
                    prefix: Bytes::new(),
                    value: None,
                    edges: vec![0],
                    children: vec![link],
                    source: None,
                }));
            }
            drop(link);
        })?;
        handle
            .join()
            .map_err(|_| std::io::Error::other("drop thread panicked"))?;
        Ok(())
    }

    #[tokio::test]
    async fn final_encoded_node_counts_against_tree_budget() -> Result<(), Box<dyn Error>> {
        let faults = FaultStore::new(Arc::new(InMemory::new()));
        let backend =
            ValidatedBackend::new(Arc::new(faults.clone()), Path::from("kv-budget")).await?;
        let log = object_log::Log::open(&backend, &LogId::new("kv")?, Options::default()).await?;
        let view = log.load().await?;
        let value = Bytes::from_static(b"value");
        let wire = Wire {
            prefix: b"key",
            value: Some(&value),
            edges: &[],
        };
        let encoded = log.node_size(
            minicbor::len(&wire),
            std::iter::empty::<&object_log::ObjectRef>(),
        )?;
        let leaf = || Node {
            prefix: Bytes::from_static(b"key"),
            value: Some(value.clone()),
            edges: Vec::new(),
            children: Vec::new(),
            source: None,
        };
        let transient = leaf().transient_bytes();
        let mut tree = Tree {
            log: &log,
            view: &view,
            limits: Limits::default(),
            budget: Budget(transient + encoded - 1),
        };
        let root = tree.dirty(leaf()).await?;
        faults.reset();
        assert!(matches!(tree.finish(root).await, Err(KvError::Limit(_))));
        assert_eq!(faults.metrics().uploaded_bytes(), 0);

        let mut tree = Tree {
            log: &log,
            view: &view,
            limits: Limits::default(),
            budget: Budget(transient + encoded),
        };
        let root = tree.dirty(leaf()).await?;
        assert!(tree.finish(root).await?.is_some());
        assert!(faults.metrics().uploaded_bytes() > 0);
        Ok(())
    }
}
