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

struct Node {
    prefix: Bytes,
    value: Option<Bytes>,
    edges: Vec<u8>,
    children: Vec<StagedObject>,
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
    async fn read(&mut self, object: &StagedObject) -> Result<Node, KvError> {
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
        let (payload, children) = self.log.read_staged_node(self.view, object).await?;
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
            children,
        })
    }

    async fn save(&mut self, mut node: Node) -> Result<Option<StagedObject>, KvError> {
        if node.value.is_none() {
            match node.children.len() {
                0 => return Ok(None),
                1 => {
                    let child = self.read(&node.children[0]).await?;
                    let mut prefix = node.prefix.to_vec();
                    prefix.push(node.edges[0]);
                    prefix.extend_from_slice(&child.prefix);
                    node = Node {
                        prefix: Bytes::from(prefix),
                        ..child
                    };
                }
                _ => {}
            }
        }
        ensure(node.prefix.len() <= self.limits.key_bytes, "key bytes")?;
        ensure(
            node.value.as_ref().map_or(0, Bytes::len) <= self.limits.value_bytes,
            "value bytes",
        )?;
        let wire = Wire {
            prefix: &node.prefix,
            value: node.value.as_deref(),
            edges: &node.edges,
        };
        let payload_len = minicbor::len(&wire);
        let len = self.log.node_size(
            payload_len,
            node.children.iter().map(StagedObject::reference),
        )?;
        self.budget.charge(len)?;
        Ok(Some(
            self.log
                .put_node(self.view, encode(&wire)?, node.children)
                .await?,
        ))
    }

    pub async fn get(
        &mut self,
        mut root: Option<StagedObject>,
        mut key: &[u8],
    ) -> Result<Option<Bytes>, KvError> {
        while let Some(object) = root {
            let node = self.read(&object).await?;
            let Some(suffix) = key.strip_prefix(node.prefix.as_ref()) else {
                return Ok(None);
            };
            if suffix.is_empty() {
                return Ok(node.value);
            }
            let Ok(index) = node.edges.binary_search(&suffix[0]) else {
                return Ok(None);
            };
            root = Some(node.children[index].clone());
            key = &suffix[1..];
        }
        Ok(None)
    }

    pub async fn apply(
        &mut self,
        mut root: Option<StagedObject>,
        command: &KvCommand,
    ) -> Result<(Option<StagedObject>, KvResult), KvError> {
        let original = root.clone();
        let mut key = command.key().as_ref();
        let mut parents = Vec::new();
        // Keep the decoded path through evaluation so a mutation reads it once.
        let (node, common) = loop {
            let Some(object) = root else {
                break (None, 0);
            };
            let node = self.read(&object).await?;
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
            root = Some(node.children[index].clone());
            parents.push((node, index));
            key = &key[common + 1..];
        };
        let previous = node
            .as_ref()
            .filter(|node| common == node.prefix.len() && common == key.len())
            .and_then(|node| node.value.clone());
        let (value, result) = command.evaluate(previous.clone())?;
        if value == previous {
            return Ok((original, result));
        }
        let mut replacement = self.replace(node, key, common, value).await?;
        while let Some((mut node, index)) = parents.pop() {
            if let Some(object) = replacement {
                node.children[index] = object;
            } else {
                node.children.remove(index);
                node.edges.remove(index);
            }
            replacement = self.save(node).await?;
        }
        Ok((replacement, result))
    }

    async fn replace(
        &mut self,
        node: Option<Node>,
        key: &[u8],
        common: usize,
        value: Option<Bytes>,
    ) -> Result<Option<StagedObject>, KvError> {
        let Some(mut node) = node else {
            return self
                .save(Node {
                    prefix: Bytes::copy_from_slice(key),
                    value,
                    edges: Vec::new(),
                    children: Vec::new(),
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
            };
            node.prefix = Bytes::copy_from_slice(&node.prefix[common + 1..]);
            let old = self.save(node).await?.ok_or(KvError::InvalidEncoding)?;
            parent.children.push(old);
            if common == key.len() {
                parent.value = value;
            } else {
                let leaf = self
                    .save(Node {
                        prefix: Bytes::copy_from_slice(&key[common + 1..]),
                        value,
                        edges: Vec::new(),
                        children: Vec::new(),
                    })
                    .await?
                    .ok_or(KvError::InvalidEncoding)?;
                let at = usize::from(parent.edges[0] < key[common]);
                parent.edges.insert(at, key[common]);
                parent.children.insert(at, leaf);
            }
            return self.save(parent).await;
        }
        if common == key.len() {
            node.value = value;
        } else {
            let leaf = self
                .save(Node {
                    prefix: Bytes::copy_from_slice(&key[common + 1..]),
                    value,
                    edges: Vec::new(),
                    children: Vec::new(),
                })
                .await?
                .ok_or(KvError::InvalidEncoding)?;
            let index = node.edges.partition_point(|edge| *edge < key[common]);
            node.edges.insert(index, key[common]);
            node.children.insert(index, leaf);
        }
        self.save(node).await
    }

    pub async fn scan(
        &mut self,
        root: Option<StagedObject>,
        start: &[u8],
        end: Option<&[u8]>,
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<KvPage, KvError> {
        let mut stack: Vec<_> = root.into_iter().map(|root| (Vec::new(), root)).collect();
        let mut entries: Vec<(Bytes, Bytes)> = Vec::new();
        let mut remaining = self.limits.response_bytes;
        while let Some((mut key, object)) = stack.pop() {
            if outside(&key, start, end, after) {
                continue;
            }
            let node = self.read(&object).await?;
            ensure(
                key.len().saturating_add(node.prefix.len()) <= self.limits.key_bytes,
                "key bytes",
            )?;
            key.extend_from_slice(&node.prefix);
            if outside(&key, start, end, after) {
                continue;
            }
            if let Some(value) = node.value
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
            for (edge, child) in node.edges.into_iter().zip(node.children).rev() {
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
