use std::mem::size_of;

use gix_object::Kind;

use crate::{
    Error, ObjectId,
    graph::Graph,
    pack::budget::{Operation, Reservation},
};

pub(crate) struct Selection {
    pub(crate) ids: Vec<ObjectId>,
    pub(crate) common: Vec<ObjectId>,
    pub(crate) shallow: Vec<ObjectId>,
    pub(crate) unshallow: Vec<ObjectId>,
    _memory: Reservation,
}

pub(crate) fn select(
    operation: &Operation,
    graph: &Graph,
    wants: &[ObjectId],
    haves: &[ObjectId],
    include_tag: bool,
    shallow: &crate::wire::ShallowRequest<'_>,
    exclusions: &[ObjectId],
) -> Result<Selection, Error> {
    if wants.is_empty() || wants.len() > 1024 || haves.len() > 32768 {
        return Err(Error::InvalidReference);
    }
    let count = graph.nodes.len();
    let memory = operation.reserve_state(
        count * (3 * size_of::<ObjectId>() + 5 + 2 * size_of::<u32>()) + size_of_val(haves),
    )?;
    operation.work(
        (wants.len() + haves.len() + shallow.ids.len()) * size_of::<ObjectId>()
            + count * size_of::<u32>(),
    )?;
    let mut old = vec![false; count];
    for &id in &shallow.ids {
        // A stale client boundary may no longer exist in the current repository.
        if let Some(index) = graph.location(id) {
            if graph.nodes[index as usize].kind != Some(Kind::Commit) {
                return Err(Error::InvalidReference);
            }
            old[index as usize] = true;
        }
    }
    let boundary = if shallow.deepens() {
        boundaries(operation, graph, wants, shallow, exclusions, &old)?
    } else {
        old.clone()
    };
    let mut wanted = vec![false; count];
    let mut present = vec![false; count];
    let mut stack = Vec::with_capacity(count);
    for &id in wants {
        schedule(
            graph.location(id).ok_or(Error::InvalidReference)?,
            &mut wanted,
            &mut stack,
        );
    }
    // Every newly unshallowed commit needs its missing parents, even when a
    // new shallower cut on another merge parent hides it from wanted tips.
    for (index, is_old) in old.iter().enumerate() {
        if *is_old && !boundary[index] {
            schedule(
                u32::try_from(index).map_err(crate::pack::pack_error)?,
                &mut wanted,
                &mut stack,
            );
        }
    }
    closure(operation, graph, &mut wanted, &mut stack, &boundary)?;
    let mut common = Vec::with_capacity(haves.len());
    for &id in haves {
        if let Some(index) = graph.location(id) {
            common.push(id);
            schedule(index, &mut present, &mut stack);
        }
    }
    common.sort_unstable();
    common.dedup();
    closure(operation, graph, &mut present, &mut stack, &old)?;
    if include_tag {
        for (index, node) in graph.nodes.iter().enumerate() {
            if node.kind == Some(Kind::Tag) {
                let target = peel(operation, graph, index)?;
                if wanted[target] && !present[target] {
                    wanted[index] = true;
                }
            }
        }
    }
    let mut shallow_ids = Vec::with_capacity(count);
    let mut unshallow = Vec::with_capacity(count);
    for (index, node) in graph.nodes.iter().enumerate() {
        if wanted[index] {
            if boundary[index] && !old[index] {
                shallow_ids.push(node.id);
            }
            if old[index] && !boundary[index] {
                unshallow.push(node.id);
            }
        }
    }
    shallow_ids.sort_unstable();
    unshallow.sort_unstable();
    let mut ids = Vec::with_capacity(count);
    ids.extend(
        graph
            .nodes
            .iter()
            .enumerate()
            .filter_map(|(index, node)| (wanted[index] && !present[index]).then_some(node.id)),
    );
    ids.sort_unstable();
    Ok(Selection {
        ids,
        common,
        shallow: shallow_ids,
        unshallow,
        _memory: memory,
    })
}

fn schedule(index: u32, seen: &mut [bool], stack: &mut Vec<u32>) {
    if !seen[index as usize] {
        seen[index as usize] = true;
        stack.push(index);
    }
}

fn closure(
    operation: &Operation,
    graph: &Graph,
    seen: &mut [bool],
    stack: &mut Vec<u32>,
    boundary: &[bool],
) -> Result<(), Error> {
    while let Some(index) = stack.pop() {
        let edges = &graph.edges[graph.nodes[index as usize].edges.clone()];
        operation.work(size_of::<u32>() * (1 + edges.len()))?;
        for &child in edges {
            if boundary[index as usize] && graph.nodes[child as usize].kind == Some(Kind::Commit) {
                continue;
            }
            schedule(child, seen, stack);
        }
    }
    Ok(())
}

// Commit-only breadth-first traversal gives minimum distance over all merge
// parents and wanted tips. Trees and tags never consume a history depth.
fn boundaries(
    operation: &Operation,
    graph: &Graph,
    wants: &[ObjectId],
    request: &crate::wire::ShallowRequest<'_>,
    exclusions: &[ObjectId],
    old: &[bool],
) -> Result<Vec<bool>, Error> {
    let count = graph.nodes.len();
    let _memory = operation.reserve_state(count * (3 + 2 * size_of::<u32>()))?;
    let mut excluded = vec![false; count];
    let mut queue = Vec::with_capacity(count);
    for &id in exclusions {
        let index = graph.location(id).ok_or(Error::InvalidReference)?;
        let index = peel(operation, graph, index as usize)?;
        if graph.nodes[index].kind != Some(Kind::Commit) {
            return Err(Error::InvalidReference);
        }
        schedule(
            u32::try_from(index).map_err(crate::pack::pack_error)?,
            &mut excluded,
            &mut queue,
        );
    }
    // Exclusions are reachability predicates, not fetch have assertions.
    let no_boundaries = vec![false; count];
    closure(operation, graph, &mut excluded, &mut queue, &no_boundaries)?;
    let mut distance = vec![u32::MAX; count];
    let mut depth = request.depth.unwrap_or(i32::MAX as u32);
    for &id in wants {
        let index = peel(
            operation,
            graph,
            graph.location(id).ok_or(Error::InvalidReference)? as usize,
        )?;
        if graph.nodes[index].kind == Some(Kind::Commit) && distance[index] == u32::MAX {
            distance[index] = 1;
            queue.push(u32::try_from(index).map_err(crate::pack::pack_error)?);
        }
    }
    let mut cursor = 0;
    while cursor < queue.len() {
        let index = queue[cursor] as usize;
        cursor += 1;
        let node = &graph.nodes[index];
        let edges = &graph.edges[node.edges.clone()];
        operation.work(size_of::<u32>() * (1 + edges.len()))?;
        if excluded[index] || request.since.is_some_and(|since| node.commit_time < since) {
            continue;
        }
        if !request.relative && distance[index] >= depth {
            continue;
        }
        for &parent in edges {
            let parent = parent as usize;
            if graph.nodes[parent].kind == Some(Kind::Commit) && distance[parent] == u32::MAX {
                distance[parent] = distance[index] + 1;
                queue.push(u32::try_from(parent).map_err(crate::pack::pack_error)?);
            }
        }
    }
    if request.relative {
        let current = old
            .iter()
            .enumerate()
            .filter(|(index, old)| **old && distance[*index] != u32::MAX)
            .map(|(index, _)| distance[index])
            .min()
            .unwrap_or(0);
        depth = depth.saturating_add(current).min(i32::MAX as u32);
    }
    let included = |index: usize| {
        distance[index] != u32::MAX
            && distance[index] <= depth
            && !excluded[index]
            && request
                .since
                .is_none_or(|since| graph.nodes[index].commit_time >= since)
    };
    let mut result = old.to_vec();
    for (index, node) in graph.nodes.iter().enumerate() {
        if node.kind != Some(Kind::Commit) {
            continue;
        }
        operation.work(size_of::<u32>() * (1 + node.edges.len()))?;
        if included(index) {
            result[index] = if request.depth.is_some() {
                depth != i32::MAX as u32 && distance[index] >= depth
            } else {
                graph.edges[node.edges.clone()].iter().any(|parent| {
                    let parent = *parent as usize;
                    graph.nodes[parent].kind == Some(Kind::Commit) && !included(parent)
                })
            };
        }
    }
    if request.depth == Some(i32::MAX as u32) {
        result.fill(false);
    }
    if request.depth.is_none()
        && !(0..count).any(|index| graph.nodes[index].kind == Some(Kind::Commit) && included(index))
    {
        return Err(Error::InvalidProtocol(
            "no commits selected by shallow request",
        ));
    }
    Ok(result)
}

fn peel(operation: &Operation, graph: &Graph, mut index: usize) -> Result<usize, Error> {
    for _ in 0..graph.nodes.len() {
        operation.work(size_of::<u32>())?;
        let node = &graph.nodes[index];
        if node.kind != Some(Kind::Tag) {
            return Ok(index);
        }
        let [target] = graph.edges[node.edges.clone()] else {
            return Err(Error::InvalidObjectGraph("tag must have one target"));
        };
        index = target as usize;
    }
    Err(Error::InvalidObjectGraph("tag chain is cyclic"))
}
