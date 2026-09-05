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
    _memory: Reservation,
}

pub(crate) fn select(
    operation: &Operation,
    graph: &Graph,
    wants: &[ObjectId],
    haves: &[ObjectId],
    include_tag: bool,
) -> Result<Selection, Error> {
    if wants.is_empty() || wants.len() > 1024 || haves.len() > 32768 {
        return Err(Error::InvalidReference);
    }
    let count = graph.nodes.len();
    let memory = operation.reserve_state(
        count * (size_of::<ObjectId>() + 2 + size_of::<u32>()) + size_of_val(haves),
    )?;
    operation.work((wants.len() + haves.len()) * size_of::<ObjectId>())?;
    let mut wanted = vec![false; count];
    let mut present = vec![false; count];
    let mut stack = Vec::with_capacity(count);
    for &id in wants {
        let index = graph.location(id).ok_or(Error::InvalidReference)?;
        schedule(index, &mut wanted, &mut stack);
    }
    closure(operation, graph, &mut wanted, &mut stack)?;
    let mut common = Vec::with_capacity(haves.len());
    for &id in haves {
        if let Some(index) = graph.location(id) {
            common.push(id);
            schedule(index, &mut present, &mut stack);
        }
    }
    common.sort_unstable();
    common.dedup();
    closure(operation, graph, &mut present, &mut stack)?;
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
) -> Result<(), Error> {
    while let Some(index) = stack.pop() {
        let edges = &graph.edges[graph.nodes[index as usize].edges.clone()];
        operation.work(size_of::<u32>() * (1 + edges.len()))?;
        for &child in edges {
            schedule(child, seen, stack);
        }
    }
    Ok(())
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
