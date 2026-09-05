use super::{Error, ObjectId, Repository, durable, peel_ref, size_of, wire};
use crate::graph::Graph;

impl Repository {
    pub(super) async fn fetch_graph(
        &self,
        reader: &mut durable::Reader<'_>,
        wants: &[ObjectId],
        haves: &[ObjectId],
        include_tag: bool,
        shallow: &wire::ShallowRequest<'_>,
    ) -> Result<Graph, Error> {
        let _roots_memory = self
            .operation
            .reserve_state(self.state.refs.len() * size_of::<ObjectId>())?;
        let mut roots = self.state.refs.values().copied().collect::<Vec<_>>();
        self.operation.work(
            (roots.len() + wants.len())
                * (roots.len().max(1).ilog2() as usize + 1)
                * size_of::<ObjectId>(),
        )?;
        roots.sort_unstable();
        // Exact published tips already establish authorization. Other wants
        // still require the complete reachable graph. Existing shallow cuts
        // and exclusions can involve history outside the wanted closures.
        let mut selective = shallow.ids.is_empty()
            && shallow.exclude.is_empty()
            && wants.iter().all(|id| roots.binary_search(id).is_ok());
        let mut graph = crate::graph::Graph::load(
            &self.operation,
            reader,
            if selective { wants } else { &roots },
        )
        .await?;
        // A stored have outside the wanted closure may be a descendant or
        // share ancestors with it. Preserve the exact common/pack contract by
        // proving its published reachability through the complete graph.
        // Absent IDs cannot prove ownership and are safely ignored.
        if selective {
            for &id in haves {
                if graph.location(id).is_none() && reader.contains(id).await? {
                    selective = false;
                    break;
                }
            }
            if !selective {
                drop(graph);
                graph = Graph::load(&self.operation, reader, &roots).await?;
            }
        }
        if selective && include_tag {
            roots.clear();
            for (name, &id) in &self.state.refs {
                if !name.starts_with(b"refs/heads/")
                    && !graph.location(id).is_some_and(|index| {
                        graph.nodes[index as usize].kind == Some(gix_object::Kind::Tag)
                    })
                    && peel_ref(&self.operation, reader, id, Some(&graph))
                        .await?
                        .is_some_and(|target| graph.location(target).is_some())
                {
                    roots.push(id);
                }
            }
            // Only the tag chains are new: their peeled targets and closures
            // already belong to the authorized wanted graph.
            graph.extend(reader, &roots).await?;
        }
        for (name, id) in &self.state.refs {
            if name.starts_with(b"refs/heads/")
                && graph.location(*id).is_some_and(|index| {
                    graph.nodes[index as usize].kind != Some(gix_object::Kind::Commit)
                })
            {
                return Err(Error::InvalidReference);
            }
        }
        Ok(graph)
    }
}
