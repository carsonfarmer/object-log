//! Ordinary fetch selection over charged closure marks instead of stored edges.
use super::{Error, ObjectId, Repository, durable, peel_ref, upload_stream, wire, wire_response};
use crate::{
    closure::{Closure, Edges, KNOWN, PRESENT, PUBLISHED, REQUESTED, WANTED},
    selection::Selection,
};
use gix_object::Kind;

impl Repository {
    pub(super) async fn prepare_closure_fetch(
        &self,
        wants: &[ObjectId],
        haves: &[ObjectId],
        include_tag: bool,
        done: bool,
        raw_pack: bool,
    ) -> Result<upload_stream::FetchPlan, Error> {
        let catalog = self.catalog().await?;
        let mut reader = durable::Reader::new(&self.log, &self.view, &catalog);
        let mut selected = self
            .closure_selection(&mut reader, wants, haves, include_tag)
            .await?;
        if !done {
            return wire_response(&self.operation, |output| {
                wire::write_fetch(
                    output,
                    self.format,
                    wire::FetchReply::Acknowledgments(&selected.common),
                )
            })
            .map(upload_stream::FetchPlan::Bytes);
        }
        // Only the selected IDs/common remain charged here; all graph marks and
        // frontier state have been released after verification, before output.
        let prefix = self.fetch_prefix(raw_pack, &selected, &[])?;
        selected.common.clear();
        drop(reader);
        let catalog_memory = self.operation.reserve(size_of::<durable::Catalog>())?;
        Ok(upload_stream::FetchPlan::Pack {
            catalog: Box::new(catalog),
            _catalog_memory: catalog_memory,
            selected,
            prefix,
        })
    }

    async fn closure_selection(
        &self,
        reader: &mut durable::Reader<'_>,
        wants: &[ObjectId],
        haves: &[ObjectId],
        include_tag: bool,
    ) -> Result<Selection, Error> {
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
        let mut closure = Closure::new(&self.operation)?;
        if wants.iter().any(|id| roots.binary_search(id).is_err()) {
            closure.walk(reader, &roots, PUBLISHED, Edges::All).await?;
            if wants.iter().any(|id| !closure.marked(*id, PUBLISHED)) {
                return Err(Error::InvalidReference);
            }
        }
        closure.walk(reader, wants, WANTED, Edges::All).await?;
        for &id in haves {
            if !closure.marked(id, WANTED)
                && !closure.marked(id, PUBLISHED)
                && reader.contains(id).await?
            {
                // Catalog presence alone never authorizes a have. Resolve all
                // candidates against exact published reachability once.
                closure.walk(reader, &roots, PUBLISHED, Edges::All).await?;
                break;
            }
        }
        for (name, &id) in &self.state.refs {
            if name.starts_with(b"refs/heads/")
                && closure.kind(id).is_some_and(|kind| kind != Kind::Commit)
            {
                return Err(Error::InvalidReference);
            }
        }
        let mut memory = self.operation.reserve_state(size_of_val(haves))?;
        let mut common = Vec::with_capacity(haves.len());
        common.extend(
            haves
                .iter()
                .copied()
                .filter(|id| closure.marked(*id, WANTED) || closure.marked(*id, PUBLISHED)),
        );
        common.sort_unstable();
        common.dedup();
        mark_have_ownership(&mut closure, reader, wants, &common).await?;
        if include_tag {
            self.include_closure_tags(reader, &mut closure).await?;
        }
        self.operation
            .work(closure.nodes.len() * size_of::<ObjectId>())?;
        let count = closure
            .nodes
            .keys()
            .filter(|id| emitted(&closure, **id))
            .count();
        memory.grow(
            count
                .checked_mul(size_of::<ObjectId>())
                .ok_or(Error::InvalidReference)?,
        )?;
        let mut ids = Vec::with_capacity(count);
        ids.extend(
            closure
                .nodes
                .keys()
                .copied()
                .filter(|id| emitted(&closure, *id)),
        );
        self.operation
            .work(ids.len() * (ids.len().max(1).ilog2() as usize + 1) * size_of::<ObjectId>())?;
        ids.sort_unstable();
        for &id in &ids {
            let node = closure.nodes.get(&id).ok_or(Error::InvalidReference)?;
            if !node.verified && reader.verify(id).await? != node.kind {
                return crate::pack::invalid("selected graph object has the wrong kind");
            }
        }
        Ok(Selection {
            ids,
            common,
            shallow: Vec::new(),
            unshallow: Vec::new(),
            _memory: memory,
        })
    }
    async fn include_closure_tags(
        &self,
        reader: &mut durable::Reader<'_>,
        closure: &mut Closure,
    ) -> Result<(), Error> {
        for (name, &id) in &self.state.refs {
            if name.starts_with(b"refs/heads/") || closure.marked(id, WANTED) {
                continue;
            }
            let included = |target| {
                emitted(closure, target)
                    .then(|| closure.kind(target))
                    .flatten()
            };
            if peel_ref(&self.operation, reader, id, Some(&included))
                .await?
                .is_some()
            {
                closure.walk(reader, &[id], WANTED, Edges::All).await?;
            }
        }
        Ok(())
    }
}

fn emitted(closure: &Closure, id: ObjectId) -> bool {
    let present = (closure.marked(id, PRESENT) && !closure.marked(id, REQUESTED))
        || closure.marked(id, KNOWN);
    closure.marked(id, WANTED) && !present
}

async fn mark_have_ownership(
    closure: &mut Closure,
    reader: &mut durable::Reader<'_>,
    wants: &[ObjectId],
    common: &[ObjectId],
) -> Result<(), Error> {
    closure.walk(reader, common, PRESENT, Edges::All).await?;
    // Mirror selection::mark_present: commit haves do not prove explicit
    // noncommit wants, while noncommit haves prove their complete closures.
    for &id in wants {
        if closure.kind(id) != Some(Kind::Commit) {
            closure
                .walk(reader, &[id], REQUESTED, Edges::Objects)
                .await?;
        }
    }
    for &id in common {
        if closure.kind(id) != Some(Kind::Commit) {
            closure.walk(reader, &[id], KNOWN, Edges::All).await?;
        }
    }
    Ok(())
}
