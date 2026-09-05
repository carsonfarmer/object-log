use std::{
    collections::{HashMap, HashSet},
    mem::size_of,
    ops::Range,
};

use gix_object::{Kind, commit::ref_iter::Token, tree::EntryKind};

use crate::{
    Error, ObjectId,
    durable::Reader,
    pack::{
        MAX_OBJECTS,
        budget::{Operation, Reservation},
        invalid, object_hash, pack_error,
    },
};

const MAX_GRAPH_BYTES: usize = 24 * 1024 * 1024;
const OBJECTS: usize = MAX_OBJECTS as usize;
// HashMap's backing table has at most two buckets per requested entry, plus
// its trailing control group. Nodes and the work queue never grow.
const FIXED_BYTES: usize = OBJECTS * (size_of::<Node>() + size_of::<u32>())
    + 2 * OBJECTS * (size_of::<(ObjectId, u32)>() + 1)
    + 16;
const MAX_EDGES: usize = (MAX_GRAPH_BYTES - FIXED_BYTES) / size_of::<u32>();

pub(crate) struct Graph {
    pub(crate) nodes: Vec<Node>,
    pub(crate) edges: Vec<u32>,
    locations: HashMap<ObjectId, u32>,
    queue: Vec<u32>,
    operation: Operation,
    _memory: Reservation,
    edge_memory: Reservation,
}

pub(crate) struct Node {
    pub(crate) id: ObjectId,
    // Tree leaves carry a declared kind until selected content is verified.
    pub(crate) kind: Option<Kind>,
    // True only after Reader authenticates the content and we parse metadata.
    pub(crate) verified: bool,
    pub(crate) commit_time: i64,
    pub(crate) edges: Range<usize>,
    queued: bool,
}

impl Graph {
    pub(crate) async fn load(
        operation: &Operation,
        reader: &mut Reader<'_>,
        roots: &[ObjectId],
    ) -> Result<Self, Error> {
        let memory = operation.reserve_state(FIXED_BYTES)?;
        let mut graph = Self {
            nodes: Vec::with_capacity(OBJECTS),
            edges: Vec::new(),
            locations: HashMap::with_capacity(OBJECTS),
            queue: Vec::with_capacity(OBJECTS),
            operation: operation.clone(),
            _memory: memory,
            edge_memory: operation.reserve_state(0)?,
        };
        for &id in roots {
            graph.schedule(reader, id, None, true).await?;
        }
        let mut cursor = 0;
        while cursor < graph.queue.len() {
            let index = graph.queue[cursor] as usize;
            cursor += 1;
            let id = graph.nodes[index].id;
            // Direct refs have no declared kind. Verify them without collecting
            // their bodies: a blob has no graph edges, however large it is.
            if graph.nodes[index].kind.is_none() || graph.nodes[index].kind == Some(Kind::Blob) {
                let kind = reader.verify(id).await?.ok_or(Error::InvalidReference)?;
                graph.expect_kind(index, kind)?;
                if kind == Kind::Blob {
                    graph.nodes[index].verified = true;
                    continue;
                }
            }
            let object = reader
                .find(id)
                .await?
                .ok_or_else(|| Error::InvalidPack("graph object is missing".into()))?;
            graph.expect_kind(index, object.kind)?;
            graph.nodes[index].verified = true;
            graph.operation.work(object.data.len())?;
            // Commit iterators may allocate a multiline extra header. Reserve
            // its input bound before parsing, including Vec growth headroom.
            // Tree name sets borrow the input and fit twice its bytes plus
            // the minimum hash-table allocation, even for SHA-1 entries.
            let _scratch = if object.kind == Kind::Tree {
                graph.operation.reserve_state(object.data.len() * 2 + 128)?
            } else {
                graph.operation.reserve(if object.kind == Kind::Commit {
                    object.data.len() * 2
                } else {
                    0
                })?
            };
            let start = graph.edges.len();
            let hash = object_hash(id.format());
            match object.kind {
                Kind::Commit => {
                    let mut complete = false;
                    for token in gix_object::CommitRefIter::from_bytes(&object.data, hash) {
                        match token.map_err(pack_error)? {
                            Token::Tree { id: target } => {
                                graph
                                    .link(reader, id, target.as_slice(), Kind::Tree, true)
                                    .await?;
                            }
                            Token::Parent { id: target } => {
                                graph
                                    .link(reader, id, target.as_slice(), Kind::Commit, true)
                                    .await?;
                            }
                            Token::Committer { signature } => {
                                graph.nodes[index].commit_time =
                                    signature.time().map_err(pack_error)?.seconds;
                            }
                            Token::Message(_) => complete = true,
                            _ => {}
                        }
                    }
                    if !complete {
                        return invalid("commit headers are incomplete");
                    }
                }
                Kind::Tree => graph.tree(reader, id, &object.data).await?,
                Kind::Tag => {
                    let tag =
                        gix_object::TagRef::from_bytes(&object.data, hash).map_err(pack_error)?;
                    graph
                        .link(
                            reader,
                            id,
                            tag.target().as_slice(),
                            tag.target_kind,
                            tag.target_kind != Kind::Blob,
                        )
                        .await?;
                }
                Kind::Blob => {}
            }
            graph.nodes[index].edges = start..graph.edges.len();
        }
        Ok(graph)
    }

    async fn tree(
        &mut self,
        reader: &mut Reader<'_>,
        id: ObjectId,
        data: &[u8],
    ) -> Result<(), Error> {
        let mut previous = None;
        let mut names = HashSet::with_capacity(data.len() / (id.format().digest_len() + 8));
        for entry in gix_object::TreeRefIter::from_bytes(data, object_hash(id.format())) {
            let entry = entry.map_err(pack_error)?;
            if !matches!(
                entry.mode.value(),
                0o040_000 | 0o100_644 | 0o100_755 | 0o120_000 | 0o160_000
            ) {
                return invalid("tree entry mode is invalid");
            }
            gix_validate::path::component(
                entry.filename,
                (entry.mode.kind() == EntryKind::Link)
                    .then_some(gix_validate::path::component::Mode::Symlink),
                gix_validate::path::component::Options {
                    protect_windows: false,
                    protect_hfs: true,
                    protect_ntfs: true,
                },
            )
            .map_err(pack_error)?;
            if !names.insert(entry.filename)
                || previous
                    .as_ref()
                    .is_some_and(|last: &gix_object::tree::EntryRef<'_>| last >= &entry)
            {
                return invalid("tree entries are duplicated or unordered");
            }
            ObjectId::from_bytes(id.format(), entry.oid.as_bytes())?;
            previous = Some(entry);
            let kind = match entry.mode.kind() {
                EntryKind::Tree => Kind::Tree,
                EntryKind::Blob | EntryKind::BlobExecutable | EntryKind::Link => Kind::Blob,
                EntryKind::Commit => continue,
            };
            self.link(reader, id, entry.oid.as_bytes(), kind, kind != Kind::Blob)
                .await?;
        }
        Ok(())
    }

    pub(crate) fn location(&self, id: ObjectId) -> Option<u32> {
        self.locations.get(&id).copied()
    }

    fn expect_kind(&mut self, index: usize, kind: Kind) -> Result<(), Error> {
        let node = &mut self.nodes[index];
        if node.kind.is_some_and(|expected| expected != kind) {
            return invalid("graph object kind does not match its reference");
        }
        node.kind = Some(kind);
        Ok(())
    }

    async fn schedule(
        &mut self,
        reader: &mut Reader<'_>,
        id: ObjectId,
        kind: Option<Kind>,
        verify: bool,
    ) -> Result<u32, Error> {
        self.operation.work(size_of::<Node>())?;
        if !reader.contains(id).await? {
            return invalid("graph references a missing object");
        }
        let index = if let Some(index) = self.location(id) {
            if let Some(kind) = kind {
                self.expect_kind(index as usize, kind)?;
            }
            index
        } else {
            if self.nodes.len() == OBJECTS {
                return invalid("graph exceeds object limit");
            }
            let index = u32::try_from(self.nodes.len()).map_err(pack_error)?;
            self.nodes.push(Node {
                id,
                kind,
                verified: false,
                commit_time: 0,
                edges: 0..0,
                queued: false,
            });
            self.locations.insert(id, index);
            index
        };
        let node = &mut self.nodes[index as usize];
        if verify && !node.queued {
            node.queued = true;
            self.queue.push(index);
        }
        Ok(index)
    }

    async fn link(
        &mut self,
        reader: &mut Reader<'_>,
        source: ObjectId,
        bytes: &[u8],
        kind: Kind,
        verify: bool,
    ) -> Result<(), Error> {
        if self.edges.len() == MAX_EDGES {
            return invalid("graph exceeds edge limit");
        }
        let id = ObjectId::from_bytes(source.format(), bytes)?;
        let index = self.schedule(reader, id, Some(kind), verify).await?;
        if self.edges.len() == self.edges.capacity() {
            let capacity = (self.edges.capacity().max(128) * 2).min(MAX_EDGES);
            self.edge_memory
                .grow((capacity - self.edges.capacity()) * size_of::<u32>())?;
            self.edges.reserve_exact(capacity - self.edges.len());
        }
        self.edges.push(index);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{fmt::Write as _, io::Write, sync::Arc};

    use object_log::{
        Log, LogId, Options, ValidatedBackend, View,
        sim::{FaultStore, Operation as StoreOperation},
    };
    use object_store::{memory::InMemory, path::Path};

    use super::*;
    use crate::{
        ObjectFormat,
        durable::{self, Catalog},
        pack::{
            self,
            budget::{LIVE_BYTES, Pool},
        },
    };

    type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;
    type Raw = (Kind, Vec<u8>);

    fn id(format: ObjectFormat, object: &Raw) -> TestResult<ObjectId> {
        let hash = gix_object::compute_hash(object_hash(format), object.0, &object.1)?;
        Ok(ObjectId::from_bytes(format, hash.as_slice())?)
    }

    fn commit(tree: ObjectId, parents: &[ObjectId], message: usize) -> Raw {
        let mut data = format!("tree {tree}\n");
        for parent in parents {
            let _ = writeln!(data, "parent {parent}");
        }
        let _ = writeln!(
            data,
            "author A <a@example.com> 0 +0000\ncommitter A <a@example.com> 0 +0000\n\n{message}"
        );
        (Kind::Commit, data.into_bytes())
    }

    fn tree(entries: &[(&str, &str, ObjectId)]) -> Raw {
        let mut data = Vec::new();
        for (mode, name, id) in entries {
            data.extend_from_slice(format!("{mode} {name}\0").as_bytes());
            data.extend_from_slice(id.as_bytes());
        }
        (Kind::Tree, data)
    }

    fn tag(target: ObjectId, kind: &str, name: &str) -> Raw {
        (Kind::Tag, format!("object {target}\ntype {kind}\ntag {name}\ntagger A <a@example.com> 0 +0000\n\n{name}\n").into_bytes())
    }

    fn pack(format: ObjectFormat, objects: &[Raw]) -> TestResult<Vec<u8>> {
        let mut writer = gix_hash::io::Write::new(Vec::new(), object_hash(format));
        writer.write_all(&gix_pack::data::header::encode(
            gix_pack::data::Version::V2,
            u32::try_from(objects.len())?,
        ))?;
        for (kind, data) in objects {
            let header = match kind {
                Kind::Tree => gix_pack::data::entry::Header::Tree,
                Kind::Commit => gix_pack::data::entry::Header::Commit,
                Kind::Tag => gix_pack::data::entry::Header::Tag,
                Kind::Blob => gix_pack::data::entry::Header::Blob,
            };
            header.write_to(data.len() as u64, &mut writer)?;
            let mut compressor =
                gix_zlib::stream::deflate::Write::new(&mut writer, gix_zlib::Compression::DEFAULT);
            compressor.write_all(data)?;
            compressor.flush()?;
        }
        let gix_hash::io::Write { hash, mut inner } = writer;
        inner.extend_from_slice(hash.try_finalize()?.as_slice());
        Ok(inner)
    }

    struct Repository {
        log: Log,
        view: View,
        catalog: Catalog,
        store: FaultStore,
        operation: Operation,
    }

    impl Repository {
        async fn new(format: ObjectFormat, groups: &[Vec<Raw>]) -> TestResult<Self> {
            let store = FaultStore::from_arc(Arc::new(InMemory::new()));
            let backend =
                ValidatedBackend::new(Arc::new(store.clone()), Path::from("graph")).await?;
            let log = Log::open(&backend, &LogId::new("graph")?, Options::default()).await?;
            let view = log.load().await?;
            let mut roots = Vec::new();
            for group in groups {
                let operation = Pool::new(LIVE_BYTES).admit()?;
                let log = log.with_request_guard(Arc::new(operation.clone()));
                let bytes = pack(format, group)?;
                let normalized = pack::normalize(&operation, format, &bytes, &[])?;
                let (descriptor, root) =
                    durable::stage(&operation, &log, &view, normalized).await?;
                roots.push((descriptor, root.reference().clone()));
            }
            let operation = Pool::new(LIVE_BYTES).admit()?;
            let log = log.with_request_guard(Arc::new(operation.clone()));
            let catalog = durable::load(&operation, &log, &view, format, &roots).await?;
            store.reset();
            Ok(Self {
                log,
                view,
                catalog,
                store,
                operation,
            })
        }

        async fn graph(&self, roots: &[ObjectId]) -> Result<Graph, Error> {
            Graph::load(
                &self.operation,
                &mut Reader::new(&self.log, &self.view, &self.catalog),
                roots,
            )
            .await
        }
    }

    #[tokio::test]
    async fn direct_large_blob_roots_are_verified_without_materializing_the_body() -> TestResult {
        for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
            let blob = (Kind::Blob, vec![42; 50 * 1024 * 1024]);
            let blob_id = id(format, &blob)?;
            let annotated = tag(blob_id, "blob", "large");
            let tag_id = id(format, &annotated)?;
            let bytes = bytes::Bytes::from(pack(format, &[blob, annotated])?);
            let backend =
                ValidatedBackend::new(Arc::new(InMemory::new()), Path::from("large-blob-root"))
                    .await?;
            let log = Log::open(
                &backend,
                &LogId::new("large-blob-root")?,
                Options::default(),
            )
            .await?;
            let view = log.load().await?;
            let stage = Pool::new(LIVE_BYTES).admit()?;
            let input = pack::ingest::Input::receive(
                &stage,
                &log,
                &view,
                futures::stream::iter([Ok(bytes)]),
            )
            .await?;
            let (descriptor, root) = input.scan(format).await?.normalize(&mut NoBases).await?;
            drop(input);
            drop(stage);
            let operation = Pool::new(16 * 1024 * 1024).admit()?;
            let log = log.with_request_guard(Arc::new(operation.clone()));
            let catalog = durable::load(
                &operation,
                &log,
                &view,
                format,
                &[(descriptor, root.reference().clone())],
            )
            .await?;
            let graph = Graph::load(
                &operation,
                &mut Reader::new(&log, &view, &catalog),
                &[blob_id],
            )
            .await?;
            assert_eq!(graph.nodes.len(), 1);
            assert_eq!(graph.nodes[0].kind, Some(Kind::Blob));
            assert!(graph.nodes[0].verified);
            assert!(graph.edges.is_empty());
            drop(graph);
            // Processing the tag first sets the direct root's declared kind
            // before that root reaches the queue; it must still avoid find().
            let graph = Graph::load(
                &operation,
                &mut Reader::new(&log, &view, &catalog),
                &[tag_id, blob_id],
            )
            .await?;
            assert_eq!(graph.nodes.len(), 2);
            assert_eq!(graph.edges.len(), 1);
            assert!(graph.nodes.iter().all(|node| node.verified));
        }
        Ok(())
    }

    struct NoBases;
    impl pack::ingest::BaseProvider for NoBases {
        async fn provide<'a>(
            &mut self,
            _: &pack::ingest::Input<'a>,
            _: ObjectId,
        ) -> Result<Option<pack::ingest::Decoded<'a>>, Error> {
            Ok(None)
        }
    }

    #[tokio::test]
    async fn both_hashes_visit_merge_parents_nested_tags_and_skip_blob_bodies_and_gitlinks()
    -> TestResult {
        for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
            let blob = (Kind::Blob, vec![42; 1024 * 1024]);
            let blob_id = id(format, &blob)?;
            let absent = ObjectId::from_bytes(format, &vec![17; format.digest_len()])?;
            let subtree = tree(&[("100644", "file", blob_id)]);
            let subtree_id = id(format, &subtree)?;
            let root = tree(&[
                ("160000", "external", absent),
                ("120000", "link", blob_id),
                ("40000", "nested", subtree_id),
                ("100755", "script", blob_id),
            ]);
            let root_id = id(format, &root)?;
            let left = commit(root_id, &[], 1);
            let right = commit(root_id, &[], 2);
            let left_id = id(format, &left)?;
            let right_id = id(format, &right)?;
            let merge = commit(root_id, &[left_id, right_id], 3);
            let merge_id = id(format, &merge)?;
            let inner = tag(merge_id, "commit", "inner");
            let inner_id = id(format, &inner)?;
            let outer = tag(inner_id, "tag", "outer");
            let outer_id = id(format, &outer)?;
            let repository = Repository::new(
                format,
                &[
                    vec![subtree, root, left, right, merge, inner, outer],
                    vec![blob],
                ],
            )
            .await?;
            let graph = repository.graph(&[outer_id, merge_id, outer_id]).await?;
            assert_eq!(graph.nodes.len(), 8);
            assert_eq!(graph.queue.len(), 7);
            assert!(graph.location(absent).is_none());
            let leaf = &graph.nodes[graph.location(blob_id).ok_or("missing blob leaf")? as usize];
            assert_eq!(leaf.kind, Some(Kind::Blob));
            assert!(!leaf.verified);
            assert_eq!(
                repository
                    .store
                    .metrics()
                    .operation(StoreOperation::Get)
                    .requests,
                1
            );
            let merge = &graph.nodes[graph.location(merge_id).ok_or("missing merge")? as usize];
            assert_eq!(graph.edges[merge.edges.clone()].len(), 3);
            let outer = &graph.nodes[graph.location(outer_id).ok_or("missing outer tag")? as usize];
            assert_eq!(
                graph.nodes[graph.edges[outer.edges.start] as usize].id,
                inner_id
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn deep_history_uses_a_deduplicated_iterative_queue() -> TestResult {
        for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
            let tree = tree(&[]);
            let tree_id = id(format, &tree)?;
            let mut objects = vec![tree];
            let mut parents = Vec::new();
            for sequence in 0..2048 {
                let object = commit(tree_id, &parents, sequence);
                parents = vec![id(format, &object)?];
                objects.push(object);
            }
            let repository = Repository::new(format, &[objects]).await?;
            let graph = repository.graph(&parents).await?;
            assert_eq!(graph.nodes.len(), 2049);
            assert_eq!(graph.queue.len(), 2049);
            assert_eq!(graph.edges.len(), 4095);
        }
        Ok(())
    }

    #[tokio::test]
    async fn malformed_and_truncated_objects_fail_instead_of_hiding_iterator_errors() -> TestResult
    {
        for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
            let empty = tree(&[]);
            let tree_id = id(format, &empty)?;
            let valid = commit(tree_id, &[], 0);
            let tag = tag(tree_id, "tree", "v1");
            let cases = [
                (Kind::Commit, Vec::new()),
                (Kind::Commit, format!("tree {tree_id}\n").into_bytes()),
                (
                    Kind::Commit,
                    valid.1[..=valid
                        .1
                        .windows(2)
                        .position(|part| part == b"\n\n")
                        .ok_or("missing separator")?]
                        .to_vec(),
                ),
                (
                    Kind::Commit,
                    format!("tree {tree_id}\nparent broken\n").into_bytes(),
                ),
                (Kind::Tag, tag.1[..20].to_vec()),
                (Kind::Tree, b"100644 file\0truncated".to_vec()),
                tree(&[("100644", "same", tree_id), ("100644", "same", tree_id)]),
                tree(&[("100644", "z", tree_id), ("100644", "a", tree_id)]),
                tree(&[("100644", "same", tree_id), ("40000", "same", tree_id)]),
                tree(&[
                    ("100644", "a", tree_id),
                    ("100644", "a.c", tree_id),
                    ("40000", "a", tree_id),
                ]),
                tree(&[("100644", ".", tree_id)]),
                tree(&[("100644", "..", tree_id)]),
                tree(&[("100644", ".git", tree_id)]),
                tree(&[("100644", "bad/name", tree_id)]),
                tree(&[("120000", ".gitmodules", tree_id)]),
                tree(&[("000000", "bad-mode", tree_id)]),
                tree(&[("100664", "bad-mode", tree_id)]),
            ];
            for object in cases {
                let root = id(format, &object)?;
                let repository = Repository::new(format, &[vec![empty.clone(), object]]).await?;
                assert!(repository.graph(&[root]).await.is_err());
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn missing_objects_and_conflicting_declared_or_actual_kinds_fail() -> TestResult {
        let format = ObjectFormat::Sha1;
        let empty = tree(&[]);
        let tree_id = id(format, &empty)?;
        let absent = ObjectId::from_bytes(format, &[7; 20])?;
        let cases = [
            tree(&[("100644", "missing", absent)]),
            commit(absent, &[], 1),
            commit(tree_id, &[tree_id], 1),
            tree(&[("100644", "blob", tree_id), ("40000", "tree", tree_id)]),
            tag(tree_id, "commit", "wrong"),
            tag(absent, "blob", "missing"),
        ];
        for object in cases {
            let root = id(format, &object)?;
            let repository = Repository::new(format, &[vec![empty.clone(), object]]).await?;
            assert!(repository.graph(&[root]).await.is_err());
        }
        Ok(())
    }

    #[tokio::test]
    async fn tag_to_a_tree_leaf_defers_blob_content_verification() -> TestResult {
        let format = ObjectFormat::Sha1;
        let blob = (Kind::Blob, b"leaf".to_vec());
        let blob_id = id(format, &blob)?;
        let tree = tree(&[("100644", "leaf", blob_id)]);
        let tree_id = id(format, &tree)?;
        let tag = tag(blob_id, "blob", "leaf");
        let tag_id = id(format, &tag)?;
        let repository = Repository::new(format, &[vec![tree, tag], vec![blob]]).await?;
        let graph = repository.graph(&[tree_id, tag_id]).await?;
        assert!(!graph.nodes[graph.location(blob_id).ok_or("missing leaf")? as usize].verified);
        assert_eq!(graph.queue.len(), 2);
        assert_eq!(
            repository
                .store
                .metrics()
                .operation(StoreOperation::Get)
                .requests,
            1
        );
        Ok(())
    }

    #[tokio::test]
    async fn wide_repeated_edges_keep_one_leaf_and_reserve_before_allocation() -> TestResult {
        let format = ObjectFormat::Sha1;
        let blob = (Kind::Blob, b"leaf".to_vec());
        let blob_id = id(format, &blob)?;
        let mut data = Vec::new();
        for index in 0..4096 {
            data.extend_from_slice(format!("100644 file{index:04}\0").as_bytes());
            data.extend_from_slice(blob_id.as_bytes());
        }
        let root = (Kind::Tree, data);
        let root_id = id(format, &root)?;
        let repository = Repository::new(format, &[vec![root], vec![blob]]).await?;
        let operation = Pool::new(FIXED_BYTES - 1).admit()?;
        let mut reader = Reader::new(&repository.log, &repository.view, &repository.catalog);
        assert!(
            Graph::load(&operation, &mut reader, &[root_id])
                .await
                .is_err()
        );
        assert_eq!(
            repository
                .store
                .metrics()
                .operation(StoreOperation::Get)
                .requests,
            0
        );
        let before = repository.operation.live_bytes();
        let graph = repository.graph(&[root_id]).await?;
        assert_eq!(graph.nodes.len(), 2);
        assert_eq!(graph.edges.len(), 4096);
        assert_eq!(
            repository.operation.live_bytes() - before,
            FIXED_BYTES + graph.edges.capacity() * size_of::<u32>()
        );
        drop(graph);
        assert_eq!(repository.operation.live_bytes(), before);
        Ok(())
    }
    #[tokio::test]
    async fn object_limit_rejects_before_reads_and_work_stays_cumulative() -> TestResult {
        let format = ObjectFormat::Sha1;
        let mut groups = vec![Vec::new(), Vec::new()];
        let mut roots = Vec::new();
        for index in 0..=MAX_OBJECTS {
            let object = (Kind::Blob, index.to_be_bytes().to_vec());
            roots.push(id(format, &object)?);
            groups[index as usize / (OBJECTS / 2 + 1)].push(object);
        }
        let repository = Repository::new(format, &groups).await?;
        assert!(repository.graph(&roots).await.is_err());
        assert_eq!(
            repository
                .store
                .metrics()
                .operation(StoreOperation::Get)
                .requests,
            0
        );
        assert!(repository.operation.work_bytes() > (OBJECTS * size_of::<Node>()) as u64);
        // The failed attempt's work is retained; a retry never admits a fresh budget.
        let remaining = pack::budget::WORK_BYTES - repository.operation.work_bytes();
        repository.operation.work(remaining)?;
        assert!(repository.graph(&roots[..1]).await.is_err());
        assert_eq!(
            repository
                .store
                .metrics()
                .operation(StoreOperation::Get)
                .requests,
            0
        );
        Ok(())
    }
}
