use std::{collections::HashMap, mem::size_of, ops::Range};

use gix_object::Kind;

use crate::{
    Error, ObjectId,
    durable::Reader,
    pack::{
        budget::{Operation, Reservation},
        invalid, pack_error,
    },
};

const MAX_GRAPH_BYTES: usize = 24 * 1024 * 1024;
// Include nodes, queue, and a conservative bound for HashMap buckets/controls.
fn table_bytes(capacity: usize) -> usize {
    capacity * (size_of::<Node>() + size_of::<u32>() + 2 * (size_of::<(ObjectId, u32)>() + 1)) + 16
}

pub(crate) struct Graph {
    pub(crate) nodes: Vec<Node>,
    pub(crate) edges: Vec<u32>,
    locations: HashMap<ObjectId, u32>,
    queue: Vec<u32>,
    operation: Operation,
    table_limit: usize,
    table_memory: Reservation,
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
        let mut graph = Self {
            nodes: Vec::new(),
            edges: Vec::new(),
            locations: HashMap::new(),
            queue: Vec::new(),
            operation: operation.clone(),
            table_limit: 0,
            table_memory: operation.reserve_state(0)?,
            edge_memory: operation.reserve_state(0)?,
        };
        graph.extend(reader, roots).await?;
        Ok(graph)
    }

    pub(crate) async fn extend(
        &mut self,
        reader: &mut Reader<'_>,
        roots: &[ObjectId],
    ) -> Result<(), Error> {
        let graph = self;
        let mut cursor = graph.queue.len();
        for &id in roots {
            graph.schedule(reader, id, None, true).await?;
        }
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
            let start = graph.edges.len();
            let operation = graph.operation.clone();
            graph.nodes[index].commit_time = crate::structure::visit(
                &operation,
                id,
                object.kind,
                &object.data,
                GraphLinks { graph, reader },
            )
            .await?;
            graph.nodes[index].edges = start..graph.edges.len();
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
            if self.nodes.len() == self.table_limit {
                self.grow_table()?;
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

    fn allocated_bytes(&self) -> usize {
        (if self.table_limit == 0 {
            0
        } else {
            table_bytes(self.table_limit)
        }) + self.edges.capacity() * size_of::<u32>()
    }

    fn check_growth(&self, bytes: usize) -> Result<(), Error> {
        // Both old and replacement allocations are live during growth.
        if bytes > MAX_GRAPH_BYTES - self.allocated_bytes() {
            return invalid("graph exceeds memory limit");
        }
        Ok(())
    }

    fn grow_table(&mut self) -> Result<(), Error> {
        let capacity = self.table_limit.max(64) * 2;
        let bytes = table_bytes(capacity);
        self.check_growth(bytes)?;
        let memory = self.operation.reserve_state(bytes)?;
        self.operation.work(
            self.nodes.len()
                * (size_of::<Node>() + size_of::<u32>() + size_of::<(ObjectId, u32)>()),
        )?;
        let mut nodes = Vec::with_capacity(capacity);
        let mut queue = Vec::with_capacity(capacity);
        let mut locations = HashMap::with_capacity(capacity);
        nodes.append(&mut self.nodes);
        queue.append(&mut self.queue);
        locations.extend(self.locations.drain());
        self.nodes = nodes;
        self.queue = queue;
        self.locations = locations;
        self.table_limit = capacity;
        self.table_memory = memory;
        Ok(())
    }

    fn grow_edges(&mut self) -> Result<(), Error> {
        let capacity = self.edges.capacity().max(128) * 2;
        let bytes = capacity * size_of::<u32>();
        self.check_growth(bytes)?;
        let memory = self.operation.reserve_state(bytes)?;
        self.operation.work(self.edges.len() * size_of::<u32>())?;
        let mut edges = Vec::with_capacity(capacity);
        edges.append(&mut self.edges);
        self.edges = edges;
        self.edge_memory = memory;
        Ok(())
    }

    async fn link(
        &mut self,
        reader: &mut Reader<'_>,
        id: ObjectId,
        kind: Kind,
        verify: bool,
    ) -> Result<(), Error> {
        let index = self.schedule(reader, id, Some(kind), verify).await?;
        if self.edges.len() == self.edges.capacity() {
            self.grow_edges()?;
        }
        self.edges.push(index);
        Ok(())
    }
}

struct GraphLinks<'a, 'store> {
    graph: &'a mut Graph,
    reader: &'a mut Reader<'store>,
}
impl crate::structure::Links for GraphLinks<'_, '_> {
    async fn link(&mut self, id: ObjectId, kind: Kind, verify: bool) -> Result<(), Error> {
        self.graph.link(self.reader, id, kind, verify).await
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
        pack::object_hash,
        pack::{
            self,
            budget::{LIVE_BYTES, Pool},
        },
    };

    type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;
    type Raw = (Kind, Vec<u8>);
    include!("closure_tests.rs");

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
            let (descriptor, root) = input
                .scan(format)
                .await?
                .normalize(&mut pack::ingest::NoBases)
                .await?;
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
        let operation = Pool::new(table_bytes(128) - 1).admit()?;
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
            graph.allocated_bytes()
        );
        drop(graph);
        assert_eq!(repository.operation.live_bytes(), before);
        Ok(())
    }
    #[tokio::test]
    async fn adaptive_graph_growth_charges_overlap_and_releases_failures() -> TestResult {
        let blob = (Kind::Blob, b"leaf".to_vec());
        let blob_id = id(ObjectFormat::Sha256, &blob)?;
        let root = tree(&[("100644", "file", blob_id)]);
        let root_id = id(ObjectFormat::Sha256, &root)?;
        let repository = Repository::new(ObjectFormat::Sha256, &[vec![blob, root]]).await?;
        let operation = Pool::new(LIVE_BYTES).admit()?;
        let before = operation.live_bytes();
        let mut reader = Reader::new(&repository.log, &repository.view, &repository.catalog);
        let mut graph = Graph::load(&operation, &mut reader, &[root_id]).await?;
        for table in [true, false] {
            let allocated = graph.allocated_bytes();
            let capacity = if table {
                graph.table_limit
            } else {
                graph.edges.capacity()
            };
            let replacement = if table {
                table_bytes(capacity * 2)
            } else {
                capacity * 2 * size_of::<u32>()
            };
            let pressure = operation
                .reserve_state(crate::pack::budget::STATE_BYTES - allocated - replacement + 1)?;
            let result = if table {
                graph.grow_table()
            } else {
                graph.grow_edges()
            };
            assert!(result.is_err());
            assert_eq!(graph.allocated_bytes(), allocated);
            assert_eq!(graph.location(root_id), Some(0));
            assert_eq!(graph.edges, [1]);
            drop(pressure);
            if table {
                graph.grow_table()?;
            } else {
                graph.grow_edges()?;
            }
            assert!(graph.allocated_bytes() > allocated);
        }
        // The graph-specific bound also includes temporary replacements.
        let allocated = graph.allocated_bytes();
        assert!(graph.check_growth(MAX_GRAPH_BYTES - allocated + 1).is_err());
        assert_eq!(graph.allocated_bytes(), allocated);
        drop(graph);
        assert_eq!(operation.live_bytes(), before);
        Ok(())
    }

    #[tokio::test]
    async fn adaptive_graph_crosses_stored_pack_limit_and_work_stays_cumulative() -> TestResult {
        let format = ObjectFormat::Sha1;
        let mut groups = vec![Vec::new(), Vec::new()];
        let mut roots = Vec::new();
        for index in 0..=pack::MAX_OBJECTS {
            let object = (Kind::Blob, index.to_be_bytes().to_vec());
            roots.push(id(format, &object)?);
            groups[index as usize / (pack::MAX_OBJECTS as usize / 2 + 1)].push(object);
        }
        let repository = Repository::new(format, &groups).await?;
        let graph = repository.graph(&roots).await?;
        assert_eq!(graph.nodes.len(), roots.len());
        drop(graph);
        repository.store.reset();
        assert_eq!(
            repository
                .store
                .metrics()
                .operation(StoreOperation::Get)
                .requests,
            0
        );
        assert!(
            repository.operation.work_bytes()
                > (pack::MAX_OBJECTS as usize * size_of::<Node>()) as u64
        );
        // Completed work is retained; a retry never admits a fresh budget.
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
