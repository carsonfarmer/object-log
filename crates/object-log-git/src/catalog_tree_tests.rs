use super::*;
use crate::pack::budget::{LIVE_BYTES, Pool};
use object_log::{
    CheckpointStatus, CollectionFinish, CollectionStart, CommitStatus, LogId, Materializer,
    Options, TransactionId, ValidatedBackend,
    sim::{FaultStore, Operation as Io},
};
use object_store::{memory::InMemory, path::Path};
use std::{
    io::Write,
    process::{Command, Stdio},
    sync::Arc,
};
type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;
fn operation() -> Result<Operation, Error> {
    Pool::new(LIVE_BYTES).admit()
}
async fn fixture_log(name: &str) -> TestResult<(Log, View, FaultStore, ValidatedBackend)> {
    let store = FaultStore::new(InMemory::new());
    let backend =
        ValidatedBackend::new(Arc::new(store.clone()), Path::from("catalog-tree")).await?;
    let log = Log::open(
        &backend,
        &LogId::new(name)?,
        Options {
            max_object_bytes: 8240,
            ..Options::default()
        },
    )
    .await?;
    let view = log.load().await?;
    Ok((log, view, store, backend))
}
fn git(path: &std::path::Path, args: &[&str], input: &[u8]) -> TestResult<Vec<u8>> {
    let mut child = Command::new("git")
        .current_dir(path)
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    child.stdin.take().ok_or("stdin")?.write_all(input)?;
    let output = child.wait_with_output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(output.stdout)
}
async fn pack(
    log: &Log,
    view: &View,
    format: ObjectFormat,
    count: usize,
    seed: usize,
) -> TestResult<(PackDescriptor, StagedObject, Vec<(ObjectId, u32)>)> {
    let directory = tempfile::tempdir()?;
    git(
        directory.path(),
        &[
            "init",
            "--bare",
            "--quiet",
            if format == ObjectFormat::Sha1 {
                "--object-format=sha1"
            } else {
                "--object-format=sha256"
            },
        ],
        &[],
    )?;
    let mut input = Vec::new();
    for index in 0..count {
        let data = format!("catalog fixture {seed} {index}");
        write!(&mut input, "blob\ndata {}\n{data}\n", data.len())?;
    }
    input.extend_from_slice(b"done\n");
    git(directory.path(), &["fast-import", "--quiet"], &input)?;
    let ids = git(
        directory.path(),
        &[
            "cat-file",
            "--batch-all-objects",
            "--batch-check=%(objectname)",
        ],
        &[],
    )?;
    let bytes = git(
        directory.path(),
        &["pack-objects", "--stdout", "--window=0"],
        &ids,
    )?;
    let op = operation()?;
    let normalized = crate::pack::normalize(&op, format, &bytes, &[])?;
    let index = gix_pack::index::File::from_data(
        Bytes::copy_from_slice(&normalized.index),
        std::path::PathBuf::new(),
        crate::pack::object_hash(format),
    )?;
    let entries = index
        .iter()
        .enumerate()
        .map(|(position, entry)| {
            Ok((
                ObjectId::from_bytes(format, entry.oid.as_slice())?,
                u32::try_from(position)?,
            ))
        })
        .collect::<TestResult<Vec<_>>>()?;
    let (descriptor, root) = crate::durable::stage(&op, log, view, normalized).await?;
    Ok((descriptor, root, entries))
}

#[tokio::test]
async fn both_hashes_split_lookup_and_cow_without_unrelated_reads() -> TestResult {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let (log, view, store, _) = fixture_log("lookup").await?;
        let (descriptor, root, entries) = pack(&log, &view, format, 128, 0).await?;
        let tree = CatalogTree::empty(format)
            .insert_pack(
                &log,
                &view,
                &operation()?,
                descriptor.clone(),
                root.clone(),
                &entries,
            )
            .await?;
        let old = log
            .read_node(&view, tree.root().ok_or("root")?.reference())
            .await?;
        assert_eq!(old.children().len(), 2);
        for &(id, index) in &[entries[0], entries[127]] {
            store.reset();
            let location = tree
                .lookup(&log, &view, &operation()?, id)
                .await?
                .ok_or("missing")?;
            assert_eq!(location.descriptor, descriptor);
            assert_eq!(location.root.reference(), root.reference());
            assert_eq!(location.index, index);
            assert_eq!(store.metrics().operation(Io::Get).requests, 2);
            assert_eq!(store.metrics().operation(Io::Put).requests, 0);
        }
        let (new_descriptor, new_root, new_entries) = pack(&log, &view, format, 1, 1).await?;
        assert!(
            tree.lookup(&log, &view, &operation()?, new_entries[0].0)
                .await?
                .is_none()
        );
        store.reset();
        let changed = tree
            .insert_pack(
                &log,
                &view,
                &operation()?,
                new_descriptor.clone(),
                new_root,
                &new_entries,
            )
            .await?;
        assert_eq!(store.metrics().operation(Io::Get).requests, 2);
        assert_eq!(store.metrics().operation(Io::Put).requests, 3);
        let new = log
            .read_node(&view, changed.root().ok_or("root")?.reference())
            .await?;
        assert_eq!(new.children().len(), 3);
        assert!(
            old.children()
                .iter()
                .any(|child| new.children().contains(child))
        );
        let found = changed
            .lookup(&log, &view, &operation()?, new_entries[0].0)
            .await?
            .ok_or("missing insertion")?;
        assert_eq!(found.descriptor, new_descriptor);
        for (id, _) in entries {
            assert!(
                changed
                    .lookup(&log, &view, &operation()?, id)
                    .await?
                    .is_some()
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn duplicate_oids_choose_lowest_pack_id_and_bad_input_has_no_writes() -> TestResult {
    let (log, view, store, _) = fixture_log("duplicates").await?;
    let format = ObjectFormat::Sha1;
    let (a, root_a, entries_a) = pack(&log, &view, format, 2, 0).await?;
    let (b, root_b, entries_b) = pack(&log, &view, format, 1, 0).await?;
    for reverse in [false, true] {
        let values = if reverse {
            [(&b, &root_b, &entries_b), (&a, &root_a, &entries_a)]
        } else {
            [(&a, &root_a, &entries_a), (&b, &root_b, &entries_b)]
        };
        let mut tree = CatalogTree::empty(format);
        for (pack, root, entries) in values {
            tree = tree
                .insert_pack(
                    &log,
                    &view,
                    &operation()?,
                    pack.clone(),
                    root.clone(),
                    entries,
                )
                .await?;
        }
        assert_eq!(
            tree.lookup(&log, &view, &operation()?, entries_b[0].0)
                .await?
                .ok_or("duplicate")?
                .descriptor
                .id,
            a.id.min(b.id)
        );
    }
    store.reset();
    assert!(
        CatalogTree::empty(format)
            .insert_pack(
                &log,
                &view,
                &operation()?,
                a.clone(),
                root_a,
                &[entries_a[0], entries_a[0]]
            )
            .await
            .is_err()
    );
    assert_eq!(store.metrics().total_requests(), 0);
    Ok(())
}

#[tokio::test]
async fn authenticated_malformed_tree_and_wrong_child_bounds_are_rejected() -> TestResult {
    let (log, view, _, _) = fixture_log("malformed").await?;
    let format = ObjectFormat::Sha256;
    let (descriptor, pack_root, entries) = pack(&log, &view, format, 2, 3).await?;
    for failure in [
        "version", "format", "order", "slot", "trailing", "branch", "height",
    ] {
        let mut payload = Payload {
            version: VERSION,
            format,
            level: 0,
            keys: entries.iter().map(|entry| entry.0).collect(),
            packs: vec![descriptor.clone()],
            slots: vec![0, 0],
            indexes: vec![0, 1],
        };
        match failure {
            "version" => payload.version += 1,
            "format" => payload.format = ObjectFormat::Sha1,
            "order" => payload.keys.reverse(),
            "slot" => payload.slots[0] = 1,
            "branch" => payload.level = 1,
            "height" => payload.level = MAX_HEIGHT + 1,
            _ => {}
        }
        let mut bytes = minicbor::to_vec(&payload)?;
        if failure == "trailing" {
            bytes.push(0);
        }
        let root = log
            .put_node(&view, bytes.into(), vec![pack_root.clone()])
            .await?;
        assert!(
            CatalogTree::from_root(format, root)
                .lookup(&log, &view, &operation()?, entries[0].0)
                .await
                .is_err(),
            "{failure}"
        );
    }
    let tree = CatalogTree::empty(format)
        .insert_pack(&log, &view, &operation()?, descriptor, pack_root, &entries)
        .await?;
    let root = tree.root().ok_or("root")?.clone();
    let payload = Payload {
        version: VERSION,
        format,
        level: 2,
        keys: vec![entries[0].0],
        packs: vec![],
        slots: vec![],
        indexes: vec![],
    };
    let wrong = log
        .put_node(&view, minicbor::to_vec(payload)?.into(), vec![root])
        .await?;
    assert!(
        CatalogTree::from_root(format, wrong)
            .lookup(&log, &view, &operation()?, entries[0].0)
            .await
            .is_err()
    );
    Ok(())
}

struct RootMachine;
impl Materializer for RootMachine {
    type State = Vec<StagedObject>;
    type Error = Error;
    fn empty(&self) -> Self::State {
        Vec::new()
    }
    fn restore(&self, _: &[u8], objects: &[StagedObject]) -> Result<Self::State, Error> {
        Ok(objects.to_vec())
    }
    fn apply(
        &self,
        state: &mut Self::State,
        _: &[u8],
        objects: &[StagedObject],
    ) -> Result<(), Error> {
        *state = objects.to_vec();
        Ok(())
    }
}
async fn publish(log: &Log, view: &View, tree: &CatalogTree) -> TestResult<View> {
    let prepared = log.prepare(
        view,
        TransactionId::new(),
        Bytes::new(),
        Bytes::new(),
        vec![tree.root().ok_or("root")?.clone()],
    )?;
    let CommitStatus::Committed(view) = log.commit(prepared).await? else {
        return Err("publication".into());
    };
    Ok(view)
}

#[tokio::test]
async fn cold_materialization_and_gc_preserve_catalog_pack_dependencies() -> TestResult {
    let format = ObjectFormat::Sha1;
    let (log, view, _, backend) = fixture_log("collection").await?;
    let (descriptor, root, entries) = pack(&log, &view, format, 128, 0).await?;
    let tree = CatalogTree::empty(format)
        .insert_pack(&log, &view, &operation()?, descriptor, root, &entries)
        .await?;
    let view = publish(&log, &view, &tree).await?;
    let (descriptor, root, additions) = pack(&log, &view, format, 1, 1).await?;
    let changed = tree
        .insert_pack(&log, &view, &operation()?, descriptor, root, &additions)
        .await?;
    let view = publish(&log, &view, &changed).await?;
    let CheckpointStatus::Published(view) = log
        .publish_checkpoint(
            &view,
            view.tail().last().ok_or("tail")?,
            Bytes::new(),
            vec![changed.root().ok_or("root")?.clone()],
        )
        .await?
    else {
        return Err("checkpoint".into());
    };
    let CollectionStart::Installed(fenced, _) = log.start_collection(&view).await? else {
        return Err("collection start".into());
    };
    let CollectionFinish::Complete(view, report) = log.resume_collection(&fenced).await? else {
        return Err("collection finish".into());
    };
    assert!(report.delete_attempts() > 0);
    assert!(matches!(
        changed
            .lookup(&log, &view, &operation()?, entries[0].0)
            .await,
        Err(Error::ObjectLog(object_log::Error::InvalidStagedObject))
    ));
    let cold = Log::open_existing(&backend, &LogId::new("collection")?, log.options()).await?;
    let state = object_log::materialize(&cold, cold.load().await?, &RootMachine).await?;
    let recovered = CatalogTree::from_root(format, state.state()[0].clone());
    for &(id, _) in &[entries[0], additions[0]] {
        let location = recovered
            .lookup(&cold, state.view(), &operation()?, id)
            .await?
            .ok_or("cold lookup")?;
        let op = operation()?;
        let catalog = crate::durable::load(
            &op,
            &cold,
            state.view(),
            format,
            &[(location.descriptor, location.root.reference().clone())],
        )
        .await?;
        assert!(
            crate::durable::Reader::new(&cold, state.view(), &catalog)
                .find(id)
                .await?
                .is_some()
        );
    }
    Ok(())
}

#[tokio::test]
async fn repeated_batches_grow_balanced_tree_and_keep_lookup_depth_bounded() -> TestResult {
    let format = ObjectFormat::Sha256;
    let (log, view, store, _) = fixture_log("height").await?;
    let mut tree = CatalogTree::empty(format);
    let mut samples = Vec::new();
    for seed in 0..40 {
        let (descriptor, root, entries) = pack(&log, &view, format, 100, seed).await?;
        samples.push(entries[0]);
        tree = tree
            .insert_pack(&log, &view, &operation()?, descriptor, root, &entries)
            .await?;
    }
    let root = load(
        &log,
        &view,
        &operation()?,
        format,
        tree.root().ok_or("root")?,
        None,
        None,
    )
    .await?;
    assert_eq!(root.payload.level, 2);
    drop(root);
    for (id, expected) in samples {
        store.reset();
        let found = tree
            .lookup(&log, &view, &operation()?, id)
            .await?
            .ok_or("lost batch")?;
        assert_eq!(found.index, expected);
        assert_eq!(store.metrics().operation(Io::Get).requests, 3);
    }
    Ok(())
}

#[tokio::test]
async fn catalog_quota_failure_and_cancellation_release_construction_memory() -> TestResult {
    use object_log::sim::FailurePhase;
    let format = ObjectFormat::Sha1;
    let (log, view, store, _) = fixture_log("cancel").await?;
    let (descriptor, root, entries) = pack(&log, &view, format, 128, 0).await?;
    let op = operation()?;
    let pressure = op.reserve_state(24 * 1024 * 1024 - 1)?;
    store.reset();
    assert!(
        CatalogTree::empty(format)
            .insert_pack(&log, &view, &op, descriptor.clone(), root.clone(), &entries)
            .await
            .is_err()
    );
    assert_eq!(store.metrics().total_requests(), 0);
    drop(pressure);
    let tree = CatalogTree::empty(format);
    let mut pause = store.pause_next_put(FailurePhase::Before);
    let mut pending = Box::pin(tree.insert_pack(&log, &view, &op, descriptor, root, &entries));
    tokio::select! {
        result = &mut pending => { result?; return Err("insertion did not pause".into()); },
        entered = pause.wait_until_entered() => assert!(entered),
    }
    assert!(op.live_bytes() > 0);
    drop(pending);
    assert_eq!(op.live_bytes(), 0);
    assert!(!pause.release());
    Ok(())
}

#[tokio::test]
async fn descendant_bounds_inherit_the_ancestor_upper_bound() -> TestResult {
    let format = ObjectFormat::Sha256;
    let (log, view, _, _) = fixture_log("bounds").await?;
    let (descriptor, pack_root, entries) = pack(&log, &view, format, 3, 2).await?;
    let mut branches = Vec::new();
    for indexes in [vec![0, 2], vec![1]] {
        let lower = entries[indexes[0]].0;
        let leaf = Payload {
            version: VERSION,
            format,
            level: 0,
            keys: indexes.iter().map(|index| entries[*index].0).collect(),
            packs: vec![descriptor.clone()],
            slots: vec![0; indexes.len()],
            indexes: indexes.iter().map(|index| entries[*index].1).collect(),
        };
        let leaf = log
            .put_node(
                &view,
                minicbor::to_vec(leaf)?.into(),
                vec![pack_root.clone()],
            )
            .await?;
        let branch = Payload {
            version: VERSION,
            format,
            level: 1,
            keys: vec![lower],
            packs: vec![],
            slots: vec![],
            indexes: vec![],
        };
        branches.push(
            log.put_node(&view, minicbor::to_vec(branch)?.into(), vec![leaf])
                .await?,
        );
    }
    let root = Payload {
        version: VERSION,
        format,
        level: 2,
        keys: vec![entries[0].0, entries[1].0],
        packs: vec![],
        slots: vec![],
        indexes: vec![],
    };
    let root = log
        .put_node(&view, minicbor::to_vec(root)?.into(), branches)
        .await?;
    let tree = CatalogTree::from_root(format, root);
    assert!(
        tree.lookup(&log, &view, &operation()?, entries[0].0)
            .await
            .is_err()
    );
    let operation = operation()?;
    let guarded = log.with_request_guard(std::sync::Arc::new(operation.clone()));
    let mut cache = CatalogCache::new(&tree, &guarded, &view, &operation)?;
    assert!(cache.lookup(entries[0].0).await.is_err());
    let calls = operation.calls();
    // Even an already decoded node must satisfy this traversal's bounds.
    assert!(cache.lookup(entries[0].0).await.is_err());
    assert_eq!(operation.calls(), calls);
    Ok(())
}

#[tokio::test]
async fn huge_authenticated_array_counts_fail_before_allocation() -> TestResult {
    let (log, view, store, _) = fixture_log("array").await?;
    let mut bytes = vec![0x87, VERSION, 1, 0, 0x9b];
    bytes.extend_from_slice(&u64::MAX.to_be_bytes());
    let root = log.put_node(&view, bytes.into(), vec![]).await?;
    let id = ObjectId::from_bytes(ObjectFormat::Sha1, &[1; 20])?;
    let op = operation()?;
    store.reset();
    assert!(
        matches!(CatalogTree::from_root(ObjectFormat::Sha1, root).lookup(&log, &view, &op, id).await,
        Err(Error::InvalidPack(message)) if message == "catalog fanout exceeds limit")
    );
    assert_eq!(store.metrics().operation(Io::Get).requests, 1);
    assert_eq!(op.live_bytes(), 0);
    Ok(())
}

#[tokio::test]
async fn conflicting_catalog_candidates_resolve_and_losing_graph_is_collectible() -> TestResult {
    use object_log::{
        Resolution,
        sim::{Failure, FailurePhase},
    };
    let format = ObjectFormat::Sha1;
    let (log, view, store, backend) = fixture_log("conflict").await?;
    let (descriptor, root, entries) = pack(&log, &view, format, 2, 0).await?;
    let tree = CatalogTree::empty(format)
        .insert_pack(&log, &view, &operation()?, descriptor, root, &entries)
        .await?;
    let view = publish(&log, &view, &tree).await?;
    let mut candidates = Vec::new();
    for seed in [1, 2] {
        let (descriptor, root, entries) = pack(&log, &view, format, 1, seed).await?;
        let changed = tree
            .insert_pack(&log, &view, &operation()?, descriptor, root, &entries)
            .await?;
        let root = changed.root().ok_or("candidate root")?.clone();
        let prepared = log.prepare(
            &view,
            TransactionId::new(),
            Bytes::new(),
            Bytes::new(),
            vec![root.clone()],
        )?;
        candidates.push((prepared, root, entries[0].0));
    }
    let (loser, loser_root, loser_id) = candidates.pop().ok_or("loser")?;
    let (winner, winner_root, winner_id) = candidates.pop().ok_or("winner")?;
    let token = winner.recovery_token()?;
    store.reset();
    store.schedule(Failure {
        operation: Io::Put,
        occurrence: 2,
        phase: FailurePhase::After,
    });
    assert!(matches!(
        log.commit(winner).await?,
        CommitStatus::Pending(_)
    ));
    assert!(matches!(
        log.commit(loser).await?,
        CommitStatus::Conflict(_)
    ));
    let cold = Log::open_existing(&backend, &LogId::new("conflict")?, log.options()).await?;
    assert!(matches!(
        cold.resume(&token).await?,
        Resolution::Committed(_)
    ));
    let view = log.load().await?;
    let CheckpointStatus::Published(view) = log
        .publish_checkpoint(
            &view,
            view.tail().last().ok_or("tail")?,
            Bytes::new(),
            vec![winner_root],
        )
        .await?
    else {
        return Err("checkpoint".into());
    };
    let CollectionStart::Installed(fenced, _) = log.start_collection(&view).await? else {
        return Err("collection".into());
    };
    let CollectionFinish::Complete(view, report) = log.resume_collection(&fenced).await? else {
        return Err("finish".into());
    };
    assert!(report.delete_attempts() > 0);
    assert!(log.read_node(&view, loser_root.reference()).await.is_err());
    let state = object_log::materialize(&cold, cold.load().await?, &RootMachine).await?;
    let tree = CatalogTree::from_root(format, state.state()[0].clone());
    assert!(
        tree.lookup(&cold, state.view(), &operation()?, winner_id)
            .await?
            .is_some()
    );
    assert!(
        tree.lookup(&cold, state.view(), &operation()?, loser_id)
            .await?
            .is_none()
    );
    Ok(())
}

#[tokio::test]
async fn distinct_pack_tables_split_by_encoded_bytes_before_fanout() -> TestResult {
    let format = ObjectFormat::Sha256;
    let (log, view, _, _) = fixture_log("byte-split").await?;
    let mut tree = CatalogTree::empty(format);
    for seed in 0..FANOUT {
        let (descriptor, root, entries) = pack(&log, &view, format, 1, seed).await?;
        tree = tree
            .insert_pack(&log, &view, &operation()?, descriptor, root, &entries)
            .await?;
    }
    let node = load(
        &log,
        &view,
        &operation()?,
        format,
        tree.root().ok_or("root")?,
        None,
        None,
    )
    .await?;
    assert_eq!(
        node.payload.level, 1,
        "one leaf's entry count fits, but its pack table does not"
    );
    assert_eq!(node.children.len(), 2);
    for child in node.children {
        assert!(child.reference().len() <= 8240);
        let child = load(&log, &view, &operation()?, format, &child, None, None).await?;
        assert!(child.payload.keys.len() < FANOUT);
    }
    Ok(())
}

#[tokio::test]
async fn cached_lookup_reuses_authenticated_paths_and_releases_memory() -> TestResult {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let (log, view, store, _) = fixture_log("cached-paths").await?;
        let (descriptor, root, entries) = pack(&log, &view, format, 128, 77).await?;
        let tree = CatalogTree::empty(format)
            .insert_pack(
                &log,
                &view,
                &operation()?,
                descriptor.clone(),
                root.clone(),
                &entries,
            )
            .await?;
        let operation = operation()?;
        store.reset();
        let guarded = log.with_request_guard(std::sync::Arc::new(operation.clone()));
        let mut cache = CatalogCache::new(&tree, &guarded, &view, &operation)?;
        for _ in 0..2 {
            for &(id, position) in &entries {
                let found = cache.lookup(id).await?.ok_or("missing cached OID")?;
                assert_eq!(found.descriptor, descriptor);
                assert_eq!(found.root.reference(), root.reference());
                assert_eq!(found.index, position);
            }
        }
        assert_eq!(store.metrics().operation(Io::Get).requests, 3);
        assert_eq!(store.metrics().operation(Io::Put).requests, 0);
        assert_eq!(operation.calls(), 3);
        assert!(operation.work_bytes() > 0);
        assert!(operation.live_bytes() < 256 * 1024);
        drop(cache);
        assert_eq!(operation.live_bytes(), 0);
        // New command-local cache must authenticate its own paths.
        let guarded = log.with_request_guard(std::sync::Arc::new(operation.clone()));
        let mut cache = CatalogCache::new(&tree, &guarded, &view, &operation)?;
        assert!(cache.lookup(entries[0].0).await?.is_some());
        assert_eq!(operation.calls(), 5);
    }
    Ok(())
}

#[tokio::test]
async fn cache_evicts_under_memory_pressure_without_resetting_work() -> TestResult {
    let (log, view, store, _) = fixture_log("cache-pressure").await?;
    let (descriptor, root, entries) = pack(&log, &view, ObjectFormat::Sha1, 128, 79).await?;
    let tree = CatalogTree::empty(ObjectFormat::Sha1)
        .insert_pack(&log, &view, &operation()?, descriptor, root, &entries)
        .await?;
    let (_, children) = log
        .read_staged_node(&view, tree.root().ok_or("root")?)
        .await?;
    let operation = operation()?;
    let guarded = log.with_request_guard(std::sync::Arc::new(operation.clone()));
    let mut cache = CatalogCache::new(&tree, &guarded, &view, &operation)?;
    store.reset();
    assert!(cache.lookup(entries[0].0).await?.is_some());
    let calls = operation.calls();
    let work = operation.work_bytes();
    let pressure = operation.reserve_state(
        crate::pack::budget::STATE_BYTES - operation.live_bytes() - read_memory(&children[1])? + 1,
    )?;
    // Initial decoder admission is one byte short. Evicting a retained leaf
    // makes room before I/O; counters from the first lookup remain charged.
    assert!(cache.lookup(entries[127].0).await?.is_some());
    assert_eq!(operation.calls(), calls + 1);
    assert!(operation.work_bytes() > work);
    drop(pressure);
    assert!(cache.lookup(entries[0].0).await?.is_some());
    assert_eq!(operation.calls(), calls + 2);
    drop(cache);
    assert_eq!(operation.live_bytes(), 0);
    Ok(())
}

#[tokio::test]
async fn cache_retains_more_than_256_small_nodes_within_the_same_byte_bound() -> TestResult {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let (_, _, store, backend) = fixture_log("wide-cache-backend").await?;
        let log = Log::open(
            &backend,
            &LogId::new("wide-cache")?,
            Options {
                max_object_bytes: 2 * 1024 * 1024,
                ..Options::default()
            },
        )
        .await?;
        let view = log.load().await?;
        let (descriptor, root, entries) = pack(&log, &view, format, 20_000, 91).await?;
        let tree = CatalogTree::empty(format)
            .insert_pack(&log, &view, &operation()?, descriptor, root, &entries)
            .await?;
        let operation = operation()?;
        let guarded = log.with_request_guard(Arc::new(operation.clone()));
        let mut cache = CatalogCache::new(&tree, &guarded, &view, &operation)?;
        store.reset();
        for &(id, _) in &entries {
            assert!(cache.lookup(id).await?.is_some());
        }
        let calls = operation.calls();
        assert!(calls > 256);
        assert!(operation.live_bytes() <= 2 * 1024 * 1024);
        for &(id, _) in entries.iter().rev() {
            assert!(cache.lookup(id).await?.is_some());
        }
        assert_eq!(
            operation.calls(),
            calls,
            "small catalog leaves should not be read twice"
        );
        assert_eq!(store.metrics().operation(Io::Put).requests, 0);
        drop(cache);
        assert_eq!(operation.live_bytes(), 0);
    }
    Ok(())
}
