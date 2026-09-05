async fn prepare_compaction_fixture(
    log: &Log,
    format: ObjectFormat,
    first: &Fixture,
    second: &Fixture,
) -> TestResult {
    publish_durable_pack(log, first, format).await?;
    let input = receive_input(
        format,
        &[RefUpdate::new(
            "refs/heads/second",
            None,
            Some(second.target),
        )?],
        &fs::read(&second.pack)?,
        true,
    );
    common_open(log, format)
        .await?
        .prepare_receive(TransactionId::new(), input)
        .await?
        .publish_receive()
        .await?;
    common_open(log, format)
        .await?
        .set_default_branch(
            TransactionId::new(),
            b"refs/heads/main",
            b"refs/heads/unborn",
        )
        .await?;
    common_open(log, format)
        .await?
        .migrate_catalog_attempt(TransactionId::new())
        .await?;
    Ok(())
}

fn falsely_advertised_blob(
    operation: &crate::pack::budget::Operation,
    format: ObjectFormat,
    blob: ObjectId,
) -> TestResult<crate::pack::Normalized> {
    use std::io::Write as _;
    let mut bytes = b"PACK\0\0\0\x02\0\0\0\x01".to_vec();
    gix_pack::data::entry::Header::Blob.write_to(5, &mut bytes)?;
    let mut compressor =
        gix_zlib::stream::deflate::Write::new(&mut bytes, gix_zlib::Compression::DEFAULT);
    compressor.write_all(b"other")?;
    compressor.flush()?;
    drop(compressor);
    let hash = crate::pack::object_hash(format);
    let mut hasher = gix_hash::hasher(hash);
    hasher.update(&bytes);
    bytes.extend_from_slice(hasher.try_finalize()?.as_slice());
    let mut normalized = crate::pack::normalize(operation, format, &bytes, &[])?;
    // Keep valid pack bytes, CRC, offsets and checksums but make its sole IDX
    // entry advertise the old blob OID. The core authenticates this entire
    // physical object; Git must still reject its false content identity.
    for slot in 0..256 {
        normalized.index[8 + slot * 4..12 + slot * 4]
            .copy_from_slice(&u32::from(slot >= usize::from(blob.as_bytes()[0])).to_be_bytes());
    }
    normalized.index[1032..1032 + format.digest_len()].copy_from_slice(blob.as_bytes());
    let end = normalized.index.len() - format.digest_len();
    let mut hasher = gix_hash::hasher(hash);
    hasher.update(&normalized.index[..end]);
    normalized.index[end..].copy_from_slice(hasher.try_finalize()?.as_slice());
    Ok(normalized)
}

#[tokio::test]
async fn live_pack_compaction_preserves_refs_and_reclaims_old_packs_after_checkpoint() -> TestResult
{
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let first = fixture(format, b"first compact")?;
        let second = fixture(format, b"second compact")?;
        let (log, _, backend) = test_log("pack-compaction").await?;
        prepare_compaction_fixture(&log, format, &first, &second).await?;
        let repository = common_open(&log, format).await?;
        let refs = repository.state.refs.clone();
        let catalog = repository.catalog().await?;
        let mut reader = durable::Reader::new(&repository.log, &repository.view, &catalog);
        let old = [
            reader
                .selected_location(first.target)
                .await?
                .ok_or("first")?
                .root,
            reader
                .selected_location(second.target)
                .await?
                .ok_or("second")?
                .root,
        ];
        drop(reader);
        drop(catalog);
        let (prepared, _memory) = repository
            .prepare_pack_compaction(TransactionId::new())
            .await?;
        assert!(matches!(
            repository.log.commit(prepared).await?,
            CommitStatus::Committed(_)
        ));
        drop(repository);
        let compacted = common_open(&log, format).await?;
        assert_eq!(compacted.state.refs, refs);
        assert_eq!(compacted.default_branch(), b"refs/heads/unborn");
        let catalog = compacted.catalog().await?;
        let mut reader = durable::Reader::new(&compacted.log, &compacted.view, &catalog);
        let a = reader
            .selected_location(first.target)
            .await?
            .ok_or("compacted first")?;
        let b = reader
            .selected_location(second.target)
            .await?
            .ok_or("compacted second")?;
        assert_eq!(a.root.reference(), b.root.reference());
        for root in &old {
            assert!(
                log.read_node(&compacted.view, root.reference())
                    .await
                    .is_ok()
            );
        }
        drop(reader);
        drop(catalog);
        drop(compacted);
        assert!(matches!(
            Repository::checkpoint_retaining_packs(&log, format).await?,
            CheckpointStatus::Published(_)
        ));
        let view = log.load().await?;
        assert!(matches!(
            log.start_collection(&view).await?,
            object_log::CollectionStart::Installed(..)
        ));
        let view = log.load().await?;
        assert!(matches!(
            log.resume_collection(&view).await?,
            object_log::CollectionFinish::Complete(..)
        ));
        let cold =
            Log::open_existing(&backend, &LogId::new("pack-compaction")?, log.options()).await?;
        let recovered = common_open(&cold, format).await?;
        assert_eq!(recovered.state.refs, refs);
        assert_eq!(recovered.default_branch(), b"refs/heads/unborn");
        for root in &old {
            assert!(
                cold.read_node(&recovered.view, root.reference())
                    .await
                    .is_err()
            );
        }
        let bytes = recovered
            .fetch_pack(&[first.target, second.target], &[], false)
            .await?;
        let path = first.directory.path().join("compacted.pack");
        fs::write(&path, bytes)?;
        command(
            Some(&first.directory.path().join("source")),
            &["index-pack", "--strict", path.to_str().ok_or("path")?],
        )?;
    }
    Ok(())
}

#[tokio::test]
async fn live_pack_compaction_splits_multiple_output_packs() -> TestResult {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let (log, _, _) = test_log("compaction-multiple").await?;
        let mut fixtures = Vec::new();
        for ordinal in 0..3u8 {
            let mut seed = u64::from(ordinal) + 1;
            let contents = (0..4 * 1024 * 1024)
                .map(|_| {
                    seed ^= seed << 13;
                    seed ^= seed >> 7;
                    seed ^= seed << 17;
                    seed.to_le_bytes()[0]
                })
                .collect::<Vec<_>>();
            let item = fixture(format, &contents)?;
            assert!(fs::metadata(&item.pack)?.len() > 4 * 1024 * 1024);
            let name = format!("refs/heads/branch-{ordinal}");
            let input = receive_input(
                format,
                &[RefUpdate::new(name, None, Some(item.target))?],
                &fs::read(&item.pack)?,
                true,
            );
            common_open(&log, format)
                .await?
                .prepare_receive(TransactionId::new(), input)
                .await?
                .publish_receive()
                .await?;
            fixtures.push(item);
        }
        common_open(&log, format)
            .await?
            .migrate_catalog_attempt(TransactionId::new())
            .await?;
        assert!(matches!(
            Repository::compact_packs(&log, format, TransactionId::new()).await?,
            CommitStatus::Committed(_)
        ));
        let repository = common_open(&log, format).await?;
        let catalog = repository.catalog().await?;
        let mut reader = durable::Reader::new(&repository.log, &repository.view, &catalog);
        let mut packs = std::collections::BTreeSet::new();
        for item in &fixtures {
            // Inspect all reachable objects: the OID order may place commits in
            // one output pack and the large blobs in other output packs.
            let graph =
                crate::graph::Graph::load(&repository.operation, &mut reader, &[item.target])
                    .await?;
            for node in &graph.nodes {
                let location = reader.selected_location(node.id).await?.ok_or("location")?;
                assert!(location.descriptor.bytes <= crate::pack::MAX_RECEIVE_PACK_BYTES as u64);
                packs.insert(location.descriptor.id);
            }
            drop(graph);
            let bytes = repository.fetch_pack(&[item.target], &[], false).await?;
            let path = item.directory.path().join("compacted.pack");
            fs::write(&path, bytes)?;
            command(
                Some(&item.directory.path().join("source")),
                &["index-pack", "--strict", path.to_str().ok_or("path")?],
            )?;
        }
        assert!(packs.len() > 1);
        assert_eq!(repository.refs().len(), 3);
    }
    Ok(())
}

#[tokio::test]
async fn live_pack_compaction_conflicts_without_rebase_and_recovers_pending() -> TestResult {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        for phase in [None, Some(FailurePhase::Before), Some(FailurePhase::After)] {
            let item = fixture(format, b"compaction recovery")?;
            let (log, faults, backend) = test_log("compaction-recovery").await?;
            publish_durable_pack(&log, &item, format).await?;
            common_open(&log, format)
                .await?
                .migrate_catalog_attempt(TransactionId::new())
                .await?;
            let repository = common_open(&log, format).await?;
            let (prepared, memory) = repository
                .prepare_pack_compaction(TransactionId::new())
                .await?;
            if let Some(phase) = phase {
                faults.reset();
                faults.schedule(Failure {
                    operation: Operation::Put,
                    occurrence: 2,
                    phase,
                });
                let CommitStatus::Pending(pending) = repository.log.commit(prepared).await? else {
                    return Err("expected pending".into());
                };
                let token = pending.recovery_token()?;
                drop(memory);
                drop(repository);
                let cold = Log::open_existing(
                    &backend,
                    &LogId::new("compaction-recovery")?,
                    log.options(),
                )
                .await?;
                assert!(matches!(
                    cold.resume(&token).await?,
                    object_log::Resolution::Committed(_)
                ));
                assert_eq!(
                    common_open(&cold, format)
                        .await?
                        .refs()
                        .get(b"refs/heads/main".as_slice()),
                    Some(&item.target)
                );
            } else {
                common_open(&log, format)
                    .await?
                    .set_default_branch(
                        TransactionId::new(),
                        b"refs/heads/main",
                        b"refs/heads/trunk",
                    )
                    .await?;
                assert!(matches!(
                    repository.log.commit(prepared).await?,
                    CommitStatus::Conflict(_)
                ));
                assert_eq!(
                    common_open(&log, format).await?.default_branch(),
                    b"refs/heads/trunk"
                );
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn live_pack_compaction_rejects_authenticated_wrong_blob_identity() -> TestResult {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let item = fixture(format, b"original blob")?;
        let blob = ObjectId::parse(
            format,
            output(
                Some(&item.directory.path().join("source")),
                &["rev-parse", "HEAD:file"],
            )?
            .trim(),
        )?;
        let (log, faults, _) = test_log("compaction-wrong-identity").await?;
        publish_durable_pack(&log, &item, format).await?;
        common_open(&log, format)
            .await?
            .migrate_catalog_attempt(TransactionId::new())
            .await?;
        let repository = common_open(&log, format).await?;
        let catalog = repository.catalog().await?;
        let mut reader = durable::Reader::new(&repository.log, &repository.view, &catalog);
        let original = reader
            .selected_location(item.target)
            .await?
            .ok_or("original pack")?;
        let index = durable::SelectedIndex::load(
            &repository.operation,
            &repository.log,
            &repository.view,
            &original.descriptor,
            &original.root,
        )
        .await?;
        let entries = index
            .entries()
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .filter(|(id, _)| *id != blob)
            .collect::<Vec<_>>();
        drop(index);
        drop(reader);
        drop(catalog);
        let mut tree = crate::catalog_tree::CatalogTree::empty(format)
            .insert_pack(
                &repository.log,
                &repository.view,
                &repository.operation,
                original.descriptor,
                original.root,
                &entries,
            )
            .await?;
        let normalized = falsely_advertised_blob(&repository.operation, format, blob)?;
        let (descriptor, root) = durable::stage(
            &repository.operation,
            &repository.log,
            &repository.view,
            normalized,
        )
        .await?;
        tree = tree
            .insert_pack(
                &repository.log,
                &repository.view,
                &repository.operation,
                descriptor,
                root,
                &[(blob, 0)],
            )
            .await?;
        let record = Record::metadata_update(
            format,
            b"refs/heads/main".to_vec(),
            b"refs/heads/main".to_vec(),
        )?
        .with_catalog(crate::format::CatalogOperation::Replace)?
        .encode()?;
        let prepared = repository.log.prepare(
            &repository.view,
            TransactionId::new(),
            record,
            bytes::Bytes::new(),
            tree.root().cloned().into_iter().collect(),
        )?;
        assert!(matches!(
            repository.log.commit(prepared).await?,
            CommitStatus::Committed(_)
        ));
        drop(repository);
        let repository = common_open(&log, format).await?;
        faults.reset();
        assert!(
            repository
                .prepare_pack_compaction(TransactionId::new())
                .await
                .is_err()
        );
        assert_eq!(faults.metrics().operation(Operation::Put).requests, 0);
    }
    Ok(())
}

async fn compacted_fetch_read_count(
    backend: &ValidatedBackend,
    log: &Log,
    faults: &FaultStore,
    format: ObjectFormat,
    ids: &[ObjectId],
    oracle: &Fixture,
) -> TestResult<u64> {
    faults.reset();
    let cold =
        Log::open_existing(backend, &LogId::new("compaction-cycles")?, log.options()).await?;
    let repository = common_open(&cold, format).await?;
    let pack = repository.fetch_pack(ids, &[], false).await?;
    let reads = faults.metrics().operation(Operation::Get).requests;
    let path = oracle.directory.path().join("cycle.pack");
    fs::write(&path, pack)?;
    command(
        Some(&oracle.directory.path().join("source")),
        &["index-pack", "--strict", path.to_str().ok_or("path")?],
    )?;
    let listing = output(
        Some(&oracle.directory.path().join("source")),
        &[
            "verify-pack",
            "-v",
            path.with_extension("idx").to_str().ok_or("index path")?,
        ],
    )?;
    for id in ids {
        let expected = id.to_string();
        assert!(
            listing
                .lines()
                .any(|line| line.split_whitespace().next() == Some(expected.as_str())),
            "fetched pack omitted requested target {id}"
        );
    }
    Ok(reads)
}

async fn checkpoint_and_collect_compacted(log: &Log, format: ObjectFormat) -> TestResult {
    let operation = Pool::new(crate::pack::budget::LIVE_BYTES).admit_maintenance()?;
    assert!(matches!(
        Repository::retain_packs(log, format, &operation).await?,
        CheckpointStatus::Published(_)
    ));
    let view = log.load().await?;
    assert!(matches!(
        log.start_collection(&view).await?,
        object_log::CollectionStart::Installed(..)
    ));
    let view = log.load().await?;
    assert!(matches!(
        log.resume_collection(&view).await?,
        object_log::CollectionFinish::Complete(..)
    ));
    Ok(())
}

#[tokio::test]
async fn live_pack_compaction_repeated_push_cycles_bound_cold_reads() -> TestResult {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let (log, faults, backend) = test_log("compaction-cycles").await?;
        common_open(&log, format)
            .await?
            .migrate_catalog_attempt(TransactionId::new())
            .await?;
        let mut fixtures = Vec::new();
        let mut first_cycle_reads = None;
        for cycle in 0..3 {
            let mut selected = Vec::new();
            for push in 0..3 {
                let item = fixture(format, format!("cycle {cycle}, push {push}").as_bytes())?;
                let input = receive_input(
                    format,
                    &[RefUpdate::new(
                        format!("refs/heads/cycle-{cycle}-{push}"),
                        None,
                        Some(item.target),
                    )?],
                    &fs::read(&item.pack)?,
                    true,
                );
                common_open(&log, format)
                    .await?
                    .prepare_receive(TransactionId::new(), input)
                    .await?
                    .publish_receive()
                    .await?;
                selected.push(item.target);
                fixtures.push(item);
            }
            let before = compacted_fetch_read_count(
                &backend,
                &log,
                &faults,
                format,
                &selected,
                &fixtures[0],
            )
            .await?;
            // Unrelated retained history must not add pack-index reads to
            // this same three-target, nine-object cold lookup workload.
            if let Some(first) = first_cycle_reads {
                assert!(before <= first + 2);
            } else {
                first_cycle_reads = Some(before);
            }
            let repository = common_open(&log, format).await?;
            let (prepared, memory) = repository
                .prepare_pack_compaction(TransactionId::new())
                .await?;
            assert!(matches!(
                repository.log.commit(prepared).await?,
                CommitStatus::Committed(_)
            ));
            let compaction_work = repository.operation.work_bytes();
            drop(memory);
            drop(repository);
            checkpoint_and_collect_compacted(&log, format).await?;
            let after = compacted_fetch_read_count(
                &backend,
                &log,
                &faults,
                format,
                &selected,
                &fixtures[0],
            )
            .await?;
            assert!(
                after < before,
                "cold reads did not fall: {before} -> {after}"
            );
            let repository = common_open(&log, format).await?;
            let expected = fixtures
                .iter()
                .enumerate()
                .map(|(ordinal, item)| {
                    (
                        format!("refs/heads/cycle-{}-{}", ordinal / 3, ordinal % 3).into_bytes(),
                        item.target,
                    )
                })
                .collect::<std::collections::BTreeMap<_, _>>();
            assert_eq!(repository.refs(), &expected);
            let all = fixtures.iter().map(|item| item.target).collect::<Vec<_>>();
            compacted_fetch_read_count(&backend, &log, &faults, format, &all, &fixtures[0]).await?;
            eprintln!(
                "compaction memory format={format:?} cycle={cycle}: cold reads {before} -> {after}; compaction work={compaction_work}; live refs={}",
                fixtures.len()
            );
        }
    }
    Ok(())
}

async fn interrupt_stream_compaction(
    repository: &Repository,
    ids: &[ObjectId],
    faults: &FaultStore,
    mode: u8,
) -> TestResult {
    let catalog = repository.catalog().await?;
    let mut reader = durable::Reader::new(&repository.log, &repository.view, &catalog);
    let tree = crate::catalog_tree::CatalogTree::empty(repository.format);
    faults.reset();
    let mut pause = faults.pause_put_at(1, FailurePhase::After);
    let mut running = Box::pin(repository.compact_group(&mut reader, &tree, ids));
    assert!(tokio::time::timeout(std::time::Duration::from_secs(10), async {
        tokio::select! { entered = pause.wait_until_entered() => entered, _ = &mut running => false }
    }).await?);
    assert_eq!(faults.metrics().operation(Operation::Put).requests, 1);
    match mode {
        0 => faults.schedule(Failure {
            operation: Operation::Put,
            occurrence: 2,
            phase: FailurePhase::Before,
        }),
        1 => faults.schedule(Failure {
            operation: Operation::Get,
            occurrence: faults.metrics().operation(Operation::Get).requests + 1,
            phase: FailurePhase::Before,
        }),
        _ => {
            drop(running);
            assert!(!pause.release());
            return Ok(());
        }
    }
    assert!(pause.release());
    assert!(running.await.is_err());
    Ok(())
}

#[tokio::test]
async fn streaming_compaction_failure_and_cancel_preserve_authority_and_release_memory()
-> TestResult {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let mut seed = 13_u64;
        let data = (0..4 * 1024 * 1024)
            .map(|_| {
                seed ^= seed << 13;
                seed ^= seed >> 7;
                seed ^= seed << 17;
                seed.to_le_bytes()[0]
            })
            .collect::<Vec<_>>();
        let item = fixture(format, &data)?;
        drop(data);
        let mut ids = output(
            Some(&item.directory.path().join("source")),
            &["rev-list", "--objects", "--all"],
        )?
        .lines()
        .map(|line| ObjectId::parse(format, line.split_whitespace().next().unwrap_or("")))
        .collect::<Result<Vec<_>, _>>()?;
        ids.sort_unstable();
        for mode in 0..3 {
            let (log, faults, _) = test_log("stream-compaction-failure").await?;
            publish_durable_pack(&log, &item, format).await?;
            common_open(&log, format)
                .await?
                .migrate_catalog_attempt(TransactionId::new())
                .await?;
            let repository = common_open(&log, format).await?;
            let refs = repository.refs().clone();
            let generation = repository.view.generation();
            let root = repository
                .state
                .catalog_tree(format)
                .and_then(|tree| tree.root().map(|root| root.reference().clone()));
            let operation = repository.operation.clone();
            interrupt_stream_compaction(&repository, &ids, &faults, mode).await?;
            drop(repository);
            assert_eq!(operation.live_bytes(), 0);
            let recovered = common_open(&log, format).await?;
            assert_eq!(recovered.refs(), &refs);
            assert_eq!(recovered.view.generation(), generation);
            assert_eq!(
                recovered
                    .state
                    .catalog_tree(format)
                    .and_then(|tree| tree.root().map(|root| root.reference().clone())),
                root
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn streaming_compaction_handles_fifty_mib_with_thirty_two_mib_admission() -> TestResult {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let item = fixture(format, &vec![b'x'; 50 * 1024 * 1024])?;
        let (log, _, _) = test_log("stream-compaction-memory").await?;
        let input = receive_input(
            format,
            &[RefUpdate::new("refs/heads/main", None, Some(item.target))?],
            &fs::read(&item.pack)?,
            true,
        );
        assert!(input.len() < 1024 * 1024);
        common_open(&log, format)
            .await?
            .prepare_receive_stream(TransactionId::new(), futures::stream::iter([Ok(input)]))
            .await?
            .publish_receive()
            .await?;
        common_open(&log, format)
            .await?
            .migrate_catalog_attempt(TransactionId::new())
            .await?;
        let operation = Pool::new(32 * 1024 * 1024).admit_maintenance()?;
        let guarded = log.with_request_guard(std::sync::Arc::new(operation.clone()));
        let repository = Repository::open_attempt(&guarded, format, &operation).await?;
        let (prepared, memory) = repository
            .prepare_pack_compaction(TransactionId::new())
            .await?;
        assert!(matches!(
            repository.log.commit(prepared).await?,
            CommitStatus::Committed(_)
        ));
        drop(memory);
        drop(repository);
        assert_eq!(operation.live_bytes(), 0);
        let recovered = common_open(&log, format).await?;
        assert_eq!(
            recovered.refs().get(b"refs/heads/main".as_slice()),
            Some(&item.target)
        );
        let pack = recovered.fetch_pack(&[item.target], &[], false).await?;
        let path = item.directory.path().join("compacted-large.pack");
        fs::write(&path, pack)?;
        command(
            Some(&item.directory.path().join("source")),
            &["index-pack", "--strict", path.to_str().ok_or("path")?],
        )?;
    }
    Ok(())
}
