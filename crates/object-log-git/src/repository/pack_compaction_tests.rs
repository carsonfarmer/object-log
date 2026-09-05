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
