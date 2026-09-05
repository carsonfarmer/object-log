fn receive_send<T: Send>(value: T) -> T { value }

// Reuse the unchanged-Git fixture and cold recovery oracle from receive_tests.

fn receive_frames(input: &Bytes, width: usize) -> impl futures::Stream<Item = Result<Bytes, Error>> + Unpin {
    // Each frame owns its own bounded allocation; slices must not retain input.
    futures::stream::iter(input.chunks(width).map(|bytes| Ok(Bytes::copy_from_slice(bytes))).collect::<Vec<_>>())
}

#[tokio::test]
async fn streaming_receive_publishes_both_hashes_and_preserves_tree_catalogs() -> TestResult {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        for tree in [false, true] {
            let fixture = fixture(format, b"streamed receive")?;
            let (log, faults, _) = test_log("streamed-receive").await?;
            if tree {
                assert!(matches!(common_open(&log, format).await?.migrate_catalog_attempt(TransactionId::new()).await?, Some(object_log::CommitStatus::Committed(_))));
            }
            let update = RefUpdate::new("refs/heads/main", None, Some(fixture.target))?;
            let input = receive_input(format, &[update], &fs::read(&fixture.pack)?, true);
            let repository = common_open(&log, format).await?;
            let operation = repository.operation.clone();
            faults.reset();
            let before = operation.calls();
            let push = receive_send(repository.prepare_receive_stream(TransactionId::new(), receive_frames(&input, 7))).await?;
            let (resolution, response) = push.publish_receive().await?;
            assert!(matches!(resolution, object_log::Resolution::Committed(_)));
            assert_eq!(operation.calls() - before, usize::try_from(faults.metrics().total_requests())?);
            assert!(String::from_utf8_lossy(&response).contains("ok refs/heads/main"));
            drop(resolution);
            drop(response);
            assert_eq!(operation.live_bytes(), 0);
            assert_eq!(cold_checked(&log, format).await?.refs().get(b"refs/heads/main".as_slice()), Some(&fixture.target));
            let delete = receive_input(format, &[RefUpdate::new("refs/heads/main", Some(fixture.target), None)?], &[], true);
            let push = common_open(&log, format).await?.prepare_receive_stream(TransactionId::new(), receive_frames(&delete, 1)).await?;
            assert!(matches!(push.publish_receive().await?.0, object_log::Resolution::Committed(_)));
            assert!(cold_checked(&log, format).await?.refs().is_empty());
        }
    }
    Ok(())
}

#[tokio::test]
async fn streaming_receive_rejects_producer_failure_and_cancellation_without_publication() -> TestResult {
    use std::{pin::Pin, sync::atomic::{AtomicUsize, Ordering}, task::Poll};
    let format = ObjectFormat::Sha1;
    let fixture = fixture(format, b"cancel streamed receive")?;
    let (log, faults, _) = test_log("streamed-failure").await?;
    let input = receive_input(format, &[RefUpdate::new("refs/heads/main", None, Some(fixture.target))?], &fs::read(&fixture.pack)?, true);
    let generation = log.load().await?.generation();
    let repository = common_open(&log, format).await?;
    let operation = repository.operation.clone();
    let polls = std::sync::Arc::new(AtomicUsize::new(0));
    let count = polls.clone();
    let first = input.clone();
    let frames = futures::stream::poll_fn(move |_| {
        match count.fetch_add(1, Ordering::Relaxed) {
            0 => Poll::Ready(Some(Ok(first.clone()))),
            _ => Poll::Ready(Some(Err(Error::InvalidProtocol("producer failed")))),
        }
    });
    assert!(matches!(repository.prepare_receive_stream(TransactionId::new(), frames).await, Err(Error::ReceiveRejected { .. })));
    assert_eq!(polls.load(Ordering::Relaxed), 2);
    assert_eq!(operation.live_bytes(), 0);
    assert_eq!(log.load().await?.generation(), generation);
    let repository = common_open(&log, format).await?;
    let operation = repository.operation.clone();
    let mut pause = faults.pause_next_put(object_log::sim::FailurePhase::Before);
    let mut pending = Box::pin(repository.prepare_receive_stream(TransactionId::new(), receive_frames(&input, 64)));
    assert!(matches!(futures::poll!(Pin::as_mut(&mut pending)), Poll::Pending));
    assert!(pause.wait_until_entered().await);
    drop(pending);
    assert_eq!(operation.live_bytes(), 0);
    assert_eq!(log.load().await?.generation(), generation);
    Ok(())
}

#[tokio::test]
async fn streaming_receive_true_thin_uses_verified_selected_bases() -> TestResult {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        for tree in [false, true] {
        let mut contents = String::new();
        for index in 0..4096 {
            writeln!(contents, "row {index:08} payload")?;
        }
        let fixture = fixture(format, contents.as_bytes())?;
        let (log, _, _) = test_log("common-receive-thin").await?;
        publish_durable_pack(&log, &fixture, format).await?;
        if tree { common_open(&log, format).await?.migrate_catalog_attempt(TransactionId::new()).await?; }
        let source = fixture.directory.path().join("source");
        let contents = contents.replacen("row 00002000", "row changed!", 1);
        fs::write(source.join("file"), contents)?;
        command(Some(&source), &["commit", "--quiet", "-am", "thin change"])?;
        let target = ObjectId::parse(
            format,
            output(Some(&source), &["rev-parse", "HEAD"])?.trim(),
        )?;
        let revisions = source.join("revisions");
        fs::write(&revisions, format!("{target}\n^{}\n", fixture.target))?;
        let packed = Command::new("git")
            .current_dir(&source)
            .args(["pack-objects", "--thin", "--revs", "--stdout"])
            .stdin(fs::File::open(&revisions)?)
            .output()?;
        assert!(packed.status.success());
        let operation = Pool::new(crate::pack::budget::LIVE_BYTES).admit()?;
        assert!(matches!(
            crate::pack::normalize_attempt(&operation, format, &packed.stdout, &[]),
            Err(crate::pack::NormalizeError::MissingBase { .. })
        ));
        drop(operation);
        let input = receive_input(
            format,
            &[RefUpdate::new(
                "refs/heads/main",
                Some(fixture.target),
                Some(target),
            )?],
            &packed.stdout,
            true,
        );
        let push = common_open(&log, format)
            .await?
            .prepare_receive_stream(TransactionId::new(), receive_frames(&input, 31))
            .await?;
        assert!(matches!(
            push.publish_receive().await?.0,
            object_log::Resolution::Committed(_)
        ));
        let recovered = cold_checked(&log, format).await?;
        assert_eq!(
            recovered.refs().get(b"refs/heads/main".as_slice()),
            Some(&target)
        );
    }
    }
    Ok(())
}

#[tokio::test]
async fn streaming_receive_reproves_completed_input_on_the_one_expired_view_retry() -> TestResult {
    for policy in [crate::ReceivePolicy::default(), crate::ReceivePolicy::AllowNonFastForward] {
        receive_expired_view_kind(policy, true).await?;
    }
    Ok(())
}

#[tokio::test]
async fn streaming_receive_verifies_an_eight_mib_blob_with_sixteen_mib_admission() -> TestResult {
    let format = ObjectFormat::Sha256;
    let mut random = 7_u64;
    let mut contents = vec![0; 8 * 1024 * 1024];
    for byte in &mut contents {
        random ^= random << 13;
        random ^= random >> 7;
        random ^= random << 17;
        *byte = u8::try_from(random & 255)?;
    }
    let fixture = fixture(format, &contents)?;
    drop(contents);
    let pack = fs::read(&fixture.pack)?;
    assert!(pack.len() > 8 * 1024 * 1024);
    // Keep the unrelated head/commit decoder allowance small so this test
    // isolates pack working memory rather than the default publication envelope.
    let backend = ValidatedBackend::new(std::sync::Arc::new(InMemory::new()), StorePath::from("streamed-large")).await?;
    let log = Log::open(&backend, &LogId::new("streamed-large")?, Options {
        max_head_bytes: 16 * 1024, max_commit_bytes: 64 * 1024, ..Options::default()
    }).await?;
    let input = receive_input(format, &[RefUpdate::new("refs/heads/main", None, Some(fixture.target))?], &pack, true);
    drop(pack);
    let pool = Pool::new(16 * 1024 * 1024);
    let repository = Repository::open_with_pool(&log, format, &pool).await?;
    let operation = repository.operation.clone();
    let prepared = repository.prepare_receive_stream(TransactionId::new(), receive_frames(&input, 64 * 1024)).await?;
    assert!(matches!(prepared.publish_receive().await?.0, object_log::Resolution::Committed(_)));
    assert_eq!(operation.live_bytes(), 0);
    assert_eq!(cold_checked(&log, format).await?.refs().get(b"refs/heads/main".as_slice()), Some(&fixture.target));
    Ok(())
}
