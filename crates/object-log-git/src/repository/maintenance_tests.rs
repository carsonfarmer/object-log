async fn fill_metadata_tail(
    log: &Log,
    format: ObjectFormat,
    target: ObjectId,
    count: usize,
) -> TestResult {
    for index in 1..count {
        let (old, new) = if index % 2 == 1 {
            (None, Some(target))
        } else {
            (Some(target), None)
        };
        let record = Machine::new(format).transaction(
            vec![RefUpdate::new("refs/tags/changing", old, new)?],
            vec![],
        )?;
        let view = log.load().await?;
        let prepared = log.prepare(&view, TransactionId::new(), record, Bytes::new(), vec![])?;
        assert!(matches!(
            log.commit(prepared).await?,
            CommitStatus::Committed(_)
        ));
    }
    Ok(())
}

#[tokio::test]
async fn conservative_checkpoint_recovers_1024_transaction_tail_for_both_hashes() -> TestResult {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let fixture = fixture(format, b"conservative metadata checkpoint")?;
        let (log, faults, _) = test_log("conservative-1024").await?;
        publish_durable_pack(&log, &fixture, format).await?;
        fill_metadata_tail(&log, format, fixture.target, 1024).await?;
        assert_eq!(log.load().await?.tail().len(), 1024);
        faults.reset();
        assert!(
            matches!(common_open(&log, format).await, Err(Error::InvalidPack(reason)) if reason == "object-log call limit exceeded")
        );
        assert_eq!(faults.metrics().operation(Operation::Get).requests, 1);
        assert_eq!(faults.metrics().operation(Operation::Put).requests, 0);
        faults.reset();
        let operation = Pool::new(crate::pack::budget::LIVE_BYTES).admit_maintenance()?;
        let object_log::CheckpointStatus::Published(view) =
            Repository::retain_packs(&log, format, &operation).await?
        else {
            return Err("maintenance checkpoint was not published".into());
        };
        assert!(view.tail().is_empty());
        // Head + all 1,024 commits twice + possible classification reads +
        // checkpoint/head publication and possible conflicting-head refresh.
        assert_eq!(operation.calls(), 2069);
        assert_eq!(faults.metrics().operation(Operation::Get).requests, 2049);
        assert_eq!(faults.metrics().operation(Operation::Put).requests, 2);
        assert_eq!(operation.live_bytes(), 0);
        println!(
            "{format:?}: 1,024-entry maintenance checkpoint, {} charged calls, {} physical requests, {} downloaded bytes, {} uploaded bytes",
            operation.calls(),
            faults.metrics().total_requests(),
            faults.metrics().downloaded_bytes(),
            faults.metrics().uploaded_bytes()
        );
        let recovered = cold_checked(&log, format).await?;
        assert_eq!(
            recovered.refs().get(b"refs/heads/main".as_slice()),
            Some(&fixture.target)
        );
        assert_eq!(recovered.state.packs.len(), 1);
        let prepared = recovered
            .prepare_receive(
                TransactionId::new(),
                receive_input(
                    format,
                    &[RefUpdate::new(
                        "refs/tags/after-maintenance",
                        None,
                        Some(fixture.target),
                    )?],
                    &empty_pack(format)?,
                    true,
                ),
            )
            .await?;
        assert!(matches!(
            prepared.publish_receive().await?.0,
            object_log::Resolution::Committed(_)
        ));
        assert!(
            cold_checked(&log, format)
                .await?
                .refs()
                .contains_key(b"refs/tags/after-maintenance".as_slice())
        );
    }
    Ok(())
}

#[tokio::test]
async fn conservative_checkpoint_late_invalid_metadata_never_publishes() -> TestResult {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let fixture = fixture(format, b"late corruption")?;
        let (log, faults, _) = test_log("conservative-late-corruption").await?;
        publish_durable_pack(&log, &fixture, format).await?;
        fill_metadata_tail(&log, format, fixture.target, 1023).await?;
        let view = log.load().await?;
        let invalid = log.prepare(
            &view,
            TransactionId::new(),
            Bytes::from_static(b"invalid Git record"),
            Bytes::new(),
            vec![],
        )?;
        assert!(matches!(
            log.commit(invalid).await?,
            CommitStatus::Committed(_)
        ));
        faults.reset();
        let operation = Pool::new(crate::pack::budget::LIVE_BYTES).admit_maintenance()?;
        assert!(matches!(
            Repository::retain_packs(&log, format, &operation).await,
            Err(Error::InvalidRecord(_))
        ));
        assert_eq!(faults.metrics().operation(Operation::Get).requests, 1025);
        assert_eq!(faults.metrics().operation(Operation::Put).requests, 0);
        assert_eq!(operation.live_bytes(), 0);
    }
    Ok(())
}

#[tokio::test]
async fn conservative_checkpoint_pending_preserves_exact_core_outcome() -> TestResult {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        for phase in [FailurePhase::Before, FailurePhase::After] {
            let fixture = fixture(format, b"pending conservative checkpoint")?;
            let (log, faults, _) = test_log("conservative-pending").await?;
            publish_durable_pack(&log, &fixture, format).await?;
            faults.reset();
            faults.schedule(Failure {
                operation: Operation::Put,
                occurrence: 2,
                phase,
            });
            let operation = Pool::new(crate::pack::budget::LIVE_BYTES).admit_maintenance()?;
            let object_log::CheckpointStatus::Pending(pending) =
                Repository::retain_packs(&log, format, &operation).await?
            else {
                return Err("expected uncertain checkpoint".into());
            };
            assert_eq!(operation.live_bytes(), 0);
            // Explicit caller resolution on a one-record fixture; the maintenance
            // entrypoint itself never enters the core's resolution reread path.
            assert!(matches!(
                log.resolve_checkpoint(pending).await?,
                object_log::CheckpointResolution::Published(_)
            ));
            assert_eq!(
                cold_checked(&log, format)
                    .await?
                    .refs()
                    .get(b"refs/heads/main".as_slice()),
                Some(&fixture.target)
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn conservative_checkpoint_conflict_preserves_the_concurrent_ref_update() -> TestResult {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let fixture = fixture(format, b"checkpoint race")?;
        let (log, faults, _) = test_log("conservative-conflict").await?;
        publish_durable_pack(&log, &fixture, format).await?;
        faults.reset();
        let mut pause = faults.pause_next_put(FailurePhase::Before);
        let background = log.clone();
        let task = tokio::spawn(async move {
            let operation = Pool::new(crate::pack::budget::LIVE_BYTES).admit_maintenance()?;
            Repository::retain_packs(&background, format, &operation).await
        });
        assert!(pause.wait_until_entered().await);
        let view = log.load().await?;
        let record = Machine::new(format).transaction(
            vec![RefUpdate::new(
                "refs/tags/concurrent",
                None,
                Some(fixture.target),
            )?],
            vec![],
        )?;
        let prepared = log.prepare(&view, TransactionId::new(), record, Bytes::new(), vec![])?;
        assert!(matches!(
            log.commit(prepared).await?,
            CommitStatus::Committed(_)
        ));
        assert!(pause.release());
        assert!(matches!(
            task.await??,
            object_log::CheckpointStatus::Conflict(_)
        ));
        let recovered = cold_checked(&log, format).await?;
        assert_eq!(
            recovered.refs().get(b"refs/tags/concurrent".as_slice()),
            Some(&fixture.target)
        );
        assert_eq!(
            recovered.refs().get(b"refs/heads/main".as_slice()),
            Some(&fixture.target)
        );
    }
    Ok(())
}

#[tokio::test]
async fn conservative_checkpoint_keeps_unreachable_packs_without_catalog_reads() -> TestResult {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let live = fixture(format, b"live")?;
        let dead = fixture(format, b"unreachable but retained")?;
        let (log, faults, _) = test_log("conservative-packs").await?;
        publish_durable_pack(&log, &live, format).await?;
        let prepared = common_open(&log, format)
            .await?
            .prepare_receive(
                TransactionId::new(),
                receive_input(
                    format,
                    &[RefUpdate::new("refs/tags/dead", None, Some(dead.target))?],
                    &fs::read(&dead.pack)?,
                    true,
                ),
            )
            .await?;
        assert!(matches!(
            prepared.publish_receive().await?.0,
            object_log::Resolution::Committed(_)
        ));
        let prepared = common_open(&log, format)
            .await?
            .prepare_receive(
                TransactionId::new(),
                receive_input(
                    format,
                    &[RefUpdate::new("refs/tags/dead", Some(dead.target), None)?],
                    &[],
                    true,
                ),
            )
            .await?;
        assert!(matches!(
            prepared.publish_receive().await?.0,
            object_log::Resolution::Committed(_)
        ));
        faults.reset();
        let operation = Pool::new(crate::pack::budget::LIVE_BYTES).admit_maintenance()?;
        assert!(matches!(
            Repository::retain_packs(&log, format, &operation).await?,
            object_log::CheckpointStatus::Published(_)
        ));
        assert_eq!(
            faults.metrics().operation(Operation::Get).requests,
            7,
            "one head and three commits read twice, no catalog or pack reads"
        );
        let recovered = common_open(&log, format).await?;
        assert_eq!(recovered.state.packs.len(), 2);
        assert_eq!(recovered.refs().len(), 1);
        assert_eq!(
            recovered.refs().get(b"refs/heads/main".as_slice()),
            Some(&live.target)
        );
    }
    Ok(())
}

#[tokio::test]
async fn conservative_checkpoint_expiry_retry_retains_cumulative_calls() -> TestResult {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let fixture = fixture(format, b"checkpoint expiry")?;
        let (log, faults, _) = test_log("conservative-expiry").await?;
        publish_durable_pack(&log, &fixture, format).await?;
        faults.reset();
        let mut pause = faults.pause_get_at(2, FailurePhase::Before);
        let background = log.clone();
        let operation = Pool::new(crate::pack::budget::LIVE_BYTES).admit_maintenance()?;
        let background_operation = operation.clone();
        let task = tokio::spawn(async move {
            Repository::retain_packs(&background, format, &background_operation).await
        });
        assert!(pause.wait_until_entered().await);
        let object_log::CheckpointStatus::Published(view) =
            common_open(&log, format).await?.checkpoint().await?
        else {
            return Err("racing checkpoint did not publish".into());
        };
        let object_log::CollectionStart::Installed(fenced, _) = log.start_collection(&view).await?
        else {
            return Err("collection not installed".into());
        };
        assert!(matches!(
            log.resume_collection(&fenced).await?,
            object_log::CollectionFinish::Complete(..)
        ));
        assert!(pause.release());
        assert!(matches!(
            task.await??,
            object_log::CheckpointStatus::Published(_)
        ));
        assert!(
            operation.retry().is_err(),
            "expired materialization consumed its sole retry"
        );
        assert!(
            operation.calls() > 3,
            "the old read and fresh materialization stay charged"
        );
        assert_eq!(operation.live_bytes(), 0);
        assert_eq!(
            cold_checked(&log, format)
                .await?
                .refs()
                .get(b"refs/heads/main".as_slice()),
            Some(&fixture.target)
        );
    }
    Ok(())
}

#[tokio::test]
async fn bounded_materialization_accepts_history_larger_than_the_decoder_window() -> TestResult {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let fixture = fixture(format, b"bounded record window")?;
        let (log, _, _) = test_log("bounded-record-window").await?;
        publish_durable_pack(&log, &fixture, format).await?;
        let names = (0..8)
            .map(|index| format!("refs/tags/{index}-{}", "x".repeat(220)))
            .collect::<Vec<_>>();
        for index in 1..384 {
            let (old, new) = if index % 2 == 1 {
                (None, Some(fixture.target))
            } else {
                (Some(fixture.target), None)
            };
            let record = Machine::new(format).transaction(
                names
                    .iter()
                    .map(|name| RefUpdate::new(name, old, new))
                    .collect::<Result<Vec<_>, _>>()?,
                vec![],
            )?;
            let view = log.load().await?;
            let prepared =
                log.prepare(&view, TransactionId::new(), record, Bytes::new(), vec![])?;
            assert!(matches!(
                log.commit(prepared).await?,
                CommitStatus::Committed(_)
            ));
        }
        let view = log.load().await?;
        let total = view
            .tail()
            .iter()
            .map(object_log::CommitRef::len)
            .sum::<u64>();
        assert!(total * 128 > u64::try_from(crate::pack::budget::LIVE_BYTES)?);
        assert!(log.materialization_read_bound(&view)? * 128 < crate::pack::budget::LIVE_BYTES);
        let repository = common_open(&log, format).await?;
        assert_eq!(repository.refs().len(), 9);
        for name in &names {
            assert_eq!(
                repository.refs().get(name.as_bytes()),
                Some(&fixture.target)
            );
        }
        drop(repository);
        assert_eq!(
            cold_checked(&log, format)
                .await?
                .refs()
                .get(b"refs/heads/main".as_slice()),
            Some(&fixture.target)
        );
    }
    Ok(())
}
