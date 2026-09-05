#[tokio::test]
async fn default_branch_survives_unborn_push_checkpoint_delete_and_cold_restore() -> TestResult {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        for branch in ["refs/heads/main", "refs/heads/master", "refs/heads/trunk"] {
            let fixture = fixture(format, b"configured default branch")?;
            let (log, _, _) = test_log("default-branch").await?;
            assert!(matches!(common_open(&log, format).await?.set_default_branch(
                TransactionId::new(), b"refs/heads/main", branch.as_bytes(),
            ).await?, CommitStatus::Committed(_)));
            let repository = common_open(&log, format).await?;
            assert_eq!(repository.default_branch(), branch.as_bytes());
            let reply = repository.upload_pack(upload_request(format, "ls-refs",
                &["unborn".into(), "symrefs".into(), "ref-prefix HEAD".into()])?).await?;
            assert!(String::from_utf8_lossy(&reply).contains(&format!("unborn HEAD symref-target:{branch}")));
            drop(reply);
            let prepared = common_open(&log, format).await?.prepare_receive(
                TransactionId::new(), receive_input(format,
                    &[RefUpdate::new(branch, None, Some(fixture.target))?],
                    &fs::read(&fixture.pack)?, true),
            ).await?;
            assert!(matches!(prepared.publish_receive().await?.0, object_log::Resolution::Committed(_)));
            let repository = cold_checked(&log, format).await?;
            assert_eq!(repository.default_branch(), branch.as_bytes());
            let reply = repository.upload_pack(upload_request(format, "ls-refs",
                &["symrefs".into(), "ref-prefix HEAD".into()])?).await?;
            assert!(String::from_utf8_lossy(&reply).contains(&format!("{} HEAD symref-target:{branch}", fixture.target)));
            drop(reply);
            assert!(matches!(common_open(&log, format).await?.checkpoint().await?, object_log::CheckpointStatus::Published(_)));
            let view = log.load().await?;
            let object_log::CollectionStart::Installed(fenced, _) = log.start_collection(&view).await? else {
                return Err("collection fence not installed".into());
            };
            log.resume_collection(&fenced).await?;
            assert_eq!(cold_checked(&log, format).await?.default_branch(), branch.as_bytes());
            let prepared = common_open(&log, format).await?.prepare_receive(TransactionId::new(),
                receive_input(format, &[RefUpdate::new(branch, Some(fixture.target), None)?], &[], true)).await?;
            assert!(matches!(prepared.publish_receive().await?.0, object_log::Resolution::Committed(_)));
            let operation = Pool::new(crate::pack::budget::LIVE_BYTES).admit_maintenance()?;
            assert!(matches!(Repository::retain_packs(&log, format, &operation).await?, object_log::CheckpointStatus::Published(_)));
            let repository = cold_checked(&log, format).await?;
            assert!(repository.refs().is_empty());
            assert_eq!(repository.default_branch(), branch.as_bytes());
        }
    }
    Ok(())
}

#[tokio::test]
async fn default_branch_stale_and_concurrent_updates_preserve_one_winner() -> TestResult {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let (log, faults, _) = test_log("default-branch-conflict").await?;
        let stale = common_open(&log, format).await?;
        common_open(&log, format).await?.set_default_branch(TransactionId::new(), b"refs/heads/main", b"refs/heads/master").await?;
        assert!(matches!(stale.set_default_branch(TransactionId::new(), b"refs/heads/main", b"refs/heads/trunk").await?, CommitStatus::Conflict(_)));
        assert_eq!(common_open(&log, format).await?.default_branch(), b"refs/heads/master");
        let current = common_open(&log, format).await?;
        faults.reset();
        assert!(matches!(current.set_default_branch(TransactionId::new(), b"refs/heads/main", b"refs/heads/trunk").await, Err(Error::StaleReference)));
        assert_eq!(faults.metrics().operation(Operation::Put).requests, 0);
    }
    Ok(())
}

#[tokio::test]
async fn default_branch_pending_returns_exact_recoverable_candidate() -> TestResult {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        for phase in [FailurePhase::Before, FailurePhase::After] {
            let (log, faults, _) = test_log("default-branch-pending").await?;
            let repository = common_open(&log, format).await?;
            faults.reset();
            faults.schedule(Failure { operation: Operation::Put, occurrence: 2, phase });
            let CommitStatus::Pending(pending) = repository.set_default_branch(
                TransactionId::new(), b"refs/heads/main", b"refs/heads/trunk",
            ).await? else { return Err("expected pending metadata publication".into()); };
            let token = pending.recovery_token()?;
            assert!(matches!(log.resume(&token).await?, object_log::Resolution::Committed(_)));
            assert_eq!(common_open(&log, format).await?.default_branch(), b"refs/heads/trunk");
        }
    }
    Ok(())
}

#[tokio::test]
async fn default_branch_update_cannot_clobber_a_concurrent_ref_push() -> TestResult {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let fixture = fixture(format, b"default metadata concurrent push")?;
        let (log, _, _) = test_log("default-branch-ref-conflict").await?;
        publish_durable_pack(&log, &fixture, format).await?;
        let stale_metadata = common_open(&log, format).await?;
        let push = common_open(&log, format).await?.prepare_receive(TransactionId::new(), receive_input(format,
            &[RefUpdate::new("refs/heads/trunk", None, Some(fixture.target))?], &empty_pack(format)?, true)).await?;
        assert!(matches!(push.publish_receive().await?.0, object_log::Resolution::Committed(_)));
        assert!(matches!(stale_metadata.set_default_branch(TransactionId::new(), b"refs/heads/main", b"refs/heads/trunk").await?, CommitStatus::Conflict(_)));
        let fresh = cold_checked(&log, format).await?;
        assert_eq!(fresh.default_branch(), b"refs/heads/main");
        assert_eq!(fresh.refs().get(b"refs/heads/trunk".as_slice()), Some(&fixture.target));
        fresh.set_default_branch(TransactionId::new(), b"refs/heads/main", b"refs/heads/trunk").await?;
        assert_eq!(cold_checked(&log, format).await?.default_branch(), b"refs/heads/trunk");
    }
    Ok(())
}

#[tokio::test]
async fn prepared_legacy_ref_push_conflicts_with_metadata_upgrade() -> TestResult {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let fixture = fixture(format, b"legacy writer metadata conflict")?;
        let (log, _, _) = test_log("default-branch-legacy-conflict").await?;
        publish_durable_pack(&log, &fixture, format).await?;
        let legacy = common_open(&log, format).await?.prepare_receive(TransactionId::new(), receive_input(format,
            &[RefUpdate::new("refs/tags/stale", None, Some(fixture.target))?], &empty_pack(format)?, true)).await?;
        common_open(&log, format).await?.set_default_branch(TransactionId::new(), b"refs/heads/main", b"refs/heads/trunk").await?;
        assert!(matches!(legacy.publish_receive().await?.0, object_log::Resolution::NotCommitted(_)));
        let fresh = cold_checked(&log, format).await?;
        assert_eq!(fresh.default_branch(), b"refs/heads/trunk");
        assert!(!fresh.refs().contains_key(b"refs/tags/stale".as_slice()));
    }
    Ok(())
}
