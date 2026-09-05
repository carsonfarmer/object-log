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

#[tokio::test]
async fn deepen_not_head_uses_persisted_default_with_partial_fetch() -> TestResult {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let mut fixture = fixture(format, b"default HEAD exclusion")?;
        let trunk = fixture.target;
        let source = fixture.directory.path().join("source");
        command(Some(&source), &["commit", "--quiet", "--allow-empty", "-m", "main tip"])?;
        fixture.target = ObjectId::parse(format, output(Some(&source), &["rev-parse", "HEAD"])?.trim())?;
        fs::write(&fixture.pack, command_output(Some(&source), &["pack-objects", "--all", "--stdout"])?.stdout)?;
        let (log, _, _) = test_log("default-head-exclusion").await?;
        publish_durable_pack(&log, &fixture, format).await?;
        let push = common_open(&log, format).await?.prepare_receive(TransactionId::new(), receive_input(format,
            &[RefUpdate::new("refs/heads/trunk", None, Some(trunk))?], &empty_pack(format)?, true)).await?;
        assert!(matches!(push.publish_receive().await?.0, object_log::Resolution::Committed(_)));
        common_open(&log, format).await?.set_default_branch(TransactionId::new(), b"refs/heads/main", b"refs/heads/trunk").await?;
        assert!(matches!(common_open(&log, format).await?.checkpoint().await?, object_log::CheckpointStatus::Published(_)));
        for filtered in [false, true] {
            let mut arguments = vec![format!("want {}", fixture.target), "deepen-not HEAD".into()];
            if filtered { arguments.push("filter blob:none".into()); }
            arguments.push("done".into());
            let cold = common_open(&log, format).await?;
            assert_eq!(cold.excluded_ref(b"HEAD")?, trunk);
            assert_ne!(trunk, fixture.target);
            let head_reply = cold.upload_pack(upload_request(format, "fetch", &arguments)?).await?;
            arguments[1] = "deepen-not refs/heads/trunk".into();
            let explicit_reply = common_open(&log, format).await?.upload_pack(upload_request(format, "fetch", &arguments)?).await?;
            assert_eq!(head_reply, explicit_reply);
            assert!(String::from_utf8_lossy(&head_reply).contains(&format!("shallow {}", fixture.target)));
        }
    }
    Ok(())
}

#[tokio::test]
async fn guarded_default_branch_matches_requests_and_keeps_pending_policy() -> TestResult {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        for phase in [None, Some(FailurePhase::Before), Some(FailurePhase::After)] {
            let (log, faults, _) = test_log("guarded-default-parity").await?;
            // One open GET and exactly two publication PUTs. Later recovery
            // must still obey this caller's exhausted policy.
            let caller = CallerGuard::new(3);
            let repository = common_open(&log.with_request_guard(caller.clone()), format).await?;
            let operation = repository.operation.clone();
            let continuation = repository.log.clone();
            let before = operation.calls();
            faults.reset();
            if let Some(phase) = phase {
                faults.schedule(Failure {operation: Operation::Put, occurrence: 2, phase});
            }
            let status = repository.set_default_branch(TransactionId::new(), b"refs/heads/main", b"refs/heads/trunk").await?;
            assert_eq!(operation.calls() - before, 2);
            assert_eq!(operation.calls() - before, usize::try_from(faults.metrics().total_requests())?);
            assert_eq!(caller.calls(), operation.calls());
            assert_eq!(operation.live_bytes(), 0);
            match (phase, status) {
                (None, CommitStatus::Committed(_)) => {}
                (Some(_), CommitStatus::Pending(pending)) => {
                    let token = pending.recovery_token()?;
                    faults.reset();
                    let object_log::Resolution::StillPending(pending) = continuation.resolve(pending).await? else {
                        return Err("caller denial discarded pending setter evidence".into());
                    };
                    assert_eq!(pending.recovery_token()?, token);
                    assert_eq!(faults.metrics().total_requests(), 0);
                    assert_eq!(operation.calls(), 3);
                    assert!(matches!(log.resume(&token).await?, object_log::Resolution::Committed(_)));
                }
                _ => return Err("setter changed its publication outcome".into()),
            }
            assert_eq!(common_open(&log, format).await?.default_branch(), b"refs/heads/trunk");
        }
    }
    Ok(())
}

#[tokio::test]
async fn guarded_default_branch_denial_before_create_or_cas_never_publishes() -> TestResult {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        for limit in [1, 2] {
            let (log, faults, _) = test_log("guarded-default-denial").await?;
            let caller = CallerGuard::new(limit);
            let repository = common_open(&log.with_request_guard(caller.clone()), format).await?;
            let operation = repository.operation.clone();
            faults.reset();
            assert!(matches!(repository.set_default_branch(TransactionId::new(), b"refs/heads/main", b"refs/heads/trunk").await,
                Err(Error::ObjectLog(object_log::Error::RequestDenied))));
            assert_eq!(faults.metrics().total_requests(), u64::try_from(limit - 1)?);
            assert_eq!(operation.calls(), limit);
            assert_eq!(caller.calls(), limit);
            assert_eq!(operation.live_bytes(), 0);
            assert!(log.load().await?.tail().is_empty());
            assert_eq!(common_open(&log, format).await?.default_branch(), b"refs/heads/main");
        }
    }
    Ok(())
}
