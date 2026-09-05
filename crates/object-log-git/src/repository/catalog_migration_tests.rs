
#[tokio::test]
async fn catalog_migration_preserves_both_hashes_metadata_and_cold_checkpoint() -> TestResult {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        for upgraded in [false, true] {
            let fixture = fixture(format, b"migration")?;
            let (log, faults, backend) = test_log("catalog-migration").await?;
            publish_durable_pack(&log, &fixture, format).await?;
            if upgraded {
                common_open(&log, format).await?.set_default_branch(TransactionId::new(), b"refs/heads/main", b"refs/heads/trunk").await?;
            }
            let before = common_open(&log, format).await?;
            let refs = before.state.refs.clone();
            let branch = before.default_branch().to_vec();
            assert!(matches!(before.migrate_catalog_attempt(TransactionId::new()).await?, Some(CommitStatus::Committed(_))));
            let cold = Log::open_existing(&backend, &LogId::new("catalog-migration")?, log.options()).await?;
            let repository = common_open(&cold, format).await?;
            assert_eq!(repository.state.refs, refs);
            assert_eq!(repository.default_branch(), branch);
            assert!(repository.state.packs.is_empty());
            let crate::state::CatalogState::Tree(Some(root)) = &repository.state.catalog else { return Err("missing catalog root".into()); };
            let tree = crate::catalog_tree::CatalogTree::from_root(format, root.clone());
            assert!(tree.lookup(&cold, &repository.view, &repository.operation, fixture.target).await?.is_some());
            let record = Record::snapshot(format, refs.clone(), Vec::new())?
                .with_metadata(crate::format::Metadata::Snapshot(branch.clone()))?
                .with_catalog(crate::format::CatalogOperation::TreeSnapshot)?.encode()?;
            let root = root.clone();
            let through = repository.view.tail().last().ok_or("tail")?.clone();
            assert!(matches!(cold.publish_checkpoint(&repository.view, &through, record, vec![root]).await?, CheckpointStatus::Published(_)));
            drop(repository);
            let repository = common_open(&cold, format).await?;
            assert_eq!(repository.state.refs, refs);
            assert_eq!(repository.default_branch(), branch);
            faults.reset();
            assert!(repository.migrate_catalog_attempt(TransactionId::new()).await?.is_none());
            assert_eq!(faults.metrics().operation(Operation::Put).requests, 0);
            let view = cold.load().await?;
            assert!(matches!(cold.start_collection(&view).await?, object_log::CollectionStart::Installed(..)));
            let view = cold.load().await?;
            assert!(matches!(cold.resume_collection(&view).await?, object_log::CollectionFinish::Complete(..)));
            let repository = common_open(&cold, format).await?;
            let crate::state::CatalogState::Tree(Some(root)) = &repository.state.catalog else { return Err("collected catalog root".into()); };
            let tree = crate::catalog_tree::CatalogTree::from_root(format, root.clone());
            let selected = tree.lookup(&cold, &repository.view, &repository.operation, fixture.target).await?.ok_or("collected selected object")?;
            let index = durable::SelectedIndex::load(&repository.operation, &cold, &repository.view, &selected.descriptor, &selected.root).await?;
            index.verify_position(fixture.target, selected.index)?;

        }
    }
    Ok(())
}

#[tokio::test]
async fn catalog_migration_conflict_does_not_rebase_and_empty_is_explicit() -> TestResult {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let (log, _, _) = test_log("migration-conflict").await?;
        let candidate = common_open(&log, format).await?;
        common_open(&log, format).await?.set_default_branch(TransactionId::new(), b"refs/heads/main", b"refs/heads/trunk").await?;
        assert!(matches!(candidate.migrate_catalog_attempt(TransactionId::new()).await?, Some(CommitStatus::Conflict(_))));
        let operation = Pool::new(crate::pack::budget::LIVE_BYTES).admit_maintenance()?;
        assert!(matches!(Repository::migrate_catalog_admitted(&log, format, TransactionId::new(), &operation).await?, Some(CommitStatus::Committed(_))));
        let repository = common_open(&log, format).await?;
        assert!(matches!(repository.state.catalog, crate::state::CatalogState::Tree(None)));
        assert_eq!(repository.default_branch(), b"refs/heads/trunk");
    }
    Ok(())
}

#[tokio::test]
async fn catalog_migration_pending_token_recovers_without_process_cache() -> TestResult {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        for phase in [FailurePhase::Before, FailurePhase::After] {
            let (log, faults, backend) = test_log("migration-pending").await?;
            let repository = common_open(&log, format).await?;
            faults.reset();
            faults.schedule(Failure { operation: Operation::Put, occurrence: 2, phase });
            let Some(CommitStatus::Pending(pending)) = repository.migrate_catalog_attempt(TransactionId::new()).await? else { return Err("expected pending migration".into()); };
            let token = pending.recovery_token()?;
            let cold = Log::open_existing(&backend, &LogId::new("migration-pending")?, log.options()).await?;
            assert!(matches!(cold.resume(&token).await?, object_log::Resolution::Committed(_)));
            assert!(matches!(common_open(&cold, format).await?.state.catalog, crate::state::CatalogState::Tree(None)));
        }
    }
    Ok(())
}

#[tokio::test]
async fn catalog_migration_rejects_invalid_transitions_before_mutation() -> TestResult {
    use object_log::Materializer;
    let format = ObjectFormat::Sha1;
    let fixture = fixture(format, b"mode")?;
    let (log, _, _) = test_log("migration-invalid").await?;
    publish_durable_pack(&log, &fixture, format).await?;
    let repository = common_open(&log, format).await?;
    let legacy = repository.state.clone();
    repository.migrate_catalog_attempt(TransactionId::new()).await?;
    let repository = common_open(&log, format).await?;
    let original = repository.state.clone();
    let crate::state::CatalogState::Tree(Some(root)) = &original.catalog else { return Err("root".into()); };
    let migrate = Record::migration(format, original.default_branch().to_vec())?.encode()?;
    let mut state = original.clone();
    assert!(Machine::new(format).apply(&mut state, &migrate, std::slice::from_ref(root)).is_err());
    assert_eq!(state.refs, original.refs);
    assert_eq!(state.default_branch, original.default_branch);
    let replacement = Record::metadata_update(format, original.default_branch().to_vec(), original.default_branch().to_vec())?
        .with_catalog(crate::format::CatalogOperation::Replace)?.encode()?;
    let mut state = legacy.clone();
    assert!(Machine::new(format).apply(&mut state, &replacement, std::slice::from_ref(root)).is_err());
    assert_eq!(state.refs, legacy.refs);
    assert!(matches!(state.catalog, crate::state::CatalogState::Legacy));
    let mut state = original.clone();
    assert!(Machine::new(format).apply(&mut state, &replacement, &[]).is_err());
    assert_eq!(state.refs, original.refs);
    let mut state = legacy.clone();
    assert!(Machine::new(format).apply(&mut state, &migrate, &[root.clone(), root.clone()]).is_err());
    assert_eq!(state.refs, legacy.refs);
    let blob = log.put_object(&repository.view, Bytes::from_static(b"not a catalog")).await?;
    assert!(Machine::new(format).apply(&mut state, &migrate, &[blob]).is_err());
    assert_eq!(state.refs, legacy.refs);
    Ok(())
}

#[tokio::test]
async fn catalog_migration_index_failure_does_not_publish() -> TestResult {
    let format = ObjectFormat::Sha256;
    let fixture = fixture(format, b"read-failure")?;
    let (log, faults, _) = test_log("migration-read-failure").await?;
    publish_durable_pack(&log, &fixture, format).await?;
    let repository = common_open(&log, format).await?;
    let before = repository.view.tail().len();
    faults.reset();
    faults.schedule(Failure { operation: Operation::Get, occurrence: 1, phase: FailurePhase::Before });
    assert!(repository.migrate_catalog_attempt(TransactionId::new()).await.is_err());
    assert_eq!(faults.metrics().operation(Operation::Put).requests, 0);
    assert_eq!(log.load().await?.tail().len(), before);
    assert!(matches!(common_open(&log, format).await?.state.catalog, crate::state::CatalogState::Legacy));
    Ok(())
}
