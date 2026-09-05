
#[tokio::test]
async fn tree_receive_reuses_small_identical_pack_without_staging() -> TestResult {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let fixture = fixture(format, b"tree duplicate")?;
        let (log, faults, _) = test_log("tree-duplicate").await?;
        publish_durable_pack(&log, &fixture, format).await?;
        common_open(&log, format).await?.migrate_catalog_attempt(TransactionId::new()).await?;
        let repository = common_open(&log, format).await?;
        let input = receive_input(format, &[RefUpdate::new("refs/tags/restored", None, Some(fixture.target))?], &fs::read(&fixture.pack)?, true);
        faults.reset();
        let push = repository.prepare_receive(TransactionId::new(), input).await?;
        assert_eq!(faults.metrics().operation(Operation::Put).requests, 0);
        assert!(matches!(push.publish_receive().await?.0, object_log::Resolution::Committed(_)));
        assert_eq!(common_open(&log, format).await?.state.refs.get(b"refs/tags/restored".as_slice()), Some(&fixture.target));
    }
    Ok(())
}

#[tokio::test]
async fn tree_resend_after_multi_leaf_pruning_checkpoints_and_cold_fetches() -> TestResult {
    fn many(mut fixture: Fixture, format: ObjectFormat, seed: &str) -> TestResult<Fixture> {
        let work = fixture.directory.path().join("source");
        for index in 0..130 { fs::write(work.join(format!("item-{index}")), format!("{seed}-{index}"))?; }
        command(Some(&work), &["add", "."])?;
        command(Some(&work), &["commit", "--quiet", "-m", "many"])?;
        fixture.target = ObjectId::parse(format, output(Some(&work), &["rev-parse", "HEAD"])?.trim())?;
        fs::write(&fixture.pack, command_output(Some(&work), &["pack-objects", "--all", "--stdout"])?.stdout)?;
        Ok(fixture)
    }
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let first = many(fixture(format, b"first")?, format, "first")?;
        let other = many(fixture(format, b"other")?, format, "other")?;
        let keep = ObjectId::parse(format, output(Some(&first.directory.path().join("source")), &["rev-parse", "HEAD:item-0"])?.trim())?;
        let (log, _, backend) = test_log("tree-mixed-roots").await?;
        publish_durable_pack(&log, &first, format).await?;
        let input = receive_input(format, &[RefUpdate::new("refs/heads/other", None, Some(other.target))?], &fs::read(&other.pack)?, true);
        common_open(&log, format).await?.prepare_receive(TransactionId::new(), input).await?.publish_receive().await?;
        common_open(&log, format).await?.migrate_catalog_attempt(TransactionId::new()).await?;
        let input = receive_input(format, &[
            RefUpdate::new("refs/heads/main", Some(first.target), None)?,
            RefUpdate::new("refs/tags/retained", None, Some(keep))?,
        ], &empty_pack(format)?, true);
        common_open(&log, format).await?.prepare_receive(TransactionId::new(), input).await?.publish_receive().await?;
        assert!(matches!(common_open(&log, format).await?.checkpoint().await?, CheckpointStatus::Published(_)));
        let repository = common_open(&log, format).await?;
        let crate::state::CatalogState::Tree(Some(root)) = &repository.state.catalog else { return Err("tree".into()); };
        let node = log.read_node(&repository.view, root.reference()).await?;
        let mut decoder = minicbor::Decoder::new(node.payload());
        decoder.array()?; decoder.u8()?; decoder.u8()?;
        assert!(decoder.u8()? > 0, "pruned catalog must retain multiple leaves");
        drop(repository);
        let input = receive_input(format, &[RefUpdate::new("refs/heads/main", None, Some(first.target))?], &fs::read(&first.pack)?, true);
        common_open(&log, format).await?.prepare_receive(TransactionId::new(), input).await?.publish_receive().await?;
        assert!(matches!(common_open(&log, format).await?.checkpoint().await?, CheckpointStatus::Published(_)));
        let cold = Log::open_existing(&backend, &LogId::new("tree-mixed-roots")?, log.options()).await?;
        let repository = common_open(&cold, format).await?;
        let pack = repository.fetch_pack(&[first.target], &[], false).await?;
        let path = first.directory.path().join("fetched.pack");
        fs::write(&path, pack)?;
        command(Some(&first.directory.path().join("source")), &["index-pack", "--strict", path.to_str().ok_or("pack path")?])?;
    }
    Ok(())
}
