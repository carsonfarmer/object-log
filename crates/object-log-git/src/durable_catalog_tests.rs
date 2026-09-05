// Included in durable::tests to reuse the independent Git pack fixture/oracle.

async fn tree_fixture(
    base_log: &Log,
    view: &View,
    format: ObjectFormat,
    count: usize,
    wrong_position: bool,
) -> TestResult<(crate::catalog_tree::CatalogTree, Vec<(ObjectId, Vec<u8>)>)> {
    let operation = test_operation();
    let log = base_log.with_request_guard(Arc::new(operation.clone()));
    let mut tree = crate::catalog_tree::CatalogTree::empty(format);
    let mut objects = Vec::new();
    for number in 0..count {
        let fixture = pack_fixture(format, vec![vec![u8::try_from(number)?; 64]], false, false)?;
        let id = fixture.objects[0].0;
        objects.extend(fixture.objects);
        let (descriptor, root) = super::stage(&operation, &log, view, fixture.normalized).await?;
        tree = tree.insert_pack(&log, view, &operation, descriptor, root,
            &[(id, u32::from(wrong_position))]).await?;
    }
    Ok((tree, objects))
}

#[tokio::test]
async fn tree_reader_only_loads_selected_indexes_and_survives_slot_reuse() -> TestResult {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let store = FaultStore::from_arc(Arc::new(InMemory::new()));
        let (base_log, view) = open(store.clone(), "tree-reader").await?;
        let (tree, objects) = tree_fixture(&base_log, &view, format, 12, false).await?;
        let operation = test_operation();
        let log = base_log.with_request_guard(Arc::new(operation.clone()));
        let catalog = Catalog::from_tree(&operation, format, tree.root().cloned())?;
        let mut reader = Reader::new(&log, &view, &catalog);
        store.reset();
        let missing = ObjectId::from_bytes(format, &vec![0xfe; format.digest_len()])?;
        assert!(!reader.contains(missing).await?);
        assert_eq!(store.metrics().operation(StoreOperation::Get).requests, 1);
        store.reset();
        assert!(reader.contains(objects[0].0).await?);
        assert_eq!(store.metrics().operation(StoreOperation::Get).requests, 1);
        store.reset();
        assert!(reader.selected_location(objects[0].0).await?.is_some());
        assert_eq!(store.metrics().operation(StoreOperation::Get).requests, 0);
        for (id, data) in &objects {
            let object = reader.find(*id).await?.ok_or("tree object missing")?;
            assert_eq!(&object.data[..], data);
        }
        // Twelve packs exceed the eight selected slots. Cached chunks must not
        // survive a slot's replacement, and planned fetch locations must be refreshed.
        for (id, data) in objects.iter().rev() {
            let object = reader.find(*id).await?.ok_or("revisited object missing")?;
            assert_eq!(&object.data[..], data);
        }
        let ids = objects.iter().map(|(id, _)| *id).collect::<Vec<_>>();
        let output = reader.fetch_pack(&ids).await?;
        verify_fetch_pack(&output, format, &ids)?;
        drop(output);
        drop(reader);
        drop(catalog);
        assert_eq!(operation.live_bytes(), 0);
    }
    Ok(())
}

#[tokio::test]
async fn tree_reader_cross_checks_positions_and_rejects_wrong_mode() -> TestResult {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let store = FaultStore::from_arc(Arc::new(InMemory::new()));
        let (base_log, view) = open(store.clone(), "tree-reader-position").await?;
        let (tree, objects) = tree_fixture(&base_log, &view, format, 1, true).await?;
        let operation = test_operation();
        let log = base_log.with_request_guard(Arc::new(operation.clone()));
        let catalog = Catalog::from_tree(&operation, format, tree.root().cloned())?;
        let mut reader = Reader::new(&log, &view, &catalog);
        store.reset();
        assert!(reader.contains(objects[0].0).await.is_err());
        // One tree node plus selected index, never a blob chunk.
        assert_eq!(store.metrics().operation(StoreOperation::Get).requests, 2);
        let legacy = super::load(&operation, &log, &view, format, &[]).await?;
        assert!(Reader::new(&log, &view, &legacy).selected_location(objects[0].0).await.is_err());
    }
    Ok(())
}

#[tokio::test]
async fn tree_reader_cancellation_releases_cache_and_does_not_read_ahead() -> TestResult {
    let store = FaultStore::from_arc(Arc::new(InMemory::new()));
    let (base_log, view) = open(store.clone(), "tree-reader-cancel").await?;
    let (tree, objects) = tree_fixture(&base_log, &view, ObjectFormat::Sha1, 1, false).await?;
    let operation = test_operation();
    let log = base_log.with_request_guard(Arc::new(operation.clone()));
    let catalog = Catalog::from_tree(&operation, ObjectFormat::Sha1, tree.root().cloned())?;
    let baseline = operation.live_bytes();
    let mut reader = Reader::new(&log, &view, &catalog);
    // Warm the catalog path with a miss, then cancel the selected-index GET.
    let missing = ObjectId::from_bytes(ObjectFormat::Sha1, &[0xfe; 20])?;
    assert!(!reader.contains(missing).await?);
    let mut pause = store.pause_next_get(FailurePhase::Before);
    let mut pending = Box::pin(reader.contains(objects[0].0));
    assert!(matches!(futures::poll!(&mut pending), std::task::Poll::Pending));
    assert!(pause.wait_until_entered().await);
    drop(pending);
    drop(reader);
    assert_eq!(operation.live_bytes(), baseline);
    Ok(())
}

#[tokio::test]
async fn tree_reader_evicts_under_state_pressure_but_never_replaces_pinned_indexes() -> TestResult {
    let store = FaultStore::from_arc(Arc::new(InMemory::new()));
    let (base_log, view) = open(store.clone(), "tree-reader-pressure").await?;
    let (tree, objects) = tree_fixture(&base_log, &view, ObjectFormat::Sha1, 2, false).await?;
    let operation = test_operation();
    let log = base_log.with_request_guard(Arc::new(operation.clone()));
    let catalog = Catalog::from_tree(&operation, ObjectFormat::Sha1, tree.root().cloned())?;
    let mut reader = Reader::new(&log, &view, &catalog);
    let selected = reader.selected_location(objects[0].0).await?.ok_or("missing selected root")?;
    let location = reader.location(objects[0].0).await?.ok_or("missing cache location")?;
    let pinned = reader.pack(location.pack);
    let bound = selected_index_bytes(&selected.descriptor, &selected.root)?;
    // All retained allocations so far are state reservations. Leave only half
    // an index's allowance: an unpinned cached index must be evicted to proceed.
    let pressure = operation.reserve_state(
        crate::pack::budget::STATE_BYTES - operation.live_bytes() - bound / 2,
    )?;
    store.reset();
    assert!(reader.contains(objects[1].0).await.is_err());
    assert_eq!(store.metrics().operation(StoreOperation::Get).requests, 0);
    drop(pinned);
    assert!(reader.contains(objects[1].0).await?);
    assert_eq!(store.metrics().operation(StoreOperation::Get).requests, 1);
    drop(pressure);
    for (id, data) in &objects {
        let object = reader.find(*id).await?.ok_or("object missing after pressure")?;
        assert_eq!(&object.data[..], data);
    }
    Ok(())
}

#[tokio::test]
async fn packed_entry_size_authenticates_geometry_without_blob_reads() -> TestResult {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let store = FaultStore::from_arc(Arc::new(InMemory::new()));
        let (base_log, view) = open(store.clone(), "entry-size").await?;
        let fixture = pack_fixture(format, vec![vec![b'x'; 1024]], false, false)?;
        let id = fixture.objects[0].0;
        let expected = fixture.normalized.bytes.len() - 12 - format.digest_len();
        let operation = test_operation();
        let log = base_log.with_request_guard(Arc::new(operation.clone()));
        let (descriptor, root) = super::stage(&operation, &log, &view, fixture.normalized).await?;
        let tree = crate::catalog_tree::CatalogTree::empty(format)
            .insert_pack(&log, &view, &operation, descriptor, root, &[(id, 0)]).await?;
        let catalog = Catalog::from_tree(&operation, format, tree.root().cloned())?;
        let mut reader = Reader::new(&log, &view, &catalog);
        store.reset();
        assert_eq!(reader.packed_entry_bytes(id).await?, Some(expected));
        assert_eq!(store.metrics().operation(StoreOperation::Get).requests, 2);
        assert_eq!(store.metrics().operation(StoreOperation::Put).requests, 0);
        let calls = operation.calls();
        assert_eq!(reader.packed_entry_bytes(id).await?, Some(expected));
        assert_eq!(operation.calls(), calls);
    }
    Ok(())
}
