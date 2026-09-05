async fn streamed_pack(reader: &mut Reader<'_>, ids: &[ObjectId]) -> TestResult<Vec<u8>> {
    let mut bytes = Vec::new();
    {
        let sink = futures::sink::unfold(&mut bytes, |bytes, frame: Bytes| async move {
            assert!(frame.len() <= 65536);
            bytes.extend_from_slice(&frame);
            Ok::<_, io::Error>(bytes)
        });
        reader.write_fetch(ids, &mut Box::pin(sink)).await?;
    }
    Ok(bytes)
}

#[tokio::test]
async fn streaming_fetch_preserves_or_expands_deltas_without_storage_writes() -> TestResult {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        for ofs in [false, true] {
            let store = FaultStore::from_arc(Arc::new(InMemory::new()));
            let (log, view) = open(store.clone(), "streamed-deltas").await?;
            let fixture = fixture(format, 10, ofs, false)?;
            let entries = indexed_entries(&fixture.normalized, format)?;
            let delta = entries.iter().find(|entry| entry.1.is_delta()).ok_or("no delta")?.0;
            let ids = fixture.objects.iter().map(|item| item.0).collect::<Vec<_>>();
            let (descriptor, root) = stage(&test_operation(), &log, &view, fixture.normalized).await?;
            let catalog = load_one(&log, &view, format, descriptor, &root).await?;
            let guarded = log.with_request_guard(Arc::new(catalog.operation.clone()));
            let mut reader = Reader::new(&guarded, &view, &catalog);
            store.reset();
            let pack = streamed_pack(&mut reader, &ids).await?;
            verify_fetch_pack(&pack, format, &ids)?;
            assert!(inspect_pack(&pack, format)?.iter().any(|entry| entry.0.is_delta()));
            let pack = streamed_pack(&mut reader, &[delta]).await?;
            verify_fetch_pack(&pack, format, &[delta])?;
            assert!(!inspect_pack(&pack, format)?[0].0.is_delta());
            assert_eq!(store.metrics().operation(StoreOperation::Put).requests, 0);
        }
    }
    Ok(())
}

#[tokio::test]
async fn streaming_fetch_large_entry_needs_only_one_output_frame() -> TestResult {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let store = FaultStore::from_arc(Arc::new(InMemory::new()));
        let (log, view) = open(store.clone(), "streamed-memory").await?;
        let fixture = fixture(format, 1, false, true)?;
        let id = fixture.objects[0].0;
        let expected = fixture.normalized.bytes.clone();
        let (descriptor, root) = stage(&test_operation(), &log, &view, fixture.normalized).await?;
        let catalog = load_one(&log, &view, format, descriptor, &root).await?;
        let guarded = log.with_request_guard(Arc::new(catalog.operation.clone()));
        let mut reader = Reader::new(&guarded, &view, &catalog);
        let range = catalog.packs[0].entry_range(0);
        reader.visit_range(0, range, |_| Ok(())).await?;
        let pressure = catalog.operation.reserve(LIVE_BYTES - catalog.operation.live_bytes() - 96 * 1024)?;
        store.reset();
        let output = streamed_pack(&mut reader, &[id]).await?;
        assert_eq!(output, expected);
        assert_eq!(store.metrics().total_requests(), 0);
        verify_fetch_pack(&output, format, &[id])?;
        drop(pressure);
    }
    Ok(())
}

#[tokio::test]
async fn streaming_fetch_crc_failure_omits_the_pack_digest() -> TestResult {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let store = FaultStore::from_arc(Arc::new(InMemory::new()));
        let (log, view) = open(store, "streamed-crc").await?;
        let fixture = fixture(format, 1, false, true)?;
        let id = fixture.objects[0].0;
        let expected = fixture.normalized.bytes.clone();
        let mut normalized = fixture.normalized;
        normalized.index[8 + 1024 + format.digest_len()] ^= 1;
        rehash_index(&mut normalized.index, format)?;
        let (descriptor, root) = stage(&test_operation(), &log, &view, normalized).await?;
        let catalog = load_one(&log, &view, format, descriptor, &root).await?;
        let guarded = log.with_request_guard(Arc::new(catalog.operation.clone()));
        let mut reader = Reader::new(&guarded, &view, &catalog);
        let mut bytes = Vec::new();
        {
            let sink = futures::sink::unfold(&mut bytes, |bytes, frame: Bytes| async move {
                bytes.extend_from_slice(&frame);
                Ok::<_, io::Error>(bytes)
            });
            assert!(reader.write_fetch(&[id], &mut Box::pin(sink)).await.is_err());
        }
        assert_eq!(bytes, expected[..expected.len() - format.digest_len()]);
    }
    Ok(())
}
