use object_store::{ObjectStore, local::LocalFileSystem};

#[tokio::test]
async fn memory_open_with_view_reads_one_head() -> Result<(), Box<dyn std::error::Error>> {
    one_head_read(Arc::new(InMemory::new())).await
}

#[tokio::test]
async fn filesystem_open_with_view_reads_one_head() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    // This read-only test does not qualify the filesystem's unsupported conditional update.
    one_head_read(Arc::new(LocalFileSystem::new_with_prefix(
        directory.path(),
    )?))
    .await
}

async fn one_head_read(store: Arc<dyn ObjectStore>) -> Result<(), Box<dyn std::error::Error>> {
    let faults = FaultStore::new(store);
    let backend =
        ValidatedBackend::assume_validated(Arc::new(faults.clone()), Path::from("open-view"));
    let id = LogId::new("existing")?;
    let initial = Log::open(&backend, &id, Options::default()).await?;
    let expected = initial.load().await?;

    faults.reset();
    let (opened, view) = Log::open_existing_with_view(&backend, &id, Options::default()).await?;
    assert_eq!(faults.metrics().operation(Operation::Get).requests, 1);
    assert_eq!(faults.metrics().total_requests(), 1);
    assert_eq!(view.storage_version(), expected.storage_version());
    assert_eq!(view.generation(), expected.generation());
    assert_eq!(view.head().incarnation, expected.head().incarnation);
    assert_eq!(view.tail(), expected.tail());
    assert!(opened.read_tail(&view).await?.is_empty());
    assert_eq!(faults.metrics().total_requests(), 1);
    Ok(())
}

#[tokio::test]
async fn open_with_view_keeps_exact_recovery_and_later_loads_fresh()
-> Result<(), Box<dyn std::error::Error>> {
    let faults = FaultStore::new(InMemory::new());
    let backend =
        ValidatedBackend::new(Arc::new(faults.clone()), Path::from("fresh-open-view")).await?;
    let id = LogId::new("existing")?;
    let writer = Log::open(&backend, &id, Options::default()).await?;
    let (reader, old) = Log::open_existing_with_view(&backend, &id, Options::default()).await?;
    let operation = Bytes::from_static(b"published after open");
    let candidate = writer.prepare(
        &old,
        TransactionId::new(),
        operation.clone(),
        Bytes::new(),
        Vec::new(),
    )?;
    let CommitStatus::Committed(published) = writer.commit(candidate).await? else {
        return Err("setup commit did not publish".into());
    };

    faults.reset();
    let fresh = reader.load().await?;
    assert_eq!(faults.metrics().total_requests(), 1);
    assert_eq!(fresh.storage_version(), published.storage_version());
    assert_eq!(old.generation(), 0);
    assert!(reader.read_tail(&old).await?.is_empty());

    faults.reset();
    let (reopened, current) =
        Log::open_existing_with_view(&backend, &id, Options::default()).await?;
    assert_eq!(faults.metrics().operation(Operation::Get).requests, 1);
    assert_eq!(faults.metrics().total_requests(), 1);
    assert_eq!(current.storage_version(), published.storage_version());
    assert_eq!(current.tail(), published.tail());
    assert_eq!(reopened.read_tail(&current).await?[0].operation, operation);
    Ok(())
}
