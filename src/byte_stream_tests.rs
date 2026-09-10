use super::*;
use crate::sim::{FailurePhase, FaultStore, Operation};
use crate::{
    CheckpointStatus, CollectionFinish, CollectionStart, CommitStatus, LogId, Options,
    TransactionId, ValidatedBackend,
};
use object_store::{
    ObjectStore, ObjectStoreExt, local::LocalFileSystem, memory::InMemory, path::Path,
};
use std::sync::Arc;

type TestResult = Result<(), Box<dyn std::error::Error>>;

async fn setup(store: Arc<dyn ObjectStore>, options: Options) -> Result<(Log, View), Error> {
    let backend = ValidatedBackend::new(store, Path::from("byte-streams")).await?;
    let log = Log::open(&backend, &LogId::new("test")?, options).await?;
    let view = log.load().await?;
    Ok((log, view))
}
fn small() -> Options {
    Options {
        max_object_bytes: 256,
        max_object_refs: 8,
        ..Options::default()
    }
}
fn descriptor(len: u64, chunk: u64) -> Bytes {
    let mut bytes = TAG.to_vec();
    bytes.extend_from_slice(&len.to_be_bytes());
    bytes.extend_from_slice(&chunk.to_be_bytes());
    bytes.into()
}
async fn collect(log: &Log) -> TestResult {
    let view = log.load().await?;
    match log.start_collection(&view).await? {
        CollectionStart::Installed(view, _) | CollectionStart::Active(view) => {
            assert!(matches!(
                log.resume_collection(&view).await?,
                CollectionFinish::Complete(..)
            ));
        }
        CollectionStart::Empty(_) => {}
        other => return Err(format!("unexpected collection: {other:?}").into()),
    }
    Ok(())
}

#[tokio::test]
async fn geometry_capacity_and_tiny_limits() -> TestResult {
    let (log, view) = setup(
        Arc::new(InMemory::new()),
        Options {
            max_object_bytes: 2 * 1024 * 1024,
            ..Options::default()
        },
    )
    .await?;
    let writer = log.byte_writer(&view)?;
    assert_eq!(writer.capacity, 2 * 1024 * 1024 * 1024);
    assert_eq!(writer.chunk_bytes, 2 * 1024 * 1024);
    let (log, view) = setup(Arc::new(InMemory::new()), small()).await?;
    let mut writer = log.byte_writer(&view)?;
    assert_eq!(writer.capacity, 3 * 256);
    writer.write(&vec![1; 768]).await?;
    let root = writer.finish().await?;
    assert!(root.reference().len() <= 256);
    assert_eq!(log.open_bytes(&view, root.reference()).await?.len(), 768);
    let mut writer = log.byte_writer(&view)?;
    assert!(matches!(
        writer.write(&vec![0; 769]).await,
        Err(Error::LimitExceeded(_))
    ));
    assert!(writer.finish().await.is_err());
    let (log, view) = setup(
        Arc::new(InMemory::new()),
        Options {
            max_object_bytes: 32,
            ..small()
        },
    )
    .await?;
    assert!(matches!(
        log.byte_writer(&view),
        Err(Error::LimitExceeded(_))
    ));
    Ok(())
}

#[tokio::test]
async fn reads_are_short_authenticated_and_cache_one_chunk() -> TestResult {
    let faults = FaultStore::new(InMemory::new());
    let (log, view) = setup(Arc::new(faults.clone()), small()).await?;
    let payload: Vec<u8> = (0_u8..251).cycle().take(600).collect();
    let mut writer = log.byte_writer(&view)?;
    drop(writer.write(b"not polled"));
    writer.write(&payload[..17]).await?;
    writer.write(&[]).await?;
    writer.write(&payload[17..]).await?;
    let root = writer.finish().await?;
    let mut reader = log.open_bytes(&view, root.reference()).await?;
    assert_eq!(reader.len(), 600);
    faults.reset();
    assert!(reader.read_at(u64::MAX, usize::MAX).await?.is_empty());
    assert!(reader.read_at(0, 0).await?.is_empty());
    assert_eq!(faults.metrics().operation(Operation::Get).requests, 0);
    assert_eq!(
        reader.read_at(250, usize::MAX).await?.as_ref(),
        &payload[250..256]
    );
    assert_eq!(reader.read_at(0, 1).await?.as_ref(), &payload[..1]);
    assert_eq!(faults.metrics().operation(Operation::Get).requests, 1);
    assert_eq!(reader.read_at(599, 100).await?.as_ref(), &payload[599..]);
    assert_eq!(reader.read_at(0, 1).await?.as_ref(), &payload[..1]);
    assert_eq!(faults.metrics().operation(Operation::Get).requests, 3);
    let mut actual = Vec::new();
    while actual.len() < payload.len() {
        actual.extend_from_slice(&reader.read_at(actual.len() as u64, 19).await?);
    }
    assert_eq!(actual, payload);
    let root = log.byte_writer(&view)?.finish().await?;
    let mut empty = log.open_bytes(&view, root.reference()).await?;
    assert!(empty.is_empty());
    assert!(empty.read_at(0, 1).await?.is_empty());
    Ok(())
}

#[tokio::test]
async fn authenticated_malformed_descriptors_fail_before_payload_reads() -> TestResult {
    let faults = FaultStore::new(InMemory::new());
    let (log, view) = setup(Arc::new(faults.clone()), small()).await?;
    let blob = log.put_object(&view, Bytes::from_static(b"abc")).await?;
    let node = log.put_node(&view, Bytes::new(), vec![]).await?;
    for (data, children) in [
        (Bytes::from_static(b"not a stream"), vec![]),
        (descriptor(0, 0), vec![]),
        (descriptor(0, 256), vec![blob.clone()]),
        (descriptor(3, 256), vec![]),
        (descriptor(2, 256), vec![blob.clone()]),
        (descriptor(3, 257), vec![blob.clone()]),
        (descriptor(4, 2), vec![blob.clone(), blob.clone()]),
        (descriptor(node.reference().len(), 256), vec![node]),
    ] {
        let root = log.put_node(&view, data, children).await?;
        faults.reset();
        assert!(matches!(
            log.open_bytes(&view, root.reference()).await,
            Err(Error::InvalidFormat(_))
        ));
        assert_eq!(faults.metrics().operation(Operation::Get).requests, 1);
    }
    Ok(())
}

#[tokio::test]
async fn failed_and_cancelled_writes_cannot_finish() -> TestResult {
    for phase in [FailurePhase::Before, FailurePhase::After] {
        let faults = FaultStore::new(InMemory::new());
        let (log, view) = setup(Arc::new(faults.clone()), small()).await?;
        let mut writer = log.byte_writer(&view)?;
        faults.reset();
        faults.fail_next(Operation::Put, phase);
        assert!(writer.write(&[1; 256]).await.is_err());
        assert!(writer.write(b"later").await.is_err());
        assert!(writer.finish().await.is_err());
        assert_eq!(faults.metrics().operation(Operation::Put).requests, 1);
        assert_eq!(log.load().await?.generation(), 0);
    }
    let faults = FaultStore::new(InMemory::new());
    let (log, view) = setup(Arc::new(faults.clone()), small()).await?;
    let mut writer = log.byte_writer(&view)?;
    faults.reset();
    let mut pause = faults.pause_next_put(FailurePhase::After);
    let mut write = Box::pin(writer.write(&[1; 256]));
    tokio::select! { result=&mut write => return Err(format!("write did not pause: {result:?}").into()), entered=pause.wait_until_entered()=>{ assert!(entered); } }
    drop(write);
    assert!(!pause.release());
    assert!(writer.write(b"later").await.is_err());
    assert!(writer.finish().await.is_err());
    assert_eq!(faults.metrics().operation(Operation::Put).requests, 1);
    Ok(())
}

async fn lifecycle(store: Arc<dyn ObjectStore>) -> TestResult {
    let (log, view) = setup(Arc::clone(&store), small()).await?;
    let mut writer = log.byte_writer(&view)?;
    writer.write(&[9; 500]).await?;
    let root = writer.finish().await?;
    let reference = root.reference().clone();
    let prepared = log.prepare(
        &view,
        TransactionId::new(),
        Bytes::new(),
        Bytes::new(),
        vec![root.clone()],
    )?;
    let CommitStatus::Committed(view) = log.commit(prepared).await? else {
        return Err("commit failed".into());
    };
    let through = view.tail().last().ok_or("missing tail")?;
    let CheckpointStatus::Published(_) = log
        .publish_checkpoint(&view, through, Bytes::new(), vec![root])
        .await?
    else {
        return Err("checkpoint failed".into());
    };
    collect(&log).await?;
    let (cold, current) = setup(store, small()).await?;
    let mut reader = cold.open_bytes(&current, &reference).await?;
    assert_eq!(reader.read_at(256, 500).await?.as_ref(), &[9; 244]);
    // Old stream proofs cannot be reused against the post-collection view.
    let mut old = log.byte_writer(&view)?;
    old.write(&[7; 256]).await?;
    collect(&log).await?;
    let root = old.finish().await?;
    let current = log.load().await?;
    assert!(matches!(
        log.prepare(
            &current,
            TransactionId::new(),
            Bytes::new(),
            Bytes::new(),
            vec![root.clone()]
        ),
        Err(Error::InvalidStagedObject)
    ));
    // The original view can prepare, but its conditional publication must lose.
    let stale = log.prepare(
        &view,
        TransactionId::new(),
        Bytes::new(),
        Bytes::new(),
        vec![root],
    )?;
    assert!(matches!(
        log.commit(stale).await?,
        CommitStatus::Conflict(_)
    ));
    Ok(())
}
#[tokio::test]
async fn published_stream_survives_checkpoint_collection_and_cold_reopen() -> TestResult {
    lifecycle(Arc::new(InMemory::new())).await?;
    Ok(())
}

#[tokio::test]
async fn filesystem_without_conditional_update_is_rejected() -> TestResult {
    let directory = tempfile::tempdir()?;
    assert!(matches!(
        setup(
            Arc::new(LocalFileSystem::new_with_prefix(directory.path())?),
            small()
        )
        .await,
        Err(Error::UnsupportedBackend("conditional update"))
    ));
    Ok(())
}

#[tokio::test]
async fn guards_charge_failed_stream_writes_without_refunding() -> TestResult {
    use std::sync::atomic::{AtomicUsize, Ordering};
    #[derive(Debug)]
    struct TwoWrites(AtomicUsize);
    impl crate::RequestGuard for TwoWrites {
        fn before_request(&self, request: crate::Request) -> Result<(), crate::RequestDenied> {
            if matches!(request, crate::Request::Write { .. })
                && self.0.fetch_add(1, Ordering::Relaxed) != 0
            {
                return Err(crate::RequestDenied);
            }
            Ok(())
        }
    }
    let faults = FaultStore::new(InMemory::new());
    let (log, view) = setup(Arc::new(faults.clone()), small()).await?;
    let guard = Arc::new(TwoWrites(AtomicUsize::new(0)));
    let log = log.with_request_guard(guard.clone());
    let mut writer = log.byte_writer(&view)?;
    faults.reset();
    assert!(matches!(
        writer.write(&[3; 512]).await,
        Err(Error::RequestDenied)
    ));
    assert!(writer.finish().await.is_err());
    assert_eq!(guard.0.load(Ordering::Relaxed), 2);
    assert_eq!(faults.metrics().operation(Operation::Put).requests, 1);
    Ok(())
}

#[tokio::test]
async fn offset_read_verifies_the_entire_chunk() -> TestResult {
    let faults = FaultStore::new(InMemory::new());
    let (log, view) = setup(Arc::new(faults.clone()), small()).await?;
    let mut writer = log.byte_writer(&view)?;
    faults.reset();
    writer.write(&[1; 256]).await?;
    let path = faults
        .metrics()
        .events
        .first()
        .ok_or("missing chunk upload")?
        .path
        .clone();
    let root = writer.finish().await?;
    let mut corrupted = vec![1; 256];
    corrupted[255] = 2;
    faults
        .put(&Path::from(path), Bytes::from(corrupted).into())
        .await?;
    let mut reader = log.open_bytes(&view, root.reference()).await?;
    assert!(matches!(
        reader.read_at(0, 1).await,
        Err(Error::CorruptObject)
    ));
    Ok(())
}
