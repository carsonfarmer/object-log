#![cfg(feature = "test-util")]

use std::{error::Error as StdError, sync::Arc};

use bytes::Bytes;
use object_log::{
    CollectionFinish, CollectionStart, CommitStatus, Log, LogId, Options, TransactionId,
    ValidatedBackend,
    sim::{FailurePhase, FaultStore, Operation},
};
use object_store::{memory::InMemory, path::Path};

#[tokio::test]
async fn byte_writer_and_commit_read_one_plan_for_their_view() -> Result<(), Box<dyn StdError>> {
    let store = FaultStore::new(InMemory::new());
    let backend = ValidatedBackend::new(Arc::new(store.clone()), Path::from("plan-cache")).await?;
    let log = Log::open(
        &backend,
        &LogId::new("stream")?,
        Options {
            max_object_bytes: 4096,
            ..Options::default()
        },
    )
    .await?;
    let initial = log.load().await?;
    log.put_object(&initial, Bytes::from_static(b"collectible"))
        .await?;
    let CollectionStart::Installed(active, _) = log.start_collection(&initial).await? else {
        return Err("collection did not install a plan".into());
    };

    store.reset();
    let mut writer = log.byte_writer(&active)?;
    writer.write(&vec![42; 10 * 4096]).await?;
    let root = writer.finish().await?;
    let prepared = log.prepare(
        &active,
        TransactionId::new(),
        Bytes::new(),
        Bytes::new(),
        vec![root],
    )?;
    assert!(matches!(
        log.commit(prepared).await?,
        CommitStatus::Committed(_)
    ));
    assert_eq!(store.metrics().operation(Operation::Get).requests, 1);

    let next = log.load().await?;
    store.reset();
    let mut pause = store.pause_next_get(FailurePhase::Before);
    let first = {
        let log = log.clone();
        let view = next.clone();
        tokio::spawn(async move { log.put_object(&view, Bytes::from_static(b"first")).await })
    };
    assert!(pause.wait_until_entered().await);
    let second = {
        let log = log.clone();
        tokio::spawn(async move { log.put_object(&next, Bytes::from_static(b"second")).await })
    };
    tokio::task::yield_now().await;
    assert!(pause.release());
    first.await??;
    second.await??;
    assert_eq!(store.metrics().operation(Operation::Get).requests, 1);
    Ok(())
}

#[tokio::test]
async fn cached_plan_keeps_staging_available_after_its_fence_clears()
-> Result<(), Box<dyn StdError>> {
    let store = FaultStore::new(InMemory::new());
    let backend = ValidatedBackend::new(Arc::new(store.clone()), Path::from("plan-clear")).await?;
    let log = Log::open(
        &backend,
        &LogId::new("stream")?,
        Options {
            max_object_bytes: 4096,
            ..Options::default()
        },
    )
    .await?;
    let initial = log.load().await?;
    log.put_object(&initial, Bytes::from_static(b"collectible"))
        .await?;
    let CollectionStart::Installed(active, _) = log.start_collection(&initial).await? else {
        return Err("collection did not install a plan".into());
    };

    let mut writer = log.byte_writer(&active)?;
    writer.write(&vec![1; 4096]).await?;
    let CollectionFinish::Complete(cleared, _) = log.resume_collection(&active).await? else {
        return Err("collection did not clear its plan".into());
    };
    assert!(cleared.collection_plan_bytes().is_none());

    store.reset();
    writer.write(&vec![2; 4096]).await?;
    let root = writer.finish().await?;
    assert_eq!(store.metrics().operation(Operation::Get).requests, 0);
    let prepared = log.prepare(
        &active,
        TransactionId::new(),
        Bytes::new(),
        Bytes::new(),
        vec![root],
    )?;
    assert!(matches!(
        log.commit(prepared).await?,
        CommitStatus::Conflict(_)
    ));
    assert!(log.load().await?.tail().is_empty());
    Ok(())
}
