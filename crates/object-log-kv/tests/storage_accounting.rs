use std::{error::Error as StdError, sync::Arc};

use bytes::Bytes;
use object_log::{
    CheckpointStatus, CommitStatus, Error, Log, LogId, Options, TransactionId, ValidatedBackend,
};
use object_log_kv::{KvCommand, KvError, KvStore, Limits};
use object_store::{memory::InMemory, path::Path};

#[tokio::test]
async fn capacity_rejection_preserves_a_collectible_tree() -> Result<(), Box<dyn StdError>> {
    let backend =
        ValidatedBackend::new(Arc::new(InMemory::new()), Path::from("kv-accounting")).await?;
    let log = Log::open(
        &backend,
        &LogId::new("kv")?,
        Options {
            max_collection_objects: 3,
            ..Options::default()
        },
    )
    .await?;
    let store = KvStore::new(log, Limits::default());
    let prepared = store
        .snapshot()
        .await?
        .prepare(
            TransactionId::new(),
            &[KvCommand::Set {
                key: Bytes::from_static(b"aa"),
                value: Bytes::from_static(b"one"),
            }],
        )
        .await?;
    let CommitStatus::Committed(before) = store.log().commit(prepared).await? else {
        return Err("first key did not publish".into());
    };
    // Adding a sibling makes three nodes plus the enclosing record: four
    // physical objects. The WAL rejects it without KV-specific bookkeeping.
    assert!(matches!(
        store
            .snapshot()
            .await?
            .prepare(
                TransactionId::new(),
                &[KvCommand::Set {
                    key: Bytes::from_static(b"ab"),
                    value: Bytes::from_static(b"two")
                },]
            )
            .await,
        Err(KvError::Log(Error::LimitExceeded("publication objects")))
    ));
    assert_eq!(store.log().load().await?.generation(), before.generation());
    let snapshot = store.snapshot().await?;
    assert_eq!(
        snapshot.get(b"aa").await?.as_deref(),
        Some(b"one".as_slice())
    );
    assert_eq!(snapshot.get(b"ab").await?, None);
    let Some(CheckpointStatus::Published(view)) = snapshot.checkpoint().await? else {
        return Err("checkpoint did not publish".into());
    };
    store.log().start_collection(&view).await?;
    Ok(())
}
