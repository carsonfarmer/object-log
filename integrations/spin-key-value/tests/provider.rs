#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::Arc;

use object_log::{Log, LogId, Options, RetentionId, RetentionStatus, ValidatedBackend};
use object_log_spin_key_value::{Config, HostLimits, Maintenance, Manager, ObjectLogKeyValueStore};
use object_store::{ObjectStore, local::LocalFileSystem, memory::InMemory, path::Path};
use spin_factor_key_value::{
    Error, StoreManager, SwapError,
    runtime_config::spin::{MakeKeyValueStore, RuntimeConfigResolver},
};

type Result<T = ()> = anyhow::Result<T>;

fn manager(store: Arc<dyn ObjectStore>, prefix: &str, limits: HostLimits) -> Result<Manager> {
    Manager::new(store, prefix, Options::default(), limits)
}

#[tokio::test]
async fn registration_and_strict_config() -> Result {
    let mut resolver = RuntimeConfigResolver::new();
    resolver.register_store_type(ObjectLogKeyValueStore)?;
    assert!(
        resolver
            .register_store_type(ObjectLogKeyValueStore)
            .is_err()
    );
    let table = toml::toml! {
        [key_value_store.default]
        type = "object-log"
        prefix = "tenant-a"
        memory = true
        [key_value_store.default.wal]
        max_tail_entries = 128
        [key_value_store.audit]
        type = "object-log"
        prefix = "tenant-a"
        memory = true
    };
    resolver.add_default_store::<ObjectLogKeyValueStore>(
        "fallback",
        toml::from_str::<Config>("prefix = 'fallback'\nmemory = true")?,
    )?;
    let runtime = resolver.resolve(Some(&table))?;
    assert!(runtime.has_store_manager("fallback"));
    let default = runtime
        .get_store_manager("default")
        .unwrap()
        .get("default")
        .await?;
    let audit = runtime
        .get_store_manager("audit")
        .unwrap()
        .get("audit")
        .await?;
    default.set("key", b"value").await?;
    assert_eq!(audit.get("key", usize::MAX).await?, None);
    for config in [
        "prefix = 'a'\nmemory = true\nunknown = 1",
        "prefix = 'a'\nmemory = true\n[limits]\nunknown = 1",
        "prefix = 'a'\nmemory = true\n[wal]\nunknown = 1",
    ] {
        assert!(toml::from_str::<Config>(config).is_err());
    }
    for config in [
        "prefix = ''\nmemory = true",
        "prefix = '/a'\nmemory = true",
        "prefix = 'a'\nmemory = true\nbucket = 'b'",
        "prefix = 'a'",
        "prefix = 'a'\nmemory = true\n[limits]\nconcurrent_operations = 0",
        "prefix = 'a'\nmemory = true\n[limits]\ncheckpoint_entries = 2048",
    ] {
        let config = toml::from_str::<Config>(config)?;
        assert!(ObjectLogKeyValueStore.make_store(config).is_err());
    }
    Ok(())
}

#[tokio::test]
async fn filesystem_rejects_missing_conditional_updates() -> Result {
    let dir = tempfile::tempdir()?;
    let manager = manager(
        Arc::new(LocalFileSystem::new_with_prefix(dir.path())?),
        "local",
        HostLimits::default(),
    )?;
    assert!(matches!(manager.get("default").await, Err(Error::Other(_))));
    Ok(())
}

#[tokio::test]
async fn bytes_batches_namespaces_and_restart() -> Result {
    let memory: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let host = manager(memory.clone(), "app-a", HostLimits::default())?;
    let store = host.get("default").await?;
    assert!(matches!(host.get("").await, Err(Error::NoSuchStore)));
    assert!(!host.is_defined(""));
    assert_eq!(store.get("missing", usize::MAX).await?, None);
    store.set("", &[]).await?;
    assert!(store.exists("").await?);
    store.set("µ/../key", &[0, 255, 128]).await?;
    store
        .set_many(vec![("same".into(), vec![1]), ("same".into(), vec![2])])
        .await?;
    assert_eq!(
        store
            .get_many(
                vec!["same".into(), "missing".into(), "same".into()],
                usize::MAX
            )
            .await?,
        vec![
            ("same".into(), Some(vec![2])),
            ("missing".into(), None),
            ("same".into(), Some(vec![2]))
        ]
    );
    assert_eq!(
        host.get("audit").await?.get("same", usize::MAX).await?,
        None
    );
    let other = manager(memory.clone(), "app-b", HostLimits::default())?;
    assert_eq!(
        other.get("default").await?.get("same", usize::MAX).await?,
        None
    );
    drop(store);
    drop(host);
    let reopened = manager(memory, "app-a", HostLimits::default())?;
    let store = reopened.get("default").await?;
    assert_eq!(
        store.get("µ/../key", usize::MAX).await?,
        Some(vec![0, 255, 128])
    );
    assert_eq!(store.get("", usize::MAX).await?, Some(vec![]));
    assert_eq!(
        store.get_keys(usize::MAX).await?,
        vec!["", "same", "µ/../key"]
    );
    let (mut keys, completion) = store.get_keys_async(usize::MAX).await;
    let mut streamed = Vec::new();
    while let Some(key) = keys.recv().await {
        streamed.push(key);
    }
    assert!(completion.await?.is_ok());
    assert_eq!(streamed, vec!["", "same", "µ/../key"]);
    store
        .delete_many(vec!["same".into(), "same".into(), "missing".into()])
        .await?;
    store.delete("missing").await?;
    assert!(!store.exists("same").await?);
    store.set_many(vec![]).await?;
    store.delete_many(vec![]).await?;
    Ok(())
}

#[tokio::test]
async fn increments_and_cas_observe_spin_contract() -> Result {
    let host = manager(Arc::new(InMemory::new()), "atomics", HostLimits::default())?;
    let store = host.get("default").await?;
    assert_eq!(store.increment("zero".into(), 0).await?, 0);
    assert_eq!(
        store.get("zero", usize::MAX).await?,
        Some(0i64.to_le_bytes().to_vec())
    );
    assert_eq!(store.increment("zero".into(), -2).await?, -2);
    store.set("max", &i64::MAX.to_le_bytes()).await?;
    assert!(store.increment("max".into(), 1).await.is_err());
    assert_eq!(
        store.get("max", usize::MAX).await?,
        Some(i64::MAX.to_le_bytes().to_vec())
    );
    store.set("bad", b"text").await?;
    assert!(store.increment("bad".into(), 1).await.is_err());
    let cas = store.new_compare_and_swap(42, "new").await?;
    assert_eq!(cas.bucket_rep().await, 42);
    assert_eq!(cas.key().await, "new");
    cas.swap(b"new-value".to_vec()).await?; // current is optional
    assert!(cas.swap(vec![]).await.is_err());
    let cas = store.new_compare_and_swap(42, "new").await?;
    assert_eq!(cas.current(usize::MAX).await?, Some(b"new-value".to_vec()));
    store.set("new", b"changed").await?;
    store.set("new", b"new-value").await?; // ABA must still conflict
    assert!(matches!(
        cas.swap(vec![]).await,
        Err(SwapError::CasFailed(_))
    ));
    let cas = store.new_compare_and_swap(42, "absent").await?;
    assert_eq!(cas.current(usize::MAX).await?, None);
    store.set("absent", b"raced").await?;
    assert!(matches!(
        cas.swap(vec![]).await,
        Err(SwapError::CasFailed(_))
    ));
    assert_eq!(
        store.get("absent", usize::MAX).await?,
        Some(b"raced".to_vec())
    );
    Ok(())
}

#[tokio::test]
async fn independent_writers_retry_conflicts_and_checkpoint() -> Result {
    let memory: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let limits = HostLimits {
        checkpoint_entries: 4,
        attempts: 64,
        ..HostLimits::default()
    };
    let first = manager(memory.clone(), "concurrent", limits)?;
    let second = manager(memory.clone(), "concurrent", limits)?;
    let stores = [first.get("default").await?, second.get("default").await?];
    let mut tasks = Vec::new();
    for index in 0..4 {
        let store = stores[index % 2].clone();
        tasks.push(tokio::spawn(async move {
            for _ in 0..12 {
                store.increment("counter".into(), 1).await?;
            }
            Ok::<_, Error>(())
        }));
    }
    for task in tasks {
        task.await??;
    }
    let reopened = manager(memory, "concurrent", limits)?;
    assert_eq!(
        reopened
            .get("default")
            .await?
            .get("counter", usize::MAX)
            .await?,
        Some(48i64.to_le_bytes().to_vec())
    );
    Ok(())
}

#[tokio::test]
async fn limits_reject_whole_batches_and_lists() -> Result {
    let limits = HostLimits {
        key_bytes: 4,
        value_bytes: 8,
        batch_entries: 2,
        batch_bytes: 12,
        list_keys: 2,
        page_entries: 1,
        ..HostLimits::default()
    };
    let host = manager(Arc::new(InMemory::new()), "limits", limits)?;
    let store = host.get("default").await?;
    assert!(store.set("12345", b"a").await.is_err());
    assert!(store.set("a", &[0; 9]).await.is_err());
    assert!(
        store
            .set_many(vec![("ok".into(), vec![1]), ("bad".into(), vec![0; 9])])
            .await
            .is_err()
    );
    assert_eq!(store.get("ok", usize::MAX).await?, None);
    assert!(
        store
            .set_many(vec![("a".into(), vec![0; 6]), ("b".into(), vec![0; 6])])
            .await
            .is_err()
    );
    assert!(
        store
            .delete_many(vec!["a".into(), "b".into(), "c".into()])
            .await
            .is_err()
    );
    store.set("a", b"v").await?;
    assert!(store.get("a", 1).await.is_err());
    assert!(store.get_many(vec!["a".into()], 1).await.is_err());
    assert!(
        store
            .new_compare_and_swap(0, "a")
            .await?
            .current(1)
            .await
            .is_err()
    );
    store.set("b", b"v").await?;
    assert_eq!(store.get_keys(usize::MAX).await?, vec!["a", "b"]);
    assert!(store.get_keys(1).await.is_err());
    store.set("c", b"v").await?;
    assert!(store.get_keys(usize::MAX).await.is_err());
    let (mut receiver, result) = store.get_keys_async(usize::MAX).await;
    assert!(receiver.recv().await.is_none());
    assert!(result.await?.is_err());
    Ok(())
}

#[tokio::test]
async fn collection_preserves_roots_and_respects_retentions() -> Result {
    let memory: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let limits = HostLimits {
        checkpoint_entries: 4,
        collection_candidates: 5,
        ..HostLimits::default()
    };
    let host = manager(memory.clone(), "maintenance", limits)?;
    let store = host.get("default").await?;
    for i in 0..12 {
        store.set("key", &[i]).await?;
    }
    let backend = ValidatedBackend::new(memory.clone(), Path::from("maintenance")).await?;
    let id = LogId::new(blake3::hash(b"default").to_hex().to_string())?;
    let log = Log::open(&backend, &id, Options::default()).await?;
    let retention = RetentionId::new();
    assert!(matches!(
        log.retain(&log.load().await?, retention).await?,
        RetentionStatus::Applied(_)
    ));
    assert_eq!(host.maintain("default").await?, Maintenance::Retained);
    assert!(matches!(
        log.release_retention(&log.load().await?, retention).await?,
        RetentionStatus::Applied(_)
    ));
    let mut complete = false;
    for _ in 0..50 {
        if host.maintain("default").await? == Maintenance::Complete {
            complete = true;
            break;
        }
    }
    assert!(complete);
    let reopened = manager(memory, "maintenance", limits)?;
    assert_eq!(
        reopened
            .get("default")
            .await?
            .get("key", usize::MAX)
            .await?,
        Some(vec![11])
    );
    Ok(())
}

#[tokio::test]
async fn boolean_and_key_queries_do_not_charge_value_response_bytes() -> Result {
    let limits = HostLimits {
        response_bytes: 32,
        ..HostLimits::default()
    };
    let host = manager(Arc::new(InMemory::new()), "small-response", limits)?;
    let store = host.get("default").await?;
    store.set("key", &[42; 100]).await?;
    assert!(store.get("key", usize::MAX).await.is_err());
    assert!(store.exists("key").await?);
    assert_eq!(store.get_keys(usize::MAX).await?, vec!["key"]);
    Ok(())
}
