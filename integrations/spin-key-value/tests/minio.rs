//! Opt-in local MinIO qualification. The parent gives credentials only to a
//! native host subprocess; its configuration uses the normal provider factory.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use anyhow::Context;
use std::{process::Command, sync::Arc};

use object_log_spin_key_value::{Config, Maintenance, ObjectLogKeyValueStore};
use spin_factor_key_value::{StoreManager, runtime_config::spin::MakeKeyValueStore};

#[test]
#[ignore = "requires disposable loopback MinIO; see README"]
fn minio_native_provider() -> anyhow::Result<()> {
    let endpoint = std::env::var("OBJECT_LOG_MINIO_ENDPOINT")?;
    let authority = endpoint
        .strip_prefix("http://127.0.0.1:")
        .or_else(|| endpoint.strip_prefix("http://localhost:"))
        .ok_or_else(|| anyhow::anyhow!("MinIO test requires a loopback HTTP endpoint"))?;
    let _: u16 = authority.parse()?;
    let status = Command::new(std::env::current_exe()?)
        .args(["--ignored", "--exact", "native_host_child", "--nocapture"])
        .env(
            "AWS_ACCESS_KEY_ID",
            std::env::var("OBJECT_LOG_MINIO_ACCESS_KEY")?,
        )
        .env(
            "AWS_SECRET_ACCESS_KEY",
            std::env::var("OBJECT_LOG_MINIO_SECRET_KEY")?,
        )
        .env("AWS_REGION", "us-east-1")
        .env_remove("AWS_SESSION_TOKEN")
        .env("SPIN_KV_MINIO_CHILD", "1")
        .status()?;
    anyhow::ensure!(status.success(), "native host qualification failed");
    Ok(())
}

#[tokio::test]
#[ignore = "child launched only by minio_native_provider"]
async fn native_host_child() -> anyhow::Result<()> {
    spin_tls::install_default_crypto_provider();
    let _ = tracing_subscriber::fmt().with_test_writer().try_init();
    anyhow::ensure!(
        std::env::var("SPIN_KV_MINIO_CHILD").as_deref() == Ok("1"),
        "run the parent test"
    );
    let prefix = format!("spin-kv-{}", object_log::TransactionId::new());
    let config = Config {
        prefix,
        bucket: Some(std::env::var("OBJECT_LOG_MINIO_BUCKET")?),
        region: Some("us-east-1".into()),
        endpoint: Some(std::env::var("OBJECT_LOG_MINIO_ENDPOINT")?),
        allow_http: true,
        memory: false,
        limits: object_log_spin_key_value::HostLimits {
            checkpoint_entries: 4,
            attempts: 32,
            ..Default::default()
        },
        wal: Default::default(),
    };
    let host = ObjectLogKeyValueStore.make_store(config.clone())?;
    let other_host = ObjectLogKeyValueStore.make_store(config.clone())?;
    let store = host.get("default").await.context("open default")?;
    store
        .set("saved", b"survives host restart")
        .await
        .context("initial set")?;
    assert_eq!(
        host.get("audit").await?.get("saved", usize::MAX).await?,
        None
    );
    let other = other_host.get("default").await.context("open default")?;
    let mut tasks = Vec::new();
    for writer in [Arc::clone(&store), other] {
        tasks.push(tokio::spawn(async move {
            for _ in 0..8 {
                writer
                    .increment("counter".into(), 1)
                    .await
                    .context("concurrent increment")?;
            }
            Ok::<_, anyhow::Error>(())
        }));
    }
    for task in tasks {
        task.await??;
    }
    drop(store);
    drop(host);
    drop(other_host);
    let restarted = ObjectLogKeyValueStore.make_store(config)?;
    let store = restarted.get("default").await?;
    assert_eq!(
        store.get("saved", usize::MAX).await?,
        Some(b"survives host restart".to_vec())
    );
    assert_eq!(
        store.get("counter", usize::MAX).await?,
        Some(16i64.to_le_bytes().to_vec())
    );
    assert!(store.set("oversize", &[0; 65537]).await.is_err());
    let mut complete = false;
    for _ in 0..20 {
        if restarted.maintain("default").await.context("maintenance")? == Maintenance::Complete {
            complete = true;
            break;
        }
    }
    assert!(complete);
    assert_eq!(
        store.get("counter", usize::MAX).await?,
        Some(16i64.to_le_bytes().to_vec())
    );
    Ok(())
}
