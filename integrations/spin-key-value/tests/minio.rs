//! Opt-in local MinIO qualification. The parent gives credentials only to a
//! native host subprocess; its configuration uses the normal provider factory.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use std::process::Command;

use object_log_spin_key_value::Config;

#[path = "support/provider.rs"]
mod provider;

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
    provider::qualify(config).await
}
