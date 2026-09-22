//! Opt-in qualification against one disposable AWS S3 prefix.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use object_log_spin_key_value::{Config, HostLimits};

#[path = "support/provider.rs"]
mod provider;

fn config(suffix: &str, limits: HostLimits) -> anyhow::Result<Config> {
    let base = std::env::var("SPIN_KV_AWS_PREFIX")?;
    anyhow::ensure!(!base.is_empty(), "SPIN_KV_AWS_PREFIX must be nonempty");
    Ok(Config {
        prefix: format!("{base}/{suffix}-{}", object_log::TransactionId::new()),
        bucket: Some(std::env::var("SPIN_KV_AWS_BUCKET")?),
        region: Some(std::env::var("SPIN_KV_AWS_REGION")?),
        endpoint: None,
        allow_http: false,
        memory: false,
        limits,
        wal: Default::default(),
    })
}

#[tokio::test]
#[ignore = "requires a disposable AWS S3 prefix; see README"]
async fn aws_native_provider() -> anyhow::Result<()> {
    spin_tls::install_default_crypto_provider();
    provider::qualify(config("provider", HostLimits::default())?).await
}

#[tokio::test]
#[ignore = "requires a disposable AWS S3 prefix; see README"]
async fn aws_large_value_envelope() -> anyhow::Result<()> {
    spin_tls::install_default_crypto_provider();
    let maximum = 8 << 20;
    let limits = HostLimits {
        value_bytes: maximum,
        batch_bytes: maximum + 1024,
        response_bytes: maximum + 1024,
        concurrent_operations: 2,
        ..HostLimits::default()
    };
    provider::qualify_large_values(config("large-values", limits)?).await
}
