use std::env;
use std::error::Error as StdError;

use object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey};
use object_store::path::Path;
use object_store::prefix::PrefixStore;

pub(crate) fn build_minio() -> Result<PrefixStore<object_store::aws::AmazonS3>, Box<dyn StdError>> {
    let endpoint = required_env("OBJECT_LOG_MINIO_ENDPOINT")?;
    let builder = configured_builder(
        &endpoint,
        &required_env("OBJECT_LOG_MINIO_ACCESS_KEY")?,
        &required_env("OBJECT_LOG_MINIO_SECRET_KEY")?,
        env::var("OBJECT_LOG_MINIO_SESSION_TOKEN").ok().as_deref(),
        &required_env("OBJECT_LOG_MINIO_BUCKET")?,
        &env::var("OBJECT_LOG_MINIO_REGION").unwrap_or_else(|_| "us-east-1".into()),
    );
    Ok(PrefixStore::new(
        builder.build()?,
        Path::from(env::var("OBJECT_LOG_MINIO_PREFIX").unwrap_or_default()),
    ))
}

fn configured_builder(
    endpoint: &str,
    access_key: &str,
    secret_key: &str,
    session_token: Option<&str>,
    bucket: &str,
    region: &str,
) -> AmazonS3Builder {
    let mut builder = AmazonS3Builder::new()
        .with_endpoint(endpoint)
        .with_access_key_id(access_key)
        .with_secret_access_key(secret_key)
        .with_bucket_name(bucket)
        .with_region(region)
        .with_allow_http(endpoint.starts_with("http://"))
        .with_virtual_hosted_style_request(false)
        .with_disable_bulk_delete(false);
    if let Some(token) = session_token {
        builder = builder.with_token(token);
    }
    builder
}

fn required_env(name: &'static str) -> Result<String, Box<dyn StdError>> {
    env::var(name).map_err(|_| format!("{name} is not set").into())
}

#[test]
fn remote_builder_preserves_region_and_temporary_credentials() {
    let builder = configured_builder(
        "https://s3.us-west-2.amazonaws.com",
        "temporary-access-key",
        "temporary-secret-key",
        Some("temporary-session-token"),
        "qualification",
        "us-west-2",
    );
    assert_eq!(
        builder.get_config_value(&AmazonS3ConfigKey::Region),
        Some("us-west-2".into())
    );
    assert_eq!(
        builder.get_config_value(&AmazonS3ConfigKey::Token),
        Some("temporary-session-token".into())
    );
}
