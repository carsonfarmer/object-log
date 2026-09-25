use std::env;
use std::error::Error as StdError;

use object_store::aws::AmazonS3Builder;
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

pub(crate) fn configured_builder(
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
