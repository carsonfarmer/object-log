use std::env;
use std::error::Error as StdError;

use object_store::aws::{AmazonS3, AmazonS3Builder};

pub(crate) fn build_minio() -> Result<AmazonS3, Box<dyn StdError>> {
    Ok(AmazonS3Builder::new()
        .with_endpoint(required_env("OBJECT_LOG_MINIO_ENDPOINT")?)
        .with_access_key_id(required_env("OBJECT_LOG_MINIO_ACCESS_KEY")?)
        .with_secret_access_key(required_env("OBJECT_LOG_MINIO_SECRET_KEY")?)
        .with_bucket_name(required_env("OBJECT_LOG_MINIO_BUCKET")?)
        .with_region("us-east-1")
        .with_allow_http(true)
        .with_virtual_hosted_style_request(false)
        .with_disable_bulk_delete(false)
        .build()?)
}

fn required_env(name: &'static str) -> Result<String, Box<dyn StdError>> {
    env::var(name).map_err(|_| format!("{name} is not set").into())
}
