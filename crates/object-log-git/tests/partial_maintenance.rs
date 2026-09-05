//! Opt-in maintenance hook for the local Spin partial-client fixture.
#![cfg(feature = "aws")]
use object_log::{
    CheckpointStatus, CollectionFinish, CollectionStart, Log, LogId, Options, ValidatedBackend,
};
use object_log_git::{ObjectFormat, Repository};
use object_store::{aws::AmazonS3Builder, path::Path};
use std::{env, sync::Arc};

#[tokio::test]
#[ignore = "invoked by check_partial.py against its isolated local MinIO namespace"]
async fn partial_fixture_checkpoint_and_gc() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = env::var("OBJECT_LOG_MINIO_ENDPOINT")?;
    let prefix = env::var("OBJECT_LOG_PARTIAL_PREFIX")?;
    if !endpoint.starts_with("http://127.0.0.1:") || !prefix.starts_with("partial-fixture-") {
        return Err("partial fixture only accepts its loopback test namespace".into());
    }
    let store = AmazonS3Builder::new()
        .with_endpoint(endpoint)
        .with_access_key_id(env::var("OBJECT_LOG_MINIO_ACCESS_KEY")?)
        .with_secret_access_key(env::var("OBJECT_LOG_MINIO_SECRET_KEY")?)
        .with_bucket_name(env::var("OBJECT_LOG_MINIO_BUCKET")?)
        .with_region("us-east-1")
        .with_allow_http(true)
        .with_virtual_hosted_style_request(false)
        .with_disable_bulk_delete(false)
        .build()?;
    let backend = ValidatedBackend::new(Arc::new(store), Path::from(prefix)).await?;
    let mut options = Options::default();
    if let Ok(refs) = env::var("OBJECT_LOG_GIT_MAX_OBJECT_REFS") {
        options.max_object_refs = refs.parse()?;
    }
    let log = Log::open_existing(&backend, &LogId::new("repository")?, options).await?;
    let format = match env::var("OBJECT_LOG_PARTIAL_FORMAT")?.as_str() {
        "sha1" => ObjectFormat::Sha1,
        "sha256" => ObjectFormat::Sha256,
        _ => return Err("format".into()),
    };
    let CheckpointStatus::Published(view) =
        Repository::open(&log, format).await?.checkpoint().await?
    else {
        return Err("checkpoint did not publish".into());
    };
    let CollectionStart::Installed(fenced, _) = log.start_collection(&view).await? else {
        return Err("fixture did not install collection".into());
    };
    let CollectionFinish::Complete(_, report) = log.resume_collection(&fenced).await? else {
        return Err("fixture did not complete collection".into());
    };
    eprintln!("partial {format:?}: checkpoint and collection complete: {report:?}");
    Ok(())
}
