#![cfg(feature = "aws")]

mod support;

use std::{env, sync::Arc};

use object_log::{
    CheckpointStatus, CollectionFinish, CollectionStart, Log, LogId, Options, TransactionId,
    ValidatedBackend,
};
use object_log_git::{ObjectFormat, Repository};
use object_store::{
    aws::{AmazonS3, AmazonS3Builder},
    path::Path,
};
use support::{TestResult, fixture, publish};

const KIB: usize = 1_024;
const MIB: usize = KIB * KIB;

#[tokio::test]
#[ignore = "requires OBJECT_LOG_MINIO_* and the pinned local MinIO from scripts/test-minio.sh"]
async fn minio_git_push_checkpoint_collection_and_cold_recovery() -> TestResult {
    let backend = ValidatedBackend::new(
        Arc::new(build_minio()?),
        Path::from("object-log-git-local-tests"),
    )
    .await?;
    let log_id = LogId::new(format!("git-minio-{}", TransactionId::new()))?;
    let options = Options {
        max_object_bytes: 64 * KIB,
        max_collection_objects: 10_000,
        ..Options::default()
    };
    let log = Log::open(&backend, &log_id, options).await?;
    let directory = tempfile::tempdir()?;
    let live_bytes = 64 * KIB;
    let live = fixture("live", live_bytes, u64::try_from(live_bytes)?)?;
    let dead = fixture("dead", MIB, u64::try_from(MIB)?)?;
    assert!(dead.pack_bytes > live.pack_bytes);

    let first = Repository::open(&log, ObjectFormat::Sha1).await?;
    publish(
        first,
        "refs/heads/main",
        None,
        Some(live.target),
        Some(&live.pack),
    )
    .await?;
    let second = Repository::open(&log, ObjectFormat::Sha1).await?;
    publish(
        second,
        "refs/heads/dead",
        None,
        Some(dead.target),
        Some(&dead.pack),
    )
    .await?;
    let third = Repository::open(&log, ObjectFormat::Sha1).await?;
    publish(third, "refs/heads/dead", Some(dead.target), None, None).await?;

    let checkpoint = Repository::open(&log, ObjectFormat::Sha1).await?;
    let CheckpointStatus::Published(view) = checkpoint.checkpoint().await? else {
        return Err("Git checkpoint did not publish".into());
    };
    assert!(view.tail().is_empty());
    let CollectionStart::Installed(fenced, start) = log.start_collection(&view).await? else {
        return Err("Git collection did not install a deletion plan".into());
    };
    assert!(start.candidate_count() > 0);
    let CollectionFinish::Complete(_, finish) = log.resume_collection(&fenced).await? else {
        return Err("Git collection did not complete".into());
    };
    assert_eq!(finish.candidate_count(), start.candidate_count());
    assert_eq!(finish.delete_attempts(), start.candidate_count());

    drop(log);
    let log = Log::open(&backend, &log_id, options).await?;
    let recovered_path = directory.path().join("recovered");
    let recovered = Repository::open(&log, ObjectFormat::Sha1).await?;
    assert_eq!(recovered.refs().len(), 1);
    assert_eq!(
        recovered.refs().get(&b"refs/heads/main"[..]),
        Some(&live.target)
    );
    support::recover(recovered, &recovered_path, &live).await?;
    Ok(())
}

fn build_minio() -> TestResult<AmazonS3> {
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

fn required_env(name: &'static str) -> TestResult<String> {
    env::var(name).map_err(|_| format!("{name} is not set").into())
}
