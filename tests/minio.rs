#![cfg(all(feature = "aws", feature = "test-util"))]

use std::env;
use std::error::Error as StdError;
use std::sync::Arc;

use bytes::Bytes;
use object_log::sim::{Failure, FailurePhase, FaultStore, Operation};
use object_log::{
    CheckpointStatus, CommitStatus, Log, LogId, Options, Resolution, ScopedStore, TransactionId,
};
use object_store::ObjectStore;
use object_store::aws::{AmazonS3, AmazonS3Builder};
use object_store::path::Path;
use uuid::Uuid;

type TestResult = Result<(), Box<dyn StdError>>;

#[tokio::test]
#[ignore = "requires the pinned local MinIO server started by make minio-test"]
async fn minio_passes_protocol_recovery_and_checkpoint_flow() -> TestResult {
    let backend = build_minio()?;
    let faults = FaultStore::new(backend);
    let store: Arc<dyn ObjectStore> = Arc::new(faults.clone());
    let log_id = LogId::new(format!("minio-{}", Uuid::new_v4().simple()))?;
    let root = Path::from("object-log-local-tests");
    let scoped = ScopedStore::new(Arc::clone(&store), root.clone(), &log_id);
    let log = Log::open(scoped, Options::default()).await?;
    let empty = log.load().await?;

    faults.reset();
    let occurrence = faults
        .metrics()
        .operation(Operation::Put)
        .requests
        .saturating_add(2);
    faults.schedule(Failure {
        operation: Operation::Put,
        occurrence,
        phase: FailurePhase::After,
    });
    let prepared = log.prepare(
        empty.cursor(),
        TransactionId::new(),
        Bytes::from_static(b"minio operation"),
        Bytes::from_static(b"minio result"),
        Vec::new(),
    )?;
    let CommitStatus::Pending(pending) = log.commit(prepared).await? else {
        return Err("lost MinIO update response did not produce pending".into());
    };
    let Resolution::Committed(committed) = log.resolve(pending).await? else {
        return Err("MinIO pending update did not resolve as committed".into());
    };
    let records = log.read_tail(&committed).await?;
    assert_eq!(records.len(), 1);
    assert_eq!(
        records[0].operation(),
        &Bytes::from_static(b"minio operation")
    );

    let through = committed.tail()[0].clone();
    let CheckpointStatus::Published(compacted) = log
        .publish_checkpoint(&committed, &through, Bytes::from_static(b"minio snapshot"))
        .await?
    else {
        return Err("MinIO checkpoint returned a conflict".into());
    };
    assert!(compacted.tail().is_empty());

    drop(log);
    let reopened = Log::open(ScopedStore::new(store, root, &log_id), Options::default()).await?;
    let recovered = reopened.load().await?;
    assert_eq!(
        reopened.read_checkpoint(&recovered).await?,
        Some(Bytes::from_static(b"minio snapshot"))
    );
    assert!(reopened.read_tail(&recovered).await?.is_empty());
    Ok(())
}

fn build_minio() -> Result<AmazonS3, Box<dyn StdError>> {
    Ok(AmazonS3Builder::new()
        .with_endpoint(required_env("OBJECT_LOG_MINIO_ENDPOINT")?)
        .with_access_key_id(required_env("OBJECT_LOG_MINIO_ACCESS_KEY")?)
        .with_secret_access_key(required_env("OBJECT_LOG_MINIO_SECRET_KEY")?)
        .with_bucket_name(required_env("OBJECT_LOG_MINIO_BUCKET")?)
        .with_region("us-east-1")
        .with_allow_http(true)
        .with_virtual_hosted_style_request(false)
        .build()?)
}

fn required_env(name: &'static str) -> Result<String, Box<dyn StdError>> {
    env::var(name).map_err(|_| format!("{name} is not set").into())
}
