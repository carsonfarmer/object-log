#![cfg(all(feature = "aws", feature = "test-util"))]

use std::env;
use std::error::Error as StdError;
use std::sync::Arc;

use bytes::Bytes;
use futures::TryStreamExt;
use object_log::sim::{Failure, FailurePhase, FaultStore, Operation};
use object_log::{
    CheckpointStatus, CollectionFinish, CollectionStart, CommitStatus, Log, LogId, Options,
    Resolution, TransactionId, ValidatedBackend,
};
use object_store::ObjectStore;
use object_store::aws::{AmazonS3, AmazonS3Builder};
use object_store::path::Path;
use uuid::Uuid;

type TestResult = Result<(), Box<dyn StdError>>;

#[tokio::test]
#[ignore = "requires the pinned local MinIO server started by make minio-test"]
async fn minio_passes_recovery_checkpoint_and_gc_flow() -> TestResult {
    let backend = build_minio()?;
    let faults = FaultStore::new(backend);
    let store: Arc<dyn ObjectStore> = Arc::new(faults.clone());
    let log_id = LogId::new(format!("minio-{}", Uuid::new_v4().simple()))?;
    let root = Path::from("object-log-local-tests");
    let backend = ValidatedBackend::new(store, root.clone()).await?;
    let scoped = backend.scope(&log_id);
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
        .publish_checkpoint(
            &committed,
            &through,
            Bytes::from_static(b"minio snapshot"),
            Vec::new(),
        )
        .await?
    else {
        return Err("MinIO checkpoint returned a conflict".into());
    };
    assert!(compacted.tail().is_empty());

    drop(log);
    let reopened = Log::open(backend.scope(&log_id), Options::default()).await?;
    let recovered = reopened.load().await?;
    assert_eq!(
        reopened
            .read_checkpoint(&recovered)
            .await?
            .ok_or("MinIO checkpoint is missing")?
            .snapshot(),
        b"minio snapshot".as_slice()
    );
    assert!(reopened.read_tail(&recovered).await?.is_empty());

    let direct: Arc<dyn ObjectStore> = Arc::new(build_minio()?);
    let gc_id = LogId::new(format!("minio-gc-{}", Uuid::new_v4().simple()))?;
    let gc_backend = ValidatedBackend::new(Arc::clone(&direct), root.clone()).await?;
    let gc_log = Log::open(gc_backend.scope(&gc_id), Options::default()).await?;
    let source = gc_log.load().await?;
    for _ in 0..1_001 {
        gc_log.put_object(Bytes::from_static(b"x")).await?;
    }
    let CollectionStart::Installed(fenced, start) = gc_log.start_collection(&source).await? else {
        return Err("MinIO collection did not install".into());
    };
    assert_eq!(
        (start.candidate_count(), start.candidate_bytes()),
        (1_001, 1_001)
    );
    assert_eq!(start.delete_attempts(), 0);
    let CollectionFinish::Complete(_, finish) = gc_log.resume_collection(&fenced).await? else {
        return Err("MinIO collection did not finish".into());
    };
    assert_eq!(
        (finish.candidate_count(), finish.candidate_bytes()),
        (1_001, 1_001)
    );
    assert_eq!(finish.delete_attempts(), 1_001);

    let scope = root.join("v1").join("logs").join(gc_id.as_str());
    let remaining = direct.list(Some(&scope)).try_collect::<Vec<_>>().await?;
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].location, scope.join("index.cbor"));
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
        .with_disable_bulk_delete(false)
        .build()?)
}

fn required_env(name: &'static str) -> Result<String, Box<dyn StdError>> {
    env::var(name).map_err(|_| format!("{name} is not set").into())
}
