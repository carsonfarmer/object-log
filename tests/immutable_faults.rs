#![cfg(feature = "test-util")]

use std::sync::Arc;

use bytes::Bytes;
use futures::TryStreamExt;
use object_log::sim::{FailurePhase, FaultStore, Operation};
use object_log::{
    CommitStatus, Error, Log, LogId, Options, Resolution, TransactionId, ValidatedBackend,
};
use object_store::{ObjectStore, memory::InMemory, path::Path};

#[cfg(feature = "aws")]
mod support;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn immutable_create_faults_never_publish_and_tokens_recover_exactly_once() -> TestResult {
    immutable_create_faults(Arc::new(InMemory::new())).await
}

#[cfg(feature = "aws")]
#[tokio::test]
#[ignore = "requires local MinIO"]
async fn minio_immutable_create_faults() -> TestResult {
    immutable_create_faults(Arc::new(support::minio::build_minio()?)).await
}

async fn immutable_create_faults(store: Arc<dyn ObjectStore>) -> TestResult {
    for phase in [FailurePhase::Before, FailurePhase::After] {
        let faults = FaultStore::new(Arc::clone(&store));
        let backend = ValidatedBackend::new(Arc::new(faults.clone()), Path::from("faults")).await?;
        let id = LogId::new(format!("immutable-{}", uuid::Uuid::new_v4().simple()))?;
        let log = Log::open(&backend, &id, Options::default()).await?;
        let initial = log.load().await?;

        faults.reset();
        faults.fail_next(Operation::Put, phase);
        assert!(matches!(
            log.put_object(&initial, Bytes::from_static(b"payload"))
                .await,
            Err(Error::Store(_))
        ));
        assert_eq!(
            faults.metrics().operation(Operation::Put).visible_mutations,
            u64::from(phase == FailurePhase::After)
        );
        assert!(faults.pending_failures().is_empty());
        assert_eq!(log.load().await?.generation(), 0);
        let blob = log
            .put_object(&initial, Bytes::from_static(b"payload"))
            .await?;
        let reference = blob.reference().clone();
        let transaction = TransactionId::new();
        let prepared = log.prepare(
            &initial,
            transaction,
            Bytes::from_static(b"operation"),
            Bytes::from_static(b"result"),
            vec![blob],
        )?;
        let token = prepared.recovery_token()?;

        faults.reset();
        faults.fail_next(Operation::Put, phase);
        assert!(matches!(log.commit(prepared).await, Err(Error::Store(_))));
        let metrics = faults.metrics();
        assert_eq!(metrics.operation(Operation::Put).requests, 1);
        assert_eq!(
            metrics.operation(Operation::Put).visible_mutations,
            u64::from(phase == FailurePhase::After)
        );
        assert!(
            metrics
                .events
                .iter()
                .all(|event| !event.path.ends_with("index.cbor"))
        );
        assert!(faults.pending_failures().is_empty());
        assert_eq!(log.load().await?.generation(), 0);
        drop(log);

        let cold = Log::open_existing(&backend, &id, Options::default()).await?;
        for _ in 0..2 {
            let Resolution::Committed(view) = cold.resume(&token).await? else {
                return Err("retry did not recover the exact candidate".into());
            };
            let records = cold.read_tail(&view).await?;
            assert_eq!(records.len(), 1);
            assert_eq!(records[0].reference().transaction_id(), transaction);
            assert_eq!(records[0].operation().as_ref(), b"operation");
            assert_eq!(records[0].result().as_ref(), b"result");
            assert_eq!(
                cold.read_object(&view, &reference).await?.as_ref(),
                b"payload"
            );
        }
        // A newly prepared retry is distinct work; the old view must still lose.
        let stale = cold.prepare(
            &initial,
            TransactionId::new(),
            Bytes::new(),
            Bytes::new(),
            vec![],
        )?;
        assert!(matches!(
            cold.commit(stale).await?,
            CommitStatus::Conflict(_)
        ));
        let scope = Path::from("faults")
            .join("v1")
            .join("logs")
            .join(id.as_str());
        let objects = store.list(Some(&scope)).map_ok(|meta| meta.location);
        store
            .delete_stream(Box::pin(objects))
            .try_collect::<Vec<_>>()
            .await?;
    }
    Ok(())
}
