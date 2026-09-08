#![cfg(feature = "test-util")]

mod support;
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use object_log::sim::{FaultStore, Operation};
use object_log::{
    CheckpointStatus, CommitStatus, Log, LogId, Options, TransactionId, ValidatedBackend,
};
use object_store::{ObjectStore, memory::InMemory, path::Path};
use std::{error::Error, sync::Arc, time::Instant};

type TestResult = Result<(), Box<dyn Error>>;

#[tokio::test]
async fn mature_tail_memory() -> TestResult {
    measure(FaultStore::new(InMemory::new())).await
}

#[cfg(feature = "aws")]
#[tokio::test]
#[ignore = "requires a local MinIO service"]
async fn mature_tail_minio() -> TestResult {
    measure(FaultStore::new(support::minio::build_minio()?)).await
}

async fn measure(store: FaultStore) -> TestResult {
    let prefix = Path::from(format!("mature-tail-{}", uuid::Uuid::new_v4()));
    let backend = ValidatedBackend::new(Arc::new(store.clone()), prefix.clone()).await?;
    for external in [false, true] {
        let id = LogId::new(if external { "external" } else { "inline" })?;
        let log = Log::open(&backend, &id, Options::default()).await?;
        let mut view = log.load().await?;
        // Establish an authenticated starting view before local appends.
        assert!(log.read_tail(&view).await?.is_empty());
        for value in 0_u64..1000 {
            let payload = Bytes::from(vec![u8::try_from(value % 256)?; 4096]);
            let (operation, objects) = if external {
                (
                    Bytes::copy_from_slice(&value.to_le_bytes()),
                    vec![log.put_object(&view, payload).await?],
                )
            } else {
                (payload, Vec::new())
            };
            let prepared = log.prepare(
                &view,
                TransactionId::new(),
                operation,
                Bytes::new(),
                objects,
            )?;
            let CommitStatus::Committed(next) = log.commit(prepared).await? else {
                return Err("uncontended append did not commit".into());
            };
            view = next;
        }
        store.reset();
        let started = Instant::now();
        let cold = Log::open_existing(&backend, &id, Options::default()).await?;
        let cold_view = cold.load().await?;
        let records = cold.read_tail(&cold_view).await?;
        assert_eq!(records.len(), 1000);
        for (index, record) in records.iter().enumerate() {
            let expected = if external {
                u64::try_from(index)?.to_le_bytes().to_vec()
            } else {
                vec![u8::try_from(index % 256)?; 4096]
            };
            assert_eq!(record.operation().as_ref(), expected);
        }
        report("cold metadata recovery", external, &store, started);

        store.reset();
        let started = Instant::now();
        let through = view.tail().last().ok_or("empty history")?;
        assert!(matches!(
            log.publish_checkpoint(&view, through, Bytes::from_static(b"snapshot"), vec![])
                .await?,
            CheckpointStatus::Published(_)
        ));
        report("warm checkpoint", external, &store, started);
        assert_eq!(store.metrics().operation(Operation::Get).requests, 0);
    }
    let paths = store
        .list(Some(&prefix))
        .map_ok(|item| item.location)
        .boxed();
    store.delete_stream(paths).try_collect::<Vec<_>>().await?;
    Ok(())
}

fn report(label: &str, external: bool, store: &FaultStore, started: Instant) {
    let metrics = store.metrics();
    println!(
        "{label}, external={external}: GET={} PUT={} downloaded={} uploaded={} elapsed={:?}",
        metrics.operation(Operation::Get).requests,
        metrics.operation(Operation::Put).requests,
        metrics.downloaded_bytes(),
        metrics.uploaded_bytes(),
        started.elapsed()
    );
}
