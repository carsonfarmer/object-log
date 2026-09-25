use std::{error::Error as StdError, sync::Arc};

use bytes::Bytes;
use object_log::{
    CollectionStart, CommitStatus, Log, LogId, Options, TransactionId, ValidatedBackend,
    sim::{FaultStore, Operation},
};
use object_log_kv::{KvCommand, KvStore, Limits};
use object_store::{memory::InMemory, path::Path};

#[tokio::test]
async fn batch_reads_the_active_collection_plan_once() -> Result<(), Box<dyn StdError>> {
    let faults = FaultStore::new(InMemory::new());
    let backend =
        ValidatedBackend::new(Arc::new(faults.clone()), Path::from("kv-plan-cache")).await?;
    let log = Log::open(&backend, &LogId::new("kv")?, Options::default()).await?;
    let initial = log.load().await?;
    log.put_object(&initial, Bytes::from_static(b"collectible"))
        .await?;
    let CollectionStart::Installed(_, _) = log.start_collection(&initial).await? else {
        return Err("collection did not install a plan".into());
    };

    let kv = KvStore::new(log, Limits::default());
    let snapshot = kv.snapshot().await?;
    let commands = (0..16)
        .map(|key| KvCommand::Set {
            key: Bytes::from(vec![key]),
            value: Bytes::from_static(b"value"),
        })
        .collect::<Vec<_>>();
    faults.reset();
    let prepared = snapshot.prepare(TransactionId::new(), &commands).await?;
    assert!(matches!(
        kv.log().commit(prepared).await?,
        CommitStatus::Committed(_)
    ));
    assert_eq!(faults.metrics().operation(Operation::Get).requests, 1);
    Ok(())
}
