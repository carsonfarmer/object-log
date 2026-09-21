use std::{error::Error as StdError, sync::Arc};

use bytes::Bytes;
use object_log::{
    CheckpointStatus, CommitStatus, Error, Log, LogId, Options, StagedObject, TransactionId,
    ValidatedBackend, View,
};
use object_store::{memory::InMemory, path::Path};

type TestResult<T = ()> = Result<T, Box<dyn StdError>>;

async fn open(limit: usize) -> TestResult<Log> {
    let backend =
        ValidatedBackend::new(Arc::new(InMemory::new()), Path::from("accounting")).await?;
    Ok(Log::open(
        &backend,
        &LogId::new("test")?,
        Options {
            max_collection_objects: limit,
            ..Options::default()
        },
    )
    .await?)
}

async fn publish(log: &Log, view: &View, roots: Vec<StagedObject>) -> TestResult<View> {
    let prepared = log.prepare(
        view,
        TransactionId::new(),
        Bytes::new(),
        Bytes::new(),
        roots,
    )?;
    let CommitStatus::Committed(view) = log.commit(prepared).await? else {
        return Err("publication did not complete".into());
    };
    Ok(view)
}

async fn checkpoint(log: &Log, view: &View, roots: Vec<StagedObject>) -> TestResult<View> {
    let through = view.tail().last().ok_or("missing tail")?;
    let CheckpointStatus::Published(view) = log
        .publish_checkpoint(view, through, Bytes::new(), roots)
        .await?
    else {
        return Err("checkpoint did not complete".into());
    };
    Ok(view)
}

#[tokio::test]
async fn admission_includes_commit_and_checkpoint_envelopes() -> TestResult {
    let log = open(3).await?;
    let view = log.load().await?;
    let leaf = log.put_object(&view, Bytes::from_static(b"leaf")).await?;
    let fitting = log.put_node(&view, Bytes::new(), vec![leaf]).await?;
    let oversized = log
        .put_node(&view, Bytes::new(), vec![fitting.clone()])
        .await?;
    assert!(matches!(
        log.prepare(
            &view,
            TransactionId::new(),
            Bytes::new(),
            Bytes::new(),
            vec![oversized.clone()]
        ),
        Err(Error::LimitExceeded("publication objects"))
    ));
    assert_eq!(log.load().await?.generation(), view.generation());
    let view = publish(&log, &view, vec![fitting.clone()]).await?;
    assert!(matches!(
        log.publish_checkpoint(&view, &view.tail()[0], Bytes::new(), vec![oversized])
            .await,
        Err(Error::LimitExceeded("publication objects"))
    ));
    assert_eq!(log.load().await?.generation(), view.generation());
    let view = checkpoint(&log, &view, vec![fitting]).await?;
    let _outcome = log.start_collection(&view).await?;
    Ok(())
}

#[tokio::test]
async fn zero_and_one_object_limits_apply_to_empty_publications() -> TestResult {
    let log = open(0).await?;
    let view = log.load().await?;
    assert!(matches!(
        log.prepare(
            &view,
            TransactionId::new(),
            Bytes::new(),
            Bytes::new(),
            Vec::new()
        ),
        Err(Error::LimitExceeded("publication objects"))
    ));
    let log = open(1).await?;
    let view = log.load().await?;
    let blob = log.put_object(&view, Bytes::new()).await?;
    assert!(matches!(
        log.prepare(
            &view,
            TransactionId::new(),
            Bytes::new(),
            Bytes::new(),
            vec![blob]
        ),
        Err(Error::LimitExceeded("publication objects"))
    ));
    let view = publish(&log, &view, Vec::new()).await?;
    let view = checkpoint(&log, &view, Vec::new()).await?;
    let _outcome = log.start_collection(&view).await?;
    Ok(())
}

#[tokio::test]
async fn shared_descendants_have_conservative_admission() -> TestResult {
    // This diamond has four unique graph objects, but five objects by path.
    // Its record fits only at six; collection will still deduplicate the leaf.
    for limit in [5, 6] {
        let log = open(limit).await?;
        let view = log.load().await?;
        let leaf = log.put_object(&view, Bytes::from_static(b"shared")).await?;
        let left = log
            .put_node(&view, Bytes::new(), vec![leaf.clone()])
            .await?;
        let right = log.put_node(&view, Bytes::new(), vec![leaf]).await?;
        let root = log.put_node(&view, Bytes::new(), vec![left, right]).await?;
        if limit == 5 {
            assert!(matches!(
                log.prepare(
                    &view,
                    TransactionId::new(),
                    Bytes::new(),
                    Bytes::new(),
                    vec![root]
                ),
                Err(Error::LimitExceeded("publication objects"))
            ));
        } else {
            let view = publish(&log, &view, vec![root]).await?;
            let _outcome = log.start_collection(&view).await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn admitted_states_need_checkpointing_when_their_tail_union_exceeds_collection() -> TestResult
{
    let log = open(3).await?;
    let view = log.load().await?;
    let first = log.put_object(&view, Bytes::from_static(b"first")).await?;
    let view = publish(&log, &view, vec![first]).await?;
    let latest = log.put_object(&view, Bytes::from_static(b"latest")).await?;
    let view = publish(&log, &view, vec![latest.clone()]).await?;
    assert!(matches!(
        log.start_collection(&view).await,
        Err(Error::LimitExceeded("collection live objects"))
    ));
    let view = checkpoint(&log, &view, vec![latest]).await?;
    let _outcome = log.start_collection(&view).await?;
    Ok(())
}
