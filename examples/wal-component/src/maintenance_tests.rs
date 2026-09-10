use super::*;
use bytes::Bytes;
use futures::TryStreamExt;
use object_log::{CollectionStart, Options, TransactionId};
use object_store::{ObjectStore, ObjectStoreExt, memory::InMemory, path::Path};

async fn session() -> SessionState {
    session_on(Arc::new(InMemory::new())).await
}
async fn session_on(store: Arc<dyn ObjectStore>) -> SessionState {
    let backend = object_log::ValidatedBackend::new(store, Path::from("maintenance-tests"))
        .await
        .unwrap();
    let log = Log::open(
        &backend,
        &object_log::LogId::new("repo").unwrap(),
        Options {
            max_collection_objects: 4,
            ..Options::default()
        },
    )
    .await
    .unwrap();
    let view = log.load().await.unwrap();
    SessionState {
        log,
        view,
        transport: transport::Transport::default(),
    }
}
async fn append(s: &mut SessionState, roots: Vec<StagedObject>) {
    let prepared = s
        .log
        .prepare(
            &s.view,
            TransactionId::new(),
            Bytes::new(),
            Bytes::new(),
            roots,
        )
        .unwrap();
    let CommitStatus::Committed(view) = s.log.commit(prepared).await.unwrap() else {
        panic!("append failed")
    };
    s.view = view;
}
#[tokio::test]
async fn checkpoint_and_resumed_batches_keep_live_objects() {
    let mut s = session().await;
    let live = s
        .log
        .put_object(&s.view, Bytes::from_static(b"keep"))
        .await
        .unwrap();
    let old = s
        .log
        .put_object(&s.view, Bytes::from_static(b"remove"))
        .await
        .unwrap();
    append(&mut s, vec![live.clone(), old.clone()]).await;
    assert!(matches!(
        maintenance::checkpoint(&s, vec![], vec![live.clone()])
            .await
            .unwrap(),
        MaintenanceState::Complete
    ));
    s.view = s.log.load().await.unwrap();
    for i in 0..8 {
        s.log
            .put_object(&s.view, Bytes::from(vec![i]))
            .await
            .unwrap();
    }
    // Leave an installed plan behind, as a stopped process would.
    assert!(matches!(
        s.log.start_collection(&s.view).await.unwrap(),
        CollectionStart::Installed(..)
    ));
    let mut complete = false;
    for _ in 0..8 {
        s.view = s.log.load().await.unwrap();
        let report = maintenance::collect(&s).await.unwrap();
        if matches!(report.state, MaintenanceState::Complete) {
            complete = true;
            break;
        }
        assert!(matches!(report.state, MaintenanceState::More));
    }
    assert!(complete);
    s.view = s.log.load().await.unwrap();
    assert_eq!(
        s.log.read_object(&s.view, live.reference()).await.unwrap(),
        b"keep"[..]
    );
    assert!(s.log.read_object(&s.view, old.reference()).await.is_err());
}
#[tokio::test]
async fn checkpoint_does_not_overwrite_a_concurrent_append() {
    let mut s = session().await;
    append(&mut s, vec![]).await;
    let stale = s.view.clone();
    append(&mut s, vec![]).await;
    let current = s.view.clone();
    s.view = stale;
    assert!(matches!(
        maintenance::checkpoint(&s, vec![], vec![]).await.unwrap(),
        MaintenanceState::Conflict
    ));
    assert_eq!(
        s.log.load().await.unwrap().generation(),
        current.generation()
    );
}

// Exercise raw materializer state: native tests cannot allocate WIT resources.
async fn latest(s: &SessionState) -> (View, Option<(Bytes, Vec<StagedObject>)>) {
    object_log::materialize(&s.log, s.view.clone(), &LatestCompleteState)
        .await
        .unwrap()
        .into_parts()
}

#[tokio::test]
async fn latest_complete_state_preserves_empty_tail_checkpoint_and_proofs() {
    let mut s = session().await;
    let (view, state) = latest(&s).await;
    assert_eq!(view.tail().len(), 0);
    assert!(state.is_none());
    let first = s
        .log
        .put_object(&s.view, Bytes::from_static(b"old"))
        .await
        .unwrap();
    append(&mut s, vec![first]).await;
    let last = s
        .log
        .put_object(&s.view, Bytes::from_static(b"latest"))
        .await
        .unwrap();
    append(&mut s, vec![last.clone()]).await;
    let (view, state) = latest(&s).await;
    assert_eq!(view.tail().len(), 2);
    let (data, roots) = state.unwrap();
    assert!(data.is_empty());
    assert_eq!(roots.len(), 1);
    assert_eq!(roots[0].reference(), last.reference());
    // Latest proofs are usable for checkpointing the exact recovered view.
    let through = view.tail().last().unwrap();
    let object_log::CheckpointStatus::Published(view) = s
        .log
        .publish_checkpoint(
            &view,
            through,
            Bytes::from_static(b"complete snapshot"),
            roots,
        )
        .await
        .unwrap()
    else {
        panic!("checkpoint failed")
    };
    s.view = view;
    let (view, state) = latest(&s).await;
    assert_eq!(view.tail().len(), 0);
    let (data, roots) = state.unwrap();
    assert_eq!(data, b"complete snapshot"[..]);
    assert_eq!(roots[0].reference(), last.reference());
    let newer = s
        .log
        .put_object(&s.view, Bytes::from_static(b"newer"))
        .await
        .unwrap();
    append(&mut s, vec![newer.clone()]).await;
    let (view, state) = latest(&s).await;
    assert_eq!(view.tail().len(), 1);
    let (data, roots) = state.unwrap();
    assert!(data.is_empty());
    assert_eq!(roots.len(), 1);
    assert_eq!(roots[0].reference(), newer.reference());
}

#[tokio::test]
async fn latest_state_does_not_skip_corrupt_older_commit_or_checkpoint() {
    for checkpoint in [false, true] {
        let store = Arc::new(InMemory::new());
        let mut s = session_on(store.clone()).await;
        append(&mut s, vec![]).await;
        if checkpoint {
            assert!(matches!(
                maintenance::checkpoint(&s, b"old checkpoint".to_vec(), vec![])
                    .await
                    .unwrap(),
                MaintenanceState::Complete
            ));
            s.view = s.log.load().await.unwrap();
        }
        let suffix = if checkpoint {
            "/checkpoints/"
        } else {
            "/commits/"
        };
        let objects: Vec<_> = store.list(None).try_collect().await.unwrap();
        let key = objects
            .into_iter()
            .find(|meta| meta.location.as_ref().contains(suffix))
            .unwrap_or_else(|| panic!("missing {suffix} object"))
            .location;
        append(&mut s, vec![]).await;
        let valid = latest(&s).await;
        assert!(valid.1.is_some());
        store
            .put(&key, Bytes::from_static(b"corrupt earlier record").into())
            .await
            .unwrap();
        assert!(matches!(
            object_log::materialize(&s.log, s.view.clone(), &LatestCompleteState).await,
            Err(object_log::MaterializeError::Log(
                object_log::Error::CorruptObject
            ))
        ));
    }
}
