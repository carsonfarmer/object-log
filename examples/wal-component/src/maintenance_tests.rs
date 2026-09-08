use super::*;
use bytes::Bytes;
use object_log::{CollectionStart, Options, TransactionId};
use object_store::{memory::InMemory, path::Path};

async fn session() -> SessionState {
    let backend = object_log::ValidatedBackend::new(
        Arc::new(InMemory::new()),
        Path::from("maintenance-tests"),
    )
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
