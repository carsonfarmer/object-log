#[tokio::test]
async fn closure_marks_revisit_shared_history_and_keep_parent_proofs_separate() -> TestResult {
    use crate::closure::{Closure, Edges, WANTED, PRESENT, ANCESTRY};
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let blob = (Kind::Blob, b"shared".to_vec());
        let blob_id = id(format, &blob)?;
        let tree = tree(&[("100644", "file", blob_id)]);
        let tree_id = id(format, &tree)?;
        let first = commit(tree_id, &[], 1);
        let first_id = id(format, &first)?;
        let other = commit(tree_id, &[], 3);
        let other_id = id(format, &other)?;
        let second = commit(tree_id, &[other_id, first_id], 2);
        let second_id = id(format, &second)?;
        let repository = Repository::new(format, &[vec![blob, tree, first, other, second]]).await?;
        let before = repository.operation.live_bytes();
        let mut reader = Reader::new(&repository.log, &repository.view, &repository.catalog);
        let mut closure = Closure::new(&repository.operation)?;
        closure.walk(&mut reader, &[first_id], WANTED, Edges::All).await?;
        closure.walk(&mut reader, &[second_id], PRESENT, Edges::All).await?;
        for id in [first_id, tree_id, blob_id] {
            assert!(closure.marked(id, WANTED));
            assert!(closure.marked(id, PRESENT));
        }
        assert!(!closure.marked(second_id, WANTED));
        closure.walk(&mut reader, &[second_id], ANCESTRY, Edges::Commits).await?;
        assert!(closure.marked(first_id, ANCESTRY));
        assert!(!closure.marked(tree_id, ANCESTRY));
        assert!(!closure.marked(blob_id, ANCESTRY));
        assert!(closure.reaches_commit(&mut reader, second_id, first_id).await?);
        // The early match leaves the other merge parent pending; a new proof
        // must neither consume that frontier nor retain its ancestry mark.
        assert!(!closure.reaches_commit(&mut reader, first_id, other_id).await?);
        closure.verify_all(&mut reader).await?;
        assert!(closure.nodes.values().all(|node| node.verified));
        drop(closure); drop(reader);
        assert_eq!(repository.operation.live_bytes(), before);
        assert_eq!(repository.store.metrics().operation(StoreOperation::Put).requests, 0);
    }
    Ok(())
}

#[tokio::test]
async fn closure_checks_duplicate_link_kinds_and_releases_cancelled_walks() -> TestResult {
    use crate::closure::{Closure, Edges, CONNECTED};
    let format = ObjectFormat::Sha1;
    let blob = (Kind::Blob, b"leaf".to_vec());
    let blob_id = id(format, &blob)?;
    let root = tree(&[("100644", "a", blob_id), ("40000", "b", blob_id)]);
    let root_id = id(format, &root)?;
    let repository = Repository::new(format, &[vec![blob, root]]).await?;
    let before = repository.operation.live_bytes();
    let mut reader = Reader::new(&repository.log, &repository.view, &repository.catalog);
    let mut closure = Closure::new(&repository.operation)?;
    assert!(closure.walk(&mut reader, &[root_id], CONNECTED, Edges::All).await.is_err());
    drop(closure); drop(reader);
    assert_eq!(repository.operation.live_bytes(), before);
    repository.store.reset();
    let mut pause = repository.store.pause_next_get(object_log::sim::FailurePhase::Before);
    let mut reader = Reader::new(&repository.log, &repository.view, &repository.catalog);
    let mut closure = Closure::new(&repository.operation)?;
    let roots = [root_id];
    let mut pending = Box::pin(closure.walk(&mut reader, &roots, CONNECTED, Edges::All));
    assert!(tokio::select! { entered = pause.wait_until_entered() => entered, _ = &mut pending => false });
    drop(pending); drop(closure); drop(reader);
    assert!(!pause.release());
    assert_eq!(repository.operation.live_bytes(), before);
    Ok(())
}
