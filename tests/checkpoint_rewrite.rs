#![cfg(feature = "test-util")]

#[path = "support/pause.rs"]
mod pause;

use std::error::Error as StdError;
use std::sync::Arc;

use bytes::Bytes;
use futures::TryStreamExt;
use object_log::sim::{Failure, FailurePhase, FaultStore, Operation};
use object_log::{
    CheckpointResolution, CheckpointStatus, CollectionFinish, CollectionStart, CommitStatus,
    Digest, Error, HistoryItem, Log, LogId, Options, Resolution, RetentionId, RetentionStatus,
    StagedObject, TransactionId, ValidatedBackend, View,
};
use object_store::{
    ObjectStore, ObjectStoreExt, local::LocalFileSystem, memory::InMemory, path::Path,
};

type TestResult = Result<(), Box<dyn StdError>>;

struct Fixture {
    backend: ValidatedBackend,
    log: Log,
    id: LogId,
    options: Options,
    raw: Arc<dyn ObjectStore>,
}

impl Fixture {
    async fn new(raw: Arc<dyn ObjectStore>, id: &str, options: Options) -> Result<Self, Error> {
        let backend = ValidatedBackend::new(Arc::clone(&raw), Path::from("rewrite-tests")).await?;
        let id = LogId::new(id)?;
        let log = Log::open(&backend, &id, options).await?;
        Ok(Self {
            backend,
            log,
            id,
            options,
            raw,
        })
    }

    async fn cold(&self) -> Result<Log, Error> {
        Log::open_existing(&self.backend, &self.id, self.options).await
    }

    async fn location(&self, kind: &str, digest: Digest) -> Result<Path, Box<dyn StdError>> {
        let prefix = Path::from(format!("rewrite-tests/v1/logs/{}/data", self.id.as_str()));
        let marker = format!("/{kind}/");
        let digest = digest.to_string();
        self.raw
            .list(Some(&prefix))
            .try_filter(|meta| {
                std::future::ready(
                    meta.location.as_ref().contains(&marker)
                        && meta.location.as_ref().ends_with(&digest),
                )
            })
            .map_ok(|meta| meta.location)
            .try_next()
            .await?
            .ok_or_else(|| "immutable object path missing".into())
    }
}

async fn commit(log: &Log, view: &View, roots: Vec<StagedObject>) -> Result<View, Error> {
    let prepared = log.prepare(
        view,
        TransactionId::new(),
        Bytes::new(),
        Bytes::new(),
        roots,
    )?;
    let CommitStatus::Committed(view) = log.commit(prepared).await? else {
        return Err(Error::InvalidFormat("test commit failed".into()));
    };
    Ok(view)
}

async fn checkpoint(log: &Log, view: &View, roots: Vec<StagedObject>) -> Result<View, Error> {
    let CheckpointStatus::Published(view) = log
        .publish_checkpoint(view, view.tail().last(), Bytes::from_static(b"base"), roots)
        .await?
    else {
        return Err(Error::InvalidFormat("test checkpoint failed".into()));
    };
    Ok(view)
}

async fn collect_all(log: &Log, mut view: View) -> Result<View, Error> {
    for _ in 0..8 {
        match log.start_collection(&view).await? {
            CollectionStart::Empty(_) => return Ok(view),
            CollectionStart::Installed(fenced, _) | CollectionStart::Active(fenced) => {
                let CollectionFinish::Complete(current, _) = log.resume_collection(&fenced).await?
                else {
                    return Err(Error::InvalidFormat("collection failed".into()));
                };
                view = current;
            }
            _ => return Err(Error::InvalidFormat("collection did not start".into())),
        }
    }
    Err(Error::InvalidFormat("collection did not finish".into()))
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn rewrite_after_full_collection_and_cold_open_preserves_outcomes() -> TestResult {
    let directory = tempfile::tempdir()?;
    let raw: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    for window in [0, 2] {
        let fixture = Fixture::new(
            Arc::clone(&raw),
            &format!("cold-{window}"),
            Options {
                resolution_window: window,
                ..Options::default()
            },
        )
        .await?;
        let log = &fixture.log;
        let empty = log.load().await?;
        let live = log.put_object(&empty, Bytes::from_static(b"live")).await?;
        let dead = log.put_object(&empty, Bytes::from_static(b"dead")).await?;
        let old_root = log
            .put_node(
                &empty,
                Bytes::from_static(b"catalog"),
                vec![live.clone(), dead.clone()],
            )
            .await?;
        let prepared = log.prepare(
            &empty,
            TransactionId::new(),
            Bytes::new(),
            Bytes::new(),
            vec![old_root.clone()],
        )?;
        let token = prepared.recovery_token()?;
        let CommitStatus::Committed(committed) = log.commit(prepared).await? else {
            return Err("initial commit failed".into());
        };
        let commit_path = fixture
            .location("commits", committed.tail()[0].digest())
            .await?;
        let compacted = checkpoint(log, &committed, vec![old_root.clone()]).await?;
        let old_checkpoint = compacted.checkpoint().ok_or("checkpoint absent")?.clone();
        let old_checkpoint_path = fixture
            .location("checkpoints", old_checkpoint.object().digest())
            .await?;
        let old_root_path = fixture
            .location("nodes", old_root.reference().digest())
            .await?;
        let dead_path = fixture.location("blobs", dead.reference().digest()).await?;
        collect_all(log, compacted).await?;
        assert!(matches!(
            raw.get(&commit_path).await,
            Err(object_store::Error::NotFound { .. })
        ));

        let cold = fixture.cold().await?;
        let source = cold.load().await?;
        assert!(source.tail().is_empty());
        let mut cursor = object_log::history(&cold, source.clone())?;
        let Some(HistoryItem::Checkpoint(base)) = cursor.next().await? else {
            return Err("cold checkpoint missing".into());
        };
        let (_, children) = cold.read_staged_node(&source, &base.proofs()[0]).await?;
        let root = cold
            .put_node(
                &source,
                Bytes::from_static(b"pruned catalog"),
                vec![children[0].clone()],
            )
            .await?;
        let CheckpointStatus::Published(rewritten) = cold
            .publish_checkpoint(
                &source,
                None,
                Bytes::from_static(b"pruned snapshot"),
                vec![root.clone()],
            )
            .await?
        else {
            return Err("rewrite failed".into());
        };
        let replacement = rewritten.checkpoint().ok_or("replacement absent")?;
        assert_eq!(
            replacement.through_sequence(),
            old_checkpoint.through_sequence()
        );
        assert_eq!(
            replacement.through_commit(),
            old_checkpoint.through_commit()
        );
        assert_eq!(rewritten.generation(), source.generation() + 1);
        assert_eq!(rewritten.collection_epoch(), source.collection_epoch());
        assert!(rewritten.tail().is_empty());
        match cold.resume(&token).await? {
            Resolution::Expired(_) if window == 0 => {}
            Resolution::Committed(_) if window > 0 => {}
            _ => return Err("rewrite changed the commit resolution window".into()),
        }

        let fresh = fixture.cold().await?;
        let current = collect_all(&fresh, fresh.load().await?).await?;
        assert_eq!(
            fresh
                .read_checkpoint(&current)
                .await?
                .ok_or("checkpoint missing")?
                .snapshot(),
            b"pruned snapshot".as_slice()
        );
        assert_eq!(
            fresh.read_object(&current, live.reference()).await?,
            Bytes::from_static(b"live")
        );
        assert_eq!(
            fresh
                .read_node(&current, root.reference())
                .await?
                .children(),
            std::slice::from_ref(live.reference())
        );
        for path in [old_checkpoint_path, old_root_path, dead_path] {
            assert!(matches!(
                raw.get(&path).await,
                Err(object_store::Error::NotFound { .. })
            ));
        }
        match fresh.resume(&token).await? {
            Resolution::Expired(_) if window == 0 => {}
            Resolution::Committed(_) if window > 0 => {}
            _ => return Err("collection changed retained outcome evidence".into()),
        }

        // The filesystem backend cannot CAS a head. Exercise read-only cold
        // recovery from the exact durable bytes after memory publication/GC.
        let filesystem: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(directory.path())?);
        let metadata = raw.list(None).try_collect::<Vec<_>>().await?;
        for object in metadata {
            let bytes = raw.get(&object.location).await?.bytes().await?;
            filesystem.put(&object.location, bytes.into()).await?;
        }
        let file_backend =
            ValidatedBackend::assume_validated(filesystem, Path::from("rewrite-tests"));
        let file_log = Log::open_existing(&file_backend, &fixture.id, fixture.options).await?;
        let file_view = file_log.load().await?;
        let mut history = object_log::history(&file_log, file_view.clone())?;
        let Some(HistoryItem::Checkpoint(base)) = history.next().await? else {
            return Err("filesystem base missing".into());
        };
        assert_eq!(base.record().snapshot(), b"pruned snapshot".as_slice());
        assert!(history.next().await?.is_none());
        assert_eq!(
            file_log
                .read_node(&file_view, root.reference())
                .await?
                .children(),
            std::slice::from_ref(live.reference())
        );
        assert_eq!(
            file_log.read_object(&file_view, live.reference()).await?,
            Bytes::from_static(b"live")
        );
    }
    Ok(())
}

#[tokio::test]
async fn rewrite_rejects_missing_base_and_corrupt_old_checkpoint() -> TestResult {
    for corrupt in [false, true] {
        let raw = Arc::new(InMemory::new());
        let fixture = Fixture::new(
            raw.clone(),
            &format!("invalid-{corrupt}"),
            Options::default(),
        )
        .await?;
        let log = &fixture.log;
        let empty = log.load().await?;
        assert!(matches!(
            log.publish_checkpoint(&empty, None, Bytes::new(), vec![])
                .await,
            Err(Error::InvalidFormat(_))
        ));
        assert_eq!(log.load().await?.generation(), empty.generation());
        let committed = commit(log, &empty, vec![]).await?;
        let source = checkpoint(log, &committed, vec![]).await?;
        let path = fixture
            .location(
                "checkpoints",
                source.checkpoint().ok_or("base missing")?.object().digest(),
            )
            .await?;
        if corrupt {
            raw.put(&path, Bytes::from_static(b"corrupt").into())
                .await?;
        } else {
            raw.delete(&path).await?;
        }
        assert!(matches!(
            log.publish_checkpoint(&source, None, Bytes::new(), vec![])
                .await,
            Err(Error::CorruptObject)
        ));
        assert_eq!(log.load().await?.checkpoint(), source.checkpoint());
        assert_eq!(log.load().await?.generation(), source.generation());
    }
    Ok(())
}

#[tokio::test]
async fn rewrite_reconciles_a_retained_concurrent_push_without_hiding_its_root() -> TestResult {
    let faults = FaultStore::new(InMemory::new());
    let fixture = Fixture::new(
        Arc::new(faults.clone()),
        "retained-push",
        Options::default(),
    )
    .await?;
    let source = commit(&fixture.log, &fixture.log.load().await?, vec![]).await?;
    let source = checkpoint(&fixture.log, &source, vec![]).await?;
    let old_boundary = source.checkpoint().ok_or("base missing")?.clone();
    let writer = fixture.cold().await?;
    let rewriter = fixture.log.clone();
    faults.reset();
    let mut gate = faults.pause_put_at(2, FailurePhase::Before);
    let rewrite = tokio::spawn(async move {
        rewriter
            .publish_checkpoint(&source, None, Bytes::from_static(b"pruned"), vec![])
            .await
    });
    pause::entered(gate.wait_until_entered()).await?;
    let retention = RetentionId::new();
    let RetentionStatus::Applied(retained) =
        writer.retain(&writer.load().await?, retention).await?
    else {
        return Err("retention failed".into());
    };
    let winning_root = writer
        .put_object(&retained, Bytes::from_static(b"winning Git root"))
        .await?;
    let pushed = commit(&writer, &retained, vec![winning_root.clone()]).await?;
    assert!(gate.release());
    let CheckpointStatus::Published(current) = rewrite.await?? else {
        return Err("rewrite failed to reconcile push".into());
    };
    assert_eq!(current.tail(), pushed.tail());
    assert_eq!(
        current
            .checkpoint()
            .ok_or("replacement missing")?
            .through_commit(),
        old_boundary.through_commit()
    );
    assert!(matches!(
        writer.start_collection(&current).await?,
        CollectionStart::Retained(_)
    ));
    let mut cursor = object_log::history(&writer, current.clone())?;
    assert!(matches!(
        cursor.next().await?,
        Some(HistoryItem::Checkpoint(_))
    ));
    let Some(HistoryItem::Commit(winner)) = cursor.next().await? else {
        return Err("winning push missing from recovery".into());
    };
    assert_eq!(
        winner.record().objects(),
        std::slice::from_ref(winning_root.reference())
    );
    let RetentionStatus::Applied(released) = writer.release_retention(&current, retention).await?
    else {
        return Err("release failed".into());
    };
    let collected = collect_all(&writer, released).await?;
    assert_eq!(
        writer
            .read_object(&collected, winning_root.reference())
            .await?,
        Bytes::from_static(b"winning Git root")
    );
    Ok(())
}

#[tokio::test]
async fn stale_rewrite_preserves_a_competing_checkpoint() -> TestResult {
    let fixture = Fixture::new(
        Arc::new(InMemory::new()),
        "checkpoint-winner",
        Options::default(),
    )
    .await?;
    let committed = commit(&fixture.log, &fixture.log.load().await?, vec![]).await?;
    let source = checkpoint(&fixture.log, &committed, vec![]).await?;
    let winner = fixture.cold().await?;
    let CheckpointStatus::Published(winning) = winner
        .publish_checkpoint(&source, None, Bytes::from_static(b"winner"), vec![])
        .await?
    else {
        return Err("winner did not publish".into());
    };
    let CheckpointStatus::Conflict(current) = fixture
        .log
        .publish_checkpoint(&source, None, Bytes::from_static(b"loser"), vec![])
        .await?
    else {
        return Err("stale rewrite did not conflict".into());
    };
    assert_eq!(current.checkpoint(), winning.checkpoint());
    assert_eq!(
        fixture
            .log
            .read_checkpoint(&current)
            .await?
            .ok_or("base missing")?
            .snapshot(),
        b"winner".as_slice()
    );
    Ok(())
}

#[tokio::test]
async fn uncertain_rewrite_resolves_exactly_after_cold_open_with_zero_or_nonzero_window()
-> TestResult {
    for window in [0, 2] {
        for phase in [FailurePhase::Before, FailurePhase::After] {
            let faults = FaultStore::new(InMemory::new());
            let fixture = Fixture::new(
                Arc::new(faults.clone()),
                &format!("pending-{window}-{phase:?}"),
                Options {
                    resolution_window: window,
                    ..Options::default()
                },
            )
            .await?;
            let committed = commit(&fixture.log, &fixture.log.load().await?, vec![]).await?;
            let source = checkpoint(&fixture.log, &committed, vec![]).await?;
            let boundary = source.checkpoint().ok_or("base missing")?.clone();
            let root = fixture
                .log
                .put_object(&source, Bytes::from_static(b"replacement root"))
                .await?;
            faults.reset();
            faults.schedule(Failure {
                operation: Operation::Put,
                occurrence: 2,
                phase,
            });
            let CheckpointStatus::Pending(pending) = fixture
                .log
                .publish_checkpoint(
                    &source,
                    None,
                    Bytes::from_static(b"exact snapshot"),
                    vec![root.clone()],
                )
                .await?
            else {
                return Err("uncertain rewrite lost pending evidence".into());
            };
            faults.reset();
            let cold = fixture.cold().await?;
            faults.reset();
            for occurrence in 1..=3 {
                faults.schedule(Failure {
                    operation: Operation::Get,
                    occurrence,
                    phase: FailurePhase::Before,
                });
            }
            let CheckpointResolution::StillPending(pending) =
                cold.resolve_checkpoint(pending).await?
            else {
                return Err("failed resolution read lost evidence".into());
            };
            faults.reset();
            let CheckpointResolution::Published(current) = cold.resolve_checkpoint(pending).await?
            else {
                return Err("cold resolution did not publish exact replacement".into());
            };
            assert_eq!(
                current.checkpoint().ok_or("base missing")?.through_commit(),
                boundary.through_commit()
            );
            assert!(current.tail().is_empty());
            let record = cold
                .read_checkpoint(&current)
                .await?
                .ok_or("replacement absent")?;
            assert_eq!(record.snapshot(), b"exact snapshot".as_slice());
            assert_eq!(record.objects(), std::slice::from_ref(root.reference()));
        }
    }
    Ok(())
}

#[tokio::test]
async fn rewrite_cannot_publish_a_candidate_selected_by_a_new_collection_plan() -> TestResult {
    for clear_before_publication in [false, true] {
        let faults = FaultStore::new(InMemory::new());
        let fixture = Fixture::new(
            Arc::new(faults.clone()),
            &format!("planned-{clear_before_publication}"),
            Options::default(),
        )
        .await?;
        let committed = commit(&fixture.log, &fixture.log.load().await?, vec![]).await?;
        let source = checkpoint(&fixture.log, &committed, vec![]).await?;
        let root = fixture
            .log
            .put_object(&source, Bytes::from_static(b"unpublished root"))
            .await?;
        let collector = fixture.cold().await?;
        let rewriter = fixture.log.clone();
        let candidate = root.clone();
        let original = source.clone();
        faults.reset();
        let mut gate = faults.pause_put_at(2, FailurePhase::Before);
        let rewrite = tokio::spawn(async move {
            rewriter
                .publish_checkpoint(&original, None, Bytes::new(), vec![candidate])
                .await
        });
        pause::entered(gate.wait_until_entered()).await?;
        let CollectionStart::Installed(fenced, _) = collector.start_collection(&source).await?
        else {
            return Err("collection plan missing".into());
        };
        assert!(matches!(
            fixture
                .log
                .publish_checkpoint(&fenced, None, Bytes::new(), vec![root.clone()])
                .await,
            Err(Error::InvalidStagedObject)
        ));
        let current = if clear_before_publication {
            let CollectionFinish::Complete(cleared, _) =
                collector.resume_collection(&fenced).await?
            else {
                return Err("collection failed".into());
            };
            cleared
        } else {
            fenced
        };
        assert!(gate.release());
        match rewrite.await?? {
            CheckpointStatus::Conflict(conflict) if !clear_before_publication => {
                assert_eq!(conflict.checkpoint(), source.checkpoint());
                assert_eq!(conflict.collection_epoch(), current.collection_epoch());
            }
            CheckpointStatus::Pending(pending) if clear_before_publication => {
                let CheckpointResolution::Expired(expired) =
                    collector.resolve_checkpoint(pending).await?
                else {
                    return Err("two collection updates did not retain conservative expiry".into());
                };
                assert_eq!(expired.checkpoint(), source.checkpoint());
                assert_eq!(expired.collection_epoch(), current.collection_epoch());
            }
            _ => return Err("rewrite crossed a collection fence".into()),
        }
        let collected = collect_all(&collector, current).await?;
        assert!(matches!(
            collector.read_object(&collected, root.reference()).await,
            Err(Error::CorruptObject)
        ));
        assert_eq!(
            fixture.cold().await?.load().await?.checkpoint(),
            source.checkpoint()
        );
    }
    Ok(())
}

#[tokio::test]
async fn rewrite_with_an_existing_tail_changes_only_the_applied_prefix() -> TestResult {
    let fixture = Fixture::new(
        Arc::new(InMemory::new()),
        "existing-suffix",
        Options::default(),
    )
    .await?;
    let committed = commit(&fixture.log, &fixture.log.load().await?, vec![]).await?;
    let base = checkpoint(&fixture.log, &committed, vec![]).await?;
    let suffix_root = fixture
        .log
        .put_object(&base, Bytes::from_static(b"suffix root"))
        .await?;
    let source = commit(&fixture.log, &base, vec![suffix_root.clone()]).await?;
    let boundary = base.checkpoint().ok_or("base missing")?;
    let CheckpointStatus::Published(rewritten) = fixture
        .log
        .publish_checkpoint(&source, None, Bytes::from_static(b"prefix only"), vec![])
        .await?
    else {
        return Err("rewrite failed".into());
    };
    assert_eq!(rewritten.tail(), source.tail());
    assert_eq!(
        rewritten
            .checkpoint()
            .ok_or("replacement missing")?
            .through_sequence(),
        boundary.through_sequence()
    );
    let cold = fixture.cold().await?;
    let mut cursor = object_log::history(&cold, cold.load().await?)?;
    let Some(HistoryItem::Checkpoint(prefix)) = cursor.next().await? else {
        return Err("prefix missing".into());
    };
    assert_eq!(prefix.record().snapshot(), b"prefix only".as_slice());
    assert_eq!(
        rewritten
            .checkpoint()
            .ok_or("replacement missing")?
            .through_commit(),
        boundary.through_commit()
    );
    let Some(HistoryItem::Commit(suffix)) = cursor.next().await? else {
        return Err("suffix missing".into());
    };
    assert_eq!(
        suffix.record().expected_tip(),
        Some(boundary.through_commit())
    );
    assert_eq!(
        suffix.record().objects(),
        std::slice::from_ref(suffix_root.reference())
    );
    assert!(cursor.next().await?.is_none());
    let next = commit(&cold, &rewritten, vec![]).await?;
    assert_eq!(
        next.tail().last().ok_or("next commit missing")?.sequence(),
        source.tail()[0].sequence() + 1
    );
    Ok(())
}
