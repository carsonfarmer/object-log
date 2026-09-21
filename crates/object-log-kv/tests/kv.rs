use std::{collections::BTreeMap, error::Error as StdError, sync::Arc};

use bytes::Bytes;
use object_log::{
    CheckpointResolution, CheckpointStatus, CollectionFinish, CollectionStart, CommitStatus, Log,
    LogId, Options, Resolution, RetentionId, RetentionStatus, TransactionId, ValidatedBackend,
    sim::{Failure, FailurePhase, FaultStore, Operation},
};
use object_log_kv::{KvCommand, KvError, KvResult, KvStore, Limits, decode_results};
use object_store::{ObjectStore, local::LocalFileSystem, memory::InMemory, path::Path};

#[cfg(feature = "aws")]
#[path = "support/minio.rs"]
mod minio;

type StoreFactory = dyn Fn() -> Result<Arc<dyn ObjectStore>, Box<dyn StdError>>;

type TestResult = Result<(), Box<dyn StdError>>;

// Keep the provider and memory assertions identical, including injected lost replies.
macro_rules! backend_cases {
    ($($case:ident),+ $(,)?) => {
        mod memory {
            use super::*;
            $(#[tokio::test]
            async fn $case() -> TestResult {
                super::$case(&|| Ok(Arc::new(InMemory::new()))).await
            })+
        }

        #[cfg(feature = "aws")]
        #[tokio::test]
        #[ignore = "requires isolated local MinIO; see README"]
        async fn minio_correctness_matrix() -> TestResult {
            $({
                let (storage, counts) = minio::build()?;
                $case(&move || Ok(Arc::clone(&storage)))
                    .await.map_err(|error| format!("{}: {error}", stringify!($case)))?;
                eprintln!("{} http=[attempts,upload_body_bytes,conflicts,transport_or_5xx_errors] {:?}",
                    stringify!($case), counts.snapshot());
            })+
            Ok(())
        }
    };
}

backend_cases! {
    ordered_batches_missing_empty_cas_and_integer_results,
    model_survives_splits_merges_binary_ranges_and_cold_checkpoints,
    conflicting_writers_cannot_publish_stale_conditions,
    lost_publication_cancellation_and_expired_evidence_remain_distinct,
    retention_pins_scan_and_checkpoint_gc_can_resume_after_failure,
    rejected_uncertain_candidate_resolves_without_replaying_over_winner,
}

async fn open(backend: &ValidatedBackend, options: Options) -> Result<KvStore, Box<dyn StdError>> {
    Ok(KvStore::new(
        Log::open(backend, &LogId::new("kv")?, options).await?,
        Limits::default(),
    ))
}
async fn fixture(
    options: Options,
) -> Result<(ValidatedBackend, KvStore, FaultStore), Box<dyn StdError>> {
    fixture_on(Arc::new(InMemory::new()), options).await
}

async fn fixture_on(
    storage: Arc<dyn ObjectStore>,
    options: Options,
) -> Result<(ValidatedBackend, KvStore, FaultStore), Box<dyn StdError>> {
    let faults = FaultStore::from_arc(storage);
    let backend = ValidatedBackend::new(Arc::new(faults.clone()), Path::from("tests")).await?;
    let store = open(&backend, options).await?;
    Ok((backend, store, faults))
}
fn set(key: &[u8], value: &[u8]) -> KvCommand {
    KvCommand::Set {
        key: Bytes::copy_from_slice(key),
        value: Bytes::copy_from_slice(value),
    }
}
async fn commit(
    store: &KvStore,
    commands: &[KvCommand],
) -> Result<Vec<KvResult>, Box<dyn StdError>> {
    let prepared = store
        .snapshot()
        .await?
        .prepare(TransactionId::new(), commands)
        .await?;
    let results = decode_results(prepared.result())?;
    assert!(matches!(
        store.log().commit(prepared).await?,
        CommitStatus::Committed(_)
    ));
    Ok(results)
}
async fn checkpoint(store: &KvStore) -> TestResult {
    assert!(matches!(
        store.snapshot().await?.checkpoint().await?,
        Some(CheckpointStatus::Published(_))
    ));
    Ok(())
}

async fn ordered_batches_missing_empty_cas_and_integer_results(
    new_store: &StoreFactory,
) -> TestResult {
    let (_, store, _) = fixture_on(new_store()?, Options::default()).await?;
    let commands = [
        set(b"", b""),
        set(b"a", b"one"),
        set(b"a", b"two"),
        KvCommand::CompareAndSwap {
            key: Bytes::from("a"),
            expected: Some(Bytes::from("wrong")),
            value: None,
        },
        KvCommand::CompareAndSwap {
            key: Bytes::from("a"),
            expected: Some(Bytes::from("two")),
            value: Some(Bytes::from("three")),
        },
        KvCommand::Increment {
            key: Bytes::from("count"),
            delta: 4,
        },
        KvCommand::Increment {
            key: Bytes::from("count"),
            delta: -1,
        },
        KvCommand::Increment {
            key: Bytes::from("absent"),
            delta: 0,
        },
        KvCommand::Delete {
            key: Bytes::from("a"),
        },
    ];
    assert_eq!(
        commit(&store, &commands).await?,
        vec![
            KvResult::Changed(true),
            KvResult::Changed(true),
            KvResult::Changed(true),
            KvResult::Swapped(false),
            KvResult::Swapped(true),
            KvResult::Integer(4),
            KvResult::Integer(3),
            KvResult::Integer(0),
            KvResult::Changed(true)
        ]
    );
    assert_eq!(
        store
            .snapshot()
            .await?
            .get_many(&[
                Bytes::new(),
                Bytes::from("a"),
                Bytes::from("count"),
                Bytes::from("absent")
            ])
            .await?,
        vec![
            Some(Bytes::new()),
            None,
            Some(Bytes::copy_from_slice(&3_i64.to_be_bytes())),
            None
        ]
    );
    let before = store.log().load().await?.generation();
    for commands in [
        vec![
            set(b"unpublished", b"x"),
            KvCommand::Increment {
                key: Bytes::new(),
                delta: 1,
            },
        ],
        vec![
            set(b"max", &i64::MAX.to_be_bytes()),
            KvCommand::Increment {
                key: Bytes::from("max"),
                delta: 1,
            },
        ],
    ] {
        assert!(matches!(
            store
                .snapshot()
                .await?
                .prepare(TransactionId::new(), &commands)
                .await,
            Err(KvError::NotInteger | KvError::IntegerOverflow)
        ));
    }
    assert_eq!(store.log().load().await?.generation(), before);
    assert_eq!(store.snapshot().await?.get(b"unpublished").await?, None);
    Ok(())
}

async fn model_survives_splits_merges_binary_ranges_and_cold_checkpoints(
    new_store: &StoreFactory,
) -> TestResult {
    let (backend, mut store, _) = fixture_on(new_store()?, Options::default()).await?;
    let mut model = BTreeMap::<Vec<u8>, Bytes>::new();
    let mut random = 0x1234_5678_u64;
    let keys: Vec<_> = (0..128_u8)
        .map(|n| {
            if n < 16 {
                vec![b'a'; usize::from(n)]
            } else if n < 32 {
                vec![255, n]
            } else {
                vec![n % 8, n]
            }
        })
        .collect();
    for round in 0..32 {
        let mut commands = Vec::new();
        let mut expected = Vec::new();
        for _ in 0..16 {
            random = random
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1);
            let key = &keys[usize::try_from(random >> 32)? % keys.len()];
            match random % 4 {
                0 => {
                    commands.push(KvCommand::Delete {
                        key: Bytes::copy_from_slice(key),
                    });
                    expected.push(KvResult::Changed(model.remove(key).is_some()));
                }
                1 => {
                    let prior = if random & 16 == 0 {
                        model.get(key).cloned()
                    } else {
                        Some(Bytes::from("mismatch"))
                    };
                    let next = Some(Bytes::copy_from_slice(&random.to_be_bytes()));
                    commands.push(KvCommand::CompareAndSwap {
                        key: Bytes::copy_from_slice(key),
                        expected: prior.clone(),
                        value: next.clone(),
                    });
                    let matched = model.get(key) == prior.as_ref();
                    if matched && let Some(value) = next {
                        model.insert(key.clone(), value);
                    }
                    expected.push(KvResult::Swapped(matched));
                }
                _ => {
                    let value = if random & 32 == 0 {
                        Bytes::new()
                    } else {
                        Bytes::copy_from_slice(&random.to_be_bytes())
                    };
                    commands.push(set(key, &value));
                    let changed = model.get(key) != Some(&value);
                    model.insert(key.clone(), value);
                    expected.push(KvResult::Changed(changed));
                }
            }
        }
        assert_eq!(commit(&store, &commands).await?, expected);
        if round % 4 == 0 {
            checkpoint(&store).await?;
            store = open(&backend, Options::default()).await?;
        }
        assert_model(&store, &model).await?;
    }
    let deletes: Vec<_> = model
        .keys()
        .map(|key| KvCommand::Delete {
            key: Bytes::copy_from_slice(key),
        })
        .collect();
    commit(&store, &deletes).await?;
    checkpoint(&store).await?;
    assert!(
        open(&backend, Options::default())
            .await?
            .snapshot()
            .await?
            .scan_prefix(b"", None, 10)
            .await?
            .entries
            .is_empty()
    );
    Ok(())
}

async fn assert_model(store: &KvStore, model: &BTreeMap<Vec<u8>, Bytes>) -> TestResult {
    let snapshot = store.snapshot().await?;
    let mut after = None;
    let mut actual = Vec::new();
    loop {
        let page = snapshot.scan(b"", None, after.as_deref(), 7).await?;
        actual.extend(page.entries);
        after = page.after;
        if after.is_none() {
            break;
        }
    }
    assert_eq!(
        actual,
        model
            .iter()
            .map(|(k, v)| (Bytes::copy_from_slice(k), v.clone()))
            .collect::<Vec<_>>()
    );
    for prefix in [b"".as_slice(), b"a", &[255], &[0]] {
        let actual = snapshot.scan_prefix(prefix, None, 128).await?.entries;
        assert_eq!(
            actual,
            model
                .iter()
                .filter(|(key, _)| key.starts_with(prefix))
                .map(|(k, v)| (Bytes::copy_from_slice(k), v.clone()))
                .collect::<Vec<_>>()
        );
    }
    let page = snapshot.scan(&[1], Some(&[6]), None, 128).await?;
    assert_eq!(
        page.entries,
        model
            .iter()
            .filter(|(key, _)| key.as_slice() >= [1].as_slice() && key.as_slice() < [6].as_slice())
            .map(|(k, v)| (Bytes::copy_from_slice(k), v.clone()))
            .collect::<Vec<_>>()
    );
    Ok(())
}

async fn conflicting_writers_cannot_publish_stale_conditions(
    new_store: &StoreFactory,
) -> TestResult {
    let (_, store, _) = fixture_on(new_store()?, Options::default()).await?;
    let snapshot = store.snapshot().await?;
    let candidates = futures::future::try_join_all((0..8).map(|_| async {
        snapshot
            .prepare(
                TransactionId::new(),
                &[KvCommand::Increment {
                    key: Bytes::from("counter"),
                    delta: 1,
                }],
            )
            .await
    }))
    .await?;
    let mut winners = 0;
    let outcomes = futures::future::try_join_all(
        candidates
            .into_iter()
            .map(|candidate| store.log().commit(candidate)),
    )
    .await?;
    for outcome in outcomes {
        match outcome {
            CommitStatus::Committed(_) => winners += 1,
            CommitStatus::Conflict(_) => {}
            CommitStatus::Pending(_) => return Err("unexpected pending result".into()),
        }
    }
    assert_eq!(winners, 1);
    assert_eq!(snapshot.get(b"counter").await?, None);
    assert_eq!(
        store.snapshot().await?.get(b"counter").await?.as_deref(),
        Some(1_i64.to_be_bytes().as_slice())
    );
    assert_eq!(
        commit(
            &store,
            &[KvCommand::Increment {
                key: Bytes::from("counter"),
                delta: 1
            }]
        )
        .await?,
        vec![KvResult::Integer(2)]
    );
    Ok(())
}

async fn lost_publication_cancellation_and_expired_evidence_remain_distinct(
    new_store: &StoreFactory,
) -> TestResult {
    let options = Options {
        resolution_window: 1,
        ..Options::default()
    };
    let (backend, store, faults) = fixture_on(new_store()?, options).await?;
    let candidate = store
        .snapshot()
        .await?
        .prepare(TransactionId::new(), &[set(b"x", b"one")])
        .await?;
    let token = candidate.recovery_token()?;
    let results = candidate.result().clone();
    faults.reset();
    faults.schedule(Failure {
        operation: Operation::Put,
        occurrence: 2,
        phase: FailurePhase::After,
    });
    assert!(matches!(
        store.log().commit(candidate).await?,
        CommitStatus::Pending(_)
    ));
    let reopened = open(&backend, options).await?;
    assert!(matches!(
        reopened.log().resume(&token).await?,
        Resolution::Committed(_)
    ));
    assert_eq!(decode_results(&results)?, vec![KvResult::Changed(true)]);
    checkpoint(&reopened).await?;
    commit(&reopened, &[set(b"x", b"two")]).await?;
    checkpoint(&reopened).await?;
    assert!(matches!(
        reopened.log().resume(&token).await?,
        Resolution::Expired(_)
    ));

    let prepared = reopened
        .snapshot()
        .await?
        .prepare(TransactionId::new(), &[set(b"cancel", b"yes")])
        .await?;
    let token = prepared.recovery_token()?;
    faults.reset();
    {
        let mut pause = faults.pause_put_at(2, FailurePhase::After);
        let publishing = reopened.log().commit(prepared);
        futures::pin_mut!(publishing);
        tokio::select! {
            result = &mut publishing => { return Err(format!("publication did not pause: {result:?}").into()); }
            entered = pause.wait_until_entered() => { assert!(entered); }
        }
    }
    // Publication was cancelled after the head changed, before returning a result.
    assert!(matches!(
        open(&backend, options).await?.log().resume(&token).await?,
        Resolution::Committed(_)
    ));
    Ok(())
}

async fn retention_pins_scan_and_checkpoint_gc_can_resume_after_failure(
    new_store: &StoreFactory,
) -> TestResult {
    let (backend, store, faults) = fixture_on(new_store()?, Options::default()).await?;
    commit(
        &store,
        &[set(b"a", b"old"), set(b"b", b"old"), set(b"c", b"old")],
    )
    .await?;
    let old = store.snapshot().await?;
    let retained = RetentionId::new();
    assert!(matches!(
        store.log().retain(old.view(), retained).await?,
        RetentionStatus::Applied(_)
    ));
    assert_eq!(
        old.scan_prefix(b"", None, 1).await?.entries,
        vec![(Bytes::from("a"), Bytes::from("old"))]
    );
    commit(&store, &[set(b"b", b"new"), set(b"d", b"new")]).await?;
    faults.reset();
    faults.schedule(Failure {
        operation: Operation::Put,
        occurrence: 2,
        phase: FailurePhase::After,
    });
    let Some(CheckpointStatus::Pending(pending)) = store.snapshot().await?.checkpoint().await?
    else {
        return Err("expected pending checkpoint".into());
    };
    assert!(matches!(
        store.log().resolve_checkpoint(pending).await?,
        CheckpointResolution::Published(_)
    ));
    assert!(matches!(
        store
            .log()
            .start_collection(&store.log().load().await?)
            .await?,
        CollectionStart::Retained(_)
    ));
    assert_eq!(
        old.scan_prefix(b"", Some(b"a"), 10).await?.entries,
        vec![
            (Bytes::from("b"), Bytes::from("old")),
            (Bytes::from("c"), Bytes::from("old"))
        ]
    );
    assert!(matches!(
        store
            .log()
            .release_retention(&store.log().load().await?, retained)
            .await?,
        RetentionStatus::Applied(_)
    ));
    let view = collect_with_competing_writers(&store).await?;
    faults.fail_next(Operation::Delete, FailurePhase::After);
    assert!(matches!(
        store.log().resume_collection(&view).await?,
        CollectionFinish::Pending(_)
    ));
    let reopened = open(&backend, Options::default()).await?;
    assert!(matches!(
        reopened
            .log()
            .resume_collection(&reopened.log().load().await?)
            .await?,
        CollectionFinish::Complete(_, _)
    ));
    assert!(matches!(
        old.get(b"b").await,
        Err(KvError::Log(object_log::Error::ViewExpired))
    ));
    assert_eq!(
        reopened.snapshot().await?.get(b"b").await?,
        Some(Bytes::from("during-gc"))
    );
    Ok(())
}

async fn collect_with_competing_writers(
    store: &KvStore,
) -> Result<object_log::View, Box<dyn StdError>> {
    let stale = store
        .snapshot()
        .await?
        .prepare(TransactionId::new(), &[set(b"b", b"stale-gc")])
        .await?;
    let CollectionStart::Installed(view, _) = store
        .log()
        .start_collection(&store.log().load().await?)
        .await?
    else {
        return Err("expected collection".into());
    };
    // Staged nodes from the old epoch are collectible and must never publish.
    assert!(matches!(
        store.log().commit(stale).await?,
        CommitStatus::Conflict(_)
    ));
    // A fresh writer can publish while that positive deletion plan is active.
    commit(store, &[set(b"b", b"during-gc")]).await?;
    Ok(view)
}

#[tokio::test]
async fn sparse_calls_stay_small_as_database_outgrows_tree_budget() -> TestResult {
    for count in [256_u32, 4096] {
        let (backend, store, faults) = fixture(Options::default()).await?;
        for batch in (0..count).collect::<Vec<_>>().chunks(128) {
            commit(
                &store,
                &batch
                    .iter()
                    .map(|key| set(&key.to_be_bytes(), &[7; 64]))
                    .collect::<Vec<_>>(),
            )
            .await?;
        }
        checkpoint(&store).await?;
        let reopened = open(&backend, Options::default()).await?;
        let bounded = KvStore::new(
            reopened.log().clone(),
            Limits {
                tree_bytes: 128 * 1024,
                ..Limits::default()
            },
        );
        faults.reset();
        let snapshot = bounded.snapshot().await?;
        assert!(faults.metrics().operation(Operation::Get).requests <= 2);
        faults.reset();
        assert_eq!(
            snapshot.get(&(count / 2).to_be_bytes()).await?,
            Some(Bytes::from(vec![7; 64]))
        );
        let read = faults.metrics();
        assert!(read.operation(Operation::Get).requests <= 3);
        assert!(read.downloaded_bytes() < 32 * 1024);
        faults.reset();
        let prepared = snapshot
            .prepare(
                TransactionId::new(),
                &[set(&(count / 2).to_be_bytes(), b"changed")],
            )
            .await?;
        let write = faults.metrics();
        assert!(write.operation(Operation::Get).requests <= 3);
        // A changed point operation reads each immutable path node only once.
        let paths: Vec<_> = write
            .events
            .iter()
            .filter(|event| event.operation == Operation::Get)
            .map(|event| &event.path)
            .collect();
        assert_eq!(
            paths.len(),
            paths
                .iter()
                .collect::<std::collections::BTreeSet<_>>()
                .len()
        );
        assert!(write.operation(Operation::Put).requests <= 3);
        assert!(write.downloaded_bytes() + write.uploaded_bytes() < 96 * 1024);
        assert!(matches!(
            bounded.log().commit(prepared).await?,
            CommitStatus::Committed(_)
        ));
        eprintln!(
            "keys={count} value_bytes=64 get_calls={} get_bytes={} prepare_gets={} prepare_puts={} prepare_bytes={}",
            read.operation(Operation::Get).requests,
            read.downloaded_bytes(),
            write.operation(Operation::Get).requests,
            write.operation(Operation::Put).requests,
            write.downloaded_bytes() + write.uploaded_bytes()
        );
    }
    Ok(())
}

#[tokio::test]
async fn limits_fail_without_publication_and_pages_bound_returned_bytes() -> TestResult {
    let (_, store, faults) = fixture(Options::default()).await?;
    commit(
        &store,
        &[set(b"a", b"1234"), set(b"b", b"5678"), set(b"c", b"9012")],
    )
    .await?;
    let limited = KvStore::new(
        store.log().clone(),
        Limits {
            response_bytes: 5,
            ..Limits::default()
        },
    );
    let snapshot = limited.snapshot().await?;
    let page = snapshot.scan_prefix(b"", None, 10).await?;
    assert_eq!(page.entries, vec![(Bytes::from("a"), Bytes::from("1234"))]);
    assert_eq!(
        snapshot
            .scan_prefix(b"", page.after.as_deref(), 10)
            .await?
            .entries,
        vec![(Bytes::from("b"), Bytes::from("5678"))]
    );
    assert!(matches!(
        snapshot
            .get_many(&[Bytes::from("a"), Bytes::from("b")])
            .await,
        Err(KvError::Limit(_))
    ));
    let limited = KvStore::new(
        store.log().clone(),
        Limits {
            key_bytes: 1,
            value_bytes: 4,
            batch_entries: 1,
            tree_bytes: 0,
            ..Limits::default()
        },
    );
    let snapshot = limited.snapshot().await?;
    faults.reset();
    for commands in [
        vec![set(b"long", b"x")],
        vec![set(b"a", b"12345")],
        vec![set(b"a", b"x"), set(b"b", b"y")],
    ] {
        assert!(matches!(
            snapshot.prepare(TransactionId::new(), &commands).await,
            Err(KvError::Limit(_))
        ));
    }
    assert!(matches!(snapshot.get(b"a").await, Err(KvError::Limit(_))));
    assert_eq!(faults.metrics().total_requests(), 0);
    Ok(())
}

#[tokio::test]
async fn filesystem_backend_is_rejected_before_kv_use() -> TestResult {
    let directory = tempfile::tempdir()?;
    let filesystem: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(directory.path())?);
    assert!(matches!(
        ValidatedBackend::new(filesystem, Path::from("kv")).await,
        Err(object_log::Error::UnsupportedBackend("conditional update"))
    ));
    Ok(())
}

#[tokio::test]
async fn full_byte_fanout_with_maximum_inline_parent_value_remains_readable() -> TestResult {
    let (_, store, _) = fixture(Options::default()).await?;
    let limits = Limits::default();
    let value = vec![42; limits.value_bytes];
    commit(&store, &[set(b"root", &value)]).await?;
    for chunk in (0..=255_u8).collect::<Vec<_>>().chunks(64) {
        let commands: Vec<_> = chunk
            .iter()
            .map(|edge| {
                let mut key = b"root".to_vec();
                key.push(*edge);
                set(&key, &[*edge])
            })
            .collect();
        commit(&store, &commands).await?;
    }
    let snapshot = store.snapshot().await?;
    assert_eq!(
        snapshot.get(b"root").await?.as_deref(),
        Some(value.as_slice())
    );
    for edge in 0..=255_u8 {
        let mut key = b"root".to_vec();
        key.push(edge);
        assert_eq!(
            snapshot.get(&key).await?.as_deref(),
            Some([edge].as_slice())
        );
    }
    let first = snapshot.scan_prefix(b"root", None, 128).await?;
    assert_eq!(first.entries.len(), 128);
    commit(&store, &[set(b"root-new", b"new")]).await?;
    let second = snapshot
        .scan_prefix(b"root", first.after.as_deref(), 128)
        .await?;
    let third = snapshot
        .scan_prefix(b"root", second.after.as_deref(), 128)
        .await?;
    let keys: Vec<_> = first
        .entries
        .into_iter()
        .chain(second.entries)
        .chain(third.entries)
        .map(|(key, _)| key)
        .collect();
    assert_eq!(keys.len(), 257);
    assert!(keys.windows(2).all(|pair| pair[0] < pair[1]));
    assert!(!keys.iter().any(|key| key.as_ref() == b"root-new"));
    let updated = vec![43; limits.value_bytes];
    assert_eq!(
        commit(&store, &[set(b"root", &updated)]).await?,
        vec![KvResult::Changed(true)]
    );
    assert_eq!(
        store.snapshot().await?.get(b"root").await?.as_deref(),
        Some(updated.as_slice())
    );
    assert_eq!(
        commit(
            &store,
            &[
                set(b"root", &updated),
                KvCommand::Delete {
                    key: Bytes::from("root")
                },
                KvCommand::Delete {
                    key: Bytes::from("root")
                },
            ]
        )
        .await?,
        vec![
            KvResult::Changed(false),
            KvResult::Changed(true),
            KvResult::Changed(false)
        ]
    );
    assert_eq!(store.snapshot().await?.get(b"root").await?, None);
    assert_eq!(
        snapshot.get(b"root").await?.as_deref(),
        Some(value.as_slice())
    );
    Ok(())
}

#[tokio::test]
async fn rejects_incompatible_roots_malformed_nodes_and_result_bytes() -> TestResult {
    for bytes in [
        b"\x80\x00".as_slice(),
        b"\x81\x84\x01\xf6\x00\xf6",
        b"\x9f\xff",
    ] {
        assert!(matches!(
            decode_results(bytes),
            Err(KvError::InvalidEncoding)
        ));
    }
    let (_, store, _) = fixture(Options::default()).await?;
    let view = store.log().load().await?;
    let bad = store.log().prepare(
        &view,
        TransactionId::new(),
        Bytes::from("legacy"),
        Bytes::new(),
        Vec::new(),
    )?;
    assert!(matches!(
        store.log().commit(bad).await?,
        CommitStatus::Committed(_)
    ));
    assert!(matches!(
        store.snapshot().await,
        Err(KvError::InvalidEncoding)
    ));

    let (_, store, _) = fixture(Options::default()).await?;
    let view = store.log().load().await?;
    // Canonical [empty prefix, empty value, one edge], but no matching child.
    let node = store
        .log()
        .put_node(
            &view,
            Bytes::from_static(b"\x83\x40\x40\x41\x00"),
            Vec::new(),
        )
        .await?;
    let bad = store.log().prepare(
        &view,
        TransactionId::new(),
        Bytes::from_static(b"object-log-kv/radix/1"),
        Bytes::new(),
        vec![node],
    )?;
    assert!(matches!(
        store.log().commit(bad).await?,
        CommitStatus::Committed(_)
    ));
    assert!(matches!(
        store.snapshot().await?.get(b"").await,
        Err(KvError::InvalidEncoding)
    ));
    Ok(())
}

async fn rejected_uncertain_candidate_resolves_without_replaying_over_winner(
    new_store: &StoreFactory,
) -> TestResult {
    let (backend, store, faults) = fixture_on(new_store()?, Options::default()).await?;
    let candidate = store
        .snapshot()
        .await?
        .prepare(TransactionId::new(), &[set(b"x", b"loser")])
        .await?;
    let token = candidate.recovery_token()?;
    faults.reset();
    faults.schedule(Failure {
        operation: Operation::Put,
        occurrence: 2,
        phase: FailurePhase::Before,
    });
    assert!(matches!(
        store.log().commit(candidate).await?,
        CommitStatus::Pending(_)
    ));
    commit(&store, &[set(b"x", b"winner")]).await?;
    let reopened = open(&backend, Options::default()).await?;
    assert!(matches!(
        reopened.log().resume(&token).await?,
        Resolution::NotCommitted(_)
    ));
    assert_eq!(
        reopened.snapshot().await?.get(b"x").await?,
        Some(Bytes::from("winner"))
    );
    Ok(())
}
