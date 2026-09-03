use std::error::Error as StdError;
use std::sync::Arc;

use bytes::Bytes;
use futures::future::join_all;
use object_log::kv::{KvCommand, KvDecision, KvError, KvMachine, KvResult, KvState};
use object_log::{
    CheckpointStatus, CommitStatus, Log, LogId, Materializer, Options, Resolution, ScopedStore,
    TransactionId, materialize,
};
use object_store::memory::InMemory;
use object_store::path::Path;

type TestResult = Result<(), Box<dyn StdError>>;

#[tokio::test]
async fn key_value_commands_have_one_committed_order() -> TestResult {
    let log = open("kv-commands").await?;
    let machine = KvMachine;

    assert_eq!(
        execute(
            &log,
            &machine,
            KvCommand::Set {
                key: Bytes::from_static(b"name"),
                value: Bytes::from_static(b"first"),
            },
        )
        .await?,
        KvResult::Previous(None)
    );
    let before_failed_swap = log.load().await?.cursor().generation();
    assert_eq!(
        execute(
            &log,
            &machine,
            KvCommand::CompareAndSwap {
                key: Bytes::from_static(b"name"),
                expected: Some(Bytes::from_static(b"wrong")),
                value: Some(Bytes::from_static(b"second")),
            },
        )
        .await?,
        KvResult::Swapped(false)
    );
    assert_eq!(log.load().await?.cursor().generation(), before_failed_swap);
    assert_eq!(
        execute(
            &log,
            &machine,
            KvCommand::CompareAndSwap {
                key: Bytes::from_static(b"name"),
                expected: Some(Bytes::from_static(b"first")),
                value: Some(Bytes::from_static(b"second")),
            },
        )
        .await?,
        KvResult::Swapped(true)
    );
    assert_eq!(
        materialize(&log, &machine).await?.state().get(b"name"),
        Some(b"second".as_slice())
    );
    assert_eq!(
        execute(
            &log,
            &machine,
            KvCommand::Delete {
                key: Bytes::from_static(b"name"),
            },
        )
        .await?,
        KvResult::Previous(Some(Bytes::from_static(b"second")))
    );
    assert!(materialize(&log, &machine).await?.state().is_empty());
    Ok(())
}

#[tokio::test]
async fn concurrent_increments_return_each_committed_value_once() -> TestResult {
    let log = open("kv-increments").await?;
    let machine = KvMachine;
    let commands = (0..32).map(|_| {
        execute(
            &log,
            &machine,
            KvCommand::Increment {
                key: Bytes::from_static(b"counter"),
                delta: 1,
            },
        )
    });
    let mut values = join_all(commands)
        .await
        .into_iter()
        .map(|result| match result {
            Ok(KvResult::Integer(value)) => Ok(value),
            Ok(_) => Err("increment returned a non-integer result".into()),
            Err(error) => Err(error),
        })
        .collect::<Result<Vec<_>, Box<dyn StdError>>>()?;
    values.sort_unstable();
    assert_eq!(values, (1..=32).collect::<Vec<_>>());

    let state = materialize(&log, &machine).await?;
    let stored = state.state().get(b"counter").ok_or("counter is missing")?;
    let encoded: [u8; size_of::<i64>()] = stored.try_into()?;
    assert_eq!(i64::from_be_bytes(encoded), 32);
    assert_eq!(state.view().tail().len(), 32);
    Ok(())
}

#[tokio::test]
async fn checkpoint_restore_matches_full_replay() -> TestResult {
    let log = open("kv-checkpoint").await?;
    let machine = KvMachine;
    execute(
        &log,
        &machine,
        KvCommand::Set {
            key: Bytes::from_static(b"name"),
            value: Bytes::from_static(b"value"),
        },
    )
    .await?;
    execute(
        &log,
        &machine,
        KvCommand::Increment {
            key: Bytes::from_static(b"counter"),
            delta: 7,
        },
    )
    .await?;

    let replayed = materialize(&log, &machine).await?;
    let through = replayed
        .view()
        .tail()
        .last()
        .cloned()
        .ok_or("materialized view has no tail")?;
    let snapshot = machine.checkpoint(replayed.state())?;
    let CheckpointStatus::Published(compacted) = log
        .publish_checkpoint(replayed.view(), &through, Bytes::from(snapshot))
        .await?
    else {
        return Err("key-value checkpoint returned a conflict".into());
    };
    assert!(compacted.tail().is_empty());

    let restored = materialize(&log, &machine).await?;
    assert_eq!(restored.state(), replayed.state());
    assert!(restored.view().checkpoint().is_some());
    Ok(())
}

#[test]
fn replay_rejects_a_mutation_applied_to_the_wrong_state() -> TestResult {
    let machine = KvMachine;
    let command = KvCommand::Increment {
        key: Bytes::from_static(b"counter"),
        delta: 1,
    };
    let KvDecision::Commit { operation, .. } = machine.evaluate(&KvState::default(), &command)?
    else {
        return Err("increment did not require a commit".into());
    };
    let mut state = KvState::default();
    machine.apply(&mut state, 0, &operation)?;
    assert!(matches!(
        machine.apply(&mut state, 1, &operation),
        Err(KvError::StateDiverged)
    ));
    Ok(())
}

async fn open(id: &str) -> Result<Log, object_log::Error> {
    let log_id = LogId::new(id)?;
    let scoped = ScopedStore::new(Arc::new(InMemory::new()), Path::from("kv-tests"), &log_id);
    Log::open(scoped, Options::default()).await
}

async fn execute(
    log: &Log,
    machine: &KvMachine,
    command: KvCommand,
) -> Result<KvResult, Box<dyn StdError>> {
    let transaction_id = TransactionId::new();
    loop {
        let current = materialize(log, machine).await?;
        let (operation, result_bytes, result) = match machine.evaluate(current.state(), &command)? {
            KvDecision::NoChange(result) => return Ok(result),
            KvDecision::Commit {
                operation,
                result_bytes,
                result,
            } => (operation, result_bytes, result),
        };
        let prepared = log.prepare(
            current.view().cursor(),
            transaction_id,
            operation,
            result_bytes,
            Vec::new(),
        )?;
        match log.commit(prepared).await? {
            CommitStatus::Committed(_) => return Ok(result),
            CommitStatus::Conflict(_) => {}
            CommitStatus::Pending(pending) => match log.resolve(pending).await? {
                Resolution::Committed(_) => return Ok(result),
                Resolution::NotCommitted(_) | Resolution::Expired(_) => {}
                Resolution::StillPending(_) => {
                    return Err("key-value commit outcome is still pending".into());
                }
            },
        }
    }
}
