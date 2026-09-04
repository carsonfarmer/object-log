use std::error::Error as StdError;
use std::sync::Arc;

use bytes::Bytes;
use futures::future::join_all;
use object_log::{
    CheckpointStatus, CommitStatus, Log, LogId, Materializer, Options, Resolution, TransactionId,
    ValidatedBackend, materialize,
};
use object_log_kv::{KvCommand, KvDecision, KvError, KvMachine, KvResult, KvState};
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
        .publish_checkpoint(replayed.view(), &through, Bytes::from(snapshot), Vec::new())
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
    machine.apply(&mut state, 0, &operation, &[])?;
    assert!(matches!(
        machine.apply(&mut state, 1, &operation, &[]),
        Err(KvError::StateDiverged)
    ));
    Ok(())
}

#[test]
fn stored_results_round_trip_through_the_public_decoder() -> TestResult {
    let machine = KvMachine;
    let cases = [
        KvCommand::Set {
            key: Bytes::from_static(b"name"),
            value: Bytes::from_static(b"value"),
        },
        KvCommand::Increment {
            key: Bytes::from_static(b"counter"),
            delta: 3,
        },
        KvCommand::CompareAndSwap {
            key: Bytes::from_static(b"name"),
            expected: None,
            value: Some(Bytes::from_static(b"value")),
        },
    ];

    for command in cases {
        let KvDecision::Commit {
            result_bytes,
            result,
            ..
        } = machine.evaluate(&KvState::default(), &command)?
        else {
            return Err("test command did not require a commit".into());
        };
        assert_eq!(machine.decode_result(&result_bytes)?, result);
    }
    Ok(())
}

#[test]
fn operations_results_and_checkpoints_have_stable_bytes() -> TestResult {
    let machine = KvMachine;
    let command = KvCommand::Set {
        key: Bytes::from_static(b"k"),
        value: Bytes::from_static(b"v"),
    };
    let KvDecision::Commit {
        operation,
        result_bytes,
        ..
    } = machine.evaluate(&KvState::default(), &command)?
    else {
        return Err("set did not require a commit".into());
    };
    assert_eq!(
        operation.as_ref(),
        b"\xa4\x01\x01\x02\x41k\x03\xf4\x05\x41v"
    );
    assert_eq!(result_bytes.as_ref(), b"\xa2\x01\x01\x02\x01");

    let mut state = KvState::default();
    machine.apply(&mut state, 0, &operation, &[])?;
    assert_eq!(
        machine.checkpoint(&state)?,
        b"\xa2\x01\x01\x02\x81\xa2\x01\x41k\x02\x41v"
    );
    Ok(())
}

#[test]
fn decoders_reject_unknown_trailing_noncanonical_and_oversized_shapes() {
    let machine = KvMachine;
    let mut state = KvState::default();
    for operation in [
        b"\xa5\x01\x01\x02\x41k\x03\xf4\x05\x41v\x06\x00".as_slice(),
        b"\xa4\x01\x01\x02\x41k\x03\xf4\x05\x41v\x00".as_slice(),
        b"\xa4\x01\x18\x01\x02\x41k\x03\xf4\x05\x41v".as_slice(),
        b"\xa5\x01\x01\x02\x41k\x03\xf4\x04\x40\x05\x41v".as_slice(),
    ] {
        assert!(machine.apply(&mut state, 0, operation, &[]).is_err());
    }

    assert!(
        machine
            .restore(b"\xa2\x01\x01\x02\x9b\xff\xff\xff\xff\xff\xff\xff\xff", &[])
            .is_err()
    );
    assert!(machine.decode_result(b"\xa2\x01\x01\x02\x01\x00").is_err());
    assert!(
        machine
            .decode_result(b"\xa3\x01\x01\x02\x01\x04\x00")
            .is_err()
    );
}

async fn open(id: &str) -> Result<Log, object_log::Error> {
    let log_id = LogId::new(id)?;
    let backend = ValidatedBackend::new(Arc::new(InMemory::new()), Path::from("kv-tests")).await?;
    let scoped = backend.scope(&log_id);
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
                Resolution::NotCommitted(_) => {}
                Resolution::Expired(_) => {
                    return Err("key-value commit evidence expired before resolution".into());
                }
                Resolution::StillPending(_) => {
                    return Err("key-value commit outcome is still pending".into());
                }
            },
        }
    }
}
