//! HTTP driver composed with the real WAL component by test-credentials.py.
use spin_sdk::http::{Request, Response};

wit_bindgen::generate!({
    path: "wit",
    inline: "package object-log:credential-test; world client { import object-log:storage/wal@0.1.0; }",
    world: "client",
    generate_all,
});
use crate::object_log::storage::wal;

#[spin_sdk::http_component]
fn handle(request: Request) -> Response {
    match exercise(request.path()) {
        Ok(()) => Response::new(200, "ok"),
        Err(error) => Response::new(500, format!("{error:?}")),
    }
}

fn exercise(path: &str) -> Result<(), wal::Failure> {
    let mut settings = wal::Config {
        endpoint: "http://127.0.0.1:19092".into(),
        bucket: "fixture".into(),
        region: "us-west-2".into(),
        credential_mode: wal::CredentialMode::InstanceRole,
        access_key: String::new(),
        secret_key: String::new(),
        session_token: None,
        prefix: "credentials".into(),
        log_id: match path {
            "/missing" => "missing",
            "/lost-head" => "lost-head",
            _ => "existing",
        }
        .into(),
        log_options: br#"{"max_object_bytes":2097152}"#.to_vec(),
        transport_limits: wal::TransportLimits {
            max_calls: 25_984,
            max_bytes: 26_180_206_592,
        },
    };
    match path {
        "/validate" => {
            settings.prefix = "credentials/validate".into();
            wal::validate_backend(&settings)?;
        }
        "/obtain" => {
            let session = wal::open(&settings)?;
            assert!(!session.has_active_collection());
            let recovery = session.recover()?;
            assert!(recovery.next()?.is_none());
            let candidate = recovery.prepare(&[7; 16], b"operation", b"result", &[])?;
            assert!(matches!(candidate.publish()?, wal::Outcome::Committed));
        }
        "/existing" => {
            let session = wal::open_existing(&settings)?;
            assert!(!session.has_active_collection());
            let recovery = session.recover()?;
            let Some(wal::HistoryItem::Commit(commit)) = recovery.next()? else {
                panic!("open-existing did not load the published commit")
            };
            assert_eq!(commit.transaction_id, vec![7; 16]);
            assert_eq!(commit.operation, b"operation");
            assert_eq!(commit.recorded_result, b"result");
            assert!(recovery.next()?.is_none());
        }
        "/recovery-gates" => {
            let session = wal::open_existing(&settings)?;
            let peeked = session.recover()?;
            let Some(wal::HistoryItem::Commit(latest)) = peeked.latest()?.item else {
                panic!("latest missed the fixture commit")
            };
            let Some(wal::HistoryItem::Commit(next)) = peeked.next()? else {
                panic!("latest consumed the fixture commit")
            };
            assert_eq!(latest.transaction_id, vec![7; 16]);
            assert_eq!(next.transaction_id, latest.transaction_id);
            let complete = session.recover()?;
            while complete.next()?.is_some() {}
            let writer = complete.write_bytes()?;
            writer.write(b"bytes")?;
            let bytes = writer.finish()?;
            let node = complete.put_node(b"node", &[])?;

            let incomplete = session.recover()?;
            let writer = incomplete.write_bytes()?;
            writer.write(b"other")?;
            let other = writer.finish()?;
            assert_eq!(incomplete.open_bytes(&bytes)?.read_at(0, 5)?, b"bytes");
            assert_eq!(incomplete.open_bytes(&other)?.read_at(0, 5)?, b"other");
            assert_eq!(incomplete.read_node(&node)?.data, b"node");
            let other_node = incomplete.put_node(b"other", &[])?;
            assert_eq!(incomplete.read_node(&other_node)?.data, b"other");
            // Preparation authenticates the remaining history without publishing.
            let _candidate = incomplete.prepare(&[9; 16], b"op", b"result", &[])?;
            assert!(incomplete.next()?.is_none());
        }
        "/refresh" => {
            let session = wal::open_existing(&settings)?;
            let before = session.usage();
            session.refresh()?;
            // The first refresh renews the fixture's expired role credentials:
            // three metadata requests, then the conditional head GET.
            assert_eq!(session.usage().calls, before.calls + 4);
            assert!(session.usage().bytes > before.bytes);

            let stale = session.recover()?;
            let lifetime = session.recover()?;
            let writer = lifetime.write_bytes()?;
            writer.write(b"before")?;
            let readable = lifetime.write_bytes()?;
            readable.write(b"reader")?;
            let bytes = readable.finish()?;
            let reader = lifetime.open_bytes(&bytes)?;
            let prepared = lifetime.prepare(&[9; 16], b"unused", b"result", &[])?;
            let token = prepared.recovery_token()?;
            let recovery = session.recover()?;
            assert!(matches!(
                recovery.next()?,
                Some(wal::HistoryItem::Commit(_))
            ));
            assert!(recovery.next()?.is_none());
            let candidate = recovery.prepare(&[8; 16], b"next", b"result", &[])?;
            assert!(matches!(candidate.publish()?, wal::Outcome::Committed));

            let before = session.usage();
            session.refresh()?;
            assert_eq!(session.usage().calls, before.calls + 1);
            assert!(session.usage().bytes > before.bytes);
            assert_eq!(reader.read_at(0, 6)?, b"reader");
            writer.write(b"after")?;
            let written = writer.finish()?;
            assert_eq!(
                lifetime.open_bytes(&written)?.read_at(0, 11)?,
                b"beforeafter"
            );
            assert_eq!(prepared.recovery_token()?, token);
            assert!(matches!(prepared.publish()?, wal::Outcome::Conflict));
            let Some(wal::HistoryItem::Commit(commit)) = stale.next()? else {
                panic!("session refresh changed an existing recovery view")
            };
            assert_eq!(commit.transaction_id, vec![7; 16]);
            assert!(stale.next()?.is_none());

            let recovery = session.recover()?;
            for transaction_id in [vec![7; 16], vec![8; 16]] {
                let Some(wal::HistoryItem::Commit(commit)) = recovery.next()? else {
                    panic!("refreshed session missed a commit")
                };
                assert_eq!(commit.transaction_id, transaction_id);
            }
            assert!(recovery.next()?.is_none());
            let before = session.usage();
            session.refresh()?;
            assert_eq!(session.usage().calls, before.calls + 1);
            assert_eq!(session.usage().bytes, before.bytes);
        }
        "/checkpoint" => {
            let session = wal::open_existing(&settings)?;
            let recovery = session.recover()?;
            while recovery.next()?.is_some() {}
            assert!(matches!(
                recovery.checkpoint(b"snapshot", &[])?,
                wal::MaintenanceState::Complete
            ));
            session.refresh()?;
            let recovery = session.recover()?;
            let Some(wal::HistoryItem::Checkpoint(entry)) = recovery.next()? else {
                panic!("missing recovered checkpoint")
            };
            assert_eq!(entry.data, b"snapshot");
            assert!(entry.objects.is_empty());
            assert!(recovery.next()?.is_none());
        }
        "/missing" => {
            assert!(matches!(
                wal::open_existing(&settings),
                Err(wal::Failure::Missing)
            ));
            settings.log_id = "missing-after-error".into();
            assert!(matches!(
                wal::open_existing(&settings),
                Err(wal::Failure::Missing)
            ));
        }
        "/mismatched-options" => {
            settings.log_options =
                br#"{"max_object_bytes":2097152,"max_tail_entries":1025}"#.to_vec();
            assert!(matches!(
                wal::open_existing(&settings),
                Err(wal::Failure::Other(_))
            ));
        }
        "/slow-metadata" => {
            for _ in 0..2 {
                assert!(matches!(wal::open(&settings), Err(wal::Failure::Other(_))));
            }
        }
        "/unavailable" | "/renewal-unavailable" => {
            assert!(matches!(wal::open(&settings), Err(wal::Failure::Other(_))));
        }
        "/lost-head" => {
            let session = wal::open(&settings)?;
            assert!(session.recover()?.next()?.is_none());
        }
        _ => panic!("unknown fixture scenario"),
    }
    Ok(())
}
