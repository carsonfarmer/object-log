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
        log_limits: wal::LogLimits {
            max_tail_entries: 1_024,
            resolution_window: 1_024,
            max_inline_operation_bytes: 64 << 10,
            max_inline_result_bytes: 4 << 10,
            max_object_refs: 1_024,
            max_object_bytes: 2 << 20,
            max_commit_bytes: 1 << 20,
            max_head_bytes: 256 << 10,
            max_checkpoint_bytes: 16 << 20,
            max_retention_ids: 1_024,
            max_collection_objects: 100_000,
            max_collection_plan_bytes: 16 << 20,
        },
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
        "/refresh" => {
            let session = wal::open_existing(&settings)?;
            let before = session.usage();
            let unchanged = session.refresh()?;
            assert_eq!(unchanged.usage().calls, before.calls + 1);
            assert_eq!(unchanged.usage().bytes, before.bytes);
            assert_eq!(session.usage().calls, unchanged.usage().calls);

            let recovery = session.recover()?;
            assert!(matches!(
                recovery.next()?,
                Some(wal::HistoryItem::Commit(_))
            ));
            assert!(recovery.next()?.is_none());
            let candidate = recovery.prepare(&[8; 16], b"next", b"result", &[])?;
            assert!(matches!(candidate.publish()?, wal::Outcome::Committed));

            let stale = unchanged.recover()?;
            assert!(matches!(stale.next()?, Some(wal::HistoryItem::Commit(_))));
            assert!(stale.next()?.is_none());
            let before = unchanged.usage();
            let changed = unchanged.refresh()?;
            assert_eq!(changed.usage().calls, before.calls + 1);
            assert!(changed.usage().bytes > before.bytes);
            assert_eq!(session.usage().calls, changed.usage().calls);

            let recovery = changed.recover()?;
            for transaction_id in [vec![7; 16], vec![8; 16]] {
                let Some(wal::HistoryItem::Commit(commit)) = recovery.next()? else {
                    panic!("refreshed session missed a commit")
                };
                assert_eq!(commit.transaction_id, transaction_id);
            }
            assert!(recovery.next()?.is_none());
            let before = changed.usage();
            let stable = changed.refresh()?;
            assert_eq!(stable.usage().calls, before.calls + 1);
            assert_eq!(stable.usage().bytes, before.bytes);
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
            settings.log_limits.max_tail_entries += 1;
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
