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
    let settings = wal::Config {
        endpoint: "http://127.0.0.1:19092".into(),
        bucket: "fixture".into(),
        region: "us-west-2".into(),
        credential_mode: wal::CredentialMode::InstanceRole,
        access_key: String::new(),
        secret_key: String::new(),
        session_token: None,
        prefix: "credentials".into(),
        log_id: if path == "/missing" {
            "missing"
        } else {
            "existing"
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
        "/obtain" => {
            let session = wal::open(&settings)?;
            assert!(!session.has_active_collection());
            let recovery = session.recover()?;
            assert!(recovery.next()?.is_none());
        }
        "/existing" => {
            let session = wal::open_existing(&settings)?;
            assert!(!session.has_active_collection());
        }
        "/missing" => assert!(matches!(
            wal::open_existing(&settings),
            Err(wal::Failure::Missing)
        )),
        "/unavailable" | "/renewal-unavailable" | "/slow-metadata" => {
            assert!(matches!(wal::open(&settings), Err(wal::Failure::Other(_))));
        }
        _ => panic!("unknown fixture scenario"),
    }
    Ok(())
}
