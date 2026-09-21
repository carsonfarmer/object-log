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
        max_collection_objects: 100_000,
    };
    match path {
        "/obtain" => {
            let session = wal::open(&settings)?;
            assert!(!session.has_active_collection());
            assert!(session.latest_complete_state()?.latest.is_none());
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
