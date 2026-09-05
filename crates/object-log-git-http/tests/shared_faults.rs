//! Shared-engine transport faults with explicit store and body gates.

use std::{error::Error as StdError, io, path::Path, process::Command, sync::Arc, time::Duration};

use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode},
};
use bytes::Bytes;
use futures::{FutureExt, channel::oneshot};
use http_body_util::BodyExt;
use object_log::{
    CommitStatus, Log, LogId, Options, Resolution, TransactionId, ValidatedBackend,
    sim::{FailurePhase, FaultStore, Operation},
};
use object_log_git::{ObjectFormat, ObjectId, RefUpdate, Repository};
use object_log_git_http::SharedGitHttpServer;
use object_store::{ObjectStoreExt, memory::InMemory, path::Path as StorePath};
use tempfile::TempDir;
use tower::ServiceExt;

// The product deliberately admits one common-engine command process-wide.
static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
type TestResult<T = ()> = Result<T, Box<dyn StdError + Send + Sync>>;
const DEADLINE: Duration = Duration::from_secs(10);
const LIMIT: usize = 10 * 1024 * 1024;

struct Fixture {
    root: TempDir,
    log: Log,
    backend: ValidatedBackend,
    faults: FaultStore,
    store: Arc<InMemory>,
    format: ObjectFormat,
    target: ObjectId,
}

impl Fixture {
    async fn new(format: ObjectFormat) -> TestResult<Self> {
        let root = tempfile::tempdir()?;
        let source = root.path().join("source");
        git(
            root.path(),
            &[
                "init",
                "--quiet",
                "-b",
                "main",
                &format!("--object-format={}", format_name(format)),
                "source",
            ],
        )?;
        std::fs::write(source.join("file"), b"durable transport fixture")?;
        git(&source, &["add", "file"])?;
        git(&source, &["commit", "--quiet", "-m", "initial"])?;
        let target = ObjectId::parse(
            format,
            String::from_utf8(git(&source, &["rev-parse", "HEAD"])?)?.trim(),
        )?;
        let pack = root.path().join("pack");
        std::fs::write(&pack, git(&source, &["pack-objects", "--all", "--stdout"])?)?;
        let store = Arc::new(InMemory::new());
        let faults = FaultStore::from_arc(store.clone());
        let backend = ValidatedBackend::new(
            Arc::new(faults.clone()),
            StorePath::from("shared-http-faults"),
        )
        .await?;
        let log = Log::open(&backend, &LogId::new("repository")?, options()).await?;
        let prepared = Repository::open_native(&log, root.path().join("initial-cache"), format)
            .await?
            .prepare_push(
                TransactionId::new(),
                vec![RefUpdate::new("refs/heads/main", None, Some(target))?],
                Some(&pack),
            )
            .await?;
        assert!(matches!(
            prepared.publish().await?,
            CommitStatus::Committed(_)
        ));
        faults.reset();
        Ok(Self {
            root,
            log,
            backend,
            faults,
            store,
            format,
            target,
        })
    }

    fn host(&self) -> SharedGitHttpServer {
        SharedGitHttpServer::new(self.log.clone(), self.format)
    }

    fn deletion(&self) -> Bytes {
        let line = format!(
            "{} {} refs/heads/main\0report-status object-format={} atomic",
            self.target,
            "0".repeat(self.target.to_string().len()),
            format_name(self.format)
        );
        Bytes::from(format!("{:04x}{line}0000", line.len() + 4))
    }

    async fn add_tag(&self, name: &str) -> TestResult {
        let repository =
            Repository::open_native(&self.log, self.root.path().join(name), self.format).await?;
        let push = repository
            .prepare_push(
                TransactionId::new(),
                vec![RefUpdate::new(
                    format!("refs/tags/{name}"),
                    None,
                    Some(self.target),
                )?],
                None,
            )
            .await?;
        assert!(matches!(push.publish().await?, CommitStatus::Committed(_)));
        Ok(())
    }
}

fn options() -> Options {
    Options {
        resolution_window: 1,
        ..Options::default()
    }
}

const fn format_name(format: ObjectFormat) -> &'static str {
    match format {
        ObjectFormat::Sha1 => "sha1",
        ObjectFormat::Sha256 => "sha256",
    }
}

fn git(directory: &Path, arguments: &[&str]) -> TestResult<Vec<u8>> {
    let output = Command::new("git")
        .current_dir(directory)
        .args(["-c", "commit.gpgsign=false", "-c", "gc.auto=0"])
        .args(arguments)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_AUTHOR_NAME", "Object Log")
        .env("GIT_AUTHOR_EMAIL", "object-log@example.invalid")
        .env("GIT_COMMITTER_NAME", "Object Log")
        .env("GIT_COMMITTER_EMAIL", "object-log@example.invalid")
        .output()?;
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).into_owned().into());
    }
    Ok(output.stdout)
}

fn receive(body: Body) -> TestResult<Request<Body>> {
    Ok(Request::post("/repo/git-receive-pack")
        .header("content-type", "application/x-git-receive-pack-request")
        .body(body)?)
}

async fn advertisement(app: Router) -> TestResult<axum::response::Response> {
    Ok(app
        .oneshot(Request::get("/repo/info/refs?service=git-receive-pack").body(Body::empty())?)
        .await?)
}

#[tokio::test]
async fn pending_http_returns_opaque_token_recoverable_after_host_drop() -> TestResult {
    let _serial = TEST_LOCK.lock().await;
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        for phase in [FailurePhase::Before, FailurePhase::After] {
            let fixture = Fixture::new(format).await?;
            let host = fixture.host();
            // A delete-only prepare performs no PUT; commit then head are the
            // two writes. Pause the head before injecting its ambiguous result.
            let mut gate = fixture.faults.pause_put_at(2, FailurePhase::Before);
            let request = receive(Body::from(fixture.deletion()))?;
            let task = tokio::spawn(host.clone().router().oneshot(request));
            assert!(tokio::time::timeout(DEADLINE, gate.wait_until_entered()).await?);
            fixture.faults.schedule(object_log::sim::Failure {
                operation: Operation::Put,
                occurrence: 2,
                phase,
            });
            fixture
                .faults
                .fail_next(Operation::Get, FailurePhase::Before);
            assert!(gate.release());
            let response = tokio::time::timeout(DEADLINE, task).await???;
            assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
            assert_eq!(
                response.headers()["content-type"],
                "application/octet-stream"
            );
            assert_eq!(
                response.headers()["cache-control"],
                "no-cache, max-age=0, must-revalidate"
            );
            let token = response.into_body().collect().await?.to_bytes();
            assert!(!token.is_empty());
            assert!(
                !token
                    .windows(b"ok refs/".len())
                    .any(|part| part == b"ok refs/")
            );
            host.shutdown().await;
            drop(host);
            let Fixture {
                log, backend, root, ..
            } = fixture;
            drop(log);
            let reopened = Log::open(&backend, &LogId::new("repository")?, options()).await?;
            assert!(matches!(
                reopened.resume(&token).await?,
                Resolution::Committed(_)
            ));
            drop(token);
            let repository =
                Repository::open_native(&reopened, root.path().join("cold-cache"), format).await?;
            assert!(repository.refs().is_empty());
        }
    }
    Ok(())
}

#[tokio::test]
async fn expired_http_never_reports_success_or_republishes() -> TestResult {
    let _serial = TEST_LOCK.lock().await;
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let fixture = Fixture::new(format).await?;
        let host = fixture.host();
        let mut gate = fixture.faults.pause_put_at(2, FailurePhase::Before);
        let task = tokio::spawn(
            host.clone()
                .router()
                .oneshot(receive(Body::from(fixture.deletion()))?),
        );
        assert!(tokio::time::timeout(DEADLINE, gate.wait_until_entered()).await?);
        // Competing native-oracle clients use their own pools. Advance and
        // compact past this candidate with a one-entry resolution window.
        fixture.add_tag("first").await?;
        fixture.add_tag("second").await?;
        let repository =
            Repository::open_native(&fixture.log, fixture.root.path().join("checkpoint"), format)
                .await?;
        assert!(matches!(
            repository.checkpoint().await?,
            object_log::CheckpointStatus::Published(_)
        ));
        assert!(gate.release());
        let response = tokio::time::timeout(DEADLINE, task).await???;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            response.headers()["content-type"],
            "application/octet-stream"
        );
        let token = response.into_body().collect().await?.to_bytes();
        assert!(
            !token
                .windows(b"ok refs/".len())
                .any(|part| part == b"ok refs/")
        );
        assert!(matches!(
            fixture.log.resume(&token).await?,
            Resolution::Expired(_)
        ));
        drop(token);
        let repository = Repository::open(&fixture.log, format).await?;
        assert_eq!(
            repository.refs().get(b"refs/heads/main".as_slice()),
            Some(&fixture.target)
        );
        host.shutdown().await;
    }
    Ok(())
}

#[tokio::test]
async fn cancelled_handler_keeps_publication_and_shutdown_waits() -> TestResult {
    let _serial = TEST_LOCK.lock().await;
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let fixture = Fixture::new(format).await?;
        let host = fixture.host();
        let mut gate = fixture.faults.pause_put_at(2, FailurePhase::Before);
        let task = tokio::spawn(
            host.clone()
                .router()
                .oneshot(receive(Body::from(fixture.deletion()))?),
        );
        assert!(tokio::time::timeout(DEADLINE, gate.wait_until_entered()).await?);
        task.abort();
        assert!(task.await.is_err_and(|error| error.is_cancelled()));
        let shutdown = host.shutdown();
        futures::pin_mut!(shutdown);
        assert!(shutdown.as_mut().now_or_never().is_none());
        assert!(
            gate.release(),
            "disconnect cancelled the durable publication"
        );
        tokio::time::timeout(DEADLINE, shutdown).await?;
        let repository = Repository::open(&fixture.log, format).await?;
        assert!(repository.refs().is_empty());
    }
    Ok(())
}

#[tokio::test]
async fn process_admission_covers_body_collection_and_response_delivery() -> TestResult {
    let _serial = TEST_LOCK.lock().await;
    let fixture = Fixture::new(ObjectFormat::Sha1).await?;
    let host = fixture.host();
    let (entered_tx, entered_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let bytes = fixture.deletion();
    let body = Body::from_stream(futures::stream::once(async move {
        let _ = entered_tx.send(());
        release_rx.await.map_err(io::Error::other)?;
        Ok::<_, io::Error>(bytes)
    }));
    let task = tokio::spawn(host.clone().router().oneshot(receive(body)?));
    tokio::time::timeout(DEADLINE, entered_rx).await??;
    let second_host = fixture.host();
    assert_eq!(
        advertisement(second_host.clone().router()).await?.status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    release_tx
        .send(())
        .map_err(|()| "body collection cancelled")?;
    let response = tokio::time::timeout(DEADLINE, task).await???;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        advertisement(second_host.clone().router()).await?.status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    drop(response);
    assert_eq!(
        advertisement(second_host.router()).await?.status(),
        StatusCode::OK
    );
    host.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn encoded_and_decoded_limits_and_gzip_errors_are_http_client_errors() -> TestResult {
    use tokio::io::AsyncWriteExt;
    let _serial = TEST_LOCK.lock().await;
    let fixture = Fixture::new(ObjectFormat::Sha1).await?;
    let app = fixture.host().router();
    let request = Request::post("/repo/git-receive-pack")
        .header("content-type", "application/x-git-receive-pack-request")
        .header("content-length", LIMIT + 1)
        .body(Body::empty())?;
    assert_eq!(
        app.clone().oneshot(request).await?.status(),
        StatusCode::PAYLOAD_TOO_LARGE
    );
    assert_eq!(
        fixture.faults.metrics().operation(Operation::Get).requests,
        0
    );
    for (size, expected) in [
        (LIMIT, StatusCode::BAD_REQUEST),
        (LIMIT + 1, StatusCode::PAYLOAD_TOO_LARGE),
    ] {
        let mut encoder = async_compression::tokio::write::GzipEncoder::new(Vec::new());
        encoder.write_all(&vec![0; size]).await?;
        encoder.shutdown().await?;
        let compressed = encoder.into_inner();
        assert!(compressed.len() < LIMIT);
        let request = Request::post("/repo/git-receive-pack")
            .header("content-type", "application/x-git-receive-pack-request")
            .header("content-encoding", "gzip")
            .body(Body::from(compressed))?;
        assert_eq!(app.clone().oneshot(request).await?.status(), expected);
    }
    let request = Request::post("/repo/git-receive-pack")
        .header("content-type", "application/x-git-receive-pack-request")
        .header("content-encoding", "gzip")
        .body(Body::from(Bytes::from_static(&[0x1f, 0x8b])))?;
    assert_eq!(
        app.clone().oneshot(request).await?.status(),
        StatusCode::BAD_REQUEST
    );
    let mut request = receive(Body::empty())?;
    request.headers_mut().append(
        "content-type",
        "application/x-git-receive-pack-request".parse()?,
    );
    assert_eq!(
        app.clone().oneshot(request).await?.status(),
        StatusCode::UNSUPPORTED_MEDIA_TYPE
    );
    assert_eq!(
        advertisement(app).await?.status(),
        StatusCode::OK,
        "failed request leaked admission"
    );
    Ok(())
}

#[tokio::test]
async fn real_tcp_disconnect_does_not_cancel_the_head_update() -> TestResult {
    use tokio::io::AsyncWriteExt;
    let _serial = TEST_LOCK.lock().await;
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let fixture = Fixture::new(format).await?;
        let host = fixture.host();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let (stop_tx, stop_rx) = oneshot::channel();
        let app = host.clone().router();
        let server = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async {
                    let _ = stop_rx.await;
                })
                .await
        });
        let mut gate = fixture.faults.pause_put_at(2, FailurePhase::Before);
        let mut socket = tokio::net::TcpStream::connect(address).await?;
        let input = fixture.deletion();
        socket.write_all(format!("POST /repo/git-receive-pack HTTP/1.1\r\nHost: {address}\r\nContent-Type: application/x-git-receive-pack-request\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", input.len()).as_bytes()).await?;
        socket.write_all(&input).await?;
        assert!(tokio::time::timeout(DEADLINE, gate.wait_until_entered()).await?);
        drop(socket);
        let shutdown = host.shutdown();
        futures::pin_mut!(shutdown);
        assert!(shutdown.as_mut().now_or_never().is_none());
        assert!(gate.release());
        tokio::time::timeout(DEADLINE, shutdown).await?;
        stop_tx
            .send(())
            .map_err(|()| "server stopped before shutdown")?;
        tokio::time::timeout(DEADLINE, server).await???;
        let repository = Repository::open(&fixture.log, format).await?;
        assert!(repository.refs().is_empty());
    }
    Ok(())
}

#[tokio::test]
async fn invalid_resolution_evidence_returns_recoverable_token_after_hidden_publication()
-> TestResult {
    let _serial = TEST_LOCK.lock().await;
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let fixture = Fixture::new(format).await?;
        let host = fixture.host();
        // Let the actual head PUT succeed, but withhold its reply. Unlike a
        // normal Store error, malformed resolution evidence makes publish_receive
        // return Err after this publication may already have happened.
        fixture.faults.schedule(object_log::sim::Failure {
            operation: Operation::Put,
            occurrence: 2,
            phase: FailurePhase::After,
        });
        let mut gate = fixture.faults.pause_put_at(2, FailurePhase::After);
        let task = tokio::spawn(
            host.clone()
                .router()
                .oneshot(receive(Body::from(fixture.deletion()))?),
        );
        assert!(tokio::time::timeout(DEADLINE, gate.wait_until_entered()).await?);
        // Earlier head reads supply the exact scoped durable location.
        let location = fixture
            .faults
            .metrics()
            .events
            .iter()
            .rev()
            .find(|event| event.path.ends_with("/index.cbor"))
            .map(|event| StorePath::from(event.path.clone()))
            .ok_or("missing head location")?;
        let published = fixture.store.get(&location).await?.bytes().await?;
        fixture
            .store
            .put(
                &location,
                Bytes::from_static(b"corrupt head evidence").into(),
            )
            .await?;
        assert!(gate.release());
        let response = tokio::time::timeout(DEADLINE, task).await???;
        // Restore the authentic candidate after exposing the transient corrupt
        // evidence. The returned token must classify that exact publication.
        fixture.store.put(&location, published.into()).await?;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            response.headers()["content-type"],
            "application/octet-stream"
        );
        let token = response.into_body().collect().await?.to_bytes();
        assert!(!token.is_empty());
        host.shutdown().await;
        drop(host);
        let reopened = Log::open(&fixture.backend, &LogId::new("repository")?, options()).await?;
        assert!(matches!(
            reopened.resume(&token).await?,
            Resolution::Committed(_)
        ));
        drop(token);
        assert!(Repository::open(&reopened, format).await?.refs().is_empty());
    }
    Ok(())
}
