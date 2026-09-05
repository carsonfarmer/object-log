use std::{
    env,
    error::Error as StdError,
    path::Path,
    process::Command,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use axum::{
    Router,
    extract::{Request, State},
    middleware::{self, Next},
    response::Response,
};
use object_log::{Log, LogId, Options, TransactionId, ValidatedBackend};
use object_log_git::ObjectFormat;
use object_log_git_http::{GitHttpServer, SharedGitHttpServer, SmartHttp};
use object_store::{
    aws::{AmazonS3, AmazonS3Builder},
    memory::InMemory,
    path::Path as StorePath,
};
use tempfile::TempDir;
use tokio::{net::TcpListener, sync::Barrier, task::JoinHandle};

static SHARED_TEST: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

type TestResult<T = ()> = Result<T, Box<dyn StdError + Send + Sync>>;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unmodified_git_pushes_clones_fetches_and_rejects_stale_updates() -> TestResult {
    client_lifecycle(None, Arc::new(InMemory::new())).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shared_engine_clients_support_both_hashes() -> TestResult {
    let _serial = SHARED_TEST.lock().await;
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        client_lifecycle(Some(format), Arc::new(InMemory::new())).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires local MinIO"]
async fn shared_minio_clients_recover_after_collection() -> TestResult {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        client_lifecycle(Some(format), Arc::new(build_minio()?)).await?;
    }
    Ok(())
}

async fn client_lifecycle(
    shared: Option<ObjectFormat>,
    store: Arc<dyn object_store::ObjectStore>,
) -> TestResult {
    let _ = tracing_subscriber::fmt().with_test_writer().try_init();
    let root = TempDir::new()?;
    let backend = ValidatedBackend::new(
        store,
        StorePath::from(format!("git-http-loopback-{}", TransactionId::new())),
    )
    .await?;
    let log = Log::open(&backend, &LogId::new("repository")?, Options::default()).await?;
    let scratch = root.path().join("scratch");
    let app = if let Some(format) = shared {
        SharedGitHttpServer::new(log.clone(), format).router()
    } else {
        GitHttpServer::new(
            SmartHttp::new(log.clone(), &scratch),
            &scratch,
            "4".parse()?,
        )
        .router()
    };
    let (url, server) = serve(app).await?;
    assert!(git_output(None, ["ls-remote", &url])?.stdout.is_empty());

    let source = root.path().join("source");
    let format = match shared {
        Some(ObjectFormat::Sha256) => "--object-format=sha256",
        _ => "--object-format=sha1",
    };
    git(
        None,
        ["init", "--quiet", "-b", "main", format, path(&source)?],
    )?;
    write(&source, "one")?;
    git(Some(&source), ["add", "file"])?;
    git(Some(&source), ["commit", "--quiet", "-m", "one"])?;
    git(Some(&source), ["remote", "add", "origin", &url])?;
    git(Some(&source), ["push", "--quiet", "-u", "origin", "main"])?;

    let clone = root.path().join("clone");
    let trace = git_trace(None, ["clone", "--quiet", &url, path(&clone)?])?;
    assert!(
        trace.status.success(),
        "{}",
        String::from_utf8_lossy(&trace.stderr)
    );
    if shared.is_some() {
        let trace = String::from_utf8_lossy(&trace.stderr);
        assert!(trace.contains("version 2"), "{trace}");
        assert!(trace.contains("command=ls-refs"), "{trace}");
        assert!(trace.contains("command=fetch"), "{trace}");
    }
    assert_eq!(std::fs::read_to_string(clone.join("file"))?, "one");

    write(&source, "two")?;
    git(Some(&source), ["commit", "--quiet", "-am", "two"])?;
    git(Some(&source), ["push", "--quiet"])?;
    git(Some(&clone), ["fetch", "--quiet"])?;
    git(Some(&clone), ["reset", "--quiet", "--hard", "origin/main"])?;
    assert_eq!(std::fs::read_to_string(clone.join("file"))?, "two");

    git(Some(&source), ["branch", "feature"])?;
    git(Some(&source), ["tag", "-a", "v1", "-m", "v1"])?;
    git(
        Some(&source),
        ["push", "--quiet", "--atomic", "origin", "feature", "v1"],
    )?;
    git(Some(&clone), ["fetch", "--quiet", "--tags"])?;
    git(Some(&clone), ["rev-parse", "--verify", "refs/tags/v1^{}"])?;
    git(
        Some(&source),
        ["push", "--quiet", "--atomic", "origin", ":feature", ":v1"],
    )?;

    let stale = root.path().join("stale");
    git(None, ["clone", "--quiet", &url, path(&stale)?])?;
    write(&source, "winner")?;
    git(Some(&source), ["commit", "--quiet", "-am", "winner"])?;
    git(Some(&source), ["push", "--quiet"])?;
    write(&stale, "loser")?;
    git(Some(&stale), ["commit", "--quiet", "-am", "loser"])?;
    assert!(
        !git_output(Some(&stale), ["push", "--quiet", "--force"])?
            .status
            .success()
    );

    let final_clone = root.path().join("final");
    git(None, ["clone", "--quiet", &url, path(&final_clone)?])?;
    git(Some(&final_clone), ["fsck", "--strict"])?;
    assert_eq!(std::fs::read_to_string(final_clone.join("file"))?, "winner");
    assert!(
        !git_output(
            Some(&final_clone),
            ["rev-parse", "--verify", "refs/remotes/origin/feature"],
        )?
        .status
        .success()
    );
    assert!(
        !git_output(
            Some(&final_clone),
            ["rev-parse", "--verify", "refs/tags/v1"],
        )?
        .status
        .success()
    );
    if shared.is_none() {
        assert!(std::fs::read_dir(scratch)?.next().is_none());
    }
    server.abort();
    if let Some(format) = shared {
        let repository = object_log_git::Repository::open(&log, format).await?;
        let object_log::CheckpointStatus::Published(view) = repository.checkpoint().await? else {
            return Err("checkpoint did not publish".into());
        };
        let object_log::CollectionStart::Installed(fenced, _) = log.start_collection(&view).await?
        else {
            return Err("collection did not start".into());
        };
        assert!(matches!(
            log.resume_collection(&fenced).await?,
            object_log::CollectionFinish::Complete(..)
        ));
        drop(log);
        let log = Log::open(&backend, &LogId::new("repository")?, Options::default()).await?;
        let (url, cold_server) = serve(SharedGitHttpServer::new(log, format).router()).await?;
        let cold = root.path().join("cold");
        git(None, ["clone", "--quiet", &url, path(&cold)?])?;
        git(Some(&cold), ["fsck", "--strict"])?;
        assert_eq!(std::fs::read_to_string(cold.join("file"))?, "winner");
        cold_server.abort();
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn large_fetch_uses_gzip_multi_round_requests_and_chunked_output() -> TestResult {
    large_fetch(None).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shared_large_fetch_uses_gzip_negotiation_and_chunked_output() -> TestResult {
    let _serial = SHARED_TEST.lock().await;
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        large_fetch(Some(format)).await?;
    }
    Ok(())
}

async fn large_fetch(shared: Option<ObjectFormat>) -> TestResult {
    let root = TempDir::new()?;
    let (url, scratch, server, _) = repository_server(&root, "git-http-large", shared).await?;
    let source = root.path().join("large-source");
    let format = match shared {
        Some(ObjectFormat::Sha256) => "--object-format=sha256",
        _ => "--object-format=sha1",
    };
    git(
        None,
        ["init", "--quiet", "-b", "main", format, path(&source)?],
    )?;
    write(&source, "base")?;
    git(Some(&source), ["add", "file"])?;
    git(Some(&source), ["commit", "--quiet", "-m", "base"])?;
    for revision in 0..384 {
        git(
            Some(&source),
            [
                "commit",
                "--quiet",
                "--allow-empty",
                "-m",
                &format!("history-{revision}"),
            ],
        )?;
    }
    git(Some(&source), ["remote", "add", "origin", &url])?;
    git(Some(&source), ["fsck", "--strict", "--no-dangling"])?;
    git(Some(&source), ["push", "--quiet", "-u", "origin", "main"])?;

    let clone = root.path().join("large-clone");
    git(None, ["clone", "--quiet", &url, path(&clone)?])?;
    git(
        Some(&source),
        ["commit", "--quiet", "--allow-empty", "-m", "tip"],
    )?;
    git(Some(&source), ["fsck", "--strict", "--no-dangling"])?;
    git(Some(&source), ["push", "--quiet"])?;
    let expected_tip = git_stdout(Some(&source), ["rev-parse", "HEAD"])?;
    if shared.is_some() {
        // A long unshared local history forces negotiation beyond the first
        // small request; common haves would otherwise finish before gzip.
        git(
            Some(&clone),
            ["config", "fetch.negotiationAlgorithm", "consecutive"],
        )?;
        git(
            Some(&clone),
            ["update-ref", "-d", "refs/remotes/origin/main"],
        )?;
        for revision in 0..384 {
            git(
                Some(&clone),
                [
                    "commit",
                    "--quiet",
                    "--allow-empty",
                    "-m",
                    &format!("local-{revision}"),
                ],
            )?;
        }
    }
    let output = git_trace(Some(&clone), ["fetch", "--quiet"])?;
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).into_owned().into());
    }
    let trace = String::from_utf8_lossy(&output.stderr).to_ascii_lowercase();
    assert!(trace.matches("=> send header: post ").count() >= 2);
    assert!(trace.contains("=> send header: content-encoding: gzip"));
    assert!(trace.contains("<= recv header: transfer-encoding: chunked"));
    assert_eq!(
        git_stdout(Some(&clone), ["rev-parse", "refs/remotes/origin/main"])?,
        expected_tip
    );
    git(
        Some(&clone),
        ["cat-file", "-e", &format!("{expected_tip}^{{tree}}")],
    )?;
    if shared.is_none() {
        assert!(std::fs::read_dir(scratch)?.next().is_none());
    }
    if shared.is_some() {
        assert!(trace.contains("acknowledgments"), "{trace}");
    }
    server.abort();
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_pushes_report_one_durable_winner() -> TestResult {
    let root = TempDir::new()?;
    let (url, scratch, server, gate) =
        repository_server(&root, "git-http-concurrent", None).await?;
    let source = root.path().join("concurrent-source");
    git(None, ["init", "--quiet", "-b", "main", path(&source)?])?;
    write(&source, "base")?;
    git(Some(&source), ["add", "file"])?;
    git(Some(&source), ["commit", "--quiet", "-m", "base"])?;
    git(Some(&source), ["remote", "add", "origin", &url])?;
    git(Some(&source), ["push", "--quiet", "-u", "origin", "main"])?;

    let left = root.path().join("left");
    let right = root.path().join("right");
    git(None, ["clone", "--quiet", &url, path(&left)?])?;
    git(None, ["clone", "--quiet", &url, path(&right)?])?;
    write(&left, "left")?;
    git(Some(&left), ["commit", "--quiet", "-am", "left"])?;
    write(&right, "right")?;
    git(Some(&right), ["commit", "--quiet", "-am", "right"])?;

    gate.arm();
    let left_push = tokio::task::spawn_blocking(move || {
        git_trace(Some(&left), ["push", "--quiet", "origin", "main"])
    });
    let right_push = tokio::task::spawn_blocking(move || {
        git_trace(Some(&right), ["push", "--quiet", "origin", "main"])
    });
    let (left_push, right_push) = tokio::try_join!(left_push, right_push)?;
    let left_push = left_push?;
    let right_push = right_push?;
    assert!(sent_receive_pack(&left_push.stderr));
    assert!(sent_receive_pack(&right_push.stderr));
    assert_ne!(left_push.status.success(), right_push.status.success());
    let expected = if left_push.status.success() {
        "left"
    } else {
        "right"
    };

    let final_clone = root.path().join("concurrent-final");
    git(None, ["clone", "--quiet", &url, path(&final_clone)?])?;
    git(Some(&final_clone), ["fsck", "--strict"])?;
    assert_eq!(std::fs::read_to_string(final_clone.join("file"))?, expected);
    assert!(std::fs::read_dir(scratch)?.next().is_none());
    server.abort();
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires OBJECT_LOG_MINIO_* and the pinned local MinIO from scripts/test-minio.sh"]
async fn minio_host_pushes_and_cold_clones() -> TestResult {
    let root = TempDir::new()?;
    let namespace = StorePath::from(format!("git-http-{}", TransactionId::new()));
    let log_id = LogId::new("repository")?;
    let backend = ValidatedBackend::new(Arc::new(build_minio()?), namespace.clone()).await?;
    let log = Log::open(&backend, &log_id, Options::default()).await?;
    let first_scratch = root.path().join("first-scratch");
    let first_app = GitHttpServer::new(
        SmartHttp::new(log, &first_scratch),
        &first_scratch,
        "2".parse()?,
    )
    .router();
    let (first_url, first_server) = serve(first_app).await?;

    let source = root.path().join("source");
    git(None, ["init", "--quiet", "-b", "main", path(&source)?])?;
    write(&source, "minio")?;
    git(Some(&source), ["add", "file"])?;
    git(Some(&source), ["commit", "--quiet", "-m", "minio"])?;
    git(Some(&source), ["remote", "add", "origin", &first_url])?;
    git(Some(&source), ["push", "--quiet", "-u", "origin", "main"])?;
    let expected_tip = git_stdout(Some(&source), ["rev-parse", "HEAD"])?;
    first_server.abort();
    let _ = first_server.await;

    let backend = ValidatedBackend::new(Arc::new(build_minio()?), namespace).await?;
    let log = Log::open(&backend, &log_id, Options::default()).await?;
    let second_scratch = root.path().join("second-scratch");
    let second_app = GitHttpServer::new(
        SmartHttp::new(log, &second_scratch),
        &second_scratch,
        "2".parse()?,
    )
    .router();
    let (second_url, second_server) = serve(second_app).await?;
    let clone = root.path().join("clone");
    git(None, ["clone", "--quiet", &second_url, path(&clone)?])?;
    assert_eq!(std::fs::read_to_string(clone.join("file"))?, "minio");
    assert_eq!(
        git_stdout(Some(&clone), ["rev-parse", "HEAD"])?,
        expected_tip
    );
    git(Some(&clone), ["fsck", "--strict"])?;
    assert!(std::fs::read_dir(first_scratch)?.next().is_none());
    assert!(std::fs::read_dir(second_scratch)?.next().is_none());
    second_server.abort();
    Ok(())
}

async fn repository_server(
    root: &TempDir,
    namespace: &str,
    shared: Option<ObjectFormat>,
) -> TestResult<(String, std::path::PathBuf, JoinHandle<()>, ReceiveGate)> {
    let backend =
        ValidatedBackend::new(Arc::new(InMemory::new()), StorePath::from(namespace)).await?;
    let log = Log::open(&backend, &LogId::new("repository")?, Options::default()).await?;
    let scratch = root.path().join(format!("{namespace}-scratch"));
    let gate = ReceiveGate::new();
    let app = if let Some(format) = shared {
        SharedGitHttpServer::new(log, format).router()
    } else {
        GitHttpServer::new(SmartHttp::new(log, &scratch), &scratch, "4".parse()?).router()
    }
    .layer(middleware::from_fn_with_state(gate.clone(), gate_receive));
    let (url, server) = serve(app).await?;
    Ok((url, scratch, server, gate))
}

#[derive(Clone)]
struct ReceiveGate {
    armed: Arc<AtomicBool>,
    barrier: Arc<Barrier>,
}

impl ReceiveGate {
    fn new() -> Self {
        Self {
            armed: Arc::new(AtomicBool::new(false)),
            barrier: Arc::new(Barrier::new(2)),
        }
    }

    fn arm(&self) {
        self.armed.store(true, Ordering::Release);
    }
}

async fn gate_receive(State(gate): State<ReceiveGate>, request: Request, next: Next) -> Response {
    if gate.armed.load(Ordering::Acquire) && request.uri().path() == "/repo/git-receive-pack" {
        let _ = tokio::time::timeout(Duration::from_secs(30), gate.barrier.wait()).await;
    }
    next.run(request).await
}

async fn serve(app: Router) -> TestResult<(String, JoinHandle<()>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let task = tokio::spawn(async move {
        if let Err(error) = axum::serve(listener, app).await {
            eprintln!("server: {error}");
        }
    });
    Ok((format!("http://{address}/repo"), task))
}

fn write(path: &Path, contents: &str) -> TestResult {
    std::fs::write(path.join("file"), contents)?;
    Ok(())
}

fn path(path: &Path) -> TestResult<&str> {
    path.to_str().ok_or_else(|| "path is not UTF-8".into())
}

fn git<const N: usize>(directory: Option<&Path>, args: [&str; N]) -> TestResult {
    let output = git_output(directory, args)?;
    if output.status.success() {
        Ok(())
    } else {
        Err(format!("git {args:?}: {}", String::from_utf8_lossy(&output.stderr)).into())
    }
}

fn git_output<const N: usize>(
    directory: Option<&Path>,
    args: [&str; N],
) -> TestResult<std::process::Output> {
    Ok(git_command(directory, args).output()?)
}

fn git_stdout<const N: usize>(directory: Option<&Path>, args: [&str; N]) -> TestResult<String> {
    let output = git_output(directory, args)?;
    if output.status.success() {
        Ok(String::from_utf8(output.stdout)?.trim().to_owned())
    } else {
        Err(format!("git {args:?}: {}", String::from_utf8_lossy(&output.stderr)).into())
    }
}

fn git_trace<const N: usize>(
    directory: Option<&Path>,
    args: [&str; N],
) -> TestResult<std::process::Output> {
    let mut command = git_command(directory, args);
    command
        .env("GIT_TRACE_PACKET", "1")
        .env("GIT_TRACE_CURL", "1")
        .env("GIT_TRACE_CURL_NO_DATA", "1");
    Ok(command.output()?)
}

fn sent_receive_pack(trace: &[u8]) -> bool {
    String::from_utf8_lossy(trace)
        .to_ascii_lowercase()
        .contains("=> send header: post /repo/git-receive-pack")
}

fn git_command<const N: usize>(directory: Option<&Path>, args: [&str; N]) -> Command {
    let mut command = Command::new("git");
    command
        .args([
            "-c",
            "commit.gpgsign=false",
            "-c",
            "gc.auto=0",
            "-c",
            "maintenance.auto=false",
        ])
        .args(args)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env(
            "GIT_CONFIG_GLOBAL",
            if cfg!(windows) { "NUL" } else { "/dev/null" },
        )
        .env("GIT_PROTOCOL", "version=2")
        .env("GIT_AUTHOR_NAME", "Object Log")
        .env("GIT_AUTHOR_EMAIL", "object-log@example.invalid")
        .env("GIT_COMMITTER_NAME", "Object Log")
        .env("GIT_COMMITTER_EMAIL", "object-log@example.invalid");
    if let Some(directory) = directory {
        command.current_dir(directory);
    }
    command
}

fn build_minio() -> TestResult<AmazonS3> {
    Ok(AmazonS3Builder::new()
        .with_endpoint(required_env("OBJECT_LOG_MINIO_ENDPOINT")?)
        .with_access_key_id(required_env("OBJECT_LOG_MINIO_ACCESS_KEY")?)
        .with_secret_access_key(required_env("OBJECT_LOG_MINIO_SECRET_KEY")?)
        .with_bucket_name(required_env("OBJECT_LOG_MINIO_BUCKET")?)
        .with_region("us-east-1")
        .with_allow_http(true)
        .with_virtual_hosted_style_request(false)
        .with_disable_bulk_delete(false)
        .build()?)
}

fn required_env(name: &'static str) -> TestResult<String> {
    env::var(name).map_err(|_| format!("{name} is not set").into())
}
