use std::{env, error::Error as StdError, path::Path, process::Command, sync::Arc, time::Duration};

use axum::Router;
use object_log::{Log, LogId, Options, TransactionId, ValidatedBackend};
use object_log_git::ObjectFormat;
use object_log_git_http::SharedGitHttpServer;
use object_store::{
    aws::{AmazonS3, AmazonS3Builder},
    memory::InMemory,
    path::Path as StorePath,
};
use tempfile::TempDir;
use tokio::{net::TcpListener, task::JoinHandle};

static SHARED_TEST: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

type TestResult<T = ()> = Result<T, Box<dyn StdError + Send + Sync>>;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shared_engine_clients_support_both_hashes() -> TestResult {
    let _serial = SHARED_TEST.lock().await;
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        client_lifecycle(format, Arc::new(InMemory::new()), false).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires local MinIO"]
async fn shared_minio_clients_recover_after_collection() -> TestResult {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        client_lifecycle(format, Arc::new(build_minio()?), false).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires local MinIO, Spin 4, and a release WASIp2 component build"]
async fn spin_minio_clients_recover_after_collection() -> TestResult {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        client_lifecycle(format, Arc::new(build_minio()?), true).await?;
    }
    Ok(())
}

#[allow(
    clippy::too_many_lines,
    reason = "one unchanged-client lifecycle is shared by native and Spin hosts with both hashes"
)]
async fn client_lifecycle(
    format: ObjectFormat,
    store: Arc<dyn object_store::ObjectStore>,
    spin: bool,
) -> TestResult {
    let _ = tracing_subscriber::fmt().with_test_writer().try_init();
    let root = TempDir::new()?;
    let namespace = format!("git-http-loopback-{}", TransactionId::new());
    let backend = ValidatedBackend::new(store, StorePath::from(namespace.clone())).await?;
    let log = Log::open(&backend, &LogId::new("repository")?, Options::default()).await?;
    let app = SharedGitHttpServer::new(log.clone(), format).router();
    let (url, mut server) = if spin {
        serve_spin(format, &namespace).await?
    } else {
        let (url, server) = serve(app).await?;
        (url, RunningHost::Native(server))
    };
    assert!(git_output(None, ["ls-remote", &url])?.stdout.is_empty());

    let source = root.path().join("source");
    let format_option = match format {
        ObjectFormat::Sha256 => "--object-format=sha256",
        ObjectFormat::Sha1 => "--object-format=sha1",
    };
    git(
        None,
        [
            "init",
            "--quiet",
            "-b",
            "main",
            format_option,
            path(&source)?,
        ],
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
    {
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
    server.stop();
    {
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
        let (url, mut cold_server) = if spin {
            serve_spin(format, &namespace).await?
        } else {
            let (url, server) = serve(SharedGitHttpServer::new(log, format).router()).await?;
            (url, RunningHost::Native(server))
        };
        let cold = root.path().join("cold");
        git(None, ["clone", "--quiet", &url, path(&cold)?])?;
        git(Some(&cold), ["fsck", "--strict"])?;
        assert_eq!(std::fs::read_to_string(cold.join("file"))?, "winner");
        cold_server.stop();
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shared_large_fetch_uses_gzip_negotiation_and_chunked_output() -> TestResult {
    let _serial = SHARED_TEST.lock().await;
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        large_fetch(format).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn default_git_large_push_probes_then_streams_both_hashes() -> TestResult {
    let _serial = SHARED_TEST.lock().await;
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let root = TempDir::new()?;
        let (url, server) = repository_server("git-http-large-push", format).await?;
        let source = root.path().join("source");
        let hash = match format {
            ObjectFormat::Sha1 => "--object-format=sha1",
            ObjectFormat::Sha256 => "--object-format=sha256",
        };
        git(
            None,
            ["init", "--quiet", "-b", "main", hash, path(&source)?],
        )?;
        let mut seed = 0x1234_5678_u32;
        let content: Vec<u8> = (0..8 * 1024 * 1024)
            .map(|_| {
                seed ^= seed << 13;
                seed ^= seed >> 17;
                seed ^= seed << 5;
                seed.to_le_bytes()[0]
            })
            .collect();
        std::fs::write(source.join("large"), &content)?;
        git(Some(&source), ["add", "large"])?;
        git(Some(&source), ["commit", "--quiet", "-m", "large"])?;
        let output = git_trace(Some(&source), ["push", "--quiet", &url, "main"])?;
        let trace = String::from_utf8_lossy(&output.stderr).to_ascii_lowercase();
        assert!(output.status.success(), "{trace}");
        assert!(
            trace.matches("=> send header: post ").count() >= 2,
            "{trace}"
        );
        assert!(
            trace.contains("=> send header: transfer-encoding: chunked"),
            "{trace}"
        );
        let clone = root.path().join("clone");
        git(None, ["clone", "--quiet", &url, path(&clone)?])?;
        git(Some(&clone), ["fsck", "--strict"])?;
        assert_eq!(std::fs::read(clone.join("large"))?, content);
        server.abort();
    }
    Ok(())
}

async fn large_fetch(format: ObjectFormat) -> TestResult {
    let root = TempDir::new()?;
    let (url, server) = repository_server("git-http-large", format).await?;
    let source = root.path().join("large-source");
    let format_option = match format {
        ObjectFormat::Sha256 => "--object-format=sha256",
        ObjectFormat::Sha1 => "--object-format=sha1",
    };
    git(
        None,
        [
            "init",
            "--quiet",
            "-b",
            "main",
            format_option,
            path(&source)?,
        ],
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
    {
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
    {
        assert!(trace.contains("acknowledgments"), "{trace}");
    }
    server.abort();
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_clients_report_one_durable_winner() -> TestResult {
    let _serial = SHARED_TEST.lock().await;
    let root = TempDir::new()?;
    let (url, server) = repository_server("git-http-concurrent", ObjectFormat::Sha1).await?;
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

    // The loser may see Busy during discovery or a stale ref after the winner.
    // This tests client-visible admission, not cross-process CAS contention.
    let left_push = tokio::task::spawn_blocking(move || {
        git_trace(Some(&left), ["push", "--quiet", "origin", "main"])
    });
    let right_push = tokio::task::spawn_blocking(move || {
        git_trace(Some(&right), ["push", "--quiet", "origin", "main"])
    });
    let (left_push, right_push) = tokio::try_join!(left_push, right_push)?;
    let left_push = left_push?;
    let right_push = right_push?;
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
    server.abort();
    Ok(())
}

enum RunningHost {
    Native(JoinHandle<()>),
    Spin(Option<(std::process::Child, String)>),
}

impl RunningHost {
    fn stop(&mut self) {
        match self {
            Self::Native(task) => task.abort(),
            Self::Spin(child) => {
                if let Some((mut child, pid)) = child.take() {
                    let _ = Command::new("kill").args(["-TERM", &pid]).status();
                    let _ = child.wait();
                }
            }
        }
    }
}
impl Drop for RunningHost {
    fn drop(&mut self) {
        self.stop();
    }
}

async fn serve_spin(format: ObjectFormat, prefix: &str) -> TestResult<(String, RunningHost)> {
    let port = std::net::TcpListener::bind("127.0.0.1:0")?
        .local_addr()?
        .port();
    let address = format!("127.0.0.1:{port}");
    let format = match format {
        ObjectFormat::Sha1 => "sha1",
        ObjectFormat::Sha256 => "sha256",
    };
    let artifact = std::env::temp_dir().join(format!("object-log-spin-{format}-{port}"));
    let log = std::fs::File::create(artifact.with_extension("log"))?;
    let rss = artifact.with_extension("rss");
    let run = Path::new(env!("CARGO_MANIFEST_DIR")).join("../object-log-git-spin/run.sh");
    let mut process = Command::new("/usr/bin/time");
    let time_report = if cfg!(target_os = "macos") {
        "-l"
    } else {
        "-v"
    };
    process
        .args([time_report, "-o"])
        .arg(&rss)
        .arg(run)
        .args(["--listen", &address]);
    for (variable, name) in [
        ("endpoint", "OBJECT_LOG_MINIO_ENDPOINT"),
        ("bucket", "OBJECT_LOG_MINIO_BUCKET"),
        ("access_key", "OBJECT_LOG_MINIO_ACCESS_KEY"),
        ("secret_key", "OBJECT_LOG_MINIO_SECRET_KEY"),
    ] {
        process
            .arg("--variable")
            .arg(format!("{variable}={}", required_env(name)?));
    }
    process.args([
        "--variable",
        &format!("prefix={prefix}"),
        "--variable",
        &format!("object_format={format}"),
    ]);
    let child = process.stdout(log.try_clone()?).stderr(log).spawn()?;
    let mut host = RunningHost::Spin(Some((child, String::new())));
    for _ in 0..100 {
        if let RunningHost::Spin(Some((child, pid))) = &mut host {
            if let Some(status) = child.try_wait()? {
                return Err(format!(
                    "Spin exited {status}: {}",
                    std::fs::read_to_string(artifact.with_extension("log"))?
                )
                .into());
            }
            if pid.is_empty() {
                let found = Command::new("pgrep")
                    .args(["-P", &child.id().to_string()])
                    .output()?;
                String::from_utf8(found.stdout)?.trim().clone_into(pid);
            }
        }
        if std::net::TcpStream::connect(&address).is_ok() {
            println!("Spin {format} RSS report: {}", rss.display());
            return Ok((format!("http://{address}/repo"), host));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Err("Spin startup timed out".into())
}

async fn repository_server(
    namespace: &str,
    format: ObjectFormat,
) -> TestResult<(String, JoinHandle<()>)> {
    let backend =
        ValidatedBackend::new(Arc::new(InMemory::new()), StorePath::from(namespace)).await?;
    let log = Log::open(&backend, &LogId::new("repository")?, Options::default()).await?;
    let app = SharedGitHttpServer::new(log, format).router();
    let (url, server) = serve(app).await?;
    Ok((url, server))
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
