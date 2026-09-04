use std::{error::Error as StdError, path::Path, process::Command, sync::Arc};

use axum::Router;
use object_log::{Log, LogId, Options, ValidatedBackend};
use object_log_git_http::{GitHttpServer, SmartHttp};
use object_store::{memory::InMemory, path::Path as StorePath};
use tempfile::TempDir;
use tokio::{net::TcpListener, task::JoinHandle};

type TestResult<T = ()> = Result<T, Box<dyn StdError + Send + Sync>>;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unmodified_git_pushes_clones_fetches_and_rejects_stale_updates() -> TestResult {
    let root = TempDir::new()?;
    let backend = ValidatedBackend::new(
        Arc::new(InMemory::new()),
        StorePath::from("git-http-loopback"),
    )
    .await?;
    let log = Log::open(
        backend.scope(&LogId::new("repository")?),
        Options::default(),
    )
    .await?;
    let scratch = root.path().join("scratch");
    let app = GitHttpServer::new(SmartHttp::new(log, &scratch), &scratch, 4).router();
    let (url, server) = serve(app).await?;
    assert!(git_output(None, ["ls-remote", &url])?.stdout.is_empty());

    let source = root.path().join("source");
    git(None, ["init", "--quiet", "-b", "main", path(&source)?])?;
    write(&source, "one")?;
    git(Some(&source), ["add", "file"])?;
    git(Some(&source), ["commit", "--quiet", "-m", "one"])?;
    git(Some(&source), ["remote", "add", "origin", &url])?;
    git(Some(&source), ["push", "--quiet", "-u", "origin", "main"])?;

    let clone = root.path().join("clone");
    git(None, ["clone", "--quiet", &url, path(&clone)?])?;
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
    assert!(std::fs::read_dir(scratch)?.next().is_none());
    server.abort();
    Ok(())
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
        Err(String::from_utf8_lossy(&output.stderr).into_owned().into())
    }
}

fn git_output<const N: usize>(
    directory: Option<&Path>,
    args: [&str; N],
) -> TestResult<std::process::Output> {
    let mut command = Command::new("git");
    command
        .args(args)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_PROTOCOL", "version=2")
        .env("GIT_AUTHOR_NAME", "Object Log")
        .env("GIT_AUTHOR_EMAIL", "object-log@example.invalid")
        .env("GIT_COMMITTER_NAME", "Object Log")
        .env("GIT_COMMITTER_EMAIL", "object-log@example.invalid");
    if let Some(directory) = directory {
        command.current_dir(directory);
    }
    Ok(command.output()?)
}
