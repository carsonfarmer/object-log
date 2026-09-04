use std::{error::Error as StdError, path::Path, process::Command, sync::Arc};

use object_log::{Log, LogId, Options, ValidatedBackend};
use object_log_git_http::{Service, SmartHttp};
use object_store::{memory::InMemory, path::Path as StorePath};
use tempfile::TempDir;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    task::JoinHandle,
};

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
    let (url, server) = serve(SmartHttp::new(log, scratch.clone())).await?;
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

async fn serve(endpoint: SmartHttp) -> TestResult<(String, JoinHandle<()>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let endpoint = Arc::new(endpoint);
    let task = tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let endpoint = Arc::clone(&endpoint);
            tokio::spawn(async move {
                if let Err(error) = respond(stream, &endpoint).await {
                    eprintln!("server: {error}");
                }
            });
        }
    });
    Ok((format!("http://{address}/repo"), task))
}

async fn respond(stream: TcpStream, endpoint: &SmartHttp) -> TestResult {
    let (read, mut write) = stream.into_split();
    let mut read = BufReader::new(read);
    let mut line = String::new();
    read.read_line(&mut line).await?;
    let request_line = std::mem::take(&mut line);
    let mut request = request_line.trim_end().split(' ');
    let method = request.next().ok_or("missing method")?.to_owned();
    let target = request.next().ok_or("missing target")?.to_owned();
    let mut content_length = 0;
    loop {
        line.clear();
        read.read_line(&mut line).await?;
        if line == "\r\n" {
            break;
        }
        if let Some(value) = line
            .strip_prefix("Content-Length: ")
            .or_else(|| line.strip_prefix("content-length: "))
        {
            content_length = value.trim().parse()?;
        }
    }
    let mut body = vec![0; content_length];
    read.read_exact(&mut body).await?;
    let mut input = body.as_slice();
    let mut response = Vec::new();
    let (service, advertisement) = match (method.as_str(), target.as_str()) {
        ("GET", "/repo/info/refs?service=git-upload-pack") => (Service::UploadPack, true),
        ("GET", "/repo/info/refs?service=git-receive-pack") => (Service::ReceivePack, true),
        ("POST", "/repo/git-upload-pack") => (Service::UploadPack, false),
        ("POST", "/repo/git-receive-pack") => (Service::ReceivePack, false),
        _ => return Err("unsupported request".into()),
    };
    if advertisement {
        endpoint.advertise(service, &mut response).await?;
    } else {
        match service {
            Service::UploadPack => endpoint.upload_pack(&mut input, &mut response).await?,
            Service::ReceivePack => {
                endpoint.receive_pack(&mut input, &mut response).await?;
            }
        }
    }
    let content_type = if advertisement {
        service.advertisement_content_type()
    } else {
        service.result_content_type()
    };
    write
        .write_all(
            format!(
                "HTTP/1.1 200 OK\r\nContent-Type: {}\r\nCache-Control: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                content_type,
                service.cache_control(),
                response.len()
            )
            .as_bytes(),
        )
        .await?;
    write.write_all(&response).await?;
    Ok(())
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
