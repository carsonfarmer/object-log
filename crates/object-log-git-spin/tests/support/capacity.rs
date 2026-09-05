// Included by minio.rs: opt-in ordinary Spin/MinIO transfer qualification.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires local MinIO, ordinary Spin and a release WASIp2 component"]
async fn spin_capacity_large_file_push_clone_and_fetch() -> TestResult {
    let _serial = TEST_LOCK.lock().await;
    let size = env::var("OBJECT_LOG_GIT_CAPACITY_BYTES")
        .map_or(Ok(50 * 1024 * 1024_u64), |value| value.parse::<u64>())?;
    assert!(size >= 50 * 1024 * 1024);
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let root = TempDir::new()?;
        let namespace = format!("spin-capacity-{}", TransactionId::new());
        let (url, mut server) = serve_spin(format, &namespace).await?;
        let source = root.path().join("source");
        let option = match format {
            ObjectFormat::Sha1 => "--object-format=sha1",
            ObjectFormat::Sha256 => "--object-format=sha256",
        };
        git(
            None,
            ["init", "--quiet", "-b", "main", option, path(&source)?],
        )?;
        let file = source.join("large");
        let random = Command::new("openssl")
            .args(["rand", "-out", path(&file)?, &size.to_string()])
            .output()?;
        if !random.status.success() {
            return Err(String::from_utf8_lossy(&random.stderr).into_owned().into());
        }
        assert_eq!(std::fs::metadata(&file)?.len(), size);
        let blob = git_stdout(Some(&source), ["hash-object", "large"])?;
        git(Some(&source), ["add", "large"])?;
        git(
            Some(&source),
            ["commit", "--quiet", "-m", "large regular file"],
        )?;
        let target = git_stdout(Some(&source), ["rev-parse", "HEAD"])?;
        let start = std::time::Instant::now();
        git(Some(&source), ["push", "--quiet", &url, "main"])?;
        eprintln!(
            "capacity {format:?} {size} bytes: ordinary push {:?}",
            start.elapsed()
        );
        server.stop()?;
        let packed_bytes = capacity_initial_pack_bytes(format, &namespace).await?;
        eprintln!(
            "capacity {format:?}: verified incoming self-contained pack {packed_bytes} bytes"
        );
        assert!(packed_bytes >= size);
        let (url, mut server) = serve_spin(format, &namespace).await?;
        capacity_push_blob_tags(&source, &blob, &url)?;
        let clone = root.path().join("clone");
        let start = std::time::Instant::now();
        git(None, ["clone", "--quiet", &url, path(&clone)?])?;
        eprintln!(
            "capacity {format:?} {size} bytes: cold clone {:?}",
            start.elapsed()
        );
        assert_eq!(std::fs::metadata(clone.join("large"))?.len(), size);
        assert_eq!(git_stdout(Some(&clone), ["hash-object", "large"])?, blob);
        assert_eq!(git_stdout(Some(&clone), ["rev-parse", "HEAD"])?, target);
        capacity_blob_tags(&clone, &blob)?;
        git(Some(&clone), ["fsck", "--strict"])?;
        let fetched = root.path().join("fetched");
        git(None, ["init", "--quiet", "--bare", option, path(&fetched)?])?;
        let start = std::time::Instant::now();
        git(Some(&fetched), ["fetch", "--quiet", "--tags", &url, "main"])?;
        eprintln!(
            "capacity {format:?} {size} bytes: fresh fetch {:?}",
            start.elapsed()
        );
        assert_eq!(
            git_stdout(Some(&fetched), ["rev-parse", "FETCH_HEAD"])?,
            target
        );
        assert_eq!(
            git_stdout(Some(&fetched), ["rev-parse", "FETCH_HEAD:large"])?,
            blob
        );
        assert_eq!(
            git_stdout(Some(&fetched), ["cat-file", "-s", "FETCH_HEAD:large"])?,
            size.to_string()
        );
        capacity_blob_tags(&fetched, &blob)?;
        git(Some(&fetched), ["fsck", "--strict"])?;
        if size <= 64 * 1024 * 1024 {
            capacity_incremental(&source, &clone, &url, size)?;
        }
        server.stop()?;
        capacity_maintenance(format, &namespace).await?;
        let (url, mut server) = serve_spin(format, &namespace).await?;
        let recovered = root.path().join("recovered");
        git(None, ["clone", "--quiet", &url, path(&recovered)?])?;
        assert_eq!(
            git_stdout(Some(&recovered), ["hash-object", "large"])?,
            git_stdout(Some(&source), ["hash-object", "large"])?
        );
        capacity_blob_tags(&recovered, &blob)?;
        git(Some(&recovered), ["fsck", "--strict"])?;
        server.stop()?;
    }
    Ok(())
}

fn capacity_incremental(source: &Path, clone: &Path, url: &str, size: u64) -> TestResult {
    use std::io::{Seek, SeekFrom, Write};
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .open(source.join("large"))?;
    file.seek(SeekFrom::Start(size / 2))?;
    file.write_all(b"small edit within a large ordinary Git blob")?;
    drop(file);
    git(
        Some(source),
        ["commit", "--quiet", "-am", "small large-file edit"],
    )?;
    git(Some(source), ["push", "--quiet", url, "main"])?;
    git(Some(clone), ["fetch", "--quiet", url, "main"])?;
    assert_eq!(
        git_stdout(Some(clone), ["rev-parse", "FETCH_HEAD:large"])?,
        git_stdout(Some(source), ["hash-object", "large"])?
    );
    git(Some(clone), ["fsck", "--strict"])?;
    Ok(())
}

async fn capacity_maintenance(format: ObjectFormat, namespace: &str) -> TestResult {
    use object_log_git::Repository;
    let backend =
        ValidatedBackend::new(Arc::new(build_minio()?), StorePath::from(namespace)).await?;
    // Host durable profile V1, opened without creating or changing its geometry.
    let log = Log::open_existing(
        &backend,
        &LogId::new("repository")?,
        Options {
            max_object_refs: 2080,
            ..Options::default()
        },
    )
    .await?;
    assert!(matches!(
        Repository::migrate_catalog(&log, format, TransactionId::new()).await?,
        Some(object_log::CommitStatus::Committed(_))
    ));
    let started = std::time::Instant::now();
    eprintln!("capacity {format:?}: compacting live packs");
    assert!(matches!(
        Repository::compact_packs(&log, format, TransactionId::new()).await?,
        object_log::CommitStatus::Committed(_)
    ));
    eprintln!("capacity {format:?}: compacted in {:?}", started.elapsed());
    let repository = Repository::open(&log, format).await?;
    let object_log::CheckpointStatus::Published(view) = repository.checkpoint().await? else {
        return Err("large-file checkpoint did not publish".into());
    };
    let object_log::CollectionStart::Installed(fenced, _) = log.start_collection(&view).await?
    else {
        return Err("large-file collection did not start".into());
    };
    assert!(matches!(
        log.resume_collection(&fenced).await?,
        object_log::CollectionFinish::Complete(..)
    ));
    eprintln!(
        "capacity {format:?}: compact/checkpoint/GC completed in {:?}",
        started.elapsed()
    );
    Ok(())
}

// A three-object full commit/tree/blob pack takes Scanned::normalize's identity
// fast path: it stages exactly the received bytes, without rewriting or bases.
async fn capacity_initial_pack_bytes(format: ObjectFormat, namespace: &str) -> TestResult<u64> {
    use std::io::Write;
    use std::process::Stdio;
    let backend =
        ValidatedBackend::new(Arc::new(build_minio()?), StorePath::from(namespace)).await?;
    let log = Log::open_existing(
        &backend,
        &LogId::new("repository")?,
        Options {
            max_object_refs: 2080,
            ..Options::default()
        },
    )
    .await?;
    let view = log.load().await?;
    let mut packs = Vec::new();
    for record in log.read_tail(&view).await? {
        for object in record.objects() {
            let node = log.read_node(&view, object).await?;
            if node.payload().starts_with(b"\xfftOc\0\0\0\x02") {
                packs.push(node);
            }
        }
    }
    assert_eq!(packs.len(), 1);
    let node = &packs[0];
    let option = match format {
        ObjectFormat::Sha1 => "--object-format=sha1",
        ObjectFormat::Sha256 => "--object-format=sha256",
    };
    let mut child = git_command(None, ["show-index", option])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    child
        .stdin
        .take()
        .ok_or("show-index stdin missing")?
        .write_all(node.payload())?;
    let output = child.wait_with_output()?;
    assert!(output.status.success());
    let offsets = String::from_utf8(output.stdout)?
        .lines()
        .map(|line| {
            line.split_whitespace()
                .next()
                .ok_or("index offset missing")?
                .parse::<u64>()
                .map_err(|error| Box::new(error) as Box<dyn StdError + Send + Sync>)
        })
        .collect::<TestResult<Vec<_>>>()?;
    assert_eq!(offsets.len(), 3);
    let first = log.read_object(&view, &node.children()[0]).await?;
    assert_eq!(&first[..12], b"PACK\0\0\0\x02\0\0\0\x03");
    let mut kinds = Vec::new();
    for offset in offsets {
        let mut start = 0;
        for chunk in node.children() {
            if offset < start + chunk.len() {
                let bytes = log.read_object(&view, chunk).await?;
                kinds.push((bytes[usize::try_from(offset - start)?] >> 4) & 7);
                break;
            }
            start += chunk.len();
        }
    }
    kinds.sort_unstable();
    assert_eq!(kinds, [1, 2, 3]); // Full commit/tree/blob, no REF/OFS deltas.
    Ok(node.children().iter().map(object_log::ObjectRef::len).sum())
}

fn capacity_blob_tags(repository: &Path, blob: &str) -> TestResult {
    for tag in ["refs/tags/blob-lightweight", "refs/tags/blob-annotated^{}"] {
        assert_eq!(git_stdout(Some(repository), ["rev-parse", tag])?, blob);
        assert_eq!(
            git_stdout(Some(repository), ["cat-file", "-t", tag])?,
            "blob"
        );
    }
    Ok(())
}

fn capacity_push_blob_tags(source: &Path, blob: &str, url: &str) -> TestResult {
    git(Some(source), ["tag", "blob-lightweight", blob])?;
    git(
        Some(source),
        ["tag", "-a", "blob-annotated", blob, "-m", "large blob tag"],
    )?;
    git(Some(source), ["push", "--quiet", "--tags", url])
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires local MinIO, ordinary Spin and a release WASIp2 component"]
async fn spin_capacity_interrupted_large_upload_preserves_authority() -> TestResult {
    use futures::TryStreamExt;
    use object_store::ObjectStore;
    let _serial = TEST_LOCK.lock().await;
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let root = TempDir::new()?;
        let namespace = format!("spin-interrupted-{}", TransactionId::new());
        let prefix = StorePath::from(namespace.clone());
        let store = Arc::new(build_minio()?);
        let backend = ValidatedBackend::new(store.clone(), prefix.clone()).await?;
        let log = Log::open(
            &backend,
            &LogId::new("repository")?,
            Options {
                max_object_refs: 2080,
                ..Options::default()
            },
        )
        .await?;
        let source = root.path().join("source");
        let option = match format {
            ObjectFormat::Sha1 => "--object-format=sha1",
            ObjectFormat::Sha256 => "--object-format=sha256",
        };
        git(
            None,
            ["init", "--quiet", "-b", "main", option, path(&source)?],
        )?;
        std::fs::write(source.join("file"), b"before interruption")?;
        git(Some(&source), ["add", "file"])?;
        git(Some(&source), ["commit", "--quiet", "-m", "initial"])?;
        let (url, mut server) = serve_spin(format, &namespace).await?;
        git(Some(&source), ["push", "--quiet", &url, "main"])?;
        let old = git_stdout(Some(&source), ["rev-parse", "HEAD"])?;
        let before = log.load().await?;
        let refs = git_stdout(None, ["ls-remote", &url])?;
        let objects = store.list(Some(&prefix)).try_collect::<Vec<_>>().await?;
        let random = Command::new("openssl")
            .args(["rand", "-out", path(&source.join("file"))?, "12582912"])
            .output()?;
        assert!(random.status.success());
        git(
            Some(&source),
            ["commit", "--quiet", "-am", "interrupted then retried"],
        )?;
        let target = git_stdout(Some(&source), ["rev-parse", "HEAD"])?;
        capacity_disconnect_pack(&source, &url, &old, &target, format)?;
        assert!(log.refresh(&before).await?.is_none());
        assert_eq!(git_stdout(None, ["ls-remote", &url])?, refs);
        let after = store.list(Some(&prefix)).try_collect::<Vec<_>>().await?;
        assert!(
            after.iter().any(|object| object.size >= 1024 * 1024
                && !objects.iter().any(|old| old.location == object.location)),
            "disconnect must occur after immutable pack chunks reached the provider"
        );
        server.stop()?;
        let (url, mut server) = serve_spin(format, &namespace).await?;
        assert_eq!(git_stdout(None, ["ls-remote", &url])?, refs);
        git(Some(&source), ["push", "--quiet", &url, "main"])?;
        let clone = root.path().join("cold");
        git(None, ["clone", "--quiet", &url, path(&clone)?])?;
        assert_eq!(git_stdout(Some(&clone), ["rev-parse", "HEAD"])?, target);
        git(Some(&clone), ["fsck", "--strict"])?;
        server.stop()?;
        eprintln!(
            "capacity {format:?}: disconnect after sending 5 MiB; provider staging confirmed, head/refs preserved; cold push/clone passed"
        );
    }
    Ok(())
}

fn capacity_disconnect_pack(
    source: &Path,
    url: &str,
    old: &str,
    target: &str,
    format: ObjectFormat,
) -> TestResult {
    use std::io::{Read, Write};
    use std::net::{Shutdown, TcpStream};
    use std::process::Stdio;
    let address = url
        .strip_prefix("http://")
        .and_then(|url| url.strip_suffix("/repo"))
        .ok_or("unexpected fixture URL")?;
    let mut socket = TcpStream::connect(address)?;
    socket.set_read_timeout(Some(Duration::from_secs(30)))?;
    socket.set_write_timeout(Some(Duration::from_secs(30)))?;
    let hash = match format {
        ObjectFormat::Sha1 => "sha1",
        ObjectFormat::Sha256 => "sha256",
    };
    let command = format!("{old} {target} refs/heads/main\0report-status object-format={hash}\n");
    let controls = format!("{:04x}{command}0000", command.len() + 4);
    write!(
        socket,
        "POST /repo/git-receive-pack HTTP/1.1\r\nHost: {address}\r\nContent-Type: application/x-git-receive-pack-request\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{controls}",
        16 * 1024 * 1024 + controls.len()
    )?;
    let mut child = git_command(
        Some(source),
        ["pack-objects", "--stdout", "--all", "--delta-base-offset"],
    )
    .stdout(Stdio::piped())
    .stderr(Stdio::null())
    .spawn()?;
    let stdout = child.stdout.take().ok_or("pack stdout missing")?;
    let written = std::io::copy(&mut stdout.take(5 * 1024 * 1024), &mut socket);
    let _ = child.kill();
    child.wait()?;
    assert_eq!(written?, 5 * 1024 * 1024);
    socket.shutdown(Shutdown::Write)?;
    let mut response = Vec::new();
    socket.take(1024 * 1024).read_to_end(&mut response)?;
    Ok(())
}
