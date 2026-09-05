// Opt-in regression for a fetch that combines separately accepted stored packs.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires native MinIO, ordinary Spin, release WASIp2 and several GiB of free disk"]
async fn spin_capacity_clone_combines_packs_beyond_old_output_limit() -> TestResult {
    use futures::{FutureExt, StreamExt, TryStreamExt};
    use object_store::ObjectStore;
    let _serial = TEST_LOCK.lock().await;
    let store = build_minio()?;
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let namespace = format!("spin-aggregate-{}", TransactionId::new());
        let result = std::panic::AssertUnwindSafe(aggregate_capacity_clone(format, &namespace))
            .catch_unwind()
            .await;
        // The serving group and local TempDirs are gone before provider cleanup,
        // including on failure. Delete only this case's unique fixture prefix.
        let prefix = StorePath::from(namespace);
        let cleanup = store
            .delete_stream(
                store
                    .list(Some(&prefix))
                    .map_ok(|item| item.location)
                    .boxed(),
            )
            .try_collect::<Vec<_>>()
            .await;
        let result = result.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
        result?;
        cleanup?;
    }
    Ok(())
}

async fn aggregate_capacity_clone(format: ObjectFormat, namespace: &str) -> TestResult {
    use std::io::Read;
    const BLOB_BYTES: u64 = 720 * 1024 * 1024;
    const OLD_OUTPUT_LIMIT: u64 = 2080 * 1024 * 1024;
    let option = match format {
        ObjectFormat::Sha1 => "--object-format=sha1",
        ObjectFormat::Sha256 => "--object-format=sha256",
    };
    let (url, mut server) = serve_spin(format, namespace).await?;
    let mut expected = Vec::new();
    for (index, branch) in ["main", "second", "third"].into_iter().enumerate() {
        // Independent roots retain all three blobs. Drop each source (working
        // file and loose objects) immediately after its separate accepted push.
        let source = TempDir::new()?;
        git(
            None,
            [
                "init",
                "--quiet",
                "-b",
                branch,
                option,
                path(source.path())?,
            ],
        )?;
        aggregate_capacity_blob(&source.path().join("large"), index, BLOB_BYTES)?;
        let blob = git_stdout(Some(source.path()), ["hash-object", "large"])?;
        git(Some(source.path()), ["add", "large"])?;
        git(Some(source.path()), ["commit", "--quiet", "-m", branch])?;
        let tip = git_stdout(Some(source.path()), ["rev-parse", "HEAD"])?;
        assert_eq!(
            git_stdout(Some(source.path()), ["rev-list", "--count", "HEAD"])?,
            "1"
        );
        git(Some(source.path()), ["push", "--quiet", &url, branch])?;
        assert!(!expected.iter().any(|(_, existing, _)| existing == &blob));
        expected.push((branch, blob, tip));
        eprintln!("aggregate {format:?}: accepted independent {branch} push");
    }
    server.stop()?;
    let (url, mut server) = serve_spin(format, namespace).await?;
    let clone = TempDir::new()?;
    git(
        None,
        [
            "clone",
            "--quiet",
            "--no-checkout",
            &url,
            path(clone.path())?,
        ],
    )?;
    let packs = std::fs::read_dir(clone.path().join(".git/objects/pack"))?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "pack")
        })
        .collect::<Vec<_>>();
    // A fresh clone has no delta bases: Git retains the received self-contained
    // pack. Measure that actual encoded pack, not decoded input/blob sizes.
    assert_eq!(packs.len(), 1, "fresh clone must retain one received pack");
    let emitted_bytes = std::fs::metadata(&packs[0])?.len();
    assert!(
        emitted_bytes > OLD_OUTPUT_LIMIT,
        "aggregate pack only {emitted_bytes} bytes"
    );
    let mut header = [0; 12];
    std::fs::File::open(&packs[0])?.read_exact(&mut header)?;
    assert_eq!(&header[..8], b"PACK\0\0\0\x02");
    assert_eq!(u32::from_be_bytes(header[8..].try_into()?), 9);
    for (branch, blob, tip) in expected {
        let reference = format!("refs/remotes/origin/{branch}");
        assert_eq!(
            git_stdout(Some(clone.path()), ["rev-parse", &reference])?,
            tip
        );
        assert_eq!(
            git_stdout(
                Some(clone.path()),
                ["rev-parse", &format!("{reference}:large")]
            )?,
            blob
        );
        // Checkout one file at a time; recompute its OID from the actual bytes.
        git(
            Some(clone.path()),
            ["checkout", "--quiet", "--detach", &reference],
        )?;
        assert_eq!(
            std::fs::metadata(clone.path().join("large"))?.len(),
            BLOB_BYTES
        );
        assert_eq!(
            git_stdout(Some(clone.path()), ["hash-object", "large"])?,
            blob
        );
    }
    git(Some(clone.path()), ["fsck", "--strict"])?;
    server.stop()?;
    eprintln!("aggregate {format:?}: cold clone verified {emitted_bytes} emitted pack bytes");
    Ok(())
}

fn aggregate_capacity_blob(file: &Path, index: usize, bytes: u64) -> TestResult {
    use std::io::Read;
    use std::process::Stdio;
    // AES-CTR over zeros gives distinct reproducible incompressible inputs,
    // streamed through OpenSSL without retaining a large buffer in the test.
    let key = format!("{:064x}", index + 1);
    let mut child = Command::new("openssl")
        .args([
            "enc",
            "-aes-256-ctr",
            "-nosalt",
            "-K",
            &key,
            "-iv",
            "00000000000000000000000000000000",
        ])
        .stdin(Stdio::piped())
        .stdout(std::fs::File::create(file)?)
        .stderr(Stdio::piped())
        .spawn()?;
    let mut input = child.stdin.take().ok_or("OpenSSL stdin missing")?;
    let written = std::io::copy(&mut std::io::repeat(0).take(bytes), &mut input);
    drop(input);
    let output = child.wait_with_output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(written?, bytes);
    assert_eq!(std::fs::metadata(file)?.len(), bytes);
    Ok(())
}
