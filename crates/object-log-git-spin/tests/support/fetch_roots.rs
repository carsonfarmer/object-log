//! Ordinary Git targeted fetch beside unrelated histories over the graph cap.
use super::{TestResult, configuration, git, serve, text};
use object_log::TransactionId;
use std::{
    fs,
    io::Write,
    path::Path,
    process::{Command, Stdio},
    time::Instant,
};

#[tokio::test]
#[ignore = "requires local MinIO, Spin and release component; pushes two 16384-commit histories"]
async fn operator_minio_targeted_fetch_ignores_unrelated_histories() -> TestResult {
    let started = Instant::now();
    for format in ["sha1", "sha256"] {
        let root = tempfile::tempdir()?;
        let source = root.path().join("source");
        git(
            None,
            &[
                "init",
                "-q",
                "-b",
                "main",
                &format!("--object-format={format}"),
                text(&source)?,
            ],
        )?;
        fs::write(source.join("file"), b"before\n")?;
        git(Some(&source), &["add", "file"])?;
        git(Some(&source), &["commit", "-q", "-m", "before"])?;
        let old = git(Some(&source), &["rev-parse", "HEAD"])?;
        let prefix = format!("fetch-roots-{}", TransactionId::new());
        let config = configuration(root.path(), &prefix, format)?;
        let (mut host, url) = serve(&config, root.path()).await?;
        git(Some(&source), &["push", "-q", &url, "main"])?;
        let client = root.path().join("client");
        git(
            None,
            &[
                "clone",
                "-q",
                "--single-branch",
                "--branch",
                "main",
                &url,
                text(&client)?,
            ],
        )?;
        for branch in 0..2 {
            let name = format!("unrelated-{branch}");
            import_history(&source, &name, 16_384)?;
            assert_eq!(
                git(Some(&source), &["rev-list", "--count", &name])?,
                b"16384\n"
            );
            git(
                Some(&source),
                &[
                    "tag",
                    "-a",
                    &format!("tag-{name}"),
                    "-m",
                    "unrelated",
                    &name,
                ],
            )?;
            git(
                Some(&source),
                &["push", "-q", &url, &name, &format!("refs/tags/tag-{name}")],
            )?;
        }
        fs::write(source.join("file"), b"after\n")?;
        git(Some(&source), &["commit", "-q", "-am", "after"])?;
        git(Some(&source), &["tag", "-a", "inner", "-m", "inner"])?;
        git(
            Some(&source),
            &["tag", "-a", "outer", "-m", "outer", "inner"],
        )?;
        git(
            Some(&source),
            &["push", "-q", &url, "main", "refs/tags/outer"],
        )?;
        let tip = git(Some(&source), &["rev-parse", "HEAD"])?;
        assert_eq!(git(Some(&source), &["rev-parse", "HEAD^"])?, old);
        host.stop()?;
        let (mut cold, url) = serve(&config, root.path()).await?;
        git(Some(&client), &["remote", "set-url", "origin", &url])?;
        git(Some(&client), &["fetch", "-q", "origin", "main"])?;
        git(Some(&client), &["merge", "-q", "--ff-only", "FETCH_HEAD"])?;
        check_main(&client, &tip)?;
        let fresh = root.path().join("fresh");
        git(
            None,
            &[
                "clone",
                "-q",
                "--single-branch",
                "--branch",
                "main",
                &url,
                text(&fresh)?,
            ],
        )?;
        check_main(&fresh, &tip)?;
        assert_eq!(git(Some(&fresh), &["rev-parse", "outer^{}"])?, tip);
        cold.stop()?;
        println!(
            "targeted fetch PASS {format}: two unrelated 16384-commit branches, native main push/fetch, cold single-branch clone, related nested tag, fsck; elapsed_seconds={:.3}",
            started.elapsed().as_secs_f64()
        );
    }
    Ok(())
}

fn check_main(repository: &Path, tip: &[u8]) -> TestResult {
    assert_eq!(git(Some(repository), &["rev-parse", "HEAD"])?, tip);
    assert_eq!(
        git(Some(repository), &["rev-list", "--count", "HEAD"])?,
        b"2\n"
    );
    assert_eq!(fs::read(repository.join("file"))?, b"after\n");
    assert!(!String::from_utf8(git(Some(repository), &["show-ref"])?)?.contains("unrelated"));
    git(Some(repository), &["fsck", "--strict"])?;
    Ok(())
}

fn import_history(repository: &Path, branch: &str, count: usize) -> TestResult {
    let mut bytes = Vec::new();
    for sequence in 0..count {
        let message = format!("{branch} {sequence}\n");
        write!(
            bytes,
            "commit refs/heads/{branch}\ncommitter Test <test@example.invalid> 0 +0000\ndata {}\n{message}",
            message.len()
        )?;
        if sequence == 0 {
            write!(
                bytes,
                "M 100644 inline unrelated\ndata {}\n{branch}\n",
                branch.len()
            )?;
        }
        bytes.push(b'\n');
    }
    let mut process = Command::new("git")
        .current_dir(repository)
        .args(["fast-import", "--quiet"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    process
        .stdin
        .take()
        .ok_or("missing import stdin")?
        .write_all(&bytes)?;
    let output = process.wait_with_output()?;
    if !output.status.success() {
        return Err(format!("fast-import: {}", String::from_utf8_lossy(&output.stderr)).into());
    }
    Ok(())
}
