//! Two individually bounded pushes forming one connected closure over 32768.
use super::{TestResult, configuration, decode, git, operator, serve, sustained_maintenance, text};
use object_log::TransactionId;
use std::{
    fs,
    io::Write,
    path::Path,
    process::{Command, Stdio},
    time::Instant,
};

#[tokio::test]
#[ignore = "requires local MinIO, Spin and release component/operator; grows a connected 32768-commit history"]
async fn operator_minio_growing_history_crosses_stored_pack_object_count() -> TestResult {
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
        let prefix = format!("growing-history-{}", TransactionId::new());
        let config = configuration(root.path(), &prefix, format)?;
        let (mut writer, url) = serve(&config, root.path()).await?;
        for start in [0, 16_384] {
            append_history(&source, start, 16_384)?;
            git(Some(&source), &["push", "-q", &url, "main"])?;
            assert_eq!(
                git(Some(&source), &["rev-list", "--count", "main"])?,
                format!("{}\n", start + 16_384).as_bytes()
            );
        }
        git(Some(&source), &["reset", "-q", "--hard", "main"])?;
        let tip = git(Some(&source), &["rev-parse", "HEAD"])?;
        let initial_count = git(Some(&source), &["rev-list", "--objects", "HEAD"])?
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .count();
        assert!(initial_count > 32_768);
        writer.stop()?;
        let (mut reader, url) = serve(&config, root.path()).await?;
        let client = root.path().join("clone");
        git(None, &["clone", "-q", &url, text(&client)?])?;
        check(&client, &tip, 32_768, b"initial")?;
        fs::write(source.join("file"), b"incremental")?;
        git(Some(&source), &["commit", "-q", "-am", "incremental"])?;
        let updated = git(Some(&source), &["rev-parse", "HEAD"])?;
        assert_eq!(git(Some(&source), &["rev-parse", "HEAD^"])?, tip);
        git(Some(&source), &["push", "-q", &url, "main"])?;
        git(Some(&client), &["fetch", "-q", "origin"])?;
        git(Some(&client), &["merge", "-q", "--ff-only", "origin/main"])?;
        check(&client, &updated, 32_769, b"incremental")?;
        reader.stop()?;
        let migration = operator(
            &config,
            &[
                "migrate-catalog",
                "--recovery-file",
                text(&root.path().join("migration.token"))?,
            ],
        )?;
        assert!(
            migration.status.success(),
            "{}",
            String::from_utf8_lossy(&migration.stderr)
        );
        assert_eq!(decode(&migration)?["outcome"], "migrated");
        // This fixture's objects are tiny: count, rather than the preferred
        // byte bound, must split the compaction output into multiple packs.
        sustained_maintenance(&config, root.path(), 2)?;
        let (mut cold, url) = serve(&config, root.path()).await?;
        let recovered = root.path().join("after-collection");
        git(None, &["clone", "-q", &url, text(&recovered)?])?;
        check(&recovered, &updated, 32_769, b"incremental")?;
        cold.stop()?;
        println!(
            "growing history PASS {format}: two accepted pushes, {initial_count} connected objects, full clone, incremental push/fetch, compaction/checkpoint/collection, cold clone/fsck; elapsed_seconds={:.3}",
            started.elapsed().as_secs_f64()
        );
    }
    Ok(())
}

fn check(repository: &Path, tip: &[u8], count: usize, content: &[u8]) -> TestResult {
    assert_eq!(git(Some(repository), &["rev-parse", "HEAD"])?, tip);
    assert_eq!(
        git(Some(repository), &["rev-list", "--count", "HEAD"])?,
        format!("{count}\n").as_bytes()
    );
    assert_eq!(fs::read(repository.join("file"))?, content);
    git(Some(repository), &["fsck", "--strict"])?;
    Ok(())
}

fn append_history(repository: &Path, start: usize, count: usize) -> TestResult {
    let parent = if start == 0 {
        None
    } else {
        Some(String::from_utf8(git(
            Some(repository),
            &["rev-parse", "refs/heads/main"],
        )?)?)
    };
    let mut bytes = Vec::new();
    for sequence in start..start + count {
        let message = format!("history {sequence}\n");
        write!(
            bytes,
            "commit refs/heads/main\ncommitter Test <test@example.invalid> 0 +0000\ndata {}\n{message}",
            message.len()
        )?;
        if sequence == start && start != 0 {
            writeln!(
                bytes,
                "from {}",
                parent.as_deref().ok_or("missing parent")?.trim()
            )?;
        }
        if sequence == 0 {
            bytes.extend_from_slice(b"M 100644 inline file\ndata 7\ninitial\n");
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
    let write_result = process
        .stdin
        .take()
        .ok_or("missing import stdin")?
        .write_all(&bytes);
    let output = process.wait_with_output()?;
    if !output.status.success() {
        return Err(format!("fast-import: {}", String::from_utf8_lossy(&output.stderr)).into());
    }
    write_result?;
    Ok(())
}
