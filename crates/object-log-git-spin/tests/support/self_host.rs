//! Exercise the real repository history through the existing operator fixture.
use super::{TestResult, configuration, decode, git, operator, serve, sustained_maintenance, text};
use object_log::TransactionId;
use std::{env, fs, path::Path, time::Instant};

struct Snapshot {
    tip: Vec<u8>,
    tree: Vec<u8>,
    commits: Vec<u8>,
}

impl Snapshot {
    fn read(repository: &Path) -> TestResult<Self> {
        Ok(Self {
            tip: git(Some(repository), &["rev-parse", "HEAD"])?,
            tree: git(Some(repository), &["rev-parse", "HEAD^{tree}"])?,
            commits: git(Some(repository), &["rev-list", "--count", "HEAD"])?,
        })
    }

    fn verify(&self, repository: &Path) -> TestResult {
        let observed = Self::read(repository)?;
        assert_eq!(observed.tip, self.tip);
        assert_eq!(observed.tree, self.tree);
        assert_eq!(observed.commits, self.commits);
        assert_eq!(
            git(Some(repository), &["symbolic-ref", "HEAD"])?,
            b"refs/heads/main\n"
        );
        assert!(git(Some(repository), &["status", "--porcelain"])?.is_empty());
        git(Some(repository), &["fsck", "--strict"])?;
        Ok(())
    }
}

#[tokio::test]
#[ignore = "requires local MinIO, Spin and release component/operator; pushes real main history"]
async fn operator_minio_hosts_object_log_history() -> TestResult {
    let started = Instant::now();
    let root = tempfile::tempdir()?;
    let source = root.path().join("source");
    isolate_source(&source)?;
    let initial = Snapshot::read(&source)?;
    let format = git(Some(&source), &["rev-parse", "--show-object-format"])?;
    let format = std::str::from_utf8(&format)?.trim();
    let prefix = format!("self-host-{}", TransactionId::new());
    let config = configuration(root.path(), &prefix, format)?;
    let (mut writer, url) = serve(&config, root.path()).await?;
    git(Some(&source), &["push", "-q", &url, "HEAD:refs/heads/main"])?;
    writer.stop()?;

    let (mut reader, url) = serve(&config, root.path()).await?;
    let clone = root.path().join("clone");
    git(None, &["clone", "-q", &url, text(&clone)?])?;
    initial.verify(&clone)?;
    println!(
        "self-host: initial cold clone verified, format={format}, tip={}, commits={}, elapsed_seconds={:.3}",
        std::str::from_utf8(&initial.tip)?.trim(),
        std::str::from_utf8(&initial.commits)?.trim(),
        started.elapsed().as_secs_f64()
    );
    let marker = format!("self-host-{}.txt", TransactionId::new());
    fs::write(source.join(&marker), b"object-log serves its own history\n")?;
    git(Some(&source), &["add", "--", &marker])?;
    git(
        Some(&source),
        &["commit", "-q", "-m", "self-host increment"],
    )?;
    let updated = Snapshot::read(&source)?;
    assert_eq!(git(Some(&source), &["rev-parse", "HEAD^"])?, initial.tip);
    git(Some(&source), &["push", "-q", &url, "HEAD:refs/heads/main"])?;
    git(Some(&clone), &["fetch", "-q", "origin"])?;
    git(Some(&clone), &["merge", "-q", "--ff-only", "origin/main"])?;
    updated.verify(&clone)?;
    reader.stop()?;

    let migration = operator(
        &config,
        &[
            "migrate-catalog",
            "--recovery-file",
            text(&root.path().join("migration.token"))?,
        ],
    )?;
    assert!(migration.status.success());
    assert_eq!(decode(&migration)?["outcome"], "migrated");
    sustained_maintenance(&config, root.path(), 2)?;
    let (mut cold, url) = serve(&config, root.path()).await?;
    let after = root.path().join("after-maintenance");
    git(None, &["clone", "-q", &url, text(&after)?])?;
    updated.verify(&after)?;
    assert_eq!(
        fs::read(after.join(marker))?,
        b"object-log serves its own history\n"
    );
    assert_eq!(
        git(None, &["ls-remote", "--refs", &url])?,
        format!(
            "{}\trefs/heads/main\n",
            std::str::from_utf8(&updated.tip)?.trim()
        )
        .as_bytes()
    );
    cold.stop()?;
    println!(
        "self-host PASS: full-history push, cold clone, incremental push/fetch, migration, compaction/checkpoint/collection, final cold clone/fsck; tip={}, elapsed_seconds={:.3}",
        std::str::from_utf8(&updated.tip)?.trim(),
        started.elapsed().as_secs_f64()
    );
    Ok(())
}

fn isolate_source(destination: &Path) -> TestResult {
    let repository = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    assert_eq!(
        git(Some(&repository), &["rev-parse", "--is-shallow-repository"])?,
        b"false\n"
    );
    let revision = env::var("OBJECT_LOG_SELF_HOST_REV").unwrap_or_else(|_| "main".into());
    let pinned = git(
        Some(&repository),
        &["rev-parse", "--verify", &format!("{revision}^{{commit}}")],
    )?;
    let pin = std::str::from_utf8(&pinned)?.trim();
    git(
        Some(&repository),
        &["merge-base", "--is-ancestor", pin, "main"],
    )?;
    println!("self-host: source revision pinned to {pin}");
    git(
        None,
        &[
            "clone",
            "-q",
            "--single-branch",
            "--branch",
            "main",
            "--no-tags",
            "--no-hardlinks",
            text(&repository)?,
            text(destination)?,
        ],
    )?;
    git(Some(destination), &["checkout", "-q", "-B", "main", pin])?;
    assert_eq!(git(Some(destination), &["rev-parse", "HEAD"])?, pinned);
    Ok(())
}
