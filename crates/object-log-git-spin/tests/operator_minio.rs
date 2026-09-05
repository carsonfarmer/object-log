//! Opt-in command-process tests against the same WAL served by real Spin.
#![cfg(all(feature = "operator", unix, not(target_arch = "wasm32")))]

use bytes::Bytes;
use object_log::{Log, LogId, Options, TransactionId, ValidatedBackend};
use object_log_git::{ObjectFormat, Repository};
use object_store::{ObjectStoreExt as _, aws::AmazonS3Builder, path::Path as StorePath};
use serde_json::Value;
use std::{
    env,
    fmt::Write as _,
    fs,
    io::Write,
    os::unix::{fs::OpenOptionsExt, process::CommandExt},
    path::Path,
    process::{Child, Command, Output},
    sync::Arc,
    time::Duration,
};

#[path = "support/spin_process.rs"]
mod spin_process;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

#[tokio::test]
#[ignore = "requires Spin 4 and a release WASIp2 component, but no provider"]
async fn operator_spin_process_group_shutdown_closes_listener() -> TestResult {
    let root = tempfile::tempdir()?;
    let config = root.path().join("shutdown.toml");
    private_file(&config, b"endpoint = \"http://127.0.0.1:9\"\nbucket = \"test\"\naccess_key = \"test\"\nsecret_key = \"test\"\n")?;
    let (mut host, _) = serve(&config, root.path()).await?;
    host.stop()?;
    host.stop()?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires local MinIO, Spin 4 and a release WASIp2 component"]
async fn operator_minio_status_and_exact_resume_preserve_both_hashes() -> TestResult {
    for (name, format) in [
        ("sha1", ObjectFormat::Sha1),
        ("sha256", ObjectFormat::Sha256),
    ] {
        lifecycle(name, format).await?;
    }
    Ok(())
}

#[allow(
    clippy::too_many_lines,
    reason = "one both-hash operator-process and cold Spin lifecycle"
)]
async fn lifecycle(name: &str, format: ObjectFormat) -> TestResult {
    let root = tempfile::tempdir()?;
    let prefix = format!("operator-{}", TransactionId::new());
    let mut config = String::new();
    for (key, value) in [
        ("endpoint", env::var("OBJECT_LOG_MINIO_ENDPOINT")?),
        ("bucket", env::var("OBJECT_LOG_MINIO_BUCKET")?),
        ("access_key", env::var("OBJECT_LOG_MINIO_ACCESS_KEY")?),
        ("secret_key", env::var("OBJECT_LOG_MINIO_SECRET_KEY")?),
        ("prefix", prefix.clone()),
        ("object_format", name.into()),
        ("auth_mode", "disabled".into()),
    ] {
        writeln!(config, "{key} = {}", serde_json::to_string(&value)?)?;
    }
    let config_path = root.path().join("repository.toml");
    private_file(&config_path, config.as_bytes())?;
    let store = AmazonS3Builder::new()
        .with_endpoint(env::var("OBJECT_LOG_MINIO_ENDPOINT")?)
        .with_bucket_name(env::var("OBJECT_LOG_MINIO_BUCKET")?)
        .with_access_key_id(env::var("OBJECT_LOG_MINIO_ACCESS_KEY")?)
        .with_secret_access_key(env::var("OBJECT_LOG_MINIO_SECRET_KEY")?)
        .with_region("us-east-1")
        .with_allow_http(true)
        .with_virtual_hosted_style_request(false)
        .build()?;
    let faults = object_log::sim::FaultStore::new(store);
    let backend =
        ValidatedBackend::new(Arc::new(faults.clone()), StorePath::from(prefix.clone())).await?;
    let id = LogId::new("repository")?;
    assert!(
        !operator(&config_path, &["collect", "--resume-only"])?
            .status
            .success()
    );
    assert!(
        !operator(
            &config_path,
            &[
                "migrate-catalog",
                "--recovery-file",
                text(&root.path().join("missing-migration.receipt"))?
            ]
        )?
        .status
        .success()
    );
    assert!(
        !operator(
            &config_path,
            &[
                "compact-packs",
                "--recovery-file",
                text(&root.path().join("missing-compaction.receipt"))?
            ]
        )?
        .status
        .success()
    );
    assert!(!operator(&config_path, &["collect"])?.status.success());
    let missing = operator(&config_path, &["status"])?;
    assert!(!missing.status.success());
    assert!(
        !operator(&config_path, &["checkpoint", "--retain-packs"])?
            .status
            .success()
    );
    assert!(
        !operator(
            &config_path,
            &[
                "set-default-branch",
                "--expected",
                "refs/heads/main",
                "--target",
                "refs/heads/trunk",
                "--recovery-file",
                text(&root.path().join("missing.receipt"))?
            ]
        )?
        .status
        .success()
    );
    assert_eq!(fs::metadata(root.path().join("missing.receipt"))?.len(), 0);
    let absent_token = root.path().join("absent.token");
    private_file(&absent_token, b"no candidate")?;
    assert!(
        !operator(
            &config_path,
            &["resume-commit", "--token-file", text(&absent_token)?]
        )?
        .status
        .success()
    );
    assert!(
        Log::open_existing(&backend, &id, Options::default())
            .await
            .is_err()
    );

    let empty = Log::open(&backend, &id, Options::default()).await?;
    let initial = empty.load().await?;
    let checkpoint = operator(&config_path, &["checkpoint", "--retain-packs"])?;
    assert!(checkpoint.status.success());
    assert_eq!(decode(&checkpoint)?["outcome"], "checkpointed");
    assert!(empty.refresh(&initial).await?.is_none());

    let source = root.path().join("source");
    git(
        None,
        &[
            "init",
            "-q",
            "-b",
            "main",
            &format!("--object-format={name}"),
            text(&source)?,
        ],
    )?;
    fs::write(source.join("file"), "one")?;
    git(Some(&source), &["add", "file"])?;
    git(Some(&source), &["commit", "-q", "-m", "one"])?;
    let (mut host, url) = serve(&config_path, root.path()).await?;
    git(Some(&source), &["push", "-q", &url, "main"])?;
    host.stop()?;
    let status = operator(&config_path, &["status"])?;
    assert!(status.status.success());
    assert_eq!(decode(&status)?["tail_entries"], 1);
    let old = git(Some(&source), &["rev-parse", "HEAD"])?;
    fs::write(source.join("file"), "two")?;
    git(Some(&source), &["commit", "-q", "-am", "two"])?;
    let new = git(Some(&source), &["rev-parse", "HEAD"])?;
    let pack = git(Some(&source), &["pack-objects", "--stdout", "--all"])?;
    let command = format!(
        "{} {} refs/heads/main\0report-status object-format={name}\n",
        String::from_utf8(old)?.trim(),
        String::from_utf8(new.clone())?.trim()
    );
    let mut input = format!("{:04x}{command}0000", command.len() + 4).into_bytes();
    input.extend(pack);
    let log = Log::open_existing(&backend, &id, Options::default()).await?;
    let mut tokens = Vec::new();
    for _ in 0..2 {
        let prepared = Repository::open(&log, format)
            .await?
            .prepare_receive(TransactionId::new(), Bytes::from(input.clone()))
            .await?;
        // A receipt copy must not retain the engine's Bytes allocation owner.
        tokens.push(prepared.recovery_token().to_vec());
        // Drop without publication; a fresh CLI process must use only the token/WAL.
    }
    for (index, token) in tokens.iter().enumerate() {
        private_file(&root.path().join(format!("{index}.token")), token)?;
    }
    let winner = root.path().join("0.token");
    for _ in 0..2 {
        let resumed = operator(
            &config_path,
            &["resume-commit", "--token-file", text(&winner)?],
        )?;
        assert!(resumed.status.success());
        assert_eq!(decode(&resumed)?["outcome"], "committed");
        assert_eq!(fs::read(&winner)?, tokens[0]);
    }
    let loser = operator(
        &config_path,
        &[
            "resume-commit",
            "--token-file",
            text(&root.path().join("1.token"))?,
        ],
    )?;
    assert!(loser.status.success());
    assert_eq!(decode(&loser)?["outcome"], "not_committed");
    assert_eq!(log.load().await?.tail().len(), 2);
    fill_tail(&log, &source, name, format, &new).await?;
    assert_eq!(log.load().await?.tail().len(), 1024);
    let full_view = log.load().await?;
    assert!(matches!(
        Repository::open(&log, format).await,
        Err(object_log_git::Error::ObjectLog(
            object_log::Error::RequestDenied
        ))
    ));
    let (mut blocked, blocked_url) = serve(&config_path, root.path()).await?;
    let rejected = git(None, &["ls-remote", &blocked_url])
        .err()
        .ok_or("full tail unexpectedly served")?;
    blocked.stop()?;
    if !rejected.to_string().contains("HTTP 503") {
        for entry in fs::read_dir(root.path())? {
            let path = entry?.path();
            if path
                .file_name()
                .is_some_and(|name| name.to_string_lossy().starts_with("spin-"))
            {
                eprintln!("{}", fs::read_to_string(path)?);
            }
        }
    }
    assert!(rejected.to_string().contains("HTTP 503"), "{rejected}");
    assert!(log.refresh(&full_view).await?.is_none());
    println!(
        "{name}: shared engine confirms full-tail admission denial; unchanged head produces Spin HTTP 503: {rejected}"
    );
    let full = operator(&config_path, &["status"])?;
    assert!(full.status.success());
    assert_eq!(decode(&full)?["tail_entries"], 1024);
    let before = log.load().await?;
    let wrong_name = if name == "sha1" { "sha256" } else { "sha1" };
    let wrong_config = root.path().join("wrong-format.toml");
    private_file(
        &wrong_config,
        config
            .replace(
                &format!("object_format = \"{name}\""),
                &format!("object_format = \"{wrong_name}\""),
            )
            .as_bytes(),
    )?;
    assert!(
        !operator(&wrong_config, &["checkpoint", "--retain-packs"])?
            .status
            .success()
    );
    assert!(log.refresh(&before).await?.is_none());
    let checkpoint = operator(&config_path, &["checkpoint", "--retain-packs"])?;
    assert!(checkpoint.status.success());
    let checkpoint = decode(&checkpoint)?;
    assert_eq!(checkpoint["outcome"], "checkpointed");
    assert_eq!(checkpoint["tail_entries"], 0);
    assert_eq!(checkpoint["checkpoint_through"], 1023);
    let duplicate = operator(&config_path, &["checkpoint", "--retain-packs"])?;
    assert!(duplicate.status.success());
    assert_eq!(decode(&duplicate)?["generation"], checkpoint["generation"]);
    let (mut cold, url) = serve(&config_path, root.path()).await?;
    let clone = root.path().join("clone");
    git(None, &["clone", "-q", &url, text(&clone)?])?;
    git(Some(&clone), &["fsck", "--strict"])?;
    assert_eq!(git(Some(&clone), &["rev-parse", "HEAD"])?, new);
    assert_eq!(fs::read_to_string(clone.join("file"))?, "two");
    cold.stop()?;
    for cycle in 0..3 {
        let tag = format!("maintenance-cycle-{cycle}");
        git(Some(&source), &["tag", &tag])?;
        let (mut writer, url) = serve(&config_path, root.path()).await?;
        git(
            Some(&source),
            &["push", "-q", &url, &format!("refs/tags/{tag}")],
        )?;
        writer.stop()?;
        let checkpoint = operator(&config_path, &["checkpoint", "--retain-packs"])?;
        assert!(checkpoint.status.success());
        assert_eq!(decode(&checkpoint)?["tail_entries"], 0);
        let (mut reader, url) = serve(&config_path, root.path()).await?;
        git(Some(&clone), &["fetch", "-q", "--tags", &url])?;
        git(Some(&clone), &["fsck", "--strict"])?;
        assert_eq!(git(Some(&clone), &["rev-parse", &tag])?, new);
        reader.stop()?;
    }
    default_branch_lifecycle(&config_path, root.path(), &source, &log, &new).await?;
    let sentinel = StorePath::from(format!("unrelated-{}/blobs/sentinel", TransactionId::new()));
    faults
        .put(
            &sentinel,
            Bytes::from_static(b"unrelated repository").into(),
        )
        .await?;
    let old_blobs = blob_paths(&faults, &prefix).await?;
    assert!(!old_blobs.is_empty());
    let migrated_tip = migration_lifecycle(&config_path, root.path(), &source, &log, &new).await?;
    collection_lifecycle(
        &config_path,
        root.path(),
        &source,
        &log,
        &faults,
        &migrated_tip,
    )
    .await?;
    let remaining = blob_paths(&faults, &prefix).await?;
    assert!(old_blobs.is_disjoint(&remaining));
    assert_eq!(
        faults.get(&sentinel).await?.bytes().await?,
        Bytes::from_static(b"unrelated repository")
    );
    println!(
        "{name}: missing target, exact resume, 1024-tail escape, three maintenance cycles, default main/trunk/master, unborn default, catalog migration, tree push/fetch, compaction/checkpoint, retention-aware fresh collection, old-pack reclamation, interrupted collection and cold push passed"
    );
    Ok(())
}

async fn blob_paths(
    store: &object_log::sim::FaultStore,
    prefix: &str,
) -> TestResult<std::collections::BTreeSet<StorePath>> {
    use futures::TryStreamExt as _;
    use object_store::ObjectStore as _;
    let scope = StorePath::from(prefix);
    Ok(store
        .list(Some(&scope))
        .try_collect::<Vec<_>>()
        .await?
        .into_iter()
        .filter(|entry| entry.location.as_ref().contains("/blobs/"))
        .map(|entry| entry.location)
        .collect())
}

async fn migration_lifecycle(
    config: &Path,
    root: &Path,
    source: &Path,
    log: &Log,
    tip: &[u8],
) -> TestResult<Vec<u8>> {
    let receipt = root.join("catalog.receipt");
    let report = operator(
        config,
        &["migrate-catalog", "--recovery-file", text(&receipt)?],
    )?;
    assert!(report.status.success());
    assert_eq!(decode(&report)?["outcome"], "migrated");
    assert_eq!(fs::metadata(&receipt)?.len(), 0);
    let before = log.load().await?;
    let report = operator(
        config,
        &[
            "migrate-catalog",
            "--recovery-file",
            text(&root.join("catalog-repeat.receipt"))?,
        ],
    )?;
    assert!(report.status.success());
    assert_eq!(decode(&report)?["outcome"], "already_tree");
    assert!(log.refresh(&before).await?.is_none());

    let (mut host, url) = serve(config, root).await?;
    let clone = root.join("clone-migrated");
    git(None, &["clone", "-q", &url, text(&clone)?])?;
    assert_eq!(git(Some(&clone), &["rev-parse", "HEAD"])?, tip);
    assert_eq!(
        git(Some(&clone), &["symbolic-ref", "HEAD"])?,
        b"refs/heads/master\n"
    );
    git(Some(&clone), &["fsck", "--strict"])?;
    fs::write(source.join("file"), "three")?;
    git(Some(source), &["add", "file"])?;
    git(
        Some(source),
        &["commit", "-q", "-m", "after catalog migration"],
    )?;
    let next = git(Some(source), &["rev-parse", "HEAD"])?;
    git(
        Some(source),
        &["push", "-q", &url, "HEAD:refs/heads/master"],
    )?;
    git(Some(&clone), &["fetch", "-q"])?;
    assert_eq!(git(Some(&clone), &["rev-parse", "origin/master"])?, next);
    git(Some(&clone), &["fsck", "--strict"])?;
    let refs = git(None, &["ls-remote", &url])?;
    host.stop()?;
    let receipt = root.join("compaction.receipt");
    let report = operator(
        config,
        &["compact-packs", "--recovery-file", text(&receipt)?],
    )?;
    assert!(report.status.success());
    assert_eq!(decode(&report)?["outcome"], "compacted");
    assert_eq!(fs::metadata(receipt)?.len(), 0);
    let (mut cold, url) = serve(config, root).await?;
    assert_eq!(git(None, &["ls-remote", &url])?, refs);
    let compacted = root.join("clone-compacted");
    git(None, &["clone", "-q", &url, text(&compacted)?])?;
    assert_eq!(git(Some(&compacted), &["rev-parse", "HEAD"])?, next);
    assert_eq!(
        git(Some(&compacted), &["symbolic-ref", "HEAD"])?,
        b"refs/heads/master\n"
    );
    git(Some(&compacted), &["fsck", "--strict"])?;
    assert_eq!(fs::read_to_string(compacted.join("file"))?, "three");
    cold.stop()?;
    let report = operator(config, &["checkpoint", "--retain-packs"])?;
    assert!(report.status.success());
    assert_eq!(decode(&report)?["outcome"], "checkpointed");
    Ok(next)
}

// Fresh planning runs through the CLI; a second library-installed plan tests
// interrupted resumption. Serving processes are drained for deterministic checks.
async fn collection_lifecycle(
    config: &Path,
    root: &Path,
    source: &Path,
    log: &Log,
    faults: &object_log::sim::FaultStore,
    tip: &[u8],
) -> TestResult {
    assert_eq!(
        decode(&operator(config, &["collect", "--resume-only"])?)?["outcome"],
        "no_active_plan"
    );
    let retention = object_log::RetentionId::new();
    let view = log.load().await?;
    assert!(matches!(
        log.retain(&view, retention).await?,
        object_log::RetentionStatus::Applied(_)
    ));
    let retained = log.load().await?;
    let blocked = operator(config, &["collect"])?;
    assert_eq!(blocked.status.code(), Some(3));
    assert_eq!(decode(&blocked)?["outcome"], "retained");
    assert!(log.refresh(&retained).await?.is_none());
    // The test owns this retention and releases only its exact ID.
    assert!(matches!(
        log.release_retention(&retained, retention).await?,
        object_log::RetentionStatus::Applied(_)
    ));
    let collected = operator(config, &["collect"])?;
    assert!(collected.status.success());
    assert_eq!(decode(&collected)?["outcome"], "collected");
    assert!(log.load().await?.collection_plan_bytes().is_none());
    let view = log.load().await?;
    log.put_object(&view, Bytes::from_static(b"unpublished collection fixture"))
        .await?;
    let object_log::CollectionStart::Installed(fenced, _) = log.start_collection(&view).await?
    else {
        return Err("fixture plan not installed".into());
    };
    faults.reset();
    let mut pause = faults.pause_next_delete(object_log::sim::FailurePhase::After);
    let mut interrupted = Box::pin(log.resume_collection(&fenced));
    assert!(
        tokio::select! { entered = pause.wait_until_entered() => entered, _ = &mut interrupted => false }
    );
    drop(interrupted);
    assert!(!pause.release());
    // Fresh executable, no local plan or receipt, after a real provider delete.
    let report = operator(config, &["collect", "--resume-only"])?;
    assert!(report.status.success());
    assert_eq!(decode(&report)?["outcome"], "collected");
    assert!(log.load().await?.collection_plan_bytes().is_none());
    assert_eq!(
        decode(&operator(config, &["collect", "--resume-only"])?)?["outcome"],
        "no_active_plan"
    );
    let (mut reader, url) = serve(config, root).await?;
    let clone = root.join("clone-after-collection");
    git(None, &["clone", "-q", &url, text(&clone)?])?;
    assert_eq!(git(Some(&clone), &["rev-parse", "HEAD"])?, tip);
    git(Some(&clone), &["fsck", "--strict"])?;
    assert_eq!(fs::read_to_string(clone.join("file"))?, "three");
    git(
        Some(source),
        &["push", "-q", &url, "HEAD:refs/tags/after-collection"],
    )?;
    git(Some(&clone), &["fetch", "-q", "--tags"])?;
    assert_eq!(git(Some(&clone), &["rev-parse", "after-collection"])?, tip);
    git(Some(&clone), &["fsck", "--strict"])?;
    reader.stop()?;
    Ok(())
}

async fn default_branch_lifecycle(
    config: &Path,
    root: &Path,
    source: &Path,
    log: &Log,
    tip: &[u8],
) -> TestResult {
    let (mut writer, url) = serve(config, root).await?;
    git(Some(source), &["push", "-q", &url, "HEAD:refs/heads/trunk"])?;
    writer.stop()?;
    let update = operator(
        config,
        &[
            "set-default-branch",
            "--expected",
            "refs/heads/main",
            "--target",
            "refs/heads/trunk",
            "--recovery-file",
            text(&root.join("trunk.receipt"))?,
        ],
    )?;
    assert!(update.status.success());
    assert_eq!(decode(&update)?["outcome"], "updated");
    assert_eq!(fs::metadata(root.join("trunk.receipt"))?.len(), 0);
    let (mut reader, url) = serve(config, root).await?;
    let trunk = root.join("clone-trunk");
    git(None, &["clone", "-q", &url, text(&trunk)?])?;
    assert_eq!(
        git(Some(&trunk), &["symbolic-ref", "HEAD"])?,
        b"refs/heads/trunk\n"
    );
    assert_eq!(git(Some(&trunk), &["rev-parse", "HEAD"])?, tip);
    git(Some(&trunk), &["fsck", "--strict"])?;
    reader.stop()?;

    let before = log.load().await?;
    let stale = operator(
        config,
        &[
            "set-default-branch",
            "--expected",
            "refs/heads/main",
            "--target",
            "refs/heads/master",
            "--recovery-file",
            text(&root.join("stale.receipt"))?,
        ],
    )?;
    assert_eq!(stale.status.code(), Some(3));
    assert_eq!(decode(&stale)?["outcome"], "stale_default");
    assert!(log.refresh(&before).await?.is_none());
    let update = operator(
        config,
        &[
            "set-default-branch",
            "--expected",
            "refs/heads/trunk",
            "--target",
            "refs/heads/master",
            "--recovery-file",
            text(&root.join("master.receipt"))?,
        ],
    )?;
    assert!(update.status.success());
    let (mut reader, url) = serve(config, root).await?;
    let unborn = root.join("clone-unborn");
    git(None, &["clone", "-q", &url, text(&unborn)?])?;
    assert_eq!(
        git(Some(&unborn), &["symbolic-ref", "HEAD"])?,
        b"refs/heads/master\n"
    );
    assert!(git(Some(&unborn), &["rev-parse", "--verify", "HEAD"]).is_err());
    reader.stop()?;

    let (mut writer, url) = serve(config, root).await?;
    git(
        Some(source),
        &["push", "-q", &url, "HEAD:refs/heads/master"],
    )?;
    writer.stop()?;
    assert!(
        operator(config, &["checkpoint", "--retain-packs"])?
            .status
            .success()
    );
    let (mut reader, url) = serve(config, root).await?;
    let master = root.join("clone-master");
    git(None, &["clone", "-q", &url, text(&master)?])?;
    assert_eq!(
        git(Some(&master), &["symbolic-ref", "HEAD"])?,
        b"refs/heads/master\n"
    );
    assert_eq!(git(Some(&master), &["rev-parse", "HEAD"])?, tip);
    git(Some(&master), &["fsck", "--strict"])?;
    assert_eq!(fs::read_to_string(master.join("file"))?, "two");
    reader.stop()?;
    Ok(())
}

// Produce two valid metadata records through the public Git engine, then replay
// their alternating tag create/delete operations as a trusted WAL producer.
// This fills recovery state without claiming 1,024 HTTP pushes or bypassing the
// serving admission ceiling; no private Git codec is copied into this test.
async fn fill_tail(
    log: &Log,
    source: &Path,
    name: &str,
    format: ObjectFormat,
    oid: &[u8],
) -> TestResult {
    let oid = std::str::from_utf8(oid)?.trim();
    let zero = "0".repeat(oid.len());
    let empty_pack = git(Some(source), &["pack-objects", "--stdout"])?;
    let mut records = Vec::new();
    for (old, new) in [(zero.as_str(), oid), (oid, zero.as_str())] {
        let command =
            format!("{old} {new} refs/tags/tail-fixture\0report-status object-format={name}\n");
        let mut input = format!("{:04x}{command}0000", command.len() + 4).into_bytes();
        if new != zero {
            input.extend_from_slice(&empty_pack);
        }
        let prepared = Repository::open(log, format)
            .await?
            .prepare_receive(TransactionId::new(), Bytes::from(input))
            .await?;
        assert!(matches!(
            prepared.publish().await?,
            object_log::CommitStatus::Committed(_)
        ));
        let view = log.load().await?;
        let tail = log.read_tail(&view).await?;
        let record = tail.last().ok_or("missing fixture record")?;
        assert!(record.objects().is_empty());
        records.push(record.operation().clone());
    }
    let mut view = log.load().await?;
    let mut index = 0;
    while view.tail().len() < 1024 {
        let prepared = log.prepare(
            &view,
            TransactionId::new(),
            records[index % 2].clone(),
            Bytes::new(),
            Vec::new(),
        )?;
        let object_log::CommitStatus::Committed(next) = log.commit(prepared).await? else {
            return Err("fixture publication uncertain".into());
        };
        view = next;
        index += 1;
    }
    Ok(())
}

fn private_file(path: &Path, bytes: &[u8]) -> TestResult {
    fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?
        .write_all(bytes)?;
    Ok(())
}

fn decode(output: &Output) -> TestResult<Value> {
    assert!(output.stdout.len() <= 2048);
    assert!(
        output.stderr.is_empty(),
        "operator stderr must not contain provider diagnostics"
    );
    for name in ["OBJECT_LOG_MINIO_ACCESS_KEY", "OBJECT_LOG_MINIO_SECRET_KEY"] {
        assert!(!String::from_utf8_lossy(&output.stdout).contains(&env::var(name)?));
    }
    Ok(serde_json::from_slice(&output.stdout)?)
}

fn operator(config: &Path, args: &[&str]) -> TestResult<Output> {
    let binary = env::var_os("OBJECT_LOG_OPERATOR_BINARY")
        .unwrap_or_else(|| env!("CARGO_BIN_EXE_object-log-git-maintain").into());
    let metrics = config.with_file_name(format!("operator-{}.rss", TransactionId::new()));
    let output = Command::new("/usr/bin/time")
        .arg(if cfg!(target_os = "macos") {
            "-l"
        } else {
            "-v"
        })
        .arg("-o")
        .arg(&metrics)
        .arg(&binary)
        .arg("--config")
        .arg(config)
        .args(args)
        .output()?;
    let report = decode(&output)?;
    println!(
        "Operator {} executable: {}\nReport: {report}\n{}",
        args.first().copied().unwrap_or("unknown"),
        Path::new(&binary).display(),
        fs::read_to_string(metrics)?
    );
    Ok(output)
}

fn text(path: &Path) -> TestResult<&str> {
    path.to_str().ok_or_else(|| "non-UTF8 test path".into())
}

fn git(directory: Option<&Path>, args: &[&str]) -> TestResult<Vec<u8>> {
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
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_AUTHOR_NAME", "Operator test")
        .env("GIT_AUTHOR_EMAIL", "operator@example.invalid")
        .env("GIT_COMMITTER_NAME", "Operator test")
        .env("GIT_COMMITTER_EMAIL", "operator@example.invalid")
        .env("GIT_PROTOCOL", "version=2");
    if let Some(directory) = directory {
        command.current_dir(directory);
    }
    let output = command.output()?;
    if !output.status.success() {
        return Err(format!("git {args:?}: {}", String::from_utf8_lossy(&output.stderr)).into());
    }
    Ok(output.stdout)
}

struct Host(Option<Child>, String);
impl Host {
    fn stop(&mut self) -> TestResult {
        if let Some(child) = &mut self.0 {
            spin_process::stop(child, &self.1, "-TERM")?;
            self.0 = None;
        }
        Ok(())
    }
}
impl Drop for Host {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

async fn serve(config: &Path, state: &Path) -> TestResult<(Host, String)> {
    let port = std::net::TcpListener::bind("127.0.0.1:0")?
        .local_addr()?
        .port();
    let address = format!("127.0.0.1:{port}");
    let log = fs::File::create(state.join(format!("spin-{port}.log")))?;
    let mut host = Host(
        Some(
            Command::new("spin")
                .args(["up", "--from"])
                .arg(Path::new(env!("CARGO_MANIFEST_DIR")).join("spin.toml"))
                .process_group(0)
                .args([
                    "--listen",
                    &address,
                    "--variable",
                    &format!("@{}", config.display()),
                ])
                .arg("--state-dir")
                .arg(state)
                .args(["--follow", "git"])
                .stdout(log.try_clone()?)
                .stderr(log)
                .spawn()?,
        ),
        address.clone(),
    );
    for _ in 0..100 {
        if let Some(child) = &mut host.0
            && child.try_wait()?.is_some()
        {
            return Err("Spin exited before readiness".into());
        }
        if std::net::TcpStream::connect(&address).is_ok() {
            return Ok((host, format!("http://{address}/repo")));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Err("Spin startup timed out".into())
}
