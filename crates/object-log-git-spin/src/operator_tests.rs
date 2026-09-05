use super::*;
use bytes::Bytes;
use object_log::sim::{FailurePhase, FaultStore, Operation};
use object_log::{CheckpointStatus, CommitStatus, TransactionId};
use object_store::memory::InMemory;
use tempfile::TempDir;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

fn private_file(root: &TempDir, name: &str, bytes: &[u8]) -> TestResult<PathBuf> {
    let path = root.path().join(name);
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&path)?
        .write_all(bytes)?;
    Ok(path)
}

fn config(endpoint: &str) -> String {
    format!(
        "endpoint = {endpoint:?}\nbucket = \"test\"\naccess_key = \"PRIVATE_ACCESS\"\nsecret_key = \"PRIVATE_SECRET\"\n"
    )
}

fn json(report: &Report) -> TestResult<serde_json::Value> {
    let mut output = Vec::new();
    report.write(&mut output)?;
    assert!(output.len() <= OUTPUT_BYTES);
    assert!(output.ends_with(b"\n"));
    let text = String::from_utf8(output.clone())?;
    for secret in [
        "PRIVATE_SECRET",
        "PRIVATE_ACCESS",
        "PRIVATE_ARGUMENT",
        "PRIVATE_TOKEN",
    ] {
        assert!(!text.contains(secret));
    }
    Ok(serde_json::from_slice(&output)?)
}

fn arguments(root: &Path, tail: &[&str]) -> Vec<OsString> {
    let mut args = vec![
        "object-log-git-maintain".into(),
        "--config".into(),
        root.into(),
    ];
    args.extend(tail.iter().map(OsString::from));
    args
}

#[test]
fn private_file_limits_check_exact_boundary_and_reject_special_files() -> TestResult {
    let root = TempDir::new()?;
    let path = private_file(&root, "token", &vec![0; TOKEN_BYTES])?;
    assert_eq!(read_file(&path, TOKEN_BYTES)?.len(), TOKEN_BYTES);
    OpenOptions::new()
        .append(true)
        .open(&path)?
        .write_all(b"x")?;
    assert_eq!(
        read_file(&path, TOKEN_BYTES).err().map(|e| e.0),
        Some("input_limit")
    );
    let small = private_file(&root, "small", b"private")?;
    std::fs::set_permissions(&small, std::fs::Permissions::from_mode(0o644))?;
    assert_eq!(
        read_file(&small, TOKEN_BYTES).err().map(|e| e.0),
        Some("input_not_private")
    );
    assert!(read_file(root.path(), TOKEN_BYTES).is_err());
    let symlink = root.path().join("link");
    std::os::unix::fs::symlink(&small, &symlink)?;
    assert!(read_file(&symlink, TOKEN_BYTES).is_err());
    let fifo = root.path().join("fifo");
    assert!(
        std::process::Command::new("mkfifo")
            .arg(&fifo)
            .status()?
            .success()
    );
    assert!(read_file(&fifo, TOKEN_BYTES).is_err());
    Ok(())
}

#[test]
fn config_is_strict_and_invalid_inputs_do_not_contact_a_provider() -> TestResult {
    let root = TempDir::new()?;
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    listener.set_nonblocking(true)?;
    let base = config(&format!("http://{}", listener.local_addr()?));
    for (index, suffix) in [
        "object_format = \"sha512\"",
        "read_only = true",
        "read_only = \"TRUE\"",
        "unknown = \"PRIVATE_SECRET\"",
        "bucket = \"duplicate\"",
        "[nested]\nsecret_key = \"PRIVATE_SECRET\"",
    ]
    .iter()
    .enumerate()
    {
        let file = private_file(
            &root,
            &format!("invalid-{index}"),
            format!("{base}{suffix}\n").as_bytes(),
        )?;
        let report = run(arguments(&file, &["status"]));
        assert_eq!(report.exit(), 2);
        assert_eq!(json(&report)?["outcome"], "invalid_config");
    }
    assert_eq!(
        listener.accept().err().map(|e| e.kind()),
        Some(std::io::ErrorKind::WouldBlock)
    );
    for (index, endpoint) in [
        "ftp://localhost",
        "http://PRIVATE_SECRET@localhost",
        "https://localhost?token=PRIVATE_SECRET",
    ]
    .iter()
    .enumerate()
    {
        let file = private_file(
            &root,
            &format!("endpoint-{index}"),
            config(endpoint).as_bytes(),
        )?;
        assert_eq!(run(arguments(&file, &["status"])).exit(), 2);
    }
    for format in ["sha1", "sha256"] {
        let file = private_file(
            &root,
            format,
            format!("{base}object_format = \"{format}\"\nread_only = \"true\"\n").as_bytes(),
        )?;
        let config = Config::load(&file)?;
        assert_eq!(config.object_format, format);
        assert_eq!(config.log_id, "repository");
    }
    let mut exact = base.into_bytes();
    exact.resize(CONFIG_BYTES, b' ');
    let file = private_file(&root, "config-boundary", &exact)?;
    assert!(Config::load(&file).is_ok());
    OpenOptions::new()
        .append(true)
        .open(&file)?
        .write_all(b" ")?;
    assert!(Config::load(&file).is_err());
    Ok(())
}

#[test]
fn argument_errors_and_help_are_bounded_redacted_json() -> TestResult {
    for args in [
        vec!["operator", "--PRIVATE_ARGUMENT"],
        vec!["operator", "resume-commit"],
        vec!["operator", "--help"],
    ] {
        let report = run(args.into_iter().map(OsString::from));
        assert!(matches!(report.exit(), 0 | 2));
        assert!(json(&report)?["usage"].is_string());
    }
    Ok(())
}

async fn fixture(name: &str, options: Options) -> TestResult<(Log, FaultStore, ValidatedBackend)> {
    let faults = FaultStore::new(InMemory::new());
    let backend =
        ValidatedBackend::new(Arc::new(faults.clone()), StorePath::from("operator-tests")).await?;
    let log = Log::open(&backend, &LogId::new(name)?, options).await?;
    faults.reset();
    Ok((log, faults, backend))
}

fn token(log: &Log, view: &View, value: &'static [u8]) -> TestResult<Vec<u8>> {
    Ok(log
        .prepare(
            view,
            TransactionId::new(),
            Bytes::from_static(value),
            Bytes::new(),
            Vec::new(),
        )?
        .recovery_token()?
        .to_vec())
}

#[tokio::test]
async fn status_survives_a_full_tail_without_git_admission() -> TestResult {
    let (log, _, _) = fixture("tail", Options::default()).await?;
    let mut view = log.load().await?;
    for _ in 0..Options::default().max_tail_entries {
        let prepared = log.prepare(
            &view,
            TransactionId::new(),
            Bytes::new(),
            Bytes::new(),
            Vec::new(),
        )?;
        let CommitStatus::Committed(next) = log.commit(prepared).await? else {
            return Err("commit pending".into());
        };
        view = next;
    }
    assert!(
        log.prepare(
            &view,
            TransactionId::new(),
            Bytes::new(),
            Bytes::new(),
            Vec::new()
        )
        .is_err()
    );
    let report = execute(&log, &Action::Status).await;
    assert_eq!(report.exit(), 0);
    assert_eq!(json(&report)?["tail_entries"], 1024);
    Ok(())
}

#[tokio::test]
async fn resume_exact_token_is_idempotent_and_losers_do_not_rebase() -> TestResult {
    let (log, _, backend) = fixture("resume", Options::default()).await?;
    let view = log.load().await?;
    let winning = Action::Resume(token(&log, &view, b"PRIVATE_TOKEN")?);
    let losing = Action::Resume(token(&log, &view, b"loser")?);
    assert_eq!(
        json(&execute(&log, &winning).await)?["outcome"],
        "committed"
    );
    let reopened = Log::open_existing(&backend, &LogId::new("resume")?, Options::default()).await?;
    assert_eq!(
        json(&execute(&reopened, &winning).await)?["outcome"],
        "committed"
    );
    assert_eq!(
        json(&execute(&reopened, &losing).await)?["outcome"],
        "not_committed"
    );
    assert_eq!(reopened.load().await?.tail().len(), 1);
    let other = Log::open(&backend, &LogId::new("other")?, Options::default()).await?;
    assert_eq!(execute(&other, &winning).await.exit(), 5);
    assert!(other.load().await?.tail().is_empty());
    Ok(())
}

#[tokio::test]
async fn pending_and_expired_are_not_reported_as_rejections() -> TestResult {
    let (log, faults, _) = fixture(
        "outcomes",
        Options {
            resolution_window: 1,
            ..Options::default()
        },
    )
    .await?;
    let pending = Action::Resume(token(&log, &log.load().await?, b"first")?);
    faults.fail_next(Operation::Get, FailurePhase::Before);
    let report = execute(&log, &pending).await;
    assert_eq!(report.exit(), 4);
    assert_eq!(json(&report)?["outcome"], "pending");
    assert_eq!(execute(&log, &pending).await.exit(), 0);
    let second = Action::Resume(token(&log, &log.load().await?, b"second")?);
    assert_eq!(execute(&log, &second).await.exit(), 0);
    let view = log.load().await?;
    let through = view.tail().last().ok_or("missing commit")?;
    assert!(matches!(
        log.publish_checkpoint(&view, through, Bytes::new(), Vec::new())
            .await?,
        CheckpointStatus::Published(_)
    ));
    let report = execute(&log, &pending).await;
    assert_eq!(report.exit(), 4);
    assert_eq!(json(&report)?["outcome"], "expired");
    assert!(log.load().await?.tail().is_empty());
    Ok(())
}

#[tokio::test]
async fn deadline_after_head_cas_preserves_uncertainty_and_same_token_recovery() -> TestResult {
    let (log, faults, backend) = fixture("deadline", Options::default()).await?;
    let action = Action::Resume(token(&log, &log.load().await?, b"PRIVATE_TOKEN")?);
    faults.reset();
    let mut pause = faults.pause_put_at(2, FailurePhase::After);
    let work = bounded(
        action.name(),
        Duration::from_millis(50),
        execute(&log, &action),
    );
    tokio::pin!(work);
    assert!(
        tokio::select! { entered = pause.wait_until_entered() => entered, _ = &mut work => false }
    );
    let report = work.await;
    assert_eq!(report.exit(), 4);
    assert_eq!(json(&report)?["outcome"], "pending");
    assert!(!pause.release());
    let reopened =
        Log::open_existing(&backend, &LogId::new("deadline")?, Options::default()).await?;
    assert_eq!(
        json(&execute(&reopened, &action).await)?["outcome"],
        "committed"
    );
    assert_eq!(reopened.load().await?.tail().len(), 1);
    Ok(())
}

#[tokio::test]
async fn invalid_token_and_huge_nested_length_never_mutate_the_head() -> TestResult {
    let (log, faults, _) = fixture("invalid", Options::default()).await?;
    let view = log.load().await?;
    // Valid outer envelope/digest, but the head byte string claims u64::MAX.
    // This reaches nested decoding, rather than merely testing a bad checksum.
    let inner = [
        0xa1, 2, 0x5b, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
    ];
    let mut encoded = vec![0xa2, 1, 0x4b];
    encoded.extend(inner);
    encoded.extend([2, 0x58, 32]);
    encoded.extend(object_log::Digest::of(&inner).as_bytes());
    for bytes in [b"PRIVATE_TOKEN".to_vec(), encoded] {
        faults.reset();
        assert_eq!(execute(&log, &Action::Resume(bytes)).await.exit(), 5);
        assert_eq!(faults.metrics().operation(Operation::Put).requests, 0);
        assert_eq!(faults.metrics().operation(Operation::Get).requests, 0);
        assert!(log.refresh(&view).await?.is_none());
    }
    Ok(())
}

#[test]
fn output_errors_and_provider_errors_do_not_leak_secrets() -> TestResult {
    let error = object_log::Error::Store(object_store::Error::Generic {
        store: "PRIVATE_ACCESS",
        source: std::io::Error::other("PRIVATE_SECRET").into(),
    });
    let report = Report::failed("status", classify(&error));
    assert_eq!(json(&report)?["outcome"], "backend_unavailable");
    assert!(
        report
            .write(std::io::Cursor::new(&mut [0_u8; 1][..]))
            .is_err()
    );
    Ok(())
}
