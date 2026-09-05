#[derive(Debug)]
struct CallerGuard {
    calls: std::sync::atomic::AtomicUsize,
    limit: usize,
}
impl CallerGuard {
    fn new(limit: usize) -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self {calls: std::sync::atomic::AtomicUsize::new(0), limit})
    }
    fn calls(&self) -> usize { self.calls.load(std::sync::atomic::Ordering::Relaxed) }
}
impl object_log::RequestGuard for CallerGuard {
    fn before_request(&self, _: object_log::Request) -> Result<(), object_log::RequestDenied> {
        self.calls.fetch_update(std::sync::atomic::Ordering::Relaxed, std::sync::atomic::Ordering::Relaxed,
            |calls| calls.checked_add(1).filter(|next| *next <= self.limit))
            .map(|_| ()).map_err(|_| object_log::RequestDenied)
    }
}

#[tokio::test]
async fn repository_preserves_callers_request_denial() -> TestResult {
    let (log, faults, _) = test_log("caller-denied").await?;
    let caller = CallerGuard::new(0);
    faults.reset();
    assert!(matches!(common_open(&log.with_request_guard(caller.clone()), ObjectFormat::Sha1).await,
        Err(Error::ObjectLog(object_log::Error::RequestDenied))));
    assert_eq!(caller.calls(), 0);
    assert_eq!(faults.metrics().total_requests(), 0);
    Ok(())
}

fn receive_input(format: ObjectFormat, updates: &[RefUpdate], pack: &[u8], report: bool) -> Bytes {
    let mut bytes = Vec::new();
    for (position, update) in updates.iter().enumerate() {
        let zero = "0".repeat(format.digest_len() * 2);
        let mut line = format!(
            "{} {} {}",
            update
                .expected
                .map_or_else(|| zero.clone(), |id| id.to_string()),
            update.target.map_or(zero, |id| id.to_string()),
            String::from_utf8_lossy(&update.name)
        );
        if position == 0 {
            line.push('\0');
            if report {
                line.push_str("report-status ");
            }
            line.push_str(match format {
                ObjectFormat::Sha1 => "object-format=sha1",
                ObjectFormat::Sha256 => "object-format=sha256",
            });
            line.push_str(" atomic");
        }
        bytes.extend_from_slice(format!("{:04x}", line.len() + 4).as_bytes());
        bytes.extend_from_slice(line.as_bytes());
    }
    bytes.extend_from_slice(b"0000");
    bytes.extend_from_slice(pack);
    Bytes::from(bytes)
}

async fn common_open(log: &Log, format: ObjectFormat) -> Result<Repository, Error> {
    Repository::open_with_pool(log, format, &Pool::new(crate::pack::budget::LIVE_BYTES)).await
}

// Rebuild a fresh Git receiver solely from the shared engine's durable view.
// This replaces the old cache-based oracle check and still validates every
// reachable object's integrity and connectivity using the unchanged Git client.
async fn cold_checked(log: &Log, format: ObjectFormat) -> TestResult<Repository> {
    let repository = common_open(log, format).await?;
    let wants: Vec<_> = repository.refs().values().copied().collect();
    if wants.is_empty() {
        return Ok(repository);
    }
    let pack = repository.fetch_pack(&wants, &[], false).await?;
    let receiver = tempfile::tempdir()?;
    command(
        Some(receiver.path()),
        &[
            "init",
            "--bare",
            "--quiet",
            &format!(
                "--object-format={}",
                match format {
                    ObjectFormat::Sha1 => "sha1",
                    ObjectFormat::Sha256 => "sha256",
                }
            ),
        ],
    )?;
    let file = receiver.path().join("received.pack");
    fs::write(&file, &pack)?;
    let result = Command::new("git")
        .current_dir(receiver.path())
        .args([
            "index-pack",
            "--stdin",
            "--strict",
            "--check-self-contained-and-connected",
        ])
        .stdin(fs::File::open(file)?)
        .output()?;
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    for (name, target) in repository.refs() {
        command(
            Some(receiver.path()),
            &[
                "update-ref",
                std::str::from_utf8(name)?,
                &target.to_string(),
            ],
        )?;
    }
    command(Some(receiver.path()), &["fsck", "--strict", "--no-reflogs"])?;
    Ok(repository)
}

fn empty_pack(format: ObjectFormat) -> TestResult<Vec<u8>> {
    let directory = tempfile::tempdir()?;
    command(
        Some(directory.path()),
        &[
            "init",
            "--bare",
            &format!(
                "--object-format={}",
                match format {
                    ObjectFormat::Sha1 => "sha1",
                    ObjectFormat::Sha256 => "sha256",
                }
            ),
        ],
    )?;
    let process = Command::new("git")
        .current_dir(directory.path())
        .args(["pack-objects", "--stdout"])
        .stdin(std::process::Stdio::null())
        .output()?;
    assert!(process.status.success());
    Ok(process.stdout)
}

#[tokio::test]
async fn common_receive_publish_recovery_ref_only_and_delete() -> TestResult {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let fixture = fixture(format, b"receive")?;
        let (log, faults, _) = test_log("common-receive").await?;
        let update = RefUpdate::new("refs/heads/main", None, Some(fixture.target))?;
        let repository = common_open(&log, format).await?;
        let operation = repository.operation.clone();
        let push = repository
            .prepare_receive(
                TransactionId::new(),
                receive_input(format, &[update], &fs::read(&fixture.pack)?, true),
            )
            .await?;
        assert!(operation.live_bytes() > 0);
        let token = push.recovery_token().clone();
        let (resolution, response) = push.publish_receive().await?;
        assert!(matches!(resolution, object_log::Resolution::Committed(_)));
        assert!(String::from_utf8_lossy(&response).contains("ok refs/heads/main"));
        assert!(matches!(
            log.resume(&token).await?,
            object_log::Resolution::Committed(_)
        ));
        drop(token);
        drop(response);
        drop(resolution);
        assert_eq!(operation.live_bytes(), 0);
        let advertisement = common_open(&log, format)
            .await?
            .receive_advertisement()
            .await?;
        assert!(String::from_utf8_lossy(&advertisement).contains(&fixture.target.to_string()));
        drop(advertisement);
        for name in ["refs/tags/one", "refs/tags/two"] {
            let input = receive_input(
                format,
                &[RefUpdate::new(name, None, Some(fixture.target))?],
                &empty_pack(format)?,
                false,
            );
            let repository = common_open(&log, format).await?;
            faults.reset();
            let push = repository
                .prepare_receive(TransactionId::new(), input)
                .await?;
            assert_eq!(pack_puts(&faults), 0, "empty pack must not be staged");
            let (resolution, response) = push.publish_receive().await?;
            assert!(matches!(resolution, object_log::Resolution::Committed(_)));
            assert!(response.is_empty());
        }
        let update = RefUpdate::new("refs/heads/main", Some(fixture.target), None)?;
        let repository = common_open(&log, format).await?;
        faults.reset();
        let push = repository
            .prepare_receive(
                TransactionId::new(),
                receive_input(format, &[update], &[], true),
            )
            .await?;
        assert_eq!(faults.metrics().operation(Operation::Get).requests, 0);
        assert_eq!(pack_puts(&faults), 0);
        assert!(matches!(
            push.publish_receive().await?.0,
            object_log::Resolution::Committed(_)
        ));
        let recovered = cold_checked(&log, format).await?;
        assert!(!recovered.refs().contains_key(b"refs/heads/main".as_slice()));
        assert_eq!(recovered.refs().len(), 2);
    }
    Ok(())
}

#[tokio::test]
async fn common_receive_rejects_stale_atomic_and_corrupt_pack() -> TestResult {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let fixture = fixture(format, b"existing")?;
        let (log, faults, _) = test_log("common-receive-rejections").await?;
        publish_durable_pack(&log, &fixture, format).await?;
        let before = log.load().await?;
        let updates = [
            RefUpdate::new("refs/tags/new", None, Some(fixture.target))?,
            RefUpdate::new("refs/heads/main", None, Some(fixture.target))?,
        ];
        let repository = common_open(&log, format).await?;
        faults.reset();
        let Error::ReceiveRejected { response, source } = repository
            .prepare_receive(
                TransactionId::new(),
                receive_input(format, &updates, &empty_pack(format)?, true),
            )
            .await
            .err()
            .ok_or("stale atomic receive accepted")?
        else {
            return Err("expected receive rejection".into());
        };
        assert!(matches!(*source, Error::StaleReference));
        assert_eq!(
            String::from_utf8_lossy(&response)
                .matches("ng refs/")
                .count(),
            2
        );
        assert_eq!(faults.metrics().operation(Operation::Put).requests, 0);
        drop(response);
        let update = RefUpdate::new("refs/tags/new", None, Some(fixture.target))?;
        let mut pack = fs::read(&fixture.pack)?;
        let last = pack.len() - 1;
        pack[last] ^= 1;
        let error = common_open(&log, format)
            .await?
            .prepare_receive(
                TransactionId::new(),
                receive_input(format, std::slice::from_ref(&update), &pack, true),
            )
            .await
            .err()
            .ok_or("corrupt repeated pack accepted")?;
        assert!(matches!(error, Error::ReceiveRejected { .. }));
        assert_eq!(log.load().await?.tail(), before.tail());
        assert!(
            !common_open(&log, format)
                .await?
                .refs()
                .contains_key(b"refs/tags/new".as_slice())
        );
    }
    Ok(())
}

#[tokio::test]
async fn common_receive_reuses_retained_pack_for_new_refs_and_restored_history() -> TestResult {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let fixture = fixture(format, b"retained history")?;
        let (log, faults, _) = test_log("common-receive-retained-pack").await?;
        publish_durable_pack(&log, &fixture, format).await?;
        let pack = fs::read(&fixture.pack)?;
        let initial_packs = common_open(&log, format).await?.state.packs;
        for name in ["refs/tags/new", "refs/heads/main"] {
            let repository = common_open(&log, format).await?;
            // The second receive restores an entirely unadvertised history.
            if name == "refs/heads/main" {
                assert!(repository.refs().is_empty());
            }
            faults.reset();
            let prepared = repository
                .prepare_receive(
                    TransactionId::new(),
                    receive_input(
                        format,
                        &[RefUpdate::new(name, None, Some(fixture.target))?],
                        &pack,
                        true,
                    ),
                )
                .await?;
            assert_eq!(pack_puts(&faults), 0, "retained pack must not be restaged");
            let (resolution, response) = prepared.publish_receive().await?;
            assert!(matches!(resolution, object_log::Resolution::Committed(_)));
            assert!(String::from_utf8_lossy(&response).contains(&format!("ok {name}")));
            drop(response);
            let recovered = cold_checked(&log, format).await?;
            assert_eq!(recovered.refs().get(name.as_bytes()), Some(&fixture.target));
            assert_eq!(recovered.state.packs.len(), initial_packs.len());
            for (id, (bytes, root)) in &initial_packs {
                let (retained_bytes, retained_root) = &recovered.state.packs[id];
                assert_eq!(retained_bytes, bytes);
                assert_eq!(retained_root.reference(), root.reference());
            }
            if name == "refs/tags/new" {
                let updates = recovered
                    .refs()
                    .iter()
                    .map(|(name, &id)| RefUpdate::new(name, Some(id), None))
                    .collect::<Result<Vec<_>, _>>()?;
                let prepared = recovered
                    .prepare_receive(
                        TransactionId::new(),
                        receive_input(format, &updates, &[], true),
                    )
                    .await?;
                assert!(matches!(
                    prepared.publish_receive().await?.0,
                    object_log::Resolution::Committed(_)
                ));
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn common_receive_conflict_and_lost_response_keep_exact_candidate() -> TestResult {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let fixture = fixture(format, b"race")?;
        let (log, faults, _) = test_log("common-receive-race").await?;
        let input = receive_input(
            format,
            &[RefUpdate::new(
                "refs/heads/main",
                None,
                Some(fixture.target),
            )?],
            &fs::read(&fixture.pack)?,
            true,
        );
        let first = common_open(&log, format)
            .await?
            .prepare_receive(TransactionId::new(), input.clone())
            .await?;
        let second = common_open(&log, format)
            .await?
            .prepare_receive(TransactionId::new(), input)
            .await?;
        let token = first.recovery_token().clone();
        faults.reset();
        faults.schedule(Failure {
            operation: Operation::Put,
            occurrence: 2,
            phase: FailurePhase::After,
        });
        let (resolution, response) = first.publish_receive().await?;
        assert!(matches!(resolution, object_log::Resolution::Committed(_)));
        assert!(String::from_utf8_lossy(&response).contains("ok refs/heads/main"));
        assert_eq!(pack_puts(&faults), 0);
        let (resolution, response) = second.publish_receive().await?;
        assert!(matches!(
            resolution,
            object_log::Resolution::NotCommitted(_)
        ));
        assert!(
            String::from_utf8_lossy(&response).contains("ng refs/heads/main atomic ref conflict")
        );
        assert!(matches!(
            log.resume(&token).await?,
            object_log::Resolution::Committed(_)
        ));
    }
    Ok(())
}

#[tokio::test]
async fn common_receive_pending_never_reports_success() -> TestResult {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        for phase in [FailurePhase::Before, FailurePhase::After] {
            let fixture = fixture(format, b"pending")?;
            let (log, faults, backend) = test_log("common-receive-pending").await?;
            let input = receive_input(
                format,
                &[RefUpdate::new(
                    "refs/heads/main",
                    None,
                    Some(fixture.target),
                )?],
                &fs::read(&fixture.pack)?,
                true,
            );
            let push = common_open(&log, format)
                .await?
                .prepare_receive(TransactionId::new(), input)
                .await?;
            let token = push.recovery_token().clone();
            faults.reset();
            faults.schedule(Failure {
                operation: Operation::Put,
                occurrence: 2,
                phase,
            });
            faults.schedule(Failure {
                operation: Operation::Get,
                occurrence: 1,
                phase: FailurePhase::Before,
            });
            let (resolution, response) = push.publish_receive().await?;
            assert!(matches!(
                resolution,
                object_log::Resolution::StillPending(_)
            ));
            assert!(
                String::from_utf8_lossy(&response)
                    .contains("ng refs/heads/main publication pending")
            );
            assert!(!String::from_utf8_lossy(&response).contains("ok refs/heads/main"));
            drop(response);
            drop(resolution);
            drop(log);
            let reopened = Log::open(
                &backend,
                &LogId::new("common-receive-pending")?,
                Options::default(),
            )
            .await?;
            assert!(matches!(
                reopened.resume(&token).await?,
                object_log::Resolution::Committed(_)
            ));
            assert_eq!(
                common_open(&reopened, format)
                    .await?
                    .refs()
                    .get(b"refs/heads/main".as_slice()),
                Some(&fixture.target)
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn common_receive_fast_forward_branch_kind_and_tag_updates() -> TestResult {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let fixture = fixture(format, b"before")?;
        let (log, _, _) = test_log("common-receive-fastforward").await?;
        publish_durable_pack(&log, &fixture, format).await?;
        let source = fixture.directory.path().join("source");
        fs::write(source.join("file"), b"after")?;
        command(Some(&source), &["commit", "--quiet", "-am", "after"])?;
        let new = ObjectId::parse(
            format,
            output(Some(&source), &["rev-parse", "HEAD"])?.trim(),
        )?;
        let pack = command_output(Some(&source), &["pack-objects", "--all", "--stdout"])?.stdout;
        let input = receive_input(
            format,
            &[RefUpdate::new(
                "refs/heads/main",
                Some(fixture.target),
                Some(new),
            )?],
            &pack,
            true,
        );
        let push = common_open(&log, format)
            .await?
            .prepare_receive(TransactionId::new(), input)
            .await?;
        assert!(matches!(
            push.publish_receive().await?.0,
            object_log::Resolution::Committed(_)
        ));
        let input = receive_input(
            format,
            &[RefUpdate::new(
                "refs/heads/main",
                Some(new),
                Some(fixture.target),
            )?],
            &empty_pack(format)?,
            true,
        );
        assert!(
            matches!(common_open(&log, format).await?.prepare_receive(TransactionId::new(), input).await, Err(Error::ReceiveRejected { source, .. }) if matches!(*source, Error::NonFastForward))
        );
        let blob = ObjectId::parse(
            format,
            output(Some(&source), &["rev-parse", "HEAD:file"])?.trim(),
        )?;
        let input = receive_input(
            format,
            &[RefUpdate::new("refs/heads/blob", None, Some(blob))?],
            &empty_pack(format)?,
            true,
        );
        assert!(
            matches!(common_open(&log, format).await?.prepare_receive(TransactionId::new(), input).await, Err(Error::ReceiveRejected { source, .. }) if matches!(*source, Error::InvalidReference))
        );
        for (old, target) in [(None, blob), (Some(blob), fixture.target)] {
            let input = receive_input(
                format,
                &[RefUpdate::new("refs/tags/movable", old, Some(target))?],
                &empty_pack(format)?,
                true,
            );
            let push = common_open(&log, format)
                .await?
                .prepare_receive(TransactionId::new(), input)
                .await?;
            assert!(matches!(
                push.publish_receive().await?.0,
                object_log::Resolution::Committed(_)
            ));
        }
    }
    Ok(())
}

#[tokio::test]
async fn common_receive_allow_rewrite_keeps_leases_cas_and_atomic_branch_validation() -> TestResult
{
    use crate::ReceivePolicy::AllowNonFastForward;

    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let fixture = fixture(format, b"before rewrite")?;
        let (log, _, _) = test_log("common-receive-rewrite").await?;
        publish_durable_pack(&log, &fixture, format).await?;
        let source = fixture.directory.path().join("source");
        fs::write(source.join("file"), b"after")?;
        command(Some(&source), &["commit", "--quiet", "-am", "after"])?;
        let new = ObjectId::parse(
            format,
            output(Some(&source), &["rev-parse", "HEAD"])?.trim(),
        )?;
        let pack = command_output(Some(&source), &["pack-objects", "--all", "--stdout"])?.stdout;
        let input = receive_input(
            format,
            &[RefUpdate::new(
                "refs/heads/main",
                Some(fixture.target),
                Some(new),
            )?],
            &pack,
            true,
        );
        assert!(matches!(
            common_open(&log, format)
                .await?
                .prepare_receive(TransactionId::new(), input)
                .await?
                .publish_receive()
                .await?
                .0,
            object_log::Resolution::Committed(_)
        ));
        let rewind = receive_input(
            format,
            &[RefUpdate::new(
                "refs/heads/main",
                Some(new),
                Some(fixture.target),
            )?],
            &empty_pack(format)?,
            true,
        );
        let first = common_open(&log, format)
            .await?
            .prepare_receive_with_policy(TransactionId::new(), rewind.clone(), AllowNonFastForward)
            .await?;
        let second = common_open(&log, format)
            .await?
            .prepare_receive_with_policy(TransactionId::new(), rewind.clone(), AllowNonFastForward)
            .await?;
        let (resolution, response) = first.publish_receive().await?;
        assert!(matches!(resolution, object_log::Resolution::Committed(_)));
        assert!(String::from_utf8_lossy(&response).contains("ok refs/heads/main"));
        drop(response);
        let (resolution, response) = second.publish_receive().await?;
        assert!(matches!(
            resolution,
            object_log::Resolution::NotCommitted(_)
        ));
        assert!(
            String::from_utf8_lossy(&response).contains("ng refs/heads/main atomic ref conflict")
        );
        drop(response);
        assert!(
            matches!(common_open(&log, format).await?.prepare_receive_with_policy(TransactionId::new(), rewind, AllowNonFastForward).await, Err(Error::ReceiveRejected { source, .. }) if matches!(*source, Error::StaleReference))
        );
        let blob = ObjectId::parse(
            format,
            output(Some(&source), &["rev-parse", "HEAD:file"])?.trim(),
        )?;
        assert_atomic_rewrite_rejected(&log, format, fixture.target, blob).await?;
        let recovered = cold_checked(&log, format).await?;
        assert_eq!(recovered.refs().len(), 1);
        assert_eq!(
            recovered.refs().get(b"refs/heads/main".as_slice()),
            Some(&fixture.target)
        );
    }
    Ok(())
}

async fn assert_atomic_rewrite_rejected(
    log: &Log,
    format: ObjectFormat,
    target: ObjectId,
    blob: ObjectId,
) -> TestResult {
    use crate::ReceivePolicy::AllowNonFastForward;
    let input = receive_input(
        format,
        &[
            RefUpdate::new("refs/tags/atomic", None, Some(target))?,
            RefUpdate::new("refs/heads/main", Some(target), Some(blob))?,
        ],
        &empty_pack(format)?,
        true,
    );
    let Error::ReceiveRejected { response, source } = common_open(log, format)
        .await?
        .prepare_receive_with_policy(TransactionId::new(), input, AllowNonFastForward)
        .await
        .err()
        .ok_or("invalid atomic rewrite accepted")?
    else {
        return Err("expected receive rejection".into());
    };
    assert!(matches!(*source, Error::InvalidReference));
    assert_eq!(
        String::from_utf8_lossy(&response)
            .matches("ng refs/")
            .count(),
        2
    );
    drop(response);
    Ok(())
}

#[tokio::test]
async fn common_receive_checks_every_leaf_and_rejects_missing_objects() -> TestResult {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let fixture = fixture(format, b"leaf")?;
        let source = fixture.directory.path().join("source");
        let raw = source.join("raw-object");
        // A tree entry declares blob, but its actual object is another tree.
        let tree = ObjectId::parse(
            format,
            output(Some(&source), &["rev-parse", "HEAD^{tree}"])?.trim(),
        )?;
        let mut contents = b"100644 wrong\0".to_vec();
        contents.extend_from_slice(tree.as_bytes());
        fs::write(&raw, contents)?;
        let badtree = output(
            Some(&source),
            &[
                "hash-object",
                "--literally",
                "-w",
                "-t",
                "tree",
                "raw-object",
            ],
        )?;
        fs::write(
            &raw,
            format!(
                "tree {}\nauthor A <a@example.com> 0 +0000\ncommitter A <a@example.com> 0 +0000\n\nbad\n",
                badtree.trim()
            ),
        )?;
        let badcommit = ObjectId::parse(
            format,
            output(
                Some(&source),
                &[
                    "hash-object",
                    "--literally",
                    "-w",
                    "-t",
                    "commit",
                    "raw-object",
                ],
            )?
            .trim(),
        )?;
        fs::write(
            &raw,
            format!("{}\n{}\n{}\n", badcommit, badtree.trim(), tree),
        )?;
        let packed = Command::new("git")
            .current_dir(&source)
            .args(["pack-objects", "--stdout"])
            .stdin(fs::File::open(&raw)?)
            .output()?;
        assert!(packed.status.success());
        let (log, _, _) = test_log("common-receive-invalid-leaf").await?;
        let input = receive_input(
            format,
            &[RefUpdate::new("refs/heads/main", None, Some(badcommit))?],
            &packed.stdout,
            true,
        );
        for policy in [
            crate::ReceivePolicy::FastForwardOnly,
            crate::ReceivePolicy::AllowNonFastForward,
        ] {
            assert!(matches!(
                common_open(&log, format)
                    .await?
                    .prepare_receive_with_policy(TransactionId::new(), input.clone(), policy)
                    .await,
                Err(Error::ReceiveRejected { .. })
            ));
        }
        assert!(log.load().await?.tail().is_empty());
        let input = receive_input(
            format,
            &[RefUpdate::new(
                "refs/heads/main",
                None,
                Some(fixture.target),
            )?],
            &empty_pack(format)?,
            true,
        );
        for policy in [
            crate::ReceivePolicy::FastForwardOnly,
            crate::ReceivePolicy::AllowNonFastForward,
        ] {
            assert!(matches!(
                common_open(&log, format)
                    .await?
                    .prepare_receive_with_policy(TransactionId::new(), input.clone(), policy)
                    .await,
                Err(Error::ReceiveRejected { .. })
            ));
        }
        assert!(log.load().await?.tail().is_empty());
    }
    Ok(())
}

#[tokio::test]
async fn common_checkpoint_preserves_live_pack_and_collects_dead_pack() -> TestResult {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let dead = fixture(format, b"dead")?;
        let fixture = fixture(format, b"live")?;
        let (log, _, _) = test_log("common-checkpoint").await?;
        let descriptor = publish_durable_pack(&log, &fixture, format).await?;
        let input = receive_input(
            format,
            &[RefUpdate::new("refs/tags/dead", None, Some(dead.target))?],
            &fs::read(&dead.pack)?,
            true,
        );
        let push = common_open(&log, format)
            .await?
            .prepare_receive(TransactionId::new(), input)
            .await?;
        assert!(matches!(
            push.publish_receive().await?.0,
            object_log::Resolution::Committed(_)
        ));
        let input = receive_input(
            format,
            &[RefUpdate::new("refs/tags/dead", Some(dead.target), None)?],
            &[],
            true,
        );
        let push = common_open(&log, format)
            .await?
            .prepare_receive(TransactionId::new(), input)
            .await?;
        assert!(matches!(
            push.publish_receive().await?.0,
            object_log::Resolution::Committed(_)
        ));
        let object_log::CheckpointStatus::Published(view) =
            common_open(&log, format).await?.checkpoint().await?
        else {
            return Err("checkpoint not published".into());
        };
        assert!(view.tail().is_empty());
        let repository = common_open(&log, format).await?;
        assert_eq!(repository.state.packs.len(), 1);
        assert!(repository.state.packs.contains_key(&descriptor.id));
        drop(repository);
        assert!(matches!(
            log.start_collection(&view).await?,
            object_log::CollectionStart::Installed(..)
        ));
        let fenced = log.load().await?;
        assert!(matches!(
            log.resume_collection(&fenced).await?,
            object_log::CollectionFinish::Complete(..)
        ));
        let repository = cold_checked(&log, format).await?;
        assert_eq!(
            repository.refs().get(b"refs/heads/main".as_slice()),
            Some(&fixture.target)
        );
    }
    Ok(())
}

#[tokio::test]
async fn common_receive_expired_view_retry_keeps_cumulative_operation() -> TestResult {
    for policy in [
        crate::ReceivePolicy::FastForwardOnly,
        crate::ReceivePolicy::AllowNonFastForward,
    ] {
        receive_expired_view(policy).await?;
    }
    Ok(())
}

async fn receive_expired_view(policy: crate::ReceivePolicy) -> TestResult {
    receive_expired_view_kind(policy, false).await
}

async fn receive_expired_view_kind(policy: crate::ReceivePolicy, streaming: bool) -> TestResult {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let dead = fixture(format, b"old collectible")?;
        let fixture = fixture(format, b"retained")?;
        let (log, _, _) = test_log("common-receive-expiry").await?;
        publish_durable_pack(&log, &fixture, format).await?;
        let input = receive_input(
            format,
            &[RefUpdate::new("refs/tags/dead", None, Some(dead.target))?],
            &fs::read(&dead.pack)?,
            true,
        );
        let push = common_open(&log, format)
            .await?
            .prepare_receive(TransactionId::new(), input)
            .await?;
        assert!(matches!(
            push.publish_receive().await?.0,
            object_log::Resolution::Committed(_)
        ));
        let old = common_open(&log, format).await?;
        let operation = old.operation.clone();
        let input = receive_input(
            format,
            &[RefUpdate::new("refs/tags/dead", Some(dead.target), None)?],
            &[],
            true,
        );
        let push = common_open(&log, format)
            .await?
            .prepare_receive(TransactionId::new(), input)
            .await?;
        assert!(matches!(
            push.publish_receive().await?.0,
            object_log::Resolution::Committed(_)
        ));
        let object_log::CheckpointStatus::Published(view) =
            common_open(&log, format).await?.checkpoint().await?
        else {
            return Err("checkpoint not published".into());
        };
        let object_log::CollectionStart::Installed(fenced, _) = log.start_collection(&view).await?
        else {
            return Err("no GC fence".into());
        };
        assert!(fenced.collection_plan_bytes().is_some());
        assert!(matches!(
            log.resume_collection(&fenced).await?,
            object_log::CollectionFinish::Complete(..)
        ));
        let before = operation.calls();
        let input = if policy == crate::ReceivePolicy::AllowNonFastForward {
            // Rewritten history uses a previously collected pack; retry must
            // preserve permission as well as the cumulative operation budget.
            receive_input(
                format,
                &[RefUpdate::new(
                    "refs/heads/main",
                    Some(fixture.target),
                    Some(dead.target),
                )?],
                &fs::read(&dead.pack)?,
                true,
            )
        } else {
            receive_input(
                format,
                &[RefUpdate::new(
                    "refs/tags/after-gc",
                    None,
                    Some(fixture.target),
                )?],
                &empty_pack(format)?,
                true,
            )
        };
        let push = if streaming {
            old.prepare_receive_stream_with_policy(TransactionId::new(), receive_frames(&input, 31), policy).await?
        } else {
            old.prepare_receive_with_policy(TransactionId::new(), input, policy).await?
        };
        assert!(
            operation.calls() > before + 6,
            "stale read and reopen stay charged"
        );
        assert!(
            operation.retry().is_err(),
            "the one retry was already consumed"
        );
        assert!(matches!(
            push.publish_receive().await?.0,
            object_log::Resolution::Committed(_)
        ));
        assert_eq!(operation.live_bytes(), 0);
    }
    Ok(())
}

#[tokio::test]
async fn common_receive_active_collection_charges_plan_before_staging() -> TestResult {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let fixture = fixture(format, b"active fence")?;
        let (log, faults, _) = test_log("common-receive-active-gc").await?;
        let view = log.load().await?;
        log.put_object(&view, Bytes::from_static(b"garbage"))
            .await?;
        let object_log::CollectionStart::Installed(fenced, _) = log.start_collection(&view).await?
        else {
            return Err("no GC fence".into());
        };
        assert!(fenced.collection_plan_bytes().is_some());
        let repository = common_open(&log, format).await?;
        let operation = repository.operation.clone();
        let before = operation.calls();
        faults.reset();
        let input = receive_input(
            format,
            &[RefUpdate::new(
                "refs/heads/main",
                None,
                Some(fixture.target),
            )?],
            &fs::read(&fixture.pack)?,
            true,
        );
        let push = repository
            .prepare_receive(TransactionId::new(), input)
            .await?;
        assert_eq!(operation.calls() - before, usize::try_from(faults.metrics().total_requests())?);
        assert!(matches!(
            push.publish_receive().await?.0,
            object_log::Resolution::Committed(_)
        ));
        let view = log.load().await?;
        assert!(matches!(
            log.resume_collection(&view).await?,
            object_log::CollectionFinish::Complete(..)
        ));
        assert_eq!(operation.live_bytes(), 0);
    }
    Ok(())
}

#[tokio::test]
async fn common_receive_true_thin_pack_uses_same_view_verified_base() -> TestResult {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let mut contents = String::new();
        for index in 0..4096 {
            writeln!(contents, "row {index:08} payload")?;
        }
        let fixture = fixture(format, contents.as_bytes())?;
        let (log, _, _) = test_log("common-receive-thin").await?;
        publish_durable_pack(&log, &fixture, format).await?;
        let source = fixture.directory.path().join("source");
        let contents = contents.replacen("row 00002000", "row changed!", 1);
        fs::write(source.join("file"), contents)?;
        command(Some(&source), &["commit", "--quiet", "-am", "thin change"])?;
        let target = ObjectId::parse(
            format,
            output(Some(&source), &["rev-parse", "HEAD"])?.trim(),
        )?;
        let revisions = source.join("revisions");
        fs::write(&revisions, format!("{target}\n^{}\n", fixture.target))?;
        let packed = Command::new("git")
            .current_dir(&source)
            .args(["pack-objects", "--thin", "--revs", "--stdout"])
            .stdin(fs::File::open(&revisions)?)
            .output()?;
        assert!(packed.status.success());
        let operation = Pool::new(crate::pack::budget::LIVE_BYTES).admit()?;
        assert!(matches!(
            crate::pack::normalize_attempt(&operation, format, &packed.stdout, &[]),
            Err(crate::pack::NormalizeError::MissingBase { .. })
        ));
        drop(operation);
        let input = receive_input(
            format,
            &[RefUpdate::new(
                "refs/heads/main",
                Some(fixture.target),
                Some(target),
            )?],
            &packed.stdout,
            true,
        );
        let push = common_open(&log, format)
            .await?
            .prepare_receive(TransactionId::new(), input)
            .await?;
        assert!(matches!(
            push.publish_receive().await?.0,
            object_log::Resolution::Committed(_)
        ));
        let recovered = cold_checked(&log, format).await?;
        assert_eq!(
            recovered.refs().get(b"refs/heads/main".as_slice()),
            Some(&target)
        );
    }
    Ok(())
}

#[tokio::test]
async fn common_receive_ref_namespace_applies_deletions_atomically() -> TestResult {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let fixture = fixture(format, b"namespace")?;
        let (log, faults, _) = test_log("common-receive-namespace").await?;
        publish_durable_pack(&log, &fixture, format).await?;
        let parent = RefUpdate::new("refs/heads/x", None, Some(fixture.target))?;
        let child = RefUpdate::new("refs/heads/x/y", None, Some(fixture.target))?;
        let input = receive_input(
            format,
            &[
                parent.clone(),
                RefUpdate::new("refs/heads/x.c", None, Some(fixture.target))?,
                child.clone(),
            ],
            &empty_pack(format)?,
            true,
        );
        let repository = common_open(&log, format).await?;
        faults.reset();
        assert!(
            matches!(repository.prepare_receive(TransactionId::new(), input).await, Err(Error::ReceiveRejected { source, .. }) if matches!(*source, Error::InvalidReference))
        );
        assert_eq!(faults.metrics().operation(Operation::Put).requests, 0);
        let input = receive_input(format, &[parent], &empty_pack(format)?, true);
        let push = common_open(&log, format)
            .await?
            .prepare_receive(TransactionId::new(), input)
            .await?;
        assert!(matches!(
            push.publish_receive().await?.0,
            object_log::Resolution::Committed(_)
        ));
        let input = receive_input(
            format,
            std::slice::from_ref(&child),
            &empty_pack(format)?,
            true,
        );
        assert!(
            matches!(common_open(&log, format).await?.prepare_receive(TransactionId::new(), input).await, Err(Error::ReceiveRejected { source, .. }) if matches!(*source, Error::InvalidReference))
        );
        let input = receive_input(
            format,
            &[
                RefUpdate::new("refs/heads/x", Some(fixture.target), None)?,
                child,
            ],
            &empty_pack(format)?,
            true,
        );
        let push = common_open(&log, format)
            .await?
            .prepare_receive(TransactionId::new(), input)
            .await?;
        assert!(matches!(
            push.publish_receive().await?.0,
            object_log::Resolution::Committed(_)
        ));
        let recovered = cold_checked(&log, format).await?;
        assert!(!recovered.refs().contains_key(b"refs/heads/x".as_slice()));
        assert_eq!(
            recovered.refs().get(b"refs/heads/x/y".as_slice()),
            Some(&fixture.target)
        );
    }
    Ok(())
}

#[tokio::test]
async fn oversized_publication_options_return_a_bounded_error() -> TestResult {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        for max_commit_bytes in [usize::MAX / 4, usize::MAX / 4 + 1] {
            let backend = ValidatedBackend::new(
                std::sync::Arc::new(InMemory::new()),
                StorePath::from("oversized-options"),
            )
            .await?;
            let log = Log::open(
                &backend,
                &LogId::new("oversized")?,
                Options {
                    max_commit_bytes,
                    ..Options::default()
                },
            )
            .await?;
            let repository = common_open(&log, format).await?;
            let fixture = fixture(format, b"bounded options")?;
            let update = RefUpdate::new("refs/heads/main", None, Some(fixture.target))?;
            let input = receive_input(format, &[update], &std::fs::read(fixture.pack)?, true);
            assert!(
                matches!(repository.prepare_receive(TransactionId::new(), input).await,
                Err(Error::ReceiveRejected { source, .. }) if matches!(*source, Error::InvalidPack(_)))
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn common_receive_token_survives_invalid_resolution_evidence() -> TestResult {
    use object_store::ObjectStoreExt;
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let fixture = fixture(format, b"hidden publication")?;
        let store = std::sync::Arc::new(InMemory::new());
        let faults = FaultStore::from_arc(store.clone());
        let backend = ValidatedBackend::new(
            std::sync::Arc::new(faults.clone()),
            StorePath::from("invalid-resolution"),
        )
        .await?;
        let log = Log::open(&backend, &LogId::new("repo")?, Options::default()).await?;
        let prepared = common_open(&log, format)
            .await?
            .prepare_receive(
                TransactionId::new(),
                receive_input(
                    format,
                    &[RefUpdate::new(
                        "refs/heads/main",
                        None,
                        Some(fixture.target),
                    )?],
                    &fs::read(&fixture.pack)?,
                    true,
                ),
            )
            .await?;
        let token = prepared.recovery_token().clone();
        let location = faults
            .metrics()
            .events
            .iter()
            .rev()
            .find(|event| event.path.ends_with("/index.cbor"))
            .map(|event| StorePath::from(event.path.clone()))
            .ok_or("missing head path")?;
        faults.reset();
        faults.schedule(Failure {
            operation: Operation::Put,
            occurrence: 2,
            phase: FailurePhase::After,
        });
        let mut gate = faults.pause_put_at(2, FailurePhase::After);
        let publishing = prepared.publish_receive();
        tokio::pin!(publishing);
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(10), async {
                tokio::select! {
                    entered = gate.wait_until_entered() => entered,
                    _ = &mut publishing => false,
                }
            })
            .await?
        );
        let authentic = store.get(&location).await?.bytes().await?;
        store
            .put(&location, Bytes::from_static(b"corrupt evidence").into())
            .await?;
        assert!(gate.release());
        assert!(publishing.await.is_err());
        store.put(&location, authentic.into()).await?;
        drop(log);
        let reopened = Log::open(&backend, &LogId::new("repo")?, Options::default()).await?;
        assert!(matches!(
            reopened.resume(&token).await?,
            object_log::Resolution::Committed(_)
        ));
        assert_eq!(
            common_open(&reopened, format)
                .await?
                .refs()
                .get(b"refs/heads/main".as_slice()),
            Some(&fixture.target)
        );
    }
    Ok(())
}

#[tokio::test]
async fn common_receive_expired_candidate_never_reports_success() -> TestResult {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let fixture = fixture(format, b"expiration")?;
        let faults = FaultStore::new(InMemory::new());
        let backend = ValidatedBackend::new(
            std::sync::Arc::new(faults.clone()),
            StorePath::from("expired-receive"),
        )
        .await?;
        let log = Log::open(
            &backend,
            &LogId::new("repo")?,
            Options {
                resolution_window: 1,
                ..Options::default()
            },
        )
        .await?;
        publish_durable_pack(&log, &fixture, format).await?;
        let candidate = common_open(&log, format)
            .await?
            .prepare_receive(
                TransactionId::new(),
                receive_input(
                    format,
                    &[RefUpdate::new(
                        "refs/heads/main",
                        Some(fixture.target),
                        None,
                    )?],
                    &[],
                    true,
                ),
            )
            .await?;
        let token = candidate.recovery_token().clone();
        // Independent test pools model separate processes; the production API
        // continues to admit only one command in each process or WASI instance.
        for name in ["refs/tags/one", "refs/tags/two"] {
            let prepared = common_open(&log, format)
                .await?
                .prepare_receive(
                    TransactionId::new(),
                    receive_input(
                        format,
                        &[RefUpdate::new(name, None, Some(fixture.target))?],
                        &empty_pack(format)?,
                        true,
                    ),
                )
                .await?;
            assert!(matches!(
                prepared.publish_receive().await?.0,
                object_log::Resolution::Committed(_)
            ));
        }
        assert!(matches!(
            common_open(&log, format).await?.checkpoint().await?,
            CheckpointStatus::Published(_)
        ));
        let (resolution, response) = candidate.publish_receive().await?;
        assert!(matches!(resolution, object_log::Resolution::Expired(_)));
        assert!(
            String::from_utf8_lossy(&response)
                .contains("ng refs/heads/main publication evidence expired")
        );
        assert!(!String::from_utf8_lossy(&response).contains("ok refs/"));
        assert!(matches!(
            log.resume(&token).await?,
            object_log::Resolution::Expired(_)
        ));
        assert_eq!(
            common_open(&log, format)
                .await?
                .refs()
                .get(b"refs/heads/main".as_slice()),
            Some(&fixture.target)
        );
    }
    Ok(())
}

#[tokio::test]
async fn guarded_receive_matches_client_attempts_through_publication_and_recovery() -> TestResult {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        for failure in [None, Some(FailurePhase::Before), Some(FailurePhase::After)] {
            let fixture = fixture(format, b"guarded publication")?;
            let (log, faults, _) = test_log("guarded-receive-parity").await?;
            faults.reset();
            let repository = common_open(&log, format).await?;
            let operation = repository.operation.clone();
            let prepared = repository.prepare_receive(TransactionId::new(), receive_input(format,
                &[RefUpdate::new("refs/heads/main", None, Some(fixture.target))?],
                &fs::read(&fixture.pack)?, true)).await?;
            assert_eq!(operation.calls(), usize::try_from(faults.metrics().total_requests())?);
            let before = operation.calls();
            faults.reset();
            if let Some(phase) = failure { faults.schedule(Failure {operation: Operation::Put, occurrence: 2, phase}); }
            let (outcome, response) = prepared.publish_receive().await?;
            assert!(matches!(outcome, object_log::Resolution::Committed(_)));
            assert_eq!(operation.calls() - before, usize::try_from(faults.metrics().total_requests())?);
            assert!(response.windows(3).any(|bytes| bytes == b"ok "));
            drop(response);
            assert_eq!(operation.live_bytes(), 0);
            assert_eq!(cold_checked(&log, format).await?.refs().get(b"refs/heads/main".as_slice()), Some(&fixture.target));
        }
    }
    Ok(())
}

#[tokio::test]
async fn shallow_push_requires_complete_server_history() -> TestResult {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let base = fixture(format, b"base")?;
        let source = base.directory.path().join("source");
        fs::write(source.join("file"), b"contribution")?;
        command(Some(&source), &["commit", "--quiet", "-am", "contribute"])?;
        let tip = ObjectId::parse(
            format,
            output(Some(&source), &["rev-parse", "HEAD"])?.trim(),
        )?;
        let selection = base.directory.path().join("selection");
        fs::write(&selection, format!("{tip}\n^{}\n", base.target))?;
        let packed = Command::new("git")
            .current_dir(&source)
            .args(["pack-objects", "--revs", "--stdout"])
            .stdin(fs::File::open(selection)?)
            .output()?;
        assert!(packed.status.success());
        let input = receive_input(
            format,
            &[RefUpdate::new("refs/heads/contribution", None, Some(tip))?],
            &packed.stdout,
            true,
        );
        let declaration = format!("shallow {}", base.target);
        let mut shallow = format!("{:04x}{declaration}", declaration.len() + 4).into_bytes();
        shallow.extend_from_slice(&input);
        let shallow = Bytes::from(shallow);
        for migrated in [false, true] {
            let (log, _, _) = test_log("shallow-push-closure").await?;
            if migrated {
                assert!(matches!(common_open(&log, format).await?.migrate_catalog_attempt(TransactionId::new()).await?, Some(CommitStatus::Committed(_))));
            }
            let before = log.load().await?.tail().len();
            for policy in [
                crate::ReceivePolicy::FastForwardOnly,
                crate::ReceivePolicy::AllowNonFastForward,
            ] {
                assert!(matches!(
                    common_open(&log, format)
                        .await?
                        .prepare_receive_with_policy(TransactionId::new(), shallow.clone(), policy)
                        .await,
                    Err(Error::ReceiveRejected { .. })
                ));
            }
            assert_eq!(log.load().await?.tail().len(), before);
            let base_input = receive_input(
                format,
                &[RefUpdate::new("refs/heads/main", None, Some(base.target))?],
                &fs::read(&base.pack)?,
                true,
            );
            let prepared = common_open(&log, format).await?
                .prepare_receive(TransactionId::new(), base_input).await?;
            assert!(matches!(prepared.publish_receive().await?.0,
                object_log::Resolution::Committed(_)));
            let prepared = common_open(&log, format)
                .await?
                .prepare_receive(TransactionId::new(), shallow.clone())
                .await?;
            assert!(matches!(
                prepared.publish_receive().await?.0,
                object_log::Resolution::Committed(_)
            ));
            let recovered = cold_checked(&log, format).await?;
            assert_eq!(
                recovered.refs().get(b"refs/heads/contribution".as_slice()),
                Some(&tip)
            );
        }
    }
    Ok(())
}
include!("receive_stream_tests.rs");

include!("receive_stream_many_tests.rs");
