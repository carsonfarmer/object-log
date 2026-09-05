#[tokio::test]
#[allow(clippy::too_many_lines, reason = "both-hash expiry and exhausted-retry fixture retains cumulative accounting evidence")]
async fn shallow_expired_view_reopens_once_with_cumulative_counters() -> TestResult {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        for spent in [false, true] {
            let old = fixture(format, b"old")?;
            let mut new = fixture(format, b"new")?;
            let source = new.directory.path().join("source");
            command(
                Some(&source),
                &["commit", "--quiet", "--allow-empty", "-m", "next"],
            )?;
            new.target = ObjectId::parse(
                format,
                output(Some(&source), &["rev-parse", "HEAD"])?.trim(),
            )?;
            fs::write(
                &new.pack,
                command_output(Some(&source), &["pack-objects", "--all", "--stdout"])?.stdout,
            )?;
            let (log, faults, _) = test_log("shallow-upload-expiry").await?;
            publish_durable_pack(&log, &old, format).await?;
            let pool = Pool::new(crate::pack::budget::LIVE_BYTES);
            let repository = Repository::open_with_pool(&log, format, &pool).await?;
            let operation = repository.operation.clone();
            if spent {
                operation.retry()?;
            }
            operation.work(1024 * 1024)?;
            let before_work = operation.work_bytes();
            let before = operation.calls();
            let view = log.load().await?;
            let staging = Pool::new(crate::pack::budget::LIVE_BYTES).admit()?;
            let normalized = crate::pack::normalize(&staging, format, &fs::read(&new.pack)?, &[])?;
            let (descriptor, root) = durable::stage(&staging, &log, &view, normalized).await?;
            let record = Machine::new(format).transaction(
                vec![RefUpdate::new(
                    "refs/heads/main",
                    Some(old.target),
                    Some(new.target),
                )?],
                vec![descriptor],
            )?;
            let prepared = log.prepare(
                &view,
                TransactionId::new(),
                record,
                Bytes::new(),
                vec![root],
            )?;
            assert!(matches!(
                log.commit(prepared).await?,
                CommitStatus::Committed(_)
            ));
            let checkpoint = common_open(&log, format).await?;
            let CheckpointStatus::Published(view) = checkpoint.checkpoint().await? else {
                return Err("checkpoint failed".into());
            };
            let object_log::CollectionStart::Installed(view, _) =
                log.start_collection(&view).await?
            else {
                return Err("collection failed".into());
            };
            assert!(matches!(
                log.resume_collection(&view).await?,
                object_log::CollectionFinish::Complete(_, _)
            ));
            faults.reset();
            let result = repository
                .upload_pack(upload_request(
                    format,
                    "fetch",
                    &[
                        format!("want {}", new.target),
                        "deepen 1".into(),
                        "done".into(),
                    ],
                )?)
                .await;
            if spent {
                assert!(
                    matches!(result, Err(Error::InvalidPack(message)) if message == "Git retry limit exceeded")
                );
            } else {
                let reply = result?;
                assert!(response_pack(&reply)?.starts_with(b"PACK"));
                assert!(
                    reply
                        .windows(b"shallow-info".len())
                        .any(|part| part == b"shallow-info")
                );
                assert_eq!(operation.live_bytes(), reply.len());
                assert!(operation.work_bytes() > before_work);
                eprintln!(
                    "shallow {format:?}: cumulative calls={}, work={}, retained_response_bytes={}, fetch_store_gets={}, fetch_downloaded_bytes={}",
                    operation.calls(),
                    operation.work_bytes(),
                    operation.live_bytes(),
                    faults.metrics().operation(Operation::Get).requests,
                    faults.metrics().downloaded_bytes()
                );
            }
            assert!(operation.calls() > before);
            assert!(operation.retry().is_err());
        }
    }
    Ok(())
}
