#[tokio::test]
#[allow(clippy::too_many_lines, reason = "both-hash expiry and exhausted-retry fixture retains cumulative accounting evidence")]
async fn partial_lazy_fetch_reopens_once_with_cumulative_counters() -> TestResult {
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
            let (log, faults, _) = test_log("partial-upload-expiry").await?;
            publish_durable_pack(&log, &old, format).await?;
            let pool = Pool::new(crate::pack::budget::LIVE_BYTES);
            let caller = CallerGuard::new(usize::MAX);
            let repository = Repository::open_with_pool(&log.with_request_guard(caller.clone()), format, &pool).await?;
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
                        format!("want {}", output(Some(&source), &["rev-parse", "HEAD:file"])?.trim()),
                        "filter blob:none".into(),
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
                let pack = response_pack(&reply)?;
                assert_eq!(u32::from_be_bytes(pack[8..12].try_into()?), 1);
                assert_eq!(operation.live_bytes(), reply.len());
                assert!(operation.work_bytes() > before_work);
                eprintln!(
                    "partial {format:?}: cumulative calls={}, work={}, retained_response_bytes={}, fetch_store_gets={}, fetch_downloaded_bytes={}",
                    operation.calls(),
                    operation.work_bytes(),
                    operation.live_bytes(),
                    faults.metrics().operation(Operation::Get).requests,
                    faults.metrics().downloaded_bytes()
                );
            }
            assert!(operation.calls() > before);
            assert_eq!(caller.calls(), operation.calls(), "reopen must not append the operation guard twice");
            assert!(operation.retry().is_err());
        }
    }
    Ok(())
}

#[tokio::test]
async fn filtered_fetch_accounts_for_store_work_and_retained_response() -> TestResult {
    let mut seed = 0x1950_2741_u64;
    let contents = (0..2 * 1024 * 1024).map(|_| {
        seed ^= seed << 13; seed ^= seed >> 7; seed ^= seed << 17;
        seed.to_le_bytes()[0]
    }).collect::<Vec<_>>();
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let fixture = fixture(format, &contents)?;
        let (log, faults, _) = test_log("partial-fetch-counters").await?;
        publish_durable_pack(&log, &fixture, format).await?;
        for filter in [None, Some("blob:none"), Some("blob:limit=4096"), Some("blob:limit=4194304")] {
            let pool = Pool::new(crate::pack::budget::LIVE_BYTES);
            let caller = CallerGuard::new(usize::MAX);
            let repository = Repository::open_with_pool(&log.with_request_guard(caller.clone()), format, &pool).await?;
            let operation = repository.operation.clone();
            let mut args = vec![format!("want {}", fixture.target)];
            if let Some(filter) = filter { args.push(format!("filter {filter}")); }
            args.push("done".into());
            let before = operation.calls();
            faults.reset();
            let response = repository.upload_pack(upload_request(format, "fetch", &args)?).await?;
            assert_eq!(operation.calls() - before, usize::try_from(faults.metrics().total_requests())?);
            assert_eq!(operation.live_bytes(), response.len());
            let omitted = matches!(filter, Some("blob:none" | "blob:limit=4096"));
            if omitted { assert!(response.len() < 1024); } else { assert!(response.len() > contents.len()); }
            eprintln!("partial {format:?} filter={}: calls={} work={} response={} GETs={} downloaded={}", filter.unwrap_or("none"), operation.calls(), operation.work_bytes(), response.len(), faults.metrics().operation(Operation::Get).requests, faults.metrics().downloaded_bytes());
        }
    }
    Ok(())
}
