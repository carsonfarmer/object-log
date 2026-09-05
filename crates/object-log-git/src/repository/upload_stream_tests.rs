#[tokio::test]
async fn prepared_upload_matches_buffered_controls_and_pack_for_both_catalogs() -> TestResult {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        for tree in [false, true] {
            let fixture = fixture(format, b"streamed upload")?;
            let (log, faults, _) = test_log("prepared-upload").await?;
            publish_durable_pack(&log, &fixture, format).await?;
            if tree {
                assert!(matches!(common_open(&log, format).await?.migrate_catalog_attempt(TransactionId::new()).await?, Some(object_log::CommitStatus::Committed(_))));
            }
            for (command, args) in [
                ("ls-refs", vec!["symrefs".into()]),
                ("fetch", vec![format!("want {}", fixture.target), format!("have {}", fixture.target)]),
                ("fetch", vec![format!("want {}", fixture.target), "done".into()]),
                ("fetch", vec![format!("want {}", fixture.target), "filter blob:none".into(), "done".into()]),
                ("fetch", vec![format!("want {}", fixture.target), "deepen 1".into(), "done".into()]),
            ] {
                let request = upload_request(format, command, &args)?;
                let expected = common_open(&log, format).await?.upload_pack(request.clone()).await?;
                let repository = common_open(&log, format).await?;
                let operation = repository.operation.clone();
                let prepared = receive_send(repository.prepare_upload(request, None)).await?;
                faults.reset();
                let mut bytes = Vec::new();
                {
                    let sink = futures::sink::unfold(&mut bytes, |bytes, frame: Bytes| async move {
                        assert!(frame.len() <= 65536);
                        bytes.extend_from_slice(&frame);
                        Ok::<_, std::io::Error>(bytes)
                    });
                    receive_send(prepared.write_to(&mut Box::pin(sink))).await?;
                }
                assert_eq!(bytes, expected);
                assert_eq!(faults.metrics().operation(Operation::Put).requests, 0);
                assert_eq!(operation.live_bytes(), 0);
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn prepared_upload_stops_on_sink_failure_and_releases_on_drop() -> TestResult {
    let format = ObjectFormat::Sha1;
    let fixture = fixture(format, b"backpressured upload")?;
    let (log, faults, _) = test_log("prepared-upload-stop").await?;
    publish_durable_pack(&log, &fixture, format).await?;
    let request = upload_request(format, "fetch", &[format!("want {}", fixture.target), "done".into()])?;
    let repository = common_open(&log, format).await?;
    let operation = repository.operation.clone();
    let prepared = repository.prepare_upload(request.clone(), None).await?;
    faults.reset();
    let sink = futures::sink::unfold((), |(), _: Bytes| async {
        Err::<(), _>(std::io::Error::other("closed sink"))
    });
    assert!(prepared.write_to(&mut Box::pin(sink)).await.is_err());
    assert_eq!(faults.metrics().total_requests(), 0);
    assert_eq!(operation.live_bytes(), 0);

    let repository = common_open(&log, format).await?;
    let operation = repository.operation.clone();
    let prepared = repository.prepare_upload(request, None).await?;
    faults.reset();
    let sink = futures::sink::unfold((), |(), _: Bytes| async {
        futures::future::pending::<std::io::Result<()>>().await
    });
    let mut sink = Box::pin(sink);
    let mut write = Box::pin(prepared.write_to(&mut sink));
    assert!(matches!(futures::poll!(&mut write), std::task::Poll::Pending));
    assert_eq!(faults.metrics().total_requests(), 0);
    drop(write);
    drop(sink);
    assert_eq!(operation.live_bytes(), 0);
    Ok(())
}

#[tokio::test]
async fn prepared_upload_expiry_after_selection_aborts_without_retry_or_flush() -> TestResult {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let live = fixture(format, b"retained fetch")?;
        let dead = fixture(format, b"collected fetch")?;
        let (log, faults, _) = test_log("prepared-upload-expiry").await?;
        publish_durable_pack(&log, &live, format).await?;
        let add = receive_input(format,
            &[RefUpdate::new("refs/tags/dead", None, Some(dead.target))?],
            &fs::read(&dead.pack)?, true);
        common_open(&log, format).await?.prepare_receive(TransactionId::new(), add)
            .await?.publish_receive().await?;
        let repository = common_open(&log, format).await?;
        let operation = repository.operation.clone();
        let prepared = repository.prepare_upload(upload_request(format, "fetch",
            &[format!("want {}", dead.target), "done".into()])?, None).await?;
        let delete = receive_input(format,
            &[RefUpdate::new("refs/tags/dead", Some(dead.target), None)?], &[], true);
        common_open(&log, format).await?.prepare_receive(TransactionId::new(), delete)
            .await?.publish_receive().await?;
        let object_log::CheckpointStatus::Published(view) = common_open(&log, format).await?.checkpoint().await? else {
            return Err("checkpoint not published".into());
        };
        let object_log::CollectionStart::Installed(fenced, _) = log.start_collection(&view).await? else {
            return Err("no collection fence".into());
        };
        assert!(matches!(log.resume_collection(&fenced).await?, object_log::CollectionFinish::Complete(..)));
        faults.reset();
        let mut bytes = Vec::new();
        {
            let sink = futures::sink::unfold(&mut bytes, |bytes, frame: Bytes| async move {
                bytes.extend_from_slice(&frame);
                Ok::<_, std::io::Error>(bytes)
            });
            assert!(matches!(prepared.write_to(&mut Box::pin(sink)).await,
                Err(Error::ObjectLog(object_log::Error::ViewExpired))));
        }
        assert_eq!(bytes, b"000dpackfile\n");
        assert_eq!(faults.metrics().operation(Operation::Put).requests, 0);
        assert_eq!(operation.live_bytes(), 0);
        operation.retry()?; // Writing did not consume a retry after the prefix.
    }
    Ok(())
}
