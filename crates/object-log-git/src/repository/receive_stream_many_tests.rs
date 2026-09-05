#[tokio::test]
async fn streaming_receive_many_small_commits_matches_buffered_admission() -> TestResult {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let fixture = fixture(format, b"base")?;
        let source = fixture.directory.path().join("source");
        for revision in 0..384 {
            command(
                Some(&source),
                &[
                    "commit",
                    "--quiet",
                    "--allow-empty",
                    "-m",
                    &format!("history-{revision}"),
                ],
            )?;
        }
        let target = ObjectId::parse(
            format,
            output(Some(&source), &["rev-parse", "HEAD"])?.trim(),
        )?;
        let packed = Command::new("git")
            .current_dir(&source)
            .args(["pack-objects", "--all", "--stdout"])
            .output()?;
        assert!(packed.status.success());
        let input = receive_input(
            format,
            &[RefUpdate::new("refs/heads/main", None, Some(target))?],
            &packed.stdout,
            true,
        );
        for streamed in [false, true] {
            let (log, faults, _) = test_log("many-small-commits").await?;
            let repository = common_open(&log, format).await?;
            let operation = repository.operation.clone();
            faults.reset();
            let prepared = if streamed {
                repository
                    .prepare_receive_stream(TransactionId::new(), receive_frames(&input, 64 * 1024))
                    .await
            } else {
                repository
                    .prepare_receive(TransactionId::new(), input.clone())
                    .await
            };
            assert!(
                prepared.is_ok(),
                "streamed={streamed}, calls={}, requests={}, result={:?}",
                operation.calls(),
                faults.metrics().total_requests(),
                prepared.as_ref().err()
            );
            assert!(matches!(
                prepared?.publish_receive().await?.0,
                object_log::Resolution::Committed(_)
            ));
            assert_eq!(
                cold_checked(&log, format)
                    .await?
                    .refs()
                    .get(b"refs/heads/main".as_slice()),
                Some(&target)
            );
        }
    }
    Ok(())
}
