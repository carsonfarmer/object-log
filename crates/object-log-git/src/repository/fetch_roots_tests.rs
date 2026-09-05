// Separate valid histories fit individual stored packs but exceed the graph's
// unchanged aggregate object cap. A small advertised-tip update must not walk
// those histories, including when their tips have annotated tags.
#[tokio::test]
async fn advertised_tip_fetch_ignores_unrelated_histories_over_graph_limit() -> TestResult {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let mut fixture = fixture(format, b"before")?;
        let source = fixture.directory.path().join("source");
        let old = fixture.target;
        fs::write(source.join("file"), b"after")?;
        command(Some(&source), &["commit", "--quiet", "-am", "after"])?;
        fixture.target = ObjectId::parse(format, output(Some(&source), &["rev-parse", "HEAD"])?.trim())?;
        fs::write(&fixture.pack, command_output(Some(&source), &["pack-objects", "--all", "--stdout"])?.stdout)?;
        let mut expected = output(Some(&source), &["rev-list", "--objects", "HEAD", &format!("^{old}")])?
            .lines().map(|line| ObjectId::parse(format, line.split(' ').next().unwrap_or("")))
            .collect::<Result<Vec<_>, _>>()?;
        expected.sort_unstable();
        assert_eq!(expected.len(), 3);
        let (log, faults, _) = test_log("demand-fetch").await?;
        publish_durable_pack(&log, &fixture, format).await?;
        let haves = [old, ObjectId::from_bytes(format, &vec![0x71; format.digest_len()])?];
        for branch in 0..2 {
            let (bytes, tip, tag) = unrelated_history_pack(format, branch, 16_384)?;
            let operation = Pool::new(crate::pack::budget::LIVE_BYTES).admit()?;
            let view = log.load().await?;
            let normalized = crate::pack::normalize(&operation, format, &bytes, &[])?;
            let (descriptor, root) = durable::stage(&operation, &log, &view, normalized).await?;
            let record = Machine::new(format).transaction(vec![
                RefUpdate::new(format!("refs/heads/unrelated-{branch}"), None, Some(tip))?,
                RefUpdate::new(format!("refs/tags/unrelated-{branch}"), None, Some(tag))?,
            ], vec![descriptor])?;
            let prepared = log.prepare(&view, TransactionId::new(), record, Bytes::new(), vec![root])?;
            assert!(matches!(log.commit(prepared).await?, CommitStatus::Committed(_)));

        }
        // Cold operation and cache for each mode; include-tag must inspect tag
        // chains without expanding their unrelated commit/tree closures.
        for include_tag in [false, true] {
            let repository = common_open(&log, format).await?;
            faults.reset();
            let before = repository.operation.work_bytes();
            let pack = repository.fetch_pack(&[fixture.target], &haves, include_tag).await?;
            assert_selected_pack(&source, &pack, format, &expected, Some(old), fixture.target)?;
            let reads = faults.metrics().operation(Operation::Get).requests;
            let work = repository.operation.work_bytes() - before;
            assert!(reads < 32, "{format:?}, include_tag={include_tag}: {reads} reads");
            assert!(work < 16 * 1024 * 1024, "{format:?}, include_tag={include_tag}: {work} work bytes");
            assert_eq!(faults.metrics().operation(Operation::Put).requests, 0);
        }
    }
    Ok(())
}

fn unrelated_history_pack(format: ObjectFormat, branch: usize, count: u32) -> TestResult<(Vec<u8>, ObjectId, ObjectId)> {
    use std::io::Write as _;
    use gix_pack::data::entry::Header;
    let hash = crate::pack::object_hash(format);
    let mut writer = gix_hash::io::Write::new(Vec::new(), hash);
    writer.write_all(&gix_pack::data::header::encode(gix_pack::data::Version::V2, count + 2))?;
    let mut append = |kind: gix_object::Kind, header: Header, data: &[u8]| -> TestResult<ObjectId> {
        let id = gix_object::compute_hash(hash, kind, data)?;
        header.write_to(data.len() as u64, &mut writer)?;
        let mut compressor = gix_zlib::stream::deflate::Write::new(&mut writer, gix_zlib::Compression::DEFAULT);
        compressor.write_all(data)?;
        compressor.flush()?;
        Ok(ObjectId::from_bytes(format, id.as_slice())?)
    };
    let tree = append(gix_object::Kind::Tree, Header::Tree, b"")?;
    let mut parent = None;
    for index in 0..count {
        let mut data = format!("tree {tree}\n");
        if let Some(parent) = parent { writeln!(data, "parent {parent}")?; }
        write!(data, "author A <a@example.com> 0 +0000\ncommitter A <a@example.com> 0 +0000\n\nbranch {branch} commit {index}\n")?;
        parent = Some(append(gix_object::Kind::Commit, Header::Commit, data.as_bytes())?);
    }
    let tip = parent.ok_or("empty history")?;
    let data = format!("object {tip}\ntype commit\ntag unrelated-{branch}\ntagger A <a@example.com> 0 +0000\n\nunrelated\n");
    let tag = append(gix_object::Kind::Tag, Header::Tag, data.as_bytes())?;
    let gix_hash::io::Write { hash, mut inner } = writer;
    inner.extend_from_slice(hash.try_finalize()?.as_slice());
    Ok((inner, tip, tag))
}

#[tokio::test]
async fn advertised_tip_fetch_preserves_external_have_ancestry_and_acknowledgments() -> TestResult {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        for descendant in [false, true] {
            let mut fixture = fixture(format, b"base")?;
            let source = fixture.directory.path().join("source");
            let base = fixture.target;
            fs::write(source.join("file"), b"main")?;
            command(Some(&source), &["commit", "--quiet", "-am", "main"])?;
            fixture.target = ObjectId::parse(format, output(Some(&source), &["rev-parse", "HEAD"])?.trim())?;
            let from = if descendant { fixture.target } else { base };
            command(Some(&source), &["checkout", "--quiet", "-b", "other", &from.to_string()])?;
            fs::write(source.join("other"), b"other")?;
            command(Some(&source), &["add", "other"])?;
            command(Some(&source), &["commit", "--quiet", "-m", "other"])?;
            let have = ObjectId::parse(format, output(Some(&source), &["rev-parse", "HEAD"])?.trim())?;
            fs::write(&fixture.pack, command_output(Some(&source), &["pack-objects", "--all", "--stdout"])?.stdout)?;
            let (log, _, _) = test_log("external-have").await?;
            publish_durable_pack(&log, &fixture, format).await?;
            // Stored but unpublished descendants do not authorize a have,
            // including the Graph fallback used by filtered requests.
            let repository = common_open(&log, format).await?;
            let ack = repository.fetch_pack_or_ack(&[fixture.target], &[have], FetchOptions {
                include_tag: false, done: false, shallow: None,
                filter: Some(wire::Filter::BlobLimit(u64::MAX)), uris: None,
            }).await?;
            assert!(std::str::from_utf8(&ack)?.contains("NAK"));
            assert!(!std::str::from_utf8(&ack)?.contains("ACK "));
            drop(repository);
            let view = log.load().await?;
            let record = Machine::new(format).transaction(vec![RefUpdate::new("refs/heads/other", None, Some(have))?], vec![])?;
            let prepared = log.prepare(&view, TransactionId::new(), record, Bytes::new(), vec![])?;
            assert!(matches!(log.commit(prepared).await?, CommitStatus::Committed(_)));
            let mut expected = output(Some(&source), &["rev-list", "--objects", &fixture.target.to_string(), &format!("^{have}")])?
                .lines().map(|line| ObjectId::parse(format, line.split(' ').next().unwrap_or(""))).collect::<Result<Vec<_>, _>>()?;
            expected.sort_unstable();
            assert_eq!(expected.len(), if descendant { 0 } else { 3 });
            for filter in [None, Some(wire::Filter::BlobLimit(u64::MAX))] {
                let repository = common_open(&log, format).await?;
                let pack = repository.fetch_pack_or_ack(&[fixture.target], &[have], FetchOptions {
                    include_tag: false, done: true, shallow: None, filter, uris: None,
                }).await?;
                assert_selected_pack(&source, &pack, format, &expected, Some(have), fixture.target)?;
                let ack = repository.fetch_pack_or_ack(&[fixture.target], &[have], FetchOptions {
                    include_tag: false, done: false, shallow: None, filter, uris: None,
                }).await?;
                assert!(std::str::from_utf8(&ack)?.contains(&format!("ACK {have}\n")));
                assert!(!std::str::from_utf8(&ack)?.contains("NAK"));
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn include_tag_discovery_skips_unrelated_large_blob_bodies() -> TestResult {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let main = fixture(format, b"wanted")?;
        let source = main.directory.path().join("source");
        let (log, faults, _) = test_log("unrelated-blob-tags").await?;
        publish_durable_pack(&log, &main, format).await?;
        let mut random = 0x9e37_79b9_u32;
        let data = (0..50 * 1024 * 1024).map(|_| {
            random ^= random << 13; random ^= random >> 17; random ^= random << 5;
            random.to_le_bytes()[0]
        }).collect::<Vec<_>>();
        let unrelated = fixture(format, &data)?;
        drop(data);
        let work = unrelated.directory.path().join("source");
        let blob = ObjectId::parse(format, output(Some(&work), &["rev-parse", "HEAD:file"])?.trim())?;
        command(Some(&work), &["tag", "-a", "large", "-m", "large", &blob.to_string()])?;
        let tag = ObjectId::parse(format, output(Some(&work), &["rev-parse", "large"])?.trim())?;
        let bytes = command_output(Some(&work), &["pack-objects", "--all", "--stdout"])?.stdout;
        assert!(bytes.len() > 50 * 1024 * 1024);
        let view = log.load().await?;
        let stage = Pool::new(crate::pack::budget::LIVE_BYTES).admit()?;
        let bytes = Bytes::from(bytes);
        let frames = bytes.chunks(1024 * 1024).map(|chunk| Ok(Bytes::copy_from_slice(chunk)));
        let input = crate::pack::ingest::Input::receive(&stage, &log, &view, futures::stream::iter(frames)).await?;
        let (descriptor, root) = input.scan(format).await?.normalize(&mut crate::pack::ingest::NoBases).await?;
        drop(input);
        let record = Machine::new(format).transaction(vec![
            RefUpdate::new("refs/tags/lightweight-large", None, Some(blob))?,
            RefUpdate::new("refs/tags/annotated-large", None, Some(tag))?,
        ], vec![descriptor])?;
        let prepared = log.prepare(&view, TransactionId::new(), record, Bytes::new(), vec![root])?;
        assert!(matches!(log.commit(prepared).await?, CommitStatus::Committed(_)));
        drop(stage);
        let mut expected = output(Some(&source), &["rev-list", "--objects", "HEAD"])?
            .lines().map(|line| ObjectId::parse(format, line.split(' ').next().unwrap_or(""))).collect::<Result<Vec<_>, _>>()?;
        expected.sort_unstable();
        let repository = common_open(&log, format).await?;
        faults.reset();
        let before = repository.operation.work_bytes();
        let pack = repository.fetch_pack(&[main.target], &[], true).await?;
        assert_selected_pack(&source, &pack, format, &expected, None, main.target)?;
        let reads = faults.metrics().operation(Operation::Get);
        assert!(reads.downloaded_bytes < 4 * 1024 * 1024, "{format:?}: {} transferred", reads.downloaded_bytes);
        assert!(reads.requests < 12, "{format:?}: {} reads", reads.requests);
        assert!(repository.operation.work_bytes() - before < 8 * 1024 * 1024);
        assert_eq!(faults.metrics().operation(Operation::Put).requests, 0);
    }
    Ok(())
}
