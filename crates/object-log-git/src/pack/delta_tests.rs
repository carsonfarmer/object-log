use super::*;
use crate::pack::ingest::{
    resolve::{BaseProvider, NoBases},
    scratch::Decoded,
};

async fn oracle(
    log: &Log,
    view: &View,
    root: &object_log::StagedObject,
    dir: &Path,
    descriptor: &crate::format::PackDescriptor,
) -> TestResult {
    git(
        dir,
        &[
            "init",
            "--bare",
            "--quiet",
            if descriptor.id.format() == ObjectFormat::Sha1 {
                "--object-format=sha1"
            } else {
                "--object-format=sha256"
            },
        ],
        &[],
    )?;
    let node = log.read_node(view, root.reference()).await?;
    let mut file = File::create(dir.join("normalized.pack"))?;
    for child in node.children() {
        file.write_all(&log.read_object(view, child).await?)?;
    }
    drop(file);
    git(
        dir,
        &[
            "index-pack",
            "--strict",
            "--index-version=2",
            "-o",
            "normalized.idx",
            "normalized.pack",
        ],
        &[],
    )?;
    assert_eq!(&node.payload()[..], fs::read(dir.join("normalized.idx"))?);
    let data = fs::read(dir.join("normalized.pack"))?;
    assert_eq!(data.len() as u64, descriptor.bytes);
    assert_eq!(
        &data[data.len() - descriptor.id.format().digest_len()..],
        descriptor.id.as_bytes()
    );
    Ok(())
}

#[tokio::test]
async fn bounded_ref_ofs_and_forward_normalization_matches_git() -> TestResult {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        for mode in ["ref", "ofs", "forward"] {
            let dir = fixture(format, FRAME_BYTES, true)?;
            if mode == "ofs" {
                let ids = git(dir.path(), &["rev-list", "--objects", "--all"], &[])?;
                assert!(ids.is_empty()); // fixture has blobs but no refs
                let bytes = fs::read(dir.path().join("input.pack"))?;
                let normalized = crate::pack::normalize(
                    &Pool::new(crate::pack::budget::LIVE_BYTES).admit()?,
                    format,
                    &bytes,
                    &[],
                )?;
                // Ask Git to repack all indexed IDs using OFS_DELTA.
                let index = gix_pack::index::File::from_data(
                    normalized.index.as_slice(),
                    std::path::PathBuf::new(),
                    crate::pack::object_hash(format),
                )?;
                let mut ids = Vec::new();
                for entry in index.iter() {
                    writeln!(&mut ids, "{}", entry.oid)?;
                }
                let pack = git(
                    dir.path(),
                    &["pack-objects", "--stdout", "--delta-base-offset"],
                    &ids,
                )?;
                fs::write(dir.path().join("input.pack"), pack)?;
            }
            let (_, base_log, view) = open().await?;
            let operation = Pool::new(6 * FRAME_BYTES).admit()?;
            let log = base_log.with_request_guard(Arc::new(operation.clone()));
            let mut input = Input::receive(
                &operation,
                &log,
                &view,
                frames(&dir.path().join("input.pack"), 8191)?,
            )
            .await?;
            if mode == "forward" {
                let scanned = input.scan(format).await?;
                let pack = fs::read(dir.path().join("input.pack"))?;
                let mut reordered = pack[..12].to_vec();
                for entry in scanned.entries.iter().rev() {
                    reordered.extend_from_slice(
                        &pack[usize::try_from(entry.header.pack_offset())?
                            ..usize::try_from(entry.end)?],
                    );
                }
                reordered.extend_from_slice(&vec![0; format.digest_len()]);
                seal(&mut reordered, format)?;
                drop(scanned);
                drop(input);
                fs::write(dir.path().join("input.pack"), reordered)?;
                input = Input::receive(
                    &operation,
                    &log,
                    &view,
                    frames(&dir.path().join("input.pack"), 8191)?,
                )
                .await?;
            }
            let scanned = input.scan(format).await?;
            assert!(scanned.entries.iter().any(|entry| entry.id.is_none()));
            let mut provider = NoBases;
            let normalize = scanned.normalize(&mut provider);
            assert_send(&normalize);
            let (descriptor, root) = normalize.await?;
            oracle(&base_log, &view, &root, dir.path(), &descriptor).await?;
            drop(input);
            assert_eq!(operation.live_bytes(), 0);
            assert_eq!(base_log.load().await?.generation(), view.generation());
        }
    }
    Ok(())
}

struct FileBase {
    path: std::path::PathBuf,
    size: usize,
    calls: usize,
}
impl BaseProvider for FileBase {
    async fn provide<'a>(
        &mut self,
        source: &Input<'a>,
        id: crate::ObjectId,
    ) -> Result<Option<Decoded<'a>>, Error> {
        self.calls += 1;
        let frames = frames(&self.path, 4093).map_err(pack_error)?;
        Ok(Some(
            source
                .stage_base(id, gix_object::Kind::Blob, self.size, frames)
                .await?,
        ))
    }
}

#[tokio::test]
async fn bounded_thin_base_is_verified_and_streamed_into_normalized_pack() -> TestResult {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let dir = fixture(format, FRAME_BYTES, true)?;
        let (_, base_log, view) = open().await?;
        let operation = Pool::new(6 * FRAME_BYTES).admit()?;
        let log = base_log.with_request_guard(Arc::new(operation.clone()));
        let input = Input::receive(
            &operation,
            &log,
            &view,
            frames(&dir.path().join("input.pack"), 8191)?,
        )
        .await?;
        let scanned = input.scan(format).await?;
        let pack = fs::read(dir.path().join("input.pack"))?;
        let entry = scanned
            .entries
            .iter()
            .find(|entry| entry.id.is_none())
            .ok_or("missing delta")?;
        let gix_pack::data::entry::Header::RefDelta { base_id } = entry.header.header else {
            return Err("not REF delta".into());
        };
        let mut thin = gix_pack::data::header::encode(gix_pack::data::Version::V2, 1).to_vec();
        thin.extend_from_slice(
            &pack[usize::try_from(entry.header.pack_offset())?..usize::try_from(entry.end)?],
        );
        thin.extend_from_slice(&vec![0; format.digest_len()]);
        seal(&mut thin, format)?;
        drop(scanned);
        drop(input);
        fs::write(dir.path().join("thin.pack"), thin)?;
        let file = File::create(dir.path().join("base"))?;
        assert!(
            Command::new("git")
                .args(["cat-file", "blob", &base_id.to_string()])
                .current_dir(dir.path())
                .stdout(file)
                .status()?
                .success()
        );
        let mut provider = FileBase {
            path: dir.path().join("base"),
            size: FRAME_BYTES,
            calls: 0,
        };
        let input = Input::receive(
            &operation,
            &log,
            &view,
            frames(&dir.path().join("thin.pack"), 13)?,
        )
        .await?;
        let (descriptor, root) = input.scan(format).await?.normalize(&mut provider).await?;
        assert_eq!(provider.calls, 1);
        oracle(&base_log, &view, &root, dir.path(), &descriptor).await?;
        drop(input);
        assert_eq!(operation.live_bytes(), 0);
    }
    Ok(())
}

fn varint(mut value: usize, out: &mut Vec<u8>) {
    loop {
        let byte = value.to_le_bytes()[0] & 127;
        value >>= 7;
        out.push(byte | if value == 0 { 0 } else { 128 });
        if value == 0 {
            break;
        }
    }
}

fn entry(header: gix_pack::data::entry::Header, data: &[u8], pack: &mut Vec<u8>) -> TestResult {
    header.write_to(data.len() as u64, pack)?;
    let mut compressor =
        gix_zlib::stream::deflate::Write::new(pack, gix_zlib::Compression::DEFAULT);
    compressor.write_all(data)?;
    compressor.flush()?;
    Ok(())
}

fn synthetic(
    format: ObjectFormat,
    base: &[u8],
    instructions: &[u8],
    include_base: bool,
) -> TestResult<Vec<u8>> {
    let id = gix_object::compute_hash(
        crate::pack::object_hash(format),
        gix_object::Kind::Blob,
        base,
    )?;
    let mut pack = gix_pack::data::header::encode(
        gix_pack::data::Version::V2,
        if include_base { 2 } else { 1 },
    )
    .to_vec();
    if include_base {
        entry(gix_pack::data::entry::Header::Blob, base, &mut pack)?;
    }
    entry(
        gix_pack::data::entry::Header::RefDelta { base_id: id },
        instructions,
        &mut pack,
    )?;
    pack.extend_from_slice(&vec![0; format.digest_len()]);
    seal(&mut pack, format)?;
    Ok(pack)
}

#[tokio::test]
async fn delta_commands_cross_windows_and_validate_exact_bounds() -> TestResult {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let base = vec![b'x'; 65536];
        let mut valid = Vec::new();
        varint(base.len(), &mut valid);
        varint(base.len() + 1, &mut valid);
        valid.extend_from_slice(&[0x80, 1, b'y']); // Default 65536 copy + literal.
        let pack = synthetic(format, &base, &valid, true)?;
        let (_, base_log, view) = open().await?;
        let operation = Pool::new(5 * FRAME_BYTES).admit()?;
        let log = base_log.with_request_guard(Arc::new(operation.clone()));
        let input = Input::receive(
            &operation,
            &log,
            &view,
            stream::iter([Ok(Bytes::from(pack))]),
        )
        .await?;
        let scanned = input.scan(format).await?;
        let resolved = scanned.resolve(&mut NoBases).await?;
        let result = resolved.objects[1].as_ref().ok_or("missing result")?;
        let mut expected = base.clone();
        expected.push(b'y');
        assert_eq!(
            result.id.as_bytes(),
            gix_object::compute_hash(
                crate::pack::object_hash(format),
                gix_object::Kind::Blob,
                &expected
            )?
            .as_slice()
        );
        drop(resolved);
        drop(scanned);
        drop(input);
        assert_eq!(operation.live_bytes(), 0);
        let cases = [
            vec![0],
            vec![0x91],
            vec![0x94, 1, 1],
            vec![2, b'x'],
            vec![1, b'x', 1, b'y'],
        ];
        for command in cases {
            let mut delta = Vec::new();
            varint(base.len(), &mut delta);
            varint(1, &mut delta);
            delta.extend_from_slice(&command);
            let pack = synthetic(format, &base, &delta, true)?;
            let (_, base_log, view) = open().await?;
            let operation = Pool::new(5 * FRAME_BYTES).admit()?;
            let log = base_log.with_request_guard(Arc::new(operation.clone()));
            let input = Input::receive(
                &operation,
                &log,
                &view,
                stream::iter([Ok(Bytes::from(pack))]),
            )
            .await?;
            let scanned = input.scan(format).await?;
            assert!(scanned.normalize(&mut NoBases).await.is_err());
            drop(input);
            assert_eq!(operation.live_bytes(), 0);
            assert_eq!(base_log.load().await?.generation(), view.generation());
        }
    }
    Ok(())
}

#[tokio::test]
async fn scratch_cancellation_expiry_and_depth_fail_without_publication() -> TestResult {
    let format = ObjectFormat::Sha1;
    let base = vec![b'x'; 65536];
    let mut delta = Vec::new();
    varint(base.len(), &mut delta);
    varint(1, &mut delta);
    delta.extend_from_slice(&[0x90, 1]);
    let pack = synthetic(format, &base, &delta, true)?;
    let (store, base_log, view) = open().await?;
    let operation = Pool::new(5 * FRAME_BYTES).admit()?;
    let log = base_log.with_request_guard(Arc::new(operation.clone()));
    let input = Input::receive(
        &operation,
        &log,
        &view,
        stream::iter([Ok(Bytes::from(pack))]),
    )
    .await?;
    // Retain only the input chunks, leaving decoded scratch collectible.
    let prepared = log.prepare(
        &view,
        TransactionId::new(),
        Bytes::new(),
        Bytes::new(),
        input.chunks.clone(),
    )?;
    let CommitStatus::Committed(current) = log.commit(prepared).await? else {
        return Err("input retention failed".into());
    };
    let scanned = input.scan(format).await?;
    let baseline = operation.live_bytes();
    store.reset();
    let mut pause = store.pause_next_put(FailurePhase::Before);
    let mut provider = NoBases;
    let mut resolve = Box::pin(scanned.resolve(&mut provider));
    tokio::select! { entered = pause.wait_until_entered() => assert!(entered), result = &mut resolve => { result?; return Err("scratch write did not pause".into()); } }
    drop(resolve);
    assert!(!pause.release());
    assert_eq!(operation.live_bytes(), baseline);
    let mut decoded =
        crate::pack::ingest::scratch::decode(&input, &scanned.entries[0], format, None).await?;
    decoded.depth = crate::pack::MAX_DELTA_DEPTH;
    let error =
        crate::pack::ingest::scratch::decode(&input, &scanned.entries[1], format, Some(&decoded))
            .await
            .err()
            .ok_or("depth accepted")?;
    assert!(error.to_string().contains("deep"));
    decoded.depth = 0;
    let CollectionStart::Installed(collection, _) = log.start_collection(&current).await? else {
        return Err("collection did not start".into());
    };
    assert!(matches!(
        log.resume_collection(&collection).await?,
        CollectionFinish::Complete(_, _)
    ));
    assert!(
        crate::pack::ingest::scratch::decode(&input, &scanned.entries[1], format, Some(&decoded))
            .await
            .is_err()
    );
    drop(decoded);
    drop(scanned);
    drop(input);
    assert_eq!(operation.live_bytes(), 0);
    Ok(())
}

struct WrongBase {
    foreign: bool,
}
impl BaseProvider for WrongBase {
    async fn provide<'a>(
        &mut self,
        source: &Input<'a>,
        id: crate::ObjectId,
    ) -> Result<Option<Decoded<'a>>, Error> {
        if self.foreign {
            let foreign = Input::empty(&source.operation, source.log, source.view, 1)?;
            Ok(Some(
                foreign
                    .stage_base(
                        id,
                        gix_object::Kind::Blob,
                        1,
                        stream::iter([Ok(Bytes::from_static(b"x"))]),
                    )
                    .await?,
            ))
        } else {
            Ok(Some(
                source
                    .stage_base(
                        id,
                        gix_object::Kind::Blob,
                        1,
                        stream::iter([Ok(Bytes::from_static(b"y"))]),
                    )
                    .await?,
            ))
        }
    }
}

#[tokio::test]
async fn thin_capabilities_reject_wrong_oid_size_and_source() -> TestResult {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        for foreign in [false, true] {
            let pack = synthetic(format, b"x", &[1, 1, 1, b'y'], false)?;
            let (_, base_log, view) = open().await?;
            let operation = Pool::new(5 * FRAME_BYTES).admit()?;
            let log = base_log.with_request_guard(Arc::new(operation.clone()));
            let input = Input::receive(
                &operation,
                &log,
                &view,
                stream::iter([Ok(Bytes::from(pack))]),
            )
            .await?;
            assert!(
                input
                    .scan(format)
                    .await?
                    .normalize(&mut WrongBase { foreign })
                    .await
                    .is_err()
            );
            let id = crate::ObjectId::from_bytes(
                format,
                gix_object::compute_hash(
                    crate::pack::object_hash(format),
                    gix_object::Kind::Blob,
                    b"x",
                )?
                .as_slice(),
            )?;
            assert!(
                input
                    .stage_base(
                        id,
                        gix_object::Kind::Blob,
                        2,
                        stream::iter([Ok(Bytes::from_static(b"x"))])
                    )
                    .await
                    .is_err()
            );
            drop(input);
            assert_eq!(operation.live_bytes(), 0);
            assert_eq!(base_log.load().await?.generation(), view.generation());
        }
    }
    Ok(())
}

#[tokio::test]
async fn real_instruction_and_scratch_boundaries_and_copy_amplification() -> TestResult {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let base = vec![b'x'; 2 * FRAME_BYTES];
        for amplify in [false, true] {
            let mut delta = Vec::new();
            varint(base.len(), &mut delta);
            varint(if amplify { 600 } else { 65536 + 40000 }, &mut delta);
            if amplify {
                for i in 0..600 {
                    delta.extend_from_slice(&[0x94, if i % 2 == 0 { 0 } else { 16 }, 1]);
                }
            } else {
                delta.extend_from_slice(&[0x87, 0, 0x80, 0x0f]); // 64KiB across 1MiB boundary
                let mut remaining = 40000;
                while remaining > 0 {
                    let count = remaining.min(127);
                    delta.push(u8::try_from(count)?);
                    delta.extend_from_slice(&vec![b'y'; count]);
                    remaining -= count;
                }
            }
            let pack = synthetic(format, &base, &delta, true)?;
            let (_, base_log, view) = open().await?;
            let operation = Pool::new(5 * FRAME_BYTES).admit()?;
            let log = base_log.with_request_guard(Arc::new(operation.clone()));
            let input = Input::receive(
                &operation,
                &log,
                &view,
                stream::iter([Ok(Bytes::from(pack))]),
            )
            .await?;
            let scanned = input.scan(format).await?;
            if amplify {
                let error = scanned
                    .resolve(&mut NoBases)
                    .await
                    .err()
                    .ok_or("copy amplification accepted")?;
                assert!(matches!(
                    error,
                    Error::ObjectLog(object_log::Error::RequestDenied)
                ));
                // Repeating the resolution cannot reset the spent transfer quota.
                assert!(scanned.resolve(&mut NoBases).await.is_err());
            } else {
                let resolved = scanned.resolve(&mut NoBases).await?;
                let result = resolved.objects[1].as_ref().ok_or("missing result")?;
                let mut expected = vec![b'x'; 65536];
                expected.extend_from_slice(&vec![b'y'; 40000]);
                assert_eq!(
                    result.id.as_bytes(),
                    gix_object::compute_hash(
                        crate::pack::object_hash(format),
                        gix_object::Kind::Blob,
                        &expected
                    )?
                    .as_slice()
                );
            }
            drop(scanned);
            drop(input);
            assert_eq!(operation.live_bytes(), 0);
        }
    }
    Ok(())
}

#[tokio::test]
async fn mixed_chain_propagates_depth_and_matches_git() -> TestResult {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let mut pack = gix_pack::data::header::encode(gix_pack::data::Version::V2, 5).to_vec();
        let mut previous = b"x".to_vec();
        let mut offset = pack.len();
        entry(gix_pack::data::entry::Header::Blob, &previous, &mut pack)?;
        for depth in 1..5 {
            let next = vec![b'x'; depth + 1];
            let header = if depth % 2 == 0 {
                gix_pack::data::entry::Header::OfsDelta {
                    base_distance: (pack.len() - offset) as u64,
                }
            } else {
                gix_pack::data::entry::Header::RefDelta {
                    base_id: gix_object::compute_hash(
                        crate::pack::object_hash(format),
                        gix_object::Kind::Blob,
                        &previous,
                    )?,
                }
            };
            let mut delta = Vec::new();
            varint(previous.len(), &mut delta);
            varint(next.len(), &mut delta);
            delta.push(u8::try_from(next.len())?);
            delta.extend_from_slice(&next);
            offset = pack.len();
            entry(header, &delta, &mut pack)?;
            previous = next;
        }
        pack.extend_from_slice(&vec![0; format.digest_len()]);
        seal(&mut pack, format)?;
        let (_, base_log, view) = open().await?;
        let operation = Pool::new(5 * FRAME_BYTES).admit()?;
        let log = base_log.with_request_guard(Arc::new(operation.clone()));
        let input = Input::receive(
            &operation,
            &log,
            &view,
            stream::iter([Ok(Bytes::from(pack))]),
        )
        .await?;
        let scanned = input.scan(format).await?;
        let resolved = scanned.resolve(&mut NoBases).await?;
        for (depth, object) in resolved.objects.iter().enumerate() {
            assert_eq!(object.as_ref().ok_or("missing chain result")?.depth, depth);
        }
        drop(resolved);
        let (descriptor, root) = scanned.normalize(&mut NoBases).await?;
        let dir = tempfile::tempdir()?;
        oracle(&base_log, &view, &root, dir.path(), &descriptor).await?;
        drop(input);
        assert_eq!(operation.live_bytes(), 0);
    }
    Ok(())
}

#[tokio::test]
async fn repeated_thin_base_is_requested_once_and_cycles_fail() -> TestResult {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let id = gix_object::compute_hash(
            crate::pack::object_hash(format),
            gix_object::Kind::Blob,
            b"x",
        )?;
        let mut pack = gix_pack::data::header::encode(gix_pack::data::Version::V2, 2).to_vec();
        for value in *b"yz" {
            entry(
                gix_pack::data::entry::Header::RefDelta { base_id: id },
                &[1, 1, 1, value],
                &mut pack,
            )?;
        }
        pack.extend_from_slice(&vec![0; format.digest_len()]);
        seal(&mut pack, format)?;
        let dir = tempfile::tempdir()?;
        fs::write(dir.path().join("base"), b"x")?;
        let (_, base_log, view) = open().await?;
        let operation = Pool::new(5 * FRAME_BYTES).admit()?;
        let log = base_log.with_request_guard(Arc::new(operation.clone()));
        let input = Input::receive(
            &operation,
            &log,
            &view,
            stream::iter([Ok(Bytes::from(pack))]),
        )
        .await?;
        let mut provider = FileBase {
            path: dir.path().join("base"),
            size: 1,
            calls: 0,
        };
        let (descriptor, root) = input.scan(format).await?.normalize(&mut provider).await?;
        assert_eq!(provider.calls, 1);
        oracle(&base_log, &view, &root, dir.path(), &descriptor).await?;
        drop(input);
        assert_eq!(operation.live_bytes(), 0);
        let cycle = synthetic(format, b"x", &[1, 1, 1, b'x'], false)?;
        let input = Input::receive(
            &operation,
            &log,
            &view,
            stream::iter([Ok(Bytes::from(cycle))]),
        )
        .await?;
        assert!(
            input
                .scan(format)
                .await?
                .normalize(&mut NoBases)
                .await
                .is_err()
        );
        drop(input);
        assert_eq!(operation.live_bytes(), 0);
    }
    Ok(())
}

struct StoredBase {
    descriptor: crate::format::PackDescriptor,
    root: object_log::StagedObject,
}
impl BaseProvider for StoredBase {
    async fn provide<'a>(
        &mut self,
        source: &Input<'a>,
        id: crate::ObjectId,
    ) -> Result<Option<Decoded<'a>>, Error> {
        let selected = crate::durable::SelectedIndex::load(
            source.operation(),
            source.log(),
            source.view(),
            &self.descriptor,
            &self.root,
        )
        .await?;
        // Production catalog lookup supplies this position directly; this test
        // has one base pack and scans only its small authenticated index.
        let mut position = None;
        for entry in selected.entries() {
            let (candidate, candidate_position) = entry?;
            if candidate == id {
                position = Some(candidate_position);
                break;
            }
        }
        match position {
            Some(position) => Ok(Some(selected.stage_base(source, id, position).await?)),
            None => Ok(None),
        }
    }
}

#[tokio::test]
async fn stored_provider_normalizes_thin_input_through_bounded_selected_decode() -> TestResult {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let dir = fixture(format, FRAME_BYTES, true)?;
        let (_, base_log, view) = open().await?;
        let operation = Pool::new(6 * FRAME_BYTES).admit()?;
        let log = base_log.with_request_guard(Arc::new(operation.clone()));
        let input = Input::receive(
            &operation,
            &log,
            &view,
            frames(&dir.path().join("input.pack"), 8191)?,
        )
        .await?;
        let scanned = input.scan(format).await?;
        let pack = fs::read(dir.path().join("input.pack"))?;
        let delta = scanned
            .entries
            .iter()
            .find(|entry| entry.id.is_none())
            .ok_or("missing delta")?;
        let mut thin = gix_pack::data::header::encode(gix_pack::data::Version::V2, 1).to_vec();
        thin.extend_from_slice(
            &pack[usize::try_from(delta.header.pack_offset())?..usize::try_from(delta.end)?],
        );
        thin.extend_from_slice(&vec![0; format.digest_len()]);
        seal(&mut thin, format)?;
        let (descriptor, root) = scanned.normalize(&mut NoBases).await?;
        drop(input);
        let mut provider = StoredBase { descriptor, root };
        fs::write(dir.path().join("thin.pack"), thin)?;
        let input = Input::receive(
            &operation,
            &log,
            &view,
            frames(&dir.path().join("thin.pack"), 13)?,
        )
        .await?;
        let (descriptor, root) = input.scan(format).await?.normalize(&mut provider).await?;
        oracle(&base_log, &view, &root, dir.path(), &descriptor).await?;
        drop(input);
        assert_eq!(operation.live_bytes(), 0);
    }
    Ok(())
}

fn assert_send<T: Send>(_: &T) {}
