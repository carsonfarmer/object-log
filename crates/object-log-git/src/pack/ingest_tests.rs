use std::{
    fs::{self, File},
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
    process::{Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use futures::{StreamExt, TryStreamExt, stream};
use object_log::{
    CollectionFinish, CollectionStart, CommitStatus, LogId, Options, TransactionId,
    ValidatedBackend,
    sim::{FailurePhase, FaultStore, Operation as StoreOperation},
};
use object_store::{ObjectStore, ObjectStoreExt, memory::InMemory, path::Path as StorePath};

use super::*;
use crate::pack::budget::{Pool, WORK_BYTES};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

async fn open() -> TestResult<(FaultStore, Log, View)> {
    let store = FaultStore::from_arc(Arc::new(InMemory::new()));
    let backend = ValidatedBackend::new(Arc::new(store.clone()), StorePath::from("ingest")).await?;
    let base_log = Log::open(&backend, &LogId::new("prototype")?, Options::default()).await?;
    let view = base_log.load().await?;
    Ok((store, base_log, view))
}

fn git(path: &Path, args: &[&str], input: &[u8]) -> TestResult<Vec<u8>> {
    let mut child = Command::new("git")
        .args(args)
        .current_dir(path)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    child
        .stdin
        .take()
        .ok_or("missing stdin")?
        .write_all(input)?;
    let output = child.wait_with_output()?;
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).into_owned().into());
    }
    Ok(output.stdout)
}

fn fixture(format: ObjectFormat, size: usize, delta: bool) -> TestResult<tempfile::TempDir> {
    let dir = tempfile::tempdir()?;
    git(
        dir.path(),
        &[
            "init",
            "--bare",
            "--quiet",
            if format == ObjectFormat::Sha1 {
                "--object-format=sha1"
            } else {
                "--object-format=sha256"
            },
        ],
        &[],
    )?;
    let mut file = File::create(dir.path().join("blob"))?;
    let mut state = 7_u64;
    let mut block = [0; 8192];
    for position in (0..size).step_by(block.len()) {
        for byte in &mut block {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            *byte = state.to_le_bytes()[0];
        }
        file.write_all(&block[..block.len().min(size - position)])?;
    }
    drop(file);
    let mut ids = git(dir.path(), &["hash-object", "-w", "blob"], &[])?;
    if delta {
        let mut file = fs::OpenOptions::new()
            .write(true)
            .open(dir.path().join("blob"))?;
        file.seek(SeekFrom::Start(100))?;
        file.write_all(b"changed input bytes")?;
        drop(file);
        ids.extend(git(dir.path(), &["hash-object", "-w", "blob"], &[])?);
    }
    let output = File::create(dir.path().join("input.pack"))?;
    let mut child = Command::new("git")
        .args([
            "-c",
            "pack.compression=0",
            "pack-objects",
            "--stdout",
            "--window=10",
            "--depth=10",
        ])
        .current_dir(dir.path())
        .stdin(Stdio::piped())
        .stdout(output)
        .stderr(Stdio::piped())
        .spawn()?;
    child
        .stdin
        .take()
        .ok_or("missing pack stdin")?
        .write_all(&ids)?;
    let output = child.wait_with_output()?;
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).into_owned().into());
    }
    Ok(dir)
}

fn frames(
    path: &Path,
    width: usize,
) -> TestResult<futures::stream::BoxStream<'static, Result<Bytes, Error>>> {
    let file = File::open(path)?;
    Ok(stream::try_unfold(file, move |mut file| async move {
        let mut bytes = vec![0; width];
        let count = file.read(&mut bytes).map_err(pack_error)?;
        bytes.truncate(count);
        Ok(if count == 0 {
            None
        } else {
            Some((Bytes::from(bytes), file))
        })
    })
    .boxed())
}

#[tokio::test]
async fn full_pack_scans_and_indexes_without_retaining_input() -> TestResult {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let dir = fixture(format, 8 * FRAME_BYTES, false)?;
        let (store, base_log, view) = open().await?;
        let operation = Pool::new(3 * FRAME_BYTES).admit()?;
        let log = base_log.with_request_guard(Arc::new(operation.clone()));
        let input = Input::receive(
            &operation,
            &log,
            &view,
            frames(&dir.path().join("input.pack"), 8191)?,
        )
        .await?;
        assert!(input.bytes > 8 * FRAME_BYTES as u64);
        assert_eq!(input.chunks.len(), 9);
        assert!(operation.reserve(usize::try_from(input.bytes)?).is_err());
        let scanned = input.scan(format).await?;
        assert_eq!(scanned.entries.len(), 1);
        assert_eq!(scanned.entries[0].result_size, 8 * FRAME_BYTES);
        let (descriptor, root) = scanned.finish().await?;
        git(
            dir.path(),
            &[
                "index-pack",
                "--strict",
                "--index-version=2",
                "-o",
                "expected.idx",
                "input.pack",
            ],
            &[],
        )?;
        let node = log.read_node(&view, root.reference()).await?;
        assert_eq!(
            &node.payload()[..],
            fs::read(dir.path().join("expected.idx"))?
        );
        let prepared = log.prepare(
            &view,
            TransactionId::new(),
            Bytes::new(),
            Bytes::new(),
            vec![root.clone()],
        )?;
        let CommitStatus::Committed(current) = log.commit(prepared).await? else {
            return Err("publication did not commit".into());
        };
        drop(input);
        assert_eq!(operation.live_bytes(), 0);
        let legacy_operation = Pool::new(crate::pack::budget::LIVE_BYTES).admit()?;
        let log = base_log.with_request_guard(Arc::new(legacy_operation.clone()));
        let catalog = crate::durable::load(
            &legacy_operation,
            &log,
            &current,
            format,
            &[(descriptor, root.reference().clone())],
        )
        .await?;
        let expected = git(dir.path(), &["hash-object", "blob"], &[])?;
        let id = crate::ObjectId::parse(format, std::str::from_utf8(&expected)?.trim())?;
        assert_eq!(
            crate::durable::Reader::new(&log, &current, &catalog)
                .verify(id)
                .await?,
            Some(gix_object::Kind::Blob)
        );
        drop(catalog);
        assert_eq!(operation.live_bytes(), 0);
        assert!(store.metrics().operation(StoreOperation::Put).requests > 9);
    }
    Ok(())
}

#[tokio::test]
async fn deltas_are_scanned_but_cannot_finish() -> TestResult {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let dir = fixture(format, FRAME_BYTES, true)?;
        let (store, base_log, view) = open().await?;
        let operation = Pool::new(4 * FRAME_BYTES).admit()?;
        let log = base_log.with_request_guard(Arc::new(operation.clone()));
        let input = Input::receive(
            &operation,
            &log,
            &view,
            frames(&dir.path().join("input.pack"), 1023)?,
        )
        .await?;
        let scanned = input.scan(format).await?;
        assert!(scanned.entries.iter().any(|entry| entry.id.is_none()));
        store.reset();
        assert!(scanned.finish().await.is_err());
        assert_eq!(store.metrics().operation(StoreOperation::Put).requests, 0);
        drop(input);
        assert_eq!(operation.live_bytes(), 0);
    }
    Ok(())
}

#[tokio::test]
async fn input_reblocks_tiny_frames_and_checks_frame_and_total_bounds() -> TestResult {
    let (store, base_log, view) = open().await?;
    let operation = Pool::new(4 * FRAME_BYTES).admit()?;
    let log = base_log.with_request_guard(Arc::new(operation.clone()));
    let input = Input::receive(
        &operation,
        &log,
        &view,
        stream::iter((0..1234).map(|_| Ok(Bytes::from_static(b"x")))),
    )
    .await?;
    assert_eq!(input.chunks.len(), 1);
    assert_eq!(input.bytes, 1234);
    drop(input);
    assert_eq!(operation.live_bytes(), 0);
    store.reset();
    assert!(
        Input::receive(
            &operation,
            &log,
            &view,
            stream::iter([Ok(Bytes::from(vec![0; FRAME_BYTES + 1]))])
        )
        .await
        .is_err()
    );
    assert_eq!(store.metrics().operation(StoreOperation::Put).requests, 0);
    assert!(
        Input::receive(
            &operation,
            &log,
            &view,
            stream::iter((0..10).map(|_| Ok(Bytes::from(vec![0; FRAME_BYTES]))))
        )
        .await
        .is_err()
    );
    assert_eq!(operation.live_bytes(), 0);
    Ok(())
}

#[tokio::test]
async fn backpressure_and_cancellation_leave_only_unpublished_chunks() -> TestResult {
    let (store, base_log, view) = open().await?;
    let operation = Pool::new(4 * FRAME_BYTES).admit()?;
    let log = base_log.with_request_guard(Arc::new(operation.clone()));
    let polls = Arc::new(AtomicUsize::new(0));
    let observed = polls.clone();
    let frames = stream::iter((0..3).map(move |_| {
        observed.fetch_add(1, Ordering::Relaxed);
        Ok(Bytes::from(vec![0; FRAME_BYTES]))
    }));
    store.reset();
    let mut pause = store.pause_put_at(2, FailurePhase::Before);
    let mut receive = Box::pin(Input::receive(&operation, &log, &view, frames));
    tokio::select! { entered = pause.wait_until_entered() => assert!(entered), result = &mut receive => { result?; return Err("input did not pause".into()); } }
    assert_eq!(polls.load(Ordering::Relaxed), 2);
    drop(receive);
    assert!(!pause.release());
    assert_eq!(operation.live_bytes(), 0);
    assert_eq!(base_log.load().await?.generation(), view.generation());
    Ok(())
}

#[tokio::test]
async fn replay_work_remains_cumulative_and_old_input_expires_after_collection() -> TestResult {
    let dir = fixture(ObjectFormat::Sha1, 32 * 1024, false)?;
    let (_, base_log, view) = open().await?;
    let operation = Pool::new(4 * FRAME_BYTES).admit()?;
    let log = base_log.with_request_guard(Arc::new(operation.clone()));
    let input = Input::receive(
        &operation,
        &log,
        &view,
        frames(&dir.path().join("input.pack"), 4096)?,
    )
    .await?;
    drop(input.scan(ObjectFormat::Sha1).await?);
    let used = operation.work_bytes();
    operation.work(WORK_BYTES - used)?;
    assert!(input.scan(ObjectFormat::Sha1).await.is_err());
    drop(input);

    let operation = Pool::new(4 * FRAME_BYTES).admit()?;
    let log = base_log.with_request_guard(Arc::new(operation.clone()));
    let input = Input::receive(
        &operation,
        &log,
        &view,
        frames(&dir.path().join("input.pack"), 4096)?,
    )
    .await?;
    let CollectionStart::Installed(fenced, _) = log.start_collection(&view).await? else {
        return Err("collection did not start".into());
    };
    let CollectionFinish::Complete(_, _) = log.resume_collection(&fenced).await? else {
        return Err("collection did not finish".into());
    };
    assert!(matches!(
        input.scan(ObjectFormat::Sha1).await,
        Err(Error::ObjectLog(object_log::Error::ViewExpired))
    ));
    drop(input);
    assert_eq!(operation.live_bytes(), 0);
    Ok(())
}

fn seal(pack: &mut [u8], format: ObjectFormat) -> TestResult {
    let end = pack.len() - format.digest_len();
    let mut hash = gix_hash::hasher(crate::pack::object_hash(format));
    hash.update(&pack[..end]);
    pack[end..].copy_from_slice(hash.try_finalize()?.as_slice());
    Ok(())
}

#[tokio::test]
async fn malformed_scans_and_duplicate_objects_never_finish() -> TestResult {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let dir = fixture(format, 128, false)?;
        let original = fs::read(dir.path().join("input.pack"))?;
        let mut cases = Vec::new();
        let mut bad = original.clone();
        bad[0] ^= 1;
        cases.push(bad);
        let mut bad = original.clone();
        bad[11] = 2;
        seal(&mut bad, format)?;
        cases.push(bad);
        let mut bad = original.clone();
        bad[12] ^= 1; // Canonical but false inflated length.
        seal(&mut bad, format)?;
        cases.push(bad);
        let mut bad = original.clone();
        let end = bad.len() - 1;
        bad[end] ^= 1;
        cases.push(bad);
        let mut bad = original.clone();
        bad.remove(bad.len() - format.digest_len() - 1);
        seal(&mut bad, format)?;
        cases.push(bad);
        for pack in cases {
            let (store, base_log, view) = open().await?;
            let operation = Pool::new(3 * FRAME_BYTES).admit()?;
            let log = base_log.with_request_guard(Arc::new(operation.clone()));
            let input = Input::receive(
                &operation,
                &log,
                &view,
                stream::iter(pack.into_iter().map(|b| Ok(Bytes::from(vec![b])))),
            )
            .await?;
            store.reset();
            assert!(input.scan(format).await.is_err());
            assert_eq!(store.metrics().operation(StoreOperation::Put).requests, 0);
            drop(input);
            assert_eq!(operation.live_bytes(), 0);
        }
        let end = original.len() - format.digest_len();
        let mut duplicate = original[..end].to_vec();
        duplicate.extend_from_slice(&original[12..]);
        duplicate[11] = 2;
        seal(&mut duplicate, format)?;
        let (store, base_log, view) = open().await?;
        let operation = Pool::new(3 * FRAME_BYTES).admit()?;
        let log = base_log.with_request_guard(Arc::new(operation.clone()));
        let input = Input::receive(
            &operation,
            &log,
            &view,
            stream::iter([Ok(Bytes::from(duplicate))]),
        )
        .await?;
        let scanned = input.scan(format).await?;
        store.reset();
        assert!(scanned.finish().await.is_err());
        assert_eq!(store.metrics().operation(StoreOperation::Put).requests, 0);
        drop(input);
        assert_eq!(operation.live_bytes(), 0);
    }
    Ok(())
}

#[tokio::test]
async fn empty_frames_and_producer_errors_release_input_without_publication() -> TestResult {
    let (store, base_log, view) = open().await?;
    let operation = Pool::new(3 * FRAME_BYTES).admit()?;
    let log = base_log.with_request_guard(Arc::new(operation.clone()));
    store.reset();
    assert!(
        Input::receive(&operation, &log, &view, stream::iter([Ok(Bytes::new())]),)
            .await
            .is_err()
    );
    assert_eq!(store.metrics().operation(StoreOperation::Put).requests, 0);
    assert!(
        Input::receive(
            &operation,
            &log,
            &view,
            stream::iter([
                Ok(Bytes::from(vec![0; FRAME_BYTES])),
                Err(pack_error("producer failed")),
            ]),
        )
        .await
        .is_err()
    );
    assert_eq!(operation.live_bytes(), 0);
    assert_eq!(base_log.load().await?.generation(), view.generation());
    Ok(())
}

#[tokio::test]
async fn scanning_crosses_small_storage_chunks_and_rejects_damaged_chunks() -> TestResult {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let dir = fixture(format, 128, false)?;
        for width in [1, 2, 7, 13, 31] {
            let store = FaultStore::from_arc(Arc::new(InMemory::new()));
            let backend =
                ValidatedBackend::new(Arc::new(store.clone()), StorePath::from("tiny-chunks"))
                    .await?;
            let base_log = Log::open(
                &backend,
                &LogId::new("tiny")?,
                Options {
                    max_object_bytes: width,
                    max_object_refs: 1024,
                    ..Options::default()
                },
            )
            .await?;
            let view = base_log.load().await?;
            let operation = Pool::new(3 * FRAME_BYTES).admit()?;
            let log = base_log.with_request_guard(Arc::new(operation.clone()));
            let input = Input::receive(
                &operation,
                &log,
                &view,
                frames(&dir.path().join("input.pack"), 19)?,
            )
            .await?;
            assert_eq!(input.scan(format).await?.entries[0].result_size, 128);
            drop(input);
            assert_eq!(operation.live_bytes(), 0);
        }
        for remove in [false, true] {
            let (store, base_log, view) = open().await?;
            let before: Vec<_> = store.list(None).try_collect().await?;
            let operation = Pool::new(3 * FRAME_BYTES).admit()?;
            let log = base_log.with_request_guard(Arc::new(operation.clone()));
            let input = Input::receive(
                &operation,
                &log,
                &view,
                frames(&dir.path().join("input.pack"), 19)?,
            )
            .await?;
            let after: Vec<_> = store.list(None).try_collect().await?;
            let added: Vec<_> = after
                .iter()
                .filter(|item| !before.iter().any(|prior| prior.location == item.location))
                .collect();
            assert_eq!(added.len(), 1);
            let path = &added[0].location;
            if remove {
                store.delete(path).await?;
            } else {
                let mut bytes = store.get(path).await?.bytes().await?.to_vec();
                bytes[0] ^= 1;
                store.put(path, Bytes::from(bytes).into()).await?;
            }
            store.reset();
            assert!(input.scan(format).await.is_err());
            assert_eq!(store.metrics().operation(StoreOperation::Put).requests, 0);
            drop(input);
            assert_eq!(operation.live_bytes(), 0);
        }
    }
    Ok(())
}

#[path = "delta_tests.rs"]
mod delta_tests;
