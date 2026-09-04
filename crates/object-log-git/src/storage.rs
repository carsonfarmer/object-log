use std::path::Path;

use bytes::Bytes;
use object_log::{Log, ObjectKind, ObjectRef, StagedObject, View};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::task::JoinSet;

use crate::ObjectId;

const CHUNK_BYTES: usize = 8 * 1024 * 1024;
const MAX_PACK_BYTES: u64 = 512 * 1024 * 1024;
const MAX_TRANSFERS: usize = 8;

#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    #[error("invalid stored pack: {0}")]
    InvalidPack(&'static str),
    #[error("pack I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    ObjectLog(#[from] object_log::Error),
}

pub(crate) struct StagedPack {
    pub(crate) root: StagedObject,
    pub(crate) bytes: u64,
}

pub(crate) async fn stage_pack(
    log: &Log,
    view: &View,
    path: &Path,
    expected: ObjectId,
) -> Result<StagedPack, Error> {
    let mut file = File::open(path).await?;
    let bytes = file.metadata().await?.len();
    let trailer_bytes = u64::try_from(expected.as_bytes().len())
        .map_err(|_| Error::InvalidPack("pack trailer length exceeds u64"))?;
    let content_bytes = bytes
        .checked_sub(trailer_bytes)
        .ok_or(Error::InvalidPack("pack is shorter than its checksum"))?;
    let chunk_bytes = log.options().max_object_bytes.min(CHUNK_BYTES);
    if bytes == 0 || bytes > MAX_PACK_BYTES {
        return Err(Error::InvalidPack("pack byte length is out of range"));
    }
    if chunk_bytes == 0 {
        return Err(Error::InvalidPack("object byte limit is zero"));
    }
    let chunk_bytes_u64 =
        u64::try_from(chunk_bytes).map_err(|_| Error::InvalidPack("chunk length exceeds u64"))?;
    let chunks = usize::try_from(bytes.div_ceil(chunk_bytes_u64))
        .map_err(|_| Error::InvalidPack("chunk count exceeds usize"))?;
    if chunks > log.options().max_object_refs {
        return Err(Error::InvalidPack("pack needs too many chunks"));
    }

    let mut children = Vec::with_capacity(chunks);
    let mut uploads = JoinSet::new();
    let mut hasher = gix::hash::hasher(expected.format().into());
    let mut trailer = [0; 32];
    let mut read = 0_u64;
    for index in 0..chunks {
        let len = usize::try_from((bytes - read).min(chunk_bytes_u64))
            .map_err(|_| Error::InvalidPack("chunk length exceeds usize"))?;
        let len_u64 =
            u64::try_from(len).map_err(|_| Error::InvalidPack("chunk length exceeds u64"))?;
        let mut chunk = vec![0; len];
        file.read_exact(&mut chunk).await?;
        let body_len = usize::try_from((content_bytes - read.min(content_bytes)).min(len_u64))
            .map_err(|_| Error::InvalidPack("hashed byte length exceeds usize"))?;
        hasher.update(&chunk[..body_len]);
        if body_len < len {
            let offset = usize::try_from(read.saturating_sub(content_bytes))
                .map_err(|_| Error::InvalidPack("pack trailer offset exceeds usize"))?;
            trailer[offset..offset + len - body_len].copy_from_slice(&chunk[body_len..]);
        }
        read = read
            .checked_add(len_u64)
            .ok_or(Error::InvalidPack("pack is too large"))?;
        let log = log.clone();
        let view = view.clone();
        uploads.spawn(async move {
            let object = log.put_object(&view, Bytes::from(chunk)).await?;
            Ok((index, object))
        });
        if uploads.len() == MAX_TRANSFERS || index + 1 == chunks {
            children.extend(finish(&mut uploads).await?);
        }
    }
    let mut extra = [0];
    if file.read(&mut extra).await? != 0 || read != bytes {
        return Err(Error::InvalidPack("pack changed while it was read"));
    }
    let digest = hasher
        .try_finalize()
        .map_err(|_| Error::InvalidPack("pack checksum failed"))?;
    if digest.as_slice() != expected.as_bytes()
        || &trailer[..expected.as_bytes().len()] != expected.as_bytes()
    {
        return Err(Error::InvalidPack("pack checksum does not match"));
    }
    let root = log.put_node(view, Bytes::new(), children).await?;
    Ok(StagedPack { root, bytes })
}

pub(crate) async fn write_pack<W: AsyncWrite + Unpin>(
    log: &Log,
    view: &View,
    root: &ObjectRef,
    bytes: u64,
    output: &mut W,
) -> Result<(), Error> {
    if bytes == 0 || bytes > MAX_PACK_BYTES {
        return Err(Error::InvalidPack("pack byte length is out of range"));
    }
    let node = log.read_node(view, root).await?;
    if !node.payload().is_empty()
        || node.children().is_empty()
        || node
            .children()
            .iter()
            .any(|child| child.kind() != ObjectKind::Blob)
    {
        return Err(Error::InvalidPack("pack root is invalid"));
    }
    let declared = node.children().iter().try_fold(0_u64, |sum, child| {
        sum.checked_add(child.len())
            .ok_or(Error::InvalidPack("pack is too large"))
    })?;
    if declared != bytes {
        return Err(Error::InvalidPack("pack byte length does not match"));
    }

    let mut written = 0_u64;
    for batch in node.children().chunks(MAX_TRANSFERS) {
        let mut downloads = JoinSet::new();
        for (index, child) in batch.iter().cloned().enumerate() {
            let log = log.clone();
            let view = view.clone();
            downloads.spawn(async move {
                let chunk = log.read_object(&view, &child).await?;
                Ok((index, chunk))
            });
        }
        for chunk in finish(&mut downloads).await? {
            output.write_all(&chunk).await?;
            written = written
                .checked_add(
                    u64::try_from(chunk.len())
                        .map_err(|_| Error::InvalidPack("pack is too large"))?,
                )
                .ok_or(Error::InvalidPack("pack is too large"))?;
        }
    }
    if written != bytes {
        return Err(Error::InvalidPack("written byte length does not match"));
    }
    output.flush().await?;
    Ok(())
}

async fn finish<T: Send + 'static>(
    tasks: &mut JoinSet<Result<(usize, T), Error>>,
) -> Result<Vec<T>, Error> {
    let mut values = Vec::with_capacity(tasks.len());
    while let Some(result) = tasks.join_next().await {
        values.push(result.map_err(|_| Error::InvalidPack("pack transfer task failed"))??);
    }
    values.sort_unstable_by_key(|(index, _)| *index);
    Ok(values.into_iter().map(|(_, value)| value).collect())
}

#[cfg(test)]
mod tests {
    use std::error::Error as StdError;
    use std::io::Write as _;
    use std::sync::Arc;

    use object_log::sim::{FaultStore, Operation};
    use object_log::{LogId, Options, ValidatedBackend};
    use object_store::ObjectStoreExt;
    use object_store::memory::InMemory;
    use object_store::path::Path as StorePath;
    use tempfile::{NamedTempFile, TempDir};

    use super::*;

    type TestResult<T = ()> = Result<T, Box<dyn StdError>>;

    async fn open(options: Options) -> TestResult<(Log, View, FaultStore, Arc<InMemory>)> {
        let inner = Arc::new(InMemory::new());
        let faults = FaultStore::from_arc(inner.clone());
        let backend = ValidatedBackend::new(
            Arc::new(faults.clone()),
            StorePath::from("git-storage-tests"),
        )
        .await?;
        let log = Log::open(&backend, &LogId::new("repo")?, options).await?;
        let view = log.load().await?;
        faults.reset();
        Ok((log, view, faults, inner))
    }

    fn input(bytes: &[u8]) -> TestResult<NamedTempFile> {
        let mut file = NamedTempFile::new()?;
        file.write_all(bytes)?;
        Ok(file)
    }

    fn pack(len: usize) -> TestResult<(Vec<u8>, ObjectId)> {
        let trailer = 20;
        let mut bytes = b"pack"
            .iter()
            .copied()
            .cycle()
            .take(len.checked_sub(trailer).ok_or("pack is too short")?)
            .collect::<Vec<_>>();
        let mut hasher = gix::hash::hasher(gix::hash::Kind::Sha1);
        hasher.update(&bytes);
        let digest = hasher.try_finalize()?;
        bytes.extend_from_slice(digest.as_slice());
        Ok((
            bytes,
            ObjectId::from_bytes(crate::ObjectFormat::Sha1, digest.as_slice())?,
        ))
    }

    async fn recover(log: &Log, view: &View, pack: &StagedPack) -> TestResult<Vec<u8>> {
        let directory = TempDir::new()?;
        let path = directory.path().join("pack");
        let mut output = File::create(&path).await?;
        write_pack(log, view, pack.root.reference(), pack.bytes, &mut output).await?;
        drop(output);
        Ok(std::fs::read(path)?)
    }

    #[tokio::test]
    async fn rejects_zero_and_too_many_chunks_before_put() -> TestResult {
        let (log, view, faults, _) = open(Options::default()).await?;
        let expected = ObjectId::from_bytes(crate::ObjectFormat::Sha1, &[1; 20])?;
        let file = input(&[])?;
        assert!(matches!(
            stage_pack(&log, &view, file.path(), expected).await,
            Err(Error::InvalidPack(_))
        ));
        assert_eq!(faults.metrics().operation(Operation::Put).requests, 0);

        let file = input(&[1])?;
        assert!(matches!(
            stage_pack(&log, &view, file.path(), expected).await,
            Err(Error::InvalidPack(_))
        ));
        assert_eq!(faults.metrics().operation(Operation::Put).requests, 0);

        let options = Options {
            max_object_bytes: 1_024,
            max_object_refs: 1,
            ..Options::default()
        };
        let (log, view, faults, _) = open(options).await?;
        let (bytes, expected) = pack(1_025)?;
        let file = input(&bytes)?;
        assert!(matches!(
            stage_pack(&log, &view, file.path(), expected).await,
            Err(Error::InvalidPack(_))
        ));
        assert_eq!(faults.metrics().operation(Operation::Put).requests, 0);
        Ok(())
    }

    #[tokio::test]
    async fn short_and_boundary_packs_round_trip() -> TestResult {
        let options = Options {
            max_object_bytes: 1_024,
            ..Options::default()
        };
        let (log, view, _, _) = open(options).await?;
        for len in [21_usize, 1_024, 1_025] {
            let (bytes, expected) = pack(len)?;
            let file = input(&bytes)?;
            let pack = stage_pack(&log, &view, file.path(), expected).await?;
            assert_eq!(pack.bytes, u64::try_from(len)?);
            assert_eq!(recover(&log, &view, &pack).await?, bytes);
            let node = log.read_node(&view, pack.root.reference()).await?;
            assert_eq!(node.children().len(), len.div_ceil(1_024));
        }
        Ok(())
    }

    #[tokio::test]
    async fn rejects_wrong_descriptor_length_before_writing() -> TestResult {
        let (log, view, _, _) = open(Options::default()).await?;
        let (bytes, expected) = pack(24)?;
        let file = input(&bytes)?;
        let pack = stage_pack(&log, &view, file.path(), expected).await?;
        let directory = TempDir::new()?;
        let path = directory.path().join("output");
        let mut output = File::create(&path).await?;
        assert!(matches!(
            write_pack(
                &log,
                &view,
                pack.root.reference(),
                pack.bytes + 1,
                &mut output
            )
            .await,
            Err(Error::InvalidPack(_))
        ));
        drop(output);
        assert_eq!(std::fs::metadata(path)?.len(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn missing_and_corrupt_children_fail() -> TestResult {
        let (log, view, faults, inner) = open(Options::default()).await?;
        let (bytes, expected) = pack(24)?;
        let file = input(&bytes)?;
        let pack = stage_pack(&log, &view, file.path(), expected).await?;
        let child = faults
            .metrics()
            .events
            .into_iter()
            .find(|event| event.operation == Operation::Put && event.path.contains("/blobs/"))
            .ok_or_else(|| std::io::Error::other("blob PUT was not recorded"))?
            .path;
        let child = StorePath::from(child);
        inner.put(&child, Bytes::from_static(b"bad").into()).await?;
        let mut output = tokio::io::sink();
        assert!(
            write_pack(&log, &view, pack.root.reference(), pack.bytes, &mut output)
                .await
                .is_err()
        );
        inner.delete(&child).await?;
        assert!(
            write_pack(&log, &view, pack.root.reference(), pack.bytes, &mut output)
                .await
                .is_err()
        );
        Ok(())
    }

    #[tokio::test]
    async fn checksum_mismatch_never_stages_a_root() -> TestResult {
        let (log, view, faults, _) = open(Options::default()).await?;
        for corrupt in [0, 23] {
            faults.reset();
            let (mut bytes, expected) = pack(24)?;
            bytes[corrupt] ^= 1;
            let file = input(&bytes)?;
            assert!(matches!(
                stage_pack(&log, &view, file.path(), expected).await,
                Err(Error::InvalidPack("pack checksum does not match"))
            ));
            assert!(!faults.metrics().events.iter().any(|event| {
                event.operation == Operation::Put && event.path.contains("/nodes/")
            }));
        }
        Ok(())
    }
}
