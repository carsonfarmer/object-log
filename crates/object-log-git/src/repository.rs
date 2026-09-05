use std::{fmt, mem::size_of};

use object_log::{Log, ObjectRef, StagedObject, View, materialize};

use crate::{
    Error, ObjectFormat, ObjectId, RefSnapshot,
    durable::{self, Catalog},
    format::PackDescriptor,
    pack::budget::{Operation, Pool, Reservation},
    state::{Machine, State},
};

#[cfg(feature = "native-oracle")]
use bytes::Bytes;
#[cfg(feature = "native-oracle")]
use object_log::{CheckpointStatus, CommitStatus, PreparedCommit, TransactionId};
#[cfg(feature = "native-oracle")]
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
};
#[cfg(feature = "native-oracle")]
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncWriteExt},
    task,
};

#[cfg(feature = "native-oracle")]
use crate::{RefUpdate, format::Record, git};

// Admission bounds include decoder Vec capacity and simultaneous canonical
// re-encoding. Head retention IDs can encode one byte into a 24-byte Vec;
// malformed Git ref arrays can encode two bytes into a 96-byte RefUpdate.
// The factors cover doubling capacity, the encoded buffers, and fixed headers.
const HEAD_DECODE_FACTOR: usize = 64;
const RECORD_DECODE_FACTOR: usize = 128;
const VIEW_RETAIN_FACTOR: usize = 8;
const STATE_RETAIN_FACTOR: usize = 4;

/// One exact Git repository view backed by an object log.
#[cfg_attr(
    not(feature = "native-oracle"),
    allow(
        dead_code,
        reason = "later common Git operations consume retained state"
    )
)]
pub struct Repository {
    log: Log,
    format: ObjectFormat,
    view: View,
    state: State,
    operation: Operation,
    _state_memory: Reservation,
    _view_memory: Reservation,
    #[cfg(feature = "native-oracle")]
    native: Option<Native>,
}

#[cfg(feature = "native-oracle")]
struct Native {
    path: PathBuf,
    objects: git::ObjectSet,
    reachable: git::ObjectKinds,
    live_packs: BTreeSet<ObjectId>,
}

#[cfg(feature = "native-oracle")]
#[derive(Debug)]
/// One native-oracle update ready for conditional publication.
#[must_use = "publish the update or retain its recovery token"]
pub struct PreparedPush {
    log: Log,
    prepared: PreparedCommit,
    recovery_token: Bytes,
}

impl Repository {
    /// Loads refs and authenticated pack references from one exact durable view.
    /// Pack indexes are loaded only by commands that read objects.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid durable state, a resource limit, or object
    /// storage failure. It retries once when collection expires the first view.
    pub async fn open(log: &Log, format: ObjectFormat) -> Result<Self, Error> {
        Self::open_with_pool(log, format, Pool::shared()).await
    }

    async fn open_with_pool(log: &Log, format: ObjectFormat, pool: &Pool) -> Result<Self, Error> {
        let operation = pool.admit()?;
        match Self::open_attempt(log, format, &operation).await {
            Err(Error::ObjectLog(object_log::Error::ViewExpired)) => {
                operation.retry()?;
                Self::open_attempt(log, format, &operation).await
            }
            result => result,
        }
    }

    async fn open_attempt(
        log: &Log,
        format: ObjectFormat,
        operation: &Operation,
    ) -> Result<Self, Error> {
        // The core loads and decodes the head before returning its exact view.
        let head = log.options().max_head_bytes;
        let head_memory = operation.reserve(memory_bound(head, HEAD_DECODE_FACTOR)?)?;
        operation.io(head)?;
        operation.work(head)?;
        let view = log.load().await?;
        let view_memory = operation.reserve_state(memory_bound(head, VIEW_RETAIN_FACTOR)?)?;
        drop(head_memory);
        let materialization_memory = preflight_view(operation, &view)?;
        let materialized = materialize(log, view, &Machine::new(format))
            .await
            .map_err(|error| match error {
                object_log::MaterializeError::Log(error) => Error::ObjectLog(error),
                object_log::MaterializeError::State(error) => error,
            })?;
        let (view, state) = materialized.into_parts();
        let state_memory = operation.reserve_state(state_bytes(&state)?)?;
        drop(materialization_memory);

        Ok(Self {
            log: log.clone(),
            format,
            view,
            state,
            operation: operation.clone(),
            _state_memory: state_memory,
            _view_memory: view_memory,
            #[cfg(feature = "native-oracle")]
            native: None,
        })
    }

    #[cfg_attr(
        not(feature = "native-oracle"),
        allow(dead_code, reason = "shared fetch commands follow graph traversal")
    )]
    async fn catalog(&self) -> Result<Catalog, Error> {
        durable::load(
            &self.operation,
            &self.log,
            &self.view,
            self.format,
            &pack_roots(&self.state),
        )
        .await
    }

    /// Returns the refs from the exact durable view.
    #[must_use]
    pub const fn refs(&self) -> &RefSnapshot {
        &self.state.refs
    }

    /// Rebuilds the temporary native oracle from one exact durable view.
    ///
    /// `work_dir` must not exist or must be empty. The method removes its
    /// partial cache and retries once if collection expires its first view.
    ///
    /// # Errors
    ///
    /// Returns an error for an unusable work directory, invalid durable state,
    /// pack recovery failure, a resource limit, or local Git failure.
    #[cfg(feature = "native-oracle")]
    pub async fn open_native(
        log: &Log,
        work_dir: impl AsRef<Path>,
        format: ObjectFormat,
    ) -> Result<Self, Error> {
        let path = work_dir.as_ref().to_owned();
        let init_path = path.clone();
        blocking(move || require_empty(&init_path)).await?;
        let pool = Pool::new(crate::pack::budget::LIVE_BYTES);
        let operation = pool.admit()?;
        match Self::open_native_attempt(log, &path, format, &operation).await {
            Err(Error::ObjectLog(object_log::Error::ViewExpired)) => {
                operation.retry()?;
                let retry_path = path.clone();
                blocking(move || clear_partial_cache(&retry_path)).await?;
                Self::open_native_attempt(log, &path, format, &operation).await
            }
            result => result,
        }
    }

    #[cfg(feature = "native-oracle")]
    async fn open_native_attempt(
        log: &Log,
        path: &Path,
        format: ObjectFormat,
        operation: &Operation,
    ) -> Result<Self, Error> {
        let mut repository = Self::open_attempt(log, format, operation).await?;
        let init_path = path.to_owned();
        blocking(move || {
            git::init(&init_path, format)?;
            Ok(())
        })
        .await?;

        let catalog = repository.catalog().await?;
        let mut objects = git::ObjectSet::new();
        let mut recovered = BTreeMap::new();
        for &id in repository.state.packs.keys() {
            let pack_objects = recover_pack(&repository, &catalog, path, id).await?;
            git::extend_objects(&mut objects, pack_objects.iter().copied())?;
            recovered.insert(id, pack_objects);
        }
        let materialize_path = path.to_owned();
        let desired = repository.state.refs.clone();
        let (objects, reachable) = blocking(move || {
            let repo = git::open(&materialize_path, format)?;
            let reachable = git::validate_snapshot(&repo, &desired, &objects)?;
            git::materialize(&repo, &desired)?;
            Ok((objects, reachable))
        })
        .await?;
        let live_packs = recovered
            .into_iter()
            .filter_map(|(id, objects)| {
                let live = objects.iter().any(|object| reachable.contains_key(object));
                live.then_some(id)
            })
            .collect();
        repository.native = Some(Native {
            path: path.to_owned(),
            objects,
            reachable,
            live_packs,
        });
        Ok(repository)
    }

    /// Returns annotated tag refs mapped to fully peeled targets.
    ///
    /// # Errors
    ///
    /// Returns an error if a durable tag cannot resolve.
    #[cfg(feature = "native-oracle")]
    pub fn peeled_tags(&self) -> Result<RefSnapshot, Error> {
        let native = self.native()?;
        let repo = git::open(&native.path, self.format)?;
        let mut peeled = RefSnapshot::new();
        for (name, &target) in &self.state.refs {
            if let Some(target) = git::peel_tag(&repo, target)? {
                peeled.insert(name.clone(), target);
            }
        }
        Ok(peeled)
    }

    /// Writes the complete reachable set for currently reachable SHA-1 wants.
    /// Output is bounded to 512 MiB and can be partial on error.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid wants, objects, or output.
    #[cfg(feature = "native-oracle")]
    pub async fn write_fetch_pack(
        &self,
        wants: &[ObjectId],
        output: impl AsRef<Path>,
    ) -> Result<(), Error> {
        if self.format != ObjectFormat::Sha1 || wants.is_empty() {
            return Err(Error::InvalidReference);
        }
        let native = self.native()?;
        for &want in wants {
            let want = gix::hash::ObjectId::try_from(want)?;
            if !native.reachable.contains_key(&want) {
                return Err(Error::InvalidReference);
            }
        }
        let path = native.path.clone();
        let output = output.as_ref().to_owned();
        let objects = native.reachable.keys().copied().collect();
        blocking(move || {
            git::write_fetch_pack(&path, objects, &output)?;
            Ok(())
        })
        .await
    }

    /// Validates and stages one atomic ref update against this exact snapshot.
    ///
    /// A supplied pack is normalized and validated before object storage is
    /// changed. This method consumes the cache so a failed or conflicting
    /// update cannot leave a reusable local view. Discard the work directory
    /// after any result. Failed normalization can leave local files there.
    /// Failed pack staging can leave unreachable immutable blobs, but it does
    /// not return or publish a pack root.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid refs, an invalid or unreachable object
    /// graph, a duplicate pack, local Git failure, or object-log failure.
    #[cfg(feature = "native-oracle")]
    pub async fn prepare_push(
        self,
        transaction_id: TransactionId,
        updates: Vec<RefUpdate>,
        pack: Option<&Path>,
    ) -> Result<PreparedPush, Error> {
        self.log.preflight(&self.view, transaction_id)?;

        let native = self.native.ok_or(Error::UnsupportedRepository)?;
        let path = native.path;
        let input = pack.map(Path::to_owned);
        let current = self.state.refs;
        let format = self.format;
        let (normalized, updates) = blocking(move || {
            let normalized = git::prepare_push(
                &path,
                format,
                &current,
                &updates,
                native.objects,
                input.as_deref(),
            )?;
            Ok((normalized, updates))
        })
        .await?;

        let mut objects = Vec::new();
        let packs = if let Some(pack) = normalized {
            let id = ObjectId::try_from(pack.id)?;
            if self.state.packs.contains_key(&id) {
                return Err(Error::InvalidRecord("pack is already present"));
            }
            let _input_memory = self.operation.reserve(crate::pack::MAX_PACK_BYTES)?;
            let file = File::open(&pack.path)
                .await
                .map_err(|error| Error::PackStorage(error.to_string()))?;
            let bytes = read_native_pack(file).await?;
            let normalized = crate::pack::normalize_stored(&self.operation, self.format, &bytes)?;
            if normalized.id != id {
                return Err(Error::InvalidPack("native pack ID changed".into()));
            }
            let (descriptor, root) =
                durable::stage(&self.operation, &self.log, &self.view, normalized).await?;
            objects.push(root);
            vec![descriptor]
        } else {
            Vec::new()
        };
        let operation = Machine::new(self.format).transaction(updates, packs)?;
        let prepared =
            self.log
                .prepare(&self.view, transaction_id, operation, Bytes::new(), objects)?;
        let recovery_token = prepared.recovery_token()?;
        Ok(PreparedPush {
            log: self.log,
            prepared,
            recovery_token,
        })
    }

    /// Publishes one checkpoint that retains all packs used by current refs.
    /// An empty tail returns the current view without object-store I/O.
    ///
    /// This method consumes the cache. It returns the core checkpoint result
    /// so the caller can resolve an uncertain publication with [`Log`].
    ///
    /// # Errors
    ///
    /// Returns an error for invalid durable objects, an invalid checkpoint, or
    /// an object-store failure.
    #[cfg(feature = "native-oracle")]
    pub async fn checkpoint(self) -> Result<CheckpointStatus, Error> {
        let Some(through) = self.view.tail().last().cloned() else {
            return Ok(CheckpointStatus::Published(self.view));
        };

        let mut roots = Vec::new();
        let mut packs = Vec::new();
        let live_packs = self.native.ok_or(Error::UnsupportedRepository)?.live_packs;
        for (id, (bytes, root)) in self.state.packs {
            if live_packs.contains(&id) {
                roots.push(root);
                packs.push(PackDescriptor { id, bytes });
            }
        }

        let snapshot = Record::snapshot(self.format, self.state.refs, packs)?.encode()?;
        Ok(self
            .log
            .publish_checkpoint(&self.view, &through, snapshot, roots)
            .await?)
    }

    #[cfg(feature = "native-oracle")]
    fn native(&self) -> Result<&Native, Error> {
        self.native.as_ref().ok_or(Error::UnsupportedRepository)
    }
}

impl fmt::Debug for Repository {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Repository")
            .field("format", &self.format)
            .field("view", &self.view)
            .field("refs", &self.state.refs)
            .field("packs", &self.state.packs.len())
            .finish_non_exhaustive()
    }
}

fn memory_bound(bytes: usize, factor: usize) -> Result<usize, Error> {
    bytes
        .checked_mul(factor)
        .ok_or_else(|| Error::InvalidPack("Git state exceeds memory".into()))
}

fn preflight_view(operation: &Operation, view: &View) -> Result<Reservation, Error> {
    let records = view
        .checkpoint()
        .into_iter()
        .map(|checkpoint| checkpoint.object().len())
        .chain(view.tail().iter().map(object_log::CommitRef::len));
    let mut total = 0_usize;
    for bytes in records {
        let bytes = usize::try_from(bytes)
            .map_err(|_| Error::InvalidPack("Git state exceeds memory".into()))?;
        total = total
            .checked_add(bytes)
            .ok_or_else(|| Error::InvalidPack("Git state exceeds memory".into()))?;
        operation.io(bytes)?;
        operation.work(bytes)?;
    }
    // Includes decoded Vec capacity, BTree nodes, proofs, and canonical re-encoding.
    // Even malformed short ref records can expand substantially during decoding.
    operation.reserve(memory_bound(total, RECORD_DECODE_FACTOR)?)
}

fn state_bytes(state: &State) -> Result<usize, Error> {
    let refs = state.refs.iter().try_fold(0_usize, |total, (name, _)| {
        total.checked_add(size_of::<(Vec<u8>, crate::ObjectId)>() + name.len())
    });
    let bytes = refs
        .and_then(|bytes| {
            bytes.checked_add(
                state.packs.len()
                    * (size_of::<(crate::ObjectId, (u64, StagedObject))>()
                        + size_of::<(PackDescriptor, ObjectRef)>()),
            )
        })
        .ok_or_else(|| Error::InvalidPack("Git state exceeds memory".into()))?;
    // A nonempty BTreeMap allocates a full root leaf even for one entry.
    let leaves = usize::from(!state.refs.is_empty()) * 12 * size_of::<(Vec<u8>, ObjectId)>()
        + usize::from(!state.packs.is_empty()) * 12 * size_of::<(ObjectId, (u64, StagedObject))>();
    memory_bound(bytes, STATE_RETAIN_FACTOR)?
        .checked_add(leaves)
        .ok_or_else(|| Error::InvalidPack("Git state exceeds memory".into()))
}

fn pack_roots(state: &State) -> Vec<(PackDescriptor, ObjectRef)> {
    state
        .packs
        .iter()
        .map(|(&id, (bytes, root))| {
            (
                PackDescriptor { id, bytes: *bytes },
                root.reference().clone(),
            )
        })
        .collect()
}

#[cfg(feature = "native-oracle")]
impl PreparedPush {
    /// Returns the token that identifies this exact publication attempt.
    #[must_use]
    pub const fn recovery_token(&self) -> &Bytes {
        &self.recovery_token
    }

    /// Conditionally publishes this push.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid staged data or an object-store failure
    /// that cannot hide a successful publication.
    pub async fn publish(self) -> Result<CommitStatus, Error> {
        Ok(self.log.commit(self.prepared).await?)
    }
}

#[cfg(feature = "native-oracle")]
async fn read_native_pack(mut file: File) -> Result<Vec<u8>, Error> {
    let length = file
        .metadata()
        .await
        .map_err(|e| Error::PackStorage(e.to_string()))?
        .len();
    let length = usize::try_from(length)
        .ok()
        .filter(|&n| n <= crate::pack::MAX_PACK_BYTES)
        .ok_or_else(|| Error::InvalidPack("native pack exceeds byte limit".into()))?;
    let mut bytes = vec![0; length];
    file.read_exact(&mut bytes)
        .await
        .map_err(|e| Error::PackStorage(e.to_string()))?;
    let mut extra = [0];
    if file
        .read(&mut extra)
        .await
        .map_err(|e| Error::PackStorage(e.to_string()))?
        != 0
    {
        return Err(Error::InvalidPack(
            "native pack changed while reading".into(),
        ));
    }
    Ok(bytes)
}

#[cfg(feature = "native-oracle")]
async fn recover_pack(
    repository: &Repository,
    catalog: &Catalog,
    work_dir: &Path,
    expected: ObjectId,
) -> Result<Vec<gix::hash::ObjectId>, Error> {
    let path = work_dir.join("object-log-recovery.pack");
    let bytes = catalog
        .pack_bytes(&repository.log, &repository.view, expected)
        .await?;
    let mut output = File::create(&path)
        .await
        .map_err(|error| Error::PackStorage(error.to_string()))?;
    output
        .write_all(&bytes)
        .await
        .map_err(|error| Error::PackStorage(error.to_string()))?;
    drop(output);
    let install_path = work_dir.to_owned();
    let input = path.clone();
    let objects = blocking(move || {
        let repo = git::open(&install_path, expected.format())?;
        let installed = git::install_pack(&repo, &input)?;
        Ok(installed.objects)
    })
    .await?;
    tokio::fs::remove_file(path)
        .await
        .map_err(|error| Error::Git(error.to_string()))?;
    Ok(objects)
}

#[cfg(feature = "native-oracle")]
async fn blocking<T>(
    operation: impl FnOnce() -> Result<T, Error> + Send + 'static,
) -> Result<T, Error>
where
    T: Send + 'static,
{
    task::spawn_blocking(operation)
        .await
        .map_err(|_| Error::BlockingTask)?
}

#[cfg(feature = "native-oracle")]
fn require_empty(path: &Path) -> Result<(), Error> {
    if !path.exists() {
        return Ok(());
    }
    let mut entries = fs::read_dir(path).map_err(|source| git::Error::Io {
        path: path.to_owned(),
        source,
    })?;
    if entries
        .next()
        .transpose()
        .map_err(|source| git::Error::Io {
            path: path.to_owned(),
            source,
        })?
        .is_some()
    {
        return Err(Error::WorkDirectoryNotEmpty);
    }
    Ok(())
}

#[cfg(feature = "native-oracle")]
fn clear_partial_cache(path: &Path) -> Result<(), Error> {
    match fs::remove_dir_all(path) {
        Ok(()) => Ok(()),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(git::Error::Io {
            path: path.to_owned(),
            source,
        }
        .into()),
    }
}

#[cfg(test)]
mod tests {
    use std::{
        error::Error as StdError,
        process::{Command, Output},
    };

    use object_log::sim::{Failure, FailurePhase, FaultStore, Operation};
    use object_log::{CheckpointResolution, LogId, Options, ValidatedBackend};
    use object_store::{memory::InMemory, path::Path as StorePath};
    use tempfile::TempDir;

    use super::*;

    type TestResult<T = ()> = Result<T, Box<dyn StdError>>;

    struct Fixture {
        directory: TempDir,
        pack: PathBuf,
        target: ObjectId,
    }

    async fn test_log(name: &str) -> TestResult<(Log, FaultStore, ValidatedBackend)> {
        let faults = FaultStore::new(InMemory::new());
        let backend = ValidatedBackend::new(
            std::sync::Arc::new(faults.clone()),
            StorePath::from("git-repository-tests"),
        )
        .await?;
        let log = Log::open(&backend, &LogId::new(name)?, Options::default()).await?;
        Ok((log, faults, backend))
    }

    fn fixture(format: ObjectFormat, contents: &[u8]) -> TestResult<Fixture> {
        let directory = tempfile::tempdir()?;
        let work = directory.path().join("source");
        let format_name = match format {
            ObjectFormat::Sha1 => "sha1",
            ObjectFormat::Sha256 => "sha256",
        };
        command(
            Some(directory.path()),
            &[
                "init",
                "--quiet",
                "-b",
                "main",
                &format!("--object-format={format_name}"),
                "source",
            ],
        )?;
        fs::write(work.join("file"), contents)?;
        command(Some(&work), &["add", "file"])?;
        command(Some(&work), &["commit", "--quiet", "-m", "initial"])?;
        let target = ObjectId::parse(format, output(Some(&work), &["rev-parse", "HEAD"])?.trim())?;
        let pack = directory.path().join("push.pack");
        fs::write(
            &pack,
            command_output(Some(&work), &["pack-objects", "--all", "--stdout"])?.stdout,
        )?;
        Ok(Fixture {
            directory,
            pack,
            target,
        })
    }

    async fn publish_durable_pack(
        log: &Log,
        fixture: &Fixture,
        format: ObjectFormat,
    ) -> TestResult<PackDescriptor> {
        let view = log.load().await?;
        let operation = Pool::new(crate::pack::budget::LIVE_BYTES).admit()?;
        let normalized =
            crate::pack::normalize(&operation, format, &fs::read(&fixture.pack)?, &[])?;
        let (descriptor, root) = durable::stage(&operation, log, &view, normalized).await?;
        let record = Machine::new(format).transaction(
            vec![RefUpdate::new(
                "refs/heads/main",
                None,
                Some(fixture.target),
            )?],
            vec![descriptor.clone()],
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
        Ok(descriptor)
    }

    #[test]
    fn decode_admission_covers_dense_malformed_ref_vectors() -> TestResult {
        // A missing optional pair makes [h''] the shortest decodable RefUpdate.
        assert!(2 * size_of::<RefUpdate>() + 16 <= 2 * RECORD_DECODE_FACTOR);
        assert!(2 * size_of::<Vec<u8>>() + 8 <= HEAD_DECODE_FACTOR);
        let mut bytes = minicbor::Encoder::new(Vec::new());
        bytes
            .map(5)?
            .u8(0)?
            .u32(1)?
            .u8(1)?
            .bool(true)?
            .u8(2)?
            .u8(1)?
            .u8(3)?
            .array(4096)?;
        for _ in 0..4096 {
            bytes.array(1)?.bytes(&[])?;
        }
        bytes.u8(4)?.array(0)?;
        let bytes = bytes.into_writer();
        assert!(Record::decode(&bytes, ObjectFormat::Sha1, 0).is_err());
        Ok(())
    }

    #[tokio::test]
    async fn common_open_accepts_384_transaction_history() -> TestResult {
        let fixture = fixture(ObjectFormat::Sha1, b"history")?;
        let (log, _, _) = test_log("history-384").await?;
        publish_durable_pack(&log, &fixture, ObjectFormat::Sha1).await?;
        for index in 0..383 {
            let view = log.load().await?;
            let (expected, target) = if index % 2 == 0 {
                (None, Some(fixture.target))
            } else {
                (Some(fixture.target), None)
            };
            let bytes = Machine::new(ObjectFormat::Sha1).transaction(
                vec![RefUpdate::new("refs/tags/changing", expected, target)?],
                vec![],
            )?;
            let prepared = log.prepare(&view, TransactionId::new(), bytes, Bytes::new(), vec![])?;
            assert!(matches!(
                log.commit(prepared).await?,
                CommitStatus::Committed(_)
            ));
        }
        let pool = Pool::new(crate::pack::budget::LIVE_BYTES);
        let repository = Repository::open_with_pool(&log, ObjectFormat::Sha1, &pool).await?;
        assert_eq!(repository.view.tail().len(), 384);
        assert_eq!(
            repository.refs().get(&b"refs/heads/main"[..]),
            Some(&fixture.target)
        );
        drop(repository);
        assert!(pool.admit().is_ok());
        Ok(())
    }

    #[tokio::test]
    async fn materialization_rejects_before_tail_reads_without_resetting_budget() -> TestResult {
        let fixture = fixture(ObjectFormat::Sha1, b"preflight")?;
        let (log, faults, _) = test_log("materialize-preflight").await?;
        publish_durable_pack(&log, &fixture, ObjectFormat::Sha1).await?;
        let view = log.load().await?;
        faults.reset();
        let operation = Pool::new(0).admit()?;
        assert!(preflight_view(&operation, &view).is_err());
        assert_eq!(faults.metrics().operation(Operation::Get).requests, 0);
        assert_eq!(operation.calls(), view.tail().len());
        assert_eq!(operation.live_bytes(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn head_transfer_is_charged_before_read_and_after_retry() -> TestResult {
        let (log, faults, _) = test_log("head-budget").await?;
        let operation = Pool::new(crate::pack::budget::LIVE_BYTES).admit()?;
        operation.io(crate::pack::budget::TRANSFER_BYTES - log.options().max_head_bytes)?;
        faults.reset();
        let repository = Repository::open_attempt(&log, ObjectFormat::Sha1, &operation).await?;
        assert_eq!(faults.metrics().operation(Operation::Get).requests, 1);
        drop(repository);
        operation.retry()?;
        faults.reset();
        assert!(
            Repository::open_attempt(&log, ObjectFormat::Sha1, &operation)
                .await
                .is_err()
        );
        assert_eq!(faults.metrics().operation(Operation::Get).requests, 0);
        assert_eq!(operation.live_bytes(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn native_stored_read_accepts_exact_limit_and_rejects_next_byte() -> TestResult {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("pack");
        let file = File::create(&path).await?;
        file.set_len(crate::pack::MAX_PACK_BYTES as u64).await?;
        drop(file);
        assert_eq!(
            read_native_pack(File::open(&path).await?).await?.len(),
            crate::pack::MAX_PACK_BYTES
        );
        let file = tokio::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .await?;
        file.set_len(crate::pack::MAX_PACK_BYTES as u64 + 1).await?;
        assert!(matches!(
            read_native_pack(File::open(&path).await?).await,
            Err(Error::InvalidPack(_))
        ));
        Ok(())
    }

    #[tokio::test]
    async fn common_repository_retains_one_authenticated_exact_view() -> TestResult {
        let fixture = fixture(ObjectFormat::Sha1, b"common repository")?;
        let (log, faults, _) = test_log("repository-common-view").await?;
        publish_durable_pack(&log, &fixture, ObjectFormat::Sha1).await?;
        faults.reset();
        let pool = Pool::new(crate::pack::budget::LIVE_BYTES);
        let repository = Repository::open_with_pool(&log, ObjectFormat::Sha1, &pool).await?;
        assert_eq!(
            repository.refs().get(&b"refs/heads/main"[..]),
            Some(&fixture.target)
        );
        assert_eq!(repository.state.packs.len(), 1);
        assert!(
            faults.metrics().events.iter().all(|event| {
                event.operation != Operation::Get || !event.path.contains("/nodes/")
            })
        );
        assert!(
            faults.metrics().events.iter().all(|event| {
                event.operation != Operation::Get || !event.path.contains("/blobs/")
            })
        );

        let view = log.load().await?;
        let record = Machine::new(ObjectFormat::Sha1).transaction(
            vec![RefUpdate::new("refs/tags/v1", None, Some(fixture.target))?],
            vec![],
        )?;
        let prepared = log.prepare(&view, TransactionId::new(), record, Bytes::new(), vec![])?;
        assert!(matches!(
            log.commit(prepared).await?,
            CommitStatus::Committed(_)
        ));
        assert!(!repository.refs().contains_key(&b"refs/tags/v1"[..]));
        assert!(pool.admit().is_err());
        drop(repository);
        assert!(pool.admit().is_ok());
        Ok(())
    }

    #[tokio::test]
    async fn common_repository_preflights_state_and_releases_cancelled_load() -> TestResult {
        let fixture = fixture(ObjectFormat::Sha1, b"bounded repository")?;
        let (log, faults, _) = test_log("repository-common-bounds").await?;
        publish_durable_pack(&log, &fixture, ObjectFormat::Sha1).await?;

        faults.reset();
        let bounded = Pool::new(0);
        assert!(
            Repository::open_with_pool(&log, ObjectFormat::Sha1, &bounded)
                .await
                .is_err()
        );
        assert_eq!(faults.metrics().operation(Operation::Get).requests, 0);
        assert!(bounded.admit().is_ok());

        faults.reset();
        let mut pause = faults.pause_get_at(2, FailurePhase::Before);
        let pool = Pool::new(crate::pack::budget::LIVE_BYTES);
        {
            let opening = Repository::open_with_pool(&log, ObjectFormat::Sha1, &pool);
            tokio::pin!(opening);
            assert!(tokio::select! {
                entered = pause.wait_until_entered() => entered,
                _ = &mut opening => false,
            });
        }
        assert!(!pause.release());
        assert!(pool.admit().is_ok());
        Ok(())
    }

    #[tokio::test]
    async fn publishes_and_cold_recovers_both_object_formats() -> TestResult {
        for (name, format) in [
            ("repository-sha1", ObjectFormat::Sha1),
            ("repository-sha256", ObjectFormat::Sha256),
        ] {
            let fixture = fixture(format, name.as_bytes())?;
            let (log, _, _) = test_log(name).await?;
            let cache = fixture.directory.path().join("cache");
            let repository = Repository::open_native(&log, &cache, format).await?;
            let update = RefUpdate::new("refs/heads/main", None, Some(fixture.target))?;
            let push = repository
                .prepare_push(TransactionId::new(), vec![update], Some(&fixture.pack))
                .await?;
            assert!(!push.recovery_token().is_empty());
            assert!(matches!(push.publish().await?, CommitStatus::Committed(_)));

            fs::remove_dir_all(&cache)?;
            let recovered = Repository::open_native(&log, &cache, format).await?;
            assert_eq!(
                recovered.refs().get(&b"refs/heads/main"[..]),
                Some(&fixture.target)
            );
            assert_eq!(
                output(Some(&cache), &["rev-parse", "refs/heads/main"])?.trim(),
                fixture.target.to_string()
            );
            let reuse = recovered
                .prepare_push(
                    TransactionId::new(),
                    vec![RefUpdate::new(
                        "refs/tags/existing",
                        None,
                        Some(fixture.target),
                    )?],
                    None,
                )
                .await?;
            assert!(matches!(reuse.publish().await?, CommitStatus::Committed(_)));
            fs::remove_dir_all(&cache)?;
            let recovered = Repository::open_native(&log, &cache, format).await?;
            assert_eq!(
                recovered.refs().get(&b"refs/tags/existing"[..]),
                Some(&fixture.target)
            );
            assert!(matches!(
                recovered.checkpoint().await?,
                CheckpointStatus::Published(view) if view.tail().is_empty()
            ));
            fs::remove_dir_all(&cache)?;
            let tail = Repository::open_native(&log, &cache, format).await?;
            let push = tail
                .prepare_push(
                    TransactionId::new(),
                    vec![RefUpdate::new(
                        "refs/tags/after-checkpoint",
                        None,
                        Some(fixture.target),
                    )?],
                    None,
                )
                .await?;
            assert!(matches!(push.publish().await?, CommitStatus::Committed(_)));
            fs::remove_dir_all(&cache)?;
            let recovered = Repository::open_native(&log, &cache, format).await?;
            assert_eq!(
                recovered.refs().get(&b"refs/tags/after-checkpoint"[..]),
                Some(&fixture.target)
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn checkpoint_reuses_materialized_pack_proofs_without_graph_reads() -> TestResult {
        let fixture = fixture(ObjectFormat::Sha1, b"checkpoint proof")?;
        let (log, faults, _) = test_log("repository-checkpoint-proof").await?;
        let repository = Repository::open_native(
            &log,
            fixture.directory.path().join("initial"),
            ObjectFormat::Sha1,
        )
        .await?;
        let push = repository
            .prepare_push(
                TransactionId::new(),
                vec![RefUpdate::new(
                    "refs/heads/main",
                    None,
                    Some(fixture.target),
                )?],
                Some(&fixture.pack),
            )
            .await?;
        assert!(matches!(push.publish().await?, CommitStatus::Committed(_)));

        let checkpoint = Repository::open_native(
            &log,
            fixture.directory.path().join("checkpoint"),
            ObjectFormat::Sha1,
        )
        .await?;
        faults.reset();
        assert!(matches!(
            checkpoint.checkpoint().await?,
            CheckpointStatus::Published(_)
        ));
        let metrics = faults.metrics();
        assert_eq!(metrics.operation(Operation::Get).requests, 1);
        assert_eq!(metrics.operation(Operation::Put).requests, 2);
        assert_eq!(
            metrics
                .events
                .iter()
                .filter(|event| {
                    event.operation == Operation::Get && event.path.contains("/commits/")
                })
                .count(),
            1
        );
        assert!(metrics.events.iter().all(|event| {
            event.operation != Operation::Get
                || (!event.path.contains("/nodes/") && !event.path.contains("/blobs/"))
        }));
        Ok(())
    }

    #[tokio::test]
    async fn rejects_loose_objects_that_are_not_in_a_durable_pack() -> TestResult {
        let fixture = fixture(ObjectFormat::Sha1, b"injected")?;
        let (log, _, _) = test_log("repository-loose-objects").await?;
        let cache = fixture.directory.path().join("cache");
        let repository = Repository::open_native(&log, &cache, ObjectFormat::Sha1).await?;
        let source = fixture.directory.path().join("source");
        for revision in ["HEAD", "HEAD^{tree}", "HEAD:file"] {
            let id = ObjectId::parse(
                ObjectFormat::Sha1,
                output(Some(&source), &["rev-parse", revision])?.trim(),
            )?;
            copy_loose_object(&source, &cache, id)?;
        }
        let target = fixture.target.to_string();
        command(
            Some(&cache),
            &["cat-file", "-e", &format!("{target}^{{tree}}")],
        )?;
        command(Some(&cache), &["cat-file", "-e", &format!("{target}:file")])?;

        let update = RefUpdate::new("refs/heads/main", None, Some(fixture.target))?;
        assert!(matches!(
            repository
                .prepare_push(TransactionId::new(), vec![update], None)
                .await,
            Err(Error::InvalidObjectGraph(_))
        ));

        fs::remove_dir_all(&cache)?;
        let recovered = Repository::open_native(&log, &cache, ObjectFormat::Sha1).await?;
        assert!(recovered.refs().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn delete_only_stages_no_pack_and_two_writers_conflict() -> TestResult {
        let competing = fixture(ObjectFormat::Sha1, b"loser")?;
        let fixture = fixture(ObjectFormat::Sha1, b"winner")?;
        let (log, faults, _) = test_log("repository-delete").await?;
        let first_path = fixture.directory.path().join("first");
        let second_path = fixture.directory.path().join("second");
        let first = Repository::open_native(&log, &first_path, ObjectFormat::Sha1).await?;
        let update = RefUpdate::new("refs/heads/main", None, Some(fixture.target))?;
        let first = first
            .prepare_push(
                TransactionId::new(),
                vec![update.clone()],
                Some(&fixture.pack),
            )
            .await?;
        let second = Repository::open_native(&log, &second_path, ObjectFormat::Sha1).await?;
        let competing_update = RefUpdate::new("refs/heads/main", None, Some(competing.target))?;
        let second = second
            .prepare_push(
                TransactionId::new(),
                vec![competing_update],
                Some(&competing.pack),
            )
            .await?;
        assert!(matches!(first.publish().await?, CommitStatus::Committed(_)));
        assert!(matches!(second.publish().await?, CommitStatus::Conflict(_)));

        fs::remove_dir_all(&first_path)?;
        let third_path = fixture.directory.path().join("third");
        let winner = Repository::open_native(&log, &third_path, ObjectFormat::Sha1).await?;
        assert_eq!(
            winner.refs().get(&b"refs/heads/main"[..]),
            Some(&fixture.target)
        );
        assert_ne!(
            winner.refs().get(&b"refs/heads/main"[..]),
            Some(&competing.target)
        );
        drop(winner);
        let current = Repository::open_native(&log, &first_path, ObjectFormat::Sha1).await?;
        faults.reset();
        let deletion = RefUpdate::new("refs/heads/main", Some(fixture.target), None)?;
        let delete = current
            .prepare_push(TransactionId::new(), vec![deletion], None)
            .await?;
        assert_eq!(faults.metrics().operation(Operation::Put).requests, 0);
        assert!(matches!(
            delete.publish().await?,
            CommitStatus::Committed(_)
        ));
        Ok(())
    }

    #[tokio::test]
    async fn stateless_fetch_accepts_an_old_reachable_tip() -> TestResult {
        let fixture = fixture(ObjectFormat::Sha1, b"one")?;
        let (log, _, _) = test_log("repository-stateless-fetch").await?;
        let old = fixture.target;
        let cache = fixture.directory.path().join("initial");
        let repository = Repository::open_native(&log, &cache, ObjectFormat::Sha1).await?;
        let push = repository
            .prepare_push(
                TransactionId::new(),
                vec![RefUpdate::new("refs/heads/main", None, Some(old))?],
                Some(&fixture.pack),
            )
            .await?;
        assert!(matches!(push.publish().await?, CommitStatus::Committed(_)));

        let source = fixture.directory.path().join("source");
        fs::write(source.join("file"), b"two")?;
        command(Some(&source), &["commit", "--quiet", "-am", "two"])?;
        let new = ObjectId::parse(
            ObjectFormat::Sha1,
            output(Some(&source), &["rev-parse", "HEAD"])?.trim(),
        )?;
        let pack = fixture.directory.path().join("update.pack");
        fs::write(
            &pack,
            command_output(Some(&source), &["pack-objects", "--all", "--stdout"])?.stdout,
        )?;
        let repository = Repository::open_native(
            &log,
            fixture.directory.path().join("update"),
            ObjectFormat::Sha1,
        )
        .await?;
        let push = repository
            .prepare_push(
                TransactionId::new(),
                vec![RefUpdate::new("refs/heads/main", Some(old), Some(new))?],
                Some(&pack),
            )
            .await?;
        assert!(matches!(push.publish().await?, CommitStatus::Committed(_)));

        let repository = Repository::open_native(
            &log,
            fixture.directory.path().join("fetch"),
            ObjectFormat::Sha1,
        )
        .await?;
        let output = fixture.directory.path().join("fetch.pack");
        repository.write_fetch_pack(&[old], &output).await?;
        let receiving = git::init(
            &fixture.directory.path().join("receiving"),
            ObjectFormat::Sha1,
        )?;
        let indexed = git::install_pack(&receiving, &output)?;
        assert!(
            indexed
                .objects
                .contains(&gix::hash::ObjectId::try_from(old)?)
        );

        let unreachable = ObjectId::from_bytes(ObjectFormat::Sha1, &[9; 20])?;
        assert!(
            repository
                .write_fetch_pack(
                    &[unreachable],
                    fixture.directory.path().join("invalid.pack")
                )
                .await
                .is_err()
        );
        Ok(())
    }

    #[tokio::test]
    async fn lost_response_resumes_without_restaging_the_pack() -> TestResult {
        let fixture = fixture(ObjectFormat::Sha1, b"resume")?;
        let (log, faults, backend) = test_log("repository-resume").await?;
        faults.reset();
        let cache = fixture.directory.path().join("pending");
        let repository = Repository::open_native(&log, &cache, ObjectFormat::Sha1).await?;
        let update = RefUpdate::new("refs/heads/main", None, Some(fixture.target))?;
        let push = repository
            .prepare_push(TransactionId::new(), vec![update], Some(&fixture.pack))
            .await?;
        let token = push.recovery_token().clone();
        let next_head_put = faults.metrics().operation(Operation::Put).requests + 2;
        faults.schedule(Failure {
            operation: Operation::Put,
            occurrence: next_head_put,
            phase: FailurePhase::After,
        });
        assert!(matches!(push.publish().await?, CommitStatus::Pending(_)));
        let staged_puts = pack_puts(&faults);

        fs::remove_dir_all(&cache)?;
        let reopened = Log::open(
            &backend,
            &LogId::new("repository-resume")?,
            Options::default(),
        )
        .await?;
        assert!(matches!(
            reopened.resume(&token).await?,
            object_log::Resolution::Committed(_)
        ));
        let _recovered = Repository::open_native(&reopened, &cache, ObjectFormat::Sha1).await?;
        assert_eq!(pack_puts(&faults), staged_puts);
        Ok(())
    }

    #[tokio::test]
    async fn checkpoint_preserves_conflict_and_pending_outcomes() -> TestResult {
        let conflict_fixture = fixture(ObjectFormat::Sha1, b"checkpoint")?;
        let (log, _, _) = test_log("repository-checkpoint-status").await?;
        let initial_path = conflict_fixture.directory.path().join("initial");
        let initial = Repository::open_native(&log, &initial_path, ObjectFormat::Sha1).await?;
        let push = initial
            .prepare_push(
                TransactionId::new(),
                vec![RefUpdate::new(
                    "refs/heads/main",
                    None,
                    Some(conflict_fixture.target),
                )?],
                Some(&conflict_fixture.pack),
            )
            .await?;
        assert!(matches!(push.publish().await?, CommitStatus::Committed(_)));

        let first = Repository::open_native(
            &log,
            conflict_fixture.directory.path().join("checkpoint-first"),
            ObjectFormat::Sha1,
        )
        .await?;
        let second = Repository::open_native(
            &log,
            conflict_fixture.directory.path().join("checkpoint-second"),
            ObjectFormat::Sha1,
        )
        .await?;
        assert!(matches!(
            first.checkpoint().await?,
            CheckpointStatus::Published(_)
        ));
        assert!(matches!(
            second.checkpoint().await?,
            CheckpointStatus::Conflict(_)
        ));

        let second_fixture = fixture(ObjectFormat::Sha1, b"pending-checkpoint")?;
        let (second_log, second_faults, _) = test_log("repository-checkpoint-pending").await?;
        let initial = Repository::open_native(
            &second_log,
            second_fixture.directory.path().join("initial"),
            ObjectFormat::Sha1,
        )
        .await?;
        let push = initial
            .prepare_push(
                TransactionId::new(),
                vec![RefUpdate::new(
                    "refs/heads/main",
                    None,
                    Some(second_fixture.target),
                )?],
                Some(&second_fixture.pack),
            )
            .await?;
        assert!(matches!(push.publish().await?, CommitStatus::Committed(_)));
        let checkpoint = Repository::open_native(
            &second_log,
            second_fixture.directory.path().join("checkpoint"),
            ObjectFormat::Sha1,
        )
        .await?;
        second_faults.reset();
        second_faults.schedule(Failure {
            operation: Operation::Put,
            occurrence: 2,
            phase: FailurePhase::After,
        });
        let CheckpointStatus::Pending(pending) = checkpoint.checkpoint().await? else {
            return Err("checkpoint response was not lost".into());
        };
        assert!(matches!(
            second_log.resolve_checkpoint(pending).await?,
            CheckpointResolution::Published(_)
        ));
        Ok(())
    }

    #[tokio::test]
    async fn refuses_nonempty_work_directory_without_deleting_it() -> TestResult {
        let directory = tempfile::tempdir()?;
        let cache = directory.path().join("cache");
        fs::create_dir(&cache)?;
        fs::write(cache.join("keep"), b"caller data")?;
        let (log, _, _) = test_log("repository-work-dir").await?;
        assert!(matches!(
            Repository::open_native(&log, &cache, ObjectFormat::Sha1).await,
            Err(Error::WorkDirectoryNotEmpty)
        ));
        assert_eq!(fs::read(cache.join("keep"))?, b"caller data");
        Ok(())
    }

    fn pack_puts(faults: &FaultStore) -> usize {
        faults
            .metrics()
            .events
            .iter()
            .filter(|event| {
                event.operation == Operation::Put
                    && (event.path.contains("/blobs/") || event.path.contains("/nodes/"))
            })
            .count()
    }

    fn copy_loose_object(source: &Path, target: &Path, id: ObjectId) -> TestResult {
        let id = id.to_string();
        let relative = Path::new("objects").join(&id[..2]).join(&id[2..]);
        let destination = target.join(&relative);
        fs::create_dir_all(
            destination
                .parent()
                .ok_or_else(|| "loose object has no parent directory".to_string())?,
        )?;
        fs::copy(source.join(".git").join(relative), destination)?;
        Ok(())
    }

    fn command(directory: Option<&Path>, args: &[&str]) -> TestResult {
        let result = command_output(directory, args)?;
        if result.status.success() {
            Ok(())
        } else {
            Err(String::from_utf8_lossy(&result.stderr).into_owned().into())
        }
    }

    fn output(directory: Option<&Path>, args: &[&str]) -> TestResult<String> {
        let result = command_output(directory, args)?;
        if result.status.success() {
            Ok(String::from_utf8(result.stdout)?)
        } else {
            Err(String::from_utf8_lossy(&result.stderr).into_owned().into())
        }
    }

    fn command_output(directory: Option<&Path>, args: &[&str]) -> TestResult<Output> {
        let mut command = Command::new("git");
        command
            .args(args)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_AUTHOR_NAME", "Object Log")
            .env("GIT_AUTHOR_EMAIL", "object-log@example.invalid")
            .env("GIT_AUTHOR_DATE", "2000-01-01T00:00:00Z")
            .env("GIT_COMMITTER_NAME", "Object Log")
            .env("GIT_COMMITTER_EMAIL", "object-log@example.invalid")
            .env("GIT_COMMITTER_DATE", "2000-01-01T00:00:00Z");
        if let Some(directory) = directory {
            command.current_dir(directory);
        }
        Ok(command.output()?)
    }
}
