use std::ffi::OsString;
use std::fs::{self, File, OpenOptions, TryLockError};
use std::io::Read;
use std::path::{Path, PathBuf};

use bytes::Bytes;
use futures::{StreamExt, TryStreamExt, stream};
use object_log::{
    CheckpointResolution, CheckpointStatus, CommitStatus, Error as LogError, Log, ObjectKind,
    ObjectRef, PendingCheckpoint, PreparedCommit, Refresh, Resolution, StagedObject, TransactionId,
    View,
};
use rusqlite::{Connection, MAIN_DB};
use uuid::Uuid;

use crate::connection::open as open_connection;
use crate::format::Record;
use crate::policy::Policy;
use crate::wal::{self, WAL_FRAME_HEADER_BYTES, WAL_HEADER_BYTES, WalCapture, WalPosition};
use crate::{PAGE_SIZE, SqliteError};

const WAL_FRAME_BYTES: usize = PAGE_SIZE as usize + WAL_FRAME_HEADER_BYTES;
const MAX_CONCURRENT_OBJECTS: usize = 32;

#[derive(Debug)]
enum CacheState {
    Clean,
    Dirty,
    PendingCheckpoint(Box<PendingCheckpoint>),
}

/// One live `SQLite` cache backed by an object log.
pub struct Database {
    log: Log,
    path: PathBuf,
    connection: Option<Connection>,
    policy: Policy,
    view: View,
    wal: WalPosition,
    state: CacheState,
    _lease: CacheLease,
}

/// One locally committed write ready for object-log publication.
pub struct StagedWrite<'a> {
    database: &'a mut Database,
    prepared: Box<PreparedCommit>,
    recovery_token: Bytes,
    wal: Option<WalPosition>,
}

/// Result of one `SQLite` write callback.
pub enum StageStatus<'a> {
    /// The callback changed no durable database page.
    ReadOnly(Bytes),
    /// The local transaction committed and needs object-log publication.
    Staged(StagedWrite<'a>),
}

/// Result of one database checkpoint attempt.
pub enum SqliteCheckpointStatus {
    /// The snapshot is durable and the local WAL is empty.
    Published(View),
    /// Another head update rejected this snapshot.
    Conflict(View),
    /// The publication result is not available yet.
    Pending,
    /// The object log no longer has enough evidence to classify the attempt.
    Expired(View),
}

impl StagedWrite<'_> {
    /// Returns the result bytes recorded with this transaction.
    #[must_use]
    pub fn result(&self) -> &Bytes {
        self.prepared.result()
    }

    /// Returns the exact object-log recovery token.
    #[must_use]
    pub const fn recovery_token(&self) -> &Bytes {
        &self.recovery_token
    }

    /// Publishes this write through its originating database.
    ///
    /// # Errors
    ///
    /// Returns an error when immutable staging, validation, publication, or
    /// recovery fails.
    pub async fn publish(self) -> Result<CommitStatus, SqliteError> {
        let Self {
            database,
            prepared,
            recovery_token: _,
            wal,
        } = self;
        let source_generation = prepared.cursor().generation();
        let status = database.log.commit(*prepared).await?;
        if let CommitStatus::Committed(view) = &status {
            if is_next_generation(source_generation, view) {
                database.accept_local(view.clone(), wal)?;
            } else {
                database.rebuild(view.clone()).await?;
            }
        }
        Ok(status)
    }
}

impl Database {
    /// Opens a disposable local cache at `path` and rebuilds the durable view.
    /// The cache and its advisory lock must be on a local filesystem.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid durable data, an unusable cache path, an
    /// unsupported `SQLite` configuration, or an object-store failure.
    pub async fn open(log: Log, path: impl AsRef<Path>) -> Result<Self, SqliteError> {
        validate_options(&log)?;
        let (path, lease) = CacheLease::acquire(path.as_ref())?;
        let view = log.load().await?;
        let (materialized, view) = materialize(&log, view).await?;
        write_cache(&path, &materialized)?;
        let connection = open_connection(&path)?;
        let captured =
            wal::committed(&connection, &WalPosition::default(), materialized.wal.len())?;
        materialized.verify_wal(&captured)?;
        let policy = Policy::install(&connection)?;
        Ok(Self {
            log,
            path,
            connection: Some(connection),
            policy,
            view,
            wal: captured.position,
            state: CacheState::Clean,
            _lease: lease,
        })
    }

    /// Runs one read callback against the latest durable database view.
    ///
    /// # Errors
    ///
    /// Returns an error when refresh, recovery, policy enforcement, or the
    /// callback fails.
    pub async fn read<T>(
        &mut self,
        callback: impl FnOnce(&Connection) -> rusqlite::Result<T>,
    ) -> Result<T, SqliteError> {
        self.ensure_current().await?;
        let _guard = self.policy.read(self.conn()?);
        Ok(callback(self.conn()?)?)
    }

    /// Commits one local `SQLite` transaction and stages its durable record.
    ///
    /// The callback runs once. A later publication conflict never reruns it.
    ///
    /// # Errors
    ///
    /// Returns an error when refresh, recovery, the callback, WAL validation,
    /// object staging, or record preparation fails.
    pub async fn stage_write(
        &mut self,
        transaction_id: TransactionId,
        callback: impl FnOnce(&rusqlite::Transaction<'_>) -> rusqlite::Result<Bytes>,
    ) -> Result<StageStatus<'_>, SqliteError> {
        self.ensure_current().await?;
        let _preflight = self.log.prepare(
            self.view.cursor(),
            transaction_id,
            Bytes::new(),
            Bytes::new(),
            Vec::new(),
        )?;
        let first = self.view.checkpoint().is_none() && self.view.tail().is_empty();
        let prior = self.wal;
        let policy = &self.policy;
        let transaction = self
            .connection
            .as_mut()
            .ok_or(SqliteError::DirtyCache)?
            .transaction()?;
        self.state = CacheState::Dirty;
        let callback_result = {
            let _guard = policy.write(&transaction);
            callback(&transaction)
        };
        let result = match callback_result {
            Ok(result) => result,
            Err(callback_error) => {
                transaction.rollback()?;
                self.state = CacheState::Clean;
                return Err(callback_error.into());
            }
        };
        transaction.commit()?;
        let (record, objects, wal) = if first {
            if wal::committed_frames(self.conn()?)? == 0 {
                self.state = CacheState::Clean;
                return Ok(StageStatus::ReadOnly(result));
            }
            let payload = backup(
                self.conn()?,
                &self.path,
                payload_limit(&self.log, PAGE_SIZE as usize)?,
            )?;
            let (record, objects) = self.stage_snapshot(payload).await?;
            (record, objects, None)
        } else {
            let current = wal::committed(
                self.conn()?,
                &prior,
                payload_limit(&self.log, WAL_FRAME_BYTES)?,
            )?;
            if current.bytes.is_empty() {
                self.state = CacheState::Clean;
                return Ok(StageStatus::ReadOnly(result));
            }
            let header = current
                .position
                .header
                .ok_or_else(|| SqliteError::InvalidWal("committed WAL has no header".into()))?;
            let (record, objects) = self
                .stage_wal(current.bytes, header, prior.frames, current.position.frames)
                .await?;
            (record, objects, Some(current.position))
        };
        let prepared =
            self.log
                .prepare(self.view.cursor(), transaction_id, record, result, objects)?;
        let recovery_token = prepared.recovery_token()?;
        Ok(StageStatus::Staged(StagedWrite {
            database: self,
            prepared: Box::new(prepared),
            recovery_token,
            wal,
        }))
    }

    /// Resolves one exact publication token without rerunning its callback.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid token, object-store failure, or failed
    /// cache recovery after a definite result.
    pub async fn resume(&mut self, token: &[u8]) -> Result<Resolution, SqliteError> {
        self.state = CacheState::Dirty;
        let resolution = self.log.resume(token).await?;
        let view = match &resolution {
            Resolution::Committed(view)
            | Resolution::NotCommitted(view)
            | Resolution::Expired(view) => Some(view.clone()),
            Resolution::StillPending(_) => None,
        };
        if let Some(view) = view {
            self.rebuild(view).await?;
        }
        Ok(resolution)
    }

    /// Publishes a complete database snapshot through the current tail.
    ///
    /// A repeated call resolves an uncertain in-process checkpoint before it
    /// starts new work. Success truncates the local WAL only after the snapshot
    /// is durable.
    ///
    /// # Errors
    ///
    /// Returns an error for backup, object staging, publication, resolution,
    /// or confirmed local truncation failure.
    pub async fn checkpoint(&mut self) -> Result<SqliteCheckpointStatus, SqliteError> {
        if let CacheState::PendingCheckpoint(pending) = &self.state {
            let resolution = self.log.resolve_checkpoint(*pending.clone()).await?;
            return self.finish_checkpoint_resolution(resolution).await;
        }

        self.ensure_current().await?;
        let Some(through) = self.view.tail().last().cloned() else {
            return Ok(SqliteCheckpointStatus::Published(self.view.clone()));
        };
        let payload = backup(
            self.conn()?,
            &self.path,
            payload_limit(&self.log, PAGE_SIZE as usize)?,
        )?;
        let (snapshot, objects) = self.stage_snapshot(payload).await?;
        self.state = CacheState::Dirty;
        let source_generation = self.view.cursor().generation();
        match self
            .log
            .publish_checkpoint(&self.view, &through, snapshot, objects)
            .await?
        {
            CheckpointStatus::Published(view) => {
                if is_next_generation(source_generation, &view) {
                    self.accept_local(view.clone(), None)?;
                } else {
                    self.rebuild(view.clone()).await?;
                }
                Ok(SqliteCheckpointStatus::Published(view))
            }
            CheckpointStatus::Conflict(view) => Ok(SqliteCheckpointStatus::Conflict(view)),
            CheckpointStatus::Pending(pending) => {
                self.state = CacheState::PendingCheckpoint(Box::new(pending));
                Ok(SqliteCheckpointStatus::Pending)
            }
        }
    }

    async fn ensure_current(&mut self) -> Result<(), SqliteError> {
        match self.log.refresh(self.view.cursor()).await? {
            Refresh::NotModified if matches!(self.state, CacheState::Clean) => {}
            Refresh::NotModified => self.rebuild(self.view.clone()).await?,
            Refresh::Updated(current) => self.rebuild(*current).await?,
        }
        Ok(())
    }

    async fn finish_checkpoint_resolution(
        &mut self,
        resolution: CheckpointResolution,
    ) -> Result<SqliteCheckpointStatus, SqliteError> {
        match resolution {
            CheckpointResolution::Published(view) => {
                self.rebuild(view.clone()).await?;
                Ok(SqliteCheckpointStatus::Published(view))
            }
            CheckpointResolution::NotPublished(view) => {
                self.rebuild(view.clone()).await?;
                Ok(SqliteCheckpointStatus::Conflict(view))
            }
            CheckpointResolution::StillPending(pending) => {
                self.state = CacheState::PendingCheckpoint(Box::new(pending));
                Ok(SqliteCheckpointStatus::Pending)
            }
            CheckpointResolution::Expired(view) => {
                self.rebuild(view.clone()).await?;
                Ok(SqliteCheckpointStatus::Expired(view))
            }
        }
    }

    fn accept_local(
        &mut self,
        view: View,
        wal_position: Option<WalPosition>,
    ) -> Result<(), SqliteError> {
        if let Some(wal_position) = wal_position {
            self.wal = wal_position;
        } else {
            truncate(self.conn()?)?;
            self.wal = wal::committed(self.conn()?, &WalPosition::default(), 0)?.position;
        }
        self.view = view;
        self.state = CacheState::Clean;
        Ok(())
    }

    async fn rebuild(&mut self, view: View) -> Result<(), SqliteError> {
        let (materialized, view) = materialize(&self.log, view).await?;
        self.state = CacheState::Dirty;
        self.connection = None;
        write_cache(&self.path, &materialized)?;
        let connection = open_connection(&self.path)?;
        let captured =
            wal::committed(&connection, &WalPosition::default(), materialized.wal.len())?;
        materialized.verify_wal(&captured)?;
        self.policy = Policy::install(&connection)?;
        self.connection = Some(connection);
        self.view = view;
        self.wal = captured.position;
        self.state = CacheState::Clean;
        Ok(())
    }

    async fn stage_snapshot(
        &self,
        payload: Bytes,
    ) -> Result<(Bytes, Vec<StagedObject>), SqliteError> {
        self.stage_payload(payload, PAGE_SIZE as usize, Record::snapshot)
            .await
    }

    async fn stage_wal(
        &self,
        payload: Bytes,
        header: [u8; WAL_HEADER_BYTES],
        prior: u32,
        current: u32,
    ) -> Result<(Bytes, Vec<StagedObject>), SqliteError> {
        self.stage_payload(payload, WAL_FRAME_BYTES, |len, inline, chunks| {
            Record::wal(len, inline, chunks, header, prior, current)
        })
        .await
    }

    async fn stage_payload<F>(
        &self,
        payload: Bytes,
        unit: usize,
        record: F,
    ) -> Result<(Bytes, Vec<StagedObject>), SqliteError>
    where
        F: Fn(usize, Option<Bytes>, usize) -> Result<Record, SqliteError>,
    {
        if payload.len() <= self.log.options().max_inline_operation_bytes {
            let inline = record(payload.len(), Some(payload.clone()), 0)?.encode()?;
            if inline.len() <= self.log.options().max_inline_operation_bytes {
                return Ok((inline, Vec::new()));
            }
        }
        let chunk_size = self.log.options().max_object_bytes / unit * unit;
        if chunk_size == 0
            || payload.len().div_ceil(chunk_size) > self.log.options().max_object_refs
        {
            return Err(SqliteError::PayloadLimit);
        }
        let payload_len = payload.len();
        let chunks = payload_len.div_ceil(chunk_size);
        let mut objects = Vec::new();
        objects
            .try_reserve_exact(chunks)
            .map_err(|_| SqliteError::PayloadLimit)?;
        let uploads = (0..payload_len).step_by(chunk_size).map(|offset| {
            let end = offset.saturating_add(chunk_size).min(payload_len);
            self.log
                .put_object(self.view.cursor(), payload.slice(offset..end))
        });
        let objects = stream::iter(uploads)
            .buffered(MAX_CONCURRENT_OBJECTS)
            .try_fold(objects, |mut objects, object| async move {
                objects.push(object);
                Ok(objects)
            })
            .await?;
        let descriptor = record(payload_len, None, objects.len())?.encode()?;
        Ok((descriptor, objects))
    }

    fn conn(&self) -> Result<&Connection, SqliteError> {
        self.connection.as_ref().ok_or(SqliteError::DirtyCache)
    }
}

struct Materialized {
    snapshot: Option<Bytes>,
    wal: Bytes,
    frames: u32,
}

impl Materialized {
    fn verify_wal(&self, actual: &WalCapture) -> Result<(), SqliteError> {
        let expected_header = self.wal.get(..WAL_HEADER_BYTES);
        let expected_frames = self.wal.get(WAL_HEADER_BYTES..).unwrap_or_default();
        if actual.position.frames != self.frames
            || actual.position.header.as_ref().map(<[u8; 32]>::as_slice) != expected_header
            || actual.bytes.as_ref() != expected_frames
        {
            return Err(SqliteError::InvalidWal(
                "SQLite did not open the reconstructed WAL boundary".into(),
            ));
        }
        Ok(())
    }
}

async fn materialize(log: &Log, mut view: View) -> Result<(Materialized, View), SqliteError> {
    loop {
        let materialized = match read_materialized(log, &view).await {
            Ok(materialized) => materialized,
            Err(SqliteError::Log(LogError::ViewExpired)) => {
                view = log.load().await?;
                continue;
            }
            Err(error) => return Err(error),
        };
        let current = log.load().await?;
        if view.checkpoint() == current.checkpoint() && view.tail() == current.tail() {
            return Ok((materialized, current));
        }
        view = current;
    }
}

async fn read_materialized(log: &Log, view: &View) -> Result<Materialized, SqliteError> {
    let checkpoint = log.read_checkpoint(view).await?;
    let tail = log.read_tail(view).await?;
    let mut snapshot = None;
    let mut wal = Vec::new();
    let mut position = WalPosition::default();

    if let Some(checkpoint) = checkpoint {
        let descriptor = Record::decode(checkpoint.snapshot(), checkpoint.objects().len())?;
        if !matches!(
            &descriptor,
            Record::SnapshotInline(_) | Record::SnapshotChunks { .. }
        ) {
            return Err(SqliteError::InvalidRecord(
                "checkpoint does not contain a snapshot".into(),
            ));
        }
        let payload = load_payload(
            log,
            view,
            &descriptor,
            checkpoint.objects(),
            PAGE_SIZE as usize,
        )
        .await?;
        validate_snapshot(&payload)?;
        snapshot = Some(payload);
    }
    for commit in tail {
        let descriptor = Record::decode(commit.operation(), commit.objects().len())?;
        match &descriptor {
            Record::SnapshotInline(_) | Record::SnapshotChunks { .. }
                if snapshot.is_none() && position.frames == 0 =>
            {
                let payload =
                    load_payload(log, view, &descriptor, commit.objects(), PAGE_SIZE as usize)
                        .await?;
                validate_snapshot(&payload)?;
                snapshot = Some(payload);
            }
            Record::WalInline { header, prior, .. } | Record::WalChunks { header, prior, .. }
                if snapshot.is_some() =>
            {
                if *prior != position.frames {
                    return Err(SqliteError::InvalidRecord(
                        "WAL records do not form one continuous epoch".into(),
                    ));
                }
                let payload =
                    load_payload(log, view, &descriptor, commit.objects(), WAL_FRAME_BYTES).await?;
                position = wal::validate_record(header, &payload, position)?;
                if wal.is_empty() {
                    append(&mut wal, header)?;
                }
                append(&mut wal, &payload)?;
            }
            _ => {
                return Err(SqliteError::InvalidRecord(
                    "snapshot and WAL records are out of order".into(),
                ));
            }
        }
    }
    if snapshot.is_none()
        && (!wal.is_empty() || view.checkpoint().is_some() || !view.tail().is_empty())
    {
        return Err(SqliteError::InvalidRecord(
            "durable history has no database snapshot".into(),
        ));
    }
    Ok(Materialized {
        snapshot,
        wal: Bytes::from(wal),
        frames: position.frames,
    })
}

async fn load_payload(
    log: &Log,
    view: &View,
    record: &Record,
    objects: &[ObjectRef],
    unit: usize,
) -> Result<Bytes, SqliteError> {
    let (payload_len, inline) = record.payload()?;
    if let Some(payload) = inline {
        return Ok(payload.clone());
    }
    let options = log.options();
    if objects.len() > options.max_object_refs {
        return Err(SqliteError::PayloadLimit);
    }
    let mut declared_len = 0_usize;
    for object in objects {
        if object.kind() != ObjectKind::Blob {
            return Err(SqliteError::InvalidRecord(
                "record chunk is not a blob".into(),
            ));
        }
        let object_len = usize::try_from(object.len())?;
        if object_len == 0
            || object_len > options.max_object_bytes
            || !object_len.is_multiple_of(unit)
        {
            return Err(SqliteError::InvalidRecord(
                "record has an invalid declared chunk length".into(),
            ));
        }
        declared_len = declared_len
            .checked_add(object_len)
            .ok_or(SqliteError::PayloadLimit)?;
    }
    if declared_len != payload_len {
        return Err(SqliteError::InvalidRecord(
            "record chunks do not match the declared length".into(),
        ));
    }
    let mut payload = Vec::new();
    payload
        .try_reserve_exact(payload_len)
        .map_err(|_| SqliteError::PayloadLimit)?;
    let payload = stream::iter(objects)
        .map(|object| log.read_object(view, object))
        .buffered(MAX_CONCURRENT_OBJECTS)
        .try_fold(payload, |mut payload, chunk| async move {
            payload.extend_from_slice(&chunk);
            Ok(payload)
        })
        .await?;
    Ok(Bytes::from(payload))
}

fn write_cache(path: &Path, materialized: &Materialized) -> Result<(), SqliteError> {
    remove_cache(path)?;
    if let Some(snapshot) = &materialized.snapshot {
        fs::write(path, snapshot)?;
    }
    if !materialized.wal.is_empty() {
        fs::write(sidecar(path, "-wal"), &materialized.wal)?;
    }
    Ok(())
}

fn remove_cache(path: &Path) -> Result<(), SqliteError> {
    for target in [
        path.to_path_buf(),
        sidecar(path, "-wal"),
        sidecar(path, "-shm"),
        sidecar(path, "-journal"),
    ] {
        match fs::remove_file(target) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

fn backup(conn: &Connection, database: &Path, max_bytes: usize) -> Result<Bytes, SqliteError> {
    let target = sidecar(database, &format!(".snapshot-{}", Uuid::new_v4().simple()));
    let cleanup = SnapshotFile(target);
    conn.backup(MAIN_DB, &cleanup.0, None)?;
    let len =
        usize::try_from(fs::metadata(&cleanup.0)?.len()).map_err(|_| SqliteError::PayloadLimit)?;
    if len > max_bytes {
        return Err(SqliteError::PayloadLimit);
    }
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(len)
        .map_err(|_| SqliteError::PayloadLimit)?;
    bytes.resize(len, 0);
    File::open(&cleanup.0)?.read_exact(&mut bytes)?;
    Ok(Bytes::from(bytes))
}

struct SnapshotFile(PathBuf);

impl Drop for SnapshotFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

fn truncate(conn: &Connection) -> Result<(), SqliteError> {
    let (busy, _, remaining): (i64, i64, i64) =
        conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })?;
    if busy != 0 || remaining != 0 {
        return Err(SqliteError::InvalidWal(
            "SQLite could not truncate the published WAL".into(),
        ));
    }
    Ok(())
}

fn validate_snapshot(bytes: &[u8]) -> Result<(), SqliteError> {
    let page_size = u16::try_from(PAGE_SIZE)?.to_be_bytes();
    if bytes.len() < PAGE_SIZE as usize
        || !bytes.len().is_multiple_of(PAGE_SIZE as usize)
        || bytes.get(..16) != Some(b"SQLite format 3\0")
        || bytes.get(16..18) != Some(page_size.as_slice())
    {
        return Err(SqliteError::InvalidSnapshot);
    }
    Ok(())
}

fn sidecar(path: &Path, suffix: &str) -> PathBuf {
    let mut value: OsString = path.as_os_str().to_owned();
    value.push(suffix);
    PathBuf::from(value)
}

struct CacheLease {
    _file: File,
}

impl CacheLease {
    fn acquire(path: &Path) -> Result<(PathBuf, Self), SqliteError> {
        let name = path.file_name().ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "cache path has no file name",
            )
        })?;
        let parent = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(parent)?;
        let path = fs::canonicalize(parent)?.join(name);
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(sidecar(&path, "-lock"))?;
        file.try_lock().map_err(|error| match error {
            TryLockError::WouldBlock => SqliteError::CacheInUse,
            TryLockError::Error(error) => SqliteError::Io(error),
        })?;
        Ok((path, Self { _file: file }))
    }
}

fn is_next_generation(source: u64, current: &View) -> bool {
    source
        .checked_add(1)
        .is_some_and(|generation| current.cursor().generation() == generation)
}

fn validate_options(log: &Log) -> Result<(), SqliteError> {
    let options = log.options();
    let descriptors = [
        Record::snapshot(PAGE_SIZE as usize, None, 1)?.encode()?,
        Record::wal(WAL_FRAME_BYTES, None, 1, [0; WAL_HEADER_BYTES], 0, 1)?.encode()?,
    ];
    if options.max_object_refs == 0
        || options.max_object_bytes < WAL_FRAME_BYTES
        || payload_limit(log, PAGE_SIZE as usize).is_err()
        || payload_limit(log, WAL_FRAME_BYTES).is_err()
        || descriptors.iter().any(|descriptor| {
            descriptor.len() > options.max_inline_operation_bytes
                || descriptor.len() > options.max_commit_bytes
                || descriptor.len() > options.max_checkpoint_bytes
        })
    {
        return Err(SqliteError::PayloadLimit);
    }
    Ok(())
}

fn payload_limit(log: &Log, unit: usize) -> Result<usize, SqliteError> {
    let options = log.options();
    let inline = options.max_inline_operation_bytes / unit * unit;
    let external = (options.max_object_bytes / unit * unit)
        .checked_mul(options.max_object_refs)
        .ok_or(SqliteError::PayloadLimit)?;
    Ok(inline.max(external))
}

fn append(target: &mut Vec<u8>, bytes: &[u8]) -> Result<(), SqliteError> {
    target
        .try_reserve(bytes.len())
        .map_err(|_| SqliteError::PayloadLimit)?;
    target.extend_from_slice(bytes);
    Ok(())
}
