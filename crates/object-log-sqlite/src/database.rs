use std::collections::HashSet;
use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{LazyLock, Mutex};

use bytes::{Bytes, BytesMut};
use object_log::{
    CheckpointResolution, CheckpointStatus, CommitStatus, Log, ObjectKind, ObjectRef,
    PendingCheckpoint, PreparedCommit, Resolution, RetentionId, RetentionStatus, TransactionId,
    View,
};
use rusqlite::{Connection, MAIN_DB};
use uuid::Uuid;

use crate::connection::open as open_connection;
use crate::format::{Record, RecordKind};
use crate::policy::Policy;
use crate::wal::{self, WAL_FRAME_HEADER_BYTES, WAL_HEADER_BYTES, WalCapture, WalPosition};
use crate::{PAGE_SIZE, SqliteError};

#[derive(Debug)]
enum CacheState {
    Clean,
    Dirty,
    PendingCheckpoint(Box<PendingCheckpoint>),
}

static OPEN_CACHES: LazyLock<Mutex<HashSet<PathBuf>>> =
    LazyLock::new(|| Mutex::new(HashSet::new()));

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
pub struct StagedWrite {
    prepared: Box<PreparedCommit>,
    recovery_token: Bytes,
    wal: WalPosition,
    snapshot: bool,
}

/// Result of one `SQLite` write callback.
pub enum StageStatus {
    /// The callback changed no durable database page.
    ReadOnly(Bytes),
    /// The local transaction committed and needs object-log publication.
    Staged(StagedWrite),
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

impl StagedWrite {
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
}

impl Database {
    /// Opens a disposable local cache at `path` and rebuilds the durable view.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid durable data, an unusable cache path, an
    /// unsupported `SQLite` configuration, or an object-store failure.
    pub async fn open(log: Log, path: impl AsRef<Path>) -> Result<Self, SqliteError> {
        let path = path.as_ref().to_path_buf();
        let lease = CacheLease::acquire(&path)?;
        let view = log.load().await?;
        let (materialized, view) = materialize(&log, view).await?;
        write_cache(&path, &materialized)?;
        let connection = open_connection(&path)?;
        let captured = wal::committed(&connection, PAGE_SIZE as usize, &WalPosition::default())?;
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
        let _guard = self.policy.read();
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
    ) -> Result<StageStatus, SqliteError> {
        self.ensure_current().await?;
        let first = self.view.checkpoint().is_none() && self.view.tail().is_empty();
        let prior = self.wal.clone();
        let policy = self.policy.clone();
        let transaction = self.conn_mut()?.transaction()?;
        let result = {
            let _guard = policy.write();
            callback(&transaction)?
        };
        transaction.commit()?;
        self.state = CacheState::Dirty;
        let current = wal::committed(self.conn()?, PAGE_SIZE as usize, &prior)?;
        if current.bytes.is_empty() {
            self.state = CacheState::Clean;
            return Ok(StageStatus::ReadOnly(result));
        }

        let (record, objects, snapshot) = if first {
            let payload = backup(self.conn()?, &self.path)?;
            validate_snapshot(&payload)?;
            let (record, objects) = self.stage_snapshot(payload).await?;
            (record, objects, true)
        } else {
            let header = current
                .position
                .header
                .ok_or_else(|| SqliteError::InvalidWal("committed WAL has no header".into()))?;
            let (record, objects) = self
                .stage_wal(
                    current.bytes.clone(),
                    header,
                    prior.frames,
                    current.position.frames,
                )
                .await?;
            (record, objects, false)
        };
        let prepared =
            self.log
                .prepare(self.view.cursor(), transaction_id, record, result, objects)?;
        let recovery_token = prepared.recovery_token()?;
        Ok(StageStatus::Staged(StagedWrite {
            prepared: Box::new(prepared),
            recovery_token,
            wal: current.position,
            snapshot,
        }))
    }

    /// Publishes one staged transaction through the object-log head.
    ///
    /// # Errors
    ///
    /// Returns an error when immutable staging, validation, or publication
    /// fails before an uncertain result is possible.
    pub async fn publish(&mut self, staged: StagedWrite) -> Result<CommitStatus, SqliteError> {
        let next_wal = staged.wal;
        let snapshot = staged.snapshot;
        let status = self.log.commit(*staged.prepared).await?;
        if let CommitStatus::Committed(view) = &status {
            self.view = view.clone();
            if snapshot {
                truncate(self.conn()?)?;
                self.wal =
                    wal::committed(self.conn()?, PAGE_SIZE as usize, &WalPosition::default())?
                        .position;
            } else {
                self.wal = next_wal;
            }
            self.state = CacheState::Clean;
        }
        Ok(status)
    }

    /// Resolves one exact publication token without rerunning its callback.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid token, object-store failure, or failed
    /// cache recovery after a definite result.
    pub async fn resume(&mut self, token: &[u8]) -> Result<Resolution, SqliteError> {
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
        if let CacheState::PendingCheckpoint(_) = self.state {
            let CacheState::PendingCheckpoint(pending) =
                std::mem::replace(&mut self.state, CacheState::Dirty)
            else {
                return Err(SqliteError::DirtyCache);
            };
            let resolution = self.log.resolve_checkpoint(*pending).await?;
            return self.finish_checkpoint_resolution(resolution).await;
        }

        self.ensure_current().await?;
        let Some(through) = self.view.tail().last().cloned() else {
            return Ok(SqliteCheckpointStatus::Published(self.view.clone()));
        };
        let payload = backup(self.conn()?, &self.path)?;
        validate_snapshot(&payload)?;
        let (snapshot, objects) = self.stage_snapshot(payload).await?;
        self.state = CacheState::Dirty;
        match self
            .log
            .publish_checkpoint(&self.view, &through, snapshot, objects)
            .await?
        {
            CheckpointStatus::Published(view) => {
                self.finish_checkpoint(view.clone())?;
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
        let current = self.log.load().await?;
        if !matches!(self.state, CacheState::Clean)
            || current.cursor().generation() != self.view.cursor().generation()
        {
            self.rebuild(current).await?;
        }
        Ok(())
    }

    async fn finish_checkpoint_resolution(
        &mut self,
        resolution: CheckpointResolution,
    ) -> Result<SqliteCheckpointStatus, SqliteError> {
        match resolution {
            CheckpointResolution::Published(view) => {
                self.finish_checkpoint(view.clone())?;
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

    fn finish_checkpoint(&mut self, view: View) -> Result<(), SqliteError> {
        self.view = view;
        truncate(self.conn()?)?;
        self.wal =
            wal::committed(self.conn()?, PAGE_SIZE as usize, &WalPosition::default())?.position;
        self.state = CacheState::Clean;
        Ok(())
    }

    async fn rebuild(&mut self, view: View) -> Result<(), SqliteError> {
        let (materialized, view) = materialize(&self.log, view).await?;
        self.state = CacheState::Dirty;
        self.connection = None;
        write_cache(&self.path, &materialized)?;
        let connection = open_connection(&self.path)?;
        let captured = wal::committed(&connection, PAGE_SIZE as usize, &WalPosition::default())?;
        materialized.verify_wal(&captured)?;
        self.policy = Policy::install(&connection)?;
        self.connection = Some(connection);
        self.view = view;
        self.wal = captured.position;
        self.state = CacheState::Clean;
        Ok(())
    }

    async fn stage_snapshot(&self, payload: Bytes) -> Result<(Bytes, Vec<ObjectRef>), SqliteError> {
        self.stage_payload(payload, RecordKind::Snapshot, None)
            .await
    }

    async fn stage_wal(
        &self,
        payload: Bytes,
        header: [u8; WAL_HEADER_BYTES],
        prior: u32,
        current: u32,
    ) -> Result<(Bytes, Vec<ObjectRef>), SqliteError> {
        wal::validate_record(&header, &payload)?;
        self.stage_payload(payload, RecordKind::Wal, Some((header, prior, current)))
            .await
    }

    async fn stage_payload(
        &self,
        payload: Bytes,
        kind: RecordKind,
        boundary: Option<([u8; WAL_HEADER_BYTES], u32, u32)>,
    ) -> Result<(Bytes, Vec<ObjectRef>), SqliteError> {
        if payload.len() <= self.log.options().max_inline_operation_bytes {
            let inline =
                record(kind, payload.len(), Some(payload.clone()), 0, boundary)?.encode()?;
            if inline.len() <= self.log.options().max_inline_operation_bytes {
                return Ok((inline, Vec::new()));
            }
        }
        let unit = match kind {
            RecordKind::Snapshot => PAGE_SIZE as usize,
            RecordKind::Wal => PAGE_SIZE as usize + WAL_FRAME_HEADER_BYTES,
        };
        let chunk_size = self.log.options().max_object_bytes / unit * unit;
        if chunk_size == 0
            || payload.len().div_ceil(chunk_size) > self.log.options().max_object_refs
        {
            return Err(SqliteError::PayloadLimit);
        }
        let mut objects = Vec::with_capacity(payload.len().div_ceil(chunk_size));
        for chunk in payload.chunks(chunk_size) {
            objects.push(self.log.put_object(Bytes::copy_from_slice(chunk)).await?);
        }
        let descriptor = record(kind, payload.len(), None, objects.len(), boundary)?.encode()?;
        Ok((descriptor, objects))
    }

    fn conn(&self) -> Result<&Connection, SqliteError> {
        self.connection.as_ref().ok_or(SqliteError::DirtyCache)
    }

    fn conn_mut(&mut self) -> Result<&mut Connection, SqliteError> {
        self.connection.as_mut().ok_or(SqliteError::DirtyCache)
    }
}

fn record(
    kind: RecordKind,
    len: usize,
    inline: Option<Bytes>,
    chunks: usize,
    boundary: Option<([u8; WAL_HEADER_BYTES], u32, u32)>,
) -> Result<Record, SqliteError> {
    match (kind, boundary) {
        (RecordKind::Snapshot, None) => Ok(Record::snapshot(len, inline, chunks)),
        (RecordKind::Wal, Some((header, prior, current))) => {
            Ok(Record::wal(len, inline, chunks, header, prior, current))
        }
        _ => Err(SqliteError::InvalidRecord(
            "record kind and boundary do not match".into(),
        )),
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
    if view.checkpoint().is_none() && view.tail().is_empty() {
        let materialized = read_materialized(log, &view).await?;
        return Ok((materialized, view));
    }
    loop {
        let retention = RetentionId::new();
        let retained = acquire(log, view, retention).await?;
        let result = read_materialized(log, &retained).await;
        let released = release(log, retained.clone(), retention).await?;
        let materialized = result?;
        if same_history(&retained, &released) {
            return Ok((materialized, released));
        }
        view = released;
    }
}

async fn read_materialized(log: &Log, view: &View) -> Result<Materialized, SqliteError> {
    let checkpoint = log.read_checkpoint(view).await?;
    let tail = log.read_tail(view).await?;
    let mut snapshot = None;
    let mut wal = BytesMut::new();
    let mut header = None;
    let mut frames = 0;

    if let Some(checkpoint) = checkpoint {
        let descriptor = Record::decode(checkpoint.snapshot(), checkpoint.objects().len())?;
        if descriptor.kind != RecordKind::Snapshot {
            return Err(SqliteError::InvalidRecord(
                "checkpoint does not contain a snapshot".into(),
            ));
        }
        let payload = load_payload(log, view, &descriptor, checkpoint.objects()).await?;
        validate_snapshot(&payload)?;
        snapshot = Some(payload);
    }
    for commit in tail {
        let descriptor = Record::decode(commit.operation(), commit.objects().len())?;
        let payload = load_payload(log, view, &descriptor, commit.objects()).await?;
        match descriptor.kind {
            RecordKind::Snapshot if snapshot.is_none() && frames == 0 => {
                validate_snapshot(&payload)?;
                snapshot = Some(payload);
            }
            RecordKind::Wal if snapshot.is_some() => {
                let record_header = descriptor
                    .wal_header
                    .ok_or_else(|| SqliteError::InvalidRecord("WAL record has no header".into()))?;
                if descriptor.prior_mx_frame != Some(frames)
                    || header.is_some_and(|existing| existing != record_header)
                {
                    return Err(SqliteError::InvalidRecord(
                        "WAL records do not form one continuous epoch".into(),
                    ));
                }
                wal::validate_record(&record_header, &payload)?;
                if wal.is_empty() {
                    wal.extend_from_slice(&record_header);
                    header = Some(record_header);
                }
                wal.extend_from_slice(&payload);
                frames = descriptor.mx_frame.ok_or_else(|| {
                    SqliteError::InvalidRecord("WAL record has no current boundary".into())
                })?;
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
        wal: wal.freeze(),
        frames,
    })
}

async fn acquire(log: &Log, mut view: View, retention: RetentionId) -> Result<View, SqliteError> {
    loop {
        match log.retain(&view, retention).await? {
            RetentionStatus::Applied(retained) => return Ok(retained),
            RetentionStatus::Conflict(current) => view = current,
            RetentionStatus::Pending => view = log.load().await?,
            RetentionStatus::ActiveCollection(_) => return Err(SqliteError::CollectionActive),
        }
    }
}

async fn release(log: &Log, mut view: View, retention: RetentionId) -> Result<View, SqliteError> {
    loop {
        match log.release_retention(&view, retention).await? {
            RetentionStatus::Applied(released) => return Ok(released),
            RetentionStatus::Conflict(current) => view = current,
            RetentionStatus::Pending => view = log.load().await?,
            RetentionStatus::ActiveCollection(_) => {
                return Err(SqliteError::InvalidRecord(
                    "collection blocked a retention release".into(),
                ));
            }
        }
    }
}

fn same_history(left: &View, right: &View) -> bool {
    left.checkpoint() == right.checkpoint() && left.tail() == right.tail()
}

async fn load_payload(
    log: &Log,
    view: &View,
    record: &Record,
    objects: &[ObjectRef],
) -> Result<Bytes, SqliteError> {
    if let Some(payload) = &record.inline {
        return Ok(payload.clone());
    }
    let unit = match record.kind {
        RecordKind::Snapshot => PAGE_SIZE as usize,
        RecordKind::Wal => PAGE_SIZE as usize + WAL_FRAME_HEADER_BYTES,
    };
    let mut payload = BytesMut::with_capacity(record.payload_len);
    for object in objects {
        if object.kind() != ObjectKind::Blob {
            return Err(SqliteError::InvalidRecord(
                "record chunk is not a blob".into(),
            ));
        }
        let chunk = log.read_object(view, object).await?;
        if chunk.is_empty() || !chunk.len().is_multiple_of(unit) {
            return Err(SqliteError::InvalidRecord(
                "record chunk splits a page or WAL frame".into(),
            ));
        }
        payload.extend_from_slice(&chunk);
    }
    if payload.len() != record.payload_len {
        return Err(SqliteError::InvalidRecord(
            "record chunks do not match the declared length".into(),
        ));
    }
    Ok(payload.freeze())
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
    ] {
        match fs::remove_file(target) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

fn backup(conn: &Connection, database: &Path) -> Result<Bytes, SqliteError> {
    let target = sidecar(database, &format!(".snapshot-{}", Uuid::new_v4().simple()));
    let cleanup = SnapshotFile(target);
    conn.backup(MAIN_DB, &cleanup.0, None)?;
    Ok(Bytes::from(fs::read(&cleanup.0)?))
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

struct CacheLease(PathBuf);

impl CacheLease {
    fn acquire(path: &Path) -> Result<Self, SqliteError> {
        let path = std::path::absolute(path)?;
        if !OPEN_CACHES
            .lock()
            .map_err(|_| SqliteError::CacheRegistry)?
            .insert(path.clone())
        {
            return Err(SqliteError::CacheInUse);
        }
        Ok(Self(path))
    }
}

impl Drop for CacheLease {
    fn drop(&mut self) {
        if let Ok(mut paths) = OPEN_CACHES.lock() {
            paths.remove(&self.0);
        }
    }
}
