#![doc = include_str!("../README.md")]
#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod format;
pub mod kv;
mod log;
mod materialize;
mod store;

#[cfg(any(test, feature = "test-util"))]
pub mod sim;

pub use log::{
    CheckpointRecord, CheckpointResolution, CheckpointStatus, CommitRecord, CommitStatus, Log,
    Options, ReferenceNode, Refresh, Resolution, View,
};
pub use materialize::{MaterializeError, Materialized, Materializer, materialize};
pub use store::{BackendCapabilities, BackendCapability, ScopedStore, ValidatedBackend};

/// Current durable object-log format version.
pub const FORMAT_VERSION: u32 = format::FORMAT_VERSION;

use bytes::Bytes;
use object_store::UpdateVersion;
use std::fmt;
use std::str::FromStr;
use uuid::Uuid;

const DIGEST_LEN: usize = 32;
const MAX_LOG_ID_LEN: usize = 128;

/// A BLAKE3 digest used for immutable object identity and integrity.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Digest([u8; DIGEST_LEN]);

impl Digest {
    /// Hashes bytes into a deterministic content identity.
    #[must_use]
    pub fn of(bytes: &[u8]) -> Self {
        Self(*blake3::hash(bytes).as_bytes())
    }

    /// Returns the raw BLAKE3 digest bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; DIGEST_LEN] {
        &self.0
    }
}

impl fmt::Display for Digest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        hex::encode(self.0).fmt(formatter)
    }
}

impl FromStr for Digest {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let decoded = hex::decode(value).map_err(|_| Error::InvalidDigest)?;
        let bytes = decoded.try_into().map_err(|_| Error::InvalidDigest)?;
        Ok(Self(bytes))
    }
}

/// A validated logical log identifier. It is not an object path.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct LogId(String);

impl LogId {
    /// Creates a validated log identifier.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidLogId`] when the identifier is empty, too long,
    /// or contains a byte that is not safe in the derived object namespace.
    pub fn new(value: impl Into<String>) -> Result<Self, Error> {
        let value = value.into();
        let valid = !value.is_empty()
            && value.len() <= MAX_LOG_ID_LEN
            && value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'));
        if !valid || value == "." || value == ".." {
            return Err(Error::InvalidLogId);
        }
        Ok(Self(value))
    }

    /// Returns the validated identifier text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for LogId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// A stable identity for one logical operation across conflict retries.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TransactionId(Uuid);

impl TransactionId {
    /// Creates a random operation identity.
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    /// Wraps a caller-supplied UUID as an operation identity.
    #[must_use]
    pub const fn from_uuid(value: Uuid) -> Self {
        Self(value)
    }

    /// Returns the underlying UUID.
    #[must_use]
    pub const fn as_uuid(&self) -> &Uuid {
        &self.0
    }
}

impl Default for TransactionId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for TransactionId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// The role of an immutable object.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ObjectKind {
    /// Caller-owned payload data.
    Blob,
    /// A canonical node with opaque payload and traversable child references.
    Node,
    /// An opaque state snapshot.
    Checkpoint,
}

/// A content-addressed immutable object reference.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectRef {
    pub(crate) kind: ObjectKind,
    pub(crate) digest: Digest,
    pub(crate) len: u64,
}

impl ObjectRef {
    /// Returns the object role.
    #[must_use]
    pub const fn kind(&self) -> ObjectKind {
        self.kind
    }

    /// Returns the deterministic content digest.
    #[must_use]
    pub const fn digest(&self) -> Digest {
        self.digest
    }

    /// Returns the encoded byte length.
    #[must_use]
    pub const fn len(&self) -> u64 {
        self.len
    }

    /// Reports whether the object has zero bytes.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }
}

/// An ordered reference to one committed WAL entry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommitRef {
    pub(crate) sequence: u64,
    pub(crate) transaction_id: TransactionId,
    pub(crate) digest: Digest,
    pub(crate) len: u64,
}

impl CommitRef {
    /// Returns the committed sequence number.
    #[must_use]
    pub const fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Returns the stable operation identity.
    #[must_use]
    pub const fn transaction_id(&self) -> TransactionId {
        self.transaction_id
    }

    /// Returns the deterministic WAL-entry digest.
    #[must_use]
    pub const fn digest(&self) -> Digest {
        self.digest
    }
}

/// A reference to a snapshot and its exact covered commit.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CheckpointRef {
    pub(crate) through_sequence: u64,
    pub(crate) through_commit: Digest,
    pub(crate) object: ObjectRef,
}

impl CheckpointRef {
    /// Returns the last sequence included in the snapshot.
    #[must_use]
    pub const fn through_sequence(&self) -> u64 {
        self.through_sequence
    }

    /// Returns the last commit digest included in the snapshot.
    #[must_use]
    pub const fn through_commit(&self) -> Digest {
        self.through_commit
    }

    /// Returns the immutable snapshot object.
    #[must_use]
    pub const fn object(&self) -> &ObjectRef {
        &self.object
    }
}

/// An opaque observed position used for conditional publication.
#[derive(Clone, Debug)]
pub struct Cursor {
    pub(crate) head: format::Head,
    pub(crate) version: UpdateVersion,
}

impl Cursor {
    /// Returns the count of published head updates.
    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.head.generation
    }

    /// Returns the sequence number for the next commit.
    #[must_use]
    pub const fn next_sequence(&self) -> u64 {
        self.head.next_sequence
    }

    /// Returns the current commit tip, if one exists.
    #[must_use]
    pub fn tip(&self) -> Option<Digest> {
        self.head.tip()
    }

    #[must_use]
    pub(crate) const fn storage_version(&self) -> &UpdateVersion {
        &self.version
    }
}

/// One exact commit candidate prepared against an observed cursor.
#[derive(Clone, Debug)]
pub struct PreparedCommit {
    pub(crate) cursor: Cursor,
    pub(crate) transaction_id: TransactionId,
    pub(crate) operation: Bytes,
    pub(crate) result: Bytes,
    pub(crate) objects: Vec<ObjectRef>,
}

impl PreparedCommit {
    /// Returns the cursor on which this candidate depends.
    #[must_use]
    pub const fn cursor(&self) -> &Cursor {
        &self.cursor
    }

    /// Returns the stable operation identity.
    #[must_use]
    pub const fn transaction_id(&self) -> TransactionId {
        self.transaction_id
    }

    /// Returns the opaque operation bytes.
    #[must_use]
    pub fn operation(&self) -> &Bytes {
        &self.operation
    }

    /// Returns the opaque recorded result bytes.
    #[must_use]
    pub fn result(&self) -> &Bytes {
        &self.result
    }

    /// Returns the immutable objects required by this commit.
    #[must_use]
    pub fn objects(&self) -> &[ObjectRef] {
        &self.objects
    }

    /// Encodes the exact candidate for recovery after process loss.
    ///
    /// Persist this token before calling [`Log::commit`]. The token contains
    /// the operation and result bytes. Protect it as application data.
    ///
    /// # Errors
    ///
    /// Returns an error when the candidate cannot use the canonical format.
    pub fn recovery_token(&self) -> Result<Bytes, Error> {
        format::encode_recovery_token(self)
    }
}

/// Evidence needed to resolve one uncertain commit publication.
#[derive(Clone, Debug)]
pub struct PendingCommit {
    pub(crate) prepared: PreparedCommit,
    pub(crate) commit_ref: CommitRef,
}

impl PendingCommit {
    /// Returns the stable operation identity.
    #[must_use]
    pub const fn transaction_id(&self) -> TransactionId {
        self.prepared.transaction_id
    }

    /// Encodes the exact candidate for recovery after process loss.
    ///
    /// # Errors
    ///
    /// Returns an error when the candidate cannot use the canonical format.
    pub fn recovery_token(&self) -> Result<Bytes, Error> {
        self.prepared.recovery_token()
    }
}

/// Evidence for one checkpoint publication with an uncertain outcome.
#[derive(Clone, Debug)]
pub struct PendingCheckpoint {
    pub(crate) view: View,
    pub(crate) through: CommitRef,
    pub(crate) checkpoint: CheckpointRef,
}

/// An object-log validation, limit, capability, or storage error.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// A logical log identifier is unsafe or outside its size limit.
    #[error("invalid log identifier")]
    InvalidLogId,
    /// A digest has invalid text or length.
    #[error("invalid digest")]
    InvalidDigest,
    /// Durable bytes or a supplied protocol value violate the contract.
    #[error("invalid object-log format: {0}")]
    InvalidFormat(String),
    /// Content does not match its declared digest, length, or immutable value.
    #[error("object data failed integrity verification")]
    CorruptObject,
    /// The backend does not provide a required storage behavior.
    #[error("backend lacks required capability: {0}")]
    UnsupportedBackend(&'static str),
    /// A configured byte or count limit was exceeded.
    #[error("configured limit exceeded: {0}")]
    LimitExceeded(&'static str),
    /// A writer used options that differ from the durable log options.
    #[error("configuration does not match the durable log contract: {0}")]
    ConfigurationMismatch(&'static str),
    /// The object-store operation failed.
    #[error("object store: {0}")]
    Store(#[from] object_store::Error),
}
