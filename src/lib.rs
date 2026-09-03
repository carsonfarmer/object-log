#![doc = include_str!("../README.md")]
#![forbid(unsafe_code)]

mod format;
pub mod log;
pub mod store;

#[cfg(any(test, feature = "test-util"))]
pub mod sim;

pub use log::{CheckpointStatus, CommitStatus, Log, Options, Refresh, Resolution, View};
pub use store::{BackendCapabilities, BackendCapability, ScopedStore};

/// Current durable object-log format version.
pub const FORMAT_VERSION: u32 = format::FORMAT_VERSION;

use bytes::Bytes;
use object_store::UpdateVersion;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;
use uuid::Uuid;

const DIGEST_LEN: usize = 32;
const MAX_LOG_ID_LEN: usize = 128;

/// A BLAKE3 digest used for immutable object identity and integrity.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Digest([u8; DIGEST_LEN]);

impl Digest {
    #[must_use]
    pub fn of(bytes: &[u8]) -> Self {
        Self(*blake3::hash(bytes).as_bytes())
    }

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
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
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
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct TransactionId(Uuid);

impl TransactionId {
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    #[must_use]
    pub const fn from_uuid(value: Uuid) -> Self {
        Self(value)
    }

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

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ObjectKind {
    Blob,
    Checkpoint,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ObjectRef {
    pub kind: ObjectKind,
    pub digest: Digest,
    pub len: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CommitRef {
    pub sequence: u64,
    pub transaction_id: TransactionId,
    pub digest: Digest,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CheckpointRef {
    pub through_sequence: u64,
    pub through_commit: Digest,
    pub object: ObjectRef,
}

/// An opaque observed position used for conditional publication.
#[derive(Clone, Debug)]
pub struct Cursor {
    pub(crate) generation: u64,
    pub(crate) next_sequence: u64,
    pub(crate) tip: Option<Digest>,
    pub(crate) version: UpdateVersion,
}

impl Cursor {
    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    #[must_use]
    pub const fn next_sequence(&self) -> u64 {
        self.next_sequence
    }

    #[must_use]
    pub const fn tip(&self) -> Option<Digest> {
        self.tip
    }

    #[must_use]
    pub const fn storage_version(&self) -> &UpdateVersion {
        &self.version
    }
}

#[derive(Clone, Debug)]
pub struct PreparedCommit {
    pub(crate) cursor: Cursor,
    pub(crate) transaction_id: TransactionId,
    pub(crate) operation: Bytes,
    pub(crate) result: Bytes,
    pub(crate) objects: Vec<ObjectRef>,
}

impl PreparedCommit {
    #[must_use]
    pub const fn cursor(&self) -> &Cursor {
        &self.cursor
    }

    #[must_use]
    pub const fn transaction_id(&self) -> TransactionId {
        self.transaction_id
    }

    #[must_use]
    pub fn operation(&self) -> &Bytes {
        &self.operation
    }

    #[must_use]
    pub fn result(&self) -> &Bytes {
        &self.result
    }

    #[must_use]
    pub fn objects(&self) -> &[ObjectRef] {
        &self.objects
    }
}

#[derive(Clone, Debug)]
pub struct PendingCommit {
    pub(crate) prepared: PreparedCommit,
    pub(crate) commit_ref: CommitRef,
}

impl PendingCommit {
    #[must_use]
    pub const fn prepared(&self) -> &PreparedCommit {
        &self.prepared
    }

    #[must_use]
    pub const fn commit_ref(&self) -> &CommitRef {
        &self.commit_ref
    }
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    #[error("invalid log identifier")]
    InvalidLogId,
    #[error("invalid digest")]
    InvalidDigest,
    #[error("invalid object-log format: {0}")]
    InvalidFormat(String),
    #[error("object data failed integrity verification")]
    CorruptObject,
    #[error("backend lacks required capability: {0}")]
    UnsupportedBackend(&'static str),
    #[error("configured limit exceeded: {0}")]
    LimitExceeded(&'static str),
    #[error("object store: {0}")]
    Store(#[from] object_store::Error),
}
