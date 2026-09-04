//! Git repository state and durable records for `object-log`.

#![deny(missing_docs, unsafe_code)]

#[cfg(feature = "native-oracle")]
mod format;
#[cfg(feature = "native-oracle")]
mod git;
#[allow(
    dead_code,
    reason = "the next Git storage tranche consumes this internal module"
)]
mod pack;
#[cfg(feature = "native-oracle")]
mod repository;
#[cfg(feature = "native-oracle")]
mod state;
#[cfg(feature = "native-oracle")]
mod storage;

#[cfg(feature = "native-oracle")]
pub use repository::{PreparedPush, Repository};

use std::{collections::BTreeMap, fmt};

use bytes::Bytes;
use minicbor::{Decode, Decoder, Encode};

/// A Git object identity algorithm.
#[derive(Clone, Copy, Debug, Decode, Encode, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[cbor(index_only)]
pub enum ObjectFormat {
    /// SHA-1.
    #[n(1)]
    Sha1,
    /// SHA-256.
    #[n(2)]
    Sha256,
}

/// A validated Git object ID.
#[derive(Clone, Copy, Debug, Encode, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[cbor(transparent)]
pub struct ObjectId(Digest);

#[derive(Clone, Copy, Debug, Decode, Encode, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[cbor(array)]
enum Digest {
    #[n(1)]
    Sha1(#[cbor(n(0), with = "minicbor::bytes")] [u8; 20]),
    #[n(2)]
    Sha256(#[cbor(n(0), with = "minicbor::bytes")] [u8; 32]),
}

impl ObjectId {
    /// Parses a hexadecimal ID.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidObjectId`] for an invalid or zero ID.
    pub fn parse(format: ObjectFormat, value: &str) -> Result<Self, Error> {
        let byte_len = match format {
            ObjectFormat::Sha1 => 20,
            ObjectFormat::Sha256 => 32,
        };
        if value.len() != byte_len * 2 {
            return Err(Error::InvalidObjectId);
        }
        let mut bytes = [0; 32];
        for (output, input) in bytes.iter_mut().zip(value.as_bytes().chunks_exact(2)) {
            *output = (nibble(input[0])? << 4) | nibble(input[1])?;
        }
        Self::from_bytes(format, &bytes[..byte_len])
    }

    pub(crate) fn from_bytes(format: ObjectFormat, bytes: &[u8]) -> Result<Self, Error> {
        if bytes.iter().all(|byte| *byte == 0) {
            return Err(Error::InvalidObjectId);
        }
        match format {
            ObjectFormat::Sha1 => bytes.try_into().map(Digest::Sha1),
            ObjectFormat::Sha256 => bytes.try_into().map(Digest::Sha256),
        }
        .map(Self)
        .map_err(|_| Error::InvalidObjectId)
    }

    /// Returns the ID algorithm.
    #[cfg(feature = "native-oracle")]
    pub(crate) const fn format(self) -> ObjectFormat {
        match self.0 {
            Digest::Sha1(_) => ObjectFormat::Sha1,
            Digest::Sha256(_) => ObjectFormat::Sha256,
        }
    }

    /// Returns the raw digest.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8] {
        match &self.0 {
            Digest::Sha1(value) => value,
            Digest::Sha256(value) => value,
        }
    }
}

impl<'bytes, Context> Decode<'bytes, Context> for ObjectId {
    fn decode(
        decoder: &mut Decoder<'bytes>,
        context: &mut Context,
    ) -> Result<Self, minicbor::decode::Error> {
        let id = Self(Digest::decode(decoder, context)?);
        (!id.as_bytes().iter().all(|byte| *byte == 0))
            .then_some(id)
            .ok_or_else(|| minicbor::decode::Error::message("zero Git object ID"))
    }
}

impl fmt::Display for ObjectId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_bytes()
            .iter()
            .try_for_each(|byte| write!(f, "{byte:02x}"))
    }
}

/// One atomic ref update.
#[derive(Clone, Debug, Decode, Encode, Eq, PartialEq)]
#[cbor(array)]
pub struct RefUpdate {
    #[cbor(n(0), with = "minicbor::bytes")]
    name: Vec<u8>,
    #[n(1)]
    expected: Option<ObjectId>,
    #[n(2)]
    target: Option<ObjectId>,
}

impl RefUpdate {
    /// Creates a validated update. `None` means absent or deleted.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid name or no change.
    pub fn new(
        name: impl Into<Bytes>,
        expected: Option<ObjectId>,
        target: Option<ObjectId>,
    ) -> Result<Self, Error> {
        let value = Self {
            name: name.into().to_vec(),
            expected,
            target,
        };
        value.validate()?;
        Ok(value)
    }

    fn validate(&self) -> Result<(), Error> {
        if !is_valid_ref_name(&self.name) {
            return Err(Error::InvalidRefName);
        }
        if self.expected == self.target {
            return Err(Error::InvalidRecord("ref update does not change the ref"));
        }
        Ok(())
    }
}

/// A complete byte-ordered ref map.
pub type RefSnapshot = BTreeMap<Vec<u8>, ObjectId>;

/// Invalid Git input or durable state.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// An object ID is invalid.
    #[error("invalid Git object ID")]
    InvalidObjectId,
    /// A ref name is not a supported valid branch or tag name.
    #[error("invalid Git branch or tag name")]
    InvalidRefName,
    /// A durable record is invalid.
    #[error("invalid Git record: {0}")]
    InvalidRecord(&'static str),
    /// Committed expected-old values do not match replay state.
    #[error("Git ref state diverged")]
    StateDiverged,
    /// A pack is malformed, corrupt, or outside a configured limit.
    #[error("invalid Git pack: {0}")]
    InvalidPack(String),
    /// A local repository uses unsupported state or configuration.
    #[cfg(feature = "native-oracle")]
    #[error("unsupported Git repository")]
    UnsupportedRepository,
    /// A ref or object target is invalid.
    #[error("invalid Git reference")]
    InvalidReference,
    /// A ref changed after its expected value was observed.
    #[error("Git reference changed")]
    StaleReference,
    /// A branch update does not descend from its current commit.
    #[error("Git branch update is not a fast-forward")]
    NonFastForward,
    /// An object reachable from a proposed ref is invalid.
    #[error("invalid reachable Git object graph: {0}")]
    InvalidObjectGraph(&'static str),
    /// The local work directory cannot be used as a new disposable cache.
    #[cfg(feature = "native-oracle")]
    #[error("Git work directory must not exist or must be empty")]
    WorkDirectoryNotEmpty,
    /// A local Git operation failed.
    #[cfg(feature = "native-oracle")]
    #[error("Git operation failed: {0}")]
    Git(String),
    /// Pack transfer or validation failed.
    #[error("Git pack storage failed: {0}")]
    PackStorage(String),
    /// An object-log operation failed.
    #[error(transparent)]
    ObjectLog(#[from] object_log::Error),
    /// A blocking local Git task stopped before it returned a result.
    #[cfg(feature = "native-oracle")]
    #[error("local Git task stopped")]
    BlockingTask,
}

#[cfg(feature = "native-oracle")]
impl From<git::Error> for Error {
    fn from(error: git::Error) -> Self {
        match error {
            git::Error::InvalidPack(message) | git::Error::Pack(message) => {
                Self::InvalidPack(message)
            }
            git::Error::NotBare | git::Error::UnsupportedRepository => Self::UnsupportedRepository,
            git::Error::InvalidReference => Self::InvalidReference,
            git::Error::StaleReference => Self::StaleReference,
            git::Error::NonFastForward => Self::NonFastForward,
            git::Error::InvalidObjectGraph(message) => Self::InvalidObjectGraph(message),
            git::Error::Repository(message) => Self::Git(message),
            git::Error::Io { path, source } => Self::Git(format!("{}: {source}", path.display())),
        }
    }
}

#[cfg(feature = "native-oracle")]
impl From<storage::Error> for Error {
    fn from(error: storage::Error) -> Self {
        match error {
            storage::Error::ObjectLog(error) => Self::ObjectLog(error),
            storage::Error::InvalidPack(message) => Self::InvalidPack(message.to_owned()),
            storage::Error::Io(error) => Self::PackStorage(error.to_string()),
        }
    }
}

fn nibble(byte: u8) -> Result<u8, Error> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(Error::InvalidObjectId),
    }
}

fn is_valid_ref_name(value: &[u8]) -> bool {
    (value.starts_with(b"refs/heads/") || value.starts_with(b"refs/tags/"))
        && std::str::from_utf8(value).is_ok()
        && gix_validate::reference::name(bstr::BStr::new(value)).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn public_values_reject_invalid_inputs() -> Result<(), Error> {
        let id = ObjectId::parse(ObjectFormat::Sha1, &"AB".repeat(20))?;
        assert_eq!(id.to_string(), "ab".repeat(20));
        assert!(ObjectId::parse(ObjectFormat::Sha1, &"00".repeat(20)).is_err());
        let zero = minicbor::to_vec(ObjectId(Digest::Sha1([0; 20])))
            .map_err(|_| Error::InvalidRecord("test encoding failed"))?;
        assert!(minicbor::decode::<ObjectId>(&zero).is_err());
        assert!(RefUpdate::new("refs/heads/main", None, Some(id)).is_ok());
        assert!(RefUpdate::new("refs/notes/x", None, Some(id)).is_err());
        assert!(RefUpdate::new(Bytes::from_static(b"refs/tags/\xff"), None, Some(id)).is_err());
        assert!(RefUpdate::new("", None, Some(id)).is_err());
        assert!(RefUpdate::new(Bytes::from_static(b"refs/heads/a\0b"), None, Some(id)).is_err());
        assert!(RefUpdate::new("refs/heads/main", Some(id), Some(id)).is_err());
        Ok(())
    }
}
