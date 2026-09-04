//! Git repository state and durable records for `object-log`.

#![deny(missing_docs, unsafe_code)]

#[allow(dead_code, reason = "used by the pending repository adapter")]
mod format;
#[allow(dead_code, reason = "used by the pending repository adapter")]
mod git;
#[allow(dead_code, reason = "used by the pending repository adapter")]
mod state;
#[allow(dead_code, reason = "used by the pending repository adapter")]
mod storage;

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
        if self.name.is_empty() || self.name.len() > 1_024 || self.name.contains(&0) {
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
    /// A ref name is empty, too long, or contains a null byte.
    #[error("invalid Git ref name")]
    InvalidRefName,
    /// A durable record is invalid.
    #[error("invalid Git record: {0}")]
    InvalidRecord(&'static str),
    /// Committed expected-old values do not match replay state.
    #[error("Git ref state diverged")]
    StateDiverged,
}

fn nibble(byte: u8) -> Result<u8, Error> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(Error::InvalidObjectId),
    }
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
        assert!(RefUpdate::new("refs/notes/x", None, Some(id)).is_ok());
        assert!(RefUpdate::new(Bytes::from_static(b"refs/tags/\xff"), None, Some(id)).is_ok());
        assert!(RefUpdate::new("", None, Some(id)).is_err());
        assert!(RefUpdate::new(Bytes::from_static(b"refs/heads/a\0b"), None, Some(id)).is_err());
        assert!(RefUpdate::new("refs/heads/main", Some(id), Some(id)).is_err());
        Ok(())
    }
}
