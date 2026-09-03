//! Versioned durable encoding.
//!
//! The publication module consumes this API in the next integration commit.

#![allow(dead_code)]

use crate::{CheckpointRef, CommitRef, Digest, Error, LogId, ObjectKind, ObjectRef, TransactionId};
use bytes::Bytes;
use minicbor::{Decode, Encode};
use std::collections::HashSet;
use uuid::Uuid;

pub(crate) const FORMAT_VERSION: u32 = 1;
const DIGEST_LEN: usize = 32;
const TRANSACTION_ID_LEN: usize = 16;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Head {
    pub log_id: LogId,
    pub generation: u64,
    pub next_sequence: u64,
    pub checkpoint: Option<CheckpointRef>,
    pub tail: Vec<CommitRef>,
    pub recent_outcomes: Vec<CommitRef>,
}

impl Head {
    pub(crate) fn empty(log_id: LogId) -> Self {
        Self {
            log_id,
            generation: 0,
            next_sequence: 0,
            checkpoint: None,
            tail: Vec::new(),
            recent_outcomes: Vec::new(),
        }
    }

    pub(crate) fn tip(&self) -> Option<Digest> {
        self.tail
            .last()
            .map(|commit| commit.digest)
            .or_else(|| self.checkpoint.as_ref().map(|base| base.through_commit))
    }

    pub(crate) fn validate(&self) -> Result<(), Error> {
        let expected_start = self.checkpoint.as_ref().map_or(0, |checkpoint| {
            checkpoint.through_sequence.saturating_add(1)
        });

        for (offset, commit) in self.tail.iter().enumerate() {
            let offset = u64::try_from(offset)
                .map_err(|_| Error::InvalidFormat("tail offset exceeds u64".into()))?;
            if commit.sequence != expected_start.saturating_add(offset) {
                return Err(Error::InvalidFormat(
                    "head tail does not contain contiguous sequences".into(),
                ));
            }
        }

        let tail_len = u64::try_from(self.tail.len())
            .map_err(|_| Error::InvalidFormat("tail length exceeds u64".into()))?;
        if self.next_sequence != expected_start.saturating_add(tail_len) {
            return Err(Error::InvalidFormat(
                "head next sequence does not follow its base and tail".into(),
            ));
        }

        let mut transaction_ids = HashSet::with_capacity(self.recent_outcomes.len());
        let mut prior_sequence = None;
        for outcome in &self.recent_outcomes {
            if outcome.sequence >= self.next_sequence {
                return Err(Error::InvalidFormat(
                    "head outcome refers to an uncommitted sequence".into(),
                ));
            }
            if prior_sequence.is_some_and(|prior| outcome.sequence <= prior) {
                return Err(Error::InvalidFormat(
                    "head outcomes are not in sequence order".into(),
                ));
            }
            if !transaction_ids.insert(outcome.transaction_id) {
                return Err(Error::InvalidFormat(
                    "head contains a duplicate transaction outcome".into(),
                ));
            }
            prior_sequence = Some(outcome.sequence);
        }

        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Commit {
    pub log_id: LogId,
    pub transaction_id: TransactionId,
    pub expected_generation: u64,
    pub expected_tip: Option<Digest>,
    pub operation: Bytes,
    pub result: Bytes,
    pub objects: Vec<ObjectRef>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Checkpoint {
    pub log_id: LogId,
    pub through_sequence: u64,
    pub through_commit: Digest,
    pub snapshot: Bytes,
}

#[derive(Clone, Debug, Decode, Encode, PartialEq)]
#[cbor(map)]
struct EnvelopeWire {
    #[cbor(n(1), with = "minicbor::bytes")]
    payload: Vec<u8>,
    #[cbor(n(2), with = "minicbor::bytes")]
    digest: Vec<u8>,
}

#[derive(Clone, Debug, Decode, Encode, PartialEq)]
#[cbor(map)]
struct HeadWire {
    #[n(1)]
    format_version: u32,
    #[n(2)]
    log_id: String,
    #[n(3)]
    generation: u64,
    #[n(4)]
    next_sequence: u64,
    #[n(5)]
    checkpoint: Option<CheckpointRefWire>,
    #[n(6)]
    tail: Vec<CommitRefWire>,
    #[n(7)]
    recent_outcomes: Vec<CommitRefWire>,
}

#[derive(Clone, Debug, Decode, Encode, PartialEq)]
#[cbor(map)]
struct CommitWire {
    #[n(1)]
    format_version: u32,
    #[n(2)]
    log_id: String,
    #[cbor(n(3), with = "minicbor::bytes")]
    transaction_id: Vec<u8>,
    #[n(4)]
    expected_generation: u64,
    #[cbor(n(5), with = "minicbor::bytes")]
    expected_tip: Option<Vec<u8>>,
    #[cbor(n(6), with = "minicbor::bytes")]
    operation: Vec<u8>,
    #[cbor(n(7), with = "minicbor::bytes")]
    result: Vec<u8>,
    #[n(8)]
    objects: Vec<ObjectRefWire>,
}

#[derive(Clone, Debug, Decode, Encode, PartialEq)]
#[cbor(map)]
struct CheckpointWire {
    #[n(1)]
    format_version: u32,
    #[n(2)]
    log_id: String,
    #[n(3)]
    through_sequence: u64,
    #[cbor(n(4), with = "minicbor::bytes")]
    through_commit: Vec<u8>,
    #[cbor(n(5), with = "minicbor::bytes")]
    snapshot: Vec<u8>,
}

#[derive(Clone, Debug, Decode, Encode, PartialEq)]
#[cbor(map)]
struct CommitRefWire {
    #[n(1)]
    sequence: u64,
    #[cbor(n(2), with = "minicbor::bytes")]
    transaction_id: Vec<u8>,
    #[cbor(n(3), with = "minicbor::bytes")]
    digest: Vec<u8>,
    #[n(4)]
    len: u64,
}

#[derive(Clone, Debug, Decode, Encode, PartialEq)]
#[cbor(map)]
struct CheckpointRefWire {
    #[n(1)]
    through_sequence: u64,
    #[cbor(n(2), with = "minicbor::bytes")]
    through_commit: Vec<u8>,
    #[n(3)]
    object: Option<ObjectRefWire>,
}

#[derive(Clone, Debug, Decode, Encode, PartialEq)]
#[cbor(map)]
struct ObjectRefWire {
    #[n(1)]
    kind: u8,
    #[cbor(n(2), with = "minicbor::bytes")]
    digest: Vec<u8>,
    #[n(3)]
    len: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
enum ObjectKindWire {
    Unspecified = 0,
    Blob = 1,
    Checkpoint = 2,
}

pub(crate) fn encode_head(head: &Head) -> Result<Bytes, Error> {
    head.validate()?;
    encode_envelope(&HeadWire {
        format_version: FORMAT_VERSION,
        log_id: head.log_id.to_string(),
        generation: head.generation,
        next_sequence: head.next_sequence,
        checkpoint: head.checkpoint.as_ref().map(CheckpointRefWire::from),
        tail: head.tail.iter().map(CommitRefWire::from).collect(),
        recent_outcomes: head
            .recent_outcomes
            .iter()
            .map(CommitRefWire::from)
            .collect(),
    })
}

pub(crate) fn decode_head(bytes: &[u8]) -> Result<Head, Error> {
    let wire: HeadWire = decode_envelope(bytes)?;
    require_version(wire.format_version)?;
    let head = Head {
        log_id: LogId::new(wire.log_id)?,
        generation: wire.generation,
        next_sequence: wire.next_sequence,
        checkpoint: wire.checkpoint.map(CheckpointRef::try_from).transpose()?,
        tail: wire
            .tail
            .into_iter()
            .map(CommitRef::try_from)
            .collect::<Result<_, _>>()?,
        recent_outcomes: wire
            .recent_outcomes
            .into_iter()
            .map(CommitRef::try_from)
            .collect::<Result<_, _>>()?,
    };
    head.validate()?;
    Ok(head)
}

pub(crate) fn encode_commit(commit: &Commit) -> Result<Bytes, Error> {
    encode_envelope(&CommitWire {
        format_version: FORMAT_VERSION,
        log_id: commit.log_id.to_string(),
        transaction_id: commit.transaction_id.as_uuid().as_bytes().to_vec(),
        expected_generation: commit.expected_generation,
        expected_tip: commit.expected_tip.map(|digest| digest.as_bytes().to_vec()),
        operation: commit.operation.to_vec(),
        result: commit.result.to_vec(),
        objects: commit.objects.iter().map(ObjectRefWire::from).collect(),
    })
}

pub(crate) fn decode_commit(bytes: &[u8]) -> Result<Commit, Error> {
    let wire: CommitWire = decode_envelope(bytes)?;
    require_version(wire.format_version)?;
    Ok(Commit {
        log_id: LogId::new(wire.log_id)?,
        transaction_id: transaction_id(&wire.transaction_id)?,
        expected_generation: wire.expected_generation,
        expected_tip: wire.expected_tip.map(|value| digest(&value)).transpose()?,
        operation: Bytes::from(wire.operation),
        result: Bytes::from(wire.result),
        objects: wire
            .objects
            .into_iter()
            .map(ObjectRef::try_from)
            .collect::<Result<_, _>>()?,
    })
}

pub(crate) fn encode_checkpoint(checkpoint: &Checkpoint) -> Result<Bytes, Error> {
    encode_envelope(&CheckpointWire {
        format_version: FORMAT_VERSION,
        log_id: checkpoint.log_id.to_string(),
        through_sequence: checkpoint.through_sequence,
        through_commit: checkpoint.through_commit.as_bytes().to_vec(),
        snapshot: checkpoint.snapshot.to_vec(),
    })
}

pub(crate) fn decode_checkpoint(bytes: &[u8]) -> Result<Checkpoint, Error> {
    let wire: CheckpointWire = decode_envelope(bytes)?;
    require_version(wire.format_version)?;
    Ok(Checkpoint {
        log_id: LogId::new(wire.log_id)?,
        through_sequence: wire.through_sequence,
        through_commit: digest(&wire.through_commit)?,
        snapshot: Bytes::from(wire.snapshot),
    })
}

fn encode_envelope(message: &impl Encode<()>) -> Result<Bytes, Error> {
    let payload = minicbor::to_vec(message)
        .map_err(|error| Error::InvalidFormat(format!("CBOR encoding failed: {error}")))?;
    let digest = Digest::of(&payload);
    Ok(Bytes::from(
        minicbor::to_vec(EnvelopeWire {
            payload,
            digest: digest.as_bytes().to_vec(),
        })
        .map_err(|error| Error::InvalidFormat(format!("CBOR encoding failed: {error}")))?,
    ))
}

fn decode_envelope<M>(bytes: &[u8]) -> Result<M, Error>
where
    M: for<'bytes> Decode<'bytes, ()>,
{
    let envelope: EnvelopeWire = decode_exact(bytes)?;
    if digest(&envelope.digest)? != Digest::of(&envelope.payload) {
        return Err(Error::CorruptObject);
    }
    decode_exact(&envelope.payload)
}

fn decode_exact<M>(bytes: &[u8]) -> Result<M, Error>
where
    M: for<'bytes> Decode<'bytes, ()>,
{
    let mut decoder = minicbor::Decoder::new(bytes);
    let value = decoder
        .decode()
        .map_err(|error| Error::InvalidFormat(error.to_string()))?;
    if decoder.position() != bytes.len() {
        return Err(Error::InvalidFormat(
            "encoded object contains trailing bytes".into(),
        ));
    }
    Ok(value)
}

fn require_version(version: u32) -> Result<(), Error> {
    if version != FORMAT_VERSION {
        return Err(Error::InvalidFormat(format!(
            "unsupported format version {version}"
        )));
    }
    Ok(())
}

fn digest(value: &[u8]) -> Result<Digest, Error> {
    let bytes: [u8; DIGEST_LEN] = value
        .try_into()
        .map_err(|_| Error::InvalidFormat("digest has an invalid length".into()))?;
    Ok(Digest(bytes))
}

fn transaction_id(value: &[u8]) -> Result<TransactionId, Error> {
    if value.len() != TRANSACTION_ID_LEN {
        return Err(Error::InvalidFormat(
            "transaction ID has an invalid length".into(),
        ));
    }
    let value = Uuid::from_slice(value)
        .map_err(|error| Error::InvalidFormat(format!("invalid transaction ID: {error}")))?;
    Ok(TransactionId::from_uuid(value))
}

impl From<&CommitRef> for CommitRefWire {
    fn from(value: &CommitRef) -> Self {
        Self {
            sequence: value.sequence,
            transaction_id: value.transaction_id.as_uuid().as_bytes().to_vec(),
            digest: value.digest.as_bytes().to_vec(),
            len: value.len,
        }
    }
}

impl TryFrom<CommitRefWire> for CommitRef {
    type Error = Error;

    fn try_from(value: CommitRefWire) -> Result<Self, Self::Error> {
        Ok(Self {
            sequence: value.sequence,
            transaction_id: transaction_id(&value.transaction_id)?,
            digest: digest(&value.digest)?,
            len: value.len,
        })
    }
}

impl From<&CheckpointRef> for CheckpointRefWire {
    fn from(value: &CheckpointRef) -> Self {
        Self {
            through_sequence: value.through_sequence,
            through_commit: value.through_commit.as_bytes().to_vec(),
            object: Some(ObjectRefWire::from(&value.object)),
        }
    }
}

impl TryFrom<CheckpointRefWire> for CheckpointRef {
    type Error = Error;

    fn try_from(value: CheckpointRefWire) -> Result<Self, Self::Error> {
        let object: ObjectRef = value
            .object
            .ok_or_else(|| Error::InvalidFormat("checkpoint object is missing".into()))?
            .try_into()?;
        if object.kind != ObjectKind::Checkpoint {
            return Err(Error::InvalidFormat(
                "checkpoint reference names a non-checkpoint object".into(),
            ));
        }
        Ok(Self {
            through_sequence: value.through_sequence,
            through_commit: digest(&value.through_commit)?,
            object,
        })
    }
}

impl From<&ObjectRef> for ObjectRefWire {
    fn from(value: &ObjectRef) -> Self {
        Self {
            kind: match value.kind {
                ObjectKind::Blob => ObjectKindWire::Blob as u8,
                ObjectKind::Checkpoint => ObjectKindWire::Checkpoint as u8,
            },
            digest: value.digest.as_bytes().to_vec(),
            len: value.len,
        }
    }
}

impl TryFrom<ObjectRefWire> for ObjectRef {
    type Error = Error;

    fn try_from(value: ObjectRefWire) -> Result<Self, Self::Error> {
        let kind = match value.kind {
            value if value == ObjectKindWire::Blob as u8 => ObjectKind::Blob,
            value if value == ObjectKindWire::Checkpoint as u8 => ObjectKind::Checkpoint,
            _ => {
                return Err(Error::InvalidFormat("invalid object kind".into()));
            }
        };
        Ok(Self {
            kind,
            digest: digest(&value.digest)?,
            len: value.len,
        })
    }
}

#[cfg(test)]
#[allow(clippy::panic)]
mod tests {
    use super::{
        Checkpoint, Commit, Head, decode_checkpoint, decode_commit, decode_head, encode_checkpoint,
        encode_commit, encode_head,
    };
    use crate::{CheckpointRef, CommitRef, Digest, Error, LogId, ObjectKind, ObjectRef};
    use bytes::Bytes;

    fn log_id() -> LogId {
        LogId::new("tenant.resource").unwrap_or_else(|error| panic!("valid ID failed: {error}"))
    }

    fn commit_ref(sequence: u64, data: &[u8]) -> CommitRef {
        CommitRef {
            sequence,
            transaction_id: crate::TransactionId::new(),
            digest: Digest::of(data),
            len: u64::try_from(data.len()).unwrap_or_else(|_| panic!("test data is too large")),
        }
    }

    #[test]
    fn head_round_trip_preserves_order_and_base() {
        let base_commit = Digest::of(b"base");
        let checkpoint_object = ObjectRef {
            kind: ObjectKind::Checkpoint,
            digest: Digest::of(b"checkpoint"),
            len: 10,
        };
        let first = commit_ref(4, b"first");
        let second = commit_ref(5, b"second");
        let head = Head {
            log_id: log_id(),
            generation: 9,
            next_sequence: 6,
            checkpoint: Some(CheckpointRef {
                through_sequence: 3,
                through_commit: base_commit,
                object: checkpoint_object,
            }),
            tail: vec![first.clone(), second.clone()],
            recent_outcomes: vec![first, second],
        };

        let encoded = encode_head(&head).unwrap_or_else(|error| panic!("encode failed: {error}"));
        let decoded =
            decode_head(&encoded).unwrap_or_else(|error| panic!("decode failed: {error}"));
        assert_eq!(decoded, head);
        assert_eq!(decoded.tip(), head.tip());
    }

    #[test]
    fn commit_round_trip_preserves_opaque_bytes() {
        let commit = Commit {
            log_id: log_id(),
            transaction_id: crate::TransactionId::new(),
            expected_generation: 2,
            expected_tip: Some(Digest::of(b"prior")),
            operation: Bytes::from_static(b"operation"),
            result: Bytes::from_static(b"result"),
            objects: vec![ObjectRef {
                kind: ObjectKind::Blob,
                digest: Digest::of(b"blob"),
                len: 4,
            }],
        };

        let encoded =
            encode_commit(&commit).unwrap_or_else(|error| panic!("encode failed: {error}"));
        let decoded =
            decode_commit(&encoded).unwrap_or_else(|error| panic!("decode failed: {error}"));
        assert_eq!(decoded, commit);
    }

    #[test]
    fn checkpoint_round_trip_preserves_snapshot() {
        let checkpoint = Checkpoint {
            log_id: log_id(),
            through_sequence: 3,
            through_commit: Digest::of(b"commit"),
            snapshot: Bytes::from_static(b"opaque snapshot"),
        };

        let encoded =
            encode_checkpoint(&checkpoint).unwrap_or_else(|error| panic!("encode failed: {error}"));
        let decoded =
            decode_checkpoint(&encoded).unwrap_or_else(|error| panic!("decode failed: {error}"));
        assert_eq!(decoded, checkpoint);
    }

    #[test]
    fn changed_envelope_is_corrupt() {
        let mut encoded = encode_commit(&Commit {
            log_id: log_id(),
            transaction_id: crate::TransactionId::new(),
            expected_generation: 0,
            expected_tip: None,
            operation: Bytes::from_static(b"operation"),
            result: Bytes::new(),
            objects: Vec::new(),
        })
        .unwrap_or_else(|error| panic!("encode failed: {error}"))
        .to_vec();
        let index = encoded.len().saturating_sub(1);
        encoded[index] ^= 1;

        assert!(matches!(decode_commit(&encoded), Err(Error::CorruptObject)));
    }

    #[test]
    fn head_rejects_sequence_gap() {
        let head = Head {
            log_id: log_id(),
            generation: 1,
            next_sequence: 2,
            checkpoint: None,
            tail: vec![commit_ref(1, b"gap")],
            recent_outcomes: Vec::new(),
        };
        assert!(matches!(encode_head(&head), Err(Error::InvalidFormat(_))));
    }
}
