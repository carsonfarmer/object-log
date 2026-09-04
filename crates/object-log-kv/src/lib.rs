//! Small key-value state machine used to prove the generic log contract.

#![deny(missing_docs)]

use std::collections::BTreeMap;

use bytes::Bytes;
use minicbor::{Decode, Encode, encode::Write};

use object_log::{Materializer, ObjectRef};

const KV_FORMAT_VERSION: u32 = 1;

/// The complete logical key-value state at one log position.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct KvState {
    entries: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl KvState {
    /// Returns a stored value without copying it.
    #[must_use]
    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
        self.entries.get(key).map(Vec::as_slice)
    }

    /// Returns the number of keys.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Reports whether the state has no keys.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// One key-value command evaluated against an exact materialized state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum KvCommand {
    /// Sets one key and returns its prior value.
    Set {
        /// Key bytes.
        key: Bytes,
        /// New value bytes.
        value: Bytes,
    },
    /// Deletes one key and returns its prior value.
    Delete {
        /// Key bytes.
        key: Bytes,
    },
    /// Adds a signed delta to one big-endian `i64` value.
    Increment {
        /// Key bytes.
        key: Bytes,
        /// Signed value to add.
        delta: i64,
    },
    /// Replaces one value only when its current value matches.
    CompareAndSwap {
        /// Key bytes.
        key: Bytes,
        /// Required current value. `None` means that the key must be absent.
        expected: Option<Bytes>,
        /// New value. `None` deletes the key.
        value: Option<Bytes>,
    },
}

/// The typed result recorded for one key-value command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum KvResult {
    /// The prior value for a set or delete command.
    Previous(Option<Bytes>),
    /// The value after an increment command.
    Integer(i64),
    /// Whether a compare-and-swap matched.
    Swapped(bool),
}

/// The result of evaluating a command before log publication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum KvDecision {
    /// The command has a result but needs no durable mutation.
    NoChange(KvResult),
    /// The command needs one durable mutation.
    Commit {
        /// Canonical operation bytes for the WAL entry.
        operation: Bytes,
        /// Canonical result bytes for the WAL entry.
        result_bytes: Bytes,
        /// Typed result returned after successful publication.
        result: KvResult,
    },
}

/// A deterministic key-value materializer and command evaluator.
#[derive(Clone, Copy, Debug, Default)]
pub struct KvMachine;

impl KvMachine {
    /// Evaluates one command against `state` without changing it.
    ///
    /// A caller publishes [`KvDecision::Commit`] against the same durable view.
    /// After a log conflict, it must materialize the winning view and evaluate
    /// the command again.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid integer data, arithmetic overflow, or a
    /// value that cannot be encoded.
    pub fn evaluate(&self, state: &KvState, command: &KvCommand) -> Result<KvDecision, KvError> {
        let integer_bytes;
        let (mutation, result) = match command {
            KvCommand::Set { key, value } => {
                let previous = state.get(key).map(Bytes::copy_from_slice);
                if previous.as_ref().is_some_and(|stored| stored == value) {
                    return Ok(KvDecision::NoChange(KvResult::Previous(previous)));
                }
                (
                    MutationWire::unconditional(key, Some(value)),
                    KvResult::Previous(previous),
                )
            }
            KvCommand::Delete { key } => {
                let previous = state.get(key).map(Bytes::copy_from_slice);
                let Some(stored) = previous else {
                    return Ok(KvDecision::NoChange(KvResult::Previous(None)));
                };
                (
                    MutationWire::unconditional(key, None),
                    KvResult::Previous(Some(stored)),
                )
            }
            KvCommand::Increment { key, delta } => {
                let prior = state.get(key);
                let current = prior.map(decode_integer).transpose()?.unwrap_or(0);
                let next = current
                    .checked_add(*delta)
                    .ok_or(KvError::IntegerOverflow)?;
                if next == current {
                    return Ok(KvDecision::NoChange(KvResult::Integer(current)));
                }
                integer_bytes = next.to_be_bytes();
                (
                    MutationWire::conditional(key, prior, Some(integer_bytes.as_slice())),
                    KvResult::Integer(next),
                )
            }
            KvCommand::CompareAndSwap {
                key,
                expected,
                value,
            } => {
                if state.get(key) != expected.as_deref() {
                    return Ok(KvDecision::NoChange(KvResult::Swapped(false)));
                }
                if expected == value {
                    return Ok(KvDecision::NoChange(KvResult::Swapped(true)));
                }
                (
                    MutationWire::conditional(key, expected.as_deref(), value.as_deref()),
                    KvResult::Swapped(true),
                )
            }
        };

        Ok(KvDecision::Commit {
            operation: encode(&mutation)?.into(),
            result_bytes: encode(&ResultWire::from(&result))?.into(),
            result,
        })
    }

    /// Decodes and validates a result stored in a committed log entry.
    ///
    /// # Errors
    ///
    /// Returns an error when the result uses an unsupported version, is not
    /// canonical, or contains fields that do not match its result kind.
    pub fn decode_result(&self, bytes: &[u8]) -> Result<KvResult, KvError> {
        let result: ResultWire<'_> = decode(bytes)?;
        require_version(result.version)?;
        match (result.kind, result.value, result.integer, result.swapped) {
            (1, value, None, None) => Ok(KvResult::Previous(value.map(Bytes::copy_from_slice))),
            (2, None, Some(value), None) => Ok(KvResult::Integer(value)),
            (3, None, None, Some(value)) => Ok(KvResult::Swapped(value)),
            _ => Err(KvError::InvalidEncoding(
                "result fields do not match its kind".to_owned(),
            )),
        }
    }

    /// Encodes one key-value state as a checkpoint.
    ///
    /// # Errors
    ///
    /// Returns an error when the state cannot be encoded.
    pub fn checkpoint(&self, state: &KvState) -> Result<Bytes, KvError> {
        let snapshot = SnapshotWire {
            version: KV_FORMAT_VERSION,
            entries: state
                .entries
                .iter()
                .map(|(key, value)| EntryWire { key, value })
                .collect(),
        };
        Ok(encode(&snapshot)?.into())
    }
}

impl Materializer for KvMachine {
    type State = KvState;
    type Error = KvError;

    fn empty(&self) -> Self::State {
        KvState::default()
    }

    fn restore(
        &self,
        checkpoint: &[u8],
        _objects: &[ObjectRef],
    ) -> Result<Self::State, Self::Error> {
        let snapshot: SnapshotWire<'_> = decode(checkpoint)?;
        require_version(snapshot.version)?;
        let mut entries = BTreeMap::new();
        let mut prior_key: Option<&[u8]> = None;
        for entry in snapshot.entries {
            if prior_key.is_some_and(|prior| prior >= entry.key) {
                return Err(KvError::InvalidEncoding(
                    "snapshot keys are not in strict byte order".to_owned(),
                ));
            }
            prior_key = Some(entry.key);
            entries.insert(entry.key.to_vec(), entry.value.to_vec());
        }
        Ok(KvState { entries })
    }

    fn apply(
        &self,
        state: &mut Self::State,
        _sequence: u64,
        operation: &[u8],
        _objects: &[ObjectRef],
    ) -> Result<(), Self::Error> {
        let mutation: MutationWire<'_> = decode(operation)?;
        require_version(mutation.version)?;
        if !mutation.check_expected && mutation.expected.is_some() {
            return Err(KvError::InvalidEncoding(
                "an unconditional mutation contains an expected value".to_owned(),
            ));
        }
        if mutation.check_expected && state.get(mutation.key) != mutation.expected {
            return Err(KvError::StateDiverged);
        }
        match mutation.value {
            Some(value) => {
                state.entries.insert(mutation.key.to_vec(), value.to_vec());
            }
            None => {
                state.entries.remove(mutation.key);
            }
        }
        Ok(())
    }
}

/// Invalid key-value operation, result, or snapshot data.
#[derive(Debug, thiserror::Error)]
pub enum KvError {
    /// Bytes do not use the current canonical key-value format.
    #[error("invalid key-value encoding: {0}")]
    InvalidEncoding(String),
    /// A stored value cannot decode as a big-endian `i64`.
    #[error("stored value is not a signed 64-bit integer")]
    NotInteger,
    /// Signed increment arithmetic overflowed.
    #[error("signed 64-bit integer overflow")]
    IntegerOverflow,
    /// Replay found a different prior value than the committed operation.
    #[error("key-value replay does not match its expected prior value")]
    StateDiverged,
}

#[derive(Decode, Encode)]
#[cbor(map)]
struct MutationWire<'a> {
    #[n(1)]
    version: u32,
    #[cbor(n(2), with = "minicbor::bytes")]
    key: &'a [u8],
    #[n(3)]
    check_expected: bool,
    #[cbor(n(4), with = "minicbor::bytes")]
    expected: Option<&'a [u8]>,
    #[cbor(n(5), with = "minicbor::bytes")]
    value: Option<&'a [u8]>,
}

impl<'a> MutationWire<'a> {
    const fn unconditional(key: &'a [u8], value: Option<&'a [u8]>) -> Self {
        Self {
            version: KV_FORMAT_VERSION,
            key,
            check_expected: false,
            expected: None,
            value,
        }
    }

    const fn conditional(
        key: &'a [u8],
        expected: Option<&'a [u8]>,
        value: Option<&'a [u8]>,
    ) -> Self {
        Self {
            version: KV_FORMAT_VERSION,
            key,
            check_expected: true,
            expected,
            value,
        }
    }
}

#[derive(Decode, Encode)]
#[cbor(map)]
struct SnapshotWire<'a> {
    #[n(1)]
    version: u32,
    #[b(2)]
    entries: Vec<EntryWire<'a>>,
}

#[derive(Decode, Encode)]
#[cbor(map)]
struct EntryWire<'a> {
    #[cbor(n(1), with = "minicbor::bytes")]
    key: &'a [u8],
    #[cbor(n(2), with = "minicbor::bytes")]
    value: &'a [u8],
}

#[derive(Decode, Encode)]
#[cbor(map)]
struct ResultWire<'a> {
    #[n(1)]
    version: u32,
    #[n(2)]
    kind: u8,
    #[cbor(n(3), with = "minicbor::bytes")]
    value: Option<&'a [u8]>,
    #[n(4)]
    integer: Option<i64>,
    #[n(5)]
    swapped: Option<bool>,
}

impl<'a> From<&'a KvResult> for ResultWire<'a> {
    fn from(result: &'a KvResult) -> Self {
        let (kind, value, integer, swapped) = match result {
            KvResult::Previous(value) => (1, value.as_deref(), None, None),
            KvResult::Integer(value) => (2, None, Some(*value), None),
            KvResult::Swapped(value) => (3, None, None, Some(*value)),
        };
        Self {
            version: KV_FORMAT_VERSION,
            kind,
            value,
            integer,
            swapped,
        }
    }
}

fn encode(value: &impl Encode<()>) -> Result<Vec<u8>, KvError> {
    minicbor::to_vec(value).map_err(|error| KvError::InvalidEncoding(error.to_string()))
}

fn decode<'bytes, T>(bytes: &'bytes [u8]) -> Result<T, KvError>
where
    T: Decode<'bytes, ()> + Encode<()>,
{
    let mut decoder = minicbor::Decoder::new(bytes);
    let value = decoder
        .decode()
        .map_err(|error| KvError::InvalidEncoding(error.to_string()))?;
    if decoder.position() != bytes.len() {
        return Err(KvError::InvalidEncoding(
            "encoded value contains trailing bytes".to_owned(),
        ));
    }
    if !is_canonical(&value, bytes) {
        return Err(KvError::InvalidEncoding(
            "encoded value is not canonical key-value format version 1".to_owned(),
        ));
    }
    Ok(value)
}

struct Exact<'a>(&'a [u8]);

impl Write for Exact<'_> {
    type Error = ();

    fn write_all(&mut self, bytes: &[u8]) -> Result<(), Self::Error> {
        self.0 = self.0.strip_prefix(bytes).ok_or(())?;
        Ok(())
    }
}

fn is_canonical(value: &impl Encode<()>, bytes: &[u8]) -> bool {
    let mut exact = Exact(bytes);
    minicbor::encode(value, &mut exact).is_ok() && exact.0.is_empty()
}

fn require_version(version: u32) -> Result<(), KvError> {
    if version != KV_FORMAT_VERSION {
        return Err(KvError::InvalidEncoding(format!(
            "unsupported key-value format version {version}"
        )));
    }
    Ok(())
}

fn decode_integer(bytes: &[u8]) -> Result<i64, KvError> {
    let encoded: [u8; size_of::<i64>()] = bytes.try_into().map_err(|_| KvError::NotInteger)?;
    Ok(i64::from_be_bytes(encoded))
}
