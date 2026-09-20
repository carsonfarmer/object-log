#![doc = include_str!("../README.md")]
#![deny(missing_docs)]

mod tree;

use bytes::Bytes;
use minicbor::{Decode, Encode, encode::Write};
use object_log::{
    CheckpointStatus, Log, MaterializeError, Materializer, PreparedCommit, StagedObject,
    TransactionId, View, materialize,
};

const FORMAT: &[u8] = b"object-log-kv/radix/1";

/// Admission limits for one handle. These do not change the durable format.
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    /// Maximum key length, including keys returned by scans.
    pub key_bytes: usize,
    /// Maximum value length. Larger values are rejected, never buffered unboundedly.
    pub value_bytes: usize,
    /// Maximum commands or keys in a batch or multi-get.
    pub batch_entries: usize,
    /// Maximum combined command key, expected-value, and new-value bytes.
    pub batch_bytes: usize,
    /// Maximum entries returned by one scan page.
    pub page_entries: usize,
    /// Maximum combined key and value bytes returned by a page or multi-get.
    pub response_bytes: usize,
    /// Cumulative encoded tree bytes read/written and scan-prefix bytes allocated.
    /// Includes repeated reads and all commands in a batch; excludes WAL envelopes.
    pub tree_bytes: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            key_bytes: 1024,
            value_bytes: 64 * 1024,
            batch_entries: 128,
            batch_bytes: 1024 * 1024,
            page_entries: 128,
            response_bytes: 1024 * 1024,
            tree_bytes: 32 * 1024 * 1024,
        }
    }
}

/// One byte-oriented command. Commands in a batch observe earlier commands.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum KvCommand {
    /// Set a value and report whether it changed.
    Set {
        /// Key bytes; the empty key is valid.
        key: Bytes,
        /// Value bytes; an empty value differs from an absent key.
        value: Bytes,
    },
    /// Delete a key and report whether it existed.
    Delete {
        /// Key bytes.
        key: Bytes,
    },
    /// Add to a big-endian signed 64-bit integer. An absent key starts at zero.
    Increment {
        /// Key bytes.
        key: Bytes,
        /// Signed delta. Adding zero to an absent key leaves it absent.
        delta: i64,
    },
    /// Replace or delete a value if it equals `expected`; a mismatch is a no-op.
    CompareAndSwap {
        /// Key bytes.
        key: Bytes,
        /// Required current value; `None` requires absence.
        expected: Option<Bytes>,
        /// Replacement value; `None` deletes the key.
        value: Option<Bytes>,
    },
}

impl KvCommand {
    fn key(&self) -> &Bytes {
        match self {
            Self::Set { key, .. }
            | Self::Delete { key }
            | Self::Increment { key, .. }
            | Self::CompareAndSwap { key, .. } => key,
        }
    }

    fn evaluate(&self, previous: Option<Bytes>) -> Result<(Option<Bytes>, KvResult), KvError> {
        Ok(match self {
            Self::Set { value, .. } => (
                Some(value.clone()),
                KvResult::Changed(previous.as_ref() != Some(value)),
            ),
            Self::Delete { .. } => (None, KvResult::Changed(previous.is_some())),
            Self::CompareAndSwap {
                expected, value, ..
            } => {
                let matched = &previous == expected;
                (
                    if matched { value.clone() } else { previous },
                    KvResult::Swapped(matched),
                )
            }
            Self::Increment { delta, .. } => {
                let current = previous
                    .as_deref()
                    .map(|bytes| {
                        bytes
                            .try_into()
                            .map(i64::from_be_bytes)
                            .map_err(|_| KvError::NotInteger)
                    })
                    .transpose()?
                    .unwrap_or(0);
                let next = current
                    .checked_add(*delta)
                    .ok_or(KvError::IntegerOverflow)?;
                let value = if *delta == 0 {
                    previous
                } else {
                    Some(Bytes::copy_from_slice(&next.to_be_bytes()))
                };
                (value, KvResult::Integer(next))
            }
        })
    }
}

/// A small result recorded in command order in the same atomic WAL publication.
#[derive(Clone, Copy, Debug, Decode, Encode, Eq, PartialEq)]
pub enum KvResult {
    /// Whether a set or delete changed the value, including creating an empty value.
    #[n(0)]
    Changed(#[n(0)] bool),
    /// Value following an increment.
    #[n(1)]
    Integer(#[n(0)] i64),
    /// Whether the compare-and-swap matched, even if the replacement was identical.
    #[n(2)]
    Swapped(#[n(0)] bool),
}

/// A KV namespace over one WAL. All writers must use this crate's format.
#[derive(Clone, Debug)]
pub struct KvStore {
    log: Log,
    limits: Limits,
}

impl KvStore {
    /// Wrap an opened WAL with KV admission limits. Performs no I/O.
    #[must_use]
    pub const fn new(log: Log, limits: Limits) -> Self {
        Self { log, limits }
    }

    /// Access WAL publication, recovery, retention, and collection operations.
    #[must_use]
    pub const fn log(&self) -> &Log {
        &self.log
    }

    /// Load one exact snapshot without reading its tree.
    ///
    /// # Errors
    /// Returns WAL errors or an incompatible KV operation/checkpoint format.
    pub async fn snapshot(&self) -> Result<KvSnapshot, KvError> {
        let materialized = materialize(&self.log, self.log.load().await?, &Root)
            .await
            .map_err(|error| match error {
                MaterializeError::Log(error) => KvError::Log(error),
                MaterializeError::State(error) => error,
            })?;
        let (view, root) = materialized.into_parts();
        Ok(KvSnapshot {
            store: self.clone(),
            view,
            root,
        })
    }
}

/// An exact immutable read/write base. Clone cheaply for stable pagination.
///
/// A snapshot never refreshes itself. Concurrent collection can expire it unless
/// its [`Self::view`] is protected by a WAL retention.
#[derive(Clone, Debug)]
pub struct KvSnapshot {
    store: KvStore,
    view: View,
    root: Option<StagedObject>,
}

impl KvSnapshot {
    /// The WAL view used for reads, conditional publication, and retention.
    #[must_use]
    pub const fn view(&self) -> &View {
        &self.view
    }

    /// Read one value; absence differs from an empty value.
    ///
    /// # Errors
    /// Returns admission, integrity, storage, or view-expiry errors.
    pub async fn get(&self, key: &[u8]) -> Result<Option<Bytes>, KvError> {
        self.check_key(key)?;
        self.tree().get(self.root.clone(), key).await
    }

    /// Read values in input order from this snapshot, with one shared budget.
    ///
    /// # Errors
    /// Returns admission, integrity, storage, or view-expiry errors.
    pub async fn get_many(&self, keys: &[Bytes]) -> Result<Vec<Option<Bytes>>, KvError> {
        ensure(
            keys.len() <= self.store.limits.batch_entries,
            "batch entries",
        )?;
        let mut bytes = Budget(self.store.limits.batch_bytes);
        for key in keys {
            self.check_key(key)?;
            bytes.charge(key.len())?;
        }
        let mut tree = self.tree();
        let mut response = Budget(self.store.limits.response_bytes);
        let mut values = Vec::with_capacity(keys.len());
        for key in keys {
            let value = tree.get(self.root.clone(), key).await?;
            response.charge(value.as_ref().map_or(0, Bytes::len))?;
            values.push(value);
        }
        Ok(values)
    }

    /// Return a bounded page in `[start, end)`, strictly after `after` if present.
    /// Continue with [`KvPage::after`] on this same snapshot.
    ///
    /// # Errors
    /// Returns admission, integrity, storage, or view-expiry errors. A single
    /// entry larger than the response allowance returns a limit error.
    pub async fn scan(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<KvPage, KvError> {
        self.check_key(start)?;
        for key in end.into_iter().chain(after) {
            self.check_key(key)?;
        }
        ensure(
            limit > 0 && limit <= self.store.limits.page_entries,
            "page entries",
        )?;
        self.tree()
            .scan(self.root.clone(), start, end, after, limit)
            .await
    }

    /// Scan all keys beginning with `prefix`, in byte order.
    ///
    /// # Errors
    /// Returns the same errors as [`Self::scan`].
    pub async fn scan_prefix(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<KvPage, KvError> {
        self.check_key(prefix)?;
        let end = tree::prefix_end(prefix);
        self.scan(prefix, end.as_deref(), after, limit).await
    }

    /// Stage an atomic batch against this exact snapshot.
    ///
    /// Commands observe prior commands in the batch. A failed CAS is a no-op
    /// with `Swapped(false)`; other commands still execute. Any error aborts
    /// preparation, although unreferenced immutable objects may remain for GC.
    /// Even all-no-op batches publish their results when committed.
    ///
    /// Persist the returned candidate's recovery token **and result bytes**
    /// before passing it to [`Log::commit`]. Decode results with [`decode_results`]
    /// only after confirmed commitment. On conflict, prepare anew from a fresh
    /// snapshot. Never replay an uncertain or expired operation as new work.
    ///
    /// # Errors
    /// Returns invalid-integer, overflow, admission, integrity, storage, or
    /// expiry errors. WAL result-size and tail limits also apply.
    pub async fn prepare(
        &self,
        transaction: TransactionId,
        commands: &[KvCommand],
    ) -> Result<PreparedCommit, KvError> {
        let limits = self.store.limits;
        ensure(
            !commands.is_empty() && commands.len() <= limits.batch_entries,
            "batch entries",
        )?;
        self.store.log.preflight(&self.view, transaction)?;
        let mut input = Budget(limits.batch_bytes);
        for command in commands {
            self.check_key(command.key())?;
            input.charge(command.key().len())?;
            let values = match command {
                KvCommand::Set { value, .. } => [Some(value), None],
                KvCommand::CompareAndSwap {
                    expected, value, ..
                } => [expected.as_ref(), value.as_ref()],
                _ => [None, None],
            };
            for value in values.into_iter().flatten() {
                ensure(value.len() <= limits.value_bytes, "value bytes")?;
                input.charge(value.len())?;
            }
        }
        let mut tree = self.tree();
        let mut root = self.root.clone();
        let mut results = Vec::with_capacity(commands.len());
        for command in commands {
            let previous = tree.get(root.clone(), command.key()).await?;
            let (next, result) = command.evaluate(previous.clone())?;
            if next != previous {
                root = tree.set(root, command.key(), next).await?;
            }
            results.push(result);
        }
        let result = encode(&results)?;
        Ok(self.store.log.prepare(
            &self.view,
            transaction,
            Bytes::from_static(FORMAT),
            result,
            root.into_iter().collect(),
        )?)
    }

    /// Checkpoint this complete tree root without copying any tree nodes.
    ///
    /// Returns `None` for an empty tail. Resolve pending checkpoints with the WAL
    /// before collection; published checkpoints retain the WAL's outcome window.
    ///
    /// # Errors
    /// Returns WAL checkpoint, storage, or view errors.
    pub async fn checkpoint(&self) -> Result<Option<CheckpointStatus>, KvError> {
        let Some(through) = self.view.tail().last() else {
            return Ok(None);
        };
        Ok(Some(
            self.store
                .log
                .publish_checkpoint(
                    &self.view,
                    through,
                    Bytes::from_static(FORMAT),
                    self.root.clone().into_iter().collect(),
                )
                .await?,
        ))
    }

    fn tree(&self) -> tree::Tree<'_> {
        tree::Tree {
            log: &self.store.log,
            view: &self.view,
            limits: self.store.limits,
            budget: Budget(self.store.limits.tree_bytes),
        }
    }

    fn check_key(&self, key: &[u8]) -> Result<(), KvError> {
        ensure(key.len() <= self.store.limits.key_bytes, "key bytes")
    }
}

/// A bounded scan result; keys and values own only their returned bytes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KvPage {
    /// Entries in strictly increasing byte order.
    pub entries: Vec<(Bytes, Bytes)>,
    /// Exclusive continuation key for the same snapshot and range.
    /// `None` means exhausted. A final nonempty page may require one empty page.
    pub after: Option<Bytes>,
}

/// Decode typed results from a prepared or committed KV candidate.
///
/// # Errors
/// Returns an error for noncanonical or invalid result bytes.
pub fn decode_results(bytes: &[u8]) -> Result<Vec<KvResult>, KvError> {
    decode(bytes)
}

/// An invalid operation, incompatible format, admission failure, or WAL error.
#[derive(Debug, thiserror::Error)]
pub enum KvError {
    /// Underlying WAL failure, including view expiry on reads.
    #[error(transparent)]
    Log(#[from] object_log::Error),
    /// Bytes are not the canonical KV format.
    #[error("invalid or incompatible key-value encoding")]
    InvalidEncoding,
    /// A per-operation KV allowance was exceeded.
    #[error("key-value limit exceeded: {0}")]
    Limit(&'static str),
    /// A stored value is not a big-endian `i64`.
    #[error("stored value is not a signed 64-bit integer")]
    NotInteger,
    /// Signed increment arithmetic overflowed.
    #[error("signed 64-bit integer overflow")]
    IntegerOverflow,
}

struct Root;
impl Materializer for Root {
    type State = Option<StagedObject>;
    type Error = KvError;
    fn empty(&self) -> Self::State {
        None
    }
    fn restore(&self, bytes: &[u8], objects: &[StagedObject]) -> Result<Self::State, KvError> {
        if bytes != FORMAT
            || objects.len() > 1
            || objects
                .first()
                .is_some_and(|object| object.reference().kind() != object_log::ObjectKind::Node)
        {
            return Err(KvError::InvalidEncoding);
        }
        Ok(objects.first().cloned())
    }
    fn apply(
        &self,
        state: &mut Self::State,
        bytes: &[u8],
        objects: &[StagedObject],
    ) -> Result<(), KvError> {
        *state = self.restore(bytes, objects)?;
        Ok(())
    }
}

struct Budget(usize);
impl Budget {
    fn charge(&mut self, bytes: usize) -> Result<(), KvError> {
        self.0 = self
            .0
            .checked_sub(bytes)
            .ok_or(KvError::Limit("cumulative bytes"))?;
        Ok(())
    }
}
fn ensure(condition: bool, limit: &'static str) -> Result<(), KvError> {
    if condition {
        Ok(())
    } else {
        Err(KvError::Limit(limit))
    }
}
fn encode(value: &impl Encode<()>) -> Result<Bytes, KvError> {
    minicbor::to_vec(value)
        .map(Bytes::from)
        .map_err(|_| KvError::InvalidEncoding)
}
fn decode<'a, T: Decode<'a, ()> + Encode<()>>(bytes: &'a [u8]) -> Result<T, KvError> {
    let mut decoder = minicbor::Decoder::new(bytes);
    let value = decoder.decode().map_err(|_| KvError::InvalidEncoding)?;
    let mut exact = Exact(bytes);
    if decoder.position() != bytes.len()
        || minicbor::encode(&value, &mut exact).is_err()
        || !exact.0.is_empty()
    {
        return Err(KvError::InvalidEncoding);
    }
    Ok(value)
}
struct Exact<'a>(&'a [u8]);
impl Write for Exact<'_> {
    type Error = ();
    fn write_all(&mut self, bytes: &[u8]) -> Result<(), ()> {
        self.0 = self.0.strip_prefix(bytes).ok_or(())?;
        Ok(())
    }
}
