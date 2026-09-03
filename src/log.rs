//! Object-log publication protocol.

use bytes::Bytes;
use futures::future::try_join_all;

use crate::format::{self, Head};
use crate::store::{ConditionalRead, CreateResult, ScopedStore, StoreKey, UpdateResult};
use crate::{
    CheckpointRef, CommitRef, Cursor, Digest, Error, LogId, ObjectKind, ObjectRef, PendingCommit,
    PreparedCommit, TransactionId,
};

/// Limits applied by one log writer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Options {
    pub max_tail_entries: usize,
    pub resolution_window: usize,
    pub max_inline_operation_bytes: usize,
    pub max_inline_result_bytes: usize,
    pub max_object_refs_per_commit: usize,
    pub max_commit_bytes: usize,
    pub max_head_bytes: usize,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            max_tail_entries: 1_024,
            resolution_window: 1_024,
            max_inline_operation_bytes: 64 * 1_024,
            max_inline_result_bytes: 4 * 1_024,
            max_object_refs_per_commit: 1_024,
            max_commit_bytes: 1024 * 1024,
            max_head_bytes: 256 * 1024,
        }
    }
}

/// One observed durable head.
#[derive(Clone, Debug)]
pub struct View {
    pub(crate) cursor: Cursor,
}

impl View {
    #[must_use]
    pub const fn cursor(&self) -> &Cursor {
        &self.cursor
    }

    /// Returns the log identity bound to this view.
    #[must_use]
    pub const fn log_id(&self) -> &LogId {
        &self.cursor.head.log_id
    }

    /// Returns the current checkpoint reference, when present.
    #[must_use]
    pub const fn checkpoint(&self) -> Option<&CheckpointRef> {
        self.cursor.head.checkpoint.as_ref()
    }

    /// Returns the ordered active commit references.
    #[must_use]
    pub fn tail(&self) -> &[CommitRef] {
        &self.cursor.head.tail
    }
}

/// The result of a conditional head refresh.
#[derive(Debug)]
pub enum Refresh {
    NotModified,
    Updated(Box<View>),
}

/// The immediate result of publishing one prepared commit.
#[derive(Debug)]
pub enum CommitStatus {
    Committed(View),
    Conflict(View),
    Pending(PendingCommit),
}

/// The result of resolving one uncertain publication.
#[derive(Debug)]
pub enum Resolution {
    Committed(View),
    NotCommitted(View),
    StillPending(PendingCommit),
    Expired(View),
}

/// The result of publishing a checkpoint.
#[derive(Debug)]
pub enum CheckpointStatus {
    Published(View),
    Conflict(View),
}

/// One decoded commit joined with its ordered head reference.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommitRecord {
    reference: CommitRef,
    expected_generation: u64,
    expected_tip: Option<Digest>,
    operation: Bytes,
    result: Bytes,
    objects: Vec<ObjectRef>,
}

impl CommitRecord {
    /// Returns the ordered reference that published this commit.
    #[must_use]
    pub const fn reference(&self) -> &CommitRef {
        &self.reference
    }

    /// Returns the head generation on which this commit was prepared.
    #[must_use]
    pub const fn expected_generation(&self) -> u64 {
        self.expected_generation
    }

    /// Returns the prior commit digest on which this commit was prepared.
    #[must_use]
    pub const fn expected_tip(&self) -> Option<Digest> {
        self.expected_tip
    }

    /// Returns the caller-defined operation bytes.
    #[must_use]
    pub const fn operation(&self) -> &Bytes {
        &self.operation
    }

    /// Returns the caller-defined result bytes.
    #[must_use]
    pub const fn result(&self) -> &Bytes {
        &self.result
    }

    /// Returns the immutable objects referenced by the operation.
    #[must_use]
    pub fn objects(&self) -> &[ObjectRef] {
        &self.objects
    }
}

/// A linearizable log in one namespace-safe object-store scope.
#[derive(Clone, Debug)]
pub struct Log {
    store: ScopedStore,
    options: Options,
}

impl Log {
    /// Opens a writable log and creates its initial head when it is absent.
    ///
    /// This probes the backend contract before it accesses the durable head.
    ///
    /// # Errors
    ///
    /// Returns an error when the backend lacks a required behavior, the head
    /// cannot be created or read, or an existing head is invalid.
    pub async fn open(store: ScopedStore, options: Options) -> Result<Self, Error> {
        store.validate_backend().await?;
        let log = Self { store, options };
        let initial = Head::empty(log.store.log_id().clone());
        let initial_bytes = format::encode_head(&initial)?;
        log.validate_encoded_head(&initial_bytes)?;

        match log
            .store
            .create(StoreKey::Head, initial_bytes.clone())
            .await
        {
            Ok(CreateResult::Created { .. }) => {}
            Ok(CreateResult::AlreadyExists) => {
                log.load().await?;
            }
            Err(create_error) => match log.store.read(StoreKey::Head).await? {
                Some(stored) => {
                    log.view_from_stored(stored)?;
                }
                None => return Err(create_error),
            },
        }
        Ok(log)
    }

    /// Loads and verifies the current durable head.
    ///
    /// # Errors
    ///
    /// Returns an error when the head is missing, unreadable, corrupt, or
    /// belongs to a different log identity.
    pub async fn load(&self) -> Result<View, Error> {
        let stored =
            self.store.read(StoreKey::Head).await?.ok_or_else(|| {
                Error::InvalidFormat("the opened log has no durable head".to_owned())
            })?;
        self.view_from_stored(stored)
    }

    /// Conditionally loads the head when it changed after `cursor`.
    ///
    /// # Errors
    ///
    /// Returns an error for a foreign cursor, a missing head, invalid durable
    /// bytes, or a backend failure.
    pub async fn refresh(&self, cursor: &Cursor) -> Result<Refresh, Error> {
        self.validate_cursor(cursor)?;
        match self
            .store
            .read_if_changed(StoreKey::Head, cursor.storage_version())
            .await?
        {
            ConditionalRead::NotModified => Ok(Refresh::NotModified),
            ConditionalRead::Modified(stored) => {
                Ok(Refresh::Updated(Box::new(self.view_from_stored(stored)?)))
            }
            ConditionalRead::Missing => Err(Error::InvalidFormat(
                "the opened log has no durable head".to_owned(),
            )),
        }
    }

    /// Stores one immutable content-addressed blob.
    ///
    /// Repeating this operation with the same bytes is idempotent.
    ///
    /// # Errors
    ///
    /// Returns an error when the byte length cannot be represented, the
    /// backend fails, or different bytes already exist at the digest key.
    pub async fn put_object(&self, bytes: Bytes) -> Result<ObjectRef, Error> {
        let len =
            u64::try_from(bytes.len()).map_err(|_| Error::LimitExceeded("object byte length"))?;
        let object = ObjectRef {
            kind: ObjectKind::Blob,
            digest: Digest::of(&bytes),
            len,
        };
        self.create_immutable(Self::object_key(&object), bytes)
            .await?;
        Ok(object)
    }

    /// Reads and verifies one object from this log namespace.
    ///
    /// # Errors
    ///
    /// Returns an error when the object is absent, corrupt, has the wrong
    /// length, or cannot be read.
    pub async fn read_object(&self, object: &ObjectRef) -> Result<Bytes, Error> {
        let stored = self
            .store
            .read(Self::object_key(object))
            .await?
            .ok_or_else(|| Error::InvalidFormat("a referenced object is missing".to_owned()))?;
        Self::verify_object(object, &stored.bytes)?;
        Ok(stored.bytes)
    }

    /// Builds one immutable candidate against an exact observed cursor.
    ///
    /// This operation does not access storage and does not rebase the opaque
    /// operation onto a newer cursor.
    ///
    /// # Errors
    ///
    /// Returns an error for a foreign cursor or a configured size or tail
    /// limit.
    pub fn prepare(
        &self,
        cursor: &Cursor,
        transaction_id: TransactionId,
        operation: Bytes,
        result: Bytes,
        objects: Vec<ObjectRef>,
    ) -> Result<PreparedCommit, Error> {
        self.validate_cursor(cursor)?;
        self.validate_prepared_sizes(&operation, &result)?;
        self.validate_object_count(&objects)?;
        if cursor.head.tail.len() >= self.options.max_tail_entries {
            return Err(Error::LimitExceeded("active tail entries"));
        }
        if cursor
            .head
            .tail
            .iter()
            .chain(&cursor.head.recent_outcomes)
            .any(|entry| entry.transaction_id == transaction_id)
        {
            return Err(Error::InvalidFormat(
                "the transaction ID is already committed".to_owned(),
            ));
        }
        Ok(PreparedCommit {
            cursor: cursor.clone(),
            transaction_id,
            operation,
            result,
            objects,
        })
    }

    /// Stages and conditionally publishes one exact prepared commit.
    ///
    /// A definite precondition failure returns [`CommitStatus::Conflict`]. A
    /// storage error during the head update returns [`CommitStatus::Pending`]
    /// because that error can hide a successful publication.
    ///
    /// # Errors
    ///
    /// Returns an error when the candidate is invalid, a referenced object is
    /// not durable and valid, immutable staging fails, or a conflict head
    /// cannot be read.
    pub async fn commit(&self, prepared: PreparedCommit) -> Result<CommitStatus, Error> {
        self.validate_prepared(&prepared).await?;
        let (commit_ref, commit_bytes) = self.encode_prepared(&prepared)?;
        self.create_immutable(StoreKey::Commit(commit_ref.digest), commit_bytes)
            .await?;
        let candidate = Self::candidate_head(&prepared, &commit_ref)?;
        let candidate_bytes = format::encode_head(&candidate)?;
        self.validate_encoded_head(&candidate_bytes)?;

        match self
            .store
            .update(
                StoreKey::Head,
                candidate_bytes,
                prepared.cursor.version.clone(),
            )
            .await
        {
            Ok(UpdateResult::Updated { version }) => Ok(CommitStatus::Committed(View {
                cursor: Cursor {
                    head: candidate,
                    version,
                },
            })),
            Ok(UpdateResult::PreconditionFailed) => Ok(CommitStatus::Conflict(self.load().await?)),
            Err(Error::Store(_)) => Ok(CommitStatus::Pending(PendingCommit {
                prepared,
                commit_ref,
            })),
            Err(error) => Err(error),
        }
    }

    /// Resolves or safely retries one uncertain head publication.
    ///
    /// The method retries only the original conditional update. It never
    /// rebases the operation onto a newer head.
    ///
    /// # Errors
    ///
    /// Returns an error when durable evidence is corrupt or does not belong to
    /// this log.
    pub async fn resolve(&self, pending: PendingCommit) -> Result<Resolution, Error> {
        self.validate_pending(&pending)?;
        let current = match self.load().await {
            Ok(view) => view,
            Err(Error::Store(_)) => return Ok(Resolution::StillPending(pending)),
            Err(error) => return Err(error),
        };

        if let Some(resolution) = Self::classify_resolution(&pending, current)? {
            if let Resolution::Committed(_) = &resolution {
                self.verify_published_commit(&pending.commit_ref).await?;
            }
            return Ok(resolution);
        }

        self.validate_prepared(&pending.prepared).await?;
        let (_, commit_bytes) = self.encode_prepared(&pending.prepared)?;
        self.verify_immutable(StoreKey::Commit(pending.commit_ref.digest), &commit_bytes)
            .await?;
        let candidate = Self::candidate_head(&pending.prepared, &pending.commit_ref)?;
        let candidate_bytes = format::encode_head(&candidate)?;
        self.validate_encoded_head(&candidate_bytes)?;
        match self
            .store
            .update(
                StoreKey::Head,
                candidate_bytes,
                pending.prepared.cursor.version.clone(),
            )
            .await
        {
            Ok(UpdateResult::Updated { version }) => Ok(Resolution::Committed(View {
                cursor: Cursor {
                    head: candidate,
                    version,
                },
            })),
            Err(Error::Store(_)) => Ok(Resolution::StillPending(pending)),
            Err(error) => Err(error),
            Ok(UpdateResult::PreconditionFailed) => {
                let current = match self.load().await {
                    Ok(view) => view,
                    Err(Error::Store(_)) => return Ok(Resolution::StillPending(pending)),
                    Err(error) => return Err(error),
                };
                Self::classify_resolution(&pending, current)?.ok_or_else(|| {
                    Error::InvalidFormat(
                        "head version changed without a monotonic head change".to_owned(),
                    )
                })
            }
        }
    }

    /// Reads and verifies every commit in the active tail.
    ///
    /// Object reads run concurrently. The returned records remain in sequence
    /// order.
    ///
    /// # Errors
    ///
    /// Returns an error for a foreign view, a missing or corrupt commit, a
    /// mismatched reference, or a broken parent chain.
    pub async fn read_tail(&self, view: &View) -> Result<Vec<CommitRecord>, Error> {
        self.validate_cursor(view.cursor())?;
        let records = try_join_all(
            view.tail()
                .iter()
                .map(|reference| self.read_commit(reference)),
        )
        .await?;

        let mut expected_tip = view
            .checkpoint()
            .map(|checkpoint| checkpoint.through_commit);
        for record in &records {
            if record.expected_tip != expected_tip {
                return Err(Error::InvalidFormat(
                    "the commit tail has a broken parent chain".to_owned(),
                ));
            }
            expected_tip = Some(record.reference.digest);
        }
        Ok(records)
    }

    fn view_from_stored(&self, stored: crate::store::StoredObject) -> Result<View, Error> {
        self.validate_encoded_head(&stored.bytes)?;
        let head = format::decode_head(&stored.bytes)?;
        if head.log_id != *self.store.log_id() {
            return Err(Error::InvalidFormat(
                "the durable head belongs to another log".to_owned(),
            ));
        }
        Ok(View {
            cursor: Cursor {
                head,
                version: stored.version,
            },
        })
    }

    fn validate_cursor(&self, cursor: &Cursor) -> Result<(), Error> {
        if cursor.head.log_id != *self.store.log_id() {
            return Err(Error::InvalidFormat(
                "the cursor belongs to another log".to_owned(),
            ));
        }
        cursor.head.validate()
    }

    fn validate_prepared_sizes(&self, operation: &Bytes, result: &Bytes) -> Result<(), Error> {
        if operation.len() > self.options.max_inline_operation_bytes {
            return Err(Error::LimitExceeded("inline operation bytes"));
        }
        if result.len() > self.options.max_inline_result_bytes {
            return Err(Error::LimitExceeded("inline result bytes"));
        }
        Ok(())
    }

    fn validate_object_count(&self, objects: &[ObjectRef]) -> Result<(), Error> {
        if objects.len() > self.options.max_object_refs_per_commit {
            return Err(Error::LimitExceeded("object references per commit"));
        }
        Ok(())
    }

    fn validate_encoded_head(&self, bytes: &Bytes) -> Result<(), Error> {
        if bytes.len() > self.options.max_head_bytes {
            return Err(Error::LimitExceeded("encoded head bytes"));
        }
        Ok(())
    }

    async fn validate_prepared(&self, prepared: &PreparedCommit) -> Result<(), Error> {
        self.validate_cursor(&prepared.cursor)?;
        self.validate_prepared_sizes(&prepared.operation, &prepared.result)?;
        self.validate_object_count(&prepared.objects)?;
        if prepared.cursor.head.tail.len() >= self.options.max_tail_entries {
            return Err(Error::LimitExceeded("active tail entries"));
        }
        try_join_all(
            prepared
                .objects
                .iter()
                .map(|object| self.read_object(object)),
        )
        .await?;
        Ok(())
    }

    fn encode_prepared(&self, prepared: &PreparedCommit) -> Result<(CommitRef, Bytes), Error> {
        let commit = format::Commit {
            log_id: self.store.log_id().clone(),
            transaction_id: prepared.transaction_id,
            expected_generation: prepared.cursor.head.generation,
            expected_tip: prepared.cursor.head.tip(),
            operation: prepared.operation.clone(),
            result: prepared.result.clone(),
            objects: prepared.objects.clone(),
        };
        let bytes = format::encode_commit(&commit)?;
        if bytes.len() > self.options.max_commit_bytes {
            return Err(Error::LimitExceeded("encoded commit bytes"));
        }
        let len =
            u64::try_from(bytes.len()).map_err(|_| Error::LimitExceeded("commit byte length"))?;
        let reference = CommitRef {
            sequence: prepared.cursor.head.next_sequence,
            transaction_id: prepared.transaction_id,
            digest: Digest::of(&bytes),
            len,
        };
        Ok((reference, bytes))
    }

    fn candidate_head(prepared: &PreparedCommit, commit_ref: &CommitRef) -> Result<Head, Error> {
        let mut head = prepared.cursor.head.clone();
        head.generation = head
            .generation
            .checked_add(1)
            .ok_or(Error::LimitExceeded("head generation"))?;
        head.next_sequence = head
            .next_sequence
            .checked_add(1)
            .ok_or(Error::LimitExceeded("commit sequence"))?;
        head.tail.push(commit_ref.clone());
        head.validate()?;
        Ok(head)
    }

    fn classify_resolution(
        pending: &PendingCommit,
        current: View,
    ) -> Result<Option<Resolution>, Error> {
        let target = &pending.commit_ref;
        let head = &current.cursor.head;
        if head
            .tail
            .iter()
            .chain(&head.recent_outcomes)
            .any(|entry| entry == target)
        {
            return Ok(Some(Resolution::Committed(current)));
        }

        if head.next_sequence > target.sequence {
            let exact_sequence_is_retained = head
                .tail
                .iter()
                .chain(&head.recent_outcomes)
                .any(|entry| entry.sequence == target.sequence);
            if exact_sequence_is_retained {
                return Ok(Some(Resolution::NotCommitted(current)));
            }
            return Ok(Some(Resolution::Expired(current)));
        }
        if head.next_sequence < target.sequence {
            return Err(Error::InvalidFormat(
                "the head precedes the pending commit position".to_owned(),
            ));
        }

        let source = &pending.prepared.cursor;
        if head == &source.head && current.cursor.version == source.version {
            return Ok(None);
        }
        Ok(Some(Resolution::NotCommitted(current)))
    }

    fn validate_pending(&self, pending: &PendingCommit) -> Result<(), Error> {
        self.validate_cursor(&pending.prepared.cursor)?;
        let (expected_ref, _) = self.encode_prepared(&pending.prepared)?;
        if expected_ref != pending.commit_ref {
            return Err(Error::InvalidFormat(
                "pending commit evidence does not match its candidate".to_owned(),
            ));
        }
        Ok(())
    }

    async fn read_commit(&self, reference: &CommitRef) -> Result<CommitRecord, Error> {
        if reference.len
            > u64::try_from(self.options.max_commit_bytes)
                .map_err(|_| Error::LimitExceeded("encoded commit bytes"))?
        {
            return Err(Error::LimitExceeded("encoded commit bytes"));
        }
        let stored = self
            .store
            .read(StoreKey::Commit(reference.digest))
            .await?
            .ok_or_else(|| Error::InvalidFormat("a referenced commit is missing".to_owned()))?;
        let len = u64::try_from(stored.bytes.len())
            .map_err(|_| Error::LimitExceeded("commit byte length"))?;
        if len != reference.len || Digest::of(&stored.bytes) != reference.digest {
            return Err(Error::CorruptObject);
        }
        let commit = format::decode_commit(&stored.bytes)?;
        if commit.log_id != *self.store.log_id()
            || commit.transaction_id != reference.transaction_id
        {
            return Err(Error::InvalidFormat(
                "a commit does not match its head reference".to_owned(),
            ));
        }
        Ok(CommitRecord {
            reference: reference.clone(),
            expected_generation: commit.expected_generation,
            expected_tip: commit.expected_tip,
            operation: commit.operation,
            result: commit.result,
            objects: commit.objects,
        })
    }

    async fn verify_published_commit(&self, reference: &CommitRef) -> Result<(), Error> {
        let record = self.read_commit(reference).await?;
        try_join_all(record.objects.iter().map(|object| self.read_object(object))).await?;
        Ok(())
    }

    async fn create_immutable(&self, key: StoreKey, bytes: Bytes) -> Result<(), Error> {
        match self.store.create(key, bytes.clone()).await {
            Ok(CreateResult::Created { .. }) => Ok(()),
            Ok(CreateResult::AlreadyExists) => self.verify_immutable(key, &bytes).await,
            Err(create_error) => match self.store.read(key).await? {
                Some(stored) if stored.bytes == bytes => Ok(()),
                Some(_) => Err(Error::CorruptObject),
                None => Err(create_error),
            },
        }
    }

    async fn verify_immutable(&self, key: StoreKey, expected: &Bytes) -> Result<(), Error> {
        let stored = self
            .store
            .read(key)
            .await?
            .ok_or_else(|| Error::InvalidFormat("an immutable object is missing".to_owned()))?;
        if stored.bytes != *expected {
            return Err(Error::CorruptObject);
        }
        Ok(())
    }

    fn object_key(object: &ObjectRef) -> StoreKey {
        match object.kind {
            ObjectKind::Blob => StoreKey::Blob(object.digest),
            ObjectKind::Checkpoint => StoreKey::Checkpoint(object.digest),
        }
    }

    fn verify_object(object: &ObjectRef, bytes: &Bytes) -> Result<(), Error> {
        let actual_len =
            u64::try_from(bytes.len()).map_err(|_| Error::LimitExceeded("object byte length"))?;
        if actual_len != object.len || Digest::of(bytes) != object.digest {
            return Err(Error::CorruptObject);
        }
        Ok(())
    }
}
