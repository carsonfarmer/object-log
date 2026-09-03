//! Object-log publication protocol.

use bytes::Bytes;
use futures::{StreamExt, TryStreamExt, stream};

use crate::format::{self, Head};
use crate::store::{
    ConditionalRead, CreateResult, ImmutableKey, ImmutableKind, ScopedStore, StoreKey, UpdateResult,
};
use crate::{
    CheckpointRef, CommitRef, Cursor, Digest, Error, LogId, ObjectKind, ObjectRef,
    PendingCheckpoint, PendingCommit, PreparedCommit, StorageId, TransactionId,
};

const MAX_CONCURRENT_READS: usize = 32;

/// Limits applied by one log writer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Options {
    /// Maximum commit references in the active tail.
    pub max_tail_entries: usize,
    /// Maximum compacted commit outcomes retained for resolution.
    pub resolution_window: usize,
    /// Maximum inline operation bytes in one commit.
    pub max_inline_operation_bytes: usize,
    /// Maximum inline result bytes in one commit.
    pub max_inline_result_bytes: usize,
    /// Maximum immutable references in one commit, checkpoint, or node.
    pub max_object_refs: usize,
    /// Maximum bytes in one immutable object.
    pub max_object_bytes: usize,
    /// Maximum encoded bytes in one WAL entry.
    pub max_commit_bytes: usize,
    /// Maximum encoded bytes in the mutable head.
    pub max_head_bytes: usize,
    /// Maximum encoded bytes in one checkpoint.
    pub max_checkpoint_bytes: usize,
    /// Maximum concurrent retention identities in the head.
    pub max_retention_ids: usize,
    /// Maximum objects in one collection scan, live graph, or deletion plan.
    pub max_collection_objects: usize,
    /// Maximum encoded bytes in one collection plan.
    pub max_collection_plan_bytes: usize,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            max_tail_entries: 1_024,
            resolution_window: 1_024,
            max_inline_operation_bytes: 64 * 1_024,
            max_inline_result_bytes: 4 * 1_024,
            max_object_refs: 1_024,
            max_object_bytes: 64 * 1024 * 1024,
            max_commit_bytes: 1024 * 1024,
            max_head_bytes: 256 * 1024,
            max_checkpoint_bytes: 16 * 1024 * 1024,
            max_retention_ids: 1_024,
            max_collection_objects: 100_000,
            max_collection_plan_bytes: 16 * 1024 * 1024,
        }
    }
}

/// One observed durable head.
#[derive(Clone, Debug)]
pub struct View {
    pub(crate) cursor: Cursor,
}

impl View {
    /// Returns the opaque cursor for conditional work against this view.
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

    /// Returns the current garbage-collection epoch.
    #[must_use]
    pub const fn collection_epoch(&self) -> u64 {
        self.cursor.head.collection_epoch
    }
}

/// The result of a conditional head refresh.
#[derive(Debug)]
pub enum Refresh {
    /// The durable head still matches the supplied cursor.
    NotModified,
    /// The durable head changed and this is its current view.
    Updated(Box<View>),
}

/// The immediate result of publishing one prepared commit.
#[derive(Debug)]
pub enum CommitStatus {
    /// The exact candidate is durable and visible.
    Committed(View),
    /// Another head update definitely rejected this candidate.
    Conflict(View),
    /// The safe final view or classification is not available yet.
    Pending(PendingCommit),
}

/// The result of resolving one uncertain publication.
#[derive(Debug)]
pub enum Resolution {
    /// The exact candidate is durable and visible.
    Committed(View),
    /// Retained evidence proves that the candidate did not publish.
    NotCommitted(View),
    /// Storage is not available enough to determine the result.
    StillPending(PendingCommit),
    /// The outcome evidence is no longer retained.
    ///
    /// This does not mean `NotCommitted`. Do not submit a non-idempotent
    /// operation again as new work after this result.
    Expired(View),
}

/// The result of publishing a checkpoint.
#[derive(Debug)]
pub enum CheckpointStatus {
    /// The exact checkpoint is durable and visible.
    Published(View),
    /// Another head update definitely rejected this checkpoint.
    Conflict(View),
    /// The safe final view or classification is not available yet.
    Pending(PendingCheckpoint),
}

/// The result of resolving one uncertain checkpoint publication.
#[derive(Debug)]
pub enum CheckpointResolution {
    /// The exact checkpoint is durable and visible.
    Published(View),
    /// Retained evidence proves that the checkpoint did not publish.
    NotPublished(View),
    /// Storage is not available enough to determine the result.
    StillPending(PendingCheckpoint),
    /// Later head updates removed conclusive publication evidence.
    Expired(View),
}

/// The result of one retention head update.
#[derive(Debug)]
pub enum RetentionStatus {
    /// The requested retention state is durable.
    Applied(View),
    /// An active collection fence blocks a new retention.
    ActiveCollection(View),
    /// Another head update rejected the requested change.
    Conflict(View),
    /// A storage error can hide a successful head update.
    Pending,
}

/// Counts reported for one collection operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CollectionReport {
    candidate_count: usize,
    candidate_bytes: u64,
    delete_attempts: usize,
}

impl CollectionReport {
    /// Returns the number of immutable deletion candidates.
    #[must_use]
    pub const fn candidate_count(&self) -> usize {
        self.candidate_count
    }

    /// Returns the total listed bytes for all deletion candidates.
    #[must_use]
    pub const fn candidate_bytes(&self) -> u64 {
        self.candidate_bytes
    }

    /// Returns the number of immutable delete requests issued.
    #[must_use]
    pub const fn delete_attempts(&self) -> usize {
        self.delete_attempts
    }
}

/// The result of creating or observing a collection fence.
#[derive(Debug)]
pub enum CollectionStart {
    /// No immutable objects need deletion.
    Empty(CollectionReport),
    /// A positive deletion plan is durable and active.
    Installed(View, CollectionReport),
    /// The supplied view already has an active collection fence.
    Active(View),
    /// An active retention prevents fence installation.
    Retained(View),
    /// Another head update rejected fence installation.
    Conflict(View),
    /// A storage error can hide a successful fence update.
    Pending,
}

/// The result of deleting and clearing one active collection plan.
#[derive(Debug)]
pub enum CollectionFinish {
    /// Every candidate is absent and the exact fence is clear.
    Complete(View, CollectionReport),
    /// Another head update prevented the exact fence from being cleared.
    Conflict(View, CollectionReport),
    /// A delete or head error leaves completion uncertain.
    Pending(CollectionReport),
}

enum CheckpointEvidence {
    Published(View),
    NotPublished(View),
    Expired(View),
    Retry,
}

/// One decoded commit joined with its ordered head reference.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommitRecord {
    reference: CommitRef,
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

/// One decoded checkpoint and its declared immutable dependencies.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CheckpointRecord {
    snapshot: Bytes,
    objects: Vec<ObjectRef>,
}

impl CheckpointRecord {
    /// Returns the caller-defined snapshot bytes.
    #[must_use]
    pub const fn snapshot(&self) -> &Bytes {
        &self.snapshot
    }

    /// Returns every immutable object needed to restore the snapshot.
    #[must_use]
    pub fn objects(&self) -> &[ObjectRef] {
        &self.objects
    }
}

/// One immutable node with opaque payload and traversable child references.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReferenceNode {
    payload: Bytes,
    children: Vec<ObjectRef>,
}

impl ReferenceNode {
    /// Returns the caller-defined node payload.
    #[must_use]
    pub const fn payload(&self) -> &Bytes {
        &self.payload
    }

    /// Returns the node's direct immutable children.
    #[must_use]
    pub fn children(&self) -> &[ObjectRef] {
        &self.children
    }
}

/// A linearizable log in one namespace-safe object-store scope.
#[derive(Clone, Debug)]
pub struct Log {
    store: ScopedStore,
    options: Options,
    incarnation: uuid::Uuid,
}

impl Log {
    /// Opens a writable log and creates its initial head when it is absent.
    ///
    /// # Errors
    ///
    /// Returns an error when the head cannot be created or read, or an existing
    /// head is invalid.
    pub async fn open(store: ScopedStore, options: Options) -> Result<Self, Error> {
        let initial = Head::empty(store.log_id().clone(), uuid::Uuid::new_v4(), options);
        let initial_bytes = format::encode_head(&initial)?;
        Self::validate_head_size(options, &initial_bytes)?;

        let incarnation = match store.create(StoreKey::Head, initial_bytes).await {
            Ok(CreateResult::Created { .. }) => initial.incarnation,
            Ok(CreateResult::AlreadyExists) => Self::load_incarnation(&store, options).await?,
            Err(create_error) => match store.read(StoreKey::Head, options.max_head_bytes).await? {
                Some(stored) => Self::incarnation_from_stored(&store, options, &stored)?,
                None => return Err(create_error),
            },
        };
        Ok(Self {
            store,
            options,
            incarnation,
        })
    }

    /// Loads and verifies the current durable head.
    ///
    /// # Errors
    ///
    /// Returns an error when the head is missing, unreadable, corrupt, or
    /// belongs to a different log identity.
    pub async fn load(&self) -> Result<View, Error> {
        let stored = self
            .store
            .read(StoreKey::Head, self.options.max_head_bytes)
            .await?
            .ok_or_else(|| Error::InvalidFormat("the opened log has no durable head".to_owned()))?;
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
            .read_if_changed(
                StoreKey::Head,
                cursor.storage_version(),
                self.options.max_head_bytes,
            )
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

    /// Stores one immutable content-addressed blob with a fresh physical identity.
    ///
    /// # Errors
    ///
    /// Returns an error when the byte length cannot be represented or the
    /// backend fails. A physical identity collision is retried with a new ID.
    pub async fn put_object(&self, bytes: Bytes) -> Result<ObjectRef, Error> {
        if bytes.len() > self.options.max_object_bytes {
            return Err(Error::LimitExceeded("object bytes"));
        }
        self.create_fresh_object(ObjectKind::Blob, bytes).await
    }

    /// Stores one immutable reference node after its direct children exist.
    ///
    /// The opaque payload can describe an adapter-specific tree node. All
    /// durable child objects must appear in `children` so a generic collector
    /// can traverse the complete graph.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid dependencies, configured limits, missing
    /// or corrupt children, or a backend failure.
    pub async fn put_node(
        &self,
        payload: Bytes,
        children: Vec<ObjectRef>,
    ) -> Result<ObjectRef, Error> {
        self.validate_dependencies(&children)?;
        let node = format::Node { payload, children };
        let bytes = format::encode_node(&node)?;
        if bytes.len() > self.options.max_object_bytes {
            return Err(Error::LimitExceeded("object bytes"));
        }
        self.verify_objects(&node.children).await?;
        self.create_fresh_object(ObjectKind::Node, bytes).await
    }

    /// Reads and verifies one object from this log namespace.
    ///
    /// # Errors
    ///
    /// Returns an error when the object is absent, corrupt, has the wrong
    /// length, or cannot be read.
    pub async fn read_object(&self, object: &ObjectRef) -> Result<Bytes, Error> {
        if object.kind != ObjectKind::Blob {
            return Err(Error::InvalidFormat(
                "a payload read requires a blob reference".to_owned(),
            ));
        }
        self.read_immutable(object).await
    }

    /// Reads and verifies one immutable reference node.
    ///
    /// Child objects remain lazy. Call [`Log::read_object`] or
    /// [`Log::read_node`] for each child that the adapter needs.
    ///
    /// # Errors
    ///
    /// Returns an error for the wrong object kind, invalid node data,
    /// configured limits, or a backend failure.
    pub async fn read_node(&self, object: &ObjectRef) -> Result<ReferenceNode, Error> {
        if object.kind != ObjectKind::Node {
            return Err(Error::InvalidFormat(
                "a node read requires a reference-node object".to_owned(),
            ));
        }
        let bytes = self.read_immutable(object).await?;
        let node = format::decode_node(&bytes)?;
        self.validate_dependencies(&node.children)?;
        Ok(ReferenceNode {
            payload: node.payload,
            children: node.children,
        })
    }

    async fn read_immutable(&self, object: &ObjectRef) -> Result<Bytes, Error> {
        let declared_len =
            usize::try_from(object.len).map_err(|_| Error::LimitExceeded("object byte length"))?;
        if declared_len > self.options.max_object_bytes {
            return Err(Error::LimitExceeded("object bytes"));
        }
        let stored = self
            .store
            .read(self.object_key(object), declared_len)
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
        self.validate_dependencies(&objects)?;
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
            storage_id: StorageId::new(),
            operation,
            result,
            objects,
        })
    }

    /// Stages and conditionally publishes one exact prepared commit.
    ///
    /// A definite precondition failure returns [`CommitStatus::Conflict`] when
    /// the winning view can also be read. [`CommitStatus::Pending`] preserves
    /// the candidate when the safe final view or classification is unavailable.
    ///
    /// # Errors
    ///
    /// Returns an error when the candidate is invalid, a referenced object is
    /// not durable and valid, immutable staging fails, or a winning head is
    /// invalid.
    pub async fn commit(&self, prepared: PreparedCommit) -> Result<CommitStatus, Error> {
        self.validate_prepared(&prepared).await?;
        let (commit_ref, commit_bytes) = self.encode_prepared(&prepared)?;
        self.create_new_commit(self.commit_key(&commit_ref), commit_bytes)
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
            Ok(UpdateResult::PreconditionFailed) => {
                let pending = PendingCommit {
                    prepared,
                    commit_ref,
                };
                let current = match self.load().await {
                    Ok(view) => view,
                    Err(Error::Store(_)) => return Ok(CommitStatus::Pending(pending)),
                    Err(error) => return Err(error),
                };
                match Self::classify_resolution(&pending, current)? {
                    Some(Resolution::Committed(view)) => {
                        match self.verify_published_commit(&pending.commit_ref).await {
                            Ok(()) => Ok(CommitStatus::Committed(view)),
                            Err(Error::Store(_)) => Ok(CommitStatus::Pending(pending)),
                            Err(error) => Err(error),
                        }
                    }
                    Some(Resolution::NotCommitted(view)) => Ok(CommitStatus::Conflict(view)),
                    Some(Resolution::Expired(_)) | None => Ok(CommitStatus::Pending(pending)),
                    Some(Resolution::StillPending(_)) => Err(Error::InvalidFormat(
                        "an in-memory classification returned pending evidence".to_owned(),
                    )),
                }
            }
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
                match self.verify_published_commit(&pending.commit_ref).await {
                    Ok(()) => {}
                    Err(Error::Store(_)) => return Ok(Resolution::StillPending(pending)),
                    Err(error) => return Err(error),
                }
            }
            return Ok(resolution);
        }

        match self.verify_objects(&pending.prepared.objects).await {
            Ok(()) => {}
            Err(Error::Store(_)) => return Ok(Resolution::StillPending(pending)),
            Err(error) => return Err(error),
        }
        let (_, commit_bytes) = self.encode_prepared(&pending.prepared)?;
        match self
            .ensure_immutable(self.commit_key(&pending.commit_ref), commit_bytes)
            .await
        {
            Ok(()) => {}
            Err(Error::Store(_)) => return Ok(Resolution::StillPending(pending)),
            Err(error) => return Err(error),
        }
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

    /// Resumes one exact candidate from a token persisted before publication.
    ///
    /// This can stage a missing immutable entry and retry only the original
    /// conditional index update. It never rebases the operation.
    ///
    /// # Errors
    ///
    /// Returns an error when the token is invalid, belongs to another log, or
    /// names corrupt durable data.
    pub async fn resume(&self, token: &[u8]) -> Result<Resolution, Error> {
        let prepared = format::decode_recovery_token(token)?;
        let (commit_ref, _) = self.encode_prepared(&prepared)?;
        self.resolve(PendingCommit {
            prepared,
            commit_ref,
        })
        .await
    }

    /// Reads and verifies every commit in the active tail.
    ///
    /// Commit reads run concurrently. The returned records remain in sequence
    /// order. Referenced objects are loaded and verified only when the caller
    /// passes their references to [`Log::read_object`].
    ///
    /// # Errors
    ///
    /// Returns an error for a foreign view, a missing or corrupt commit, a
    /// mismatched reference, or a broken parent chain.
    pub async fn read_tail(&self, view: &View) -> Result<Vec<CommitRecord>, Error> {
        self.validate_cursor(view.cursor())?;
        let records = stream::iter(
            view.tail()
                .iter()
                .map(|reference| self.read_commit(reference)),
        )
        .buffered(MAX_CONCURRENT_READS)
        .try_collect::<Vec<_>>()
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

    /// Publishes an opaque base that covers one exact prefix of `view`.
    ///
    /// The base object becomes durable before the index update. A concurrent
    /// index update returns [`CheckpointStatus::Conflict`] and preserves the
    /// current durable history.
    ///
    /// # Errors
    ///
    /// Returns an error for a foreign view, a reference outside its active
    /// tail, an oversized base, invalid history, or a backend failure. A store
    /// error during the index update can hide a successful maintenance update.
    /// The method then returns [`CheckpointStatus::Pending`]. The caller must
    /// preserve that evidence and pass it to [`Log::resolve_checkpoint`].
    pub async fn publish_checkpoint(
        &self,
        view: &View,
        through: &CommitRef,
        snapshot: Bytes,
        objects: Vec<ObjectRef>,
    ) -> Result<CheckpointStatus, Error> {
        self.read_tail(view).await?;
        self.validate_dependencies(&objects)?;
        self.verify_objects(&objects).await?;
        let checkpoint = format::Checkpoint {
            log_id: self.store.log_id().clone(),
            incarnation: self.incarnation,
            through_sequence: through.sequence,
            through_commit: through.digest,
            snapshot,
            objects,
        };
        let bytes = format::encode_checkpoint(&checkpoint)?;
        self.validate_checkpoint_bytes(bytes.len())?;
        let object = self
            .create_fresh_object(ObjectKind::Checkpoint, bytes)
            .await?;
        let candidate = Self::checkpoint_head(view, through, object.clone())?;
        let candidate_bytes = format::encode_head(&candidate)?;
        self.validate_encoded_head(&candidate_bytes)?;
        let pending = PendingCheckpoint {
            view: view.clone(),
            through: through.clone(),
            checkpoint: CheckpointRef {
                through_sequence: through.sequence,
                through_commit: through.digest,
                object,
            },
        };

        match self
            .store
            .update(StoreKey::Head, candidate_bytes, view.cursor.version.clone())
            .await
        {
            Ok(UpdateResult::Updated { version }) => Ok(CheckpointStatus::Published(View {
                cursor: Cursor {
                    head: candidate,
                    version,
                },
            })),
            Ok(UpdateResult::PreconditionFailed) => {
                let current = match self.load().await {
                    Ok(view) => view,
                    Err(Error::Store(_)) => return Ok(CheckpointStatus::Pending(pending)),
                    Err(error) => return Err(error),
                };
                match Self::classify_checkpoint(&pending, current)? {
                    CheckpointEvidence::Published(view) => {
                        match self.verify_checkpoint(&pending.checkpoint).await {
                            Ok(()) => Ok(CheckpointStatus::Published(view)),
                            Err(Error::Store(_)) => Ok(CheckpointStatus::Pending(pending)),
                            Err(error) => Err(error),
                        }
                    }
                    CheckpointEvidence::NotPublished(view) => Ok(CheckpointStatus::Conflict(view)),
                    CheckpointEvidence::Expired(_) | CheckpointEvidence::Retry => {
                        Ok(CheckpointStatus::Pending(pending))
                    }
                }
            }
            Err(Error::Store(_)) => Ok(CheckpointStatus::Pending(pending)),
            Err(error) => Err(error),
        }
    }

    /// Resolves or safely retries one uncertain checkpoint publication.
    ///
    /// It retries only the original conditional index update. It never applies
    /// the snapshot to a different log prefix.
    ///
    /// # Errors
    ///
    /// Returns an error when the evidence or durable checkpoint is invalid.
    pub async fn resolve_checkpoint(
        &self,
        pending: PendingCheckpoint,
    ) -> Result<CheckpointResolution, Error> {
        self.validate_cursor(pending.view.cursor())?;
        let candidate = Self::checkpoint_head(
            &pending.view,
            &pending.through,
            pending.checkpoint.object.clone(),
        )?;
        if candidate.checkpoint.as_ref() != Some(&pending.checkpoint) {
            return Err(Error::InvalidFormat(
                "pending checkpoint evidence does not match its candidate".to_owned(),
            ));
        }

        let current = match self.load().await {
            Ok(view) => view,
            Err(Error::Store(_)) => return Ok(CheckpointResolution::StillPending(pending)),
            Err(error) => return Err(error),
        };
        match Self::classify_checkpoint(&pending, current)? {
            CheckpointEvidence::Published(view) => {
                return match self.verify_checkpoint(&pending.checkpoint).await {
                    Ok(()) => Ok(CheckpointResolution::Published(view)),
                    Err(Error::Store(_)) => Ok(CheckpointResolution::StillPending(pending)),
                    Err(error) => Err(error),
                };
            }
            CheckpointEvidence::NotPublished(view) => {
                return Ok(CheckpointResolution::NotPublished(view));
            }
            CheckpointEvidence::Expired(view) => {
                return Ok(CheckpointResolution::Expired(view));
            }
            CheckpointEvidence::Retry => {}
        }

        match self.read_tail(&pending.view).await {
            Ok(_) => {}
            Err(Error::Store(_)) => return Ok(CheckpointResolution::StillPending(pending)),
            Err(error) => return Err(error),
        }
        match self.verify_checkpoint(&pending.checkpoint).await {
            Ok(()) => {}
            Err(Error::Store(_)) => return Ok(CheckpointResolution::StillPending(pending)),
            Err(error) => return Err(error),
        }
        let candidate_bytes = format::encode_head(&candidate)?;
        self.validate_encoded_head(&candidate_bytes)?;
        match self
            .store
            .update(
                StoreKey::Head,
                candidate_bytes,
                pending.view.cursor.version.clone(),
            )
            .await
        {
            Ok(UpdateResult::Updated { version }) => Ok(CheckpointResolution::Published(View {
                cursor: Cursor {
                    head: candidate,
                    version,
                },
            })),
            Err(Error::Store(_)) => Ok(CheckpointResolution::StillPending(pending)),
            Err(error) => Err(error),
            Ok(UpdateResult::PreconditionFailed) => {
                let current = match self.load().await {
                    Ok(view) => view,
                    Err(Error::Store(_)) => {
                        return Ok(CheckpointResolution::StillPending(pending));
                    }
                    Err(error) => return Err(error),
                };
                match Self::classify_checkpoint(&pending, current)? {
                    CheckpointEvidence::Published(view) => {
                        Ok(CheckpointResolution::Published(view))
                    }
                    CheckpointEvidence::NotPublished(view) => {
                        Ok(CheckpointResolution::NotPublished(view))
                    }
                    CheckpointEvidence::Expired(view) => Ok(CheckpointResolution::Expired(view)),
                    CheckpointEvidence::Retry => Ok(CheckpointResolution::StillPending(pending)),
                }
            }
        }
    }

    /// Reads and verifies the base snapshot referenced by `view`.
    ///
    /// The returned object references remain lazy. This method verifies the
    /// checkpoint envelope, but it does not read the declared root objects.
    ///
    /// # Errors
    ///
    /// Returns an error when the view is foreign or its base is missing,
    /// oversized, corrupt, or does not cover the declared entry.
    pub async fn read_checkpoint(&self, view: &View) -> Result<Option<CheckpointRecord>, Error> {
        self.validate_cursor(view.cursor())?;
        let Some(reference) = view.checkpoint() else {
            return Ok(None);
        };
        let declared_len = usize::try_from(reference.object.len)
            .map_err(|_| Error::LimitExceeded("checkpoint byte length"))?;
        self.validate_checkpoint_bytes(declared_len)?;
        let checkpoint = self.load_checkpoint(reference).await?;
        Ok(Some(CheckpointRecord {
            snapshot: checkpoint.snapshot,
            objects: checkpoint.objects,
        }))
    }

    async fn verify_checkpoint(&self, reference: &CheckpointRef) -> Result<(), Error> {
        let checkpoint = self.load_checkpoint(reference).await?;
        self.verify_objects(&checkpoint.objects).await
    }

    async fn load_checkpoint(
        &self,
        reference: &CheckpointRef,
    ) -> Result<format::Checkpoint, Error> {
        let bytes = self.read_immutable(&reference.object).await?;
        let checkpoint = format::decode_checkpoint(&bytes)?;
        self.validate_dependencies(&checkpoint.objects)?;
        if checkpoint.log_id != *self.store.log_id()
            || checkpoint.incarnation != self.incarnation
            || checkpoint.through_sequence != reference.through_sequence
            || checkpoint.through_commit != reference.through_commit
        {
            return Err(Error::InvalidFormat(
                "a checkpoint does not match its index reference".to_owned(),
            ));
        }
        Ok(checkpoint)
    }

    fn view_from_stored(&self, stored: crate::store::StoredObject) -> Result<View, Error> {
        let head = format::decode_head(&stored.bytes)?;
        if head.log_id != *self.store.log_id() {
            return Err(Error::InvalidFormat(
                "the durable head belongs to another log".to_owned(),
            ));
        }
        if head.incarnation != self.incarnation {
            return Err(Error::InvalidFormat(
                "the durable head belongs to another log incarnation".to_owned(),
            ));
        }
        if head.options != self.options {
            return Err(Error::ConfigurationMismatch("options"));
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
        if cursor.head.incarnation != self.incarnation {
            return Err(Error::InvalidFormat(
                "the cursor belongs to another log incarnation".to_owned(),
            ));
        }
        if cursor.head.options != self.options {
            return Err(Error::ConfigurationMismatch("options"));
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
        if objects.len() > self.options.max_object_refs {
            return Err(Error::LimitExceeded("object references"));
        }
        Ok(())
    }

    fn validate_dependencies(&self, objects: &[ObjectRef]) -> Result<(), Error> {
        self.validate_object_count(objects)?;
        if objects
            .iter()
            .any(|object| object.kind == ObjectKind::Checkpoint)
        {
            return Err(Error::InvalidFormat(
                "application dependencies cannot name a checkpoint".to_owned(),
            ));
        }
        Ok(())
    }

    fn validate_encoded_head(&self, bytes: &Bytes) -> Result<(), Error> {
        Self::validate_head_size(self.options, bytes)
    }

    fn validate_head_size(options: Options, bytes: &Bytes) -> Result<(), Error> {
        if bytes.len() > options.max_head_bytes {
            return Err(Error::LimitExceeded("encoded head bytes"));
        }
        Ok(())
    }

    fn validate_checkpoint_bytes(&self, len: usize) -> Result<(), Error> {
        if len > self.options.max_checkpoint_bytes {
            return Err(Error::LimitExceeded("encoded checkpoint bytes"));
        }
        if len > self.options.max_object_bytes {
            return Err(Error::LimitExceeded("object bytes"));
        }
        Ok(())
    }

    async fn validate_prepared(&self, prepared: &PreparedCommit) -> Result<(), Error> {
        self.validate_cursor(&prepared.cursor)?;
        self.validate_prepared_sizes(&prepared.operation, &prepared.result)?;
        self.validate_dependencies(&prepared.objects)?;
        if prepared.cursor.head.tail.len() >= self.options.max_tail_entries {
            return Err(Error::LimitExceeded("active tail entries"));
        }
        self.verify_objects(&prepared.objects).await?;
        Ok(())
    }

    fn encode_prepared(&self, prepared: &PreparedCommit) -> Result<(CommitRef, Bytes), Error> {
        let commit = format::Commit {
            log_id: self.store.log_id().clone(),
            incarnation: self.incarnation,
            transaction_id: prepared.transaction_id,
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
            storage_id: prepared.storage_id,
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
        Ok(head)
    }

    fn checkpoint_head(view: &View, through: &CommitRef, object: ObjectRef) -> Result<Head, Error> {
        let mut head = view.cursor.head.clone();
        let through_index = head
            .tail
            .iter()
            .position(|entry| entry == through)
            .ok_or_else(|| {
                Error::InvalidFormat("the checkpoint entry is not in the active tail".to_owned())
            })?;
        let removed = head.tail.drain(..=through_index).collect::<Vec<_>>();
        head.recent_outcomes.extend(removed);
        let resolution_window = head.options.resolution_window;
        if head.recent_outcomes.len() > resolution_window {
            let excess = head.recent_outcomes.len().saturating_sub(resolution_window);
            head.recent_outcomes.drain(..excess);
        }
        head.checkpoint = Some(CheckpointRef {
            through_sequence: through.sequence,
            through_commit: through.digest,
            object,
        });
        head.generation = head
            .generation
            .checked_add(1)
            .ok_or(Error::LimitExceeded("head generation"))?;
        Ok(head)
    }

    fn classify_resolution(
        pending: &PendingCommit,
        current: View,
    ) -> Result<Option<Resolution>, Error> {
        let target = &pending.commit_ref;
        let head = &current.cursor.head;
        if Self::contains_commit(&current, target) {
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

    fn contains_commit(view: &View, target: &CommitRef) -> bool {
        view.cursor
            .head
            .tail
            .iter()
            .chain(&view.cursor.head.recent_outcomes)
            .any(|entry| entry == target)
    }

    fn classify_checkpoint(
        pending: &PendingCheckpoint,
        current: View,
    ) -> Result<CheckpointEvidence, Error> {
        if current.checkpoint() == Some(&pending.checkpoint) {
            return Ok(CheckpointEvidence::Published(current));
        }
        if current.cursor.head == pending.view.cursor.head
            && current.cursor.version == pending.view.cursor.version
        {
            return Ok(CheckpointEvidence::Retry);
        }
        let next_generation = pending
            .view
            .cursor
            .head
            .generation
            .checked_add(1)
            .ok_or(Error::LimitExceeded("head generation"))?;
        match current.cursor.head.generation.cmp(&next_generation) {
            std::cmp::Ordering::Less => Err(Error::InvalidFormat(
                "the head precedes pending checkpoint evidence".to_owned(),
            )),
            std::cmp::Ordering::Equal => Ok(CheckpointEvidence::NotPublished(current)),
            std::cmp::Ordering::Greater => Ok(CheckpointEvidence::Expired(current)),
        }
    }

    fn validate_pending(&self, pending: &PendingCommit) -> Result<(), Error> {
        self.validate_cursor(&pending.prepared.cursor)?;
        self.validate_prepared_sizes(&pending.prepared.operation, &pending.prepared.result)?;
        self.validate_dependencies(&pending.prepared.objects)?;
        if pending.prepared.cursor.head.tail.len() >= self.options.max_tail_entries {
            return Err(Error::LimitExceeded("active tail entries"));
        }
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
            .read(self.commit_key(reference), self.options.max_commit_bytes)
            .await?
            .ok_or_else(|| Error::InvalidFormat("a referenced commit is missing".to_owned()))?;
        let len = u64::try_from(stored.bytes.len())
            .map_err(|_| Error::LimitExceeded("commit byte length"))?;
        if len != reference.len || Digest::of(&stored.bytes) != reference.digest {
            return Err(Error::CorruptObject);
        }
        let commit = format::decode_commit(&stored.bytes)?;
        self.validate_prepared_sizes(&commit.operation, &commit.result)?;
        self.validate_dependencies(&commit.objects)?;
        if commit.log_id != *self.store.log_id()
            || commit.incarnation != self.incarnation
            || commit.transaction_id != reference.transaction_id
        {
            return Err(Error::InvalidFormat(
                "a commit does not match its head reference".to_owned(),
            ));
        }
        Ok(CommitRecord {
            reference: reference.clone(),
            expected_tip: commit.expected_tip,
            operation: commit.operation,
            result: commit.result,
            objects: commit.objects,
        })
    }

    async fn verify_published_commit(&self, reference: &CommitRef) -> Result<(), Error> {
        let record = self.read_commit(reference).await?;
        self.verify_objects(&record.objects).await
    }

    async fn verify_objects(&self, objects: &[ObjectRef]) -> Result<(), Error> {
        stream::iter(objects.iter().map(Ok::<_, Error>))
            .try_for_each_concurrent(MAX_CONCURRENT_READS, |object| async move {
                self.verify_object_durable(object).await
            })
            .await
    }

    async fn verify_object_durable(&self, object: &ObjectRef) -> Result<(), Error> {
        let declared_len =
            usize::try_from(object.len).map_err(|_| Error::LimitExceeded("object byte length"))?;
        if declared_len > self.options.max_object_bytes {
            return Err(Error::LimitExceeded("object bytes"));
        }
        let (digest, len) = self
            .store
            .read_integrity(self.object_key(object), declared_len)
            .await?
            .ok_or_else(|| Error::InvalidFormat("a referenced object is missing".to_owned()))?;
        if digest != object.digest || len != object.len {
            return Err(Error::CorruptObject);
        }
        Ok(())
    }

    async fn create_new_commit(&self, key: StoreKey, bytes: Bytes) -> Result<(), Error> {
        match self.store.create(key, bytes).await? {
            CreateResult::Created { .. } => Ok(()),
            CreateResult::AlreadyExists => Err(Error::PhysicalIdentityCollision),
        }
    }

    async fn ensure_immutable(&self, key: StoreKey, bytes: Bytes) -> Result<(), Error> {
        match self.store.create(key, bytes.clone()).await {
            Ok(CreateResult::Created { .. }) => Ok(()),
            Ok(CreateResult::AlreadyExists) => self.verify_immutable(key, &bytes).await,
            Err(create_error) => match self.store.read(key, bytes.len()).await? {
                Some(stored) if stored.bytes == bytes => Ok(()),
                Some(_) => Err(Error::CorruptObject),
                None => Err(create_error),
            },
        }
    }

    async fn create_fresh_object(
        &self,
        kind: ObjectKind,
        bytes: Bytes,
    ) -> Result<ObjectRef, Error> {
        self.create_fresh_object_with(kind, bytes, StorageId::new)
            .await
    }

    async fn create_fresh_object_with(
        &self,
        kind: ObjectKind,
        bytes: Bytes,
        mut new_storage_id: impl FnMut() -> StorageId,
    ) -> Result<ObjectRef, Error> {
        const MAX_ATTEMPTS: usize = 16;

        let len =
            u64::try_from(bytes.len()).map_err(|_| Error::LimitExceeded("object byte length"))?;
        let digest = Digest::of(&bytes);
        for _ in 0..MAX_ATTEMPTS {
            let object = ObjectRef {
                kind,
                storage_id: new_storage_id(),
                digest,
                len,
            };
            let key = self.object_key(&object);
            match self.store.create(key, bytes.clone()).await {
                Ok(CreateResult::Created { .. }) => return Ok(object),
                Ok(CreateResult::AlreadyExists) => {}
                Err(create_error) => return Err(create_error),
            }
        }
        Err(Error::LimitExceeded("fresh physical storage identity"))
    }

    async fn verify_immutable(&self, key: StoreKey, expected: &Bytes) -> Result<(), Error> {
        let stored = self
            .store
            .read(key, expected.len())
            .await?
            .ok_or_else(|| Error::InvalidFormat("an immutable object is missing".to_owned()))?;
        if stored.bytes != *expected {
            return Err(Error::CorruptObject);
        }
        Ok(())
    }

    fn object_key(&self, object: &ObjectRef) -> StoreKey {
        let kind = match object.kind {
            ObjectKind::Blob => ImmutableKind::Blob,
            ObjectKind::Node => ImmutableKind::Node,
            ObjectKind::Checkpoint => ImmutableKind::Checkpoint,
        };
        StoreKey::Immutable(ImmutableKey {
            incarnation: self.incarnation,
            kind,
            storage_id: object.storage_id,
            digest: object.digest,
        })
    }

    fn commit_key(&self, reference: &CommitRef) -> StoreKey {
        StoreKey::Immutable(ImmutableKey {
            incarnation: self.incarnation,
            kind: ImmutableKind::Commit,
            storage_id: reference.storage_id,
            digest: reference.digest,
        })
    }

    fn verify_object(object: &ObjectRef, bytes: &Bytes) -> Result<(), Error> {
        let actual_len =
            u64::try_from(bytes.len()).map_err(|_| Error::LimitExceeded("object byte length"))?;
        if actual_len != object.len || Digest::of(bytes) != object.digest {
            return Err(Error::CorruptObject);
        }
        Ok(())
    }

    async fn load_incarnation(store: &ScopedStore, options: Options) -> Result<uuid::Uuid, Error> {
        let stored = store
            .read(StoreKey::Head, options.max_head_bytes)
            .await?
            .ok_or_else(|| Error::InvalidFormat("the opened log has no durable head".to_owned()))?;
        Self::incarnation_from_stored(store, options, &stored)
    }

    fn incarnation_from_stored(
        store: &ScopedStore,
        options: Options,
        stored: &crate::store::StoredObject,
    ) -> Result<uuid::Uuid, Error> {
        let head = format::decode_head(&stored.bytes)?;
        if head.log_id != *store.log_id() {
            return Err(Error::InvalidFormat(
                "the durable head belongs to another log".to_owned(),
            ));
        }
        if head.options != options {
            return Err(Error::ConfigurationMismatch("options"));
        }
        Ok(head.incarnation)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::ValidatedBackend;
    use crate::sim::{Failure, FailurePhase, FaultStore, Operation};
    use object_store::memory::InMemory;
    use object_store::path::Path;

    use super::*;

    #[test]
    fn collection_report_exposes_only_bounded_counts() {
        let report = CollectionReport {
            candidate_count: 3,
            candidate_bytes: 30,
            delete_attempts: 2,
        };
        assert_eq!(report.candidate_count(), 3);
        assert_eq!(report.candidate_bytes(), 30);
        assert_eq!(report.delete_attempts(), 2);
    }

    #[tokio::test]
    async fn initial_view_has_collection_epoch_zero() -> Result<(), Box<dyn std::error::Error>> {
        let log = test_log("initial-collection-epoch", Options::default()).await?;
        assert_eq!(log.load().await?.collection_epoch(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn fresh_staging_reallocates_a_colliding_physical_id()
    -> Result<(), Box<dyn std::error::Error>> {
        let log = test_log("storage-id-collision", Options::default()).await?;
        let bytes = Bytes::from_static(b"same bytes");
        let collision = StorageId::from_uuid(uuid::Uuid::from_u128(7));
        let replacement = StorageId::from_uuid(uuid::Uuid::from_u128(8));
        let occupied = ObjectRef {
            kind: ObjectKind::Blob,
            storage_id: collision,
            digest: Digest::of(&bytes),
            len: u64::try_from(bytes.len())?,
        };
        log.store
            .create(log.object_key(&occupied), bytes.clone())
            .await?;
        let mut ids = [collision, replacement].into_iter();

        let staged = log
            .create_fresh_object_with(ObjectKind::Blob, bytes, || {
                ids.next().unwrap_or(replacement)
            })
            .await?;

        assert_eq!(staged.storage_id, replacement);
        Ok(())
    }

    #[tokio::test]
    async fn fresh_staging_does_not_accept_a_collision_after_an_ambiguous_error()
    -> Result<(), Box<dyn std::error::Error>> {
        let faults = FaultStore::new(InMemory::new());
        let backend = ValidatedBackend::new(
            Arc::new(faults.clone()),
            Path::from("ambiguous-collision-tests"),
        )
        .await?;
        let log = Log::open(
            backend.scope(&LogId::new("ambiguous-storage-id-collision")?),
            Options::default(),
        )
        .await?;
        let bytes = Bytes::from_static(b"same bytes");
        let collision = StorageId::from_uuid(uuid::Uuid::from_u128(7));
        let occupied = ObjectRef {
            kind: ObjectKind::Blob,
            storage_id: collision,
            digest: Digest::of(&bytes),
            len: u64::try_from(bytes.len())?,
        };
        log.store
            .create(log.object_key(&occupied), bytes.clone())
            .await?;
        faults.reset();
        faults.schedule(Failure {
            operation: Operation::Put,
            occurrence: 1,
            phase: FailurePhase::Before,
        });

        let error = log
            .create_fresh_object_with(ObjectKind::Blob, bytes, || collision)
            .await
            .err()
            .ok_or("ambiguous fresh create returned its colliding ID")?;

        assert!(matches!(&error, Error::Store(error) if FaultStore::is_injected(error)));
        Ok(())
    }

    #[tokio::test]
    async fn fresh_commit_rejects_a_collision_but_exact_recovery_can_reuse_it()
    -> Result<(), Box<dyn std::error::Error>> {
        let log = test_log("commit-storage-id-collision", Options::default()).await?;
        let view = log.load().await?;
        let mut prepared = log.prepare(
            view.cursor(),
            TransactionId::new(),
            Bytes::from_static(b"operation"),
            Bytes::new(),
            Vec::new(),
        )?;
        prepared.storage_id = StorageId::from_uuid(uuid::Uuid::from_u128(7));
        let token = prepared.recovery_token()?;
        let (reference, bytes) = log.encode_prepared(&prepared)?;
        log.store.create(log.commit_key(&reference), bytes).await?;

        assert!(matches!(
            log.commit(prepared).await,
            Err(Error::PhysicalIdentityCollision)
        ));
        assert!(matches!(
            log.resume(&token).await?,
            Resolution::Committed(_)
        ));
        Ok(())
    }

    #[tokio::test]
    async fn recovery_rejects_a_commit_from_the_wrong_parent()
    -> Result<(), Box<dyn std::error::Error>> {
        let log = test_log("wrong-parent", Options::default()).await?;
        let view = install_commit(
            &log,
            format::Commit {
                log_id: log.store.log_id().clone(),
                incarnation: log.incarnation,
                transaction_id: TransactionId::new(),
                expected_tip: Some(Digest::of(b"wrong parent")),
                operation: Bytes::new(),
                result: Bytes::new(),
                objects: Vec::new(),
            },
        )
        .await?;

        assert!(matches!(
            log.read_tail(&view).await,
            Err(Error::InvalidFormat(_))
        ));
        Ok(())
    }

    #[tokio::test]
    async fn recovery_applies_durable_inline_limits() -> Result<(), Box<dyn std::error::Error>> {
        let options = Options {
            max_inline_operation_bytes: 1,
            ..Options::default()
        };
        let log = test_log("inline-limit", options).await?;
        let view = install_commit(
            &log,
            format::Commit {
                log_id: log.store.log_id().clone(),
                incarnation: log.incarnation,
                transaction_id: TransactionId::new(),
                expected_tip: None,
                operation: Bytes::from_static(b"too large"),
                result: Bytes::new(),
                objects: Vec::new(),
            },
        )
        .await?;

        assert!(matches!(
            log.read_tail(&view).await,
            Err(Error::LimitExceeded("inline operation bytes"))
        ));
        Ok(())
    }

    #[tokio::test]
    async fn recovery_rejects_a_commit_from_another_log() -> Result<(), Box<dyn std::error::Error>>
    {
        let log = test_log("local-log", Options::default()).await?;
        let view = install_commit(
            &log,
            format::Commit {
                log_id: LogId::new("foreign-log")?,
                incarnation: log.incarnation,
                transaction_id: TransactionId::new(),
                expected_tip: None,
                operation: Bytes::new(),
                result: Bytes::new(),
                objects: Vec::new(),
            },
        )
        .await?;

        assert!(matches!(
            log.read_tail(&view).await,
            Err(Error::InvalidFormat(_))
        ));
        Ok(())
    }

    async fn test_log(id: &str, options: Options) -> Result<Log, Error> {
        let backend =
            ValidatedBackend::new(Arc::new(InMemory::new()), Path::from("log-tests")).await?;
        let store = backend.scope(&LogId::new(id)?);
        Log::open(store, options).await
    }

    async fn install_commit(log: &Log, commit: format::Commit) -> Result<View, Error> {
        let source = log.load().await?;
        let bytes = format::encode_commit(&commit)?;
        let reference = CommitRef {
            sequence: 0,
            transaction_id: commit.transaction_id,
            storage_id: StorageId::new(),
            digest: Digest::of(&bytes),
            len: u64::try_from(bytes.len())
                .map_err(|_| Error::LimitExceeded("commit byte length"))?,
        };
        log.store.create(log.commit_key(&reference), bytes).await?;
        let mut head = source.cursor.head.clone();
        head.generation = 1;
        head.next_sequence = 1;
        head.tail.push(reference);
        let encoded = format::encode_head(&head)?;
        log.store
            .update(StoreKey::Head, encoded, source.cursor.version)
            .await?;
        log.load().await
    }
}
