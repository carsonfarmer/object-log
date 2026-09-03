//! Object-log publication protocol.

use bytes::Bytes;
use futures::{StreamExt, TryStreamExt, stream};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use crate::format::{self, CollectionCandidate, CollectionPlan, CollectionPlanRef, Head};
use crate::store::{
    ConditionalRead, CreateResult, ImmutableKey, ImmutableKind, MAX_DELETE_BATCH, ScopedStore,
    StoreKey, UpdateResult,
};
use crate::{
    CheckpointRef, CommitRef, Cursor, Digest, Error, LogId, ObjectKind, ObjectRef,
    PendingCheckpoint, PendingCommit, PreparedCommit, RetentionId, StorageId, TransactionId,
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
    const fn new(candidate_count: usize, candidate_bytes: u64, delete_attempts: usize) -> Self {
        Self {
            candidate_count,
            candidate_bytes,
            delete_attempts,
        }
    }

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

    /// Returns the number of planned candidate deletions submitted.
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

    /// Returns the limits fixed when this log was opened.
    #[must_use]
    pub const fn options(&self) -> Options {
        self.options
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

    /// Adds one stable retention to the supplied head view.
    ///
    /// A retention protects the complete log namespace. It has no expiry. The
    /// caller must keep `id` until release is confirmed.
    ///
    /// # Errors
    ///
    /// Returns an error for a foreign view, a durable limit, invalid head data,
    /// or a backend failure that cannot hide a successful update.
    pub async fn retain(&self, view: &View, id: RetentionId) -> Result<RetentionStatus, Error> {
        self.validate_cursor(view.cursor())?;
        if view.cursor.head.retention_ids.contains(&id) || view.cursor.head.active_plan.is_some() {
            return match self.refresh(view.cursor()).await? {
                Refresh::NotModified if view.cursor.head.retention_ids.contains(&id) => {
                    Ok(RetentionStatus::Applied(view.clone()))
                }
                Refresh::NotModified => Ok(RetentionStatus::ActiveCollection(view.clone())),
                Refresh::Updated(current) if current.cursor.head.retention_ids.contains(&id) => {
                    Ok(RetentionStatus::Applied(*current))
                }
                Refresh::Updated(current) if current.cursor.head.active_plan.is_some() => {
                    Ok(RetentionStatus::ActiveCollection(*current))
                }
                Refresh::Updated(current) => Ok(RetentionStatus::Conflict(*current)),
            };
        }
        if view.cursor.head.retention_ids.len() >= self.options.max_retention_ids {
            return Err(Error::LimitExceeded("retention IDs"));
        }

        let mut candidate = view.cursor.head.clone();
        candidate.retention_ids.insert(id);
        candidate.advance_generation()?;
        let bytes = format::encode_head(&candidate)?;
        self.validate_encoded_head(&bytes)?;
        match self
            .store
            .update(StoreKey::Head, bytes, view.cursor.version.clone())
            .await
        {
            Ok(UpdateResult::Updated { version }) => Ok(RetentionStatus::Applied(View {
                cursor: Cursor {
                    head: candidate,
                    version,
                },
            })),
            Ok(UpdateResult::PreconditionFailed) => {
                let current = match self.load().await {
                    Ok(current) => current,
                    Err(Error::Store(_)) => return Ok(RetentionStatus::Pending),
                    Err(error) => return Err(error),
                };
                if current.cursor.head.retention_ids.contains(&id) {
                    Ok(RetentionStatus::Applied(current))
                } else if current.cursor.head.active_plan.is_some() {
                    Ok(RetentionStatus::ActiveCollection(current))
                } else {
                    Ok(RetentionStatus::Conflict(current))
                }
            }
            Err(Error::Store(_)) => Ok(RetentionStatus::Pending),
            Err(error) => Err(error),
        }
    }

    /// Removes one stable retention from the supplied head view.
    ///
    /// Releasing an ID that is already absent succeeds without a head update.
    ///
    /// # Errors
    ///
    /// Returns an error for a foreign view, invalid head data, or a backend
    /// failure that cannot hide a successful update.
    pub async fn release_retention(
        &self,
        view: &View,
        id: RetentionId,
    ) -> Result<RetentionStatus, Error> {
        self.validate_cursor(view.cursor())?;
        if !view.cursor.head.retention_ids.contains(&id) {
            return match self.refresh(view.cursor()).await? {
                Refresh::NotModified => Ok(RetentionStatus::Applied(view.clone())),
                Refresh::Updated(current) if !current.cursor.head.retention_ids.contains(&id) => {
                    Ok(RetentionStatus::Applied(*current))
                }
                Refresh::Updated(current) => Ok(RetentionStatus::Conflict(*current)),
            };
        }

        let mut candidate = view.cursor.head.clone();
        candidate.retention_ids.remove(&id);
        candidate.advance_generation()?;
        let bytes = format::encode_head(&candidate)?;
        self.validate_encoded_head(&bytes)?;
        match self
            .store
            .update(StoreKey::Head, bytes, view.cursor.version.clone())
            .await
        {
            Ok(UpdateResult::Updated { version }) => Ok(RetentionStatus::Applied(View {
                cursor: Cursor {
                    head: candidate,
                    version,
                },
            })),
            Ok(UpdateResult::PreconditionFailed) => {
                let current = match self.load().await {
                    Ok(current) => current,
                    Err(Error::Store(_)) => return Ok(RetentionStatus::Pending),
                    Err(error) => return Err(error),
                };
                if current.cursor.head.retention_ids.contains(&id) {
                    Ok(RetentionStatus::Conflict(current))
                } else {
                    Ok(RetentionStatus::Applied(current))
                }
            }
            Err(Error::Store(_)) => Ok(RetentionStatus::Pending),
            Err(error) => Err(error),
        }
    }

    /// Creates a positive deletion plan and installs its head fence.
    ///
    /// The method verifies the complete live graph and bounded namespace before
    /// it creates a plan. It does not delete an object.
    ///
    /// # Errors
    ///
    /// Returns an error for a foreign view, invalid live data, a configured
    /// bound, or a storage failure before the head update.
    pub async fn start_collection(&self, view: &View) -> Result<CollectionStart, Error> {
        self.validate_cursor(view.cursor())?;
        if view.cursor.head.active_plan.is_some() {
            return Ok(CollectionStart::Active(view.clone()));
        }
        if !view.cursor.head.retention_ids.is_empty() {
            return Ok(CollectionStart::Retained(view.clone()));
        }

        let live = self.mark_live(view).await?;
        let mut candidates = Vec::new();
        let mut scanned = 0_usize;
        let mut listed = self.store.list_scoped();
        while let Some(item) = listed.next().await {
            if scanned == self.options.max_collection_objects {
                return Err(Error::LimitExceeded("collection scan objects"));
            }
            scanned += 1;
            let item = item?;
            if let Some(key) = item.immutable_key
                && !live.contains_key(&key)
            {
                candidates.push(CollectionCandidate {
                    key,
                    bytes: item.size,
                });
            }
        }
        candidates.sort_unstable_by_key(|candidate| candidate.key);
        let candidate_bytes = candidates.iter().try_fold(0_u64, |total, candidate| {
            total
                .checked_add(candidate.bytes)
                .ok_or(Error::LimitExceeded("collection candidate bytes"))
        })?;
        let report = CollectionReport::new(candidates.len(), candidate_bytes, 0);
        if candidates.is_empty() {
            return Ok(CollectionStart::Empty(report));
        }

        let epoch = view
            .collection_epoch()
            .checked_add(1)
            .ok_or(Error::LimitExceeded("collection epoch"))?;
        let plan = CollectionPlan {
            log_id: self.store.log_id().clone(),
            collection_epoch: epoch,
            candidates,
        };
        let plan_ref = self.create_collection_plan(&plan).await?;
        let plan_key = Self::collection_plan_key(self.incarnation, &plan_ref);
        let mut candidate = view.cursor.head.clone();
        candidate.advance_generation()?;
        candidate.collection_epoch = epoch;
        candidate.active_plan = Some(plan_ref);
        let bytes = format::encode_head(&candidate)?;
        self.validate_encoded_head(&bytes)?;
        match self
            .store
            .update(StoreKey::Head, bytes, view.cursor.version.clone())
            .await
        {
            Ok(UpdateResult::Updated { version }) => Ok(CollectionStart::Installed(
                View {
                    cursor: Cursor {
                        head: candidate,
                        version,
                    },
                },
                report,
            )),
            Ok(UpdateResult::PreconditionFailed) => {
                self.cleanup_collection_plan(plan_key).await?;
                match self.load().await {
                    Ok(current) => Ok(CollectionStart::Conflict(current)),
                    Err(Error::Store(_)) => Ok(CollectionStart::Pending),
                    Err(error) => Err(error),
                }
            }
            Err(Error::Store(_)) => Ok(CollectionStart::Pending),
            Err(error) => Err(error),
        }
    }

    /// Deletes one active plan and conditionally clears its exact head fence.
    ///
    /// Every retry submits the complete positive deletion set again. Missing
    /// objects count as successful deletes.
    /// If deletion of the plan object fails after fence clearing, a later
    /// collection can remove it. This does not change candidate completion.
    ///
    /// # Errors
    ///
    /// Returns an error for a foreign view or invalid durable plan data.
    pub async fn resume_collection(&self, view: &View) -> Result<CollectionFinish, Error> {
        self.validate_cursor(view.cursor())?;
        let requested_plan = view.cursor.head.active_plan.as_ref();
        let current = self.load().await?;
        let Some(plan_ref) = current.cursor.head.active_plan.as_ref() else {
            if let Some(requested_plan) = requested_plan {
                let key = Self::collection_plan_key(view.cursor.head.incarnation, requested_plan);
                self.cleanup_collection_plan(key).await?;
            }
            return Ok(CollectionFinish::Complete(
                current,
                CollectionReport::new(0, 0, 0),
            ));
        };
        if requested_plan != Some(plan_ref) {
            return Ok(CollectionFinish::Conflict(
                current,
                CollectionReport::new(0, 0, 0),
            ));
        }
        let plan_key = Self::collection_plan_key(current.cursor.head.incarnation, plan_ref);
        let plan = self
            .read_collection_plan(&current.cursor.head, plan_ref)
            .await?;
        let candidate_bytes = plan.candidate_bytes()?;
        let mut report = CollectionReport::new(plan.candidates.len(), candidate_bytes, 0);
        for batch in plan.candidates.chunks(MAX_DELETE_BATCH) {
            report.delete_attempts += batch.len();
            match self
                .store
                .delete_immutable_batch(batch.iter().map(|candidate| candidate.key))
                .await
            {
                Ok(()) => {}
                Err(Error::Store(_)) => return Ok(CollectionFinish::Pending(report)),
                Err(error) => return Err(error),
            }
        }

        let mut candidate = current.cursor.head.clone();
        candidate.active_plan = None;
        candidate.advance_generation()?;
        let bytes = format::encode_head(&candidate)?;
        self.validate_encoded_head(&bytes)?;
        match self
            .store
            .update(StoreKey::Head, bytes, current.cursor.version.clone())
            .await
        {
            Ok(UpdateResult::Updated { version }) => {
                self.cleanup_collection_plan(plan_key).await?;
                Ok(CollectionFinish::Complete(
                    View {
                        cursor: Cursor {
                            head: candidate,
                            version,
                        },
                    },
                    report,
                ))
            }
            Ok(UpdateResult::PreconditionFailed) => {
                let current = match self.load().await {
                    Ok(current) => current,
                    Err(Error::Store(_)) => return Ok(CollectionFinish::Pending(report)),
                    Err(error) => return Err(error),
                };
                if current.cursor.head.active_plan.is_none() {
                    self.cleanup_collection_plan(plan_key).await?;
                    Ok(CollectionFinish::Complete(current, report))
                } else {
                    Ok(CollectionFinish::Conflict(current, report))
                }
            }
            Err(Error::Store(_)) => Ok(CollectionFinish::Pending(report)),
            Err(error) => Err(error),
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
    /// Returns expiry when a missing object belongs to an older unretained
    /// view. A missing object in the current epoch is corruption.
    pub async fn read_object(&self, view: &View, object: &ObjectRef) -> Result<Bytes, Error> {
        self.validate_cursor(view.cursor())?;
        if object.kind != ObjectKind::Blob {
            return Err(Error::InvalidFormat(
                "a payload read requires a blob reference".to_owned(),
            ));
        }
        self.read_immutable_for_view(view, object).await
    }

    /// Reads and verifies one immutable reference node.
    ///
    /// Child objects remain lazy. Call [`Log::read_object`] or
    /// [`Log::read_node`] for each child that the adapter needs.
    ///
    /// # Errors
    ///
    /// Returns expiry when a missing node belongs to an older unretained view.
    /// A missing node in the current epoch is corruption.
    pub async fn read_node(&self, view: &View, object: &ObjectRef) -> Result<ReferenceNode, Error> {
        self.validate_cursor(view.cursor())?;
        if object.kind != ObjectKind::Node {
            return Err(Error::InvalidFormat(
                "a node read requires a reference-node object".to_owned(),
            ));
        }
        let bytes = self.read_immutable_for_view(view, object).await?;
        let node = format::decode_node(&bytes)?;
        self.validate_dependencies(&node.children)?;
        Ok(ReferenceNode {
            payload: node.payload,
            children: node.children,
        })
    }

    async fn read_immutable_for_view(
        &self,
        view: &View,
        object: &ObjectRef,
    ) -> Result<Bytes, Error> {
        let Some(bytes) = self.read_immutable_optional(object).await? else {
            return Err(self.missing_read_error(view).await?);
        };
        Ok(bytes)
    }

    async fn read_immutable_optional(&self, object: &ObjectRef) -> Result<Option<Bytes>, Error> {
        let declared_len =
            usize::try_from(object.len).map_err(|_| Error::LimitExceeded("object byte length"))?;
        if declared_len > self.options.max_object_bytes {
            return Err(Error::LimitExceeded("object bytes"));
        }
        let Some(stored) = self
            .store
            .read(self.object_key(object), declared_len)
            .await?
        else {
            return Ok(None);
        };
        Self::verify_object(object, &stored.bytes)?;
        Ok(Some(stored.bytes))
    }

    async fn read_immutable(&self, object: &ObjectRef) -> Result<Bytes, Error> {
        self.read_immutable_optional(object)
            .await?
            .ok_or_else(|| Error::InvalidFormat("a referenced object is missing".to_owned()))
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
        self.validate_prepared(&prepared)?;
        let (commit_ref, commit_bytes) = self.encode_prepared(&prepared)?;
        self.verify_publication(
            &prepared.cursor.head,
            self.commit_immutable_key(&commit_ref),
            &prepared.objects,
        )
        .await?;
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
                        if Self::tail_contains(&view, &pending.commit_ref) {
                            match self.verify_published_commit(&pending.commit_ref).await {
                                Ok(()) => Ok(CommitStatus::Committed(view)),
                                Err(Error::Store(_)) => Ok(CommitStatus::Pending(pending)),
                                Err(error) => Err(error),
                            }
                        } else {
                            Ok(CommitStatus::Committed(view))
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
            if let Resolution::Committed(view) = &resolution
                && Self::tail_contains(view, &pending.commit_ref)
            {
                match self.verify_published_commit(&pending.commit_ref).await {
                    Ok(()) => {}
                    Err(Error::Store(_)) => return Ok(Resolution::StillPending(pending)),
                    Err(error) => return Err(error),
                }
            }
            return Ok(resolution);
        }

        let (_, commit_bytes) = self.encode_prepared(&pending.prepared)?;
        match self
            .verify_publication(
                &pending.prepared.cursor.head,
                self.commit_immutable_key(&pending.commit_ref),
                &pending.prepared.objects,
            )
            .await
        {
            Ok(()) => {}
            Err(Error::Store(_)) => return Ok(Resolution::StillPending(pending)),
            Err(error) => return Err(error),
        }
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
    /// Returns expiry for missing commits in an older unretained view. A
    /// missing commit in the current epoch is corruption.
    pub async fn read_tail(&self, view: &View) -> Result<Vec<CommitRecord>, Error> {
        self.validate_cursor(view.cursor())?;
        let log = Arc::new(self.clone());
        let records = stream::iter(view.tail().to_vec().into_iter().map(move |reference| {
            let log = Arc::clone(&log);
            async move { log.read_commit_optional(&reference).await }
        }))
        .buffered(MAX_CONCURRENT_READS)
        .try_collect::<Vec<Option<CommitRecord>>>()
        .await?;
        if records.iter().any(Option::is_none) {
            return Err(self.missing_read_error(view).await?);
        }
        let records = records.into_iter().flatten().collect::<Vec<_>>();

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
        let blocked = self
            .verify_publication_dependencies(&view.cursor.head, &checkpoint.objects)
            .await?;
        let object = self
            .create_fresh_object_with(
                ObjectKind::Checkpoint,
                bytes,
                blocked.as_deref(),
                StorageId::new,
            )
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
        match self.verify_checkpoint_publication(&pending).await {
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
    /// Returns expiry for a missing checkpoint in an older unretained view. A
    /// missing checkpoint in the current epoch is corruption.
    pub async fn read_checkpoint(&self, view: &View) -> Result<Option<CheckpointRecord>, Error> {
        self.validate_cursor(view.cursor())?;
        let Some(reference) = view.checkpoint() else {
            return Ok(None);
        };
        let declared_len = usize::try_from(reference.object.len)
            .map_err(|_| Error::LimitExceeded("checkpoint byte length"))?;
        self.validate_checkpoint_bytes(declared_len)?;
        let checkpoint = self.load_checkpoint_for_view(view, reference).await?;
        Ok(Some(CheckpointRecord {
            snapshot: checkpoint.snapshot,
            objects: checkpoint.objects,
        }))
    }

    async fn verify_checkpoint(&self, reference: &CheckpointRef) -> Result<(), Error> {
        let checkpoint = self.load_checkpoint(reference).await?;
        self.verify_objects(&checkpoint.objects).await
    }

    async fn verify_checkpoint_publication(
        &self,
        pending: &PendingCheckpoint,
    ) -> Result<(), Error> {
        let checkpoint = self.load_checkpoint(&pending.checkpoint).await?;
        self.verify_publication(
            &pending.view.cursor.head,
            self.object_immutable_key(&pending.checkpoint.object),
            &checkpoint.objects,
        )
        .await
    }

    async fn load_checkpoint(
        &self,
        reference: &CheckpointRef,
    ) -> Result<format::Checkpoint, Error> {
        let bytes = self.read_immutable(&reference.object).await?;
        self.decode_checkpoint_reference(reference, &bytes)
    }

    async fn load_checkpoint_for_view(
        &self,
        view: &View,
        reference: &CheckpointRef,
    ) -> Result<format::Checkpoint, Error> {
        let bytes = self
            .read_immutable_for_view(view, &reference.object)
            .await?;
        self.decode_checkpoint_reference(reference, &bytes)
    }

    fn decode_checkpoint_reference(
        &self,
        reference: &CheckpointRef,
        bytes: &Bytes,
    ) -> Result<format::Checkpoint, Error> {
        let checkpoint = format::decode_checkpoint(bytes)?;
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

    async fn missing_read_error(&self, view: &View) -> Result<Error, Error> {
        let current = self.load().await?;
        match current.collection_epoch().cmp(&view.collection_epoch()) {
            std::cmp::Ordering::Greater => Ok(Error::ViewExpired),
            std::cmp::Ordering::Equal => Ok(Error::CorruptObject),
            std::cmp::Ordering::Less => Err(Error::InvalidFormat(
                "the durable collection epoch precedes the supplied view".to_owned(),
            )),
        }
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

    fn validate_prepared(&self, prepared: &PreparedCommit) -> Result<(), Error> {
        self.validate_cursor(&prepared.cursor)?;
        self.validate_prepared_sizes(&prepared.operation, &prepared.result)?;
        self.validate_dependencies(&prepared.objects)?;
        if prepared.cursor.head.tail.len() >= self.options.max_tail_entries {
            return Err(Error::LimitExceeded("active tail entries"));
        }
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
        head.advance_generation()?;
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
        head.advance_generation()?;
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

    fn tail_contains(view: &View, target: &CommitRef) -> bool {
        view.cursor.head.tail.iter().any(|entry| entry == target)
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
        self.read_commit_optional(reference)
            .await?
            .ok_or_else(|| Error::InvalidFormat("a referenced commit is missing".to_owned()))
    }

    async fn read_commit_optional(
        &self,
        reference: &CommitRef,
    ) -> Result<Option<CommitRecord>, Error> {
        if reference.len
            > u64::try_from(self.options.max_commit_bytes)
                .map_err(|_| Error::LimitExceeded("encoded commit bytes"))?
        {
            return Err(Error::LimitExceeded("encoded commit bytes"));
        }
        let Some(stored) = self
            .store
            .read(self.commit_key(reference), self.options.max_commit_bytes)
            .await?
        else {
            return Ok(None);
        };
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
        Ok(Some(CommitRecord {
            reference: reference.clone(),
            expected_tip: commit.expected_tip,
            operation: commit.operation,
            result: commit.result,
            objects: commit.objects,
        }))
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

    async fn verify_publication(
        &self,
        head: &Head,
        new_key: ImmutableKey,
        objects: &[ObjectRef],
    ) -> Result<(), Error> {
        let blocked = self.verify_publication_dependencies(head, objects).await?;
        if blocked
            .as_deref()
            .is_some_and(|blocked| Self::is_collection_candidate(blocked, new_key))
        {
            return Err(Error::CollectionFence);
        }
        Ok(())
    }

    async fn verify_publication_dependencies(
        &self,
        head: &Head,
        objects: &[ObjectRef],
    ) -> Result<Option<Vec<CollectionCandidate>>, Error> {
        let Some(plan_ref) = head.active_plan.as_ref() else {
            let mut visited = HashMap::with_capacity(objects.len());
            self.mark_object_graph(objects, &mut visited, None).await?;
            return Ok(None);
        };
        let plan = self.read_collection_plan(head, plan_ref).await?;
        let mut visited = HashMap::with_capacity(objects.len());
        self.mark_object_graph(objects, &mut visited, Some(&plan.candidates))
            .await?;
        Ok(Some(plan.candidates))
    }

    async fn mark_live(&self, view: &View) -> Result<HashMap<ImmutableKey, u64>, Error> {
        let mut live = HashMap::new();
        let tail = self.read_tail(view).await?;
        let mut roots = Vec::new();
        for record in tail {
            self.insert_live(
                &mut live,
                self.commit_immutable_key(&record.reference),
                record.reference.len,
            )?;
            if record.objects.len() > self.options.max_collection_objects - roots.len() {
                return Err(Error::LimitExceeded("collection live objects"));
            }
            roots.extend(record.objects);
        }
        if let Some(reference) = view.checkpoint() {
            self.insert_live(
                &mut live,
                self.object_immutable_key(&reference.object),
                reference.object.len,
            )?;
            let checkpoint = self.load_checkpoint(reference).await?;
            if checkpoint.objects.len() > self.options.max_collection_objects - roots.len() {
                return Err(Error::LimitExceeded("collection live objects"));
            }
            roots.extend(checkpoint.objects);
        }
        self.mark_object_graph(&roots, &mut live, None).await?;
        Ok(live)
    }

    async fn mark_object_graph(
        &self,
        roots: &[ObjectRef],
        visited: &mut HashMap<ImmutableKey, u64>,
        blocked: Option<&[CollectionCandidate]>,
    ) -> Result<(), Error> {
        let mut pending = VecDeque::new();
        for object in roots {
            self.enqueue_object(object, visited, blocked, &mut pending)?;
        }

        let log = Arc::new(self.clone());
        while !pending.is_empty() {
            let count = pending.len().min(MAX_CONCURRENT_READS);
            let batch = pending.drain(..count).collect::<Vec<_>>();
            let log = Arc::clone(&log);
            let children = stream::iter(batch.into_iter().map(move |object| {
                let log = Arc::clone(&log);
                async move { log.read_graph_children(&object).await }
            }))
            .buffer_unordered(MAX_CONCURRENT_READS)
            .try_collect::<Vec<_>>()
            .await?;
            for children in children {
                for child in children {
                    self.enqueue_object(&child, visited, blocked, &mut pending)?;
                }
            }
        }
        Ok(())
    }

    fn enqueue_object(
        &self,
        object: &ObjectRef,
        visited: &mut HashMap<ImmutableKey, u64>,
        blocked: Option<&[CollectionCandidate]>,
        pending: &mut VecDeque<ObjectRef>,
    ) -> Result<(), Error> {
        if object.kind == ObjectKind::Checkpoint {
            return Err(Error::InvalidFormat(
                "application dependencies cannot name a checkpoint".to_owned(),
            ));
        }
        let key = self.object_immutable_key(object);
        if blocked.is_some_and(|blocked| Self::is_collection_candidate(blocked, key)) {
            return Err(Error::CollectionFence);
        }
        if let Some(declared_len) = visited.get(&key) {
            if *declared_len != object.len {
                return Err(Error::CorruptObject);
            }
        } else {
            visited.insert(key, object.len);
            if visited.len() > self.options.max_collection_objects {
                return Err(Error::LimitExceeded("collection live objects"));
            }
            pending.push_back(object.clone());
        }
        Ok(())
    }

    fn insert_live(
        &self,
        live: &mut HashMap<ImmutableKey, u64>,
        key: ImmutableKey,
        len: u64,
    ) -> Result<(), Error> {
        if let Some(declared_len) = live.insert(key, len)
            && declared_len != len
        {
            return Err(Error::CorruptObject);
        }
        if live.len() > self.options.max_collection_objects {
            return Err(Error::LimitExceeded("collection live objects"));
        }
        Ok(())
    }

    fn is_collection_candidate(candidates: &[CollectionCandidate], key: ImmutableKey) -> bool {
        candidates
            .binary_search_by_key(&key, |candidate| candidate.key)
            .is_ok()
    }

    async fn read_graph_children(&self, object: &ObjectRef) -> Result<Vec<ObjectRef>, Error> {
        match object.kind {
            ObjectKind::Blob => {
                self.verify_object_durable(object).await?;
                Ok(Vec::new())
            }
            ObjectKind::Node => {
                let bytes = self.read_immutable(object).await?;
                let node = format::decode_node(&bytes)?;
                self.validate_dependencies(&node.children)?;
                Ok(node.children)
            }
            ObjectKind::Checkpoint => Err(Error::InvalidFormat(
                "application dependencies cannot name a checkpoint".to_owned(),
            )),
        }
    }

    async fn create_collection_plan(
        &self,
        plan: &CollectionPlan,
    ) -> Result<CollectionPlanRef, Error> {
        const MAX_ATTEMPTS: usize = 16;

        let bytes = format::encode_collection_plan(plan, self.options)?;
        let digest = Digest::of(&bytes);
        let len = u64::try_from(bytes.len())
            .map_err(|_| Error::LimitExceeded("collection plan byte length"))?;
        for _ in 0..MAX_ATTEMPTS {
            let reference = CollectionPlanRef {
                storage_id: StorageId::new(),
                digest,
                len,
            };
            match self
                .store
                .create(
                    StoreKey::Immutable(Self::collection_plan_key(self.incarnation, &reference)),
                    bytes.clone(),
                )
                .await?
            {
                CreateResult::Created { .. } => return Ok(reference),
                CreateResult::AlreadyExists => {}
            }
        }
        Err(Error::LimitExceeded("fresh physical storage identity"))
    }

    async fn read_collection_plan(
        &self,
        head: &Head,
        reference: &CollectionPlanRef,
    ) -> Result<CollectionPlan, Error> {
        let declared_len = usize::try_from(reference.len)
            .map_err(|_| Error::LimitExceeded("collection plan byte length"))?;
        if declared_len > self.options.max_collection_plan_bytes {
            return Err(Error::LimitExceeded("encoded collection plan bytes"));
        }
        let key = Self::collection_plan_key(head.incarnation, reference);
        let stored = self
            .store
            .read(StoreKey::Immutable(key), declared_len)
            .await?
            .ok_or_else(|| Error::InvalidFormat("the active collection plan is missing".into()))?;
        if stored.bytes.len() != declared_len || Digest::of(&stored.bytes) != reference.digest {
            return Err(Error::CorruptObject);
        }
        let plan = format::decode_collection_plan(&stored.bytes, self.options)?;
        if plan.log_id != *self.store.log_id() || plan.collection_epoch != head.collection_epoch {
            return Err(Error::InvalidFormat(
                "the collection plan does not match its head fence".into(),
            ));
        }
        Ok(plan)
    }

    async fn cleanup_collection_plan(&self, key: ImmutableKey) -> Result<(), Error> {
        match self
            .store
            .delete_immutable_batch(std::iter::once(key))
            .await
        {
            Ok(()) | Err(Error::Store(_)) => Ok(()),
            Err(error) => Err(error),
        }
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
        self.create_fresh_object_with(kind, bytes, None, StorageId::new)
            .await
    }

    async fn create_fresh_object_with(
        &self,
        kind: ObjectKind,
        bytes: Bytes,
        blocked: Option<&[CollectionCandidate]>,
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
            if blocked.is_some_and(|blocked| {
                Self::is_collection_candidate(blocked, self.object_immutable_key(&object))
            }) {
                continue;
            }
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
        StoreKey::Immutable(self.object_immutable_key(object))
    }

    fn object_immutable_key(&self, object: &ObjectRef) -> ImmutableKey {
        let kind = match object.kind {
            ObjectKind::Blob => ImmutableKind::Blob,
            ObjectKind::Node => ImmutableKind::Node,
            ObjectKind::Checkpoint => ImmutableKind::Checkpoint,
        };
        ImmutableKey {
            incarnation: self.incarnation,
            kind,
            storage_id: object.storage_id,
            digest: object.digest,
        }
    }

    fn commit_key(&self, reference: &CommitRef) -> StoreKey {
        StoreKey::Immutable(self.commit_immutable_key(reference))
    }

    fn commit_immutable_key(&self, reference: &CommitRef) -> ImmutableKey {
        ImmutableKey {
            incarnation: self.incarnation,
            kind: ImmutableKind::Commit,
            storage_id: reference.storage_id,
            digest: reference.digest,
        }
    }

    fn collection_plan_key(incarnation: uuid::Uuid, reference: &CollectionPlanRef) -> ImmutableKey {
        ImmutableKey {
            incarnation,
            kind: ImmutableKind::CollectionPlan,
            storage_id: reference.storage_id,
            digest: reference.digest,
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
            .create_fresh_object_with(ObjectKind::Blob, bytes, None, || {
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
            .create_fresh_object_with(ObjectKind::Blob, bytes, None, || collision)
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

    #[tokio::test]
    async fn active_plan_rejects_duplicate_physical_refs_with_conflicting_lengths()
    -> Result<(), Box<dyn std::error::Error>> {
        let log = test_log("conflicting-lengths", Options::default()).await?;
        let source = log.load().await?;
        let object = log.put_object(Bytes::from_static(b"object")).await?;
        let mut conflicting = object.clone();
        conflicting.len = conflicting.len.saturating_add(1);
        let fenced = install_plan(
            &log,
            &source,
            vec![CollectionCandidate {
                key: ImmutableKey::new(
                    log.incarnation,
                    ImmutableKind::Blob,
                    Digest::of(b"unrelated"),
                ),
                bytes: 1,
            }],
        )
        .await?;
        let prepared = log.prepare(
            fenced.cursor(),
            TransactionId::new(),
            Bytes::new(),
            Bytes::new(),
            vec![object, conflicting],
        )?;

        assert!(matches!(
            log.commit(prepared).await,
            Err(Error::CorruptObject)
        ));
        Ok(())
    }

    #[tokio::test]
    async fn active_plan_fences_the_new_commit_physical_key()
    -> Result<(), Box<dyn std::error::Error>> {
        let log = test_log("commit-key-fence", Options::default()).await?;
        let source = log.load().await?;
        let transaction_id = TransactionId::new();
        let storage_id = StorageId::from_uuid(uuid::Uuid::from_u128(41));
        let mut predicted = log.prepare(
            source.cursor(),
            transaction_id,
            Bytes::from_static(b"operation"),
            Bytes::new(),
            Vec::new(),
        )?;
        predicted.storage_id = storage_id;
        let (predicted_ref, _) = log.encode_prepared(&predicted)?;
        let fenced = install_plan(
            &log,
            &source,
            vec![CollectionCandidate {
                key: log.commit_immutable_key(&predicted_ref),
                bytes: predicted_ref.len,
            }],
        )
        .await?;
        let mut prepared = log.prepare(
            fenced.cursor(),
            transaction_id,
            Bytes::from_static(b"operation"),
            Bytes::new(),
            Vec::new(),
        )?;
        prepared.storage_id = storage_id;
        let token = prepared.recovery_token()?;

        assert!(matches!(
            log.commit(prepared).await,
            Err(Error::CollectionFence)
        ));
        assert!(matches!(
            log.resume(&token).await,
            Err(Error::CollectionFence)
        ));
        Ok(())
    }

    #[tokio::test]
    async fn active_plan_fences_an_exact_checkpoint_retry() -> Result<(), Box<dyn std::error::Error>>
    {
        let log = test_log("checkpoint-retry-fence", Options::default()).await?;
        let source = log.load().await?;
        let prepared = log.prepare(
            source.cursor(),
            TransactionId::new(),
            Bytes::from_static(b"commit"),
            Bytes::new(),
            Vec::new(),
        )?;
        let CommitStatus::Committed(committed) = log.commit(prepared).await? else {
            return Err("commit failed".into());
        };
        let through = committed.tail()[0].clone();
        let checkpoint = format::Checkpoint {
            log_id: log.store.log_id().clone(),
            incarnation: log.incarnation,
            through_sequence: through.sequence,
            through_commit: through.digest,
            snapshot: Bytes::from_static(b"checkpoint"),
            objects: Vec::new(),
        };
        let bytes = format::encode_checkpoint(&checkpoint)?;
        let object = ObjectRef {
            kind: ObjectKind::Checkpoint,
            storage_id: StorageId::from_uuid(uuid::Uuid::from_u128(47)),
            digest: Digest::of(&bytes),
            len: u64::try_from(bytes.len())?,
        };
        log.store.create(log.object_key(&object), bytes).await?;
        let fenced = install_plan(
            &log,
            &committed,
            vec![CollectionCandidate {
                key: log.object_immutable_key(&object),
                bytes: object.len,
            }],
        )
        .await?;
        let pending = PendingCheckpoint {
            view: fenced,
            through: through.clone(),
            checkpoint: CheckpointRef {
                through_sequence: through.sequence,
                through_commit: through.digest,
                object,
            },
        };

        assert!(matches!(
            log.resolve_checkpoint(pending).await,
            Err(Error::CollectionFence)
        ));
        Ok(())
    }

    #[tokio::test]
    async fn checkpoint_allocation_skips_a_planned_physical_key()
    -> Result<(), Box<dyn std::error::Error>> {
        let log = test_log("checkpoint-key-fence", Options::default()).await?;
        let bytes = format::encode_checkpoint(&format::Checkpoint {
            log_id: log.store.log_id().clone(),
            incarnation: log.incarnation,
            through_sequence: 0,
            through_commit: Digest::of(b"commit"),
            snapshot: Bytes::new(),
            objects: Vec::new(),
        })?;
        let blocked_id = StorageId::from_uuid(uuid::Uuid::from_u128(51));
        let replacement_id = StorageId::from_uuid(uuid::Uuid::from_u128(52));
        let blocked = [CollectionCandidate {
            key: ImmutableKey {
                incarnation: log.incarnation,
                kind: ImmutableKind::Checkpoint,
                storage_id: blocked_id,
                digest: Digest::of(&bytes),
            },
            bytes: u64::try_from(bytes.len())?,
        }];
        let mut ids = [blocked_id, replacement_id].into_iter();

        let object = log
            .create_fresh_object_with(ObjectKind::Checkpoint, bytes, Some(&blocked), || {
                ids.next().unwrap_or(replacement_id)
            })
            .await?;

        assert_eq!(object.storage_id, replacement_id);
        Ok(())
    }

    #[tokio::test]
    async fn old_incarnation_candidate_cannot_delete_current_data()
    -> Result<(), Box<dyn std::error::Error>> {
        let log = test_log("old-incarnation-delete", Options::default()).await?;
        let source = log.load().await?;
        let bytes = Bytes::from_static(b"same content");
        let current = log.put_object(bytes.clone()).await?;
        let old_key = ImmutableKey {
            incarnation: uuid::Uuid::from_u128(61),
            kind: ImmutableKind::Blob,
            storage_id: StorageId::from_uuid(uuid::Uuid::from_u128(62)),
            digest: Digest::of(&bytes),
        };
        log.store
            .create(StoreKey::Immutable(old_key), bytes.clone())
            .await?;
        let fenced = install_plan(
            &log,
            &source,
            vec![CollectionCandidate {
                key: old_key,
                bytes: u64::try_from(bytes.len())?,
            }],
        )
        .await?;
        let CollectionFinish::Complete(cleared, _) = log.resume_collection(&fenced).await? else {
            return Err("collection did not clear".into());
        };

        assert!(
            log.store
                .read(StoreKey::Immutable(old_key), bytes.len())
                .await?
                .is_none()
        );
        assert_eq!(log.read_object(&cleared, &current).await?, bytes);
        Ok(())
    }

    #[tokio::test]
    async fn resume_rejects_a_different_current_plan_before_delete()
    -> Result<(), Box<dyn std::error::Error>> {
        let faults = FaultStore::new(InMemory::new());
        let backend =
            ValidatedBackend::new(Arc::new(faults.clone()), Path::from("different-plan-tests"))
                .await?;
        let log = Log::open(
            backend.scope(&LogId::new("different-plan")?),
            Options::default(),
        )
        .await?;
        let source = log.load().await?;
        let first = install_plan(
            &log,
            &source,
            vec![CollectionCandidate {
                key: ImmutableKey::new(log.incarnation, ImmutableKind::Blob, Digest::of(b"first")),
                bytes: 1,
            }],
        )
        .await?;
        let second_plan = CollectionPlan {
            log_id: log.store.log_id().clone(),
            collection_epoch: 2,
            candidates: vec![CollectionCandidate {
                key: ImmutableKey::new(log.incarnation, ImmutableKind::Blob, Digest::of(b"second")),
                bytes: 1,
            }],
        };
        let second_ref = log.create_collection_plan(&second_plan).await?;
        let mut head = first.cursor.head.clone();
        head.generation = head
            .generation
            .checked_add(1)
            .ok_or("generation overflow")?;
        head.collection_epoch = 2;
        head.active_plan = Some(second_ref);
        let encoded = format::encode_head(&head)?;
        log.store
            .update(StoreKey::Head, encoded, first.cursor.version.clone())
            .await?;
        faults.reset();

        assert!(matches!(
            log.resume_collection(&first).await?,
            CollectionFinish::Conflict(_, _)
        ));
        assert_eq!(faults.metrics().operation(Operation::Delete).requests, 0);
        Ok(())
    }

    #[tokio::test]
    async fn oversized_live_reference_fails_before_collection_io()
    -> Result<(), Box<dyn std::error::Error>> {
        let faults = FaultStore::new(InMemory::new());
        let options = Options {
            max_object_bytes: 1,
            ..Options::default()
        };
        let backend =
            ValidatedBackend::new(Arc::new(faults.clone()), Path::from("oversized-live-tests"))
                .await?;
        let log = Log::open(backend.scope(&LogId::new("oversized-live")?), options).await?;
        let view = install_commit(
            &log,
            format::Commit {
                log_id: log.store.log_id().clone(),
                incarnation: log.incarnation,
                transaction_id: TransactionId::new(),
                expected_tip: None,
                operation: Bytes::new(),
                result: Bytes::new(),
                objects: vec![ObjectRef {
                    kind: ObjectKind::Blob,
                    storage_id: StorageId::new(),
                    digest: Digest::of(b"oversized"),
                    len: 2,
                }],
            },
        )
        .await?;
        faults.reset();

        assert!(matches!(
            log.start_collection(&view).await,
            Err(Error::LimitExceeded("object bytes"))
        ));
        assert_eq!(faults.metrics().operation(Operation::List).requests, 0);
        assert_eq!(faults.metrics().operation(Operation::Delete).requests, 0);
        assert_eq!(faults.metrics().operation(Operation::Put).requests, 0);
        Ok(())
    }

    async fn test_log(id: &str, options: Options) -> Result<Log, Error> {
        let backend =
            ValidatedBackend::new(Arc::new(InMemory::new()), Path::from("log-tests")).await?;
        let store = backend.scope(&LogId::new(id)?);
        Log::open(store, options).await
    }

    async fn install_plan(
        log: &Log,
        source: &View,
        mut candidates: Vec<CollectionCandidate>,
    ) -> Result<View, Error> {
        candidates.sort_unstable_by_key(|candidate| candidate.key);
        let epoch = source
            .collection_epoch()
            .checked_add(1)
            .ok_or(Error::LimitExceeded("collection epoch"))?;
        let plan = CollectionPlan {
            log_id: log.store.log_id().clone(),
            collection_epoch: epoch,
            candidates,
        };
        let plan_ref = log.create_collection_plan(&plan).await?;
        let mut head = source.cursor.head.clone();
        head.generation = head
            .generation
            .checked_add(1)
            .ok_or(Error::LimitExceeded("head generation"))?;
        head.collection_epoch = epoch;
        head.active_plan = Some(plan_ref);
        let bytes = format::encode_head(&head)?;
        let UpdateResult::Updated { version } = log
            .store
            .update(StoreKey::Head, bytes, source.cursor.version.clone())
            .await?
        else {
            return Err(Error::InvalidFormat("test plan lost its CAS".into()));
        };
        Ok(View {
            cursor: Cursor { head, version },
        })
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
