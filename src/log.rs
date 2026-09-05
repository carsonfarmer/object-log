//! Object-log publication protocol.

use bytes::Bytes;
use futures::{StreamExt, TryStreamExt, stream};
use object_store::UpdateVersion;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use crate::format::{self, CollectionCandidate, CollectionPlan, CollectionPlanRef, Head};
use crate::store::{
    ConditionalRead, CreateResult, ImmutableKey, ImmutableKind, MAX_DELETE_BATCH, ScopedStore,
    StoreKey, UpdateResult,
};
use crate::{
    CheckpointRef, CommitRef, Digest, Error, LogId, ObjectKind, ObjectRef, ObservedState,
    PendingCheckpoint, PendingCommit, PreparedCommit, RetentionId, StagedObject, StagingDomain,
    StorageId, TransactionId, ValidatedBackend, View,
};

const MAX_CONCURRENT_READS: usize = 32;
const MAX_FRESH_OBJECT_ATTEMPTS: usize = 16;

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

/// The immediate result of publishing one prepared commit.
#[derive(Debug)]
pub enum CommitStatus {
    /// The exact candidate is durable and visible.
    Committed(View),
    /// Another head update definitely rejected this candidate.
    Conflict(View),
    /// The final view or classification cannot be determined safely.
    Pending(PendingCommit),
}

/// The result of resolving one uncertain publication.
#[derive(Debug)]
pub enum Resolution {
    /// The exact candidate is durable and visible.
    Committed(View),
    /// Retained evidence proves that the candidate did not publish.
    NotCommitted(View),
    /// A storage failure prevents result determination.
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
    /// The final view or classification cannot be determined safely.
    Pending(PendingCheckpoint),
}

/// The result of resolving one uncertain checkpoint publication.
#[derive(Debug)]
pub enum CheckpointResolution {
    /// The exact checkpoint is durable and visible.
    Published(View),
    /// Retained evidence proves that the checkpoint did not publish.
    NotPublished(View),
    /// A storage failure prevents result determination.
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
    staging_domain: Arc<StagingDomain>,
}

impl Log {
    /// Opens a writable log and creates its initial head when it is absent.
    ///
    /// # Errors
    ///
    /// Returns an error when the head cannot be created or read, or an existing
    /// head is invalid.
    pub async fn open(
        backend: &ValidatedBackend,
        log_id: &LogId,
        options: Options,
    ) -> Result<Self, Error> {
        let store = backend.scope(log_id);
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
        let staging_domain = Arc::new(StagingDomain);
        Ok(Self {
            store,
            options,
            incarnation,
            staging_domain,
        })
    }

    /// Opens an existing writable log without creating or updating its head.
    ///
    /// This method only reads the durable head. Constructing the supplied
    /// [`ValidatedBackend`] separately still performs capability probes,
    /// including temporary writes in an isolated probe namespace.
    ///
    /// # Errors
    ///
    /// Returns an error when the head is absent, unreadable, or invalid, or its
    /// durable options differ from `options`. An absent head is not initialized.
    pub async fn open_existing(
        backend: &ValidatedBackend,
        log_id: &LogId,
        options: Options,
    ) -> Result<Self, Error> {
        let store = backend.scope(log_id);
        let incarnation = Self::load_incarnation(&store, options).await?;
        Ok(Self {
            store,
            options,
            incarnation,
            staging_domain: Arc::new(StagingDomain),
        })
    }

    /// Clones this log with operation-local request admission.
    ///
    /// Preserves identity and staged proofs; all resulting clones and nested
    /// operations share the guard. Existing guards run first, in attachment order;
    /// a refusal stops admission before later guards or the client are invoked.
    /// Earlier admissions are not refunded when a later guard refuses. This
    /// changes only the new clone and never removes a caller's existing policy.
    /// Keep the same guarded handle across an operation's retries. Admission is
    /// optional caller policy, not part of the durable log contract. Backend
    /// validation and opening performed before attachment are not charged.
    /// Guards remain alive with the handle and in-flight futures. This is not
    /// an allocation limit: decoding and retained state need separate bounds.
    /// A refused first publication returns [`Error::RequestDenied`]. Resolution
    /// retains pending evidence when admission prevents classification or retry;
    /// refusal must never be interpreted as proof that publication failed.
    #[must_use]
    pub fn with_request_guard(&self, guard: Arc<dyn crate::RequestGuard>) -> Self {
        Self {
            store: self.store.with_request_guard(guard),
            ..self.clone()
        }
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

    /// Conditionally loads the head when it changed after `view`.
    ///
    /// # Errors
    ///
    /// Returns an error for a foreign view, a missing head, invalid durable
    /// bytes, or a backend failure.
    pub async fn refresh(&self, view: &View) -> Result<Option<View>, Error> {
        self.validate_view(view)?;
        match self
            .store
            .read_if_changed(
                StoreKey::Head,
                view.storage_version(),
                self.options.max_head_bytes,
            )
            .await?
        {
            ConditionalRead::NotModified => Ok(None),
            ConditionalRead::Modified(stored) => Ok(Some(self.view_from_stored(stored)?)),
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
        self.validate_view(view)?;
        if view.head().retention_ids.contains(&id) || view.head().active_plan.is_some() {
            return match self.refresh(view).await? {
                None if view.head().retention_ids.contains(&id) => {
                    Ok(RetentionStatus::Applied(view.clone()))
                }
                None => Ok(RetentionStatus::ActiveCollection(view.clone())),
                Some(current) if current.head().retention_ids.contains(&id) => {
                    Ok(RetentionStatus::Applied(current))
                }
                Some(current) if current.head().active_plan.is_some() => {
                    Ok(RetentionStatus::ActiveCollection(current))
                }
                Some(current) => Ok(RetentionStatus::Conflict(current)),
            };
        }
        if view.head().retention_ids.len() >= self.options.max_retention_ids {
            return Err(Error::LimitExceeded("retention IDs"));
        }

        let mut candidate = view.head().clone();
        candidate.retention_ids.insert(id);
        candidate.advance_generation()?;
        let bytes = format::encode_head(&candidate)?;
        self.validate_encoded_head(&bytes)?;
        match self
            .store
            .update(StoreKey::Head, bytes, view.storage_version().clone())
            .await
        {
            Ok(UpdateResult::Updated { version }) => {
                Ok(RetentionStatus::Applied(Self::view(candidate, version)))
            }
            Ok(UpdateResult::PreconditionFailed) => {
                let current = match self.load().await {
                    Ok(current) => current,
                    Err(Error::Store(_) | Error::RequestDenied) => {
                        return Ok(RetentionStatus::Pending);
                    }
                    Err(error) => return Err(error),
                };
                if current.head().retention_ids.contains(&id) {
                    Ok(RetentionStatus::Applied(current))
                } else if current.head().active_plan.is_some() {
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
        self.validate_view(view)?;
        if !view.head().retention_ids.contains(&id) {
            return match self.refresh(view).await? {
                None => Ok(RetentionStatus::Applied(view.clone())),
                Some(current) if !current.head().retention_ids.contains(&id) => {
                    Ok(RetentionStatus::Applied(current))
                }
                Some(current) => Ok(RetentionStatus::Conflict(current)),
            };
        }

        let mut candidate = view.head().clone();
        candidate.retention_ids.remove(&id);
        candidate.advance_generation()?;
        let bytes = format::encode_head(&candidate)?;
        self.validate_encoded_head(&bytes)?;
        match self
            .store
            .update(StoreKey::Head, bytes, view.storage_version().clone())
            .await
        {
            Ok(UpdateResult::Updated { version }) => {
                Ok(RetentionStatus::Applied(Self::view(candidate, version)))
            }
            Ok(UpdateResult::PreconditionFailed) => {
                let current = match self.load().await {
                    Ok(current) => current,
                    Err(Error::Store(_) | Error::RequestDenied) => {
                        return Ok(RetentionStatus::Pending);
                    }
                    Err(error) => return Err(error),
                };
                if current.head().retention_ids.contains(&id) {
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
        self.validate_view(view)?;
        if view.head().active_plan.is_some() {
            return Ok(CollectionStart::Active(view.clone()));
        }
        if !view.head().retention_ids.is_empty() {
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
        let mut candidate = view.head().clone();
        candidate.advance_generation()?;
        candidate.collection_epoch = epoch;
        candidate.active_plan = Some(plan_ref);
        let bytes = format::encode_head(&candidate)?;
        self.validate_encoded_head(&bytes)?;
        match self
            .store
            .update(StoreKey::Head, bytes, view.storage_version().clone())
            .await
        {
            Ok(UpdateResult::Updated { version }) => Ok(CollectionStart::Installed(
                Self::view(candidate, version),
                report,
            )),
            Ok(UpdateResult::PreconditionFailed) => {
                self.cleanup_collection_plan(plan_key).await?;
                match self.load().await {
                    Ok(current) => Ok(CollectionStart::Conflict(current)),
                    Err(Error::Store(_) | Error::RequestDenied) => Ok(CollectionStart::Pending),
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
        self.validate_view(view)?;
        let requested_plan = view.head().active_plan.as_ref();
        let current = self.load().await?;
        let Some(plan_ref) = current.head().active_plan.as_ref() else {
            if let Some(requested_plan) = requested_plan {
                let key = Self::collection_plan_key(view.head().incarnation, requested_plan);
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
        let plan_key = Self::collection_plan_key(current.head().incarnation, plan_ref);
        let plan = self.read_collection_plan(current.head(), plan_ref).await?;
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
                Err(Error::RequestDenied) => {
                    report.delete_attempts -= batch.len();
                    return Ok(CollectionFinish::Pending(report));
                }
                Err(Error::Store(_)) => return Ok(CollectionFinish::Pending(report)),
                Err(error) => return Err(error),
            }
        }

        drop(plan);

        let mut candidate = current.head().clone();
        candidate.active_plan = None;
        candidate.advance_generation()?;
        let bytes = format::encode_head(&candidate)?;
        self.validate_encoded_head(&bytes)?;
        match self
            .store
            .update(StoreKey::Head, bytes, current.storage_version().clone())
            .await
        {
            Ok(UpdateResult::Updated { version }) => {
                self.cleanup_collection_plan(plan_key).await?;
                Ok(CollectionFinish::Complete(
                    Self::view(candidate, version),
                    report,
                ))
            }
            Ok(UpdateResult::PreconditionFailed) => {
                let current = match self.load().await {
                    Ok(current) => current,
                    Err(Error::Store(_) | Error::RequestDenied) => {
                        return Ok(CollectionFinish::Pending(report));
                    }
                    Err(error) => return Err(error),
                };
                if current.head().active_plan.is_none() {
                    self.cleanup_collection_plan(plan_key).await?;
                    Ok(CollectionFinish::Complete(current, report))
                } else {
                    Ok(CollectionFinish::Conflict(current, report))
                }
            }
            Err(Error::Store(_) | Error::RequestDenied) => Ok(CollectionFinish::Pending(report)),
            Err(error) => Err(error),
        }
    }

    /// Stores one immutable content-addressed blob for an observed collection epoch.
    ///
    /// Clones of this log handle can use the returned proof. A separately
    /// opened handle must verify the durable reference again. The backend must
    /// keep the exact created bytes until object-log garbage collection deletes
    /// their physical key.
    ///
    /// # Errors
    ///
    /// Returns an error for a foreign view, an active collection fence, a
    /// configured limit, or a backend failure. A physical identity collision
    /// is retried with a new ID.
    pub async fn put_object(&self, view: &View, bytes: Bytes) -> Result<StagedObject, Error> {
        self.validate_view(view)?;
        if bytes.len() > self.options.max_object_bytes {
            return Err(Error::LimitExceeded("object bytes"));
        }
        let blocked = self.active_collection_candidates(view.head()).await?;
        let object = self
            .create_fresh_object_with(ObjectKind::Blob, bytes, blocked.as_deref(), StorageId::new)
            .await?;
        Ok(self.staged_object(view, object))
    }

    /// Returns the exact encoded size of a node before its children are stored.
    ///
    /// Includes the payload, authenticated child references, and node envelope.
    /// Child identities and kinds do not affect their encoded width; only their
    /// byte lengths are needed. Performs no storage I/O or buffer allocation.
    /// This checks encoding fit, not child existence, provenance, or publication
    /// readiness. [`Self::put_node`] still validates the supplied child proofs.
    ///
    /// # Errors
    /// Returns a limit error for too many children, size overflow, or an encoded
    /// node exceeding this log's maximum object size.
    pub fn node_size(
        &self,
        payload_bytes: usize,
        child_lengths: impl IntoIterator<Item = u64>,
    ) -> Result<usize, Error> {
        format::node_size(payload_bytes, child_lengths, self.options).map(|size| size.encoded)
    }

    /// Stores one immutable reference node after its direct children exist.
    ///
    /// The opaque payload can describe an adapter-specific tree node. All
    /// durable child objects must appear in `children` so a generic collector
    /// can traverse the complete graph.
    ///
    /// # Errors
    ///
    /// Returns an error for a foreign view, an invalid or stale child proof,
    /// an active collection fence, a configured limit, or a backend failure.
    pub async fn put_node(
        &self,
        view: &View,
        payload: Bytes,
        children: Vec<StagedObject>,
    ) -> Result<StagedObject, Error> {
        self.validate_staged_objects(view, &children)?;
        let children = children
            .into_iter()
            .map(|child| child.object)
            .collect::<Vec<_>>();
        let node = format::Node { payload, children };
        let bytes = format::encode_node(&node, self.options)?;
        let blocked = self.active_collection_candidates(view.head()).await?;
        let object = self
            .create_fresh_object_with(ObjectKind::Node, bytes, blocked.as_deref(), StorageId::new)
            .await?;
        Ok(self.staged_object(view, object))
    }

    /// Verifies existing object graphs and creates publication proofs.
    ///
    /// # Errors
    ///
    /// Returns an error for a foreign view, an invalid reference graph, an
    /// active collection fence, a configured limit, or a backend failure.
    pub async fn stage_objects(
        &self,
        view: &View,
        objects: Vec<ObjectRef>,
    ) -> Result<Vec<StagedObject>, Error> {
        self.validate_view(view)?;
        self.validate_dependencies(&objects)?;
        self.verify_publication_dependencies(view.head(), &objects)
            .await?;
        Ok(objects
            .into_iter()
            .map(|object| self.staged_object(view, object))
            .collect())
    }

    /// Reads and verifies one object from this log namespace.
    ///
    /// # Errors
    ///
    /// Returns expiry when a missing object belongs to an older unretained
    /// view. A missing object in the current epoch is corruption.
    pub async fn read_object(&self, view: &View, object: &ObjectRef) -> Result<Bytes, Error> {
        self.validate_view(view)?;
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
        self.validate_view(view)?;
        if object.kind != ObjectKind::Node {
            return Err(Error::InvalidFormat(
                "a node read requires a reference-node object".to_owned(),
            ));
        }
        let bytes = self.read_immutable_for_view(view, object).await?;
        let node = format::decode_node(&bytes, self.options)?;
        Ok(ReferenceNode {
            payload: node.payload,
            children: node.children,
        })
    }

    /// Reads an authenticated node and derives publication proofs for its children.
    ///
    /// The parent must carry a proof from this log handle (or its clone) and
    /// the supplied collection epoch. Only the parent is read and verified;
    /// its existing graph proof permits reuse of unchanged child subtrees.
    /// The returned payload and ordered children come from that verified node.
    ///
    /// Separately opened handles must obtain a fresh parent proof through
    /// materialization or [`Self::stage_objects`]. This operation does not
    /// refresh the view or grant a retention lease.
    ///
    /// # Errors
    ///
    /// Returns an error for foreign views or proofs, epoch mismatches, non-node
    /// parents, invalid storage, or configured limits. A missing parent has the
    /// same expiry or corruption classification as [`Self::read_node`].
    pub async fn read_staged_node(
        &self,
        view: &View,
        node: &StagedObject,
    ) -> Result<(Bytes, Vec<StagedObject>), Error> {
        self.validate_view(view)?;
        self.validate_staged_object(view, node)?;
        let node = self.read_node(view, node.reference()).await?;
        let children = node
            .children
            .into_iter()
            .map(|child| self.staged_object(view, child))
            .collect();
        Ok((node.payload, children))
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

    /// Checks whether a transaction can be prepared against an observed view.
    ///
    /// The successful path performs no I/O and makes no allocation. A later
    /// concurrent update can still make the view stale.
    ///
    /// # Errors
    ///
    /// Returns an error for a foreign view, a full tail, or a transaction ID
    /// that is already committed.
    pub fn preflight(&self, view: &View, transaction_id: TransactionId) -> Result<(), Error> {
        self.validate_view(view)?;
        self.validate_commit_position(view, transaction_id)
    }

    /// Builds one immutable candidate against an exact observed view.
    ///
    /// This operation does not access storage and does not rebase the opaque
    /// operation onto a newer view.
    ///
    /// # Errors
    ///
    /// Returns an error for a foreign view, an invalid or stale object proof,
    /// or a configured size or tail limit.
    pub fn prepare(
        &self,
        view: &View,
        transaction_id: TransactionId,
        operation: Bytes,
        result: Bytes,
        objects: Vec<StagedObject>,
    ) -> Result<PreparedCommit, Error> {
        self.validate_staged_objects(view, &objects)?;
        self.validate_prepared_sizes(&operation, &result)?;
        self.validate_commit_position(view, transaction_id)?;
        Ok(PreparedCommit {
            view: view.clone(),
            staging_domain: Arc::clone(&self.staging_domain),
            transaction_id,
            storage_id: StorageId::new(),
            operation,
            result,
            objects: objects.into_iter().map(|staged| staged.object).collect(),
        })
    }

    /// Stages and conditionally publishes one exact prepared commit.
    ///
    /// A definite precondition failure returns [`CommitStatus::Conflict`] when
    /// the winning view can also be read. [`CommitStatus::Pending`] preserves
    /// the candidate when the safe final view or classification is unavailable.
    /// Same-process staged proofs avoid immutable dependency reads. Reopened or
    /// decoded recovery evidence verifies its complete dependency graph.
    ///
    /// # Errors
    ///
    /// Returns an error when the candidate is invalid, a referenced object is
    /// not durable and valid, immutable staging fails, or a winning head is
    /// invalid.
    pub async fn commit(&self, prepared: PreparedCommit) -> Result<CommitStatus, Error> {
        let mut prepared = prepared;
        self.validate_prepared(&prepared)?;
        let (commit_ref, commit_bytes) = self.encode_prepared(&prepared)?;
        self.verify_publication(
            prepared.view.head(),
            self.commit_immutable_key(&commit_ref),
            &prepared.objects,
            &prepared.staging_domain,
        )
        .await?;
        self.create_new_commit(self.commit_key(&commit_ref), commit_bytes)
            .await?;
        prepared.staging_domain = Arc::clone(&self.staging_domain);
        let candidate = Self::candidate_head(&prepared, &commit_ref)?;
        let candidate_bytes = format::encode_head(&candidate)?;
        self.validate_encoded_head(&candidate_bytes)?;

        match self
            .store
            .update(
                StoreKey::Head,
                candidate_bytes,
                prepared.view.storage_version().clone(),
            )
            .await
        {
            Ok(UpdateResult::Updated { version }) => {
                Ok(CommitStatus::Committed(Self::view(candidate, version)))
            }
            Ok(UpdateResult::PreconditionFailed) => {
                let pending = PendingCommit {
                    prepared: Box::new(prepared),
                    commit_ref,
                };
                let current = match self.load().await {
                    Ok(view) => view,
                    Err(Error::Store(_) | Error::RequestDenied) => {
                        return Ok(CommitStatus::Pending(pending));
                    }
                    Err(error) => return Err(error),
                };
                match Self::classify_resolution(&pending, current)? {
                    Some(Resolution::Committed(view)) => Ok(CommitStatus::Committed(view)),
                    Some(Resolution::NotCommitted(view)) => Ok(CommitStatus::Conflict(view)),
                    Some(Resolution::Expired(_)) | None => Ok(CommitStatus::Pending(pending)),
                    Some(Resolution::StillPending(_)) => Err(Error::InvalidFormat(
                        "an in-memory classification returned pending evidence".to_owned(),
                    )),
                }
            }
            Err(Error::Store(_)) => Ok(CommitStatus::Pending(PendingCommit {
                prepared: Box::new(prepared),
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
        let mut pending = pending;
        self.validate_pending(&pending)?;
        let current = match self.load().await {
            Ok(view) => view,
            Err(Error::Store(_) | Error::RequestDenied) => {
                return Ok(Resolution::StillPending(pending));
            }
            Err(error) => return Err(error),
        };

        if let Some(resolution) = Self::classify_resolution(&pending, current)? {
            if let Resolution::Committed(view) = &resolution
                && Self::tail_contains(view, &pending.commit_ref)
                && !self.proof_matches(&pending.prepared.staging_domain)
            {
                match self.verify_published_commit(&pending.commit_ref).await {
                    Ok(()) => {}
                    Err(Error::Store(_) | Error::RequestDenied) => {
                        return Ok(Resolution::StillPending(pending));
                    }
                    Err(error) => return Err(error),
                }
            }
            return Ok(resolution);
        }

        let (_, commit_bytes) = self.encode_prepared(&pending.prepared)?;
        match self
            .verify_publication(
                pending.prepared.view.head(),
                self.commit_immutable_key(&pending.commit_ref),
                &pending.prepared.objects,
                &pending.prepared.staging_domain,
            )
            .await
        {
            Ok(()) => {}
            Err(Error::Store(_) | Error::RequestDenied) => {
                return Ok(Resolution::StillPending(pending));
            }
            Err(error) => return Err(error),
        }
        if !self.proof_matches(&pending.prepared.staging_domain) {
            match self
                .ensure_immutable(self.commit_key(&pending.commit_ref), commit_bytes)
                .await
            {
                Ok(()) => {}
                Err(Error::Store(_) | Error::RequestDenied) => {
                    return Ok(Resolution::StillPending(pending));
                }
                Err(error) => return Err(error),
            }
        }
        pending.prepared.staging_domain = Arc::clone(&self.staging_domain);
        let candidate = Self::candidate_head(&pending.prepared, &pending.commit_ref)?;
        let candidate_bytes = format::encode_head(&candidate)?;
        self.validate_encoded_head(&candidate_bytes)?;
        match self
            .store
            .update(
                StoreKey::Head,
                candidate_bytes,
                pending.prepared.view.storage_version().clone(),
            )
            .await
        {
            Ok(UpdateResult::Updated { version }) => {
                Ok(Resolution::Committed(Self::view(candidate, version)))
            }
            Err(Error::Store(_) | Error::RequestDenied) => Ok(Resolution::StillPending(pending)),
            Err(error) => Err(error),
            Ok(UpdateResult::PreconditionFailed) => {
                let current = match self.load().await {
                    Ok(view) => view,
                    Err(Error::Store(_) | Error::RequestDenied) => {
                        return Ok(Resolution::StillPending(pending));
                    }
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
            prepared: Box::new(prepared),
            commit_ref,
        })
        .await
    }

    /// Bounds encoded history bytes buffered by materialization or checkpoint validation.
    ///
    /// This is the larger of the checkpoint length and the largest concurrently
    /// buffered tail window, including one record held by the consumer. It uses
    /// authenticated view lengths and performs no I/O. Successful evaluation
    /// uses a fixed stack buffer without heap allocation.
    ///
    /// This is not a heap bound: callers must separately budget decoder
    /// allocations, application state, publication proofs, and other work. It
    /// does not bound [`Self::read_tail`] or [`Self::resolve_checkpoint`], which
    /// may retain the complete tail, or missing-read head classification.
    ///
    /// # Errors
    ///
    /// Returns an error for a foreign view or unrepresentable lengths or sums.
    pub fn materialization_read_bound(&self, view: &View) -> Result<usize, Error> {
        self.validate_view(view)?;
        let checkpoint = usize::try_from(view.checkpoint().map_or(0, |value| value.object().len()))
            .map_err(|_| Error::LimitExceeded("materialization encoded bytes"))?;
        let mut largest = [0_usize; MAX_CONCURRENT_READS + 1];
        for record in view.tail() {
            let mut length = usize::try_from(record.len())
                .map_err(|_| Error::LimitExceeded("materialization encoded bytes"))?;
            for retained in &mut largest {
                if length > *retained {
                    std::mem::swap(retained, &mut length);
                }
            }
        }
        let tail = largest.into_iter().try_fold(0_usize, |total, length| {
            total
                .checked_add(length)
                .ok_or(Error::LimitExceeded("materialization encoded bytes"))
        })?;
        Ok(checkpoint.max(tail))
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
        self.tail_records(view)?
            .try_fold(
                Vec::with_capacity(view.tail().len()),
                |mut records, record| async move {
                    records.push(record);
                    Ok(records)
                },
            )
            .await
    }

    // Keep ordered decoding bounded independently of the tail length. Callers
    // consume the stream to completion before returning or publishing state.
    pub(crate) fn tail_records<'a>(
        &'a self,
        view: &'a View,
    ) -> Result<impl futures::Stream<Item = Result<CommitRecord, Error>> + 'a, Error> {
        self.validate_view(view)?;
        let mut expected_tip = view
            .checkpoint()
            .map(|checkpoint| checkpoint.through_commit);
        Ok(stream::iter(view.tail().iter().cloned())
            .map(move |reference| self.read_commit_optional(reference))
            .buffered(MAX_CONCURRENT_READS)
            .then(move |record| async move {
                match record? {
                    Some(record) => Ok(record),
                    None => Err(self.missing_read_error(view).await?),
                }
            })
            .map(move |record| {
                let record = record?;
                if record.expected_tip != expected_tip {
                    return Err(Error::InvalidFormat(
                        "the commit tail has a broken parent chain".to_owned(),
                    ));
                }
                expected_tip = Some(record.reference.digest);
                Ok(record)
            }))
    }

    /// Publishes an opaque base that covers one exact prefix of `view`.
    ///
    /// The base object becomes durable before the index update. A concurrent
    /// index update returns [`CheckpointStatus::Conflict`] and preserves the
    /// current durable history.
    /// Same-process staged proofs avoid immutable dependency reads. Reopened
    /// pending evidence verifies its complete dependency graph.
    ///
    /// # Errors
    ///
    /// Returns an error for a foreign view, a reference outside its active
    /// tail, an invalid or stale object proof, an oversized base, invalid
    /// history, or a backend failure. A store error during the index update can
    /// hide a successful maintenance update. The method then returns
    /// [`CheckpointStatus::Pending`]. The caller must preserve that evidence
    /// and pass it to [`Log::resolve_checkpoint`].
    pub async fn publish_checkpoint(
        &self,
        view: &View,
        through: &CommitRef,
        snapshot: Bytes,
        objects: Vec<StagedObject>,
    ) -> Result<CheckpointStatus, Error> {
        self.tail_records(view)?
            .try_for_each(|_| futures::future::ready(Ok(())))
            .await?;
        self.validate_staged_objects(view, &objects)?;
        let objects = objects
            .into_iter()
            .map(|staged| staged.object)
            .collect::<Vec<_>>();
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
        let blocked = self.active_collection_candidates(view.head()).await?;
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
            staging_domain: Arc::clone(&self.staging_domain),
            through: through.clone(),
            checkpoint: CheckpointRef {
                through_sequence: through.sequence,
                through_commit: through.digest,
                object,
            },
        };

        match self
            .store
            .update(
                StoreKey::Head,
                candidate_bytes,
                view.storage_version().clone(),
            )
            .await
        {
            Ok(UpdateResult::Updated { version }) => {
                Ok(CheckpointStatus::Published(Self::view(candidate, version)))
            }
            Ok(UpdateResult::PreconditionFailed) => {
                let current = match self.load().await {
                    Ok(view) => view,
                    Err(Error::Store(_) | Error::RequestDenied) => {
                        return Ok(CheckpointStatus::Pending(pending));
                    }
                    Err(error) => return Err(error),
                };
                match Self::classify_checkpoint(&pending, current)? {
                    CheckpointEvidence::Published(view) => Ok(CheckpointStatus::Published(view)),
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
        let mut pending = pending;
        self.validate_view(&pending.view)?;
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
            Err(Error::Store(_) | Error::RequestDenied) => {
                return Ok(CheckpointResolution::StillPending(pending));
            }
            Err(error) => return Err(error),
        };
        match Self::classify_checkpoint(&pending, current)? {
            CheckpointEvidence::Published(view) => {
                if self.proof_matches(&pending.staging_domain) {
                    return Ok(CheckpointResolution::Published(view));
                }
                return match self.verify_checkpoint(&pending.checkpoint).await {
                    Ok(()) => Ok(CheckpointResolution::Published(view)),
                    Err(Error::Store(_) | Error::RequestDenied) => {
                        Ok(CheckpointResolution::StillPending(pending))
                    }
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
            Err(Error::Store(_) | Error::RequestDenied) => {
                return Ok(CheckpointResolution::StillPending(pending));
            }
            Err(error) => return Err(error),
        }
        match self.verify_checkpoint_publication(&pending).await {
            Ok(()) => {}
            Err(Error::Store(_) | Error::RequestDenied) => {
                return Ok(CheckpointResolution::StillPending(pending));
            }
            Err(error) => return Err(error),
        }
        pending.staging_domain = Arc::clone(&self.staging_domain);
        let candidate_bytes = format::encode_head(&candidate)?;
        self.validate_encoded_head(&candidate_bytes)?;
        match self
            .store
            .update(
                StoreKey::Head,
                candidate_bytes,
                pending.view.storage_version().clone(),
            )
            .await
        {
            Ok(UpdateResult::Updated { version }) => Ok(CheckpointResolution::Published(
                Self::view(candidate, version),
            )),
            Err(Error::Store(_) | Error::RequestDenied) => {
                Ok(CheckpointResolution::StillPending(pending))
            }
            Err(error) => Err(error),
            Ok(UpdateResult::PreconditionFailed) => {
                let current = match self.load().await {
                    Ok(view) => view,
                    Err(Error::Store(_) | Error::RequestDenied) => {
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
        self.validate_view(view)?;
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
        self.verify_object_graph(&checkpoint.objects).await
    }

    async fn verify_checkpoint_publication(
        &self,
        pending: &PendingCheckpoint,
    ) -> Result<(), Error> {
        if self.proof_matches(&pending.staging_domain) {
            return self
                .verify_publication(
                    pending.view.head(),
                    self.object_immutable_key(&pending.checkpoint.object),
                    &[],
                    &pending.staging_domain,
                )
                .await;
        }
        let checkpoint = self.load_checkpoint(&pending.checkpoint).await?;
        self.verify_publication(
            pending.view.head(),
            self.object_immutable_key(&pending.checkpoint.object),
            &checkpoint.objects,
            &pending.staging_domain,
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
        Ok(Self::view(head, stored.version))
    }

    fn view(head: Head, version: UpdateVersion) -> View {
        View {
            observed: Arc::new(ObservedState { head, version }),
        }
    }

    fn validate_view(&self, view: &View) -> Result<(), Error> {
        if view.head().log_id != *self.store.log_id() {
            return Err(Error::InvalidFormat(
                "the view belongs to another log".to_owned(),
            ));
        }
        if view.head().incarnation != self.incarnation {
            return Err(Error::InvalidFormat(
                "the view belongs to another log incarnation".to_owned(),
            ));
        }
        if view.head().options != self.options {
            return Err(Error::ConfigurationMismatch("options"));
        }
        Ok(())
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

    pub(crate) fn staged_object(&self, view: &View, object: ObjectRef) -> StagedObject {
        StagedObject {
            object,
            domain: Arc::clone(&self.staging_domain),
            collection_epoch: view.collection_epoch(),
        }
    }

    fn validate_staged_objects(&self, view: &View, objects: &[StagedObject]) -> Result<(), Error> {
        self.validate_view(view)?;
        if objects.len() > self.options.max_object_refs {
            return Err(Error::LimitExceeded("object references"));
        }
        objects
            .iter()
            .try_for_each(|object| self.validate_staged_object(view, object))
    }

    fn validate_staged_object(&self, view: &View, object: &StagedObject) -> Result<(), Error> {
        if !Arc::ptr_eq(&object.domain, &self.staging_domain)
            || object.collection_epoch != view.collection_epoch()
        {
            return Err(Error::InvalidStagedObject);
        }
        Ok(())
    }

    fn proof_matches(&self, proof: &Arc<StagingDomain>) -> bool {
        Arc::ptr_eq(proof, &self.staging_domain)
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
        self.validate_view(&prepared.view)?;
        self.validate_prepared_sizes(&prepared.operation, &prepared.result)?;
        self.validate_dependencies(&prepared.objects)?;
        self.validate_commit_position(&prepared.view, prepared.transaction_id)
    }

    fn validate_commit_position(
        &self,
        view: &View,
        transaction_id: TransactionId,
    ) -> Result<(), Error> {
        if view.tail().len() >= self.options.max_tail_entries {
            return Err(Error::LimitExceeded("active tail entries"));
        }
        if view
            .head()
            .tail
            .iter()
            .chain(&view.head().recent_outcomes)
            .any(|entry| entry.transaction_id == transaction_id)
        {
            return Err(Error::InvalidFormat(
                "the transaction ID is already committed".to_owned(),
            ));
        }
        Ok(())
    }

    fn encode_prepared(&self, prepared: &PreparedCommit) -> Result<(CommitRef, Bytes), Error> {
        let commit = format::Commit {
            log_id: self.store.log_id().clone(),
            incarnation: self.incarnation,
            transaction_id: prepared.transaction_id,
            expected_tip: prepared.view.head().tip(),
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
            sequence: prepared.view.head().next_sequence,
            transaction_id: prepared.transaction_id,
            storage_id: prepared.storage_id,
            digest: Digest::of(&bytes),
            len,
        };
        Ok((reference, bytes))
    }

    fn candidate_head(prepared: &PreparedCommit, commit_ref: &CommitRef) -> Result<Head, Error> {
        let mut head = prepared.view.head().clone();
        head.advance_generation()?;
        head.next_sequence = head
            .next_sequence
            .checked_add(1)
            .ok_or(Error::LimitExceeded("commit sequence"))?;
        head.tail.push(commit_ref.clone());
        Ok(head)
    }

    fn checkpoint_head(view: &View, through: &CommitRef, object: ObjectRef) -> Result<Head, Error> {
        let mut head = view.head().clone();
        let through_index = head
            .tail
            .iter()
            .position(|entry| entry == through)
            .ok_or_else(|| {
                Error::InvalidFormat("the checkpoint entry is not in the active tail".to_owned())
            })?;
        head.recent_outcomes
            .extend(head.tail.drain(..=through_index));
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
        let head = current.head();
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

        let source = &pending.prepared.view;
        if head == source.head() && current.storage_version() == source.storage_version() {
            return Ok(None);
        }
        Ok(Some(Resolution::NotCommitted(current)))
    }

    fn contains_commit(view: &View, target: &CommitRef) -> bool {
        view.head()
            .tail
            .iter()
            .chain(&view.head().recent_outcomes)
            .any(|entry| entry == target)
    }

    fn tail_contains(view: &View, target: &CommitRef) -> bool {
        view.tail().iter().any(|entry| entry == target)
    }

    fn classify_checkpoint(
        pending: &PendingCheckpoint,
        current: View,
    ) -> Result<CheckpointEvidence, Error> {
        if current.checkpoint() == Some(&pending.checkpoint) {
            return Ok(CheckpointEvidence::Published(current));
        }
        if current.head() == pending.view.head()
            && current.storage_version() == pending.view.storage_version()
        {
            return Ok(CheckpointEvidence::Retry);
        }
        let next_generation = pending
            .view
            .head()
            .generation
            .checked_add(1)
            .ok_or(Error::LimitExceeded("head generation"))?;
        match current.generation().cmp(&next_generation) {
            std::cmp::Ordering::Less => Err(Error::InvalidFormat(
                "the head precedes pending checkpoint evidence".to_owned(),
            )),
            std::cmp::Ordering::Equal => Ok(CheckpointEvidence::NotPublished(current)),
            std::cmp::Ordering::Greater => Ok(CheckpointEvidence::Expired(current)),
        }
    }

    fn validate_pending(&self, pending: &PendingCommit) -> Result<(), Error> {
        self.validate_view(&pending.prepared.view)?;
        self.validate_prepared_sizes(&pending.prepared.operation, &pending.prepared.result)?;
        self.validate_dependencies(&pending.prepared.objects)?;
        if pending.prepared.view.tail().len() >= self.options.max_tail_entries {
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
        self.read_commit_optional(reference.clone())
            .await?
            .ok_or_else(|| Error::InvalidFormat("a referenced commit is missing".to_owned()))
    }

    async fn read_commit_optional(
        &self,
        reference: CommitRef,
    ) -> Result<Option<CommitRecord>, Error> {
        if reference.len
            > u64::try_from(self.options.max_commit_bytes)
                .map_err(|_| Error::LimitExceeded("encoded commit bytes"))?
        {
            return Err(Error::LimitExceeded("encoded commit bytes"));
        }
        let declared_len = usize::try_from(reference.len)
            .map_err(|_| Error::LimitExceeded("commit byte length"))?;
        let Some(stored) = self
            .store
            .read(self.commit_key(&reference), declared_len)
            .await
            .map_err(|error| match error {
                Error::LimitExceeded("read bytes") => Error::CorruptObject,
                error => error,
            })?
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
            reference,
            expected_tip: commit.expected_tip,
            operation: commit.operation,
            result: commit.result,
            objects: commit.objects,
        }))
    }

    async fn verify_published_commit(&self, reference: &CommitRef) -> Result<(), Error> {
        let record = self.read_commit(reference).await?;
        self.verify_object_graph(&record.objects).await
    }

    async fn verify_publication(
        &self,
        head: &Head,
        new_key: ImmutableKey,
        objects: &[ObjectRef],
        staging: &Arc<StagingDomain>,
    ) -> Result<(), Error> {
        let blocked = if self.proof_matches(staging) {
            self.active_collection_candidates(head).await?
        } else {
            self.verify_publication_dependencies(head, objects).await?
        };
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
        let Some(blocked) = self.active_collection_candidates(head).await? else {
            self.verify_object_graph(objects).await?;
            return Ok(None);
        };
        let mut visited = HashMap::with_capacity(objects.len());
        self.mark_object_graph(objects, &mut visited, Some(&blocked))
            .await?;
        Ok(Some(blocked))
    }

    async fn verify_object_graph(&self, objects: &[ObjectRef]) -> Result<(), Error> {
        let mut visited = HashMap::with_capacity(objects.len());
        self.mark_object_graph(objects, &mut visited, None).await
    }

    async fn active_collection_candidates(
        &self,
        head: &Head,
    ) -> Result<Option<Vec<CollectionCandidate>>, Error> {
        let Some(plan_ref) = head.active_plan.as_ref() else {
            return Ok(None);
        };
        Ok(Some(
            self.read_collection_plan(head, plan_ref).await?.candidates,
        ))
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
                let node = format::decode_node(&bytes, self.options)?;
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
            Ok(()) | Err(Error::Store(_) | Error::RequestDenied) => Ok(()),
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

    async fn create_fresh_object_with(
        &self,
        kind: ObjectKind,
        bytes: Bytes,
        blocked: Option<&[CollectionCandidate]>,
        mut new_storage_id: impl FnMut() -> StorageId,
    ) -> Result<ObjectRef, Error> {
        let len =
            u64::try_from(bytes.len()).map_err(|_| Error::LimitExceeded("object byte length"))?;
        let digest = Digest::of(&bytes);
        for _ in 0..MAX_FRESH_OBJECT_ATTEMPTS {
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

    use crate::sim::{Failure, FailurePhase, FaultStore, Operation};
    use crate::{LogId, ValidatedBackend};
    use object_store::memory::InMemory;
    use object_store::path::Path;

    use super::*;

    include!("request_guard_tests.rs");

    #[tokio::test]
    async fn commit_ref_len_matches_the_encoded_commit_and_read_reference()
    -> Result<(), Box<dyn std::error::Error>> {
        let log = test_log("commit-ref-len", Options::default()).await?;
        let view = log.load().await?;
        let prepared = log.prepare(
            &view,
            TransactionId::new(),
            Bytes::from_static(b"operation"),
            Bytes::from_static(b"result"),
            Vec::new(),
        )?;
        let (reference, bytes) = log.encode_prepared(&prepared)?;
        let encoded_len = u64::try_from(bytes.len())?;
        assert_eq!(reference.len(), encoded_len);

        let CommitStatus::Committed(committed) = log.commit(prepared).await? else {
            return Err("commit lost its uncontended publication".into());
        };
        let published = committed
            .tail()
            .last()
            .ok_or("committed view has no tail")?;
        assert_eq!(published.len(), encoded_len);
        assert_eq!(
            log.read_commit(published).await?.reference().len(),
            encoded_len
        );
        Ok(())
    }

    #[derive(Debug, Default)]
    struct ReadProbeStore {
        inner: InMemory,
        body_polls: Arc<std::sync::atomic::AtomicUsize>,
        oversize_commits: bool,
        delay_heads: std::sync::atomic::AtomicBool,
        head_reads: std::sync::atomic::AtomicUsize,
    }

    impl std::fmt::Display for ReadProbeStore {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("read probe store")
        }
    }

    #[async_trait::async_trait]
    impl object_store::ObjectStore for ReadProbeStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: object_store::PutPayload,
            options: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            self.inner.put_opts(location, payload, options).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            options: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, options).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            if location.as_ref().ends_with("/index.cbor")
                && self.delay_heads.load(std::sync::atomic::Ordering::Relaxed)
            {
                self.head_reads
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
            let mut result = self.inner.get_opts(location, options).await?;
            if self.oversize_commits && location.as_ref().contains("/commits/") {
                result.meta.size += 1;
                result.range.end += 1;
                let polls = Arc::clone(&self.body_polls);
                let payload = std::mem::replace(
                    &mut result.payload,
                    object_store::GetResultPayload::Stream(stream::empty().boxed()),
                );
                let object_store::GetResultPayload::Stream(body) = payload else {
                    return Err(object_store::Error::Generic {
                        store: "oversized commit test",
                        source: "memory store did not return a stream".into(),
                    });
                };
                result.payload = object_store::GetResultPayload::Stream(
                    body.chain(stream::once(async { Ok(Bytes::from_static(b"x")) }))
                        .inspect(move |_| {
                            polls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        })
                        .boxed(),
                );
            }
            Ok(result)
        }

        fn delete_stream(
            &self,
            locations: futures::stream::BoxStream<'static, object_store::Result<Path>>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    #[tokio::test]
    async fn oversized_commit_is_rejected_without_polling_its_body()
    -> Result<(), Box<dyn std::error::Error>> {
        let store = Arc::new(ReadProbeStore {
            oversize_commits: true,
            ..ReadProbeStore::default()
        });
        let backend = ValidatedBackend::new(store.clone(), Path::from("read-bound")).await?;
        let log = Log::open(&backend, &LogId::new("commit")?, Options::default()).await?;
        let view = log.load().await?;
        let prepared = log.prepare(
            &view,
            TransactionId::new(),
            Bytes::from_static(b"operation"),
            Bytes::new(),
            Vec::new(),
        )?;
        let CommitStatus::Committed(view) = log.commit(prepared).await? else {
            return Err("commit lost its uncontended publication".into());
        };
        assert!(view.tail()[0].len() + 1 < log.options.max_commit_bytes as u64);
        assert!(matches!(
            log.read_tail(&view).await,
            Err(Error::CorruptObject)
        ));
        assert_eq!(
            store.body_polls.load(std::sync::atomic::Ordering::Relaxed),
            0
        );
        Ok(())
    }

    #[tokio::test]
    async fn node_size_preflights_without_io_and_matches_stored_nodes()
    -> Result<(), Box<dyn std::error::Error>> {
        let faults = FaultStore::new(InMemory::new());
        let backend =
            ValidatedBackend::new(Arc::new(faults.clone()), Path::from("node-size")).await?;
        let log = Log::open(&backend, &LogId::new("node")?, Options::default()).await?;
        faults.reset();
        let predicted = log.node_size(3, [24, 256])?;
        assert!(matches!(
            log.node_size(usize::MAX, []),
            Err(Error::LimitExceeded("object bytes"))
        ));
        assert_eq!(faults.metrics().operation(Operation::Get).requests, 0);
        assert_eq!(faults.metrics().operation(Operation::Put).requests, 0);
        let view = log.load().await?;
        let first = log.put_object(&view, Bytes::from(vec![1; 24])).await?;
        let second = log.put_object(&view, Bytes::from(vec![2; 256])).await?;
        let node = log
            .put_node(&view, Bytes::from_static(b"abc"), vec![first, second])
            .await?;
        assert_eq!(node.reference().len(), u64::try_from(predicted)?);
        Ok(())
    }

    #[tokio::test]
    async fn checkpoint_collision_exhaustion_remains_bounded()
    -> Result<(), Box<dyn std::error::Error>> {
        let faults = FaultStore::new(InMemory::new());
        let backend = ValidatedBackend::new(
            Arc::new(faults.clone()),
            Path::from("checkpoint-collisions"),
        )
        .await?;
        let log = Log::open(
            &backend,
            &LogId::new("checkpoint-collisions")?,
            Options::default(),
        )
        .await?;
        let bytes = Bytes::from_static(b"checkpoint bytes");
        let collision = StorageId::from_uuid(uuid::Uuid::from_u128(7));
        let occupied = ObjectRef {
            kind: ObjectKind::Checkpoint,
            storage_id: collision,
            digest: Digest::of(&bytes),
            len: u64::try_from(bytes.len())?,
        };
        log.store
            .create(log.object_key(&occupied), bytes.clone())
            .await?;
        faults.reset();
        let uploaded = bytes.len() * MAX_FRESH_OBJECT_ATTEMPTS;
        let result = log
            .create_fresh_object_with(ObjectKind::Checkpoint, bytes, None, || collision)
            .await;
        assert!(matches!(
            result,
            Err(Error::LimitExceeded("fresh physical storage identity"))
        ));
        assert_eq!(
            faults.metrics().operation(Operation::Put).requests,
            u64::try_from(MAX_FRESH_OBJECT_ATTEMPTS)?
        );
        assert_eq!(faults.metrics().uploaded_bytes(), u64::try_from(uploaded)?);
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
            &backend,
            &LogId::new("ambiguous-storage-id-collision")?,
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
            &view,
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
    async fn active_plan_rejects_existing_ref_with_conflicting_length()
    -> Result<(), Box<dyn std::error::Error>> {
        let log = test_log("conflicting-lengths", Options::default()).await?;
        let source = log.load().await?;
        let object = log
            .put_object(&source, Bytes::from_static(b"object"))
            .await?;
        let mut conflicting = object.reference().clone();
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
        assert!(matches!(
            log.stage_objects(&fenced, vec![conflicting]).await,
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
            &source,
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
            &fenced,
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
            &source,
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
            staging_domain: Arc::clone(&log.staging_domain),
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
        let current = log.put_object(&source, bytes.clone()).await?;
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
        assert_eq!(log.read_object(&cleared, current.reference()).await?, bytes);
        Ok(())
    }

    #[tokio::test]
    async fn resume_rejects_a_different_current_plan_before_delete()
    -> Result<(), Box<dyn std::error::Error>> {
        let faults = FaultStore::new(InMemory::new());
        let backend =
            ValidatedBackend::new(Arc::new(faults.clone()), Path::from("different-plan-tests"))
                .await?;
        let log = Log::open(&backend, &LogId::new("different-plan")?, Options::default()).await?;
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
        let mut head = first.head().clone();
        head.generation = head
            .generation
            .checked_add(1)
            .ok_or("generation overflow")?;
        head.collection_epoch = 2;
        head.active_plan = Some(second_ref);
        let encoded = format::encode_head(&head)?;
        log.store
            .update(StoreKey::Head, encoded, first.storage_version().clone())
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
        let log = Log::open(&backend, &LogId::new("oversized-live")?, options).await?;
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

    #[tokio::test]
    async fn open_existing_never_initializes_a_missing_head()
    -> Result<(), Box<dyn std::error::Error>> {
        let faults = FaultStore::new(InMemory::new());
        let backend =
            ValidatedBackend::new(Arc::new(faults.clone()), Path::from("existing-only")).await?;
        let id = LogId::new("missing")?;
        faults.reset();
        assert!(matches!(
            Log::open_existing(&backend, &id, Options::default()).await,
            Err(Error::InvalidFormat(_))
        ));
        assert_eq!(faults.metrics().operation(Operation::Put).requests, 0);
        assert_eq!(faults.metrics().operation(Operation::Get).requests, 1);
        assert!(
            backend
                .scope(&id)
                .read(StoreKey::Head, Options::default().max_head_bytes)
                .await?
                .is_none()
        );
        Ok(())
    }

    #[tokio::test]
    async fn open_existing_checks_durable_options_and_head_integrity_without_writes()
    -> Result<(), Box<dyn std::error::Error>> {
        let faults = FaultStore::new(InMemory::new());
        let backend =
            ValidatedBackend::new(Arc::new(faults.clone()), Path::from("existing-validation"))
                .await?;
        let id = LogId::new("existing")?;
        let log = Log::open(&backend, &id, Options::default()).await?;
        let changed = Options {
            max_inline_operation_bytes: 1,
            ..Options::default()
        };
        faults.reset();
        assert!(matches!(
            Log::open_existing(&backend, &id, changed).await,
            Err(Error::ConfigurationMismatch("options"))
        ));
        assert_eq!(faults.metrics().operation(Operation::Put).requests, 0);
        let stored = log
            .store
            .read(StoreKey::Head, log.options.max_head_bytes)
            .await?
            .ok_or("head is missing")?;
        log.store
            .update(
                StoreKey::Head,
                Bytes::from_static(b"malformed head"),
                stored.version,
            )
            .await?;
        faults.reset();
        assert!(
            Log::open_existing(&backend, &id, Options::default())
                .await
                .is_err()
        );
        assert_eq!(faults.metrics().operation(Operation::Put).requests, 0);
        Ok(())
    }

    #[tokio::test]
    async fn open_existing_reopens_state_but_revalidates_foreign_staging_proofs()
    -> Result<(), Box<dyn std::error::Error>> {
        let faults = FaultStore::new(InMemory::new());
        let backend =
            ValidatedBackend::new(Arc::new(faults.clone()), Path::from("existing-proofs")).await?;
        let id = LogId::new("existing")?;
        let first = Log::open(&backend, &id, Options::default()).await?;
        let view = first.load().await?;
        let object = first
            .put_object(&view, Bytes::from_static(b"durable bytes"))
            .await?;
        faults.reset();
        let reopened = Log::open_existing(&backend, &id, Options::default()).await?;
        assert_eq!(faults.metrics().operation(Operation::Put).requests, 0);
        assert_eq!(faults.metrics().operation(Operation::Get).requests, 1);
        assert_eq!(reopened.incarnation, first.incarnation);
        assert_eq!(reopened.options(), first.options());
        let view = reopened.load().await?;
        assert!(matches!(
            reopened.prepare(
                &view,
                TransactionId::new(),
                Bytes::new(),
                Bytes::new(),
                vec![object.clone()]
            ),
            Err(Error::InvalidStagedObject)
        ));
        let objects = reopened
            .stage_objects(&view, vec![object.reference().clone()])
            .await?;
        let prepared = reopened.prepare(
            &view,
            TransactionId::new(),
            Bytes::from_static(b"operation"),
            Bytes::new(),
            objects,
        )?;
        assert!(matches!(
            reopened.commit(prepared).await?,
            CommitStatus::Committed(_)
        ));
        let cold = Log::open_existing(&backend, &id, Options::default()).await?;
        let view = cold.load().await?;
        assert_eq!(
            cold.read_tail(&view).await?[0].operation(),
            &Bytes::from_static(b"operation")
        );
        assert_eq!(
            cold.read_object(&view, object.reference()).await?,
            Bytes::from_static(b"durable bytes")
        );
        Ok(())
    }

    async fn staged_read_log(
        id: &str,
        options: Options,
    ) -> Result<(Log, FaultStore, ValidatedBackend), Error> {
        let faults = FaultStore::new(InMemory::new());
        let backend =
            ValidatedBackend::new(Arc::new(faults.clone()), Path::from("staged-node-tests"))
                .await?;
        let log = Log::open(&backend, &LogId::new(id)?, options).await?;
        Ok((log, faults, backend))
    }

    async fn publish_root(log: &Log, view: &View, root: StagedObject) -> Result<View, Error> {
        let prepared = log.prepare(
            view,
            TransactionId::new(),
            Bytes::from_static(&[1]),
            Bytes::new(),
            vec![root],
        )?;
        match log.commit(prepared).await? {
            CommitStatus::Committed(view) => Ok(view),
            _ => Err(Error::InvalidFormat("test root did not publish".into())),
        }
    }

    #[tokio::test]
    async fn staged_node_reads_only_parent_independent_of_subtree_size()
    -> Result<(), Box<dyn std::error::Error>> {
        for count in [1, 64] {
            let (log, faults, _) = staged_read_log("parent-only", Options::default()).await?;
            let view = log.load().await?;
            let mut leaves = Vec::new();
            for _ in 0..count {
                leaves.push(log.put_object(&view, Bytes::from_static(b"leaf")).await?);
            }
            let branch = log
                .put_node(&view, Bytes::from_static(b"branch"), leaves)
                .await?;
            let root = log
                .put_node(&view, Bytes::from_static(b"root"), vec![branch.clone()])
                .await?;
            faults.reset();
            let (payload, children) = log.clone().read_staged_node(&view, &root).await?;
            assert_eq!(payload, Bytes::from_static(b"root"));
            assert_eq!(children.len(), 1);
            assert_eq!(children[0].reference(), branch.reference());
            assert_eq!(faults.metrics().operation(Operation::Get).requests, 1);
            assert_eq!(faults.metrics().operation(Operation::Put).requests, 0);
            log.prepare(
                &view,
                TransactionId::new(),
                Bytes::new(),
                Bytes::new(),
                children,
            )?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn staged_node_rejects_foreign_proofs_and_non_nodes_before_io()
    -> Result<(), Box<dyn std::error::Error>> {
        let (log, faults, backend) = staged_read_log("provenance", Options::default()).await?;
        let view = log.load().await?;
        let root = log.put_node(&view, Bytes::new(), Vec::new()).await?;
        let blob = log.put_object(&view, Bytes::new()).await?;
        let reopened =
            Log::open_existing(&backend, &LogId::new("provenance")?, Options::default()).await?;
        let foreign = Log::open(&backend, &LogId::new("other")?, Options::default())
            .await?
            .load()
            .await?;
        faults.reset();
        assert!(matches!(
            reopened.read_staged_node(&view, &root).await,
            Err(Error::InvalidStagedObject)
        ));
        assert!(matches!(
            log.read_staged_node(&foreign, &root).await,
            Err(Error::InvalidFormat(_))
        ));
        assert!(matches!(
            log.read_staged_node(&view, &blob).await,
            Err(Error::InvalidFormat(_))
        ));
        assert_eq!(faults.metrics().total_requests(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn staged_node_allows_empty_nodes_with_zero_reference_limit()
    -> Result<(), Box<dyn std::error::Error>> {
        let options = Options {
            max_object_refs: 0,
            ..Options::default()
        };
        let (log, faults, _) = staged_read_log("empty", options).await?;
        let view = log.load().await?;
        let root = log
            .put_node(&view, Bytes::from_static(b"empty"), Vec::new())
            .await?;
        faults.reset();
        let (payload, children) = log.read_staged_node(&view, &root).await?;
        assert_eq!(payload, Bytes::from_static(b"empty"));
        assert!(children.is_empty());
        assert_eq!(faults.metrics().operation(Operation::Get).requests, 1);
        Ok(())
    }

    #[tokio::test]
    async fn staged_node_missing_and_corrupt_parent_never_grants_children()
    -> Result<(), Box<dyn std::error::Error>> {
        for failure in ["missing", "corrupt", "expired"] {
            let (log, faults, _) = staged_read_log(failure, Options::default()).await?;
            let view = log.load().await?;
            let blob = log.put_object(&view, Bytes::from_static(b"child")).await?;
            let root = log
                .put_node(&view, Bytes::from_static(b"parent"), vec![blob])
                .await?;
            if failure == "corrupt" {
                let key = log.object_key(root.reference());
                let stored = log
                    .store
                    .read(key, log.options.max_object_bytes)
                    .await?
                    .ok_or("parent is missing")?;
                let mut bytes = stored.bytes.to_vec();
                bytes[0] ^= 1;
                log.store
                    .update(key, Bytes::from(bytes), stored.version)
                    .await?;
            } else if failure == "missing" {
                log.store
                    .delete_immutable_batch(
                        [log.object_immutable_key(root.reference())].into_iter(),
                    )
                    .await?;
            } else {
                let CollectionStart::Installed(fenced, _) = log.start_collection(&view).await?
                else {
                    return Err("collection did not start".into());
                };
                assert!(matches!(
                    log.resume_collection(&fenced).await?,
                    CollectionFinish::Complete(..)
                ));
            }
            faults.reset();
            let result = log.read_staged_node(&view, &root).await;
            if failure == "expired" {
                assert!(matches!(result, Err(Error::ViewExpired)));
            } else {
                assert!(matches!(result, Err(Error::CorruptObject)));
            }
            assert_eq!(
                faults.metrics().operation(Operation::Get).requests,
                if failure == "corrupt" { 1 } else { 2 }
            );
            assert_eq!(faults.metrics().operation(Operation::Put).requests, 0);
        }
        Ok(())
    }

    #[tokio::test]
    async fn staged_node_epoch_fence_accepts_only_fresh_live_parent_proofs()
    -> Result<(), Box<dyn std::error::Error>> {
        let (log, faults, _) = staged_read_log("fenced", Options::default()).await?;
        let view = log.load().await?;
        let blob = log.put_object(&view, Bytes::from_static(b"live")).await?;
        let root = log
            .put_node(&view, Bytes::from_static(b"root"), vec![blob])
            .await?;
        let view = publish_root(&log, &view, root.clone()).await?;
        log.put_object(&view, Bytes::from_static(b"orphan")).await?;
        let CollectionStart::Installed(fenced, _) = log.start_collection(&view).await? else {
            return Err("collection did not start".into());
        };
        faults.reset();
        assert!(matches!(
            log.read_staged_node(&fenced, &root).await,
            Err(Error::InvalidStagedObject)
        ));
        assert_eq!(faults.metrics().total_requests(), 0);
        let state = crate::materialize(&log, fenced, &FoldProbe::default()).await?;
        faults.reset();
        let (_, children) = log
            .read_staged_node(state.view(), &state.state().objects[0])
            .await?;
        assert_eq!(faults.metrics().operation(Operation::Get).requests, 1);
        let replacement = log
            .put_node(state.view(), Bytes::from_static(b"replacement"), children)
            .await?;
        let published = publish_root(&log, state.view(), replacement).await?;
        assert!(matches!(
            log.resume_collection(&published).await?,
            CollectionFinish::Complete(..)
        ));
        Ok(())
    }

    #[tokio::test]
    async fn staged_node_cow_reuses_siblings_through_cold_resolution_and_collection()
    -> Result<(), Box<dyn std::error::Error>> {
        let (log, faults, backend) = staged_read_log("cow", Options::default()).await?;
        let view = log.load().await?;
        let left_blob = log
            .put_object(&view, Bytes::from_static(b"old-left"))
            .await?;
        let left = log
            .put_node(&view, Bytes::new(), vec![left_blob.clone()])
            .await?;
        let right_blob = log.put_object(&view, Bytes::from_static(b"right")).await?;
        let right = log.put_node(&view, Bytes::new(), vec![right_blob]).await?;
        let root = log
            .put_node(
                &view,
                Bytes::from_static(b"root"),
                vec![left.clone(), right.clone()],
            )
            .await?;
        let view = publish_root(&log, &view, root.clone()).await?;
        let (_, mut children) = log.read_staged_node(&view, &root).await?;
        let new_blob = log
            .put_object(&view, Bytes::from_static(b"new-left"))
            .await?;
        children[0] = log.put_node(&view, Bytes::new(), vec![new_blob]).await?;
        let replacement = log
            .put_node(&view, Bytes::from_static(b"replacement"), children)
            .await?;
        let prepared = log.prepare(
            &view,
            TransactionId::new(),
            Bytes::from_static(&[1]),
            Bytes::new(),
            vec![replacement],
        )?;
        let token = prepared.recovery_token()?;
        faults.reset();
        faults.schedule(Failure {
            operation: Operation::Put,
            occurrence: 2,
            phase: FailurePhase::After,
        });
        assert!(matches!(
            log.commit(prepared).await?,
            CommitStatus::Pending(_)
        ));
        let cold = Log::open_existing(&backend, &LogId::new("cow")?, Options::default()).await?;
        assert!(matches!(
            cold.resume(&token).await?,
            Resolution::Committed(_)
        ));
        let state = crate::materialize(&cold, cold.load().await?, &FoldProbe::default()).await?;
        let current_root = state
            .state()
            .objects
            .last()
            .ok_or("root is missing")?
            .clone();
        let through = state.view().tail().last().ok_or("tail is missing")?;
        let CheckpointStatus::Published(view) = cold
            .publish_checkpoint(
                state.view(),
                through,
                Bytes::from_static(&[2]),
                vec![current_root],
            )
            .await?
        else {
            return Err("checkpoint did not publish".into());
        };
        let CollectionStart::Installed(fenced, _) = cold.start_collection(&view).await? else {
            return Err("collection did not start".into());
        };
        let CollectionFinish::Complete(view, report) = cold.resume_collection(&fenced).await?
        else {
            return Err("collection did not finish".into());
        };
        assert!(report.delete_attempts >= 3);
        let state = crate::materialize(&cold, view, &FoldProbe::default()).await?;
        let (_, children) = cold
            .read_staged_node(state.view(), &state.state().objects[0])
            .await?;
        assert_eq!(children[1].reference(), right.reference());
        let (_, right_children) = cold.read_staged_node(state.view(), &children[1]).await?;
        assert_eq!(
            cold.read_object(state.view(), right_children[0].reference())
                .await?,
            Bytes::from_static(b"right")
        );
        for dead in [root.reference(), left.reference(), left_blob.reference()] {
            assert!(
                cold.store
                    .read(cold.object_key(dead), cold.options.max_object_bytes)
                    .await?
                    .is_none()
            );
        }
        Ok(())
    }

    #[derive(Default)]
    struct FoldProbe {
        applied: std::cell::Cell<usize>,
        dropped: std::rc::Rc<std::cell::Cell<bool>>,
        reads: Option<FaultStore>,
        maximum_ahead: std::cell::Cell<usize>,
    }

    struct FoldState {
        total: usize,
        objects: Vec<StagedObject>,
        dropped: std::rc::Rc<std::cell::Cell<bool>>,
    }

    impl Drop for FoldState {
        fn drop(&mut self) {
            self.dropped.set(true);
        }
    }

    impl crate::Materializer for FoldProbe {
        type State = FoldState;
        type Error = std::io::Error;

        fn empty(&self) -> FoldState {
            FoldState {
                total: 0,
                objects: Vec::new(),
                dropped: self.dropped.clone(),
            }
        }

        fn restore(
            &self,
            snapshot: &[u8],
            objects: &[StagedObject],
        ) -> Result<FoldState, Self::Error> {
            let mut state = self.empty();
            state.total = usize::from(snapshot[0]);
            state.objects = objects.to_vec();
            Ok(state)
        }

        fn apply(
            &self,
            state: &mut FoldState,
            operation: &[u8],
            objects: &[StagedObject],
        ) -> Result<(), Self::Error> {
            if let Some(reads) = &self.reads {
                let completed = usize::try_from(reads.metrics().operation(Operation::Get).requests)
                    .map_err(std::io::Error::other)?;
                self.maximum_ahead
                    .set(self.maximum_ahead.get().max(completed - self.applied.get()));
            }
            if operation[0] == 255 {
                return Err(std::io::Error::other("rejected operation"));
            }
            state.total += usize::from(operation[0]);
            state.objects.extend_from_slice(objects);
            self.applied.set(self.applied.get() + 1);
            Ok(())
        }
    }

    async fn fold_append(log: &Log, view: &View, operation: Bytes) -> Result<View, Error> {
        let prepared = log.prepare(
            view,
            TransactionId::new(),
            operation,
            Bytes::new(),
            Vec::new(),
        )?;
        match log.commit(prepared).await? {
            CommitStatus::Committed(view) => Ok(view),
            _ => Err(Error::InvalidFormat("test append did not publish".into())),
        }
    }

    #[tokio::test]
    async fn materialization_read_bound_tracks_largest_lengths_and_checkpoint()
    -> Result<(), Box<dyn std::error::Error>> {
        let (log, faults, _) = staged_read_log("history-bound", Options::default()).await?;
        let mut view = log.load().await?;
        assert_eq!(log.materialization_read_bound(&view)?, 0);
        let window = MAX_CONCURRENT_READS + 1;
        for size in 1..=2 * window {
            view = fold_append(&log, &view, Bytes::from(vec![1; size * 100])).await?;
        }
        assert!(
            view.tail()
                .windows(2)
                .all(|pair| pair[0].len() < pair[1].len())
        );
        let expected: u64 = view.tail()[window..].iter().map(CommitRef::len).sum();
        faults.reset();
        assert_eq!(
            log.materialization_read_bound(&view)?,
            usize::try_from(expected)?
        );
        assert_eq!(faults.metrics().total_requests(), 0);
        let through = view.tail()[window - 1].clone();
        let CheckpointStatus::Published(view) = log
            .publish_checkpoint(
                &view,
                &through,
                Bytes::from(vec![1; 256 * 1024]),
                Vec::new(),
            )
            .await?
        else {
            return Err("checkpoint did not publish".into());
        };
        let checkpoint = view
            .checkpoint()
            .ok_or("checkpoint is missing")?
            .object()
            .len();
        assert!(checkpoint > expected);
        faults.reset();
        assert_eq!(
            log.materialization_read_bound(&view)?,
            usize::try_from(checkpoint)?
        );
        assert_eq!(faults.metrics().total_requests(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn materialization_read_bound_rejects_foreign_views_and_overflow_without_io()
    -> Result<(), Box<dyn std::error::Error>> {
        let (log, faults, backend) = staged_read_log("bound-errors", Options::default()).await?;
        let mut view = log.load().await?;
        for _ in 0..2 {
            view = fold_append(&log, &view, Bytes::from_static(&[1])).await?;
        }
        let foreign = Log::open(&backend, &LogId::new("foreign")?, Options::default())
            .await?
            .load()
            .await?;
        let mut head = view.head().clone();
        // Synthetic authenticated-length extremes exercise checked arithmetic
        // without creating or allocating impossible-sized storage objects.
        head.tail[0].len = u64::MAX;
        head.tail[1].len = 1;
        let overflow = Log::view(head, view.storage_version().clone());
        faults.reset();
        assert!(matches!(
            log.materialization_read_bound(&foreign),
            Err(Error::InvalidFormat(_))
        ));
        assert!(matches!(
            log.materialization_read_bound(&overflow),
            Err(Error::LimitExceeded("materialization encoded bytes"))
        ));
        assert_eq!(faults.metrics().total_requests(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn materialization_bounds_completed_records_ahead_of_application()
    -> Result<(), Box<dyn std::error::Error>> {
        let faults = FaultStore::new(InMemory::new());
        let backend =
            ValidatedBackend::new(Arc::new(faults.clone()), Path::from("bounded-fold")).await?;
        let log = Log::open(&backend, &LogId::new("tail")?, Options::default()).await?;
        let mut view = log.load().await?;
        let count = MAX_CONCURRENT_READS * 3;
        for _ in 0..count {
            view = fold_append(&log, &view, Bytes::from(vec![1; 32 * 1024])).await?;
        }
        let encoded_bound = log.materialization_read_bound(&view)?;
        let maximum_record = view
            .tail()
            .iter()
            .map(CommitRef::len)
            .max()
            .ok_or("tail is empty")?;
        let total_encoded: u64 = view.tail().iter().map(CommitRef::len).sum();
        assert!(u64::try_from(encoded_bound)? < total_encoded);
        faults.reset();
        let probe = FoldProbe {
            reads: Some(faults.clone()),
            ..FoldProbe::default()
        };
        let state = crate::materialize(&log, view, &probe).await?;
        assert_eq!(state.state().total, count);
        assert_eq!(probe.applied.get(), count);
        assert!(
            u64::try_from(encoded_bound)?
                >= u64::try_from(probe.maximum_ahead.get())? * maximum_record
        );
        // The store completes each GET before its record can be decoded. The
        // completed-but-unapplied count therefore bounds retained records.
        assert!(
            probe.maximum_ahead.get() <= MAX_CONCURRENT_READS,
            "{} records completed ahead of application",
            probe.maximum_ahead.get(),
        );
        assert_eq!(
            faults.metrics().operation(Operation::Get).requests,
            u64::try_from(count)?
        );
        faults.reset();
        let through = state.view().tail().last().ok_or("tail is empty")?;
        assert!(matches!(
            log.publish_checkpoint(state.view(), through, Bytes::from_static(&[96]), Vec::new())
                .await?,
            CheckpointStatus::Published(_)
        ));
        assert_eq!(
            faults.metrics().operation(Operation::Get).requests,
            u64::try_from(count)?
        );
        assert_eq!(faults.metrics().operation(Operation::Put).requests, 2);
        Ok(())
    }

    #[tokio::test]
    async fn materialization_classifies_multiple_missing_commits_with_one_head_read()
    -> Result<(), Box<dyn std::error::Error>> {
        let store = Arc::new(ReadProbeStore::default());
        let backend = ValidatedBackend::new(store.clone(), Path::from("missing-tail")).await?;
        let log = Log::open(&backend, &LogId::new("commits")?, Options::default()).await?;
        let mut view = log.load().await?;
        for _ in 0..8 {
            view = fold_append(&log, &view, Bytes::from_static(&[1])).await?;
        }
        log.store
            .delete_immutable_batch(
                view.tail()
                    .iter()
                    .map(|record| log.commit_immutable_key(record)),
            )
            .await?;
        store
            .delay_heads
            .store(true, std::sync::atomic::Ordering::Relaxed);
        let result = crate::materialize(&log, view, &FoldProbe::default()).await;
        assert!(matches!(
            result,
            Err(crate::MaterializeError::Log(Error::CorruptObject))
        ));
        assert_eq!(
            store.head_reads.load(std::sync::atomic::Ordering::Relaxed),
            1
        );
        Ok(())
    }

    #[tokio::test]
    async fn materialization_drops_partial_state_on_late_errors()
    -> Result<(), Box<dyn std::error::Error>> {
        for failure in ["body", "parent", "domain"] {
            let faults = FaultStore::new(InMemory::new());
            let backend =
                ValidatedBackend::new(Arc::new(faults.clone()), Path::from("late-failure")).await?;
            let log = Log::open(&backend, &LogId::new(failure)?, Options::default()).await?;
            let mut view = log.load().await?;
            for position in 0..3 {
                let byte = if failure == "domain" && position == 2 {
                    255
                } else {
                    1
                };
                view = fold_append(&log, &view, Bytes::from(vec![byte])).await?;
            }
            let last = view.tail()[2].clone();
            if failure == "body" {
                let key = log.commit_key(&last);
                let stored = log
                    .store
                    .read(key, log.options.max_commit_bytes)
                    .await?
                    .ok_or("missing test commit")?;
                let mut bytes = stored.bytes.to_vec();
                bytes[0] ^= 1;
                log.store
                    .update(key, Bytes::from(bytes), stored.version)
                    .await?;
            } else if failure == "parent" {
                let stored = log.read_commit(&last).await?;
                let bytes = format::encode_commit(&format::Commit {
                    log_id: log.store.log_id().clone(),
                    incarnation: log.incarnation,
                    transaction_id: last.transaction_id,
                    expected_tip: None,
                    operation: stored.operation,
                    result: stored.result,
                    objects: stored.objects,
                })?;
                let mut replacement = last;
                replacement.digest = Digest::of(&bytes);
                replacement.len = u64::try_from(bytes.len())?;
                log.store
                    .create(log.commit_key(&replacement), bytes)
                    .await?;
                let mut head = view.head().clone();
                head.tail[2] = replacement;
                log.store
                    .update(
                        StoreKey::Head,
                        format::encode_head(&head)?,
                        view.storage_version().clone(),
                    )
                    .await?;
                view = log.load().await?;
            }
            let probe = FoldProbe::default();
            let result = crate::materialize(&log, view.clone(), &probe).await;
            match failure {
                "body" => assert!(matches!(
                    result,
                    Err(crate::MaterializeError::Log(Error::CorruptObject))
                )),
                "parent" => {
                    assert!(matches!(
                        result,
                        Err(crate::MaterializeError::Log(Error::InvalidFormat(_)))
                    ));
                    assert!(matches!(
                        log.read_tail(&view).await,
                        Err(Error::InvalidFormat(_))
                    ));
                }
                _ => assert!(matches!(result, Err(crate::MaterializeError::State(_)))),
            }
            assert_eq!(probe.applied.get(), 2);
            assert!(probe.dropped.get(), "partial state escaped after {failure}");
            if failure != "domain" {
                faults.reset();
                assert!(
                    log.publish_checkpoint(
                        &view,
                        &view.tail()[2],
                        Bytes::from_static(&[3]),
                        Vec::new()
                    )
                    .await
                    .is_err()
                );
                assert_eq!(faults.metrics().operation(Operation::Put).requests, 0);
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn materialization_restores_checkpoint_and_republishes_tail_proofs()
    -> Result<(), Box<dyn std::error::Error>> {
        let log = test_log("fold-checkpoint", Options::default()).await?;
        let view = fold_append(&log, &log.load().await?, Bytes::from_static(&[1])).await?;
        let object = log
            .put_object(&view, Bytes::from_static(b"checkpoint object"))
            .await?;
        let CheckpointStatus::Published(view) = log
            .publish_checkpoint(
                &view,
                &view.tail()[0],
                Bytes::from_static(&[1]),
                vec![object],
            )
            .await?
        else {
            return Err("checkpoint did not publish".into());
        };
        let object = log
            .put_object(&view, Bytes::from_static(b"tail object"))
            .await?;
        let prepared = log.prepare(
            &view,
            TransactionId::new(),
            Bytes::from_static(&[2]),
            Bytes::new(),
            vec![object],
        )?;
        let CommitStatus::Committed(view) = log.commit(prepared).await? else {
            return Err("tail did not publish".into());
        };
        let probe = FoldProbe::default();
        let state = crate::materialize(&log, view.clone(), &probe).await?;
        assert_eq!(state.state().total, 3);
        assert_eq!(probe.applied.get(), 1);
        assert_eq!(state.state().objects.len(), 2);
        assert!(matches!(
            log.publish_checkpoint(
                state.view(),
                &view.tail()[0],
                Bytes::from_static(&[3]),
                state.state().objects.clone(),
            )
            .await?,
            CheckpointStatus::Published(_)
        ));
        Ok(())
    }

    #[tokio::test]
    async fn materialization_returns_expiry_after_history_collection()
    -> Result<(), Box<dyn std::error::Error>> {
        let log = test_log("fold-expiry", Options::default()).await?;
        let old = fold_append(&log, &log.load().await?, Bytes::from_static(&[1])).await?;
        let CheckpointStatus::Published(view) = log
            .publish_checkpoint(&old, &old.tail()[0], Bytes::from_static(&[1]), Vec::new())
            .await?
        else {
            return Err("checkpoint did not publish".into());
        };
        let CollectionStart::Installed(view, _) = log.start_collection(&view).await? else {
            return Err("collection did not start".into());
        };
        assert!(matches!(
            log.resume_collection(&view).await?,
            CollectionFinish::Complete(..)
        ));
        let probe = FoldProbe::default();
        assert!(matches!(
            crate::materialize(&log, old, &probe).await,
            Err(crate::MaterializeError::Log(Error::ViewExpired))
        ));
        assert!(probe.dropped.get());
        Ok(())
    }

    async fn test_log(id: &str, options: Options) -> Result<Log, Error> {
        let backend =
            ValidatedBackend::new(Arc::new(InMemory::new()), Path::from("log-tests")).await?;
        Log::open(&backend, &LogId::new(id)?, options).await
    }

    #[tokio::test]
    async fn authenticated_late_invalid_collection_plan_never_deletes()
    -> Result<(), Box<dyn std::error::Error>> {
        let (log, faults, _) = staged_read_log("late-invalid-plan", Options::default()).await?;
        let source = log.load().await?;
        let orphan = log
            .put_object(&source, Bytes::from_static(b"keep until validated"))
            .await?;
        let plan = CollectionPlan {
            log_id: log.store.log_id().clone(),
            collection_epoch: 1,
            candidates: vec![CollectionCandidate {
                key: log.object_immutable_key(orphan.reference()),
                bytes: orphan.reference().len(),
            }],
        };
        let encoded = format::encode_collection_plan(&plan, log.options)?;
        let mut decoder = minicbor::Decoder::new(&encoded);
        decoder.map()?;
        decoder.u8()?;
        let mut payload = decoder.bytes()?.to_vec();
        payload.push(0); // The complete valid candidate is followed by an invalid trailing item.
        let mut writer = minicbor::Encoder::new(Vec::new());
        writer
            .map(2)?
            .u8(1)?
            .bytes(&payload)?
            .u8(2)?
            .bytes(Digest::of(&payload).as_bytes())?;
        let bytes = Bytes::from(writer.into_writer());
        let reference = super::CollectionPlanRef {
            storage_id: StorageId::new(),
            digest: Digest::of(&bytes),
            len: u64::try_from(bytes.len())?,
        };
        log.store
            .create(
                StoreKey::Immutable(Log::collection_plan_key(log.incarnation, &reference)),
                bytes,
            )
            .await?;
        let mut head = source.head().clone();
        head.generation += 1;
        head.collection_epoch = 1;
        head.active_plan = Some(reference);
        let UpdateResult::Updated { version } = log
            .store
            .update(
                StoreKey::Head,
                format::encode_head(&head)?,
                source.storage_version().clone(),
            )
            .await?
        else {
            return Err("test head publication conflicted".into());
        };
        let fenced = Log::view(head, version);
        faults.reset();
        assert!(matches!(
            log.resume_collection(&fenced).await,
            Err(Error::InvalidFormat(_))
        ));
        assert_eq!(faults.metrics().operation(Operation::Delete).requests, 0);
        assert_eq!(faults.metrics().operation(Operation::Put).requests, 0);
        assert_eq!(
            log.load().await?.storage_version(),
            fenced.storage_version()
        );
        assert!(
            log.store
                .read(
                    log.object_key(orphan.reference()),
                    log.options.max_object_bytes
                )
                .await?
                .is_some()
        );
        Ok(())
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
        let mut head = source.head().clone();
        head.generation = head
            .generation
            .checked_add(1)
            .ok_or(Error::LimitExceeded("head generation"))?;
        head.collection_epoch = epoch;
        head.active_plan = Some(plan_ref);
        let bytes = format::encode_head(&head)?;
        let UpdateResult::Updated { version } = log
            .store
            .update(StoreKey::Head, bytes, source.storage_version().clone())
            .await?
        else {
            return Err(Error::InvalidFormat("test plan lost its CAS".into()));
        };
        Ok(Log::view(head, version))
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
        let mut head = source.head().clone();
        head.generation = 1;
        head.next_sequence = 1;
        head.tail.push(reference);
        let encoded = format::encode_head(&head)?;
        log.store
            .update(StoreKey::Head, encoded, source.storage_version().clone())
            .await?;
        log.load().await
    }
}
