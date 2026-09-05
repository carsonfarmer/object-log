//! Namespace-safe object-store operations.

use std::collections::BTreeSet;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use futures::stream::{self, BoxStream};
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::{GetOptions, ObjectStore, ObjectStoreExt, PutMode, PutOptions, UpdateVersion};
use uuid::Uuid;

use crate::{Digest, Error, LogId, StorageId};

pub(crate) const MAX_DELETE_BATCH: usize = 1_000;

/// One logical invocation of the underlying object-store client.
///
/// Byte counts are payload bounds, not HTTP headers or wire bytes. Client retries,
/// listing pages and delete batching may perform additional network requests.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum Request {
    /// GET or conditional GET; the core accepts at most this many body bytes.
    Read {
        /// Maximum accepted response body bytes.
        max_bytes: usize,
    },
    /// Conditional PUT of an exact encoded payload.
    Write {
        /// Exact encoded request body bytes.
        bytes: usize,
    },
    /// One listing stream. Its pages and returned metadata have no byte bound here.
    List,
    /// One delete stream containing this many immutable objects.
    Delete {
        /// Number of immutable keys submitted to the stream.
        objects: usize,
    },
}

/// A request was refused before invoking the object-store client.
#[derive(Clone, Copy, Debug, thiserror::Error)]
#[error("object-store request denied")]
pub struct RequestDenied;

/// Operation-local request admission, shared by all clones of a guarded log.
///
/// Implementations must synchronize concurrent admissions atomically. Accepted
/// charges are cumulative: cancellation and failures do not refund them. This
/// synchronous callback must not block or call back into the log. It bounds core
/// client invocations, not retries or pagination hidden inside the backend.
pub trait RequestGuard: std::fmt::Debug + Send + Sync {
    /// Admits one request immediately before the client is invoked.
    ///
    /// # Errors
    /// Return `RequestDenied` to prevent this invocation entirely.
    fn before_request(&self, request: Request) -> Result<(), RequestDenied>;
}

/// One backend behavior required by the publication protocol.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum BackendCapability {
    /// Create-only writes fail when the key already exists.
    ConditionalCreate,
    /// Version-based updates reject a stale storage version.
    ConditionalUpdate,
    /// ETag-based reads distinguish changed and unchanged bytes.
    ConditionalRead,
    /// A successful write is visible to the next read.
    ConsistentReadAfterWrite,
}

/// Behaviors observed by an isolated backend capability probe.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackendCapabilities {
    supported: BTreeSet<BackendCapability>,
}

/// One object-store root whose observable protocol capabilities were verified.
///
/// The probe cannot prove long-term object retention. The operator must prevent
/// lifecycle expiry, overwrite, and deletion of protocol objects outside
/// object-log garbage collection.
#[derive(Clone, Debug)]
pub struct ValidatedBackend {
    store: Arc<dyn ObjectStore>,
    root: Path,
    capabilities: BackendCapabilities,
}

impl ValidatedBackend {
    /// Probes and validates one object-store root for many logical logs.
    ///
    /// The probe checks immediate request behavior. It cannot prove that a
    /// created immutable object will remain unchanged. The caller must provide
    /// that storage contract.
    ///
    /// # Errors
    ///
    /// Returns [`Error::UnsupportedBackend`] for a missing required behavior,
    /// or a storage error when the probe cannot complete.
    pub async fn new(store: Arc<dyn ObjectStore>, root: Path) -> Result<Self, Error> {
        let probe_id = LogId::new(format!("probe-{}", Uuid::new_v4().simple()))?;
        let probe = ScopedStore::unvalidated(Arc::clone(&store), root.clone(), &probe_id);
        let capabilities = probe.probe_capabilities().await?;
        capabilities.require_protocol()?;
        Ok(Self {
            store,
            root,
            capabilities,
        })
    }

    /// Returns the observed backend capabilities.
    #[must_use]
    pub const fn capabilities(&self) -> &BackendCapabilities {
        &self.capabilities
    }

    pub(crate) fn scope(&self, log_id: &LogId) -> ScopedStore {
        ScopedStore::unvalidated(Arc::clone(&self.store), self.root.clone(), log_id)
    }
}

impl BackendCapabilities {
    /// Reports whether the probe observed `capability`.
    #[must_use]
    pub fn supports(&self, capability: BackendCapability) -> bool {
        self.supported.contains(&capability)
    }

    fn require_protocol(&self) -> Result<(), Error> {
        const REQUIRED: [(BackendCapability, &str); 4] = [
            (BackendCapability::ConditionalCreate, "conditional create"),
            (BackendCapability::ConditionalUpdate, "conditional update"),
            (BackendCapability::ConditionalRead, "conditional read"),
            (
                BackendCapability::ConsistentReadAfterWrite,
                "consistent read after write",
            ),
        ];

        for (capability, name) in REQUIRED {
            if !self.supports(capability) {
                return Err(Error::UnsupportedBackend(name));
            }
        }
        Ok(())
    }
}

/// A protocol object in one opened log namespace.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StoreKey {
    Head,
    Immutable(ImmutableKey),
}

/// The role of one immutable physical object.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) enum ImmutableKind {
    Commit,
    Blob,
    Node,
    Checkpoint,
    CollectionPlan,
}

impl ImmutableKind {
    const fn segment(self) -> &'static str {
        match self {
            Self::Commit => "commits",
            Self::Blob => "blobs",
            Self::Node => "nodes",
            Self::Checkpoint => "checkpoints",
            Self::CollectionPlan => "collection-plans",
        }
    }

    fn from_segment(segment: &str) -> Option<Self> {
        match segment {
            "commits" => Some(Self::Commit),
            "blobs" => Some(Self::Blob),
            "nodes" => Some(Self::Node),
            "checkpoints" => Some(Self::Checkpoint),
            "collection-plans" => Some(Self::CollectionPlan),
            _ => None,
        }
    }
}

/// One immutable physical key that cannot address the mutable head.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct ImmutableKey {
    pub(crate) incarnation: Uuid,
    pub(crate) kind: ImmutableKind,
    pub(crate) storage_id: StorageId,
    pub(crate) digest: Digest,
}

impl ImmutableKey {
    #[cfg(test)]
    pub(crate) fn new(incarnation: Uuid, kind: ImmutableKind, digest: Digest) -> Self {
        Self {
            incarnation,
            kind,
            storage_id: StorageId::new(),
            digest,
        }
    }

    pub(crate) const fn from_parts(
        incarnation: Uuid,
        kind: ImmutableKind,
        storage_id: Uuid,
        digest: Digest,
    ) -> Self {
        Self {
            incarnation,
            kind,
            storage_id: StorageId::from_uuid(storage_id),
            digest,
        }
    }
}

/// One emitted entry from a scoped object listing.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ListedObject {
    pub(crate) immutable_key: Option<ImmutableKey>,
    pub(crate) size: u64,
}

/// Bytes and the opaque storage version observed with them.
#[derive(Clone, Debug)]
pub(crate) struct StoredObject {
    pub bytes: Bytes,
    pub version: UpdateVersion,
}

/// The result of a conditional read.
#[derive(Clone, Debug)]
pub(crate) enum ConditionalRead {
    NotModified,
    Modified(StoredObject),
    Missing,
}

/// The result of a create-only write.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum CreateResult {
    Created { version: UpdateVersion },
    AlreadyExists,
}

/// The result of a conditional update.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum UpdateResult {
    Updated { version: UpdateVersion },
    PreconditionFailed,
}

/// A namespace-safe adapter for one logical log.
///
/// Every protocol key is derived from the root prefix and validated [`LogId`].
/// No operation accepts a caller-supplied object path.
#[derive(Clone, Debug)]
pub(crate) struct ScopedStore {
    store: Arc<dyn ObjectStore>,
    scope: Path,
    log_id: LogId,
    guard: Option<Arc<dyn RequestGuard>>,
}

impl ScopedStore {
    fn unvalidated(store: Arc<dyn ObjectStore>, root: Path, log_id: &LogId) -> Self {
        let scope = root
            .join("v1")
            .join("logs")
            .join(log_id.as_str().to_owned());
        Self {
            store,
            scope,
            log_id: log_id.clone(),
            guard: None,
        }
    }

    pub(crate) fn with_request_guard(&self, guard: Arc<dyn RequestGuard>) -> Self {
        Self {
            guard: Some(guard),
            ..self.clone()
        }
    }

    fn admit(&self, request: Request) -> Result<(), Error> {
        self.guard.as_ref().map_or(Ok(()), |guard| {
            guard
                .before_request(request)
                .map_err(|_| Error::RequestDenied)
        })
    }

    /// Returns the validated identity bound to this namespace.
    #[must_use]
    pub(crate) const fn log_id(&self) -> &LogId {
        &self.log_id
    }

    /// Reads one protocol object and its opaque update version.
    ///
    /// # Errors
    ///
    /// Returns a storage error when the backend read or byte stream fails.
    pub(crate) async fn read(
        &self,
        key: StoreKey,
        max_bytes: usize,
    ) -> Result<Option<StoredObject>, Error> {
        let location = self.location(key);
        self.admit(Request::Read { max_bytes })?;
        match self.store.get(&location).await {
            Ok(result) => Ok(Some(collect_object(result, max_bytes).await?)),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    pub(crate) async fn read_integrity(
        &self,
        key: StoreKey,
        max_bytes: usize,
    ) -> Result<Option<(Digest, u64)>, Error> {
        self.admit(Request::Read { max_bytes })?;
        let result = match self.store.get(&self.location(key)).await {
            Ok(result) => result,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let max_bytes = u64::try_from(max_bytes).map_err(|_| Error::LimitExceeded("read bytes"))?;
        if result.meta.size > max_bytes {
            return Err(Error::LimitExceeded("read bytes"));
        }
        let mut digest = blake3::Hasher::new();
        let mut len = 0_u64;
        let mut stream = result.into_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            digest.update(&chunk);
            len = len
                .checked_add(
                    u64::try_from(chunk.len()).map_err(|_| Error::LimitExceeded("read bytes"))?,
                )
                .ok_or(Error::LimitExceeded("read bytes"))?;
            if len > max_bytes {
                return Err(Error::LimitExceeded("read bytes"));
            }
        }
        Ok(Some((Digest(*digest.finalize().as_bytes()), len)))
    }

    /// Reads one protocol object only if its `ETag` changed.
    ///
    /// # Errors
    ///
    /// Returns [`Error::UnsupportedBackend`] when the observed version has no
    /// `ETag`. Returns a storage error for other backend failures.
    pub(crate) async fn read_if_changed(
        &self,
        key: StoreKey,
        observed: &UpdateVersion,
        max_bytes: usize,
    ) -> Result<ConditionalRead, Error> {
        let Some(e_tag) = observed.e_tag.as_ref() else {
            return Err(Error::UnsupportedBackend("conditional read"));
        };
        let options = GetOptions::new().with_if_none_match(Some(e_tag.clone()));
        self.admit(Request::Read { max_bytes })?;
        match self.store.get_opts(&self.location(key), options).await {
            Ok(result) => Ok(ConditionalRead::Modified(
                collect_object(result, max_bytes).await?,
            )),
            Err(object_store::Error::NotModified { .. }) => Ok(ConditionalRead::NotModified),
            Err(object_store::Error::NotFound { .. }) => Ok(ConditionalRead::Missing),
            Err(error) => Err(error.into()),
        }
    }

    /// Creates one protocol object without replacing an existing object.
    ///
    /// # Errors
    ///
    /// Returns a storage error other than a definite already-exists response.
    pub(crate) async fn create(&self, key: StoreKey, bytes: Bytes) -> Result<CreateResult, Error> {
        self.admit(Request::Write { bytes: bytes.len() })?;
        match self
            .store
            .put_opts(
                &self.location(key),
                bytes.into(),
                PutOptions {
                    mode: PutMode::Create,
                    ..PutOptions::default()
                },
            )
            .await
        {
            Ok(result) => Ok(CreateResult::Created {
                version: result.into(),
            }),
            Err(object_store::Error::AlreadyExists { .. }) => Ok(CreateResult::AlreadyExists),
            Err(error) => Err(error.into()),
        }
    }

    /// Replaces one protocol object only at the observed storage version.
    ///
    /// # Errors
    ///
    /// Returns a storage error other than a definite precondition failure.
    pub(crate) async fn update(
        &self,
        key: StoreKey,
        bytes: Bytes,
        observed: UpdateVersion,
    ) -> Result<UpdateResult, Error> {
        self.admit(Request::Write { bytes: bytes.len() })?;
        match self
            .store
            .put_opts(
                &self.location(key),
                bytes.into(),
                PutOptions {
                    mode: PutMode::Update(observed),
                    ..PutOptions::default()
                },
            )
            .await
        {
            Ok(result) => Ok(UpdateResult::Updated {
                version: result.into(),
            }),
            Err(object_store::Error::Precondition { .. }) => Ok(UpdateResult::PreconditionFailed),
            Err(error) => Err(error.into()),
        }
    }

    /// Lists every object in this log scope without exposing its physical path.
    ///
    /// Only exact canonical immutable paths have an [`ImmutableKey`]. Callers
    /// must still count unclassified entries when they enforce scan limits.
    pub(crate) fn list_scoped(&self) -> BoxStream<'static, Result<ListedObject, Error>> {
        let scope = self.scope.clone();
        let guard = self.guard.clone();
        let store = self.clone();
        stream::once(async move {
            store.admit(Request::List)?;
            Ok::<_, Error>(store.store.list(Some(&store.scope)).map_err(Error::from))
        })
        .try_flatten()
        .map(move |result| {
            let _guard = &guard;
            result.map(|metadata| ListedObject {
                immutable_key: classify_immutable(&scope, &metadata.location),
                size: metadata.size,
            })
        })
        .boxed()
    }

    /// Deletes one bounded batch of immutable physical keys.
    ///
    /// The method drains every backend result. A missing key is a successful
    /// delete. Any other error makes the complete batch result uncertain, and
    /// this method does not retry it.
    pub(crate) async fn delete_immutable_batch(
        &self,
        keys: impl ExactSizeIterator<Item = ImmutableKey>,
    ) -> Result<(), Error> {
        let expected_count = keys.len();
        if expected_count > MAX_DELETE_BATCH {
            return Err(Error::LimitExceeded("immutable delete batch"));
        }
        if expected_count == 0 {
            return Ok(());
        }

        let locations = keys
            .map(|key| Ok(self.immutable_location(key)))
            .collect::<Vec<_>>();
        self.admit(Request::Delete {
            objects: expected_count,
        })?;
        let mut results = self.store.delete_stream(stream::iter(locations).boxed());
        let mut first_error = None;
        while let Some(result) = results.next().await {
            if let Err(error) = result
                && !matches!(&error, object_store::Error::NotFound { .. })
                && first_error.is_none()
            {
                first_error = Some(error);
            }
        }
        if let Some(error) = first_error {
            return Err(error.into());
        }
        Ok(())
    }

    /// Probes backend behavior below a fresh private prefix and cleans up its
    /// own object before returning.
    ///
    /// A missing capability is reported in the returned set. An unexpected
    /// backend failure is returned as an error.
    ///
    /// # Errors
    ///
    /// Returns a storage error for a failure that is not a supported negative
    /// capability response.
    async fn probe_capabilities(&self) -> Result<BackendCapabilities, Error> {
        let location = self
            .scope
            .clone()
            .join(".probe")
            .join(Uuid::new_v4().simple().to_string());
        let result = self.run_probe(&location).await;
        let cleanup = self.delete_probe_object(&location).await;

        match (result, cleanup) {
            (Err(error), _) | (Ok(_), Err(error)) => Err(error),
            (Ok(capabilities), Ok(())) => Ok(capabilities),
        }
    }

    fn location(&self, key: StoreKey) -> Path {
        match key {
            StoreKey::Head => self.scope.clone().join("index.cbor"),
            StoreKey::Immutable(key) => self.immutable_location(key),
        }
    }

    fn immutable_location(&self, key: ImmutableKey) -> Path {
        self.scope
            .clone()
            .join("data")
            .join(key.incarnation.simple().to_string())
            .join(key.kind.segment())
            .join(key.storage_id.as_uuid().simple().to_string())
            .join(key.digest.to_string())
    }

    async fn run_probe(&self, location: &Path) -> Result<BackendCapabilities, Error> {
        let mut supported = BTreeSet::new();
        let first_bytes = Bytes::from_static(b"object-log capability probe: first");
        let second_bytes = Bytes::from_static(b"object-log capability probe: second");

        let first_result = self
            .store
            .put_opts(
                location,
                first_bytes.clone().into(),
                PutOptions {
                    mode: PutMode::Create,
                    ..PutOptions::default()
                },
            )
            .await;
        let first_version = match first_result {
            Ok(result) => UpdateVersion::from(result),
            Err(error) if is_unsupported(&error) => {
                return Ok(BackendCapabilities { supported });
            }
            Err(error) => return Err(error.into()),
        };

        match self
            .store
            .put_opts(
                location,
                first_bytes.clone().into(),
                PutOptions {
                    mode: PutMode::Create,
                    ..PutOptions::default()
                },
            )
            .await
        {
            Err(object_store::Error::AlreadyExists { .. }) => {
                supported.insert(BackendCapability::ConditionalCreate);
            }
            Ok(_) => {}
            Err(error) if is_unsupported(&error) => {}
            Err(error) => return Err(error.into()),
        }

        let read = self.store.get(location).await?;
        let read = collect_object(read, first_bytes.len()).await?;
        if read.bytes == first_bytes {
            supported.insert(BackendCapability::ConsistentReadAfterWrite);
        }

        let unchanged_conditional_read = if let Some(e_tag) = first_version.e_tag.as_ref() {
            let options = GetOptions::new().with_if_none_match(Some(e_tag.clone()));
            match self.store.get_opts(location, options).await {
                Err(object_store::Error::NotModified { .. }) => true,
                Ok(_) => false,
                Err(error) if is_unsupported(&error) => false,
                Err(error) => return Err(error.into()),
            }
        } else {
            false
        };

        let update_version = self
            .probe_conditional_update(
                location,
                first_version.clone(),
                first_bytes,
                second_bytes.clone(),
            )
            .await?;
        let current_version = if let Some(version) = update_version {
            supported.insert(BackendCapability::ConditionalUpdate);
            version
        } else {
            self.store
                .put(location, second_bytes.clone().into())
                .await?
                .into()
        };
        let changed_conditional_read = match first_version.e_tag {
            Some(e_tag) => {
                let options = GetOptions::new().with_if_none_match(Some(e_tag));
                match self.store.get_opts(location, options).await {
                    Ok(result) => {
                        let result = collect_object(result, second_bytes.len()).await?;
                        result.bytes == second_bytes && result.version == current_version
                    }
                    Err(object_store::Error::NotModified { .. }) => false,
                    Err(error) if is_unsupported(&error) => false,
                    Err(error) => return Err(error.into()),
                }
            }
            None => false,
        };
        if unchanged_conditional_read && changed_conditional_read {
            supported.insert(BackendCapability::ConditionalRead);
        }

        Ok(BackendCapabilities { supported })
    }

    async fn probe_conditional_update(
        &self,
        location: &Path,
        first_version: UpdateVersion,
        first_bytes: Bytes,
        second_bytes: Bytes,
    ) -> Result<Option<UpdateVersion>, Error> {
        let update = self
            .store
            .put_opts(
                location,
                second_bytes.clone().into(),
                PutOptions {
                    mode: PutMode::Update(first_version.clone()),
                    ..PutOptions::default()
                },
            )
            .await;
        let update_version = match update {
            Ok(result) => UpdateVersion::from(result),
            Err(error) if is_unsupported(&error) => return Ok(None),
            Err(object_store::Error::Precondition { .. }) => return Ok(None),
            Err(error) => return Err(error.into()),
        };

        match self
            .store
            .put_opts(
                location,
                first_bytes.into(),
                PutOptions {
                    mode: PutMode::Update(first_version),
                    ..PutOptions::default()
                },
            )
            .await
        {
            Err(object_store::Error::Precondition { .. }) => {
                let current =
                    collect_object(self.store.get(location).await?, second_bytes.len()).await?;
                if current.bytes != second_bytes || current.version != update_version {
                    return Ok(None);
                }
                Ok(Some(update_version))
            }
            Ok(_) => Ok(None),
            Err(error) if is_unsupported(&error) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    async fn delete_probe_object(&self, location: &Path) -> Result<(), Error> {
        match self.store.delete(location).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(error) => Err(error.into()),
        }
    }
}

fn classify_immutable(scope: &Path, location: &Path) -> Option<ImmutableKey> {
    let mut parts = location.prefix_match(scope)?;
    if parts.next()?.as_ref() != "data" {
        return None;
    }
    let incarnation = parse_simple_uuid(parts.next()?.as_ref())?;
    let kind = ImmutableKind::from_segment(parts.next()?.as_ref())?;
    let storage_id = parse_simple_uuid(parts.next()?.as_ref())?;
    let digest_text = parts.next()?;
    if parts.next().is_some() {
        return None;
    }
    let digest = digest_text.as_ref().parse::<Digest>().ok()?;
    (digest.to_string() == digest_text.as_ref()).then_some(ImmutableKey::from_parts(
        incarnation,
        kind,
        storage_id,
        digest,
    ))
}

fn parse_simple_uuid(value: &str) -> Option<Uuid> {
    let uuid = Uuid::parse_str(value).ok()?;
    (uuid.simple().to_string() == value).then_some(uuid)
}

async fn collect_object(
    result: object_store::GetResult,
    max_bytes: usize,
) -> Result<StoredObject, Error> {
    let version = UpdateVersion {
        e_tag: result.meta.e_tag.clone(),
        version: result.meta.version.clone(),
    };
    let max_bytes_u64 = u64::try_from(max_bytes).map_err(|_| Error::LimitExceeded("read bytes"))?;
    if result.meta.size > max_bytes_u64 {
        return Err(Error::LimitExceeded("read bytes"));
    }
    let mut bytes = BytesMut::with_capacity(
        usize::try_from(result.meta.size).map_err(|_| Error::LimitExceeded("read bytes"))?,
    );
    let mut stream = result.into_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        let next_len = bytes
            .len()
            .checked_add(chunk.len())
            .ok_or(Error::LimitExceeded("read bytes"))?;
        if next_len > max_bytes {
            return Err(Error::LimitExceeded("read bytes"));
        }
        bytes.extend_from_slice(&chunk);
    }
    let bytes = bytes.freeze();
    Ok(StoredObject { bytes, version })
}

fn is_unsupported(error: &object_store::Error) -> bool {
    matches!(
        error,
        object_store::Error::NotSupported { .. } | object_store::Error::NotImplemented { .. }
    )
}

#[cfg(test)]
mod tests {
    use std::error::Error as StdError;

    use futures::TryStreamExt;
    use object_store::local::LocalFileSystem;
    use object_store::memory::InMemory;
    use tempfile::TempDir;

    use super::*;
    use crate::sim::{Failure, FailurePhase, FaultStore, Operation};

    type TestResult = Result<(), Box<dyn StdError>>;

    #[tokio::test]
    async fn request_guard_filesystem_refusals_never_reach_backend() -> TestResult {
        #[derive(Debug)]
        struct Deny;
        impl RequestGuard for Deny {
            fn before_request(&self, _: Request) -> Result<(), RequestDenied> {
                Err(RequestDenied)
            }
        }
        let directory = TempDir::new()?;
        let faults = FaultStore::new(LocalFileSystem::new_with_prefix(directory.path())?);
        let scoped = test_scope(Arc::new(faults.clone()), "guard-filesystem")?;
        let key = ImmutableKey::new(Uuid::new_v4(), ImmutableKind::Blob, Digest::of(b"data"));
        scoped
            .create(StoreKey::Immutable(key), Bytes::from_static(b"data"))
            .await?;
        let denied = scoped.with_request_guard(Arc::new(Deny));
        faults.reset();
        assert!(matches!(
            denied.read(StoreKey::Immutable(key), 4).await,
            Err(Error::RequestDenied)
        ));
        assert!(matches!(
            denied.create(StoreKey::Immutable(key), Bytes::new()).await,
            Err(Error::RequestDenied)
        ));
        assert!(matches!(
            denied.list_scoped().try_collect::<Vec<_>>().await,
            Err(Error::RequestDenied)
        ));
        assert!(matches!(
            denied.delete_immutable_batch(std::iter::once(key)).await,
            Err(Error::RequestDenied)
        ));
        assert_eq!(faults.metrics().total_requests(), 0);
        assert_eq!(
            scoped
                .read(StoreKey::Immutable(key), 4)
                .await?
                .ok_or("missing original")?
                .bytes,
            Bytes::from_static(b"data")
        );
        Ok(())
    }

    #[test]
    fn new_immutable_keys_get_distinct_physical_ids() {
        let incarnation = Uuid::new_v4();
        let digest = Digest::of(b"same content");
        let first = ImmutableKey::new(incarnation, ImmutableKind::Blob, digest);
        let second = ImmutableKey::new(incarnation, ImmutableKind::Blob, digest);

        assert_eq!(first.incarnation, second.incarnation);
        assert_eq!(first.digest, second.digest);
        assert_ne!(first.storage_id, second.storage_id);
    }

    #[tokio::test]
    async fn scoped_listing_classifies_only_canonical_immutable_paths() -> TestResult {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let scoped = test_scope(Arc::clone(&store), "tenant")?;
        let incarnation = Uuid::parse_str("00112233-4455-4677-8899-aabbccddeeff")?;
        let digest = Digest::of(b"classified");
        let kinds = [
            ImmutableKind::Commit,
            ImmutableKind::Blob,
            ImmutableKind::Node,
            ImmutableKind::Checkpoint,
            ImmutableKind::CollectionPlan,
        ];
        let mut expected = BTreeSet::new();
        for (index, kind) in kinds.into_iter().enumerate() {
            let key = ImmutableKey::from_parts(
                incarnation,
                kind,
                Uuid::from_u128(u128::try_from(index)?.saturating_add(1)),
                digest,
            );
            put_raw(&store, scoped.immutable_location(key), index + 1).await?;
            expected.insert(key);
        }

        let malformed = [
            scoped.scope.clone().join("wal").join(digest.to_string()),
            scoped
                .scope
                .clone()
                .join("data")
                .join(incarnation.to_string())
                .join("blobs")
                .join(Uuid::new_v4().simple().to_string())
                .join(digest.to_string()),
            scoped
                .scope
                .clone()
                .join("data")
                .join(incarnation.simple().to_string())
                .join("unknown")
                .join(Uuid::new_v4().simple().to_string())
                .join(digest.to_string()),
            scoped
                .scope
                .clone()
                .join("data")
                .join(incarnation.simple().to_string())
                .join("blobs")
                .join(Uuid::new_v4().simple().to_string())
                .join(digest.to_string().to_uppercase()),
            scoped
                .scope
                .clone()
                .join("data")
                .join(incarnation.simple().to_string())
                .join("blobs")
                .join(Uuid::new_v4().simple().to_string())
                .join(digest.to_string())
                .join("extra"),
        ];
        for location in malformed {
            put_raw(&store, location, 7).await?;
        }

        let listed = scoped.list_scoped().try_collect::<Vec<_>>().await?;
        assert_eq!(listed.len(), 10);
        assert_eq!(listed.iter().map(|item| item.size).sum::<u64>(), 50);
        assert_eq!(
            listed
                .iter()
                .filter_map(|item| item.immutable_key)
                .collect::<BTreeSet<_>>(),
            expected
        );
        assert_eq!(
            listed
                .iter()
                .filter(|item| item.immutable_key.is_none())
                .count(),
            5
        );
        Ok(())
    }

    #[tokio::test]
    async fn scoped_listing_does_not_cross_log_segments() -> TestResult {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let first = test_scope(Arc::clone(&store), "tenant")?;
        let second = test_scope(Arc::clone(&store), "tenant-other")?;
        let digest = Digest::of(b"tenant isolation");
        let incarnation = Uuid::new_v4();
        let first_key = ImmutableKey::new(incarnation, ImmutableKind::Blob, digest);
        let second_key = ImmutableKey::new(incarnation, ImmutableKind::Blob, digest);
        put_raw(&store, first.immutable_location(first_key), 1).await?;
        put_raw(&store, second.immutable_location(second_key), 2).await?;

        let listed = first.list_scoped().try_collect::<Vec<_>>().await?;
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].immutable_key, Some(first_key));
        assert_eq!(listed[0].size, 1);
        Ok(())
    }

    #[tokio::test]
    async fn delete_batch_rejects_excess_before_io() -> TestResult {
        let faults = FaultStore::new(InMemory::new());
        let scoped = test_scope(Arc::new(faults.clone()), "limits")?;
        let incarnation = Uuid::new_v4();
        let digest = Digest::of(b"delete limits");
        let excessive = (0_u128..1_001)
            .map(|storage_id| {
                ImmutableKey::from_parts(
                    incarnation,
                    ImmutableKind::Blob,
                    Uuid::from_u128(storage_id),
                    digest,
                )
            })
            .collect::<Vec<_>>();
        assert!(matches!(
            scoped.delete_immutable_batch(excessive.into_iter()).await,
            Err(Error::LimitExceeded("immutable delete batch"))
        ));
        assert_eq!(faults.metrics().operation(Operation::Delete).requests, 0);
        Ok(())
    }

    #[tokio::test]
    async fn delete_batch_accepts_exact_limit() -> TestResult {
        let faults = FaultStore::new(InMemory::new());
        let scoped = test_scope(Arc::new(faults.clone()), "limit-boundary")?;
        let incarnation = Uuid::new_v4();
        let digest = Digest::of(b"delete boundary");
        let keys = (0_u128..1_000)
            .map(|storage_id| {
                ImmutableKey::from_parts(
                    incarnation,
                    ImmutableKind::Blob,
                    Uuid::from_u128(storage_id),
                    digest,
                )
            })
            .collect::<Vec<_>>();

        scoped.delete_immutable_batch(keys.into_iter()).await?;
        assert_eq!(
            faults.metrics().operation(Operation::Delete).requests,
            1_000
        );
        Ok(())
    }

    #[tokio::test]
    async fn immutable_delete_is_repeatable_in_memory_and_filesystem() -> TestResult {
        repeatable_delete(Arc::new(InMemory::new()), "memory").await?;

        let directory = TempDir::new()?;
        let filesystem = LocalFileSystem::new_with_prefix(directory.path())?;
        repeatable_delete(Arc::new(filesystem), "filesystem").await
    }

    #[tokio::test]
    async fn delete_batch_drains_partial_failures_before_retry() -> TestResult {
        for phase in [FailurePhase::Before, FailurePhase::After] {
            let faults = FaultStore::new(InMemory::new());
            let store: Arc<dyn ObjectStore> = Arc::new(faults.clone());
            let scoped = test_scope(Arc::clone(&store), "partial")?;
            let incarnation = Uuid::new_v4();
            let keys = (1_u128..=3)
                .map(|storage_id| {
                    ImmutableKey::from_parts(
                        incarnation,
                        ImmutableKind::Blob,
                        Uuid::from_u128(storage_id),
                        Digest::of(&storage_id.to_be_bytes()),
                    )
                })
                .collect::<Vec<_>>();
            for key in &keys {
                put_raw(&store, scoped.immutable_location(*key), 1).await?;
            }
            faults.reset();
            faults.schedule(Failure {
                operation: Operation::Delete,
                occurrence: 2,
                phase,
            });

            let error = scoped
                .delete_immutable_batch(keys.iter().copied())
                .await
                .err()
                .ok_or("a partial delete returned success")?;
            assert!(matches!(
                &error,
                Error::Store(error) if FaultStore::is_injected(error)
            ));
            assert_eq!(faults.metrics().operation(Operation::Delete).requests, 3);
            assert!(is_missing(&store, &scoped.immutable_location(keys[0])).await);
            assert_eq!(
                is_missing(&store, &scoped.immutable_location(keys[1])).await,
                phase == FailurePhase::After
            );
            assert!(is_missing(&store, &scoped.immutable_location(keys[2])).await);

            scoped.delete_immutable_batch(keys.iter().copied()).await?;
            for key in keys {
                assert!(is_missing(&store, &scoped.immutable_location(key)).await);
            }
        }
        Ok(())
    }

    fn test_scope(store: Arc<dyn ObjectStore>, log_id: &str) -> Result<ScopedStore, Error> {
        Ok(ScopedStore::unvalidated(
            store,
            Path::from("gc-tests"),
            &LogId::new(log_id)?,
        ))
    }

    async fn put_raw(
        store: &Arc<dyn ObjectStore>,
        location: Path,
        size: usize,
    ) -> Result<(), object_store::Error> {
        store
            .put(&location, Bytes::from(vec![0_u8; size]).into())
            .await?;
        Ok(())
    }

    async fn is_missing(store: &Arc<dyn ObjectStore>, location: &Path) -> bool {
        matches!(
            store.get(location).await,
            Err(object_store::Error::NotFound { .. })
        )
    }

    async fn repeatable_delete(store: Arc<dyn ObjectStore>, log_id: &str) -> TestResult {
        let scoped = test_scope(Arc::clone(&store), log_id)?;
        let key = ImmutableKey::new(
            Uuid::new_v4(),
            ImmutableKind::Checkpoint,
            Digest::of(log_id.as_bytes()),
        );
        let location = scoped.immutable_location(key);
        put_raw(&store, location.clone(), 4).await?;

        scoped.delete_immutable_batch(std::iter::once(key)).await?;
        assert!(is_missing(&store, &location).await);
        scoped.delete_immutable_batch(std::iter::once(key)).await?;
        assert!(is_missing(&store, &location).await);
        Ok(())
    }
}
