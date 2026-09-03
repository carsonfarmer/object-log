//! Namespace-safe object-store operations.

use std::collections::BTreeSet;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use object_store::path::Path;
use object_store::{GetOptions, ObjectStore, ObjectStoreExt, PutMode, PutOptions, UpdateVersion};
use uuid::Uuid;

use crate::{Digest, Error, LogId};

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

/// One object-store root whose protocol capabilities were verified once.
#[derive(Clone, Debug)]
pub struct ValidatedBackend {
    store: Arc<dyn ObjectStore>,
    root: Path,
    capabilities: BackendCapabilities,
}

impl ValidatedBackend {
    /// Probes and validates one object-store root for many logical logs.
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

    /// Derives an isolated logical-log scope without further storage requests.
    #[must_use]
    pub fn scope(&self, log_id: &LogId) -> ScopedStore {
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
    Commit(Digest),
    Blob(Digest),
    Node(Digest),
    Checkpoint(Digest),
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
pub struct ScopedStore {
    store: Arc<dyn ObjectStore>,
    scope: Path,
    log_id: LogId,
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
        }
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
            StoreKey::Commit(digest) => self
                .scope
                .clone()
                .join("wal")
                .join(format!("{digest}.cbor")),
            StoreKey::Blob(digest) => self.scope.clone().join("objects").join(digest.to_string()),
            StoreKey::Node(digest) => self
                .scope
                .clone()
                .join("nodes")
                .join(format!("{digest}.cbor")),
            StoreKey::Checkpoint(digest) => self
                .scope
                .clone()
                .join("bases")
                .join(format!("{digest}.cbor")),
        }
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
