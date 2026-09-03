//! Namespace-safe object-store operations.

use std::collections::BTreeSet;
use std::sync::Arc;

use bytes::Bytes;
use futures::TryStreamExt;
use object_store::path::Path;
use object_store::{
    GetOptions, ObjectMeta, ObjectStore, ObjectStoreExt, PutMode, PutOptions, UpdateVersion,
};
use uuid::Uuid;

use crate::{Digest, Error, LogId};

/// One backend behavior required by the publication protocol.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum BackendCapability {
    ConditionalCreate,
    ConditionalUpdate,
    ConditionalRead,
    ConsistentReadAfterWrite,
    ConsistentList,
}

/// Behaviors observed by an isolated backend capability probe.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackendCapabilities {
    pub supported: BTreeSet<BackendCapability>,
}

impl BackendCapabilities {
    /// Reports whether the probe observed `capability`.
    #[must_use]
    pub fn supports(&self, capability: BackendCapability) -> bool {
        self.supported.contains(&capability)
    }

    /// Verifies every behavior required by the object-log protocol.
    ///
    /// # Errors
    ///
    /// Returns [`Error::UnsupportedBackend`] for the first missing behavior.
    pub fn require_protocol(&self) -> Result<(), Error> {
        const REQUIRED: [(BackendCapability, &str); 5] = [
            (BackendCapability::ConditionalCreate, "conditional create"),
            (BackendCapability::ConditionalUpdate, "conditional update"),
            (BackendCapability::ConditionalRead, "conditional read"),
            (
                BackendCapability::ConsistentReadAfterWrite,
                "consistent read after write",
            ),
            (BackendCapability::ConsistentList, "consistent list"),
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
pub enum StoreKey {
    Head,
    Commit(Digest),
    Blob(Digest),
    Checkpoint(Digest),
}

/// One immutable object collection in an opened log namespace.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StoreCollection {
    Commits,
    Blobs,
    Checkpoints,
}

/// Bytes and the opaque storage version observed with them.
#[derive(Clone, Debug)]
pub struct StoredObject {
    pub bytes: Bytes,
    pub version: UpdateVersion,
}

/// The result of a conditional read.
#[derive(Clone, Debug)]
pub enum ConditionalRead {
    NotModified,
    Modified(StoredObject),
    Missing,
}

/// The result of a create-only write.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CreateResult {
    Created { version: UpdateVersion },
    AlreadyExists,
}

/// The result of a conditional update.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum UpdateResult {
    Updated { version: UpdateVersion },
    PreconditionFailed,
}

/// Metadata for one object returned by a scoped collection listing.
#[derive(Clone, Debug)]
pub struct ListedObject {
    pub key: StoreKey,
    pub len: u64,
    pub version: UpdateVersion,
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
    /// Opens an isolated namespace without accessing the backend.
    #[must_use]
    pub fn new(store: Arc<dyn ObjectStore>, root: Path, log_id: &LogId) -> Self {
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
    pub const fn log_id(&self) -> &LogId {
        &self.log_id
    }

    /// Reads one protocol object and its opaque update version.
    ///
    /// # Errors
    ///
    /// Returns a storage error when the backend read or byte stream fails.
    pub async fn read(&self, key: StoreKey) -> Result<Option<StoredObject>, Error> {
        let location = self.location(key);
        match self.store.get(&location).await {
            Ok(result) => Ok(Some(collect_object(result).await?)),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    /// Reads one protocol object only if its `ETag` changed.
    ///
    /// # Errors
    ///
    /// Returns [`Error::UnsupportedBackend`] when the observed version has no
    /// `ETag`. Returns a storage error for other backend failures.
    pub async fn read_if_changed(
        &self,
        key: StoreKey,
        observed: &UpdateVersion,
    ) -> Result<ConditionalRead, Error> {
        let Some(e_tag) = observed.e_tag.as_ref() else {
            return Err(Error::UnsupportedBackend("conditional read"));
        };
        let options = GetOptions::new().with_if_none_match(Some(e_tag.clone()));
        match self.store.get_opts(&self.location(key), options).await {
            Ok(result) => Ok(ConditionalRead::Modified(collect_object(result).await?)),
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
    pub async fn create(&self, key: StoreKey, bytes: Bytes) -> Result<CreateResult, Error> {
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
    pub async fn update(
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

    /// Lists one immutable collection inside this log namespace.
    ///
    /// # Errors
    ///
    /// Returns a storage or format error if listing fails or an object name is
    /// not a digest.
    pub async fn list(&self, collection: StoreCollection) -> Result<Vec<ListedObject>, Error> {
        let prefix = self.collection_prefix(collection);
        let metadata = self
            .store
            .list(Some(&prefix))
            .try_collect::<Vec<_>>()
            .await?;
        metadata
            .into_iter()
            .map(|meta| listed_object(collection, meta))
            .collect()
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
    pub async fn probe_capabilities(&self) -> Result<BackendCapabilities, Error> {
        let probe_prefix = self
            .scope
            .clone()
            .join(".probe")
            .join(Uuid::new_v4().simple().to_string());
        let location = probe_prefix.clone().join("object");
        let result = self.run_probe(&probe_prefix, &location).await;
        let cleanup = self.delete_probe_object(&location).await;

        match (result, cleanup) {
            (Err(error), _) | (Ok(_), Err(error)) => Err(error),
            (Ok(capabilities), Ok(())) => Ok(capabilities),
        }
    }

    /// Probes and rejects a backend that cannot safely host a writable log.
    ///
    /// # Errors
    ///
    /// Returns [`Error::UnsupportedBackend`] for a missing required behavior,
    /// or a storage error when the probe cannot complete.
    pub async fn validate_backend(&self) -> Result<BackendCapabilities, Error> {
        let capabilities = self.probe_capabilities().await?;
        capabilities.require_protocol()?;
        Ok(capabilities)
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
            StoreKey::Checkpoint(digest) => self
                .scope
                .clone()
                .join("bases")
                .join(format!("{digest}.cbor")),
        }
    }

    fn collection_prefix(&self, collection: StoreCollection) -> Path {
        self.scope.clone().join(match collection {
            StoreCollection::Commits => "wal",
            StoreCollection::Blobs => "objects",
            StoreCollection::Checkpoints => "bases",
        })
    }

    async fn run_probe(
        &self,
        probe_prefix: &Path,
        location: &Path,
    ) -> Result<BackendCapabilities, Error> {
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
        let read = collect_object(read).await?;
        if read.bytes == first_bytes {
            supported.insert(BackendCapability::ConsistentReadAfterWrite);
        }

        if let Some(e_tag) = first_version.e_tag.as_ref() {
            let options = GetOptions::new().with_if_none_match(Some(e_tag.clone()));
            match self.store.get_opts(location, options).await {
                Err(object_store::Error::NotModified { .. }) => {
                    supported.insert(BackendCapability::ConditionalRead);
                }
                Ok(_) => {}
                Err(error) if is_unsupported(&error) => {}
                Err(error) => return Err(error.into()),
            }
        }

        let listed = self
            .store
            .list(Some(probe_prefix))
            .try_collect::<Vec<_>>()
            .await?;
        if listed.iter().any(|meta| meta.location == *location) {
            supported.insert(BackendCapability::ConsistentList);
        }

        if self
            .probe_conditional_update(location, first_version, first_bytes, second_bytes)
            .await?
        {
            supported.insert(BackendCapability::ConditionalUpdate);
        }

        Ok(BackendCapabilities { supported })
    }

    async fn probe_conditional_update(
        &self,
        location: &Path,
        first_version: UpdateVersion,
        first_bytes: Bytes,
        second_bytes: Bytes,
    ) -> Result<bool, Error> {
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
            Err(error) if is_unsupported(&error) => return Ok(false),
            Err(object_store::Error::Precondition { .. }) => return Ok(false),
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
                let current = collect_object(self.store.get(location).await?).await?;
                Ok(current.bytes == second_bytes && current.version == update_version)
            }
            Ok(_) => Ok(false),
            Err(error) if is_unsupported(&error) => Ok(false),
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

async fn collect_object(result: object_store::GetResult) -> Result<StoredObject, Error> {
    let version = UpdateVersion {
        e_tag: result.meta.e_tag.clone(),
        version: result.meta.version.clone(),
    };
    let bytes = result.bytes().await?;
    Ok(StoredObject { bytes, version })
}

fn listed_object(collection: StoreCollection, meta: ObjectMeta) -> Result<ListedObject, Error> {
    let name = meta
        .location
        .filename()
        .ok_or_else(|| Error::InvalidFormat("listed object has no file name".to_owned()))?;
    let name = match collection {
        StoreCollection::Commits | StoreCollection::Checkpoints => {
            name.strip_suffix(".cbor").ok_or_else(|| {
                Error::InvalidFormat("metadata object has no .cbor suffix".to_owned())
            })?
        }
        StoreCollection::Blobs => name,
    };
    let digest = name.parse()?;
    let key = match collection {
        StoreCollection::Commits => StoreKey::Commit(digest),
        StoreCollection::Blobs => StoreKey::Blob(digest),
        StoreCollection::Checkpoints => StoreKey::Checkpoint(digest),
    };
    Ok(ListedObject {
        key,
        len: meta.size,
        version: UpdateVersion {
            e_tag: meta.e_tag,
            version: meta.version,
        },
    })
}

fn is_unsupported(error: &object_store::Error) -> bool {
    matches!(
        error,
        object_store::Error::NotSupported { .. } | object_store::Error::NotImplemented { .. }
    )
}
