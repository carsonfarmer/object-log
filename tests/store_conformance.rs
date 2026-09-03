use std::error::Error as StdError;
use std::sync::Arc;

use bytes::Bytes;
use futures::TryStreamExt;
use object_log::store::{
    BackendCapability, ConditionalRead, CreateResult, StoreCollection, StoreKey, UpdateResult,
};
use object_log::{Digest, Error, LogId, ScopedStore};
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, UpdateVersion};
use tempfile::TempDir;

#[derive(Clone, Copy)]
enum UpdateSupport {
    Required,
    Unsupported,
}

#[tokio::test]
async fn memory_backend_conforms() -> Result<(), Box<dyn StdError>> {
    run_conformance(Arc::new(InMemory::new()), UpdateSupport::Required).await
}

#[tokio::test]
async fn filesystem_backend_reports_missing_update() -> Result<(), Box<dyn StdError>> {
    let directory = TempDir::new()?;
    let backend = LocalFileSystem::new_with_prefix(directory.path())?;
    run_conformance(Arc::new(backend), UpdateSupport::Unsupported).await
}

#[tokio::test]
async fn log_namespaces_are_isolated() -> Result<(), Box<dyn StdError>> {
    let backend: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let root = Path::from("shared-root");
    let first = ScopedStore::new(Arc::clone(&backend), root.clone(), &LogId::new("tenant-a")?);
    let second = ScopedStore::new(backend, root, &LogId::new("tenant-b")?);
    let digest = Digest::of(b"same logical object");

    assert!(matches!(
        first
            .create(StoreKey::Blob(digest), Bytes::from_static(b"first"))
            .await?,
        CreateResult::Created { .. }
    ));
    assert!(matches!(
        second
            .create(StoreKey::Blob(digest), Bytes::from_static(b"second"))
            .await?,
        CreateResult::Created { .. }
    ));

    let first_read = first.read(StoreKey::Blob(digest)).await?;
    let second_read = second.read(StoreKey::Blob(digest)).await?;
    assert_eq!(
        first_read.map(|object| object.bytes),
        Some(Bytes::from("first"))
    );
    assert_eq!(
        second_read.map(|object| object.bytes),
        Some(Bytes::from("second"))
    );
    assert_eq!(first.list(StoreCollection::Blobs).await?.len(), 1);
    assert_eq!(second.list(StoreCollection::Blobs).await?.len(), 1);
    Ok(())
}

#[tokio::test]
async fn probe_removes_only_its_object() -> Result<(), Box<dyn StdError>> {
    let backend: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let sentinel = Path::from("unrelated/sentinel");
    backend
        .put(&sentinel, Bytes::from_static(b"keep").into())
        .await?;
    let before = backend.list(None).try_collect::<Vec<_>>().await?;
    let scoped = ScopedStore::new(
        Arc::clone(&backend),
        Path::from("root"),
        &LogId::new("probe-cleanup")?,
    );

    let capabilities = scoped.probe_capabilities().await?;
    assert!(capabilities.supports(BackendCapability::ConditionalUpdate));

    let after = backend.list(None).try_collect::<Vec<_>>().await?;
    assert_eq!(before.len(), after.len());
    assert!(after.iter().any(|meta| meta.location == sentinel));
    Ok(())
}

async fn run_conformance(
    backend: Arc<dyn ObjectStore>,
    update_support: UpdateSupport,
) -> Result<(), Box<dyn StdError>> {
    let scoped = ScopedStore::new(backend, Path::from("conformance"), &LogId::new("test-log")?);
    let capabilities = scoped.probe_capabilities().await?;

    assert!(capabilities.supports(BackendCapability::ConditionalCreate));
    assert!(capabilities.supports(BackendCapability::ConditionalRead));
    assert!(capabilities.supports(BackendCapability::ConsistentReadAfterWrite));
    assert!(capabilities.supports(BackendCapability::ConsistentList));

    match update_support {
        UpdateSupport::Required => {
            assert!(capabilities.supports(BackendCapability::ConditionalUpdate));
            scoped.validate_backend().await?;
        }
        UpdateSupport::Unsupported => {
            assert!(!capabilities.supports(BackendCapability::ConditionalUpdate));
            assert!(matches!(
                scoped.validate_backend().await,
                Err(Error::UnsupportedBackend("conditional update"))
            ));
        }
    }

    let head_key = StoreKey::Head;
    let initial = Bytes::from_static(b"head generation one");
    let created = scoped.create(head_key, initial.clone()).await?;
    let first_version = match created {
        CreateResult::Created { version } => version,
        CreateResult::AlreadyExists => return Err("fresh head already existed".into()),
    };
    assert_eq!(
        scoped
            .create(head_key, Bytes::from_static(b"replacement"))
            .await?,
        CreateResult::AlreadyExists
    );

    let read = scoped.read(head_key).await?;
    let read = read.ok_or("created head was missing")?;
    assert_eq!(read.bytes, initial);
    assert_eq!(read.version, first_version);
    assert!(matches!(
        scoped.read_if_changed(head_key, &first_version).await?,
        ConditionalRead::NotModified
    ));

    let missing_digest = Digest::of(b"missing");
    assert!(scoped.read(StoreKey::Blob(missing_digest)).await?.is_none());

    exercise_update(&scoped, update_support, head_key, initial, first_version).await?;

    let first_blob = Digest::of(b"first blob");
    let second_blob = Digest::of(b"second blob");
    let first_bytes = Bytes::from(vec![0_u8, 1, 2, 0, 255]);
    let second_bytes = Bytes::from_static(b"another object");
    scoped
        .create(StoreKey::Blob(first_blob), first_bytes.clone())
        .await?;
    scoped
        .create(StoreKey::Blob(second_blob), second_bytes)
        .await?;

    let listed = scoped.list(StoreCollection::Blobs).await?;
    assert_eq!(listed.len(), 2);
    assert!(
        listed
            .iter()
            .any(|object| object.key == StoreKey::Blob(first_blob))
    );
    assert!(
        listed
            .iter()
            .any(|object| object.key == StoreKey::Blob(second_blob))
    );
    assert_eq!(
        scoped
            .read(StoreKey::Blob(first_blob))
            .await?
            .map(|object| object.bytes),
        Some(first_bytes)
    );
    Ok(())
}

async fn exercise_update(
    scoped: &ScopedStore,
    update_support: UpdateSupport,
    head_key: StoreKey,
    initial: Bytes,
    first_version: UpdateVersion,
) -> Result<(), Box<dyn StdError>> {
    match update_support {
        UpdateSupport::Required => {
            let updated_bytes = Bytes::from_static(b"head generation two");
            let updated = scoped
                .update(head_key, updated_bytes.clone(), first_version.clone())
                .await?;
            let second_version = match updated {
                UpdateResult::Updated { version } => version,
                UpdateResult::PreconditionFailed => {
                    return Err("current version failed its precondition".into());
                }
            };
            assert_eq!(
                scoped
                    .update(
                        head_key,
                        Bytes::from_static(b"stale replacement"),
                        first_version.clone(),
                    )
                    .await?,
                UpdateResult::PreconditionFailed
            );
            let changed = scoped.read_if_changed(head_key, &first_version).await?;
            match changed {
                ConditionalRead::Modified(object) => {
                    assert_eq!(object.bytes, updated_bytes);
                    assert_eq!(object.version, second_version);
                }
                ConditionalRead::NotModified | ConditionalRead::Missing => {
                    return Err("updated head was not returned as changed".into());
                }
            }
        }
        UpdateSupport::Unsupported => {
            assert!(
                scoped
                    .update(
                        head_key,
                        Bytes::from_static(b"head generation two"),
                        first_version,
                    )
                    .await
                    .is_err()
            );
            assert_eq!(
                scoped.read(head_key).await?.map(|object| object.bytes),
                Some(initial)
            );
        }
    }
    Ok(())
}
