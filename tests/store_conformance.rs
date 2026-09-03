use std::error::Error as StdError;
use std::sync::Arc;

use bytes::Bytes;
use futures::TryStreamExt;
use object_log::{BackendCapability, Error, Log, LogId, Options, ScopedStore};
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
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
    let first_store =
        ScopedStore::new(Arc::clone(&backend), root.clone(), &LogId::new("tenant-a")?);
    let second_store = ScopedStore::new(backend, root, &LogId::new("tenant-b")?);
    let first = Log::open(first_store, Options::default()).await?;
    let second = Log::open(second_store, Options::default()).await?;

    let bytes = Bytes::from_static(b"same logical object");
    let first_object = first.put_object(bytes.clone()).await?;
    let second_object = second.put_object(bytes.clone()).await?;
    assert_eq!(first_object, second_object);
    assert_eq!(first.read_object(&first_object).await?, bytes);
    assert_eq!(second.read_object(&second_object).await?, bytes);
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

    match update_support {
        UpdateSupport::Required => {
            scoped.validate_backend().await?;
            Log::open(scoped, Options::default()).await?;
        }
        UpdateSupport::Unsupported => {
            assert!(matches!(
                Log::open(scoped, Options::default()).await,
                Err(Error::UnsupportedBackend("conditional update"))
            ));
        }
    }
    Ok(())
}
