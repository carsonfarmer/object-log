use std::error::Error as StdError;
use std::sync::Arc;

use bytes::Bytes;
use futures::TryStreamExt;
use object_log::{BackendCapability, Error, Log, LogId, Options, ValidatedBackend};
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use tempfile::TempDir;

#[cfg(feature = "aws")]
mod support;

#[tokio::test]
async fn memory_backend_conforms() -> Result<(), Box<dyn StdError>> {
    backend_conforms(Arc::new(InMemory::new())).await
}

async fn backend_conforms(store: Arc<dyn ObjectStore>) -> Result<(), Box<dyn StdError>> {
    let backend = ValidatedBackend::new(store, Path::from("conformance")).await?;
    assert!(
        backend
            .capabilities()
            .supports(BackendCapability::ConditionalUpdate)
    );
    let log_id = LogId::new("test-log")?;
    Log::open(&backend, &log_id, Options::default()).await?;
    Ok(())
}

#[tokio::test]
async fn filesystem_backend_reports_missing_update() -> Result<(), Box<dyn StdError>> {
    let directory = TempDir::new()?;
    let backend = LocalFileSystem::new_with_prefix(directory.path())?;
    assert!(matches!(
        ValidatedBackend::new(Arc::new(backend), Path::from("conformance")).await,
        Err(Error::UnsupportedBackend("conditional update"))
    ));
    Ok(())
}

#[tokio::test]
async fn log_namespaces_are_isolated() -> Result<(), Box<dyn StdError>> {
    namespaces_are_isolated(Arc::new(InMemory::new())).await
}

async fn namespaces_are_isolated(backend: Arc<dyn ObjectStore>) -> Result<(), Box<dyn StdError>> {
    let root = Path::from("shared-root");
    let backend = ValidatedBackend::new(backend, root).await?;
    let first = Log::open(&backend, &LogId::new("tenant-a")?, Options::default()).await?;
    let second = Log::open(&backend, &LogId::new("tenant-b")?, Options::default()).await?;

    let bytes = Bytes::from_static(b"same logical object");
    let first_view = first.load().await?;
    let second_view = second.load().await?;
    let first_object = first.put_object(&first_view, bytes.clone()).await?;
    let second_object = second.put_object(&second_view, bytes.clone()).await?;
    assert_ne!(first_object.reference(), second_object.reference());
    assert_eq!(
        first_object.reference().kind(),
        second_object.reference().kind()
    );
    assert_eq!(
        first_object.reference().digest(),
        second_object.reference().digest()
    );
    assert_eq!(
        first_object.reference().len(),
        second_object.reference().len()
    );
    assert_eq!(
        first
            .read_object(&first.load().await?, first_object.reference())
            .await?,
        bytes
    );
    assert_eq!(
        second
            .read_object(&second.load().await?, second_object.reference())
            .await?,
        bytes
    );
    Ok(())
}

#[tokio::test]
async fn probe_removes_only_its_object() -> Result<(), Box<dyn StdError>> {
    probe_cleanup(Arc::new(InMemory::new())).await
}

async fn probe_cleanup(backend: Arc<dyn ObjectStore>) -> Result<(), Box<dyn StdError>> {
    let sentinel = Path::from("unrelated/sentinel");
    backend
        .put(&sentinel, Bytes::from_static(b"keep").into())
        .await?;
    let before = backend.list(None).try_collect::<Vec<_>>().await?;
    let validated = ValidatedBackend::new(Arc::clone(&backend), Path::from("root")).await?;
    assert!(
        validated
            .capabilities()
            .supports(BackendCapability::ConditionalUpdate)
    );

    let after = backend.list(None).try_collect::<Vec<_>>().await?;
    assert_eq!(before.len(), after.len());
    assert!(after.iter().any(|meta| meta.location == sentinel));
    Ok(())
}

#[cfg(feature = "aws")]
#[tokio::test]
#[ignore = "requires local MinIO"]
async fn minio_backend_conformance() -> Result<(), Box<dyn StdError>> {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::prefix::PrefixStore::new(
        support::minio::build_minio()?,
        Path::from(format!("conformance-{}", uuid::Uuid::new_v4().simple())),
    ));
    backend_conforms(Arc::clone(&store)).await?;
    namespaces_are_isolated(Arc::clone(&store)).await?;
    probe_cleanup(Arc::clone(&store)).await?;
    let objects = store.list(None).map_ok(|meta| meta.location);
    store
        .delete_stream(Box::pin(objects))
        .try_collect::<Vec<_>>()
        .await?;
    Ok(())
}
