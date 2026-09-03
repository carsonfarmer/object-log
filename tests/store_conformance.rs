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

#[tokio::test]
async fn memory_backend_conforms() -> Result<(), Box<dyn StdError>> {
    let backend =
        ValidatedBackend::new(Arc::new(InMemory::new()), Path::from("conformance")).await?;
    assert!(
        backend
            .capabilities()
            .supports(BackendCapability::ConditionalUpdate)
    );
    let log_id = LogId::new("test-log")?;
    Log::open(backend.scope(&log_id), Options::default()).await?;
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
    let backend: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let root = Path::from("shared-root");
    let backend = ValidatedBackend::new(backend, root).await?;
    let first = Log::open(backend.scope(&LogId::new("tenant-a")?), Options::default()).await?;
    let second = Log::open(backend.scope(&LogId::new("tenant-b")?), Options::default()).await?;

    let bytes = Bytes::from_static(b"same logical object");
    let first_view = first.load().await?;
    let second_view = second.load().await?;
    let first_object = first.put_object(first_view.cursor(), bytes.clone()).await?;
    let second_object = second
        .put_object(second_view.cursor(), bytes.clone())
        .await?;
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
    let backend: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
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
