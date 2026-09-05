//! Git storage geometry, retaining the original durable options on old logs.

use object_log::{Error, Log, LogId, Options, ValidatedBackend};

// Durable profile V1: changing this value requires an explicit compatibility path.
const PROFILE_V1_OBJECT_REFS: usize = 2080;
const _: () = assert!(
    PROFILE_V1_OBJECT_REFS >= (2 * object_log_git::MAX_STREAM_PACK_BYTES).div_ceil(1024 * 1024)
);

fn profile() -> Options {
    let defaults = Options::default();
    // Normalization may append thin bases up to twice the receive-pack ceiling.
    // One root directly names its fixed 1 MiB chunks; all other core limits stay
    // at their original values. Options are durable and never changed in place.
    Options {
        max_object_refs: PROFILE_V1_OBJECT_REFS,
        ..defaults
    }
}

pub(crate) async fn open(backend: &ValidatedBackend, id: &LogId) -> Result<Log, Error> {
    match Log::open(backend, id, profile()).await {
        Err(Error::ConfigurationMismatch("options")) => {
            Log::open_existing(backend, id, Options::default()).await
        }
        result => result,
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) async fn open_existing(backend: &ValidatedBackend, id: &LogId) -> Result<Log, Error> {
    match Log::open_existing(backend, id, profile()).await {
        Err(Error::ConfigurationMismatch("options")) => {
            Log::open_existing(backend, id, Options::default()).await
        }
        result => result,
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[tokio::test]
    async fn opens_old_and_large_profiles_without_changing_durable_options() -> anyhow::Result<()> {
        let backend = ValidatedBackend::new(
            Arc::new(object_store::memory::InMemory::new()),
            object_store::path::Path::from("profiles"),
        )
        .await?;
        for (name, options) in [("old", Options::default()), ("large", profile())] {
            let id = LogId::new(name)?;
            let original = Log::open(&backend, &id, options).await?;
            let before = original.load().await?;
            assert_eq!(open(&backend, &id).await?.options(), options);
            assert_eq!(open_existing(&backend, &id).await?.options(), options);
            assert_eq!(original.load().await?.generation(), before.generation());
        }
        let id = LogId::new("new")?;
        assert_eq!(open(&backend, &id).await?.options(), profile());
        let absent = LogId::new("absent")?;
        assert!(open_existing(&backend, &absent).await.is_err());
        assert!(
            Log::open_existing(&backend, &absent, profile())
                .await
                .is_err()
        );
        Ok(())
    }
}
