use std::{any::Any, sync::Arc, time::Duration};

use anyhow::{Context, ensure};
use object_log::{Log, LogId, Options, ValidatedBackend};
use object_log_kv::{KvStore, Limits};
use object_store::{ClientOptions, ObjectStore, RetryConfig, aws::AmazonS3Builder, path::Path};
use serde::{Deserialize, Serialize};
use spin_factor_key_value::{Error, Store, StoreManager, runtime_config::spin::MakeKeyValueStore};
use tokio::sync::{OnceCell, Semaphore};

use crate::store::{BackendStore, Maintenance, public_error};

/// A registered runtime-config provider (`type = "object-log"`).
#[derive(Clone, Copy, Debug, Default)]
pub struct ObjectLogKeyValueStore;

/// Host-only backend configuration. Unknown fields fail closed.
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// An explicit deployment/tenant prefix. Different apps must use different prefixes.
    pub prefix: String,
    /// Native S3 bucket. Omit only for the explicit ephemeral memory mode.
    pub bucket: Option<String>,
    /// AWS region (credential acquisition remains in object_store).
    pub region: Option<String>,
    /// Optional S3-compatible endpoint.
    pub endpoint: Option<String>,
    /// Permit HTTP for local MinIO; defaults to false.
    #[serde(default)]
    pub allow_http: bool,
    /// Explicit ephemeral testing/development mode.
    #[serde(default)]
    pub memory: bool,
    /// Admission and per-call limits.
    #[serde(default)]
    pub limits: HostLimits,
    /// Durable WAL limits; all openers must use the same values.
    #[serde(default, with = "WalOptions")]
    pub wal: Options,
}

/// Bounded host work. These are admission limits, not a whole-process RSS cap.
#[derive(Clone, Copy, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct HostLimits {
    /// Maximum UTF-8 key bytes.
    pub key_bytes: usize,
    /// Maximum bytes in one value.
    pub value_bytes: usize,
    /// Maximum commands or keys in a batch.
    pub batch_entries: usize,
    /// Maximum aggregate input key and value bytes.
    pub batch_bytes: usize,
    /// Maximum entries in an internal scan page.
    pub page_entries: usize,
    /// Maximum returned bytes plus collection headers and element storage.
    pub response_bytes: usize,
    /// KV tree work allowance per call or scan page.
    pub tree_bytes: usize,
    /// Maximum returned keys across all scan pages.
    pub list_keys: usize,
    /// Maximum concurrent operations per configured manager; no waiting queue.
    pub concurrent_operations: usize,
    /// Maximum definite-conflict attempts per mutation.
    pub attempts: usize,
    /// Maximum logical object-store invocations, including adapter retries.
    /// Excludes retries and pagination internal to object_store.
    pub requests: usize,
    /// Checkpoint before writing when the tail reaches this size.
    pub checkpoint_entries: usize,
    /// Maximum candidates in one collection plan.
    pub collection_candidates: usize,
}

impl Default for HostLimits {
    fn default() -> Self {
        Self {
            key_bytes: 256,
            value_bytes: 64 * 1024,
            batch_entries: 128,
            batch_bytes: 1024 * 1024,
            page_entries: 128,
            response_bytes: 1024 * 1024,
            tree_bytes: 32 * 1024 * 1024,
            list_keys: 4096,
            concurrent_operations: 16,
            attempts: 8,
            requests: 100_000,
            checkpoint_entries: 64,
            collection_candidates: 1024,
        }
    }
}

impl HostLimits {
    pub(crate) fn kv(self) -> Limits {
        Limits {
            key_bytes: self.key_bytes,
            value_bytes: self.value_bytes,
            batch_entries: self.batch_entries,
            batch_bytes: self.batch_bytes,
            page_entries: self.page_entries,
            response_bytes: self.response_bytes,
            tree_bytes: self.tree_bytes,
        }
    }

    fn validate(self, wal: Options) -> anyhow::Result<()> {
        ensure!(
            self.concurrent_operations > 0 && self.concurrent_operations <= Semaphore::MAX_PERMITS,
            "invalid concurrent_operations"
        );
        ensure!(
            self.attempts > 0 && self.requests > 0,
            "attempts and requests must be positive"
        );
        ensure!(
            self.page_entries > 0 && self.list_keys > 0,
            "scan limits must be positive"
        );
        ensure!(
            self.checkpoint_entries > 0 && self.checkpoint_entries <= wal.max_tail_entries,
            "checkpoint_entries must fit the WAL tail"
        );
        ensure!(
            self.collection_candidates > 0,
            "collection_candidates must be positive"
        );
        ensure!(self.value_bytes >= 8, "value_bytes must admit integers");
        Ok(())
    }
}

/// Serde mirror of the public durable WAL options (no Spin types in the core).
#[derive(Clone, Copy, Deserialize, Serialize)]
#[serde(default = "Options::default", deny_unknown_fields, remote = "Options")]
struct WalOptions {
    pub max_tail_entries: usize,
    pub resolution_window: usize,
    pub max_inline_operation_bytes: usize,
    pub max_inline_result_bytes: usize,
    pub max_object_refs: usize,
    pub max_object_bytes: usize,
    pub max_commit_bytes: usize,
    pub max_head_bytes: usize,
    pub max_checkpoint_bytes: usize,
    pub max_retention_ids: usize,
    pub max_collection_objects: usize,
    pub max_collection_plan_bytes: usize,
}

impl MakeKeyValueStore for ObjectLogKeyValueStore {
    const RUNTIME_CONFIG_TYPE: &'static str = "object-log";
    type RuntimeConfig = Config;
    type StoreManager = Manager;

    fn make_store(&self, config: Config) -> anyhow::Result<Manager> {
        let store: Arc<dyn ObjectStore> = if config.memory {
            ensure!(
                config.bucket.is_none()
                    && config.endpoint.is_none()
                    && config.region.is_none()
                    && !config.allow_http,
                "memory mode cannot contain S3 settings"
            );
            Arc::new(object_store::memory::InMemory::new())
        } else {
            let mut builder = AmazonS3Builder::from_env()
                .with_bucket_name(config.bucket.as_deref().context("bucket is required")?)
                .with_client_options(
                    ClientOptions::new()
                        .with_allow_http(config.allow_http)
                        .with_timeout(Duration::from_secs(30)),
                )
                .with_retry(RetryConfig {
                    max_retries: 2,
                    retry_timeout: Duration::from_secs(60),
                    ..Default::default()
                });
            if let Some(region) = &config.region {
                builder = builder.with_region(region);
            }
            if let Some(endpoint) = &config.endpoint {
                builder = builder.with_endpoint(endpoint);
            }
            Arc::new(builder.build().context("invalid native S3 configuration")?)
        };
        Manager::new(store, &config.prefix, config.wal, config.limits)
    }
}

/// Manager for a host-configured namespace. Names map to distinct WAL identities.
/// Spin's delegating manager and component manifest enforce label permissions.
/// Clones share backend validation and admission. `StoreManager::metadata`
/// exposes a clone so hosts using the standard resolver can run maintenance.
#[derive(Clone)]
pub struct Manager {
    store: Arc<dyn ObjectStore>,
    prefix: Path,
    backend: Arc<OnceCell<ValidatedBackend>>,
    wal: Options,
    limits: HostLimits,
    admission: Arc<Semaphore>,
}

impl Manager {
    /// Inject a native backend. Its conditional-write contract is probed on first use.
    pub fn new(
        store: Arc<dyn ObjectStore>,
        prefix: &str,
        wal: Options,
        limits: HostLimits,
    ) -> anyhow::Result<Self> {
        ensure!(
            !prefix.is_empty(),
            "an explicit nonempty deployment prefix is required"
        );
        let prefix_path = Path::parse(prefix)?;
        ensure!(prefix_path.as_ref() == prefix, "prefix must be canonical");
        limits.validate(wal)?;
        Ok(Self {
            store,
            prefix: prefix_path,
            backend: Arc::new(OnceCell::new()),
            wal,
            limits,
            admission: Arc::new(Semaphore::new(limits.concurrent_operations)),
        })
    }

    async fn open(&self, name: &str) -> Result<BackendStore, Error> {
        if name.is_empty() || name.len() > 256 {
            return Err(Error::NoSuchStore);
        }
        let _permit = self
            .admission
            .try_acquire()
            .map_err(|_| Error::Other("object-log busy: concurrent operation limit".into()))?;
        let backend = self
            .backend
            .get_or_try_init(|| async {
                ValidatedBackend::new(self.store.clone(), self.prefix.clone()).await
            })
            .await
            .map_err(public_error)?;
        // Length-framed by the fixed digest, with no path interpretation of guest labels.
        let id =
            LogId::new(blake3::hash(name.as_bytes()).to_hex().to_string()).map_err(public_error)?;
        let log = Log::open(backend, &id, self.wal)
            .await
            .map_err(public_error)?;
        Ok(BackendStore {
            kv: KvStore::new(log, self.limits.kv()),
            limits: self.limits,
            admission: self.admission.clone(),
        })
    }

    /// Perform one bounded checkpoint/collection pass from the host.
    /// Prefer a drained deployment; active operations may return expired-view errors.
    /// Never clears reader retentions. Repeating this method resumes an installed plan.
    pub async fn maintain(&self, name: &str) -> Result<Maintenance, Error> {
        self.open(name).await?.maintain().await
    }
}

#[async_trait::async_trait]
impl StoreManager for Manager {
    async fn get(&self, name: &str) -> Result<Arc<dyn Store>, Error> {
        Ok(Arc::new(self.open(name).await?))
    }
    fn is_defined(&self, name: &str) -> bool {
        !name.is_empty() && name.len() <= 256
    }
    fn summary(&self, _name: &str) -> Option<String> {
        Some("object-log (native host storage)".into())
    }
    fn metadata(&self) -> Arc<dyn Any> {
        Arc::new(self.clone())
    }
}
