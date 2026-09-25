use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use bytes::Bytes;
use object_log::{
    CheckpointResolution, CheckpointStatus, CollectionFinish, CollectionStart, CommitStatus,
    PreparedCommit, Request, RequestDenied, RequestGuard, Resolution, TransactionId,
};
use object_log_kv::{KvCommand, KvError, KvSnapshot, KvStore};
use spin_factor_key_value::{Cas, Error, Store, SwapError, v3};
use tokio::sync::{Mutex, Semaphore, mpsc, oneshot};
use tracing::Instrument;

use crate::HostLimits;

#[derive(Clone)]
pub(crate) struct BackendStore {
    pub kv: KvStore,
    pub limits: HostLimits,
    pub admission: Arc<Semaphore>,
    pub owner: Arc<Owner>,
    pub owner_bytes: Arc<AtomicUsize>,
    pub closing: Arc<AtomicBool>,
}

pub(crate) struct Owner {
    sender: mpsc::Sender<WriteJob>,
}

struct WriteJob {
    commands: Vec<KvCommand>,
    input: InputCharge,
    admitted: Instant,
    done: oneshot::Sender<Result<(), Error>>,
}

struct InputCharge {
    bytes: usize,
    total: Arc<AtomicUsize>,
}

impl Drop for InputCharge {
    fn drop(&mut self) {
        self.total.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

struct Writer {
    kv: KvStore,
    limits: HostLimits,
}

enum GroupFailure {
    Limit,
    Other(Error),
}

impl Owner {
    pub(crate) fn spawn(kv: KvStore, limits: HostLimits) -> Arc<Self> {
        let (sender, mut receiver) = mpsc::channel(limits.concurrent_operations);
        tokio::spawn(async move {
            let mut next: Option<WriteJob> = None;
            loop {
                let first = match next.take() {
                    Some(job) => job,
                    None => match receiver.recv().await {
                        Some(job) => job,
                        None => return,
                    },
                };
                let mut count = first.commands.len();
                let mut bytes = first.input.bytes;
                let mut jobs = vec![first];
                if count < limits.batch_entries && bytes < limits.batch_bytes {
                    tokio::task::yield_now().await;
                }
                while let Ok(job) = receiver.try_recv() {
                    if count
                        .checked_add(job.commands.len())
                        .is_some_and(|next| next <= limits.batch_entries)
                        && bytes.saturating_add(job.input.bytes) <= limits.batch_bytes
                    {
                        count += job.commands.len();
                        bytes += job.input.bytes;
                        jobs.push(job);
                    } else {
                        next = Some(job);
                        break;
                    }
                }
                let jobs = jobs
                    .into_iter()
                    .filter_map(|job| {
                        if job.admitted.elapsed() >= Duration::from_millis(limits.owner_wait_ms) {
                            let _ = job
                                .done
                                .send(Err(other("object-log busy: owner wait limit")));
                            None
                        } else {
                            Some(job)
                        }
                    })
                    .collect::<Vec<_>>();
                if !jobs.is_empty() {
                    Self::publish_group(&kv, limits, jobs).await;
                }
            }
        });
        Arc::new(Self { sender })
    }

    async fn publish_group(kv: &KvStore, limits: HostLimits, jobs: Vec<WriteJob>) {
        if jobs.len() == 1 {
            let writer = Writer::new(kv.clone(), limits);
            let result = writer.write(&jobs[0].commands).await;
            if let Some(job) = jobs.into_iter().next() {
                let _ = job.done.send(result);
            }
            return;
        }
        let commands = jobs
            .iter()
            .flat_map(|job| job.commands.iter().cloned())
            .collect::<Vec<_>>();
        let writer = Writer::new(kv.clone(), limits);
        match writer.write_group(&commands).await {
            Ok(()) => {
                for job in jobs {
                    let _ = job.done.send(Ok(()));
                }
            }
            Err(GroupFailure::Limit) => {
                for job in jobs {
                    let writer = Writer::new(kv.clone(), limits);
                    let result = writer.write(&job.commands).await;
                    let _ = job.done.send(result);
                }
            }
            Err(GroupFailure::Other(error)) => {
                let message = error.to_string();
                for job in jobs {
                    let _ = job.done.send(Err(other(&message)));
                }
            }
        }
    }

    async fn write(
        &self,
        commands: Vec<KvCommand>,
        input: InputCharge,
        admitted: Instant,
    ) -> Result<(), Error> {
        let (done, result) = oneshot::channel();
        self.sender
            .try_send(WriteJob {
                commands,
                input,
                admitted,
                done,
            })
            .map_err(|_| other("object-log owner unavailable"))?;
        result.await.map_err(|_| unknown())?
    }
}

impl Writer {
    fn new(kv: KvStore, limits: HostLimits) -> Self {
        let log = kv
            .log()
            .with_request_guard(Arc::new(Requests(AtomicUsize::new(limits.requests))));
        Self {
            kv: KvStore::new(log, limits.kv()),
            limits,
        }
    }

    async fn snapshot(&self) -> Result<KvSnapshot, Error> {
        self.kv.snapshot().await.map_err(public_error)
    }

    async fn write_snapshot(&self) -> Result<KvSnapshot, Error> {
        let snapshot = self.snapshot().await?;
        if snapshot.view().tail().len() >= self.limits.checkpoint_entries {
            BackendStore::checkpoint(&snapshot, &self.kv).await?;
            self.snapshot().await
        } else {
            Ok(snapshot)
        }
    }

    async fn publish(&self, snapshot: &KvSnapshot, commands: &[KvCommand]) -> Result<bool, Error> {
        let candidate = snapshot
            .prepare(TransactionId::new(), commands)
            .await
            .map_err(public_kv_error)?;
        self.commit(candidate).await
    }

    async fn publish_unobserved(
        &self,
        snapshot: &KvSnapshot,
        commands: &[KvCommand],
    ) -> Result<bool, Error> {
        let Some(candidate) = snapshot
            .prepare_if_changed(TransactionId::new(), commands)
            .await
            .map_err(public_kv_error)?
        else {
            return Ok(true);
        };
        self.commit(candidate).await
    }

    async fn commit(&self, candidate: PreparedCommit) -> Result<bool, Error> {
        match self
            .kv
            .log()
            .commit(candidate)
            .await
            .map_err(public_error)?
        {
            CommitStatus::Committed(_) => Ok(true),
            CommitStatus::Conflict(_) => Ok(false),
            CommitStatus::Pending(mut pending) => {
                for attempt in 0..self.limits.attempts {
                    match self.kv.log().resolve(pending).await.map_err(|error| {
                        tracing::warn!(error = %error, "object-log pending resolution failed");
                        unknown()
                    })? {
                        Resolution::Committed(_) => return Ok(true),
                        Resolution::NotCommitted(_) => return Ok(false),
                        Resolution::StillPending(next) => {
                            pending = next;
                            if attempt + 1 < self.limits.attempts {
                                back_off(attempt).await;
                            }
                        }
                        Resolution::Expired(_) => return Err(unknown()),
                    }
                }
                Err(unknown())
            }
        }
    }

    async fn write(&self, commands: &[KvCommand]) -> Result<(), Error> {
        for attempt in 0..self.limits.attempts {
            let snapshot = self.write_snapshot().await?;
            if self.publish_unobserved(&snapshot, commands).await? {
                return Ok(());
            }
            if attempt + 1 < self.limits.attempts {
                back_off(attempt).await;
            }
        }
        Err(other("object-log contention limit"))
    }

    async fn write_group(&self, commands: &[KvCommand]) -> Result<(), GroupFailure> {
        for attempt in 0..self.limits.attempts {
            let snapshot = self.write_snapshot().await.map_err(GroupFailure::Other)?;
            let candidate = match snapshot
                .prepare_if_changed(TransactionId::new(), commands)
                .await
            {
                Ok(Some(candidate)) => candidate,
                Ok(None) => return Ok(()),
                Err(
                    error @ (KvError::Limit(_) | KvError::Log(object_log::Error::LimitExceeded(_))),
                ) => {
                    tracing::warn!(error = %error, "group preparation failed");
                    return Err(GroupFailure::Limit);
                }
                Err(error) => return Err(GroupFailure::Other(public_kv_error(error))),
            };
            if self.commit(candidate).await.map_err(GroupFailure::Other)? {
                return Ok(());
            }
            if attempt + 1 < self.limits.attempts {
                back_off(attempt).await;
            }
        }
        Err(GroupFailure::Other(other("object-log contention limit")))
    }
}

/// Outcome of one host-only maintenance pass. `More` may require another pass.
#[derive(Debug, PartialEq, Eq)]
pub enum Maintenance {
    Complete,
    More,
    Retained,
    Contended,
    Pending,
}

// Provider details stay in host diagnostics, never in guest errors.
pub(crate) fn public_error(error: impl std::fmt::Display) -> Error {
    tracing::warn!(error = %error, "object-log key-value operation failed");
    Error::Other("object-log storage or admission error".into())
}

fn public_kv_error(error: KvError) -> Error {
    if matches!(
        error,
        KvError::Log(object_log::Error::LimitExceeded("publication objects"))
    ) {
        tracing::warn!(error = %error, "object-log key-value operation failed");
        other("object-log total-state object limit exceeded")
    } else {
        public_error(error)
    }
}

fn other(message: &str) -> Error {
    Error::Other(message.into())
}
fn unknown() -> Error {
    tracing::warn!("object-log publication outcome unknown; operation was not replayed");
    other("object-log publication outcome unknown; do not blindly retry mutations")
}

async fn back_off(attempt: usize) {
    let ceiling_ms = 2_u64 << attempt.min(7);
    let random = TransactionId::new();
    let jitter_ms = u64::from(random.as_uuid().as_bytes()[0]) % ceiling_ms;
    tokio::time::sleep(Duration::from_millis(ceiling_ms + jitter_ms)).await;
}

#[derive(Debug)]
struct Requests(AtomicUsize);
impl RequestGuard for Requests {
    fn before_request(&self, _request: Request) -> Result<(), RequestDenied> {
        self.0
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_sub(1))
            .map(|_| ())
            .map_err(|_| RequestDenied)
    }
}

impl BackendStore {
    // The permit remains owned by admitted work after guest cancellation.
    async fn run<T, F, Fut>(&self, work: F) -> Result<T, Error>
    where
        T: Send + 'static,
        F: FnOnce(Self) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, Error>> + Send + 'static,
    {
        if self.closing.load(Ordering::Acquire) {
            return Err(other("object-log manager is draining"));
        }
        let permit = self
            .admission
            .clone()
            .try_acquire_owned()
            .map_err(|_| other("object-log busy: concurrent operation limit"))?;
        if self.closing.load(Ordering::Acquire) {
            return Err(other("object-log manager is draining"));
        }
        let log = self
            .kv
            .log()
            .with_request_guard(Arc::new(Requests(AtomicUsize::new(self.limits.requests))));
        let store = Self {
            kv: KvStore::new(log, self.limits.kv()),
            ..self.clone()
        };
        tokio::spawn(
            async move {
                let _permit = permit;
                work(store).await
            }
            .in_current_span(),
        )
        .await
        .map_err(public_error)?
    }

    fn key(&self, key: &str) -> Result<Bytes, Error> {
        self.check_key(key)?;
        Ok(Bytes::copy_from_slice(key.as_bytes()))
    }

    fn check_key(&self, key: &str) -> Result<(), Error> {
        if key.len() > self.limits.key_bytes {
            return Err(other("object-log key limit"));
        }
        Ok(())
    }

    fn batch(&self, count: usize, bytes: usize) -> Result<(), Error> {
        if count > self.limits.batch_entries || bytes > self.limits.batch_bytes {
            return Err(other("object-log batch limit"));
        }
        Ok(())
    }

    fn value(&self, value: &[u8]) -> Result<Bytes, Error> {
        self.check_value(value)?;
        Ok(Bytes::copy_from_slice(value))
    }

    fn check_value(&self, value: &[u8]) -> Result<(), Error> {
        if value.len() > self.limits.value_bytes {
            return Err(other("object-log value limit"));
        }
        Ok(())
    }

    async fn snapshot(&self) -> Result<KvSnapshot, Error> {
        self.kv.snapshot().await.map_err(public_error)
    }

    async fn checkpoint(snapshot: &KvSnapshot, kv: &KvStore) -> Result<bool, Error> {
        match snapshot.checkpoint().await.map_err(public_kv_error)? {
            None | Some(CheckpointStatus::Published(_)) => Ok(true),
            Some(CheckpointStatus::Conflict(_)) => Ok(false),
            Some(CheckpointStatus::Pending(pending)) => {
                match kv
                    .log()
                    .resolve_checkpoint(pending)
                    .await
                    .map_err(public_error)?
                {
                    CheckpointResolution::Published(_) => Ok(true),
                    // Checkpoints do not change KV state. If later publications
                    // replaced the evidence, reload their authenticated state.
                    // The caller's mutation has not been prepared or sent yet.
                    CheckpointResolution::NotPublished(_) | CheckpointResolution::Expired(_) => {
                        Ok(false)
                    }
                    CheckpointResolution::StillPending(_) => Err(other(
                        "object-log checkpoint unresolved; requested mutation not published",
                    )),
                }
            }
        }
    }

    async fn write_snapshot(&self) -> Result<KvSnapshot, Error> {
        Writer {
            kv: self.kv.clone(),
            limits: self.limits,
        }
        .write_snapshot()
        .await
    }

    // Only a definite rejection allows revalidation/re-preparation. Pending work
    // retains its exact candidate and is resolved a bounded number of times.
    async fn publish(&self, snapshot: &KvSnapshot, commands: &[KvCommand]) -> Result<bool, Error> {
        Writer {
            kv: self.kv.clone(),
            limits: self.limits,
        }
        .publish(snapshot, commands)
        .await
    }

    async fn write(&self, commands: Vec<KvCommand>) -> Result<(), Error> {
        if commands.is_empty() {
            return if self.closing.load(Ordering::Acquire) {
                Err(other("object-log manager is draining"))
            } else {
                Ok(())
            };
        }
        let admitted = Instant::now();
        self.run(move |store| async move {
            let bytes = commands.iter().fold(0usize, |total, command| {
                total.saturating_add(match command {
                    KvCommand::Set { key, value } => key.len().saturating_add(value.len()),
                    KvCommand::Delete { key } => key.len(),
                    _ => 0,
                })
            });
            let total = store.owner_bytes.clone();
            total
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                    current
                        .checked_add(bytes)
                        .filter(|n| *n <= store.limits.owner_input_bytes)
                })
                .map_err(|_| other("object-log busy: owner input limit"))?;
            let input = InputCharge { bytes, total };
            store.owner.write(commands, input, admitted).await
        })
        .await
    }

    pub(crate) async fn maintain(&self) -> Result<Maintenance, Error> {
        self.run(|store| async move {
            // A fenced plan may prevent materializing its old tree. Resume it first.
            let view = store.kv.log().load().await.map_err(public_error)?;
            let view = if view.collection_plan_bytes().is_some() {
                view
            } else {
                let snapshot = store.snapshot().await?;
                if !Self::checkpoint(&snapshot, &store.kv).await? {
                    return Ok(Maintenance::Contended);
                }
                let view = store.kv.log().load().await.map_err(public_error)?;
                match store
                    .kv
                    .log()
                    .start_collection_with_limit(&view, store.limits.collection_candidates)
                    .await
                    .map_err(public_error)?
                {
                    CollectionStart::Empty(_) => return Ok(Maintenance::Complete),
                    CollectionStart::Installed(view, _) | CollectionStart::Active(view) => view,
                    CollectionStart::Retained(_) => return Ok(Maintenance::Retained),
                    CollectionStart::Conflict(_) => return Ok(Maintenance::Contended),
                    CollectionStart::Pending => return Ok(Maintenance::Pending),
                }
            };
            match store
                .kv
                .log()
                .resume_collection(&view)
                .await
                .map_err(public_error)?
            {
                CollectionFinish::Complete(_, report) => {
                    tracing::info!(
                        deleted = report.delete_attempts(),
                        "object-log collection pass complete"
                    );
                    Ok(Maintenance::More)
                }
                CollectionFinish::Conflict(_, _) => Ok(Maintenance::Contended),
                CollectionFinish::Pending(_) => Ok(Maintenance::Pending),
            }
        })
        .await
    }
}

#[async_trait::async_trait]
impl Store for BackendStore {
    async fn get(&self, key: &str, max_result_bytes: usize) -> Result<Option<Vec<u8>>, Error> {
        let key = self.key(key)?;
        self.run(move |store| async move {
            let value = store
                .snapshot()
                .await?
                .get(&key)
                .await
                .map_err(public_error)?;
            let bytes =
                std::mem::size_of::<Option<Vec<u8>>>() + value.as_ref().map_or(0, Bytes::len);
            if bytes > max_result_bytes.min(store.limits.response_bytes) {
                return Err(other("object-log response limit"));
            }
            Ok(value.map(|v| v.to_vec()))
        })
        .await
    }

    async fn set(&self, key: &str, value: &[u8]) -> Result<(), Error> {
        self.write(vec![KvCommand::Set {
            key: self.key(key)?,
            value: self.value(value)?,
        }])
        .await
    }

    async fn delete(&self, key: &str) -> Result<(), Error> {
        self.write(vec![KvCommand::Delete {
            key: self.key(key)?,
        }])
        .await
    }

    async fn exists(&self, key: &str) -> Result<bool, Error> {
        let key = self.key(key)?;
        self.run(move |store| async move {
            Ok(store
                .snapshot()
                .await?
                .get(&key)
                .await
                .map_err(public_error)?
                .is_some())
        })
        .await
    }

    async fn get_keys(&self, max_result_bytes: usize) -> Result<Vec<String>, Error> {
        let limit = max_result_bytes.min(self.limits.response_bytes);
        let header_bytes = std::mem::size_of::<Vec<String>>();
        if header_bytes > limit {
            return Err(other("object-log key listing limit"));
        }
        self.run(move |store| async move {
            // KV scans read value-bearing pages. Keep their transient allowance
            // large enough for one admitted record even when the key-only output
            // has a smaller limit.
            let mut limits = store.limits.kv();
            limits.response_bytes = limits
                .response_bytes
                .max(limits.key_bytes.saturating_add(limits.value_bytes));
            let kv = KvStore::new(store.kv.log().clone(), limits);
            let snapshot = kv.snapshot().await.map_err(public_error)?;
            let mut after = None;
            let mut keys = Vec::new();
            let mut bytes = header_bytes;
            loop {
                let page = snapshot
                    .scan(b"", None, after.as_deref(), store.limits.page_entries)
                    .await
                    .map_err(public_error)?;
                for (key, _) in page.entries {
                    bytes = bytes.saturating_add(key.len() + std::mem::size_of::<String>());
                    if keys.len() >= store.limits.list_keys || bytes > limit {
                        return Err(other("object-log key listing limit"));
                    }
                    keys.push(String::from_utf8(key.to_vec()).map_err(public_error)?);
                }
                after = page.after;
                if after.is_none() {
                    return Ok(keys);
                }
            }
        })
        .await
    }

    async fn get_keys_async(
        &self,
        max_result_bytes: usize,
    ) -> (
        tokio::sync::mpsc::Receiver<String>,
        tokio::sync::oneshot::Receiver<Result<(), v3::Error>>,
    ) {
        // Buffer only the same bounded result as the synchronous interface. No
        // detached producer can remain blocked on a guest which stops reading.
        let result = self.get_keys(max_result_bytes).await;
        let capacity = result.as_ref().map_or(1, |keys| keys.len().max(1));
        let (sender, receiver) = tokio::sync::mpsc::channel(capacity);
        let (done, completion) = tokio::sync::oneshot::channel();
        let result = result
            .and_then(|keys| {
                for key in keys {
                    sender.try_send(key).map_err(public_error)?;
                }
                Ok(())
            })
            .map_err(spin_factor_key_value::to_v3_err);
        let _ = done.send(result);
        (receiver, completion)
    }

    async fn get_many(
        &self,
        keys: Vec<String>,
        max_result_bytes: usize,
    ) -> Result<Vec<(String, Option<Vec<u8>>)>, Error> {
        self.batch(keys.len(), keys.iter().map(String::len).sum())?;
        let encoded = keys
            .iter()
            .map(|k| self.key(k))
            .collect::<Result<Vec<_>, _>>()?;
        self.run(move |store| async move {
            let values = store
                .snapshot()
                .await?
                .get_many(&encoded)
                .await
                .map_err(public_error)?;
            let header_bytes = std::mem::size_of::<Vec<(String, Option<Vec<u8>>)>>();
            let bytes = keys.iter().zip(&values).fold(header_bytes, |n, (k, v)| {
                n.saturating_add(
                    std::mem::size_of::<(String, Option<Vec<u8>>)>()
                        + k.len()
                        + v.as_ref().map_or(0, Bytes::len),
                )
            });
            if bytes > max_result_bytes.min(store.limits.response_bytes) {
                return Err(other("object-log response limit"));
            }
            Ok(keys
                .into_iter()
                .zip(values.into_iter().map(|v| v.map(|v| v.to_vec())))
                .collect())
        })
        .await
    }

    async fn set_many(&self, values: Vec<(String, Vec<u8>)>) -> Result<(), Error> {
        self.batch(
            values.len(),
            values.iter().map(|(k, v)| k.len() + v.len()).sum(),
        )?;
        let commands = values
            .into_iter()
            .map(|(key, value)| {
                self.check_key(&key)?;
                self.check_value(&value)?;
                Ok(KvCommand::Set {
                    key: Bytes::from(key),
                    value: Bytes::from(value),
                })
            })
            .collect::<Result<_, Error>>()?;
        self.write(commands).await
    }

    async fn delete_many(&self, keys: Vec<String>) -> Result<(), Error> {
        self.batch(keys.len(), keys.iter().map(String::len).sum())?;
        let commands = keys
            .into_iter()
            .map(|key| {
                self.check_key(&key)?;
                Ok(KvCommand::Delete {
                    key: Bytes::from(key),
                })
            })
            .collect::<Result<_, Error>>()?;
        self.write(commands).await
    }

    async fn increment(&self, key: String, delta: i64) -> Result<i64, Error> {
        let key = self.key(&key)?;
        self.run(move |store| async move {
            for attempt in 0..store.limits.attempts {
                let snapshot = store.write_snapshot().await?;
                let previous = snapshot.get(&key).await.map_err(public_error)?;
                // Match Spin's default backend's little-endian representation.
                let current = previous
                    .as_deref()
                    .map(|bytes| {
                        <[u8; 8]>::try_from(bytes)
                            .map(i64::from_le_bytes)
                            .map_err(|_| other("object-log value is not an i64"))
                    })
                    .transpose()?
                    .unwrap_or(0);
                let next = current
                    .checked_add(delta)
                    .ok_or_else(|| other("object-log integer overflow"))?;
                let command = KvCommand::Set {
                    key: key.clone(),
                    value: Bytes::copy_from_slice(&next.to_le_bytes()),
                };
                // Even delta=0 creates an absent key, as required by WASI atomics.
                if store.publish(&snapshot, &[command]).await? {
                    return Ok(next);
                }
                if attempt + 1 < store.limits.attempts {
                    back_off(attempt).await;
                }
            }
            Err(other("object-log contention limit"))
        })
        .await
    }

    async fn new_compare_and_swap(
        &self,
        bucket_rep: u32,
        key: &str,
    ) -> Result<Arc<dyn Cas>, Error> {
        let key = self.key(key)?;
        let generation = self
            .run(|store| async move { Ok(store.write_snapshot().await?.view().generation()) })
            .await?;
        Ok(Arc::new(CompareSwap {
            store: self.clone(),
            key,
            bucket_rep,
            generation: Mutex::new(Some(generation)),
        }))
    }
}

struct CompareSwap {
    store: BackendStore,
    key: Bytes,
    bucket_rep: u32,
    generation: Mutex<Option<u64>>,
}

#[async_trait::async_trait]
impl Cas for CompareSwap {
    async fn current(&self, max_result_bytes: usize) -> Result<Option<Vec<u8>>, Error> {
        let key = self.key.clone();
        let mut observed = self.generation.lock().await;
        if observed.is_none() {
            return Err(other("CAS handle already consumed"));
        }
        let (generation, value) = self
            .store
            .run(move |store| async move {
                let snapshot = store.write_snapshot().await?;
                let value = snapshot.get(&key).await.map_err(public_error)?;
                if std::mem::size_of::<Option<Vec<u8>>>() + value.as_ref().map_or(0, Bytes::len)
                    > max_result_bytes.min(store.limits.response_bytes)
                {
                    return Err(other("object-log response limit"));
                }
                Ok((snapshot.view().generation(), value.map(|v| v.to_vec())))
            })
            .await?;
        *observed = Some(generation);
        Ok(value)
    }

    async fn swap(&self, value: Vec<u8>) -> Result<(), SwapError> {
        let value = self
            .store
            .value(&value)
            .map_err(|e| SwapError::Other(e.to_string()))?;
        let generation = self
            .generation
            .lock()
            .await
            .take()
            .ok_or_else(|| SwapError::Other("CAS handle already consumed".into()))?;
        let key = self.key.clone();
        // Publish against the exact observed log generation, preventing ABA as
        // well as insertion races. Unrelated writes can conservatively conflict.
        let committed = self
            .store
            .run(move |store| async move {
                let snapshot = store.snapshot().await?;
                if snapshot.view().generation() != generation {
                    return Ok(false);
                }
                store
                    .publish(&snapshot, &[KvCommand::Set { key, value }])
                    .await
            })
            .await
            .map_err(|e| SwapError::Other(e.to_string()))?;
        if committed {
            Ok(())
        } else {
            Err(SwapError::CasFailed("store changed since current".into()))
        }
    }
    async fn bucket_rep(&self) -> u32 {
        self.bucket_rep
    }
    async fn key(&self) -> String {
        String::from_utf8_lossy(&self.key).into_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_log::{
        Log, LogId, Options, ValidatedBackend,
        sim::{Failure, FailurePhase, FaultStore, Operation},
    };
    use object_store::{ObjectStore, memory::InMemory, path::Path};

    #[tokio::test]
    async fn group_storage_failure_does_not_fan_out() -> Result<(), Box<dyn std::error::Error>> {
        let faults = Arc::new(FaultStore::new(InMemory::new()));
        let backend = ValidatedBackend::new(
            faults.clone() as Arc<dyn ObjectStore>,
            Path::from("group-fault"),
        )
        .await?;
        let log = Log::open(&backend, &LogId::new("store")?, Options::default()).await?;
        let kv = KvStore::new(log, HostLimits::default().kv());
        faults.reset();
        faults.schedule(Failure {
            operation: Operation::Put,
            occurrence: 1,
            phase: FailurePhase::Before,
        });
        let total = Arc::new(AtomicUsize::new(4));
        let (first_done, first) = oneshot::channel();
        let (second_done, second) = oneshot::channel();
        let job = |key: &'static [u8], value: &'static [u8], done| WriteJob {
            commands: vec![KvCommand::Set {
                key: Bytes::from_static(key),
                value: Bytes::from_static(value),
            }],
            input: InputCharge {
                bytes: 2,
                total: total.clone(),
            },
            admitted: Instant::now(),
            done,
        };
        Owner::publish_group(
            &kv,
            HostLimits::default(),
            vec![job(b"a", b"1", first_done), job(b"b", b"2", second_done)],
        )
        .await;
        assert!(first.await?.is_err());
        assert!(second.await?.is_err());
        assert_eq!(kv.snapshot().await?.get(b"a").await?, None);
        assert_eq!(kv.snapshot().await?.get(b"b").await?, None);
        assert_eq!(faults.metrics().operation(Operation::Put).requests, 1);
        assert_eq!(total.load(Ordering::Relaxed), 0);
        Ok(())
    }
}
