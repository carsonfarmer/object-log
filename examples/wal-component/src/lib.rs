//! Reusable WASIp2 binding for object-log with S3 transport.
use bytes::Bytes;
use exports::object_log::storage::wal::*;
use object_log::{
    CheckpointResolution, CheckpointStatus, CommitStatus, HistoryItem as LogHistoryItem, Log,
    Resolution as LogResolution, RetentionId, RetentionStatus, StagedObject, TransactionId, View,
};
use std::cell::{Ref, RefCell};
use std::sync::Arc;
mod executor;
mod maintenance;
mod transport;
wit_bindgen::generate!({ path: "wit", world: "storage" });

struct Component;
struct SessionState {
    log: Log,
    view: RefCell<View>,
    transport: transport::Transport,
}
// Resource state shares its log handle while each value retains its own lifecycle.
struct LogState<T> {
    log: Log,
    value: RefCell<T>,
}
type CandidateState = LogState<Option<object_log::PreparedCommit>>;
type RecoveryState = LogState<object_log::HistoryCursor>;

impl<T> LogState<Option<T>> {
    fn live(&self, closed: &str) -> Result<Ref<'_, T>, Failure> {
        Ref::filter_map(self.value.borrow(), Option::as_ref)
            .map_err(|_| Failure::Other(closed.into()))
    }
}
impl SessionState {
    fn current_view(&self) -> View {
        self.view.borrow().clone()
    }

    fn accept_retention(&self, status: RetentionStatus) -> RetentionState {
        let (state, view) = match status {
            RetentionStatus::Applied(view) => (RetentionState::Applied, view),
            RetentionStatus::ActiveCollection(view) => (RetentionState::ActiveCollection, view),
            RetentionStatus::Conflict(view) => (RetentionState::Conflict, view),
            RetentionStatus::Pending => return RetentionState::Pending,
        };
        self.view.replace(view);
        state
    }
}

fn s3_builder(
    settings: &Config,
    transport: impl object_store::client::HttpConnector,
) -> Result<object_store::aws::AmazonS3Builder, Failure> {
    if settings.region.is_empty() {
        return Err(Failure::Other("S3 region is required".into()));
    }
    let mut builder = object_store::aws::AmazonS3Builder::new()
        .with_endpoint(settings.endpoint.as_str())
        .with_bucket_name(settings.bucket.as_str())
        .with_region(settings.region.as_str())
        .with_client_options(
            object_store::ClientOptions::new()
                .with_allow_http(settings.endpoint.starts_with("http://"))
                .with_timeout_disabled()
                .with_connect_timeout(std::time::Duration::from_secs(5))
                .with_read_timeout(std::time::Duration::from_secs(30)),
        )
        .with_http_connector(transport)
        .with_crypto_provider(Arc::new(transport::Crypto))
        .with_retry(object_store::RetryConfig {
            max_retries: 0,
            ..Default::default()
        });
    match settings.credential_mode {
        CredentialMode::StaticCredentials => {
            if settings.access_key.is_empty()
                || settings.secret_key.is_empty()
                || settings.session_token.as_deref() == Some("")
            {
                return Err(Failure::Other(
                    "static S3 credentials are incomplete".into(),
                ));
            }
            builder = builder
                .with_access_key_id(&settings.access_key)
                .with_secret_access_key(&settings.secret_key);
            if let Some(token) = &settings.session_token {
                builder = builder.with_token(token);
            }
        }
        CredentialMode::InstanceRole => {
            if !settings.access_key.is_empty()
                || !settings.secret_key.is_empty()
                || settings.session_token.is_some()
            {
                return Err(Failure::Other(
                    "instance-role credentials cannot include static keys or a token".into(),
                ));
            }
            // No static setters: object_store obtains and refreshes IMDSv2
            // credentials through the same injected WASI HTTP connector.
            builder = builder.with_metadata_endpoint(INSTANCE_METADATA_ENDPOINT);
        }
    }
    Ok(builder)
}

#[cfg(not(feature = "test-imds"))]
const INSTANCE_METADATA_ENDPOINT: &str = "http://169.254.169.254";
// The opt-in composed test uses only this loopback fixture, never caller input.
#[cfg(feature = "test-imds")]
const INSTANCE_METADATA_ENDPOINT: &str = "http://127.0.0.1:19092";

impl From<object_log::Error> for Failure {
    fn from(error: object_log::Error) -> Self {
        match error {
            object_log::Error::LogNotFound => Failure::Missing,
            object_log::Error::ViewExpired => Failure::Expired,
            object_log::Error::LimitExceeded(limit) => Failure::Limit(limit.into()),
            error => Failure::Other(error.to_string()),
        }
    }
}
fn proofs(objects: &[ObjectBorrow<'_>]) -> Vec<StagedObject> {
    objects
        .iter()
        .map(|object| object.get::<StagedObject>().clone())
        .collect()
}
fn retention_id(value: Vec<u8>) -> Result<RetentionId, Failure> {
    uuid::Uuid::from_slice(&value)
        .map(RetentionId::from_uuid)
        .map_err(|_| Failure::Other("retention ID must contain 16 bytes".into()))
}

struct WriterState(RefCell<Option<object_log::ByteWriter>>);
struct ReaderState(RefCell<object_log::ByteReader>);
impl GuestByteWriter for WriterState {
    fn write(&self, data: Vec<u8>) -> Result<(), Failure> {
        let mut writer = self.0.borrow_mut();
        let writer = writer
            .as_mut()
            .ok_or_else(|| Failure::Other("closed byte writer".into()))?;
        Ok(executor::run(writer.write(&data))?)
    }
    fn finish(&self) -> Result<Object, Failure> {
        let writer = self
            .0
            .borrow_mut()
            .take()
            .ok_or_else(|| Failure::Other("closed byte writer".into()))?;
        Ok(Object::new(executor::run(writer.finish())?))
    }
}
impl GuestByteReader for ReaderState {
    fn length(&self) -> u64 {
        self.0.borrow().len()
    }
    fn read_at(&self, offset: u64, max_len: u32) -> Result<Vec<u8>, Failure> {
        Ok(executor::run(self.0.borrow_mut().read_at(offset, max_len as usize))?.to_vec())
    }
}
impl GuestObject for StagedObject {}
impl GuestSession for SessionState {
    fn has_active_collection(&self) -> bool {
        self.view.borrow().collection_plan_bytes().is_some()
    }
    fn retain(&self, id: Vec<u8>) -> Result<RetentionState, Failure> {
        let view = self.current_view();
        let status = executor::run(self.log.retain(&view, retention_id(id)?))?;
        Ok(self.accept_retention(status))
    }
    fn release_retention(&self, id: Vec<u8>) -> Result<RetentionState, Failure> {
        let view = self.current_view();
        let status = executor::run(self.log.release_retention(&view, retention_id(id)?))?;
        Ok(self.accept_retention(status))
    }
    fn clear_retentions_after_drain(&self) -> Result<RetentionState, Failure> {
        let view = self.current_view();
        let status = executor::run(self.log.clear_retentions_after_drain(&view))?;
        Ok(self.accept_retention(status))
    }
    fn collect(&self, max_candidates: u64) -> Result<CollectionResult, Failure> {
        let max_candidates = usize::try_from(max_candidates)
            .map_err(|_| Failure::Limit("collection candidate objects".into()))?;
        executor::run(maintenance::collect(self, max_candidates))
    }
    fn usage(&self) -> Usage {
        let (calls, bytes) = self.transport.usage();
        Usage { calls, bytes }
    }
    fn refresh(&self) -> Result<Session, Failure> {
        let current = self.current_view();
        let view = executor::run(self.log.refresh(&current))?.unwrap_or(current);
        Ok(Session::new(Self {
            log: self.log.clone(),
            view: RefCell::new(view),
            transport: self.transport.clone(),
        }))
    }

    fn recover(&self) -> Result<Recovery, Failure> {
        let view = self.current_view();
        Ok(Recovery::new(RecoveryState {
            log: self.log.clone(),
            value: RefCell::new(object_log::history(&self.log, view)?),
        }))
    }

    fn resume(&self, token: Vec<u8>) -> Result<Resolution, Failure> {
        let (resolution, view) = match executor::run(self.log.resume(&token))? {
            LogResolution::Committed(view) => (Resolution::Committed, view),
            LogResolution::NotCommitted(view) => (Resolution::NotCommitted, view),
            LogResolution::Expired(view) => (Resolution::Expired, view),
            LogResolution::StillPending(pending) => {
                return Ok(Resolution::StillPending(pending.recovery_token()?.to_vec()));
            }
        };
        self.view.replace(view);
        Ok(resolution)
    }
}
impl RecoveryState {
    fn bound_view(&self) -> Ref<'_, View> {
        Ref::map(self.value.borrow(), object_log::HistoryCursor::view)
    }

    fn latest_record(&self) -> Result<(usize, Option<LogHistoryItem>), Failure> {
        let view = self.bound_view();
        let entries = view.tail().len();
        let item = if let Some(index) = entries.checked_sub(1) {
            Some(LogHistoryItem::Commit(executor::run(
                object_log::tail_record(&self.log, &view, index),
            )?))
        } else {
            executor::run(object_log::history(&self.log, view.clone())?.next())?
        };
        Ok((entries, item))
    }
}

fn history_item(item: LogHistoryItem) -> HistoryItem {
    match item {
        LogHistoryItem::Checkpoint(authenticated) => {
            let (record, objects) = authenticated.into_parts();
            HistoryItem::Checkpoint(Entry {
                data: record.snapshot().to_vec(),
                objects: objects.into_iter().map(Object::new).collect(),
            })
        }
        LogHistoryItem::Commit(authenticated) => {
            let (record, objects) = authenticated.into_parts();
            let reference = record.reference();
            HistoryItem::Commit(CommitRecord {
                sequence: reference.sequence(),
                transaction_id: reference.transaction_id().as_uuid().as_bytes().to_vec(),
                operation: record.operation().to_vec(),
                recorded_result: record.result().to_vec(),
                objects: objects.into_iter().map(Object::new).collect(),
            })
        }
    }
}
impl GuestRecovery for RecoveryState {
    fn next(&self) -> Result<Option<HistoryItem>, Failure> {
        Ok(executor::run(self.value.borrow_mut().next())?.map(history_item))
    }

    fn latest(&self) -> Result<LatestRecord, Failure> {
        let (entries, item) = self.latest_record()?;
        Ok(LatestRecord {
            tail_entries: entries as u64,
            item: item.map(history_item),
        })
    }

    fn write_bytes(&self) -> Result<ByteWriter, Failure> {
        let view = self.bound_view();
        let writer = self.log.byte_writer(&view)?;
        Ok(ByteWriter::new(WriterState(RefCell::new(Some(writer)))))
    }

    fn open_bytes(&self, value: ObjectBorrow<'_>) -> Result<ByteReader, Failure> {
        let value = value.get::<StagedObject>();
        let view = self.bound_view();
        let reader = executor::run(self.log.open_bytes(&view, value.reference()))?;
        Ok(ByteReader::new(ReaderState(RefCell::new(reader))))
    }

    fn read_node(&self, value: ObjectBorrow<'_>) -> Result<Entry, Failure> {
        let value = value.get::<StagedObject>();
        let view = self.bound_view();
        let (data, objects) = executor::run(self.log.read_staged_node(&view, value))?;
        Ok(Entry {
            data: data.into(),
            objects: objects.into_iter().map(Object::new).collect(),
        })
    }
    fn put_node(&self, data: Vec<u8>, children: Vec<ObjectBorrow<'_>>) -> Result<Object, Failure> {
        let view = self.bound_view();
        Ok(Object::new(executor::run(self.log.put_node(
            &view,
            Bytes::from(data),
            proofs(&children),
        ))?))
    }
    fn prepare(
        &self,
        transaction_id: Vec<u8>,
        operation: Vec<u8>,
        result: Vec<u8>,
        roots: Vec<ObjectBorrow<'_>>,
    ) -> Result<Candidate, Failure> {
        let transaction_id = uuid::Uuid::from_slice(&transaction_id)
            .map(TransactionId::from_uuid)
            .map_err(|_| Failure::Other("transaction ID must contain 16 bytes".into()))?;
        while executor::run(self.value.borrow_mut().next())?.is_some() {}
        let view = self.bound_view();
        let prepared = self.log.prepare(
            &view,
            transaction_id,
            Bytes::from(operation),
            Bytes::from(result),
            proofs(&roots),
        )?;
        Ok(Candidate::new(CandidateState {
            log: self.log.clone(),
            value: RefCell::new(Some(prepared)),
        }))
    }

    fn checkpoint(
        &self,
        data: Vec<u8>,
        roots: Vec<ObjectBorrow<'_>>,
    ) -> Result<MaintenanceState, Failure> {
        let view = self.bound_view();
        executor::run(self.log.read_checkpoint(&view))?;
        let through = view
            .tail()
            .last()
            .ok_or_else(|| Failure::Other("checkpoint requires an active tail".into()))?;
        checkpoint_state(
            &self.log,
            executor::run(self.log.publish_checkpoint(
                &view,
                through,
                data.into(),
                proofs(&roots),
            ))?,
        )
    }
}
fn checkpoint_state(log: &Log, status: CheckpointStatus) -> Result<MaintenanceState, Failure> {
    match status {
        CheckpointStatus::Published(_) => Ok(MaintenanceState::Complete),
        CheckpointStatus::Conflict(_) => Ok(MaintenanceState::Conflict),
        CheckpointStatus::Pending(pending) => match executor::run(log.resolve_checkpoint(pending))?
        {
            CheckpointResolution::Published(_) => Ok(MaintenanceState::Complete),
            CheckpointResolution::NotPublished(_) => Ok(MaintenanceState::Conflict),
            CheckpointResolution::Expired(_) | CheckpointResolution::StillPending(_) => {
                Ok(MaintenanceState::Pending)
            }
        },
    }
}
impl GuestCandidate for CandidateState {
    fn recovery_token(&self) -> Result<Vec<u8>, Failure> {
        Ok(self.live("published candidate")?.recovery_token()?.to_vec())
    }

    fn publish(&self) -> Result<Outcome, Failure> {
        let prepared = self.live("published candidate")?.clone();
        let status = executor::run(self.log.commit(prepared))?;
        self.value.borrow_mut().take();
        match status {
            CommitStatus::Committed(_) => Ok(Outcome::Committed),
            CommitStatus::Conflict(_) => Ok(Outcome::Conflict),
            CommitStatus::Pending(pending) => {
                Ok(Outcome::Pending(pending.recovery_token()?.to_vec()))
            }
        }
    }
}
impl Guest for Component {
    type ByteWriter = WriterState;
    type ByteReader = ReaderState;
    type Candidate = CandidateState;
    type Recovery = RecoveryState;
    type Session = SessionState;
    type Object = StagedObject;
    fn open(settings: Config) -> Result<Session, Failure> {
        open_session(settings, true)
    }
    fn open_existing(settings: Config) -> Result<Session, Failure> {
        open_session(settings, false)
    }
    fn validate_backend(settings: Config) -> Result<(), Failure> {
        executor::run(async {
            let (store, _) = connect_store(&settings)?;
            object_log::ValidatedBackend::new(
                store,
                object_store::path::Path::from(settings.prefix),
            )
            .await?;
            Ok(())
        })
    }
}

fn connect_store(
    settings: &Config,
) -> Result<(Arc<dyn object_store::ObjectStore>, transport::Transport), Failure> {
    let transport = transport::Transport::new(
        settings.transport_limits.max_calls,
        settings.transport_limits.max_bytes,
    )
    .map_err(|error| Failure::Other(error.into()))?;
    let store = s3_builder(settings, transport.clone())?
        .build()
        .map_err(|error| Failure::Other(error.to_string()))?;
    Ok((Arc::new(store), transport))
}

fn open_session(settings: Config, create: bool) -> Result<Session, Failure> {
    executor::run(async {
        let (store, transport) = connect_store(&settings)?;
        let backend = object_log::ValidatedBackend::assume_validated(
            store,
            object_store::path::Path::from(settings.prefix),
        );
        let log_id = object_log::LogId::new(settings.log_id)?;
        let options = log_options(settings.log_limits)?;
        let (log, view) = if create {
            let log = Log::open(&backend, &log_id, options).await?;
            let view = log.load().await?;
            (log, view)
        } else {
            Log::open_existing_with_view(&backend, &log_id, options).await?
        };
        Ok(Session::new(SessionState {
            log,
            view: RefCell::new(view),
            transport,
        }))
    })
}

fn log_options(limits: LogLimits) -> Result<object_log::Options, Failure> {
    let limit =
        |value, name| usize::try_from(value).map_err(|_| Failure::Other(format!("invalid {name}")));
    Ok(object_log::Options {
        max_tail_entries: limit(limits.max_tail_entries, "tail entry limit")?,
        resolution_window: limit(limits.resolution_window, "resolution window")?,
        max_inline_operation_bytes: limit(
            limits.max_inline_operation_bytes,
            "inline operation limit",
        )?,
        max_inline_result_bytes: limit(limits.max_inline_result_bytes, "inline result limit")?,
        max_object_refs: limit(limits.max_object_refs, "object reference limit")?,
        max_object_bytes: limit(limits.max_object_bytes, "object byte limit")?,
        max_commit_bytes: limit(limits.max_commit_bytes, "commit byte limit")?,
        max_head_bytes: limit(limits.max_head_bytes, "head byte limit")?,
        max_checkpoint_bytes: limit(limits.max_checkpoint_bytes, "checkpoint byte limit")?,
        max_retention_ids: limit(limits.max_retention_ids, "retention limit")?,
        max_collection_objects: limit(limits.max_collection_objects, "collection object limit")?,
        max_collection_plan_bytes: limit(
            limits.max_collection_plan_bytes,
            "collection plan byte limit",
        )?,
    })
}

#[cfg(test)]
mod credential_tests;
export!(Component);

#[cfg(test)]
mod maintenance_tests;
