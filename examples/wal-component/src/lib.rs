//! Feasibility binding of the existing WAL; no Git rules or new authority.
use bytes::Bytes;
use exports::object_log::storage::wal::*;
use object_log::{
    CommitStatus, Log, Materializer, RetentionId, RetentionStatus, StagedObject, View,
};
use std::cell::RefCell;
use std::sync::Arc;
mod maintenance;
mod transport;
wit_bindgen::generate!({ path: "wit", world: "storage" });

struct Component;
struct SessionState {
    log: Log,
    view: RefCell<View>,
    transport: transport::Transport,
}
struct CandidateState {
    log: Log,
    prepared: object_log::PreparedCommit,
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
        .with_virtual_hosted_style_request(false)
        .with_disable_bulk_delete(false)
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
            builder = builder
                .with_metadata_endpoint(INSTANCE_METADATA_ENDPOINT)
                .with_config(
                    object_store::aws::AmazonS3ConfigKey::ImdsV1Fallback,
                    "false",
                );
        }
    }
    Ok(builder)
}

#[cfg(not(feature = "test-imds"))]
const INSTANCE_METADATA_ENDPOINT: &str = "http://169.254.169.254";
// The opt-in composed test uses only this loopback fixture, never caller input.
#[cfg(feature = "test-imds")]
const INSTANCE_METADATA_ENDPOINT: &str = "http://127.0.0.1:19092";

fn failure(error: object_log::Error) -> Failure {
    match error {
        object_log::Error::LogNotFound => Failure::Missing,
        object_log::Error::ViewExpired => Failure::Expired,
        object_log::Error::LimitExceeded(limit) => Failure::Limit(limit.into()),
        error => Failure::Other(error.to_string()),
    }
}
fn entry((data, objects): (Bytes, Vec<StagedObject>)) -> Entry {
    Entry {
        data: data.into(),
        objects: objects.into_iter().map(Object::new).collect(),
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
// Each record represents a complete state. Delta consumers must fold differently.
struct LatestCompleteState;
impl Materializer for LatestCompleteState {
    type State = Option<(Bytes, Vec<StagedObject>)>;
    type Error = std::convert::Infallible;
    fn empty(&self) -> Self::State {
        None
    }
    fn restore(&self, data: &[u8], objects: &[StagedObject]) -> Result<Self::State, Self::Error> {
        Ok(Some((Bytes::copy_from_slice(data), objects.to_vec())))
    }
    fn apply(
        &self,
        state: &mut Self::State,
        data: &[u8],
        objects: &[StagedObject],
    ) -> Result<(), Self::Error> {
        *state = self.restore(data, objects)?;
        Ok(())
    }
}
struct WriterState(RefCell<Option<object_log::ByteWriter>>);
struct ReaderState(RefCell<object_log::ByteReader>);
impl GuestByteWriter for WriterState {
    fn write(&self, data: Vec<u8>) -> Result<(), Failure> {
        let mut writer = self.0.borrow_mut();
        let writer = writer
            .as_mut()
            .ok_or_else(|| Failure::Other("closed byte writer".into()))?;
        spin_executor::run(writer.write(&data)).map_err(failure)
    }
    fn finish(&self) -> Result<Object, Failure> {
        let writer = self
            .0
            .borrow_mut()
            .take()
            .ok_or_else(|| Failure::Other("closed byte writer".into()))?;
        spin_executor::run(writer.finish())
            .map(Object::new)
            .map_err(failure)
    }
}
impl GuestByteReader for ReaderState {
    fn length(&self) -> u64 {
        self.0.borrow().len()
    }
    fn read_at(&self, offset: u64, max_len: u32) -> Result<Vec<u8>, Failure> {
        spin_executor::run(self.0.borrow_mut().read_at(offset, max_len as usize))
            .map(|bytes| bytes.to_vec())
            .map_err(failure)
    }
}
impl GuestObject for StagedObject {}
impl GuestSession for SessionState {
    fn has_active_collection(&self) -> bool {
        self.view.borrow().collection_plan_bytes().is_some()
    }
    fn retain(&self, id: Vec<u8>) -> Result<RetentionState, Failure> {
        let view = self.current_view();
        let status =
            spin_executor::run(self.log.retain(&view, retention_id(id)?)).map_err(failure)?;
        Ok(self.accept_retention(status))
    }
    fn release_retention(&self, id: Vec<u8>) -> Result<RetentionState, Failure> {
        let view = self.current_view();
        let status = spin_executor::run(self.log.release_retention(&view, retention_id(id)?))
            .map_err(failure)?;
        Ok(self.accept_retention(status))
    }
    fn clear_retentions_after_drain(&self) -> Result<RetentionState, Failure> {
        let view = self.current_view();
        let status =
            spin_executor::run(self.log.clear_retentions_after_drain(&view)).map_err(failure)?;
        Ok(self.accept_retention(status))
    }
    fn write_bytes(&self) -> Result<ByteWriter, Failure> {
        let view = self.view.borrow();
        self.log
            .byte_writer(&view)
            .map(|writer| ByteWriter::new(WriterState(RefCell::new(Some(writer)))))
            .map_err(failure)
    }
    fn open_bytes(&self, value: ObjectBorrow<'_>) -> Result<ByteReader, Failure> {
        let value = value.get::<StagedObject>();
        let view = self.view.borrow().clone();
        spin_executor::run(self.log.open_bytes(&view, value.reference()))
            .map(|reader| ByteReader::new(ReaderState(RefCell::new(reader))))
            .map_err(failure)
    }
    fn checkpoint(
        &self,
        data: Vec<u8>,
        roots: Vec<ObjectBorrow<'_>>,
    ) -> Result<MaintenanceState, Failure> {
        spin_executor::run(maintenance::checkpoint(self, data, proofs(&roots)))
    }
    fn collect(&self, max_candidates: u64) -> Result<CollectionResult, Failure> {
        let max_candidates = usize::try_from(max_candidates)
            .map_err(|_| Failure::Limit("collection candidate objects".into()))?;
        spin_executor::run(maintenance::collect(self, max_candidates))
    }
    fn usage(&self) -> Usage {
        let (calls, bytes) = self.transport.usage();
        Usage { calls, bytes }
    }
    fn refresh(&self) -> Result<Session, Failure> {
        let view = spin_executor::run(self.log.load()).map_err(failure)?;
        Ok(Session::new(Self {
            log: self.log.clone(),
            view: RefCell::new(view),
            transport: self.transport.clone(),
        }))
    }

    fn latest_complete_state(&self) -> Result<RecoveredState, Failure> {
        let view = self.current_view();
        spin_executor::run(async {
            object_log::materialize(&self.log, view, &LatestCompleteState)
                .await
                .map(|value| RecoveredState {
                    tail_entries: value.view().tail().len() as u64,
                    latest: value.into_parts().1.map(entry),
                })
                .map_err(|error| match error {
                    object_log::MaterializeError::Log(error) => failure(error),
                    object_log::MaterializeError::State(never) => match never {},
                })
        })
    }
    fn read_node(&self, value: ObjectBorrow<'_>) -> Result<Entry, Failure> {
        let value = value.get::<StagedObject>();
        let view = self.current_view();
        spin_executor::run(self.log.read_staged_node(&view, value))
            .map(entry)
            .map_err(failure)
    }
    fn put_node(&self, data: Vec<u8>, children: Vec<ObjectBorrow<'_>>) -> Result<Object, Failure> {
        let view = self.current_view();
        spin_executor::run(
            self.log
                .put_node(&view, Bytes::from(data), proofs(&children)),
        )
        .map(Object::new)
        .map_err(failure)
    }
    fn prepare(&self, data: Vec<u8>, roots: Vec<ObjectBorrow<'_>>) -> Result<Candidate, Failure> {
        let view = self.current_view();
        let prepared = self
            .log
            .prepare(
                &view,
                object_log::TransactionId::new(),
                Bytes::from(data),
                Bytes::new(),
                proofs(&roots),
            )
            .map_err(failure)?;
        Ok(Candidate::new(CandidateState {
            log: self.log.clone(),
            prepared,
        }))
    }
}
impl GuestCandidate for CandidateState {
    fn publish(&self) -> Result<Outcome, Failure> {
        match spin_executor::run(self.log.commit(self.prepared.clone())).map_err(failure)? {
            CommitStatus::Committed(_) => Ok(Outcome::Committed),
            CommitStatus::Conflict(_) => Ok(Outcome::Conflict),
            CommitStatus::Pending(pending) => Ok(Outcome::Pending(
                pending.recovery_token().map_err(failure)?.to_vec(),
            )),
        }
    }
}
impl Guest for Component {
    type ByteWriter = WriterState;
    type ByteReader = ReaderState;
    type Candidate = CandidateState;
    type Session = SessionState;
    type Object = StagedObject;
    fn open(settings: Config) -> Result<Session, Failure> {
        open_session(settings, true)
    }
    fn open_existing(settings: Config) -> Result<Session, Failure> {
        open_session(settings, false)
    }
}

fn open_session(settings: Config, create: bool) -> Result<Session, Failure> {
    spin_executor::run(async {
        let transport = transport::Transport::default();
        let store = s3_builder(&settings, transport.clone())?
            .build()
            .map_err(|error| Failure::Other(error.to_string()))?;
        let backend = object_log::ValidatedBackend::new(
            Arc::new(store),
            object_store::path::Path::from(settings.prefix),
        )
        .await
        .map_err(failure)?;
        let log_id = object_log::LogId::new(settings.log_id).map_err(failure)?;
        let options = object_log::Options {
            max_object_bytes: 2 * 1024 * 1024,
            max_collection_objects: usize::try_from(settings.max_collection_objects)
                .map_err(|_| Failure::Other("invalid collection object limit".into()))?,
            ..Default::default()
        };
        let log = if create {
            Log::open(&backend, &log_id, options).await
        } else {
            Log::open_existing(&backend, &log_id, options).await
        }
        .map_err(failure)?;
        let view = log.load().await.map_err(failure)?;
        Ok(Session::new(SessionState {
            log,
            view: RefCell::new(view),
            transport,
        }))
    })
}

#[cfg(test)]
mod credential_tests;
export!(Component);

#[cfg(test)]
mod maintenance_tests;
