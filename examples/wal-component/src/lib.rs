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
    transport: transport::Transport,
) -> object_store::aws::AmazonS3Builder {
    let mut builder = object_store::aws::AmazonS3Builder::new()
        .with_endpoint(settings.endpoint.as_str())
        .with_bucket_name(settings.bucket.as_str())
        .with_region(settings.region.as_str())
        .with_access_key_id(settings.access_key.as_str())
        .with_secret_access_key(settings.secret_key.as_str())
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
    if let Some(token) = settings.session_token.as_deref() {
        builder = builder.with_token(token);
    }
    builder
}

fn failure(error: object_log::Error) -> Failure {
    match error {
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
    fn collect(&self) -> Result<CollectionResult, Failure> {
        spin_executor::run(maintenance::collect(self))
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
        spin_executor::run(async {
            let transport = transport::Transport::default();
            let store = s3_builder(&settings, transport.clone())
                .build()
                .map_err(|error| Failure::Other(error.to_string()))?;
            let backend = object_log::ValidatedBackend::new(
                Arc::new(store),
                object_store::path::Path::from(settings.prefix),
            )
            .await
            .map_err(failure)?;
            let log = Log::open(
                &backend,
                &object_log::LogId::new(settings.log_id).map_err(failure)?,
                object_log::Options {
                    max_object_bytes: 2 * 1024 * 1024,
                    max_collection_objects: usize::try_from(settings.max_collection_objects)
                        .map_err(|_| Failure::Other("invalid collection object limit".into()))?,
                    ..Default::default()
                },
            )
            .await
            .map_err(failure)?;
            let view = log.load().await.map_err(failure)?;
            Ok(Session::new(SessionState {
                log,
                view: RefCell::new(view),
                transport,
            }))
        })
    }
}

#[cfg(test)]
mod credential_tests {
    use super::*;
    use object_store::aws::AmazonS3ConfigKey;

    fn settings(session_token: Option<&str>) -> Config {
        Config {
            endpoint: "https://s3.us-west-2.amazonaws.com".into(),
            bucket: "qualification".into(),
            region: "us-west-2".into(),
            access_key: "temporary-access-key".into(),
            secret_key: "temporary-secret-key".into(),
            session_token: session_token.map(str::to_owned),
            prefix: "isolated-prefix".into(),
            log_id: "repo-sha1".into(),
            max_collection_objects: 100_000,
        }
    }

    #[test]
    fn supplies_temporary_credential_token_to_s3_signing() {
        let builder = s3_builder(
            &settings(Some("temporary-session-token")),
            transport::Transport::default(),
        );
        assert_eq!(
            builder.get_config_value(&AmazonS3ConfigKey::Token),
            Some("temporary-session-token".into())
        );
    }

    #[test]
    fn leaves_session_token_unset_for_long_lived_credentials() {
        let builder = s3_builder(&settings(None), transport::Transport::default());
        assert_eq!(builder.get_config_value(&AmazonS3ConfigKey::Token), None);
    }
}
export!(Component);

#[cfg(test)]
mod maintenance_tests;
