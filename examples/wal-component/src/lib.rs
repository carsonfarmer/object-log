//! Feasibility binding of the existing WAL; no Git rules or new authority.
use bytes::Bytes;
use exports::object_log::storage::wal::*;
use object_log::{CommitStatus, Log, Materializer, StagedObject, View};
use std::cell::RefCell;
use std::sync::Arc;
mod maintenance;
mod transport;
wit_bindgen::generate!({ path: "wit", world: "storage" });

struct Component;
struct SessionState {
    log: Log,
    view: View,
    transport: transport::Transport,
}
struct CandidateState {
    log: Log,
    prepared: object_log::PreparedCommit,
}
struct ObjectState {
    staged: StagedObject,
}

fn failure(error: object_log::Error) -> Failure {
    match error {
        object_log::Error::ViewExpired => Failure::Expired,
        error => Failure::Other(error.to_string()),
    }
}
fn entry(data: &[u8], objects: &[StagedObject]) -> Entry {
    Entry {
        snapshot: false,
        data: data.to_vec(),
        objects: objects
            .iter()
            .cloned()
            .map(|staged| Object::new(ObjectState { staged }))
            .collect(),
    }
}
fn proofs(objects: &[ObjectBorrow<'_>]) -> Vec<StagedObject> {
    objects
        .iter()
        .map(|object| object.get::<ObjectState>().staged.clone())
        .collect()
}
struct Records;
impl Materializer for Records {
    type State = Vec<Entry>;
    type Error = std::convert::Infallible;
    fn empty(&self) -> Self::State {
        Vec::new()
    }
    fn restore(&self, data: &[u8], objects: &[StagedObject]) -> Result<Self::State, Self::Error> {
        let mut snapshot = entry(data, objects);
        snapshot.snapshot = true;
        Ok(vec![snapshot])
    }
    fn apply(
        &self,
        state: &mut Self::State,
        data: &[u8],
        objects: &[StagedObject],
    ) -> Result<(), Self::Error> {
        state.push(entry(data, objects));
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
            .map(|staged| Object::new(ObjectState { staged }))
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
impl GuestObject for ObjectState {}
impl GuestSession for SessionState {
    fn write_bytes(&self) -> Result<ByteWriter, Failure> {
        self.log
            .byte_writer(&self.view)
            .map(|writer| ByteWriter::new(WriterState(RefCell::new(Some(writer)))))
            .map_err(failure)
    }
    fn open_bytes(&self, value: ObjectBorrow<'_>) -> Result<ByteReader, Failure> {
        let value = value.get::<ObjectState>();
        spin_executor::run(self.log.open_bytes(&self.view, value.staged.reference()))
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
            view,
            transport: self.transport.clone(),
        }))
    }

    fn records(&self) -> Result<Vec<Entry>, Failure> {
        spin_executor::run(async {
            object_log::materialize(&self.log, self.view.clone(), &Records)
                .await
                .map(|value| value.into_parts().1)
                .map_err(|error| match error {
                    object_log::MaterializeError::Log(error) => failure(error),
                    object_log::MaterializeError::State(never) => match never {},
                })
        })
    }
    fn read_node(&self, value: ObjectBorrow<'_>) -> Result<Entry, Failure> {
        let value = value.get::<ObjectState>();
        spin_executor::run(self.log.read_staged_node(&self.view, &value.staged))
            .map(|(data, children)| entry(&data, &children))
            .map_err(failure)
    }
    fn put_node(&self, data: Vec<u8>, children: Vec<ObjectBorrow<'_>>) -> Result<Object, Failure> {
        spin_executor::run(
            self.log
                .put_node(&self.view, Bytes::from(data), proofs(&children)),
        )
        .map(|staged| Object::new(ObjectState { staged }))
        .map_err(failure)
    }
    fn prepare(&self, data: Vec<u8>, roots: Vec<ObjectBorrow<'_>>) -> Result<Candidate, Failure> {
        let prepared = self
            .log
            .prepare(
                &self.view,
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
    type Object = ObjectState;
    fn open(settings: Config) -> Result<Session, Failure> {
        spin_executor::run(async {
            let transport = transport::Transport::default();
            let store = object_store::aws::AmazonS3Builder::new()
                .with_endpoint(&settings.endpoint)
                .with_bucket_name(settings.bucket)
                .with_region(settings.region)
                .with_access_key_id(settings.access_key)
                .with_secret_access_key(settings.secret_key)
                .with_virtual_hosted_style_request(false)
                .with_disable_bulk_delete(false)
                .with_client_options(
                    object_store::ClientOptions::new()
                        .with_allow_http(settings.endpoint.starts_with("http://"))
                        .with_timeout_disabled()
                        .with_connect_timeout(std::time::Duration::from_secs(5))
                        .with_read_timeout(std::time::Duration::from_secs(30)),
                )
                .with_http_connector(transport.clone())
                .with_crypto_provider(Arc::new(transport::Crypto))
                .with_retry(object_store::RetryConfig {
                    max_retries: 0,
                    ..Default::default()
                })
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
                    ..Default::default()
                },
            )
            .await
            .map_err(failure)?;
            let view = log.load().await.map_err(failure)?;
            Ok(Session::new(SessionState {
                log,
                view,
                transport,
            }))
        })
    }
}
export!(Component);

#[cfg(test)]
mod maintenance_tests;
