//! Feasibility binding of the existing WAL; no Git rules or new authority.
use bytes::Bytes;
use exports::object_log::storage::wal::*;
use object_log::{CommitStatus, Log, Materializer, Resolution, StagedObject, View};
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
impl GuestObject for ObjectState {}
impl GuestSession for SessionState {
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
    fn read(&self, value: ObjectBorrow<'_>) -> Result<Vec<u8>, Failure> {
        let value = value.get::<ObjectState>();
        spin_executor::run(self.log.read_object(&self.view, value.staged.reference()))
            .map(|bytes| bytes.to_vec())
            .map_err(failure)
    }
    fn read_node(&self, value: ObjectBorrow<'_>) -> Result<Entry, Failure> {
        let value = value.get::<ObjectState>();
        spin_executor::run(self.log.read_staged_node(&self.view, &value.staged))
            .map(|(data, children)| entry(&data, &children))
            .map_err(failure)
    }
    fn put(&self, data: Vec<u8>) -> Result<Object, Failure> {
        spin_executor::run(self.log.put_object(&self.view, Bytes::from(data)))
            .map(|staged| Object::new(ObjectState { staged }))
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
    fn reprove(&self, values: Vec<ObjectBorrow<'_>>) -> Result<Vec<Object>, Failure> {
        let refs = values
            .iter()
            .map(|value| value.get::<ObjectState>().staged.reference().clone())
            .collect();
        spin_executor::run(self.log.stage_objects(&self.view, refs))
            .map(|objects| {
                objects
                    .into_iter()
                    .map(|staged| Object::new(ObjectState { staged }))
                    .collect()
            })
            .map_err(failure)
    }
    fn resume(&self, token: Vec<u8>) -> Result<Outcome, Failure> {
        match spin_executor::run(self.log.resume(&token)).map_err(failure)? {
            Resolution::Committed(_) => Ok(Outcome::Committed),
            Resolution::NotCommitted(_) => Ok(Outcome::Conflict),
            Resolution::Expired(_) => Ok(Outcome::Expired),
            Resolution::StillPending(pending) => Ok(Outcome::Pending(
                pending.recovery_token().map_err(failure)?.to_vec(),
            )),
        }
    }
}
impl GuestCandidate for CandidateState {
    fn token(&self) -> Result<Vec<u8>, Failure> {
        self.prepared
            .recovery_token()
            .map(|bytes| bytes.to_vec())
            .map_err(failure)
    }
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
