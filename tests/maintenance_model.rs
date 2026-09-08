#![cfg(feature = "test-util")]

use std::{collections::BTreeSet, sync::Arc};

use bytes::Bytes;
use futures::TryStreamExt;
use object_log::sim::{Failure, FailurePhase, FaultStore, Operation};
use object_log::{
    CheckpointStatus, CollectionFinish, CollectionStart, CommitStatus, Log, LogId, ObjectRef,
    Options, PreparedCommit, Resolution, StagedObject, TransactionId, ValidatedBackend,
};
use object_store::{ObjectStore, memory::InMemory, path::Path};

#[cfg(feature = "aws")]
mod support;

type TestResult = Result<(), Box<dyn std::error::Error>>;

// Model state comes from submitted commands and the chosen schedule. Reads and
// returned publication statuses are observations, never the history oracle.
#[tokio::test]
async fn seeded_stage_prepare_checkpoint_collection_and_crash_model() -> TestResult {
    maintenance_model(Arc::new(InMemory::new())).await
}

#[cfg(feature = "aws")]
#[tokio::test]
#[ignore = "requires local MinIO"]
async fn minio_maintenance_model() -> TestResult {
    maintenance_model(Arc::new(support::minio::build_minio()?)).await
}

async fn maintenance_model(store: Arc<dyn ObjectStore>) -> TestResult {
    for seed in [1_u64, 0x5eed, 0xcafe, 0xdead_beef] {
        let faults = FaultStore::new(Arc::clone(&store));
        let root = Path::from("maintenance-model");
        let backend = ValidatedBackend::new(Arc::new(faults.clone()), root.clone()).await?;
        let id = LogId::new(format!("seed-{seed}-{}", uuid::Uuid::new_v4().simple()))?;
        let log_scope = root.join("v1").join("logs").join(id.as_str());
        let scope = log_scope.clone().join("data");
        let mut log = Log::open(&backend, &id, Options::default()).await?;
        let mut oracle = MaintenanceOracle::default();
        let mut revision = 0;
        let mut staged: Option<(u64, StagedObject, String)> = None;
        let mut prepared: Option<(u64, u64, PreparedCommit, ObjectRef, String)> = None;
        let mut pending_tokens = Vec::new();
        let mut random = seed;
        let mut trace = Vec::new();
        for step in 0..128_u64 {
            let action = next_action(&mut random, step);
            trace.push(action);
            let view = log.load().await?;
            faults.reset();
            match action {
                0 => {
                    let object = log
                        .put_object(&view, Bytes::copy_from_slice(&step.to_le_bytes()))
                        .await?;
                    let key = created_key(&faults, "blobs")?;
                    staged = Some((step, object, key));
                }
                1 => {
                    if let Some((value, object, key)) = staged.take() {
                        let reference = object.reference().clone();
                        let candidate = log.prepare(
                            &view,
                            TransactionId::from_uuid(uuid::Uuid::from_u128(
                                (u128::from(seed) << 64) | u128::from(step),
                            )),
                            Bytes::copy_from_slice(&value.to_le_bytes()),
                            Bytes::new(),
                            vec![object],
                        )?;
                        prepared = Some((revision, value, candidate, reference, key));
                    }
                }
                2 => {
                    if let Some((observed, value, candidate, reference, key)) = prepared.take() {
                        let wins = observed == revision;
                        if let Some(token) =
                            publish(&log, &faults, candidate, wins, step % 2 == 0).await?
                        {
                            pending_tokens.push(token);
                        }
                        if wins {
                            oracle.history.push(value);
                            oracle.blobs.push(reference);
                            oracle.live_blobs.insert(key);
                            oracle.tail_keys.insert(created_key(&faults, "commits")?);
                            revision += 1;
                        }
                    }
                }
                3 => {
                    if oracle.checkpoint(&log, &view, &faults).await? {
                        revision += 1;
                    }
                }
                4 => {
                    // Discard process-local uncommitted work before sweeping its objects.
                    prepared = None;
                    staged = None;
                    if oracle
                        .collect(&log, &view, &faults, &scope, seed, &trace)
                        .await?
                    {
                        revision += 1;
                    }
                }
                5 => {
                    prepared = None;
                    staged = None;
                    log = Log::open_existing(&backend, &id, Options::default()).await?;
                }
                _ => resolve_pending(&log, &mut pending_tokens).await?,
            }
            if action != 4 {
                oracle.record_created_objects(&faults);
            }
            oracle.check(&log, seed, &trace).await?;
        }
        resolve_pending(&log, &mut pending_tokens).await?;
        oracle.check(&log, seed, &trace).await?;
        let objects = store.list(Some(&log_scope)).map_ok(|meta| meta.location);
        store
            .delete_stream(Box::pin(objects))
            .try_collect::<Vec<_>>()
            .await?;
    }
    Ok(())
}

async fn resolve_pending(log: &Log, tokens: &mut Vec<Bytes>) -> TestResult {
    for token in tokens.drain(..) {
        assert!(matches!(
            log.resume(&token).await?,
            Resolution::Committed(_)
        ));
    }
    Ok(())
}

async fn publish(
    log: &Log,
    faults: &FaultStore,
    candidate: PreparedCommit,
    wins: bool,
    lose_response: bool,
) -> Result<Option<Bytes>, Box<dyn std::error::Error>> {
    let token = candidate.recovery_token()?;
    if wins && lose_response {
        faults.schedule(Failure {
            operation: Operation::Put,
            occurrence: 2, // Immutable commit first, then the publishing head CAS.
            phase: FailurePhase::After,
        });
    }
    let outcome = log.commit(candidate).await?;
    if wins && lose_response {
        assert!(matches!(outcome, CommitStatus::Pending(_)));
        assert!(faults.pending_failures().is_empty());
        Ok(Some(token))
    } else {
        assert!(matches!(
            (wins, outcome),
            (true, CommitStatus::Committed(_)) | (false, CommitStatus::Conflict(_))
        ));
        Ok(None)
    }
}

#[derive(Default)]
struct MaintenanceOracle {
    history: Vec<u64>,
    blobs: Vec<ObjectRef>,
    live_blobs: BTreeSet<String>,
    tail_keys: BTreeSet<String>,
    checkpoint_key: Option<String>,
    all_keys: BTreeSet<String>,
    base: usize,
}

impl MaintenanceOracle {
    fn record_created_objects(&mut self, faults: &FaultStore) {
        self.all_keys.extend(
            faults
                .metrics()
                .events
                .iter()
                .filter(|event| event.operation == Operation::Put && event.path.contains("/data/"))
                .map(|event| event.path.clone()),
        );
    }

    async fn checkpoint(
        &mut self,
        log: &Log,
        view: &object_log::View,
        faults: &FaultStore,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        if self.history.len() == self.base {
            return Ok(false);
        }
        let roots = log.stage_objects(view, self.blobs.clone()).await?;
        let through = view.tail().last().ok_or("model expects nonempty tail")?;
        assert!(matches!(
            log.publish_checkpoint(view, through, encode_history(&self.history), roots)
                .await?,
            CheckpointStatus::Published(_)
        ));
        self.checkpoint_key = Some(created_key(faults, "checkpoints")?);
        self.base = self.history.len();
        self.tail_keys.clear();
        Ok(true)
    }

    async fn collect(
        &mut self,
        log: &Log,
        view: &object_log::View,
        faults: &FaultStore,
        scope: &Path,
        seed: u64,
        trace: &[u64],
    ) -> Result<bool, Box<dyn std::error::Error>> {
        let mut changed = false;
        let mut live = self
            .live_blobs
            .union(&self.tail_keys)
            .cloned()
            .collect::<BTreeSet<_>>();
        live.extend(self.checkpoint_key.iter().cloned());
        let dead = self.all_keys.difference(&live).count();
        match log.start_collection(view).await? {
            CollectionStart::Empty(report) => {
                assert_eq!(dead, 0, "seed {seed:x} trace {trace:?}");
                assert_eq!(report.candidate_count(), 0);
            }
            CollectionStart::Installed(fenced, report) => {
                assert_eq!(
                    report.candidate_count(),
                    dead,
                    "seed {seed:x} trace {trace:?}"
                );
                assert!(matches!(
                    log.resume_collection(&fenced).await?,
                    CollectionFinish::Complete(_, _)
                ));
                changed = true;
            }
            other => {
                return Err(format!(
                    "unexpected collection {other:?}; seed {seed:x} trace {trace:?}"
                )
                .into());
            }
        }
        let actual = faults
            .list(Some(scope))
            .map_ok(|m| m.location.to_string())
            .try_collect::<BTreeSet<_>>()
            .await?;
        assert_eq!(
            actual, live,
            "collection oracle; seed {seed:x} trace {trace:?}"
        );
        self.all_keys = live;
        Ok(changed)
    }

    async fn check(&self, log: &Log, seed: u64, trace: &[u64]) -> TestResult {
        let current = log.load().await?;
        let checkpoint = log.read_checkpoint(&current).await?;
        assert_eq!(
            checkpoint.as_ref().map(|c| c.snapshot().clone()),
            (self.base > 0).then(|| encode_history(&self.history[..self.base])),
            "seed {seed:x} trace {trace:?}"
        );
        if let Some(checkpoint) = &checkpoint {
            assert_eq!(checkpoint.objects(), &self.blobs[..self.base]);
            assert_eq!(
                current
                    .checkpoint()
                    .map(object_log::CheckpointRef::through_sequence),
                Some(u64::try_from(self.base - 1)?)
            );
        }
        let tail = log.read_tail(&current).await?;
        assert_eq!(
            tail.len(),
            self.history.len() - self.base,
            "seed {seed:x} trace {trace:?}"
        );
        for (index, record) in tail.iter().enumerate() {
            assert_eq!(
                record.objects(),
                &self.blobs[self.base + index..=self.base + index]
            );
            assert!(record.result().is_empty());
            assert_eq!(
                record.reference().sequence(),
                u64::try_from(self.base + index)?
            );
            assert_eq!(
                record.operation().as_ref(),
                self.history[self.base + index].to_le_bytes()
            );
        }
        for (reference, value) in self.blobs.iter().zip(&self.history) {
            assert_eq!(
                log.read_object(&current, reference).await?.as_ref(),
                value.to_le_bytes()
            );
        }
        Ok(())
    }
}

fn next_action(random: &mut u64, step: u64) -> u64 {
    *random ^= *random << 13;
    *random ^= *random >> 7;
    *random ^= *random << 17;
    // Guarantee prepare/checkpoint conflict and collection before varying the schedule.
    let prefix = [0, 1, 2, 0, 1, 3, 2, 4, 5, 6];
    usize::try_from(step)
        .ok()
        .and_then(|index| prefix.get(index))
        .copied()
        .unwrap_or(*random % 7)
}

fn encode_history(history: &[u64]) -> Bytes {
    Bytes::from(
        history
            .iter()
            .flat_map(|value| value.to_le_bytes())
            .collect::<Vec<_>>(),
    )
}

fn created_key(faults: &FaultStore, segment: &str) -> Result<String, Box<dyn std::error::Error>> {
    let marker = format!("/{segment}/");
    faults
        .metrics()
        .events
        .iter()
        .find(|event| event.operation == Operation::Put && event.path.contains(&marker))
        .map(|event| event.path.clone())
        .ok_or_else(|| format!("missing {segment} create").into())
}
