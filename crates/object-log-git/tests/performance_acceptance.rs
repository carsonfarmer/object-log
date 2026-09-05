mod support;

use std::{fs, path::Path as FilePath, sync::Arc};

use futures::TryStreamExt;
use object_log::sim::{FaultStore, Metrics, Operation};
use object_log::{CheckpointStatus, Log, LogId, Options, ValidatedBackend};
use object_log_git::{ObjectFormat, Repository};
use object_store::{ObjectStore, memory::InMemory, path::Path};
use support::{TestResult, fixture, publish};

const KIB: usize = 1_024;
const MIB: usize = KIB * KIB;

#[tokio::test]
#[ignore = "creates and recovers an 8 MiB Git pack"]
async fn git_request_and_byte_accounting() -> TestResult {
    let small = measure("small", 4 * KIB, Options::default()).await?;
    let large = measure(
        "large",
        8 * MIB,
        Options {
            max_object_bytes: 256 * KIB,
            ..Options::default()
        },
    )
    .await?;

    assert!(large.pack_bytes > 1_000 * small.pack_bytes);
    assert!(large.publication.total_requests() > small.publication.total_requests());
    assert!(large.publication.uploaded_bytes() > 1_000 * small.publication.uploaded_bytes());
    assert!(large.recovery.total_requests() > small.recovery.total_requests());
    assert!(large.recovery.downloaded_bytes() > 1_000 * small.recovery.downloaded_bytes());
    assert!(large.receiver_bytes > small.receiver_bytes);
    Ok(())
}

struct Measurement {
    pack_bytes: u64,
    publication: Metrics,
    recovery: Metrics,
    receiver_bytes: u64,
}

async fn measure(label: &str, payload_bytes: usize, options: Options) -> TestResult<Measurement> {
    let inner = Arc::new(InMemory::new());
    let faults = FaultStore::from_arc(inner.clone());
    let root = Path::from(format!("git-performance-{label}"));
    let backend = ValidatedBackend::new(Arc::new(faults.clone()), root.clone()).await?;
    let log = Log::open(&backend, &LogId::new("repository")?, options).await?;
    let directory = tempfile::tempdir()?;
    let source = fixture("source", payload_bytes, u64::try_from(payload_bytes)?)?;
    let pack_bytes = source.pack_bytes;

    let repository = Repository::open(&log, ObjectFormat::Sha1).await?;
    faults.reset();
    publish(
        repository,
        "refs/heads/main",
        None,
        Some(source.target),
        Some(&source.pack),
    )
    .await?;
    let publication = faults.metrics();
    let chunks = publication
        .events
        .iter()
        .filter(|event| event.operation == Operation::Put && event.path.contains("/blobs/"))
        .count();
    let chunks = u64::try_from(chunks)?;
    // The common engine verifies the staged index and selected object ranges
    // before publishing. The native reference previously verified local files.
    assert_eq!(publication.operation(Operation::Get).requests, chunks + 1);
    assert!(publication.downloaded_bytes() >= pack_bytes);
    // Chunk blobs, one authenticated geometry node, commit, and mutable head.
    assert_eq!(publication.operation(Operation::Put).requests, chunks + 3);
    assert_eq!(publication.total_requests(), 2 * chunks + 4);
    let durable_after_publication = stored_bytes(&inner, &root).await?;

    let repository = Repository::open(&log, ObjectFormat::Sha1).await?;
    faults.reset();
    let CheckpointStatus::Published(checkpoint_view) = repository.checkpoint().await? else {
        return Err("Git checkpoint did not publish".into());
    };
    assert!(checkpoint_view.tail().is_empty());
    let checkpoint = faults.metrics();
    assert_eq!(checkpoint.operation(Operation::Put).requests, 2);
    // Tail commit, index, and the reachable graph's object ranges.
    assert_eq!(checkpoint.operation(Operation::Get).requests, chunks + 2);
    assert_eq!(checkpoint.total_requests(), chunks + 4);
    let durable_after_checkpoint = stored_bytes(&inner, &root).await?;
    assert!(durable_after_checkpoint > durable_after_publication);

    let receiver_path = directory.path().join("recovered");
    faults.reset();
    let recovered = Repository::open(&log, ObjectFormat::Sha1).await?;
    assert_eq!(
        recovered.refs().get(&b"refs/heads/main"[..]),
        Some(&source.target)
    );
    let pack = support::fetch(recovered, source.target).await?;
    let recovery = faults.metrics();
    assert_eq!(recovery.operation(Operation::Put).requests, 0);
    // Cold fetch reads head/checkpoint, index and reachable ranges. The 8 MiB
    // object exceeds the decoder cache, so pack emission rereads its chunks;
    // the small fixture stays cached. These exact fixture counts guard drift.
    let expected_gets = if payload_bytes == 4 * KIB { 4 } else { 69 };
    assert_eq!(recovery.operation(Operation::Get).requests, expected_gets);
    assert_eq!(recovery.total_requests(), expected_gets);
    assert_eq!(recovery.uploaded_bytes(), 0);
    assert!(recovery.downloaded_bytes() >= pack_bytes);
    let recovered = Repository::open(&log, ObjectFormat::Sha1).await?;
    support::recover(recovered, &receiver_path, &source).await?;
    assert!(pack.starts_with(b"PACK"));
    let receiver_bytes = directory_bytes(&receiver_path)?;
    assert!(receiver_bytes >= pack_bytes);

    println!(
        "{label}: pack={pack_bytes} B, durable={durable_after_publication} B -> {durable_after_checkpoint} B, Git receiver={receiver_bytes} B"
    );
    report(label, "publication", &publication);
    report(label, "checkpoint", &checkpoint);
    report(label, "recovery", &recovery);
    Ok(Measurement {
        pack_bytes,
        publication,
        recovery,
        receiver_bytes,
    })
}

async fn stored_bytes(store: &InMemory, root: &Path) -> TestResult<u64> {
    Ok(store
        .list(Some(root))
        .try_fold(0_u64, |total, object| async move {
            total
                .checked_add(object.size)
                .ok_or_else(|| object_store::Error::Generic {
                    store: "object-log-git test",
                    source: Box::new(std::io::Error::other("stored byte count overflowed")),
                })
        })
        .await?)
}

fn directory_bytes(path: &FilePath) -> TestResult<u64> {
    fs::read_dir(path)?.try_fold(0_u64, |total, entry| {
        let entry = entry?;
        let metadata = entry.metadata()?;
        let bytes = if metadata.is_dir() {
            directory_bytes(&entry.path())?
        } else {
            metadata.len()
        };
        total
            .checked_add(bytes)
            .ok_or_else(|| "directory byte count overflowed".into())
    })
}

fn report(label: &str, phase: &str, metrics: &Metrics) {
    println!(
        "{label} {phase}: requests={} (GET={}, PUT={}), uploaded={} B, downloaded={} B",
        metrics.total_requests(),
        metrics.operation(Operation::Get).requests,
        metrics.operation(Operation::Put).requests,
        metrics.uploaded_bytes(),
        metrics.downloaded_bytes(),
    );
}
