use std::{
    collections::BTreeSet,
    error::Error as StdError,
    fs,
    path::{Path, PathBuf},
    process::{Command, Output},
    sync::Arc,
    time::{Duration, Instant},
};

use futures::TryStreamExt;
use object_log::{
    CheckpointStatus, CollectionFinish, CollectionStart, CommitStatus, Log, LogId, Options,
    TransactionId, ValidatedBackend, View,
};
use object_log_git::{ObjectFormat, ObjectId, RefUpdate, Repository};
use object_store::{ObjectStore, memory::InMemory, path::Path as StorePath};

const DEAD_BYTES: usize = 2 * 1024 * 1024;
const GC_DEADLINE: Duration = Duration::from_secs(10);

type TestResult<T = ()> = Result<T, Box<dyn StdError>>;

struct Fixture {
    pack: PathBuf,
    target: ObjectId,
    contents: Vec<u8>,
}

#[tokio::test]
async fn checkpoint_makes_dead_pack_collectable_and_keeps_live_pack() -> TestResult {
    let raw = Arc::new(InMemory::new());
    let root = StorePath::from("git-gc-tests");
    let backend = ValidatedBackend::new(raw.clone(), root.clone()).await?;
    let log = Log::open(
        backend.scope(&LogId::new("repository")?),
        Options {
            max_object_bytes: 16 * 1024,
            max_collection_objects: 10_000,
            ..Options::default()
        },
    )
    .await?;
    let directory = tempfile::tempdir()?;

    let empty = repository(&log, directory.path(), "empty").await?;
    let CheckpointStatus::Published(empty_view) = empty.checkpoint().await? else {
        return Err("empty checkpoint did not return its current view".into());
    };
    assert!(empty_view.tail().is_empty());

    let live = fixture(directory.path(), "live", &pseudo_random(4_096, 1))?;
    let dead = fixture(directory.path(), "dead", &pseudo_random(DEAD_BYTES, 2))?;
    let first = repository(&log, directory.path(), "first").await?;
    let first_view = publish(
        first,
        RefUpdate::new("refs/heads/main", None, Some(live.target))?,
        Some(&live.pack),
    )
    .await?;
    assert_eq!(first_view.tail().len(), 1);
    let live_pack = pack_keys(&stored_keys(&raw, &root).await?);

    let second = repository(&log, directory.path(), "second").await?;
    let second_view = publish(
        second,
        RefUpdate::new("refs/heads/dead", None, Some(dead.target))?,
        Some(&dead.pack),
    )
    .await?;
    assert_eq!(second_view.tail().len(), 2);
    let both_packs = pack_keys(&stored_keys(&raw, &root).await?);
    let dead_pack = both_packs
        .difference(&live_pack)
        .cloned()
        .collect::<BTreeSet<_>>();
    assert!(dead_pack.len() >= 100);

    let third = repository(&log, directory.path(), "third").await?;
    publish(
        third,
        RefUpdate::new("refs/heads/dead", Some(dead.target), None)?,
        None,
    )
    .await?;

    let checkpoint = repository(&log, directory.path(), "checkpoint").await?;
    let CheckpointStatus::Published(checkpoint_view) = checkpoint.checkpoint().await? else {
        return Err("Git checkpoint did not publish".into());
    };
    assert!(checkpoint_view.tail().is_empty());

    let tail = repository(&log, directory.path(), "tail").await?;
    let current = publish(
        tail,
        RefUpdate::new("refs/tags/after-checkpoint", None, Some(live.target))?,
        None,
    )
    .await?;
    assert_eq!(current.tail().len(), 1);

    let before = stored_keys(&raw, &root).await?;
    let started = Instant::now();
    let (current, candidates) = tokio::time::timeout(GC_DEADLINE, collect(&log, &current))
        .await
        .map_err(|_| format!("Git GC exceeded {GC_DEADLINE:?}"))??;
    let elapsed = started.elapsed();
    assert!(candidates >= dead_pack.len());
    assert!(candidates >= 100);

    let after = stored_keys(&raw, &root).await?;
    assert_eq!(after.len(), before.len() - candidates);
    assert!(live_pack.is_subset(&after));
    assert!(dead_pack.is_disjoint(&after));

    assert_recovery(&log, directory.path(), &live).await?;

    let generation = current.cursor().generation();
    let epoch = current.collection_epoch();
    let CollectionStart::Empty(report) = log.start_collection(&current).await? else {
        return Err("the second collection was not empty".into());
    };
    assert_eq!(report.candidate_count(), 0);
    let unchanged = log.load().await?;
    assert_eq!(unchanged.cursor().generation(), generation);
    assert_eq!(unchanged.collection_epoch(), epoch);
    eprintln!(
        "Git GC: {candidates} objects removed in {elapsed:?}; {} pack objects remain",
        live_pack.len()
    );
    Ok(())
}

async fn assert_recovery(log: &Log, root: &Path, live: &Fixture) -> TestResult {
    let path = root.join("recovered");
    let recovered = Repository::open(log, &path, ObjectFormat::Sha1).await?;
    assert_eq!(
        recovered.refs().get(&b"refs/heads/main"[..]),
        Some(&live.target)
    );
    assert_eq!(
        recovered.refs().get(&b"refs/tags/after-checkpoint"[..]),
        Some(&live.target)
    );
    command(Some(&path), &["fsck", "--strict", "--no-progress"])?;
    assert_eq!(
        command_output(Some(&path), &["show", "refs/heads/main:file"])?.stdout,
        live.contents
    );
    Ok(())
}

async fn publish(
    repository: Repository,
    update: RefUpdate,
    pack: Option<&Path>,
) -> TestResult<View> {
    let push = repository
        .prepare_push(TransactionId::new(), vec![update], pack)
        .await?;
    match push.publish().await? {
        CommitStatus::Committed(view) => Ok(view),
        CommitStatus::Conflict(_) => Err("Git push conflicted".into()),
        CommitStatus::Pending(_) => Err("Git push remained pending".into()),
    }
}

async fn repository(log: &Log, root: &Path, name: &str) -> TestResult<Repository> {
    Ok(Repository::open(log, root.join(name), ObjectFormat::Sha1).await?)
}

async fn collect(log: &Log, view: &View) -> TestResult<(View, usize)> {
    let CollectionStart::Installed(fenced, start) = log.start_collection(view).await? else {
        return Err("collection did not install a deletion plan".into());
    };
    let CollectionFinish::Complete(current, finish) = log.resume_collection(&fenced).await? else {
        return Err("collection did not finish its deletion plan".into());
    };
    assert_eq!(finish.candidate_count(), start.candidate_count());
    assert_eq!(finish.delete_attempts(), start.candidate_count());
    Ok((current, start.candidate_count()))
}

async fn stored_keys(store: &InMemory, root: &StorePath) -> TestResult<BTreeSet<String>> {
    Ok(store
        .list(Some(root))
        .map_ok(|object| object.location.to_string())
        .try_collect()
        .await?)
}

fn pack_keys(keys: &BTreeSet<String>) -> BTreeSet<String> {
    keys.iter()
        .filter(|key| key.contains("/blobs/") || key.contains("/nodes/"))
        .cloned()
        .collect()
}

fn fixture(root: &Path, name: &str, contents: &[u8]) -> TestResult<Fixture> {
    let work = root.join(name);
    command(
        Some(root),
        &[
            "init",
            "--quiet",
            "-b",
            "main",
            "--object-format=sha1",
            name,
        ],
    )?;
    fs::write(work.join("file"), contents)?;
    command(Some(&work), &["add", "file"])?;
    command(Some(&work), &["commit", "--quiet", "-m", name])?;
    let target = ObjectId::parse(
        ObjectFormat::Sha1,
        output(Some(&work), &["rev-parse", "HEAD"])?.trim(),
    )?;
    let pack = root.join(format!("{name}.pack"));
    fs::write(
        &pack,
        command_output(Some(&work), &["pack-objects", "--all", "--stdout"])?.stdout,
    )?;
    Ok(Fixture {
        pack,
        target,
        contents: contents.to_vec(),
    })
}

fn pseudo_random(len: usize, seed: u64) -> Vec<u8> {
    let mut state = seed;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state.to_le_bytes()[0]
        })
        .collect()
}

fn command(directory: Option<&Path>, args: &[&str]) -> TestResult {
    let result = command_output(directory, args)?;
    if result.status.success() {
        Ok(())
    } else {
        Err(String::from_utf8_lossy(&result.stderr).into_owned().into())
    }
}

fn output(directory: Option<&Path>, args: &[&str]) -> TestResult<String> {
    let result = command_output(directory, args)?;
    if result.status.success() {
        Ok(String::from_utf8(result.stdout)?)
    } else {
        Err(String::from_utf8_lossy(&result.stderr).into_owned().into())
    }
}

fn command_output(directory: Option<&Path>, args: &[&str]) -> TestResult<Output> {
    let mut command = Command::new("git");
    command
        .args(args)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_AUTHOR_NAME", "Object Log")
        .env("GIT_AUTHOR_EMAIL", "object-log@example.invalid")
        .env("GIT_AUTHOR_DATE", "2000-01-01T00:00:00Z")
        .env("GIT_COMMITTER_NAME", "Object Log")
        .env("GIT_COMMITTER_EMAIL", "object-log@example.invalid")
        .env("GIT_COMMITTER_DATE", "2000-01-01T00:00:00Z");
    if let Some(directory) = directory {
        command.current_dir(directory);
    }
    Ok(command.output()?)
}
