use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
};

use bytes::Bytes;
use object_log::{CommitStatus, Log, PreparedCommit, TransactionId, View, materialize};
use tokio::{fs::File, task};

use crate::{
    Error, ObjectFormat, ObjectId, RefSnapshot, RefUpdate, format::PackDescriptor, git,
    state::Machine, storage,
};

/// One disposable local Git cache backed by an object log.
#[derive(Debug)]
pub struct Repository {
    log: Log,
    path: PathBuf,
    format: ObjectFormat,
    view: View,
    refs: RefSnapshot,
    packs: BTreeSet<ObjectId>,
    objects: git::ObjectSet,
}

/// One validated Git update ready for conditional publication.
#[must_use = "publish the update or retain its recovery token"]
#[derive(Debug)]
pub struct PreparedPush {
    log: Log,
    prepared: PreparedCommit,
    recovery_token: Bytes,
}

impl Repository {
    /// Rebuilds one disposable bare Git repository from durable state.
    ///
    /// `work_dir` must not exist or must be an empty directory. The adapter
    /// never removes it or other caller data.
    ///
    /// # Errors
    ///
    /// Returns an error for an unusable work directory, invalid durable state,
    /// pack recovery failure, or local Git failure.
    pub async fn open(
        log: Log,
        work_dir: impl AsRef<Path>,
        format: ObjectFormat,
    ) -> Result<Self, Error> {
        let materialized = materialize(&log, &Machine::new(format))
            .await
            .map_err(|error| match error {
                object_log::MaterializeError::Log(error) => Error::ObjectLog(error),
                object_log::MaterializeError::State(error) => error,
            })?;
        let (view, state) = materialized.into_parts();
        let path = work_dir.as_ref().to_owned();
        let init_path = path.clone();
        blocking(move || {
            require_empty(&init_path)?;
            git::init(&init_path, format)?;
            Ok(())
        })
        .await?;

        let mut objects = git::ObjectSet::new();
        for (&id, &(bytes, ref root)) in &state.packs {
            git::extend_objects(
                &mut objects,
                recover_pack(&log, &view, &path, id, bytes, root).await?,
            )?;
        }
        let materialize_path = path.clone();
        let desired = state.refs;
        let (refs, objects) = blocking(move || {
            let repo = git::open(&materialize_path, format)?;
            git::validate_snapshot(&repo, &desired, &objects)?;
            git::materialize(&repo, &desired)?;
            Ok((desired, objects))
        })
        .await?;

        Ok(Self {
            log,
            path,
            format,
            view,
            packs: state.packs.into_keys().collect(),
            objects,
            refs,
        })
    }

    /// Returns the exact durable ref snapshot in this cache.
    #[must_use]
    pub const fn refs(&self) -> &RefSnapshot {
        &self.refs
    }

    /// Validates and stages one atomic ref update against this exact snapshot.
    ///
    /// A supplied pack is normalized and validated before object storage is
    /// changed. This method consumes the cache so a failed or conflicting
    /// update cannot leave a reusable local view. Discard the work directory
    /// after any result. Failed normalization can leave local files there.
    /// Failed pack staging can leave unreachable immutable blobs, but it does
    /// not return or publish a pack root.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid refs, an invalid or unreachable object
    /// graph, a duplicate pack, local Git failure, or object-log failure.
    pub async fn prepare_push(
        self,
        transaction_id: TransactionId,
        updates: Vec<RefUpdate>,
        pack: Option<&Path>,
    ) -> Result<PreparedPush, Error> {
        let machine = Machine::new(self.format);
        let operation = machine.transaction(updates.clone(), Vec::new())?;
        let preflight = self.log.prepare(
            self.view.cursor(),
            transaction_id,
            operation,
            Bytes::new(),
            Vec::new(),
        )?;

        let path = self.path.clone();
        let input = pack.map(Path::to_owned);
        let current = self.refs;
        let format = self.format;
        let (normalized, updates) = blocking(move || {
            let normalized = git::prepare_push(
                &path,
                format,
                &current,
                &updates,
                self.objects,
                input.as_deref(),
            )?;
            Ok((normalized, updates))
        })
        .await?;

        let prepared = if let Some(pack) = normalized {
            let id = ObjectId::try_from(pack.id)?;
            if self.packs.contains(&id) {
                return Err(Error::InvalidRecord("pack is already present"));
            }
            let staged = storage::stage_pack(&self.log, &self.view, &pack.path, id).await?;
            let operation = machine.transaction(
                updates,
                vec![PackDescriptor {
                    id,
                    bytes: staged.bytes,
                }],
            )?;
            self.log.prepare(
                self.view.cursor(),
                transaction_id,
                operation,
                Bytes::new(),
                vec![staged.root],
            )?
        } else {
            preflight
        };
        let recovery_token = prepared.recovery_token()?;
        Ok(PreparedPush {
            log: self.log,
            prepared,
            recovery_token,
        })
    }
}

impl PreparedPush {
    /// Returns the token that identifies this exact publication attempt.
    #[must_use]
    pub const fn recovery_token(&self) -> &Bytes {
        &self.recovery_token
    }

    /// Conditionally publishes this push.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid staged data or an object-store failure
    /// that cannot hide a successful publication.
    pub async fn publish(self) -> Result<CommitStatus, Error> {
        Ok(self.log.commit(self.prepared).await?)
    }
}

async fn recover_pack(
    log: &Log,
    view: &View,
    work_dir: &Path,
    expected: ObjectId,
    bytes: u64,
    root: &object_log::ObjectRef,
) -> Result<Vec<gix::hash::ObjectId>, Error> {
    let path = work_dir.join("object-log-recovery.pack");
    let mut output = File::create(&path)
        .await
        .map_err(|error| Error::PackStorage(error.to_string()))?;
    storage::write_pack(log, view, root, bytes, &mut output).await?;
    drop(output);
    let install_path = work_dir.to_owned();
    let input = path.clone();
    let objects = blocking(move || {
        let repo = git::open(&install_path, expected.format())?;
        let installed = git::install_pack(&repo, &input)?;
        if ObjectId::try_from(installed.id)? != expected {
            return Err(Error::InvalidPack(
                "installed pack ID does not match the durable record".into(),
            ));
        }
        Ok(installed.objects)
    })
    .await?;
    tokio::fs::remove_file(path)
        .await
        .map_err(|error| Error::Git(error.to_string()))?;
    Ok(objects)
}

async fn blocking<T>(
    operation: impl FnOnce() -> Result<T, Error> + Send + 'static,
) -> Result<T, Error>
where
    T: Send + 'static,
{
    task::spawn_blocking(operation)
        .await
        .map_err(|_| Error::BlockingTask)?
}

fn require_empty(path: &Path) -> Result<(), Error> {
    if !path.exists() {
        return Ok(());
    }
    let mut entries = fs::read_dir(path).map_err(|source| git::Error::Io {
        path: path.to_owned(),
        source,
    })?;
    if entries
        .next()
        .transpose()
        .map_err(|source| git::Error::Io {
            path: path.to_owned(),
            source,
        })?
        .is_some()
    {
        return Err(Error::WorkDirectoryNotEmpty);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        error::Error as StdError,
        process::{Command, Output},
    };

    use object_log::sim::{Failure, FailurePhase, FaultStore, Operation};
    use object_log::{LogId, Options, ValidatedBackend};
    use object_store::{memory::InMemory, path::Path as StorePath};
    use tempfile::TempDir;

    use super::*;

    type TestResult<T = ()> = Result<T, Box<dyn StdError>>;

    struct Fixture {
        directory: TempDir,
        pack: PathBuf,
        target: ObjectId,
    }

    async fn test_log(name: &str) -> TestResult<(Log, FaultStore, object_log::ScopedStore)> {
        let faults = FaultStore::new(InMemory::new());
        let backend = ValidatedBackend::new(
            std::sync::Arc::new(faults.clone()),
            StorePath::from("git-repository-tests"),
        )
        .await?;
        let scoped = backend.scope(&LogId::new(name)?);
        let log = Log::open(scoped.clone(), Options::default()).await?;
        Ok((log, faults, scoped))
    }

    fn fixture(format: ObjectFormat, contents: &[u8]) -> TestResult<Fixture> {
        let directory = tempfile::tempdir()?;
        let work = directory.path().join("source");
        let format_name = match format {
            ObjectFormat::Sha1 => "sha1",
            ObjectFormat::Sha256 => "sha256",
        };
        command(
            Some(directory.path()),
            &[
                "init",
                "--quiet",
                "-b",
                "main",
                &format!("--object-format={format_name}"),
                "source",
            ],
        )?;
        fs::write(work.join("file"), contents)?;
        command(Some(&work), &["add", "file"])?;
        command(Some(&work), &["commit", "--quiet", "-m", "initial"])?;
        let target = ObjectId::parse(format, output(Some(&work), &["rev-parse", "HEAD"])?.trim())?;
        let pack = directory.path().join("push.pack");
        fs::write(
            &pack,
            command_output(Some(&work), &["pack-objects", "--all", "--stdout"])?.stdout,
        )?;
        Ok(Fixture {
            directory,
            pack,
            target,
        })
    }

    #[tokio::test]
    async fn publishes_and_cold_recovers_both_object_formats() -> TestResult {
        for (name, format) in [
            ("repository-sha1", ObjectFormat::Sha1),
            ("repository-sha256", ObjectFormat::Sha256),
        ] {
            let fixture = fixture(format, name.as_bytes())?;
            let (log, _, _) = test_log(name).await?;
            let cache = fixture.directory.path().join("cache");
            let repository = Repository::open(log.clone(), &cache, format).await?;
            let update = RefUpdate::new("refs/heads/main", None, Some(fixture.target))?;
            let push = repository
                .prepare_push(TransactionId::new(), vec![update], Some(&fixture.pack))
                .await?;
            assert!(!push.recovery_token().is_empty());
            assert!(matches!(push.publish().await?, CommitStatus::Committed(_)));

            fs::remove_dir_all(&cache)?;
            let recovered = Repository::open(log.clone(), &cache, format).await?;
            assert_eq!(
                recovered.refs().get(&b"refs/heads/main"[..]),
                Some(&fixture.target)
            );
            assert_eq!(
                output(Some(&cache), &["rev-parse", "refs/heads/main"])?.trim(),
                fixture.target.to_string()
            );
            let reuse = recovered
                .prepare_push(
                    TransactionId::new(),
                    vec![RefUpdate::new(
                        "refs/tags/existing",
                        None,
                        Some(fixture.target),
                    )?],
                    None,
                )
                .await?;
            assert!(matches!(reuse.publish().await?, CommitStatus::Committed(_)));
            fs::remove_dir_all(&cache)?;
            let recovered = Repository::open(log, &cache, format).await?;
            assert_eq!(
                recovered.refs().get(&b"refs/tags/existing"[..]),
                Some(&fixture.target)
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn rejects_loose_objects_that_are_not_in_a_durable_pack() -> TestResult {
        let fixture = fixture(ObjectFormat::Sha1, b"injected")?;
        let (log, _, _) = test_log("repository-loose-objects").await?;
        let cache = fixture.directory.path().join("cache");
        let repository = Repository::open(log.clone(), &cache, ObjectFormat::Sha1).await?;
        let source = fixture.directory.path().join("source");
        for revision in ["HEAD", "HEAD^{tree}", "HEAD:file"] {
            let id = ObjectId::parse(
                ObjectFormat::Sha1,
                output(Some(&source), &["rev-parse", revision])?.trim(),
            )?;
            copy_loose_object(&source, &cache, id)?;
        }
        let target = fixture.target.to_string();
        command(
            Some(&cache),
            &["cat-file", "-e", &format!("{target}^{{tree}}")],
        )?;
        command(Some(&cache), &["cat-file", "-e", &format!("{target}:file")])?;

        let update = RefUpdate::new("refs/heads/main", None, Some(fixture.target))?;
        assert!(matches!(
            repository
                .prepare_push(TransactionId::new(), vec![update], None)
                .await,
            Err(Error::InvalidObjectGraph(_))
        ));

        fs::remove_dir_all(&cache)?;
        let recovered = Repository::open(log, &cache, ObjectFormat::Sha1).await?;
        assert!(recovered.refs().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn delete_only_stages_no_pack_and_two_writers_conflict() -> TestResult {
        let competing = fixture(ObjectFormat::Sha1, b"loser")?;
        let fixture = fixture(ObjectFormat::Sha1, b"winner")?;
        let (log, faults, _) = test_log("repository-delete").await?;
        let first_path = fixture.directory.path().join("first");
        let second_path = fixture.directory.path().join("second");
        let first = Repository::open(log.clone(), &first_path, ObjectFormat::Sha1).await?;
        let second = Repository::open(log.clone(), &second_path, ObjectFormat::Sha1).await?;
        let update = RefUpdate::new("refs/heads/main", None, Some(fixture.target))?;
        let first = first
            .prepare_push(
                TransactionId::new(),
                vec![update.clone()],
                Some(&fixture.pack),
            )
            .await?;
        let competing_update = RefUpdate::new("refs/heads/main", None, Some(competing.target))?;
        let second = second
            .prepare_push(
                TransactionId::new(),
                vec![competing_update],
                Some(&competing.pack),
            )
            .await?;
        assert!(matches!(first.publish().await?, CommitStatus::Committed(_)));
        assert!(matches!(second.publish().await?, CommitStatus::Conflict(_)));

        fs::remove_dir_all(&first_path)?;
        let third_path = fixture.directory.path().join("third");
        let winner = Repository::open(log.clone(), &third_path, ObjectFormat::Sha1).await?;
        assert_eq!(
            winner.refs().get(&b"refs/heads/main"[..]),
            Some(&fixture.target)
        );
        assert_ne!(
            winner.refs().get(&b"refs/heads/main"[..]),
            Some(&competing.target)
        );
        let current = Repository::open(log.clone(), &first_path, ObjectFormat::Sha1).await?;
        faults.reset();
        let deletion = RefUpdate::new("refs/heads/main", Some(fixture.target), None)?;
        let delete = current
            .prepare_push(TransactionId::new(), vec![deletion], None)
            .await?;
        assert_eq!(faults.metrics().operation(Operation::Put).requests, 0);
        assert!(matches!(
            delete.publish().await?,
            CommitStatus::Committed(_)
        ));
        Ok(())
    }

    #[tokio::test]
    async fn lost_response_resumes_without_restaging_the_pack() -> TestResult {
        let fixture = fixture(ObjectFormat::Sha1, b"resume")?;
        let (log, faults, scoped) = test_log("repository-resume").await?;
        faults.reset();
        let cache = fixture.directory.path().join("pending");
        let repository = Repository::open(log, &cache, ObjectFormat::Sha1).await?;
        let update = RefUpdate::new("refs/heads/main", None, Some(fixture.target))?;
        let push = repository
            .prepare_push(TransactionId::new(), vec![update], Some(&fixture.pack))
            .await?;
        let token = push.recovery_token().clone();
        let next_head_put = faults.metrics().operation(Operation::Put).requests + 2;
        faults.schedule(Failure {
            operation: Operation::Put,
            occurrence: next_head_put,
            phase: FailurePhase::After,
        });
        assert!(matches!(push.publish().await?, CommitStatus::Pending(_)));
        let staged_puts = pack_puts(&faults);

        fs::remove_dir_all(&cache)?;
        let reopened = Log::open(scoped, Options::default()).await?;
        assert!(matches!(
            reopened.resume(&token).await?,
            object_log::Resolution::Committed(_)
        ));
        let _recovered = Repository::open(reopened, &cache, ObjectFormat::Sha1).await?;
        assert_eq!(pack_puts(&faults), staged_puts);
        Ok(())
    }

    #[tokio::test]
    async fn refuses_nonempty_work_directory_without_deleting_it() -> TestResult {
        let directory = tempfile::tempdir()?;
        let cache = directory.path().join("cache");
        fs::create_dir(&cache)?;
        fs::write(cache.join("keep"), b"caller data")?;
        let (log, _, _) = test_log("repository-work-dir").await?;
        assert!(matches!(
            Repository::open(log, &cache, ObjectFormat::Sha1).await,
            Err(Error::WorkDirectoryNotEmpty)
        ));
        assert_eq!(fs::read(cache.join("keep"))?, b"caller data");
        Ok(())
    }

    fn pack_puts(faults: &FaultStore) -> usize {
        faults
            .metrics()
            .events
            .iter()
            .filter(|event| {
                event.operation == Operation::Put
                    && (event.path.contains("/blobs/") || event.path.contains("/nodes/"))
            })
            .count()
    }

    fn copy_loose_object(source: &Path, target: &Path, id: ObjectId) -> TestResult {
        let id = id.to_string();
        let relative = Path::new("objects").join(&id[..2]).join(&id[2..]);
        let destination = target.join(&relative);
        fs::create_dir_all(
            destination
                .parent()
                .ok_or_else(|| "loose object has no parent directory".to_string())?,
        )?;
        fs::copy(source.join(".git").join(relative), destination)?;
        Ok(())
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
}
