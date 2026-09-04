use std::{
    error::Error as StdError,
    fs,
    path::{Path, PathBuf},
    process::{Command, Output},
};

use object_log::{CommitStatus, TransactionId, View};
use object_log_git::{ObjectFormat, ObjectId, RefUpdate, Repository};
use tempfile::TempDir;

pub(crate) type TestResult<T = ()> = Result<T, Box<dyn StdError>>;

pub(crate) struct Fixture {
    _directory: TempDir,
    pub(crate) pack: PathBuf,
    pub(crate) pack_bytes: u64,
    pub(crate) target: ObjectId,
    pub(crate) contents: Vec<u8>,
}

pub(crate) fn fixture(name: &str, bytes: usize, seed: u64) -> TestResult<Fixture> {
    let directory = tempfile::tempdir()?;
    let root = directory.path();
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
    let contents = pseudo_random(bytes, seed);
    fs::write(work.join("file"), &contents)?;
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
    let pack_bytes = fs::metadata(&pack)?.len();
    Ok(Fixture {
        _directory: directory,
        pack,
        pack_bytes,
        target,
        contents,
    })
}

pub(crate) async fn publish(
    repository: Repository,
    name: &str,
    expected: Option<ObjectId>,
    target: Option<ObjectId>,
    pack: Option<&Path>,
) -> TestResult<View> {
    let update = RefUpdate::new(name.to_owned(), expected, target)?;
    let prepared = repository
        .prepare_push(TransactionId::new(), vec![update], pack)
        .await?;
    match prepared.publish().await? {
        CommitStatus::Committed(view) => Ok(view),
        CommitStatus::Conflict(_) => Err("Git publication conflicted".into()),
        CommitStatus::Pending(_) => Err("Git publication remained pending".into()),
    }
}

pub(crate) fn assert_repository(path: &Path, fixture: &Fixture) -> TestResult {
    command(Some(path), &["fsck", "--strict", "--no-progress"])?;
    let actual = command_output(Some(path), &["show", "refs/heads/main:file"])?;
    if !actual.status.success() {
        return Err(String::from_utf8_lossy(&actual.stderr).into_owned().into());
    }
    assert_eq!(actual.stdout, fixture.contents);
    Ok(())
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
