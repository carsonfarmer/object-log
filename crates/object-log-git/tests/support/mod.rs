use std::{
    error::Error as StdError,
    fs,
    io::Write,
    path::{Path, PathBuf},
    process::{Command, Output},
};

use bytes::Bytes;
use object_log::{Resolution, TransactionId, View};
use object_log_git::{ObjectFormat, ObjectId, Repository};
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
    let prepared = repository
        .prepare_receive(
            TransactionId::new(),
            receive(&[(name, expected, target)], pack)?,
        )
        .await?;
    let (resolution, response) = prepared.publish_receive().await?;
    assert!(
        response
            .windows(b"unpack ok".len())
            .any(|w| w == b"unpack ok")
    );
    match resolution {
        Resolution::Committed(view) => Ok(view),
        _ => Err("Git publication did not commit".into()),
    }
}

pub(crate) fn receive(
    updates: &[(&str, Option<ObjectId>, Option<ObjectId>)],
    pack: Option<&Path>,
) -> TestResult<Bytes> {
    let mut request = Vec::new();
    let zero = "0".repeat(40);
    for (index, (name, expected, target)) in updates.iter().enumerate() {
        let capabilities = if index == 0 {
            "\0report-status object-format=sha1"
        } else {
            ""
        };
        packet(
            &mut request,
            format!(
                "{} {} {name}{capabilities}\n",
                expected.map_or_else(|| zero.clone(), |id| id.to_string()),
                target.map_or_else(|| zero.clone(), |id| id.to_string())
            )
            .as_bytes(),
        )?;
    }
    request.extend_from_slice(b"0000");
    if let Some(pack) = pack {
        request.extend_from_slice(&fs::read(pack)?);
    } else if updates.iter().any(|(_, _, target)| target.is_some()) {
        let empty = b"PACK\0\0\0\x02\0\0\0\0";
        let mut hasher = gix_hash::hasher(gix_hash::Kind::Sha1);
        hasher.update(empty);
        request.extend_from_slice(empty);
        request.extend_from_slice(hasher.try_finalize()?.as_slice());
    }
    Ok(request.into())
}

fn packet(output: &mut Vec<u8>, line: &[u8]) -> TestResult {
    write!(output, "{:04x}", line.len() + 4)?;
    output.extend_from_slice(line);
    Ok(())
}

pub(crate) async fn fetch(repository: Repository, target: ObjectId) -> TestResult<Vec<u8>> {
    let mut request = Vec::new();
    packet(&mut request, b"command=fetch\n")?;
    packet(&mut request, b"object-format=sha1\n")?;
    request.extend_from_slice(b"0001");
    packet(&mut request, format!("want {target}\n").as_bytes())?;
    packet(&mut request, b"done\n")?;
    request.extend_from_slice(b"0000");
    let response = repository.upload_pack(request.into()).await?;
    let mut remaining = response.as_ref();
    let mut raw = Vec::new();
    let mut pack_section = false;
    while !remaining.is_empty() {
        let header = remaining.get(..4).ok_or("truncated packet")?;
        let length = usize::from_str_radix(std::str::from_utf8(header)?, 16)?;
        remaining = &remaining[4..];
        if length <= 2 {
            continue;
        }
        let payload = remaining.get(..length - 4).ok_or("truncated payload")?;
        remaining = &remaining[length - 4..];
        if payload == b"packfile\n" {
            pack_section = true;
            continue;
        }
        if pack_section {
            assert_eq!(payload.first(), Some(&1));
            raw.extend_from_slice(&payload[1..]);
        }
    }
    assert!(raw.starts_with(b"PACK"));
    Ok(raw)
}

pub(crate) async fn recover(repository: Repository, path: &Path, fixture: &Fixture) -> TestResult {
    let refs = repository.refs().clone();
    let pack = fetch(repository, fixture.target).await?;
    command(
        None,
        &[
            "init",
            "--quiet",
            "--bare",
            "--object-format=sha1",
            path.to_str().ok_or("non-UTF8 path")?,
        ],
    )?;
    let pack_path = path.join("incoming.pack");
    fs::write(&pack_path, pack)?;
    command(
        Some(path),
        &[
            "index-pack",
            "--strict",
            "--check-self-contained-and-connected",
            pack_path.to_str().ok_or("non-UTF8 path")?,
        ],
    )?;
    // Import into the receiver ODB, then verify refs and complete graph with Git.
    let mut child = Command::new("git")
        .current_dir(path)
        .args(["index-pack", "--stdin", "--strict"])
        .stdin(std::process::Stdio::from(fs::File::open(&pack_path)?))
        .stdout(std::process::Stdio::null())
        .spawn()?;
    assert!(child.wait()?.success());
    fs::remove_file(pack_path.with_extension("idx"))?;
    fs::remove_file(pack_path)?;
    for (name, target) in refs {
        command(
            Some(path),
            &[
                "update-ref",
                std::str::from_utf8(&name)?,
                &target.to_string(),
            ],
        )?;
    }
    assert_repository(path, fixture)
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
