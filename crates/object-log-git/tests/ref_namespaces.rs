use std::{
    collections::BTreeMap,
    io::Write,
    path::Path,
    process::{Command, Stdio},
    sync::Arc,
};

use object_log::{
    CheckpointStatus, Log, LogId, Options, Resolution, TransactionId, ValidatedBackend,
};
use object_log_git::{ObjectFormat, ObjectId, Repository};
use object_store::{memory::InMemory, path::Path as StorePath};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

// Keep names as bytes even when crossing the installed Git command boundary.
fn git(path: &Path, args: &[&str], input: &[u8]) -> TestResult<Vec<u8>> {
    let mut child = Command::new("git")
        .current_dir(path)
        .env("GIT_AUTHOR_NAME", "Test")
        .env("GIT_AUTHOR_EMAIL", "test@example.com")
        .env("GIT_COMMITTER_NAME", "Test")
        .env("GIT_COMMITTER_EMAIL", "test@example.com")
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    child
        .stdin
        .take()
        .ok_or("missing Git stdin")?
        .write_all(input)?;
    let output = child.wait_with_output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(output.stdout)
}

fn packet(output: &mut Vec<u8>, line: &[u8]) -> TestResult {
    write!(output, "{:04x}", line.len() + 4)?;
    output.extend_from_slice(line);
    Ok(())
}

fn update_ref(path: &Path, name: &[u8], id: &str) -> TestResult {
    let mut input = b"update ".to_vec();
    input.extend_from_slice(name);
    input.push(0);
    input.extend_from_slice(id.as_bytes());
    input.extend_from_slice(b"\0\0");
    git(path, &["update-ref", "--stdin", "-z"], &input)?;
    Ok(())
}

fn refs(path: &Path, format: ObjectFormat) -> TestResult<BTreeMap<Vec<u8>, ObjectId>> {
    let listing = git(
        path,
        &["for-each-ref", "--format=%(refname) %(objectname)"],
        &[],
    )?;
    listing
        .split(|&b| b == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| {
            let space = line
                .iter()
                .position(|&b| b == b' ')
                .ok_or("invalid ref listing")?;
            Ok((
                line[..space].to_vec(),
                ObjectId::parse(format, std::str::from_utf8(&line[space + 1..])?)?,
            ))
        })
        .collect()
}

fn unpack_response(mut remaining: &[u8]) -> TestResult<Vec<u8>> {
    let mut output = Vec::new();
    let mut pack = false;
    while !remaining.is_empty() {
        let length = usize::from_str_radix(
            std::str::from_utf8(remaining.get(..4).ok_or("packet header")?)?,
            16,
        )?;
        remaining = &remaining[4..];
        if length <= 2 {
            continue;
        }
        let payload = remaining.get(..length - 4).ok_or("packet payload")?;
        remaining = &remaining[length - 4..];
        if payload == b"packfile\n" {
            pack = true;
        } else if pack {
            assert_eq!(payload.first(), Some(&1));
            output.extend_from_slice(&payload[1..]);
        }
    }
    assert!(output.starts_with(b"PACK"));
    Ok(output)
}

#[tokio::test]
async fn notes_mirror_and_byte_refs_survive_receive_checkpoint_and_cold_fetch() -> TestResult {
    for (format, format_name) in [
        (ObjectFormat::Sha1, "sha1"),
        (ObjectFormat::Sha256, "sha256"),
    ] {
        let source = tempfile::tempdir()?;
        let format_arg = format!("--object-format={format_name}");
        // Reftable keeps valid non-UTF8 names independent of filesystem
        // filename restrictions (notably on macOS).
        git(
            source.path(),
            &[
                "init",
                "--quiet",
                "--ref-format=reftable",
                "-b",
                "main",
                &format_arg,
            ],
            &[],
        )?;
        git(
            source.path(),
            &["commit", "--quiet", "--allow-empty", "-m", "initial"],
            &[],
        )?;
        git(
            source.path(),
            &["notes", "add", "-m", "durable note", "HEAD"],
            &[],
        )?;
        let head = String::from_utf8(git(source.path(), &["rev-parse", "HEAD"], &[])?)?;
        for name in [
            b"refs/remotes/origin/main".as_slice(),
            b"refs/archive/saved",
            b"refs/archive/\xff",
        ] {
            update_ref(source.path(), name, head.trim())?;
        }
        let expected = refs(source.path(), format)?;
        let pack = git(source.path(), &["pack-objects", "--all", "--stdout"], &[])?;
        let backend =
            ValidatedBackend::new(Arc::new(InMemory::new()), StorePath::from("ref-namespaces"))
                .await?;
        let log = Log::open(&backend, &LogId::new("repository")?, Options::default()).await?;
        let mut receive = Vec::new();
        for (index, (name, id)) in expected.iter().enumerate() {
            let mut line = format!("{} {id} ", "0".repeat(head.trim().len())).into_bytes();
            line.extend_from_slice(name);
            if index == 0 {
                line.extend_from_slice(
                    format!("\0report-status atomic object-format={format_name}").as_bytes(),
                );
            }
            packet(&mut receive, &line)?;
        }
        receive.extend_from_slice(b"0000");
        receive.extend_from_slice(&pack);
        let prepared = Repository::open(&log, format)
            .await?
            .prepare_receive(TransactionId::new(), receive.into())
            .await?;
        let (resolution, response) = prepared.publish_receive().await?;
        assert!(matches!(resolution, Resolution::Committed(_)));
        assert!(
            response
                .windows(b"ok refs/archive/\xff".len())
                .any(|line| line == b"ok refs/archive/\xff")
        );
        drop(response);
        for checkpoint in [false, true] {
            if checkpoint {
                assert!(matches!(
                    Repository::open(&log, format).await?.checkpoint().await?,
                    CheckpointStatus::Published(_)
                ));
            }
            let cold_log =
                Log::open(&backend, &LogId::new("repository")?, Options::default()).await?;
            let mut discovery = Vec::new();
            packet(&mut discovery, b"command=ls-refs\n")?;
            packet(
                &mut discovery,
                format!("object-format={format_name}\n").as_bytes(),
            )?;
            discovery.extend_from_slice(b"00010000");
            let response = Repository::open(&cold_log, format)
                .await?
                .upload_pack(discovery.into())
                .await?;
            for (name, id) in &expected {
                let mut line = format!("{id} ").into_bytes();
                line.extend_from_slice(name);
                line.push(b'\n');
                assert!(response.windows(line.len()).any(|bytes| bytes == line));
            }
            drop(response);
            let repository = Repository::open(&cold_log, format).await?;
            assert_eq!(repository.refs(), &expected);
            check_fetch(repository, format, format_name, &expected, head.trim()).await?;
        }
    }
    Ok(())
}

async fn check_fetch(
    repository: Repository,
    format: ObjectFormat,
    format_name: &str,
    expected: &BTreeMap<Vec<u8>, ObjectId>,
    head: &str,
) -> TestResult {
    let format_arg = format!("--object-format={format_name}");
    let mut fetch = Vec::new();
    packet(&mut fetch, b"command=fetch\n")?;
    packet(
        &mut fetch,
        format!("object-format={format_name}\n").as_bytes(),
    )?;
    fetch.extend_from_slice(b"0001");
    for id in expected.values() {
        packet(&mut fetch, format!("want {id}\n").as_bytes())?;
    }
    packet(&mut fetch, b"done\n")?;
    fetch.extend_from_slice(b"0000");
    let response = repository.upload_pack(fetch.into()).await?;
    let pack = unpack_response(&response)?;
    drop(response);
    let receiver = tempfile::tempdir()?;
    git(
        receiver.path(),
        &[
            "init",
            "--quiet",
            "--bare",
            "--ref-format=reftable",
            &format_arg,
        ],
        &[],
    )?;
    git(
        receiver.path(),
        &[
            "index-pack",
            "--stdin",
            "--strict",
            "--check-self-contained-and-connected",
        ],
        &pack,
    )?;
    for (name, id) in expected {
        update_ref(receiver.path(), name, &id.to_string())?;
    }
    assert_eq!(&refs(receiver.path(), format)?, expected);
    assert_eq!(
        git(receiver.path(), &["notes", "show", head.trim()], &[])?,
        b"durable note\n"
    );
    git(receiver.path(), &["fsck", "--strict", "--no-reflogs"], &[])?;
    Ok(())
}
