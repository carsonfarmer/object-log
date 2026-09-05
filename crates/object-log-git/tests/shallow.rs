//! Protocol-v2 shallow selection compared with installed Git, for both hashes.
use object_log::{Log, LogId, Options, Resolution, TransactionId, ValidatedBackend};
use object_log_git::{ObjectFormat, Repository};
use object_store::{memory::InMemory, path::Path as StorePath};
use std::{
    collections::BTreeSet,
    io::Write,
    path::Path,
    process::{Command, Stdio},
    sync::Arc,
};
type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

fn git(path: &Path, args: &[&str], input: &[u8]) -> TestResult<Vec<u8>> {
    git_at(path, args, input, "2000-01-01T00:00:00Z")
}
fn git_at(path: &Path, args: &[&str], input: &[u8], date: &str) -> TestResult<Vec<u8>> {
    let mut child = Command::new("git")
        .current_dir(path)
        .args(args)
        .env("GIT_PROTOCOL", "version=2")
        .env("GIT_AUTHOR_DATE", date)
        .env("GIT_COMMITTER_DATE", date)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_AUTHOR_NAME", "Test")
        .env("GIT_AUTHOR_EMAIL", "test@example.invalid")
        .env("GIT_COMMITTER_NAME", "Test")
        .env("GIT_COMMITTER_EMAIL", "test@example.invalid")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    child.stdin.take().ok_or("stdin")?.write_all(input)?;
    let output = child.wait_with_output()?;
    assert!(
        output.status.success(),
        "{args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(output.stdout)
}
fn text(path: &Path, args: &[&str]) -> TestResult<String> {
    Ok(String::from_utf8(git(path, args, &[])?)?.trim().to_owned())
}
fn packet(out: &mut Vec<u8>, line: &str) -> TestResult {
    writeln!(out, "{:04x}{line}", line.len() + 5)?;
    Ok(())
}
fn request(format: &str, args: &[String]) -> TestResult<Vec<u8>> {
    let mut out = Vec::new();
    packet(&mut out, "command=fetch")?;
    packet(&mut out, &format!("object-format={format}"))?;
    out.extend_from_slice(b"0001");
    for arg in args {
        packet(&mut out, arg)?;
    }
    out.extend_from_slice(b"0000");
    Ok(out)
}
#[derive(Debug, PartialEq, Eq)]
struct Reply {
    shallow: BTreeSet<String>,
    unshallow: BTreeSet<String>,
    ids: BTreeSet<String>,
}
fn reply(path: &Path, mut bytes: &[u8]) -> TestResult<Reply> {
    let (mut shallow, mut unshallow, mut pack) = (BTreeSet::new(), BTreeSet::new(), Vec::new());
    let mut in_pack = false;
    while !bytes.is_empty() {
        let n = usize::from_str_radix(std::str::from_utf8(bytes.get(..4).ok_or("packet")?)?, 16)?;
        bytes = &bytes[4..];
        if n <= 2 {
            continue;
        }
        let line = bytes.get(..n - 4).ok_or("payload")?;
        bytes = &bytes[n - 4..];
        if line == b"packfile\n" {
            in_pack = true;
        } else if in_pack {
            if line[0] == 1 {
                pack.extend_from_slice(&line[1..]);
            }
        } else if let Some(id) = line.strip_prefix(b"shallow ") {
            shallow.insert(std::str::from_utf8(id)?.trim().to_owned());
        } else if let Some(id) = line.strip_prefix(b"unshallow ") {
            unshallow.insert(std::str::from_utf8(id)?.trim().to_owned());
        }
    }
    assert!(pack.starts_with(b"PACK"));
    let file = path.join("selection.pack");
    std::fs::write(&file, pack)?;
    git(path, &["index-pack", file.to_str().ok_or("path")?], &[])?;
    let index = std::fs::read(file.with_extension("idx"))?;
    let entries = git(path, &["show-index"], &index)?;
    let ids = std::str::from_utf8(&entries)?
        .lines()
        .map(|line| {
            line.split_whitespace()
                .nth(1)
                .ok_or("index")
                .map(str::to_owned)
        })
        .collect::<Result<_, _>>()?;
    Ok(Reply {
        shallow,
        unshallow,
        ids,
    })
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "shared both-hash oracle fixture with distinct negotiation cases"
)]
async fn shallow_depth_merge_deepen_unshallow_and_exclusion_match_git() -> TestResult {
    for (format, name) in [
        (ObjectFormat::Sha1, "sha1"),
        (ObjectFormat::Sha256, "sha256"),
    ] {
        let source = tempfile::tempdir()?;
        let path = source.path();
        git(
            path,
            &[
                "init",
                "--quiet",
                "-b",
                "main",
                &format!("--object-format={name}"),
            ],
            &[],
        )?;
        let mut commits = Vec::new();
        for sequence in 0..8 {
            std::fs::write(path.join("file"), sequence.to_string())?;
            git(path, &["add", "file"], &[])?;
            git_at(
                path,
                &["commit", "--quiet", "-m", &sequence.to_string()],
                &[],
                &format!("2000-01-{:02}T00:00:00Z", sequence + 1),
            )?;
            commits.push(text(path, &["rev-parse", "HEAD"])?);
        }
        git(path, &["branch", "old", &commits[2]], &[])?;
        git(path, &["tag", "-a", "v1", "-m", "tag"], &[])?;
        let tag = text(path, &["rev-parse", "v1"])?;
        git(
            path,
            &["checkout", "--quiet", "-b", "side", &commits[3]],
            &[],
        )?;
        std::fs::write(path.join("side"), "side")?;
        git(path, &["add", "side"], &[])?;
        git(path, &["commit", "--quiet", "-m", "side"], &[])?;
        let side = text(path, &["rev-parse", "HEAD"])?;
        git(path, &["checkout", "--quiet", "main"], &[])?;
        git(
            path,
            &["merge", "--quiet", "--no-ff", "side", "-m", "merge"],
            &[],
        )?;
        let tip = text(path, &["rev-parse", "HEAD"])?;
        let tree = text(path, &["rev-parse", "HEAD^{tree}"])?;
        let ancient = String::from_utf8(git_at(
            path,
            &["commit-tree", &tree],
            b"ancient\n",
            "1990-01-01T00:00:00Z",
        )?)?
        .trim()
        .to_owned();
        git(path, &["update-ref", "refs/heads/ancient", &ancient], &[])?;
        let cut = String::from_utf8(git_at(
            path,
            &["commit-tree", &tree, "-p", &ancient, "-p", &commits[7]],
            b"cut\n",
            "2023-01-01T00:00:00Z",
        )?)?
        .trim()
        .to_owned();
        git(path, &["update-ref", "refs/heads/cut", &cut], &[])?;
        let store =
            ValidatedBackend::new(Arc::new(InMemory::new()), StorePath::from("shallow")).await?;
        let log = Log::open(&store, &LogId::new("repository")?, Options::default()).await?;
        let mut receive = Vec::new();
        let refs = [
            ("refs/heads/main", &tip),
            ("refs/heads/old", &commits[2]),
            ("refs/heads/side", &side),
            ("refs/tags/v1", &tag),
            ("refs/heads/ancient", &ancient),
            ("refs/heads/cut", &cut),
        ];
        for (index, (reference, id)) in refs.iter().enumerate() {
            let caps = if index == 0 {
                format!("\0report-status object-format={name}")
            } else {
                String::new()
            };
            packet(
                &mut receive,
                &format!("{} {id} {reference}{caps}", "0".repeat(tip.len())),
            )?;
        }
        receive.extend_from_slice(b"0000");
        receive.extend(git(path, &["pack-objects", "--all", "--stdout"], &[])?);
        let prepared = Repository::open(&log, format)
            .await?
            .prepare_receive(TransactionId::new(), receive.into())
            .await?;
        assert!(matches!(
            prepared.publish_receive().await?.0,
            Resolution::Committed(_)
        ));
        let cases = [
            vec![format!("want {cut}"), "deepen-not HEAD".into()],
            vec![
                format!("want {cut}"),
                format!("shallow {}", commits[7]),
                format!("have {cut}"),
                "deepen-since 946944000".into(),
            ],
            vec![
                format!("want {cut}"),
                format!("shallow {}", commits[7]),
                format!("have {cut}"),
                "deepen-not ancient".into(),
            ],
            vec![format!("want {tip}"), "deepen 1".into()],
            vec![format!("want {tip}"), "deepen 3".into()],
            vec![
                format!("want {tip}"),
                format!("want {}", commits[3]),
                "deepen 2".into(),
            ],
            vec![
                format!("want {tag}"),
                "deepen 2".into(),
                "include-tag".into(),
            ],
            vec![
                format!("want {tip}"),
                format!("shallow {}", commits[6]),
                format!("shallow {side}"),
                format!("have {tip}"),
                "deepen 2".into(),
                "deepen-relative".into(),
            ],
            vec![
                format!("want {tip}"),
                format!("shallow {}", commits[6]),
                format!("shallow {side}"),
                format!("have {tip}"),
                "deepen 2147483647".into(),
            ],
            vec![
                format!("want {tip}"),
                format!("shallow {}", commits[6]),
                format!("shallow {side}"),
                format!("have {}", commits[7]),
            ],
            vec![format!("want {tip}"), "deepen-not old".into()],
            vec![format!("want {tip}"), "deepen-since 1".into()],
            vec![
                format!("want {}", commits[7]),
                "deepen-since 946944000".into(),
            ],
            vec![
                format!("want {}", commits[7]),
                format!("want {ancient}"),
                "deepen-since 946944000".into(),
            ],
            vec![
                format!("want {tip}"),
                format!("shallow {}", commits[6]),
                format!("shallow {ancient}"),
                format!("have {tip}"),
                format!("have {ancient}"),
                "deepen 2147483647".into(),
            ],
            vec![
                format!("want {tip}"),
                format!("shallow {ancient}"),
                "deepen 1".into(),
                "deepen-relative".into(),
            ],
        ];
        let negotiation = request(
            name,
            &[
                format!("want {tip}"),
                format!("have {tip}"),
                format!("shallow {}", commits[6]),
                "deepen 1".into(),
            ],
        )?;
        let ack = Repository::open(&log, format)
            .await?
            .upload_pack(negotiation.into())
            .await?;
        let mut expected_ack = Vec::new();
        packet(&mut expected_ack, "acknowledgments")?;
        packet(&mut expected_ack, &format!("ACK {tip}"))?;
        expected_ack.extend_from_slice(b"0000");
        assert_eq!(ack.as_ref(), expected_ack);
        drop(ack);
        for mut args in cases {
            args.push("done".into());
            let input = request(name, &args)?;
            let expected = reply(
                path,
                &git(path, &["upload-pack", "--stateless-rpc", ".git"], &input)?,
            )?;
            let actual = reply(
                path,
                &Repository::open(&log, format)
                    .await?
                    .upload_pack(input.into())
                    .await?,
            )?;
            assert_eq!(actual.shallow, expected.shallow, "{name} {args:?}");
            assert_eq!(actual.unshallow, expected.unshallow, "{name} {args:?}");
            assert!(
                actual.ids.is_subset(&expected.ids),
                "unexpected objects {name} {args:?}: {actual:?} vs {expected:?}"
            );
            let shallow_file = path.join("client-shallow");
            std::fs::write(
                &shallow_file,
                args.iter()
                    .filter_map(|arg| arg.strip_prefix("shallow "))
                    .collect::<Vec<_>>()
                    .join("\n")
                    + "\n",
            )?;
            let mut have_args = vec![
                "--shallow-file",
                shallow_file.to_str().ok_or("path")?,
                "rev-list",
                "--objects",
            ];
            have_args.extend(args.iter().filter_map(|arg| arg.strip_prefix("have ")));
            let present = if have_args.len() > 4 {
                git(path, &have_args, &[])?
                    .split(|byte| *byte == b'\n')
                    .filter_map(|line| {
                        std::str::from_utf8(line)
                            .ok()?
                            .split_whitespace()
                            .next()
                            .map(str::to_owned)
                    })
                    .collect::<BTreeSet<_>>()
            } else {
                BTreeSet::new()
            };
            // Git can redundantly resend shared tree/blob objects while deepening.
            for id in expected.ids.difference(&actual.ids) {
                assert!(
                    present.contains(id),
                    "omitted object not present in shallow client: {id} {name} {args:?}"
                );
            }
        }
    }
    Ok(())
}
