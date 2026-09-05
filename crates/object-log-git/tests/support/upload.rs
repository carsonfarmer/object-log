//! Installed Git protocol oracle helpers shared by upload feature tests.
use std::{
    collections::BTreeSet,
    io::Write,
    path::Path,
    process::{Command, Stdio},
};

pub(crate) type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

pub(crate) fn git(path: &Path, args: &[&str], input: &[u8]) -> TestResult<Vec<u8>> {
    git_at(path, args, input, "2000-01-01T00:00:00Z")
}
pub(crate) fn git_at(path: &Path, args: &[&str], input: &[u8], date: &str) -> TestResult<Vec<u8>> {
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
pub(crate) fn text(path: &Path, args: &[&str]) -> TestResult<String> {
    Ok(String::from_utf8(git(path, args, &[])?)?.trim().to_owned())
}
pub(crate) fn packet(out: &mut Vec<u8>, line: &str) -> TestResult {
    writeln!(out, "{:04x}{line}", line.len() + 5)?;
    Ok(())
}
pub(crate) fn request(format: &str, args: &[String]) -> TestResult<Vec<u8>> {
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
pub(crate) struct Reply {
    pub(crate) shallow: BTreeSet<String>,
    pub(crate) unshallow: BTreeSet<String>,
    pub(crate) ids: BTreeSet<String>,
}
pub(crate) fn reply(path: &Path, mut bytes: &[u8]) -> TestResult<Reply> {
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
