use super::Result;
use object_log_git::ObjectFormat;
use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
    process::{Command, Stdio},
};

pub struct Fixture {
    pub _directory: tempfile::TempDir,
    pub source: PathBuf,
    pub format: ObjectFormat,
    pub tip: String,
    pub have: Option<String>,
    pub seed_pack: Vec<u8>,
}
pub fn git(path: &Path, args: &[&str], input: &[u8]) -> Result<Vec<u8>> {
    let mut child = Command::new("git")
        .current_dir(path)
        .args(args)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_AUTHOR_NAME", "Object Log")
        .env("GIT_AUTHOR_EMAIL", "object-log@example.invalid")
        .env("GIT_COMMITTER_NAME", "Object Log")
        .env("GIT_COMMITTER_EMAIL", "object-log@example.invalid")
        .env("GIT_AUTHOR_DATE", "2000-01-01T00:00:00Z")
        .env("GIT_COMMITTER_DATE", "2000-01-01T00:00:00Z")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let mut stdin = child.stdin.take().ok_or("missing Git stdin")?;
    // Large pack input and stdout must drain concurrently.
    let output = std::thread::scope(|scope| {
        let writer = scope.spawn(move || stdin.write_all(input));
        let output = child.wait_with_output();
        writer
            .join()
            .map_err(|_| std::io::Error::other("Git stdin writer panicked"))??;
        output
    })?;
    if !output.status.success() {
        return Err(format!("git {args:?}: {}", String::from_utf8_lossy(&output.stderr)).into());
    }
    Ok(output.stdout)
}
pub fn hash_name(format: ObjectFormat) -> &'static str {
    match format {
        ObjectFormat::Sha1 => "sha1",
        ObjectFormat::Sha256 => "sha256",
    }
}
fn text(bytes: Vec<u8>) -> Result<String> {
    Ok(String::from_utf8(bytes)?.trim().to_owned())
}
impl Fixture {
    pub fn new(format: ObjectFormat, label: &str) -> Result<Self> {
        let directory = tempfile::tempdir()?;
        let source = directory.path().join("source");
        git(
            directory.path(),
            &[
                "init",
                "-q",
                "-b",
                "main",
                &format!("--object-format={}", hash_name(format)),
                "source",
            ],
            &[],
        )?;
        git(&source, &["config", "pack.threads", "1"], &[])?;
        let size = if label == "8mib" {
            8 * 1024 * 1024
        } else {
            4096
        };
        let mut state = 17_u64;
        let mut contents: Vec<u8> = (0..size)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                state.to_le_bytes()[0]
            })
            .collect();
        let commits = if matches!(label, "history" | "incremental" | "thin") {
            384
        } else {
            1
        };
        let mut tip = String::new();
        for i in 0_u64..commits {
            contents[..8].copy_from_slice(&i.to_le_bytes());
            fs::write(source.join("file"), &contents)?;
            git(&source, &["add", "file"], &[])?;
            let tree = text(git(&source, &["write-tree"], &[])?)?;
            let mut args = vec!["commit-tree", &tree];
            if !tip.is_empty() {
                args.extend(["-p", &tip]);
            }
            tip = text(git(&source, &args, format!("commit {i}\n").as_bytes())?)?;
        }
        git(&source, &["update-ref", "refs/heads/main", &tip], &[])?;
        let have = matches!(label, "incremental" | "thin").then(|| tip.clone());
        let seed_pack = git(
            &source,
            &["pack-objects", "--stdout", "--revs"],
            format!("{tip}\n").as_bytes(),
        )?;
        if have.is_some() {
            contents[16..24].copy_from_slice(&999_u64.to_le_bytes());
            fs::write(source.join("file"), &contents)?;
            git(&source, &["add", "file"], &[])?;
            let tree = text(git(&source, &["write-tree"], &[])?)?;
            tip = text(git(
                &source,
                &["commit-tree", &tree, "-p", &tip],
                b"incremental\n",
            )?)?;
            git(&source, &["update-ref", "refs/heads/main", &tip], &[])?;
        }
        Ok(Self {
            _directory: directory,
            source,
            format,
            tip,
            have,
            seed_pack,
        })
    }
    pub fn revisions(&self) -> Vec<u8> {
        let mut revisions = format!("{}\n", self.tip);
        if let Some(have) = &self.have {
            revisions.push('^');
            revisions.push_str(have);
            revisions.push('\n');
        }
        revisions.into_bytes()
    }
    pub fn oracle(&self, thin: bool) -> Result<Vec<u8>> {
        let mut args = vec!["pack-objects", "--stdout", "--revs"];
        if thin {
            args.push("--thin");
        }
        git(&self.source, &args, &self.revisions())
    }
}
