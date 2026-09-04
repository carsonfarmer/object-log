use std::{
    fs,
    io::{BufReader, Read, Seek},
    path::{Path, PathBuf},
    sync::atomic::AtomicBool,
};

use gix::hash::{Kind, ObjectId};

use crate::{Error as RecordError, ObjectFormat, ObjectId as RecordObjectId};

mod local;

const PACK_HEADER_LEN: usize = 12;
const MAX_PACK_BYTES: usize = 512 * 1024 * 1024;
const MAX_OBJECT_BYTES: usize = 256 * 1024 * 1024;
const MAX_OBJECTS: u32 = 1_000_000;

impl TryFrom<Kind> for ObjectFormat {
    type Error = RecordError;

    fn try_from(value: Kind) -> Result<Self, Self::Error> {
        match value {
            Kind::Sha1 => Ok(Self::Sha1),
            Kind::Sha256 => Ok(Self::Sha256),
            _ => Err(RecordError::InvalidObjectId),
        }
    }
}

impl From<ObjectFormat> for Kind {
    fn from(value: ObjectFormat) -> Self {
        match value {
            ObjectFormat::Sha1 => Self::Sha1,
            ObjectFormat::Sha256 => Self::Sha256,
        }
    }
}

impl TryFrom<ObjectId> for RecordObjectId {
    type Error = RecordError;

    fn try_from(value: ObjectId) -> Result<Self, Self::Error> {
        Self::from_bytes(value.kind().try_into()?, value.as_slice())
    }
}

impl TryFrom<RecordObjectId> for ObjectId {
    type Error = RecordError;

    fn try_from(value: RecordObjectId) -> Result<Self, Self::Error> {
        Self::try_from(value.as_bytes()).map_err(|_| RecordError::InvalidObjectId)
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    #[error("invalid pack: {0}")]
    InvalidPack(String),
    #[error("pack operation failed: {0}")]
    Pack(String),
    #[error("repository must be bare")]
    NotBare,
    #[error("repository state is unsupported")]
    UnsupportedRepository,
    #[error("invalid reference")]
    InvalidReference,
    #[error("reference changed")]
    StaleReference,
    #[error("branch update is not a fast-forward")]
    NonFastForward,
    #[error("repository operation failed: {0}")]
    Repository(String),
    #[error("I/O failed for {path}: {source}")]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct NormalizedPack {
    pub(crate) id: ObjectId,
    pub(crate) path: PathBuf,
    pub(crate) objects: Vec<ObjectId>,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct IndexedPack {
    pub(crate) id: ObjectId,
    pub(crate) objects: Vec<ObjectId>,
}

/// Validate and normalize a received pack. Thin-pack bases come from `repo`.
/// The normalized, self-contained pack is also installed in `repo`.
pub(crate) fn normalize_pack(repo: &gix::Repository, path: &Path) -> Result<NormalizedPack, Error> {
    let outcome = write_pack(repo, path, true)?;
    let path = outcome
        .data_path
        .clone()
        .ok_or_else(|| Error::Pack("gix did not return a pack path".into()))?;
    let indexed = indexed_outcome(repo, &outcome);
    let cleanup = remove_keep(outcome.keep_path.as_deref());
    let indexed = indexed?;
    cleanup?;
    Ok(NormalizedPack {
        id: indexed.id,
        path,
        objects: indexed.objects,
    })
}

/// Install and index a self-contained pack that was returned by `normalize_pack`.
pub(crate) fn install_pack(repo: &gix::Repository, path: &Path) -> Result<IndexedPack, Error> {
    let outcome = write_pack(repo, path, false)?;
    let indexed = indexed_outcome(repo, &outcome);
    let cleanup = remove_keep(outcome.keep_path.as_deref());
    cleanup?;
    indexed
}

/// List all standard pack indexes and the object IDs in each pack.
pub(crate) fn list_pack_objects(repo: &gix::Repository) -> Result<Vec<IndexedPack>, Error> {
    let directory = pack_directory(repo)?;
    let entries = fs::read_dir(&directory).map_err(|source| Error::Io {
        path: directory.clone(),
        source,
    })?;
    let mut indexes = Vec::new();
    for entry in entries {
        let path = entry
            .map_err(|source| Error::Io {
                path: directory.clone(),
                source,
            })?
            .path();
        if path.extension().is_some_and(|extension| extension == "idx") {
            indexes.push(path);
        }
    }
    indexes.sort();
    indexes
        .into_iter()
        .map(|path| read_index(&path, repo.object_hash()))
        .collect()
}

/// Resolve every object through the repository object database.
pub(crate) fn verify_object_access(
    repo: &gix::Repository,
    objects: &[ObjectId],
) -> Result<(), Error> {
    if objects.len() > MAX_OBJECTS as usize {
        return Err(Error::InvalidPack("too many indexed objects".into()));
    }
    for id in objects {
        repo.find_object(*id)
            .map_err(|error| Error::Pack(error.to_string()))?;
    }
    Ok(())
}

fn write_pack(
    repo: &gix::Repository,
    path: &Path,
    resolve_thin: bool,
) -> Result<gix_pack::bundle::write::Outcome, Error> {
    let mut file = fs::File::open(path).map_err(|source| Error::Io {
        path: path.to_owned(),
        source,
    })?;
    let length = file
        .metadata()
        .map_err(|source| Error::Io {
            path: path.to_owned(),
            source,
        })?
        .len();
    let mut header = [0; PACK_HEADER_LEN];
    file.read_exact(&mut header).map_err(|source| Error::Io {
        path: path.to_owned(),
        source,
    })?;
    validate_header(&header, length)?;
    file.rewind().map_err(|source| Error::Io {
        path: path.to_owned(),
        source,
    })?;
    let directory = pack_directory(repo)?;
    fs::create_dir_all(&directory).map_err(|source| Error::Io {
        path: directory.clone(),
        source,
    })?;
    let mut reader = BufReader::new(file);
    let mut progress = gix::progress::Discard;
    let interrupt = AtomicBool::new(false);
    let options = gix_pack::bundle::write::Options {
        thread_limit: Some(1),
        alloc_limit_bytes: Some(MAX_OBJECT_BYTES),
        ..Default::default()
    };
    let bases = resolve_thin.then_some(repo);
    let outcome = gix_pack::Bundle::write_to_directory(
        &mut reader,
        Some(&directory),
        &mut progress,
        &interrupt,
        bases,
        repo.object_hash(),
        options,
    )
    .map_err(|error| Error::Pack(error.to_string()))?;
    if outcome.index.num_objects > MAX_OBJECTS {
        remove_keep(outcome.keep_path.as_deref())?;
        return Err(Error::InvalidPack("too many normalized objects".into()));
    }
    Ok(outcome)
}

fn validate_header(header: &[u8; PACK_HEADER_LEN], length: u64) -> Result<(), Error> {
    if length > MAX_PACK_BYTES as u64 {
        return Err(Error::InvalidPack("pack exceeds the byte limit".into()));
    }
    let (version, objects) = gix_pack::data::header::decode(header)
        .map_err(|error| Error::InvalidPack(error.to_string()))?;
    if version != gix_pack::data::Version::V2 {
        return Err(Error::InvalidPack(
            "only pack version 2 is supported".into(),
        ));
    }
    if objects > MAX_OBJECTS {
        return Err(Error::InvalidPack("pack has too many objects".into()));
    }
    Ok(())
}

fn indexed_outcome(
    repo: &gix::Repository,
    outcome: &gix_pack::bundle::write::Outcome,
) -> Result<IndexedPack, Error> {
    let path = outcome
        .index_path
        .as_deref()
        .ok_or_else(|| Error::Pack("gix did not return an index path".into()))?;
    read_index(path, repo.object_hash())
}

fn read_index(path: &Path, hash: Kind) -> Result<IndexedPack, Error> {
    let index =
        gix_pack::index::File::at(path, hash).map_err(|error| Error::Pack(error.to_string()))?;
    index
        .verify_checksum(&mut gix::progress::Discard, &AtomicBool::new(false))
        .map_err(|error| Error::Pack(error.to_string()))?;
    let objects = index.iter().map(|entry| entry.oid).collect::<Vec<_>>();
    if objects.len() > MAX_OBJECTS as usize {
        return Err(Error::InvalidPack("index has too many objects".into()));
    }
    Ok(IndexedPack {
        id: index.pack_checksum(),
        objects,
    })
}

fn pack_directory(repo: &gix::Repository) -> Result<PathBuf, Error> {
    if !repo.is_bare() {
        return Err(Error::NotBare);
    }
    Ok(repo.git_dir().join("objects/pack"))
}

fn remove_keep(path: Option<&Path>) -> Result<(), Error> {
    let Some(path) = path else { return Ok(()) };
    fs::remove_file(path).map_err(|source| Error::Io {
        path: path.to_owned(),
        source,
    })
}

#[cfg(test)]
mod tests {
    use std::{
        error::Error as StdError,
        ffi::OsStr,
        io::Write,
        process::{Command, Stdio},
    };

    use super::*;

    #[test]
    fn normalizes_installs_and_reads_sha1_and_sha256() -> Result<(), Box<dyn StdError>> {
        for (name, hash) in [("sha1", Kind::Sha1), ("sha256", Kind::Sha256)] {
            let fixture = fixture(name)?;
            let source = bare(fixture.path().join("source"), hash)?;
            let normalized = normalize_pack(&source, &fixture.pack)?;
            assert!(!normalized.objects.is_empty());
            verify_object_access(&source, &normalized.objects)?;
            let record_id = RecordObjectId::try_from(normalized.id)?;
            assert_eq!(ObjectId::try_from(record_id)?, normalized.id);
            assert_eq!(record_id.format(), ObjectFormat::try_from(hash)?);

            let installed = bare(fixture.path().join("installed"), hash)?;
            let result = install_pack(&installed, &normalized.path)?;
            assert_eq!(result.id, normalized.id);
            assert_eq!(result.objects, normalized.objects);
            assert_eq!(list_pack_objects(&installed)?, vec![result]);
            verify_object_access(&installed, &normalized.objects)?;
        }
        Ok(())
    }

    #[test]
    fn normalizes_a_thin_pack_with_a_repository_base() -> Result<(), Box<dyn StdError>> {
        let fixture = fixture("sha1")?;
        let repo = bare(fixture.path().join("target"), Kind::Sha1)?;
        normalize_pack(&repo, &fixture.pack)?;
        let thin = thin_pack(&fixture.path().join("work"))?;
        let header: &[u8; PACK_HEADER_LEN] = thin
            .get(..PACK_HEADER_LEN)
            .and_then(|bytes| bytes.try_into().ok())
            .ok_or_else(|| "thin pack has no header".to_string())?;
        let (_, thin_objects) = gix_pack::data::header::decode(header)?;
        let path = fixture.path().join("thin.pack");
        fs::write(&path, thin)?;

        let normalized = normalize_pack(&repo, &path)?;
        assert!(normalized.objects.len() > thin_objects as usize);
        verify_object_access(&repo, &normalized.objects)?;
        Ok(())
    }

    #[test]
    fn rejects_corruption_and_excess_object_count() -> Result<(), Box<dyn StdError>> {
        let fixture = fixture("sha1")?;
        let repo = bare(fixture.path().join("target"), Kind::Sha1)?;
        let corrupt_path = fixture.path().join("corrupt.pack");
        let mut corrupt = fs::read(&fixture.pack)?;
        let last = corrupt
            .last_mut()
            .ok_or_else(|| "fixture pack is empty".to_string())?;
        *last ^= 1;
        fs::write(&corrupt_path, corrupt)?;
        assert!(install_pack(&repo, &corrupt_path).is_err());

        let header_path = fixture.path().join("header.pack");
        let mut header =
            gix_pack::data::header::encode(gix_pack::data::Version::V2, MAX_OBJECTS + 1);
        fs::write(&header_path, header)?;
        assert!(install_pack(&repo, &header_path).is_err());
        header[4..8].copy_from_slice(&3_u32.to_be_bytes());
        fs::write(&header_path, header)?;
        assert!(install_pack(&repo, &header_path).is_err());
        Ok(())
    }

    struct Fixture {
        directory: tempfile::TempDir,
        pack: PathBuf,
    }

    impl Fixture {
        fn path(&self) -> &Path {
            self.directory.path()
        }
    }

    fn fixture(format: &str) -> Result<Fixture, Box<dyn StdError>> {
        let directory = tempfile::tempdir()?;
        let work = directory.path().join("work");
        git([
            OsStr::new("init"),
            OsStr::new("--quiet"),
            OsStr::new("--object-format"),
            OsStr::new(format),
            work.as_os_str(),
        ])?;
        let mut contents = vec![b'a'; 64 * 1024];
        fs::write(work.join("file"), &contents)?;
        git_in(&work, ["add", "file"])?;
        git_in(&work, ["commit", "--quiet", "-m", "one"])?;
        let midpoint = contents.len() / 2;
        contents[midpoint] = b'b';
        fs::write(work.join("file"), contents)?;
        git_in(&work, ["commit", "--quiet", "-am", "two"])?;
        git_in(&work, ["repack", "-ad"])?;
        let pack_directory = work.join(".git/objects/pack");
        let pack_path = fs::read_dir(pack_directory)?
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .find(|path| {
                path.extension()
                    .is_some_and(|extension| extension == "pack")
            })
            .ok_or_else(|| "git did not create a pack".to_string())?;
        Ok(Fixture {
            pack: pack_path,
            directory,
        })
    }

    fn bare(path: PathBuf, hash: Kind) -> Result<gix::Repository, Box<dyn StdError>> {
        let options = gix::create::Options {
            object_hash: Some(hash),
            ..Default::default()
        };
        Ok(
            gix::ThreadSafeRepository::init(path, gix::create::Kind::Bare, options)?
                .to_thread_local(),
        )
    }

    fn git<const N: usize>(args: [&OsStr; N]) -> Result<(), Box<dyn StdError>> {
        command(None, args)
    }

    fn git_in<const N: usize>(directory: &Path, args: [&str; N]) -> Result<(), Box<dyn StdError>> {
        command(Some(directory), args.map(OsStr::new))
    }

    fn thin_pack(directory: &Path) -> Result<Vec<u8>, Box<dyn StdError>> {
        let mut child = configured_git(Some(directory))
            .args(["pack-objects", "--thin", "--stdout", "--revs"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()?;
        child
            .stdin
            .as_mut()
            .ok_or_else(|| "git stdin is not available".to_string())?
            .write_all(b"HEAD\n^HEAD~1\n")?;
        let output = child.wait_with_output()?;
        check(&output)?;
        Ok(output.stdout)
    }

    fn command<const N: usize>(
        directory: Option<&Path>,
        args: [&OsStr; N],
    ) -> Result<(), Box<dyn StdError>> {
        let output = configured_git(directory).args(args).output()?;
        check(&output)
    }

    fn configured_git(directory: Option<&Path>) -> Command {
        let mut command = Command::new("git");
        command
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
        command
    }

    fn check(output: &std::process::Output) -> Result<(), Box<dyn StdError>> {
        if output.status.success() {
            Ok(())
        } else {
            Err(String::from_utf8_lossy(&output.stderr).into_owned().into())
        }
    }
}
