use std::{collections::BTreeMap, path::Path};

use gix::{
    bstr::BString,
    hash::ObjectId,
    refs::{
        FullName, Target,
        transaction::{Change, LogChange, PreviousValue, RefEdit, RefLog},
    },
};

use super::Error;
use crate::{ObjectFormat, RefSnapshot, RefUpdate};

const HEAD: &str = "refs/heads/main";

#[derive(Clone, Copy)]
enum RefKind {
    Branch,
    Tag,
}

pub(super) fn init(path: &Path, format: ObjectFormat) -> Result<gix::Repository, Error> {
    let options = gix::create::Options {
        object_hash: Some(format.into()),
        ..Default::default()
    };
    let repo = gix::ThreadSafeRepository::init(path, gix::create::Kind::Bare, options)
        .map_err(error)?
        .to_thread_local();
    let head = FullName::try_from(HEAD).map_err(error)?;
    commit(
        &repo,
        [RefEdit {
            name: FullName::try_from("HEAD").map_err(error)?,
            deref: false,
            change: Change::Update {
                expected: PreviousValue::Any,
                new: Target::Symbolic(head),
                log: LogChange::default(),
            },
        }],
    )?;
    validate_repository(&repo, format)?;
    Ok(repo)
}

pub(super) fn open(path: &Path, format: ObjectFormat) -> Result<gix::Repository, Error> {
    let options = gix::open::Options::isolated()
        .strict_config(true)
        .open_path_as_is(true);
    let repo = gix::open_opts(path, options).map_err(error)?;
    validate_repository(&repo, format)?;
    Ok(repo)
}

pub(super) fn apply(repo: &gix::Repository, updates: &[RefUpdate]) -> Result<(), Error> {
    let edits = updates
        .iter()
        .map(|update| prepare(repo, update))
        .collect::<Result<Vec<_>, _>>()?;
    commit(repo, edits)
}

pub(super) fn materialize(repo: &gix::Repository, desired: &RefSnapshot) -> Result<(), Error> {
    let current = snapshot(repo)?;
    let mut edits = Vec::with_capacity(current.len() + desired.len());
    for (name, target) in desired {
        let (name, kind) = ref_name(name)?;
        let target = ObjectId::try_from(*target).map_err(|_| Error::InvalidReference)?;
        verify_target(repo, kind, target)?;
        let previous = current
            .get::<[u8]>(name.as_bstr().as_ref())
            .copied()
            .map(ObjectId::try_from)
            .transpose()
            .map_err(|_| Error::InvalidReference)?;
        if previous != Some(target) {
            edits.push(edit(name, previous, Some(target))?);
        }
    }
    for (name, target) in current {
        if !desired.contains_key(&name) {
            let (name, _) = ref_name(&name)?;
            edits.push(edit(
                name,
                Some(ObjectId::try_from(target).map_err(|_| Error::InvalidReference)?),
                None,
            )?);
        }
    }
    commit(repo, edits)
}

pub(super) fn snapshot(repo: &gix::Repository) -> Result<RefSnapshot, Error> {
    let platform = repo.references().map_err(error)?;
    let references = platform.all().map_err(error)?;
    let mut snapshot = BTreeMap::new();
    for reference in references {
        let reference = reference.map_err(error)?;
        ref_name(reference.name().as_bstr().as_ref())?;
        let id = reference
            .try_id()
            .ok_or(Error::UnsupportedRepository)?
            .detach()
            .try_into()
            .map_err(|_| Error::InvalidReference)?;
        snapshot.insert(reference.name().as_bstr().to_vec(), id);
    }
    Ok(snapshot)
}

fn validate_repository(repo: &gix::Repository, format: ObjectFormat) -> Result<(), Error> {
    if !repo.is_bare() || repo.object_hash() != format.into() || repo.is_shallow() {
        return Err(Error::UnsupportedRepository);
    }
    let head = repo.head_name().map_err(error)?;
    if head.as_ref().map(FullName::as_bstr).map(AsRef::as_ref) != Some(HEAD.as_bytes()) {
        return Err(Error::UnsupportedRepository);
    }
    snapshot(repo)?;
    Ok(())
}

fn prepare(repo: &gix::Repository, update: &RefUpdate) -> Result<RefEdit, Error> {
    let (name, kind) = ref_name(&update.name)?;
    let current = direct_target(repo, &name)?;
    let expected = update
        .expected
        .map(ObjectId::try_from)
        .transpose()
        .map_err(|_| Error::InvalidReference)?;
    if current != expected {
        return Err(Error::StaleReference);
    }
    let target = update
        .target
        .map(ObjectId::try_from)
        .transpose()
        .map_err(|_| Error::InvalidReference)?;
    if let Some(target) = target {
        verify_target(repo, kind, target)?;
        if matches!(kind, RefKind::Branch)
            && let Some(current) = current
            && !is_ancestor(repo, current, target)?
        {
            return Err(Error::NonFastForward);
        }
    }
    edit(name, current, target)
}

fn ref_name(value: &[u8]) -> Result<(FullName, RefKind), Error> {
    let name = FullName::try_from(BString::from(value)).map_err(|_| Error::InvalidReference)?;
    let value: &[u8] = name.as_bstr().as_ref();
    let kind = if value.starts_with(b"refs/heads/") {
        RefKind::Branch
    } else if value.starts_with(b"refs/tags/") {
        RefKind::Tag
    } else {
        return Err(Error::InvalidReference);
    };
    Ok((name, kind))
}

fn direct_target(repo: &gix::Repository, name: &FullName) -> Result<Option<ObjectId>, Error> {
    repo.try_find_reference(name)
        .map_err(error)?
        .map(|reference| {
            reference
                .try_id()
                .map(gix::Id::detach)
                .ok_or(Error::UnsupportedRepository)
        })
        .transpose()
}

fn verify_target(repo: &gix::Repository, kind: RefKind, target: ObjectId) -> Result<(), Error> {
    match kind {
        RefKind::Branch => repo.find_commit(target).map(|_| ()).map_err(error),
        RefKind::Tag => repo.find_object(target).map(|_| ()).map_err(error),
    }
}

fn is_ancestor(repo: &gix::Repository, old: ObjectId, new: ObjectId) -> Result<bool, Error> {
    match repo.merge_base(old, new) {
        Ok(base) => Ok(base.detach() == old),
        Err(gix::repository::merge_base::Error::NotFound { .. }) => Ok(false),
        Err(cause) => Err(error(cause)),
    }
}

fn edit(
    name: FullName,
    previous: Option<ObjectId>,
    target: Option<ObjectId>,
) -> Result<RefEdit, Error> {
    let expected = previous.map_or(PreviousValue::MustNotExist, |id| {
        PreviousValue::MustExistAndMatch(Target::Object(id))
    });
    let change = match target {
        Some(id) => Change::Update {
            expected,
            new: Target::Object(id),
            log: LogChange::default(),
        },
        None if previous.is_some() => Change::Delete {
            expected,
            log: RefLog::AndReference,
        },
        None => return Err(Error::StaleReference),
    };
    Ok(RefEdit {
        name,
        deref: false,
        change,
    })
}

fn commit(repo: &gix::Repository, edits: impl IntoIterator<Item = RefEdit>) -> Result<(), Error> {
    repo.edit_references_as(edits, None)
        .map(|_| ())
        .map_err(error)
}

fn error(value: impl std::fmt::Display) -> Error {
    Error::Repository(value.to_string())
}

#[cfg(test)]
mod tests {
    use std::{error::Error as StdError, fs, process::Command};

    use super::*;
    use crate::ObjectId as RecordObjectId;

    #[test]
    fn creates_opens_and_materializes_both_formats() -> Result<(), Box<dyn StdError>> {
        for (name, format) in [
            ("sha1", ObjectFormat::Sha1),
            ("sha256", ObjectFormat::Sha256),
        ] {
            let fixture = fixture(name, format)?;
            let path = fixture.root.path().join("bare");
            let repo = init(&path, format)?;
            import(&repo, &fixture.work)?;
            let desired = RefSnapshot::from([
                (b"refs/heads/main".to_vec(), fixture.first),
                (b"refs/tags/blob".to_vec(), fixture.blob),
            ]);
            materialize(&repo, &desired)?;

            assert_eq!(snapshot(&repo)?, desired);
            assert_eq!(snapshot(&open(&path, format)?)?, desired);
            let other = match format {
                ObjectFormat::Sha1 => ObjectFormat::Sha256,
                ObjectFormat::Sha256 => ObjectFormat::Sha1,
            };
            assert!(open(&path, other).is_err());
            assert_eq!(
                git(Some(&path), &["rev-parse", "refs/heads/main"])?,
                fixture.first.to_string()
            );
        }
        Ok(())
    }

    #[test]
    fn enforces_ref_and_branch_policy() -> Result<(), Box<dyn StdError>> {
        let fixture = fixture("sha1", ObjectFormat::Sha1)?;
        let path = fixture.root.path().join("bare");
        let repo = init(&path, ObjectFormat::Sha1)?;
        import(&repo, &fixture.work)?;
        materialize(
            &repo,
            &RefSnapshot::from([
                (b"refs/heads/main".to_vec(), fixture.first),
                (b"refs/tags/blob".to_vec(), fixture.blob),
            ]),
        )?;

        let invalid = RefUpdate::new("refs/heads/bad..name", None, Some(fixture.first))?;
        assert!(matches!(
            apply(&repo, &[invalid]),
            Err(Error::InvalidReference)
        ));
        let missing = RecordObjectId::from_bytes(ObjectFormat::Sha1, &[9; 20])?;
        let missing = RefUpdate::new("refs/heads/missing", None, Some(missing))?;
        assert!(apply(&repo, &[missing]).is_err());

        apply(
            &repo,
            &[RefUpdate::new(
                "refs/heads/main",
                Some(fixture.first),
                Some(fixture.second),
            )?],
        )?;
        let stale = RefUpdate::new("refs/heads/main", Some(fixture.first), Some(fixture.side))?;
        assert!(matches!(apply(&repo, &[stale]), Err(Error::StaleReference)));
        let non_fast_forward =
            RefUpdate::new("refs/heads/main", Some(fixture.second), Some(fixture.side))?;
        assert!(matches!(
            apply(&repo, &[non_fast_forward]),
            Err(Error::NonFastForward)
        ));

        apply(
            &repo,
            &[RefUpdate::new("refs/tags/blob", Some(fixture.blob), None)?],
        )?;
        assert!(!snapshot(&repo)?.contains_key(&b"refs/tags/blob"[..]));
        Ok(())
    }

    struct Fixture {
        root: tempfile::TempDir,
        work: std::path::PathBuf,
        first: RecordObjectId,
        second: RecordObjectId,
        side: RecordObjectId,
        blob: RecordObjectId,
    }

    fn fixture(name: &str, format: ObjectFormat) -> Result<Fixture, Box<dyn StdError>> {
        let root = tempfile::tempdir()?;
        let work = root.path().join("work");
        let format_arg = format!("--object-format={name}");
        git(
            Some(root.path()),
            &["init", "--quiet", "-b", "main", &format_arg, "work"],
        )?;
        fs::write(work.join("file"), b"one\n")?;
        git(Some(&work), &["add", "file"])?;
        git(Some(&work), &["commit", "--quiet", "-m", "one"])?;
        fs::write(work.join("file"), b"two\n")?;
        git(Some(&work), &["commit", "--quiet", "-am", "two"])?;
        git(
            Some(&work),
            &["checkout", "--quiet", "-b", "side", "HEAD~1"],
        )?;
        fs::write(work.join("side"), b"side\n")?;
        git(Some(&work), &["add", "side"])?;
        git(Some(&work), &["commit", "--quiet", "-m", "side"])?;
        Ok(Fixture {
            first: parse(&work, format, "main~1")?,
            second: parse(&work, format, "main")?,
            side: parse(&work, format, "side")?,
            blob: parse(&work, format, "main:file")?,
            root,
            work,
        })
    }

    fn import(repo: &gix::Repository, source: &Path) -> Result<(), Box<dyn StdError>> {
        let source = source
            .to_str()
            .ok_or_else(|| "test path is not UTF-8".to_string())?;
        git(
            Some(repo.git_dir()),
            &[
                "fetch",
                "--quiet",
                "--no-write-fetch-head",
                source,
                "refs/heads/main",
                "refs/heads/side",
            ],
        )?;
        Ok(())
    }

    fn parse(
        path: &Path,
        format: ObjectFormat,
        revision: &str,
    ) -> Result<RecordObjectId, Box<dyn StdError>> {
        Ok(RecordObjectId::parse(
            format,
            &git(Some(path), &["rev-parse", revision])?,
        )?)
    }

    fn git(directory: Option<&Path>, args: &[&str]) -> Result<String, Box<dyn StdError>> {
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
        let output = command.output()?;
        if output.status.success() {
            Ok(String::from_utf8(output.stdout)?.trim().to_owned())
        } else {
            Err(String::from_utf8_lossy(&output.stderr).into_owned().into())
        }
    }
}
