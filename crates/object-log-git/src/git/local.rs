use std::{collections::BTreeMap, path::Path};

use gix::{
    bstr::BString,
    hash::ObjectId,
    refs::{
        FullName, Target,
        transaction::{Change, LogChange, PreviousValue, RefEdit, RefLog},
    },
};

use super::{Error, ObjectSet};
use crate::{ObjectFormat, RefSnapshot, RefUpdate};

const HEAD: &str = "refs/heads/main";
const MAX_GRAPH_DEPTH: usize = 65_536;
const MAX_GRAPH_BYTES: u64 = 1024 * 1024 * 1024;

#[derive(Clone, Copy)]
enum RefKind {
    Branch,
    Tag,
}

pub(crate) fn init(path: &Path, format: ObjectFormat) -> Result<gix::Repository, Error> {
    let options = gix::create::Options {
        object_hash: Some(format.into()),
        ..Default::default()
    };
    let repo = gix::ThreadSafeRepository::init_opts(
        path,
        gix::create::Kind::Bare,
        options,
        open_options(),
    )
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

pub(crate) fn open(path: &Path, format: ObjectFormat) -> Result<gix::Repository, Error> {
    let repo = gix::open_opts(path, open_options()).map_err(error)?;
    validate_repository(&repo, format)?;
    Ok(repo)
}

pub(super) fn apply_at(
    repo: &gix::Repository,
    current: &RefSnapshot,
    updates: &[RefUpdate],
    objects: &ObjectSet,
) -> Result<(), Error> {
    verify_updates(repo, current, updates, objects)?;
    let edits = updates
        .iter()
        .map(|update| {
            let (name, _) = ref_name(&update.name)?;
            let previous = update
                .expected
                .map(ObjectId::try_from)
                .transpose()
                .map_err(|_| Error::InvalidReference)?;
            let target = update
                .target
                .map(ObjectId::try_from)
                .transpose()
                .map_err(|_| Error::InvalidReference)?;
            edit(name, previous, target)
        })
        .collect::<Result<Vec<_>, _>>()?;
    commit(repo, edits)
}

pub(crate) fn materialize(repo: &gix::Repository, desired: &RefSnapshot) -> Result<(), Error> {
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

pub(crate) fn verify_updates(
    repo: &gix::Repository,
    current: &RefSnapshot,
    updates: &[RefUpdate],
    objects: &ObjectSet,
) -> Result<(), Error> {
    let mut roots = Vec::with_capacity(updates.len());
    let mut ancestry = Vec::new();
    for update in updates {
        let (_, kind) = ref_name(&update.name)?;
        if current.get(update.name.as_slice()).copied() != update.expected {
            return Err(Error::StaleReference);
        }
        let Some(target) = update.target else {
            continue;
        };
        let target = ObjectId::try_from(target).map_err(|_| Error::InvalidReference)?;
        if matches!(kind, RefKind::Branch)
            && let Some(old) = update.expected
        {
            ancestry.push((
                ObjectId::try_from(old).map_err(|_| Error::InvalidReference)?,
                target,
            ));
        }
        let expected = matches!(kind, RefKind::Branch).then_some(gix::objs::Kind::Commit);
        roots.push((target, expected));
    }
    verify_graph(repo, roots, objects)?;
    for (old, new) in ancestry {
        if !is_ancestor(repo, old, new)? {
            return Err(Error::NonFastForward);
        }
    }
    Ok(())
}

pub(crate) fn validate_snapshot(
    repo: &gix::Repository,
    refs: &RefSnapshot,
    objects: &ObjectSet,
) -> Result<(), Error> {
    let roots = refs
        .iter()
        .map(|(name, target)| {
            let (_, kind) = ref_name(name)?;
            let target = ObjectId::try_from(*target).map_err(|_| Error::InvalidReference)?;
            let expected = matches!(kind, RefKind::Branch).then_some(gix::objs::Kind::Commit);
            Ok((target, expected))
        })
        .collect::<Result<Vec<_>, Error>>()?;
    verify_graph(repo, roots, objects)
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
    reject_alternates(repo)?;
    let head = repo.head_name().map_err(error)?;
    if head.as_ref().map(FullName::as_bstr).map(AsRef::as_ref) != Some(HEAD.as_bytes()) {
        return Err(Error::UnsupportedRepository);
    }
    snapshot(repo)?;
    Ok(())
}

fn open_options() -> gix::open::Options {
    gix::open::Options::isolated()
        .strict_config(true)
        .open_path_as_is(true)
        .object_store_slots(gix::odb::store::init::Slots::Given(1_024))
        .config_overrides([
            format!("gitoxide.objects.allocLimit={}", super::MAX_OBJECT_BYTES),
            "gitoxide.objects.noReplace=true".to_owned(),
        ])
}

fn reject_alternates(repo: &gix::Repository) -> Result<(), Error> {
    for name in ["alternates", "http-alternates"] {
        let path = repo.git_dir().join("objects/info").join(name);
        if path
            .try_exists()
            .map_err(|source| Error::Io { path, source })?
        {
            return Err(Error::UnsupportedRepository);
        }
    }
    Ok(())
}

fn verify_graph(
    repo: &gix::Repository,
    roots: Vec<(ObjectId, Option<gix::objs::Kind>)>,
    objects: &ObjectSet,
) -> Result<(), Error> {
    let mut pending = roots
        .into_iter()
        .map(|(id, kind)| (id, kind, 0))
        .collect::<Vec<_>>();
    let mut seen = BTreeMap::new();
    let mut bytes = 0_u64;
    while let Some((id, expected, depth)) = pending.pop() {
        if depth > MAX_GRAPH_DEPTH {
            return Err(Error::InvalidObjectGraph("depth limit exceeded"));
        }
        if let Some(kind) = seen.get(&id) {
            if expected.is_some_and(|expected| expected != *kind) {
                return Err(Error::InvalidObjectGraph(
                    "object kind conflicts with its edge",
                ));
            }
            continue;
        }
        if !objects.contains(&id) {
            return Err(Error::InvalidObjectGraph(
                "object is not in a verified pack",
            ));
        }
        let object = repo.find_object(id).map_err(error)?;
        if object.data.len() > super::MAX_OBJECT_BYTES
            || expected.is_some_and(|expected| expected != object.kind)
        {
            return Err(Error::InvalidObjectGraph("object kind or size is invalid"));
        }
        bytes = bytes
            .checked_add(object.data.len() as u64)
            .filter(|bytes| *bytes <= MAX_GRAPH_BYTES)
            .ok_or(Error::InvalidObjectGraph("decoded byte limit exceeded"))?;
        let data = gix::objs::Data::new(&object.data, object.kind, id.kind());
        data.verify_checksum(id.as_ref()).map_err(error)?;
        seen.insert(id, object.kind);
        let next = depth + 1;
        match data.decode().map_err(error)? {
            gix::objs::ObjectRef::Commit(commit) => {
                pending.push((commit.tree(), Some(gix::objs::Kind::Tree), next));
                pending.extend(
                    commit
                        .parents()
                        .map(|id| (id, Some(gix::objs::Kind::Commit), next)),
                );
            }
            gix::objs::ObjectRef::Tree(tree) => {
                pending.extend(tree.entries.into_iter().filter_map(|entry| {
                    use gix::objs::tree::EntryKind;
                    let kind = match entry.mode.kind() {
                        EntryKind::Tree => gix::objs::Kind::Tree,
                        EntryKind::Blob | EntryKind::BlobExecutable | EntryKind::Link => {
                            gix::objs::Kind::Blob
                        }
                        EntryKind::Commit => return None,
                    };
                    Some((entry.oid.to_owned(), Some(kind), next))
                }));
            }
            gix::objs::ObjectRef::Tag(tag) => {
                pending.push((tag.target(), Some(tag.target_kind), next));
            }
            gix::objs::ObjectRef::Blob(_) => {}
        }
    }
    Ok(())
}

fn ref_name(value: &[u8]) -> Result<(FullName, RefKind), Error> {
    std::str::from_utf8(value).map_err(|_| Error::InvalidReference)?;
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

pub(crate) fn validate_ref_name(value: &[u8]) -> Result<(), Error> {
    ref_name(value).map(|_| ())
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
            verify_updates(
                &repo,
                &RefSnapshot::new(),
                &[RefUpdate::new(
                    "refs/heads/main",
                    None,
                    Some(fixture.second),
                )?],
                &fixture.objects,
            )?;
            let desired = RefSnapshot::from([
                (b"refs/heads/main".to_vec(), fixture.first),
                (b"refs/tags/blob".to_vec(), fixture.blob),
            ]);
            validate_snapshot(&repo, &desired, &fixture.objects)?;
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
    fn rejects_missing_and_corrupt_reachable_objects() -> Result<(), Box<dyn StdError>> {
        for revision in ["main^{tree}", "main:file"] {
            let fixture = fixture("sha1", ObjectFormat::Sha1)?;
            let missing = parse(&fixture.work, ObjectFormat::Sha1, revision)?;
            fs::remove_file(loose_path(&fixture.work, missing))?;
            assert!(verify_main(&fixture).is_err());
            let repo = gix::open_opts(fixture.work.join(".git"), open_options())?;
            let refs = RefSnapshot::from([(b"refs/heads/main".to_vec(), fixture.second)]);
            assert!(validate_snapshot(&repo, &refs, &fixture.objects).is_err());
        }

        let fixture = fixture("sha1", ObjectFormat::Sha1)?;
        let blob = loose_path(&fixture.work, fixture.blob);
        fs::remove_file(&blob)?;
        fs::write(blob, b"corrupt")?;
        assert!(verify_main(&fixture).is_err());
        Ok(())
    }

    #[test]
    fn enforces_ref_and_branch_policy() -> Result<(), Box<dyn StdError>> {
        assert!(validate_ref_name(b"refs/tags/\xff").is_err());
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

        let missing = RecordObjectId::from_bytes(ObjectFormat::Sha1, &[9; 20])?;
        let missing = RefUpdate::new("refs/heads/missing", None, Some(missing))?;
        assert!(apply(&repo, &fixture.objects, &[missing]).is_err());

        apply(
            &repo,
            &fixture.objects,
            &[RefUpdate::new(
                "refs/heads/main",
                Some(fixture.first),
                Some(fixture.second),
            )?],
        )?;
        let stale = RefUpdate::new("refs/heads/main", Some(fixture.first), Some(fixture.side))?;
        assert!(matches!(
            apply(&repo, &fixture.objects, &[stale]),
            Err(Error::StaleReference)
        ));
        let non_fast_forward =
            RefUpdate::new("refs/heads/main", Some(fixture.second), Some(fixture.side))?;
        assert!(matches!(
            apply(&repo, &fixture.objects, &[non_fast_forward]),
            Err(Error::NonFastForward)
        ));

        apply(
            &repo,
            &fixture.objects,
            &[RefUpdate::new("refs/tags/blob", Some(fixture.blob), None)?],
        )?;
        assert!(!snapshot(&repo)?.contains_key(&b"refs/tags/blob"[..]));
        Ok(())
    }

    fn apply(
        repo: &gix::Repository,
        objects: &ObjectSet,
        updates: &[RefUpdate],
    ) -> Result<(), Error> {
        apply_at(repo, &snapshot(repo)?, updates, objects)
    }

    struct Fixture {
        root: tempfile::TempDir,
        work: std::path::PathBuf,
        first: RecordObjectId,
        second: RecordObjectId,
        side: RecordObjectId,
        blob: RecordObjectId,
        objects: ObjectSet,
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
        let objects = object_set(&work, format)?;
        Ok(Fixture {
            first: parse(&work, format, "main~1")?,
            second: parse(&work, format, "main")?,
            side: parse(&work, format, "side")?,
            blob: parse(&work, format, "main:file")?,
            objects,
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

    fn verify_main(fixture: &Fixture) -> Result<(), Box<dyn StdError>> {
        let repo = gix::open_opts(fixture.work.join(".git"), open_options())?;
        verify_updates(
            &repo,
            &RefSnapshot::new(),
            &[RefUpdate::new(
                "refs/heads/main",
                None,
                Some(fixture.second),
            )?],
            &fixture.objects,
        )?;
        Ok(())
    }

    fn object_set(path: &Path, format: ObjectFormat) -> Result<ObjectSet, Box<dyn StdError>> {
        let mut objects = ObjectSet::new();
        for line in git(Some(path), &["rev-list", "--objects", "--all"])?.lines() {
            let id = line
                .split_whitespace()
                .next()
                .ok_or_else(|| "object listing contains an empty line".to_string())?;
            objects.insert(ObjectId::try_from(RecordObjectId::parse(format, id)?)?);
        }
        Ok(objects)
    }

    fn loose_path(work: &Path, id: RecordObjectId) -> std::path::PathBuf {
        let id = id.to_string();
        work.join(".git/objects").join(&id[..2]).join(&id[2..])
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
