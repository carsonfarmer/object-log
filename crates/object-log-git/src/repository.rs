use std::{
    fmt,
    io::{self, Write},
    mem::size_of,
};

use object_log::{CommitStatus, Log, ObjectRef, PreparedCommit, StagedObject, View, materialize};

mod receive_command;

use crate::{
    Error, ObjectFormat, ObjectId, RefSnapshot,
    durable::{self, Catalog},
    format::PackDescriptor,
    pack::budget::{Operation, Pool, Reservation},
    state::{Machine, State},
    wire::{self, AdvertisedRef, FetchReply, UploadRequest},
};

use bytes::Bytes;
// Admission bounds include decoder Vec capacity and simultaneous canonical
// re-encoding. Head retention IDs can encode one byte into a 24-byte Vec;
// malformed Git ref arrays can encode two bytes into a 96-byte RefUpdate.
// The factors cover doubling capacity, the encoded buffers, and fixed headers.
const HEAD_DECODE_FACTOR: usize = 64;
const RECORD_DECODE_FACTOR: usize = 128;
const VIEW_RETAIN_FACTOR: usize = 8;
const STATE_RETAIN_FACTOR: usize = 4;

/// One exact Git repository view backed by an object log.
pub struct Repository {
    log: Log,
    format: ObjectFormat,
    view: View,
    state: State,
    operation: Operation,
    state_memory: Reservation,
    view_memory: Reservation,
}

/// One atomic ref update ready for conditional publication.
#[must_use = "publish the update or retain its recovery token"]
pub struct PreparedPush {
    log: Log,
    prepared: PreparedCommit,
    recovery_token: Bytes,
    receive: receive_command::Publication,
}

impl Repository {
    /// Loads refs and authenticated pack references from one exact durable view.
    /// Pack indexes are loaded only by commands that read objects.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid durable state, a resource limit, or object
    /// storage failure. It retries once when collection expires the first view.
    pub async fn open(log: &Log, format: ObjectFormat) -> Result<Self, Error> {
        Self::open_with_pool(log, format, Pool::shared()).await
    }

    async fn open_with_pool(log: &Log, format: ObjectFormat, pool: &Pool) -> Result<Self, Error> {
        let operation = pool.admit()?;
        match Self::open_attempt(log, format, &operation).await {
            Err(Error::ObjectLog(object_log::Error::ViewExpired)) => {
                operation.retry()?;
                Self::open_attempt(log, format, &operation).await
            }
            result => result,
        }
    }

    async fn open_attempt(
        log: &Log,
        format: ObjectFormat,
        operation: &Operation,
    ) -> Result<Self, Error> {
        // The core loads and decodes the head before returning its exact view.
        let head = log.options().max_head_bytes;
        let head_memory = operation.reserve(memory_bound(head, HEAD_DECODE_FACTOR)?)?;
        operation.io(head)?;
        operation.work(head)?;
        let view = log.load().await?;
        let view_memory = operation.reserve_state(memory_bound(head, VIEW_RETAIN_FACTOR)?)?;
        drop(head_memory);
        let materialization_memory = preflight_view(operation, &view)?;
        let materialized = materialize(log, view, &Machine::new(format))
            .await
            .map_err(|error| match error {
                object_log::MaterializeError::Log(error) => Error::ObjectLog(error),
                object_log::MaterializeError::State(error) => error,
            })?;
        let (view, state) = materialized.into_parts();
        let state_memory = operation.reserve_state(state_bytes(&state)?)?;
        drop(materialization_memory);

        Ok(Self {
            log: log.clone(),
            format,
            view,
            state,
            operation: operation.clone(),
            state_memory,
            view_memory,
        })
    }

    async fn catalog(&self) -> Result<Catalog, Error> {
        durable::load(
            &self.operation,
            &self.log,
            &self.view,
            self.format,
            &pack_roots(&self.state),
        )
        .await
    }

    #[cfg(test)]
    async fn fetch_pack(
        &self,
        wants: &[ObjectId],
        haves: &[ObjectId],
        include_tag: bool,
    ) -> Result<Bytes, Error> {
        self.fetch_pack_or_ack(wants, haves, include_tag, true)
            .await
    }

    async fn fetch_pack_or_ack(
        &self,
        wants: &[ObjectId],
        haves: &[ObjectId],
        include_tag: bool,
        done: bool,
    ) -> Result<Bytes, Error> {
        let catalog = self.catalog().await?;
        let mut reader = durable::Reader::new(&self.log, &self.view, &catalog);
        let _roots_memory = self
            .operation
            .reserve_state(self.state.refs.len() * size_of::<ObjectId>())?;
        let roots = self.state.refs.values().copied().collect::<Vec<_>>();
        let graph = crate::graph::Graph::load(&self.operation, &mut reader, &roots).await?;
        for (name, id) in &self.state.refs {
            if name.starts_with(b"refs/heads/")
                && graph.location(*id).is_none_or(|index| {
                    graph.nodes[index as usize].kind != Some(gix_object::Kind::Commit)
                })
            {
                return Err(Error::InvalidReference);
            }
        }
        let selected =
            crate::selection::select(&self.operation, &graph, wants, haves, include_tag)?;
        if !done {
            return wire_response(&self.operation, |output| {
                wire::write_fetch(
                    output,
                    self.format,
                    FetchReply::Acknowledgments(&selected.common),
                )
            });
        }
        for id in &selected.ids {
            let node = &graph.nodes[graph.location(*id).ok_or(Error::InvalidReference)? as usize];
            if !node.verified {
                let kind = reader.verify(*id).await?.ok_or(Error::InvalidReference)?;
                if Some(kind) != node.kind {
                    return crate::pack::invalid("selected graph object has the wrong kind");
                }
            }
        }
        reader.fetch_pack(&selected.ids).await
    }

    /// Returns protocol-v2 upload-pack discovery bytes for this object format.
    #[must_use]
    pub fn upload_advertisement(format: ObjectFormat) -> Bytes {
        Bytes::from_static(wire::upload_advertisement(format))
    }

    /// Serves one protocol-v2 ls-refs or fetch command from this exact view.
    /// The response is fully validated and buffered before it is returned.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid input, resource exhaustion, or storage
    /// failure. One expired-view retry shares all counters with repository open.
    pub async fn upload_pack(self, input: Bytes) -> Result<Bytes, Error> {
        if input.len() > wire::MAX_UPLOAD_BYTES {
            return Err(Error::InvalidProtocol("upload control bytes"));
        }
        let operation = self.operation.clone();
        let _input_memory = operation.reserve(input.len())?;
        // Vec growth plus into_boxed_slice can temporarily retain three copies.
        let maximum = 3 * ((1024 + 32768) * size_of::<ObjectId>() + 1024 * size_of::<&[u8]>());
        let _parse_memory = operation.reserve((input.len() * 4 + 128).min(maximum))?;
        operation.work(input.len())?;
        let request = wire::parse_upload(&input, self.format)?;
        match self.upload_attempt(&request).await {
            Err(Error::ObjectLog(object_log::Error::ViewExpired)) => {
                let (log, format) = (self.log.clone(), self.format);
                drop(self);
                operation.retry()?;
                Self::open_attempt(&log, format, &operation)
                    .await?
                    .upload_attempt(&request)
                    .await
            }
            result => result,
        }
    }

    async fn upload_attempt(&self, request: &UploadRequest<'_>) -> Result<Bytes, Error> {
        match request {
            UploadRequest::LsRefs {
                peel,
                symrefs,
                unborn,
                prefixes,
            } => self.ls_refs(*peel, *symrefs, *unborn, prefixes).await,
            UploadRequest::Fetch {
                wants,
                haves,
                done,
                include_tag,
                ..
            } => {
                let reply = self
                    .fetch_pack_or_ack(wants, haves, *include_tag, *done)
                    .await?;
                if *done {
                    wire_response(&self.operation, |output| {
                        wire::write_fetch(output, self.format, FetchReply::Pack(&reply))
                    })
                } else {
                    Ok(reply)
                }
            }
        }
    }

    async fn ls_refs(
        &self,
        peel: bool,
        symrefs: bool,
        unborn: bool,
        prefixes: &[&[u8]],
    ) -> Result<Bytes, Error> {
        let _memory = self
            .operation
            .reserve_state((self.state.refs.len() + 1) * size_of::<AdvertisedRef<'_>>())?;
        let mut refs = Vec::with_capacity(self.state.refs.len() + 1);
        let main = self.state.refs.get(&b"refs/heads/main"[..]).copied();
        if matches_prefix(&self.operation, b"HEAD", prefixes)? && (main.is_some() || unborn) {
            refs.push(AdvertisedRef {
                name: b"HEAD",
                target: main,
                peeled: None,
                symref_target: symrefs.then_some(b"refs/heads/main".as_slice()),
            });
        }
        for (name, &target) in &self.state.refs {
            self.operation.work(name.len())?;
            if matches_prefix(&self.operation, name, prefixes)? {
                refs.push(AdvertisedRef {
                    name,
                    target: Some(target),
                    peeled: None,
                    symref_target: None,
                });
            }
        }
        if peel
            && refs
                .iter()
                .any(|reference| reference.name.starts_with(b"refs/tags/"))
        {
            let catalog = self.catalog().await?;
            let mut reader = durable::Reader::new(&self.log, &self.view, &catalog);
            for reference in &mut refs {
                if reference.name.starts_with(b"refs/tags/") {
                    reference.peeled = peel_ref(
                        &self.operation,
                        &mut reader,
                        reference.target.ok_or(Error::InvalidReference)?,
                    )
                    .await?;
                }
            }
        }
        wire_response(&self.operation, |output| {
            wire::write_ls_refs(output, self.format, &refs)
        })
    }

    /// Returns the refs from the exact durable view.
    #[must_use]
    pub const fn refs(&self) -> &RefSnapshot {
        &self.state.refs
    }
}

impl fmt::Debug for Repository {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Repository")
            .field("format", &self.format)
            .field("view", &self.view)
            .field("refs", &self.state.refs)
            .field("packs", &self.state.packs.len())
            .finish_non_exhaustive()
    }
}

fn memory_bound(bytes: usize, factor: usize) -> Result<usize, Error> {
    bytes
        .checked_mul(factor)
        .ok_or_else(|| Error::InvalidPack("Git state exceeds memory".into()))
}

fn preflight_view(operation: &Operation, view: &View) -> Result<Reservation, Error> {
    let records = view
        .checkpoint()
        .into_iter()
        .map(|checkpoint| checkpoint.object().len())
        .chain(view.tail().iter().map(object_log::CommitRef::len));
    let mut total = 0_usize;
    for bytes in records {
        let bytes = usize::try_from(bytes)
            .map_err(|_| Error::InvalidPack("Git state exceeds memory".into()))?;
        total = total
            .checked_add(bytes)
            .ok_or_else(|| Error::InvalidPack("Git state exceeds memory".into()))?;
        operation.io(bytes)?;
        operation.work(bytes)?;
    }
    // Includes decoded Vec capacity, BTree nodes, proofs, and canonical re-encoding.
    // Even malformed short ref records can expand substantially during decoding.
    operation.reserve(memory_bound(total, RECORD_DECODE_FACTOR)?)
}

fn state_bytes(state: &State) -> Result<usize, Error> {
    let refs = state.refs.iter().try_fold(0_usize, |total, (name, _)| {
        total.checked_add(size_of::<(Vec<u8>, crate::ObjectId)>() + name.len())
    });
    let bytes = refs
        .and_then(|bytes| {
            bytes.checked_add(
                state.packs.len()
                    * (size_of::<(crate::ObjectId, (u64, StagedObject))>()
                        + size_of::<(PackDescriptor, ObjectRef)>()),
            )
        })
        .ok_or_else(|| Error::InvalidPack("Git state exceeds memory".into()))?;
    // A nonempty BTreeMap allocates a full root leaf even for one entry.
    let leaves = usize::from(!state.refs.is_empty()) * 12 * size_of::<(Vec<u8>, ObjectId)>()
        + usize::from(!state.packs.is_empty()) * 12 * size_of::<(ObjectId, (u64, StagedObject))>();
    memory_bound(bytes, STATE_RETAIN_FACTOR)?
        .checked_add(leaves)
        .ok_or_else(|| Error::InvalidPack("Git state exceeds memory".into()))
}

fn pack_roots(state: &State) -> Vec<(PackDescriptor, ObjectRef)> {
    state
        .packs
        .iter()
        .map(|(&id, (bytes, root))| {
            (
                PackDescriptor { id, bytes: *bytes },
                root.reference().clone(),
            )
        })
        .collect()
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

fn matches_prefix(operation: &Operation, name: &[u8], prefixes: &[&[u8]]) -> Result<bool, Error> {
    if prefixes.is_empty() {
        return Ok(true);
    }
    for prefix in prefixes {
        operation.work(prefix.len().min(name.len()))?;
        if name.starts_with(prefix) {
            return Ok(true);
        }
    }
    Ok(false)
}

async fn peel_ref(
    operation: &Operation,
    reader: &mut durable::Reader<'_>,
    id: ObjectId,
) -> Result<Option<ObjectId>, Error> {
    let mut object = reader.find(id).await?.ok_or(Error::InvalidReference)?;
    if object.kind != gix_object::Kind::Tag {
        return Ok(None);
    }
    for _ in 0..crate::pack::MAX_OBJECTS {
        operation.work(object.data.len())?;
        let tag =
            gix_object::TagRef::from_bytes(&object.data, crate::pack::object_hash(id.format()))
                .map_err(crate::pack::pack_error)?;
        let target = ObjectId::from_bytes(id.format(), tag.target().as_slice())?;
        let expected = tag.target_kind;
        object = reader.find(target).await?.ok_or(Error::InvalidReference)?;
        if object.kind != expected {
            return Err(Error::InvalidReference);
        }
        if expected != gix_object::Kind::Tag {
            return Ok(Some(target));
        }
    }
    Err(Error::InvalidObjectGraph("tag chain is too long"))
}

impl fmt::Debug for PreparedPush {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PreparedPush")
            .field("prepared", &self.prepared)
            .finish_non_exhaustive()
    }
}
struct UploadBuffer<'a> {
    bytes: Option<Vec<u8>>,
    length: usize,
    operation: &'a Operation,
}

impl Write for UploadBuffer<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let length = self
            .length
            .checked_add(bytes.len())
            .filter(|length| *length <= wire::MAX_FETCH_RESPONSE_BYTES)
            .ok_or_else(|| io::Error::other(Error::InvalidProtocol("upload response bytes")))?;
        self.operation.work(bytes.len()).map_err(io::Error::other)?;
        if let Some(output) = &mut self.bytes {
            if length > output.capacity() {
                return Err(io::Error::other(Error::InvalidProtocol(
                    "response changed during encoding",
                )));
            }
            output.extend_from_slice(bytes);
        }
        self.length = length;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn wire_response(
    operation: &Operation,
    write: impl Fn(&mut UploadBuffer<'_>) -> Result<(), wire::Error>,
) -> Result<Bytes, Error> {
    // Wire encoders build one packet line at a time, including Vec growth.
    let _scratch = operation.reserve(2 * 65536 + 256)?;
    let mut output = UploadBuffer {
        bytes: None,
        length: 0,
        operation,
    };
    write(&mut output)?;
    let memory = operation.reserve(output.length)?;
    output.bytes = Some(Vec::with_capacity(output.length));
    output.length = 0;
    write(&mut output)?;
    Ok(crate::pack::budget::hold(
        Bytes::from(
            output
                .bytes
                .ok_or(Error::InvalidProtocol("missing response buffer"))?,
        ),
        memory,
    ))
}

#[cfg(test)]
mod tests {
    use std::{
        error::Error as StdError,
        fmt::Write as _,
        fs,
        path::{Path, PathBuf},
        process::{Command, Output},
    };

    use crate::{RefUpdate, format::Record};
    use object_log::sim::{Failure, FailurePhase, FaultStore, Operation};
    use object_log::{
        CheckpointResolution, CheckpointStatus, LogId, Options, TransactionId, ValidatedBackend,
    };
    use object_store::{memory::InMemory, path::Path as StorePath};
    use tempfile::TempDir;

    use super::*;

    type TestResult<T = ()> = Result<T, Box<dyn StdError>>;
    include!("repository/receive_tests.rs");

    struct Fixture {
        directory: TempDir,
        pack: PathBuf,
        target: ObjectId,
    }

    async fn test_log(name: &str) -> TestResult<(Log, FaultStore, ValidatedBackend)> {
        let faults = FaultStore::new(InMemory::new());
        let backend = ValidatedBackend::new(
            std::sync::Arc::new(faults.clone()),
            StorePath::from("git-repository-tests"),
        )
        .await?;
        let log = Log::open(&backend, &LogId::new(name)?, Options::default()).await?;
        Ok((log, faults, backend))
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

    async fn publish_durable_pack(
        log: &Log,
        fixture: &Fixture,
        format: ObjectFormat,
    ) -> TestResult<PackDescriptor> {
        let view = log.load().await?;
        let operation = Pool::new(crate::pack::budget::LIVE_BYTES).admit()?;
        let normalized =
            crate::pack::normalize(&operation, format, &fs::read(&fixture.pack)?, &[])?;
        let (descriptor, root) = durable::stage(&operation, log, &view, normalized).await?;
        let record = Machine::new(format).transaction(
            vec![RefUpdate::new(
                "refs/heads/main",
                None,
                Some(fixture.target),
            )?],
            vec![descriptor.clone()],
        )?;
        let prepared = log.prepare(
            &view,
            TransactionId::new(),
            record,
            Bytes::new(),
            vec![root],
        )?;
        assert!(matches!(
            log.commit(prepared).await?,
            CommitStatus::Committed(_)
        ));
        Ok(descriptor)
    }

    #[test]
    fn decode_admission_covers_dense_malformed_ref_vectors() -> TestResult {
        // A missing optional pair makes [h''] the shortest decodable RefUpdate.
        assert!(2 * size_of::<RefUpdate>() + 16 <= 2 * RECORD_DECODE_FACTOR);
        assert!(2 * size_of::<Vec<u8>>() + 8 <= HEAD_DECODE_FACTOR);
        let mut bytes = minicbor::Encoder::new(Vec::new());
        bytes
            .map(5)?
            .u8(0)?
            .u32(1)?
            .u8(1)?
            .bool(true)?
            .u8(2)?
            .u8(1)?
            .u8(3)?
            .array(4096)?;
        for _ in 0..4096 {
            bytes.array(1)?.bytes(&[])?;
        }
        bytes.u8(4)?.array(0)?;
        let bytes = bytes.into_writer();
        assert!(Record::decode(&bytes, ObjectFormat::Sha1, 0).is_err());
        Ok(())
    }

    #[tokio::test]
    #[allow(
        clippy::too_many_lines,
        reason = "one both-hash fixture checks full and incremental selection"
    )]
    async fn selected_fetch_matches_git_and_includes_complete_tag_chains() -> TestResult {
        for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
            let mut fixture = fixture(format, b"before")?;
            let source = fixture.directory.path().join("source");
            let old = fixture.target;
            fs::write(source.join("file"), b"after")?;
            command(Some(&source), &["commit", "--quiet", "-am", "after"])?;
            fixture.target = ObjectId::parse(
                format,
                output(Some(&source), &["rev-parse", "HEAD"])?.trim(),
            )?;
            command(Some(&source), &["tag", "-a", "inner", "-m", "inner"])?;
            command(
                Some(&source),
                &["tag", "-a", "outer", "-m", "outer", "inner"],
            )?;
            let inner = ObjectId::parse(
                format,
                output(Some(&source), &["rev-parse", "inner"])?.trim(),
            )?;
            let outer = ObjectId::parse(
                format,
                output(Some(&source), &["rev-parse", "outer"])?.trim(),
            )?;
            command(
                Some(&source),
                &["checkout", "--quiet", "-b", "unreachable", &old.to_string()],
            )?;
            fs::write(source.join("file"), b"unrelated")?;
            command(Some(&source), &["commit", "--quiet", "-am", "unrelated"])?;
            let unreachable = ObjectId::parse(
                format,
                output(Some(&source), &["rev-parse", "HEAD"])?.trim(),
            )?;
            command(Some(&source), &["checkout", "--quiet", "main"])?;
            fs::write(
                &fixture.pack,
                command_output(Some(&source), &["pack-objects", "--all", "--stdout"])?.stdout,
            )?;
            let (log, _, _) = test_log("selected-fetch").await?;
            publish_durable_pack(&log, &fixture, format).await?;
            let view = log.load().await?;
            let update = Machine::new(format).transaction(
                vec![RefUpdate::new("refs/tags/outer", None, Some(outer))?],
                vec![],
            )?;
            let prepared =
                log.prepare(&view, TransactionId::new(), update, Bytes::new(), vec![])?;
            assert!(matches!(
                log.commit(prepared).await?,
                CommitStatus::Committed(_)
            ));
            let pool = Pool::new(crate::pack::budget::LIVE_BYTES);
            let repository = Repository::open_with_pool(&log, format, &pool).await?;
            let full = output(
                Some(&source),
                &["rev-list", "--objects", &fixture.target.to_string()],
            )?;
            let mut full = full
                .lines()
                .map(|line| ObjectId::parse(format, line.split(' ').next().unwrap_or("")))
                .collect::<Result<Vec<_>, _>>()?;
            full.sort_unstable();
            let pack = repository.fetch_pack(&[fixture.target], &[], false).await?;
            assert_selected_pack(&source, &pack, format, &full, None, fixture.target)?;
            drop(pack);
            let expected = output(
                Some(&source),
                &[
                    "rev-list",
                    "--objects",
                    &fixture.target.to_string(),
                    &format!("^{old}"),
                ],
            )?;
            let mut expected = expected
                .lines()
                .map(|line| ObjectId::parse(format, line.split(' ').next().unwrap_or("")))
                .collect::<Result<Vec<_>, _>>()?;
            expected.sort_unstable();
            let pack = repository
                .fetch_pack(
                    &[fixture.target, fixture.target],
                    &[old, old, unreachable],
                    false,
                )
                .await?;
            assert_selected_pack(&source, &pack, format, &expected, Some(old), fixture.target)?;
            drop(pack);
            expected.extend([inner, outer]);
            expected.sort_unstable();
            let pack = repository
                .fetch_pack(&[fixture.target], &[old], true)
                .await?;
            assert_selected_pack(&source, &pack, format, &expected, Some(old), fixture.target)?;
            drop(pack);
            let empty = repository
                .fetch_pack(&[fixture.target], &[fixture.target], false)
                .await?;
            assert_selected_pack(
                &source,
                &empty,
                format,
                &[],
                Some(fixture.target),
                fixture.target,
            )?;
            assert!(matches!(
                repository.fetch_pack(&[unreachable], &[], false).await,
                Err(Error::InvalidReference)
            ));
        }
        Ok(())
    }

    #[tokio::test]
    async fn selected_fetch_rejects_wrong_tree_and_tag_leaf_kinds() -> TestResult {
        for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
            for tag_leaf in [false, true] {
                let mut fixture = fixture(format, b"valid")?;
                let source = fixture.directory.path().join("source");
                let mut objects = output(Some(&source), &["rev-list", "--objects", "HEAD"])?;
                let file = source.join("raw-object");
                fs::write(&file, [])?;
                let actual_tree = output(
                    Some(&source),
                    &["hash-object", "-w", "-t", "tree", "raw-object"],
                )?;
                let actual_tree = ObjectId::parse(format, actual_tree.trim())?;
                writeln!(objects, "{actual_tree}")?;
                let (kind, data) = if tag_leaf {
                    ("tag", format!("object {actual_tree}\ntype blob\ntag bad\ntagger A <a@example.com> 0 +0000\n\nbad\n").into_bytes())
                } else {
                    let mut tree = b"100644 wrong\0".to_vec();
                    tree.extend_from_slice(actual_tree.as_bytes());
                    ("tree", tree)
                };
                fs::write(&file, data)?;
                let wrong = output(
                    Some(&source),
                    &["hash-object", "--literally", "-w", "-t", kind, "raw-object"],
                )?;
                let wrong = ObjectId::parse(format, wrong.trim())?;
                writeln!(objects, "{wrong}")?;
                let want = if tag_leaf {
                    wrong
                } else {
                    fs::write(
                        &file,
                        format!(
                            "tree {wrong}\nauthor A <a@example.com> 0 +0000\ncommitter A <a@example.com> 0 +0000\n\nbad\n"
                        ),
                    )?;
                    let commit = output(
                        Some(&source),
                        &[
                            "hash-object",
                            "--literally",
                            "-w",
                            "-t",
                            "commit",
                            "raw-object",
                        ],
                    )?;
                    fixture.target = ObjectId::parse(format, commit.trim())?;
                    writeln!(objects, "{}", fixture.target)?;
                    fixture.target
                };
                fs::write(&file, objects)?;
                let packed = Command::new("git")
                    .current_dir(&source)
                    .args(["pack-objects", "--stdout"])
                    .stdin(fs::File::open(&file)?)
                    .output()?;
                assert!(
                    packed.status.success(),
                    "{}",
                    String::from_utf8_lossy(&packed.stderr)
                );
                fs::write(&fixture.pack, packed.stdout)?;
                let (log, _, _) = test_log("wrong-selected-leaf").await?;
                publish_durable_pack(&log, &fixture, format).await?;
                if tag_leaf {
                    let view = log.load().await?;
                    let update = Machine::new(format).transaction(
                        vec![RefUpdate::new("refs/tags/bad", None, Some(wrong))?],
                        vec![],
                    )?;
                    let prepared =
                        log.prepare(&view, TransactionId::new(), update, Bytes::new(), vec![])?;
                    assert!(matches!(
                        log.commit(prepared).await?,
                        CommitStatus::Committed(_)
                    ));
                }
                let pool = Pool::new(crate::pack::budget::LIVE_BYTES);
                let repository = Repository::open_with_pool(&log, format, &pool).await?;
                assert!(matches!(repository.fetch_pack(&[want], &[], false).await,
                    Err(Error::InvalidPack(message)) if message == "selected graph object has the wrong kind"));
            }
        }
        Ok(())
    }

    fn upload_request(format: ObjectFormat, command: &str, args: &[String]) -> TestResult<Bytes> {
        let mut bytes = Vec::new();
        for line in [
            format!("command={command}"),
            format!(
                "object-format={}",
                if format == ObjectFormat::Sha1 {
                    "sha1"
                } else {
                    "sha256"
                }
            ),
        ] {
            gix_packetline::blocking_io::encode::text_to_write(line.as_bytes(), &mut bytes)?;
        }
        bytes.extend_from_slice(b"0001");
        for line in args {
            gix_packetline::blocking_io::encode::text_to_write(line.as_bytes(), &mut bytes)?;
        }
        bytes.extend_from_slice(b"0000");
        Ok(bytes.into())
    }

    fn response_pack(mut bytes: &[u8]) -> TestResult<Vec<u8>> {
        let mut pack = Vec::new();
        while !bytes.is_empty() {
            let line = gix_packetline::decode::all_at_once(bytes)?;
            let length = match line {
                gix_packetline::PacketLineRef::Data(data) => {
                    if data.first() == Some(&1) {
                        pack.extend_from_slice(&data[1..]);
                    }
                    data.len() + 4
                }
                _ => 4,
            };
            bytes = &bytes[length..];
        }
        Ok(pack)
    }

    #[tokio::test]
    async fn upload_v2_discovers_refs_negotiates_and_frames_both_hashes() -> TestResult {
        for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
            let advertisement = Repository::upload_advertisement(format);
            assert!(advertisement.starts_with(b"000eversion 2\n"));
            assert!(!advertisement.windows(9).any(|part| part == b"# service"));
            let fixture = fixture(format, b"upload v2")?;
            let (log, faults, _) = test_log("upload-v2").await?;
            let pool = Pool::new(crate::pack::budget::LIVE_BYTES);
            let empty = Repository::open_with_pool(&log, format, &pool).await?;
            let request = upload_request(format, "ls-refs", &["unborn".into(), "symrefs".into()])?;
            let reply = empty.upload_pack(request).await?;
            assert!(
                String::from_utf8_lossy(&reply)
                    .contains("unborn HEAD symref-target:refs/heads/main")
            );
            drop(reply);
            publish_durable_pack(&log, &fixture, format).await?;
            let repository = Repository::open_with_pool(&log, format, &pool).await?;
            faults.reset();
            let reply = repository
                .upload_pack(upload_request(
                    format,
                    "ls-refs",
                    &["peel".into(), "symrefs".into(), "ref-prefix HEAD".into()],
                )?)
                .await?;
            assert!(String::from_utf8_lossy(&reply).contains(&format!(
                "{} HEAD symref-target:refs/heads/main",
                fixture.target
            )));
            assert!(!String::from_utf8_lossy(&reply).contains(" refs/heads/main\n"));
            assert_eq!(faults.metrics().operation(Operation::Get).requests, 0);
            drop(reply);
            let repository = Repository::open_with_pool(&log, format, &pool).await?;
            let reply = repository
                .upload_pack(upload_request(
                    format,
                    "fetch",
                    &[
                        format!("want {}", fixture.target),
                        format!("have {}", fixture.target),
                    ],
                )?)
                .await?;
            assert!(String::from_utf8_lossy(&reply).contains(&format!("ACK {}", fixture.target)));
            assert!(!reply.windows(8).any(|part| part == b"packfile"));
            drop(reply);
            for have in [false, true] {
                let repository = Repository::open_with_pool(&log, format, &pool).await?;
                let mut args = vec![
                    format!("want {}", fixture.target),
                    "thin-pack".into(),
                    "ofs-delta".into(),
                    "include-tag".into(),
                ];
                if have {
                    args.push(format!("have {}", fixture.target));
                }
                args.push("done".into());
                let reply = repository
                    .upload_pack(upload_request(format, "fetch", &args)?)
                    .await?;
                let bytes = response_pack(&reply)?;
                let pack = gix_pack::data::File::from_data(
                    bytes.as_slice(),
                    PathBuf::new(),
                    crate::pack::object_hash(format),
                )?;
                assert_eq!(pack.num_objects(), if have { 0 } else { 3 });
                let check = Pool::new(crate::pack::budget::LIVE_BYTES).admit()?;
                crate::pack::normalize(&check, format, &bytes, &[])?;
                drop(reply);
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn upload_ls_refs_peels_full_filtered_tag_chains() -> TestResult {
        for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
            let fixture = fixture(format, b"tags")?;
            let source = fixture.directory.path().join("source");
            command(Some(&source), &["tag", "-a", "inner", "-m", "inner"])?;
            command(
                Some(&source),
                &["tag", "-a", "outer", "inner", "-m", "outer"],
            )?;
            let inner = ObjectId::parse(
                format,
                output(Some(&source), &["rev-parse", "inner"])?.trim(),
            )?;
            let outer = ObjectId::parse(
                format,
                output(Some(&source), &["rev-parse", "outer"])?.trim(),
            )?;
            fs::write(
                &fixture.pack,
                command_output(Some(&source), &["pack-objects", "--all", "--stdout"])?.stdout,
            )?;
            let (log, faults, _) = test_log("upload-tags").await?;
            publish_durable_pack(&log, &fixture, format).await?;
            let view = log.load().await?;
            let record = Machine::new(format).transaction(
                vec![
                    RefUpdate::new("refs/tags/inner", None, Some(inner))?,
                    RefUpdate::new("refs/tags/outer", None, Some(outer))?,
                ],
                vec![],
            )?;
            let prepared =
                log.prepare(&view, TransactionId::new(), record, Bytes::new(), vec![])?;
            assert!(matches!(
                log.commit(prepared).await?,
                CommitStatus::Committed(_)
            ));
            let pool = Pool::new(crate::pack::budget::LIVE_BYTES);
            let repository = Repository::open_with_pool(&log, format, &pool).await?;
            faults.reset();
            let reply = repository
                .upload_pack(upload_request(
                    format,
                    "ls-refs",
                    &["peel".into(), "ref-prefix refs/tags/outer".into()],
                )?)
                .await?;
            let text = String::from_utf8_lossy(&reply);
            assert!(text.contains(&format!(
                "{outer} refs/tags/outer peeled:{}",
                fixture.target
            )));
            assert!(!text.contains("refs/tags/inner"));
            assert_eq!(faults.metrics().operation(Operation::Get).requests, 2);
        }
        Ok(())
    }

    #[tokio::test]
    async fn upload_ls_refs_rejects_incorrect_final_tag_kind() -> TestResult {
        use std::fmt::Write as _;

        for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
            let fixture = fixture(format, b"invalid tag target")?;
            let source = fixture.directory.path().join("source");
            let target = output(Some(&source), &["rev-parse", "HEAD^{tree}"])?;
            let file = source.join("malformed-tag");
            fs::write(
                &file,
                format!(
                    "object {}\ntype blob\ntag bad\ntagger A <a@example.com> 0 +0000\n\nbad\n",
                    target.trim()
                ),
            )?;
            let bad = output(
                Some(&source),
                &[
                    "hash-object",
                    "--literally",
                    "-w",
                    "-t",
                    "tag",
                    "malformed-tag",
                ],
            )?;
            let bad = ObjectId::parse(format, bad.trim())?;
            let mut objects = output(Some(&source), &["rev-list", "--objects", "HEAD"])?;
            writeln!(objects, "{bad}")?;
            fs::write(&file, objects)?;
            let packed = Command::new("git")
                .current_dir(&source)
                .args(["pack-objects", "--stdout"])
                .stdin(fs::File::open(&file)?)
                .output()?;
            assert!(
                packed.status.success(),
                "{}",
                String::from_utf8_lossy(&packed.stderr)
            );
            fs::write(&fixture.pack, packed.stdout)?;
            let (log, _, _) = test_log("upload-wrong-tag-kind").await?;
            publish_durable_pack(&log, &fixture, format).await?;
            let view = log.load().await?;
            let record = Machine::new(format).transaction(
                vec![RefUpdate::new("refs/tags/bad", None, Some(bad))?],
                vec![],
            )?;
            let prepared =
                log.prepare(&view, TransactionId::new(), record, Bytes::new(), vec![])?;
            assert!(matches!(
                log.commit(prepared).await?,
                CommitStatus::Committed(_)
            ));
            let pool = Pool::new(crate::pack::budget::LIVE_BYTES);
            let repository = Repository::open_with_pool(&log, format, &pool).await?;
            let reply = repository
                .upload_pack(upload_request(
                    format,
                    "ls-refs",
                    &["peel".into(), "ref-prefix refs/tags/bad".into()],
                )?)
                .await;
            assert!(matches!(reply, Err(Error::InvalidReference)));
        }
        Ok(())
    }

    #[tokio::test]
    async fn upload_limits_fail_before_io_and_response_retains_its_memory() -> TestResult {
        let (log, faults, _) = test_log("upload-limits").await?;
        let pool = Pool::new(crate::pack::budget::LIVE_BYTES);
        let repository = Repository::open_with_pool(&log, ObjectFormat::Sha1, &pool).await?;
        faults.reset();
        assert!(matches!(
            repository
                .upload_pack(Bytes::from(vec![0; wire::MAX_UPLOAD_BYTES + 1]))
                .await,
            Err(Error::InvalidProtocol("upload control bytes"))
        ));
        assert_eq!(faults.metrics().operation(Operation::Get).requests, 0);
        let repository = Repository::open_with_pool(&log, ObjectFormat::Sha1, &pool).await?;
        let operation = repository.operation.clone();
        let reply = repository
            .upload_pack(upload_request(ObjectFormat::Sha1, "ls-refs", &[])?)
            .await?;
        assert_eq!(&reply[..], b"0000");
        assert_eq!(operation.live_bytes(), reply.len());
        drop(operation);
        assert!(matches!(pool.admit(), Err(Error::Busy)));
        drop(reply);
        assert!(pool.admit().is_ok());
        Ok(())
    }

    #[tokio::test]
    async fn upload_expired_view_reopens_once_with_the_original_budget() -> TestResult {
        for spent in [false, true] {
            let old = fixture(ObjectFormat::Sha1, b"old")?;
            let new = fixture(ObjectFormat::Sha1, b"new")?;
            let (log, _, _) = test_log("upload-expiry").await?;
            publish_durable_pack(&log, &old, ObjectFormat::Sha1).await?;
            let pool = Pool::new(crate::pack::budget::LIVE_BYTES);
            let repository = Repository::open_with_pool(&log, ObjectFormat::Sha1, &pool).await?;
            let operation = repository.operation.clone();
            if spent {
                operation.retry()?;
            }
            let before = operation.calls();
            let view = log.load().await?;
            let staging = Pool::new(crate::pack::budget::LIVE_BYTES).admit()?;
            let normalized =
                crate::pack::normalize(&staging, ObjectFormat::Sha1, &fs::read(&new.pack)?, &[])?;
            let (descriptor, root) = durable::stage(&staging, &log, &view, normalized).await?;
            let record = Machine::new(ObjectFormat::Sha1).transaction(
                vec![RefUpdate::new(
                    "refs/heads/main",
                    Some(old.target),
                    Some(new.target),
                )?],
                vec![descriptor],
            )?;
            let prepared = log.prepare(
                &view,
                TransactionId::new(),
                record,
                Bytes::new(),
                vec![root],
            )?;
            assert!(matches!(
                log.commit(prepared).await?,
                CommitStatus::Committed(_)
            ));
            let checkpoint = common_open(&log, ObjectFormat::Sha1).await?;
            let CheckpointStatus::Published(view) = checkpoint.checkpoint().await? else {
                return Err("checkpoint failed".into());
            };
            let object_log::CollectionStart::Installed(view, _) =
                log.start_collection(&view).await?
            else {
                return Err("collection failed".into());
            };
            assert!(matches!(
                log.resume_collection(&view).await?,
                object_log::CollectionFinish::Complete(_, _)
            ));
            let result = repository
                .upload_pack(upload_request(
                    ObjectFormat::Sha1,
                    "fetch",
                    &[format!("want {}", new.target), "done".into()],
                )?)
                .await;
            if spent {
                assert!(
                    matches!(result, Err(Error::InvalidPack(message)) if message == "Git retry limit exceeded")
                );
            } else {
                let reply = result?;
                assert!(response_pack(&reply)?.starts_with(b"PACK"));
            }
            assert!(operation.calls() > before);
            assert!(operation.retry().is_err());
        }
        Ok(())
    }

    fn assert_selected_pack(
        source: &Path,
        bytes: &[u8],
        format: ObjectFormat,
        expected: &[ObjectId],
        have: Option<ObjectId>,
        target: ObjectId,
    ) -> TestResult {
        let pool = Pool::new(crate::pack::budget::LIVE_BYTES);
        let operation = pool.admit()?;
        let normalized = crate::pack::normalize(&operation, format, bytes, &[])?;
        let index = gix_pack::index::File::from_data(
            normalized.index.as_slice(),
            PathBuf::new(),
            crate::pack::object_hash(format),
        )?;
        let ids = index
            .iter()
            .map(|entry| ObjectId::from_bytes(format, entry.oid.as_slice()))
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(ids, expected);
        let receiver = tempfile::tempdir()?;
        command(
            Some(receiver.path()),
            &[
                "init",
                "--bare",
                "--quiet",
                &format!(
                    "--object-format={}",
                    match format {
                        ObjectFormat::Sha1 => "sha1",
                        ObjectFormat::Sha256 => "sha256",
                    }
                ),
            ],
        )?;
        let file = receiver.path().join("fetch-validation.pack");
        if let Some(have) = have {
            let history = output(Some(source), &["rev-list", "--objects", &have.to_string()])?;
            let input = receiver.path().join("have-objects");
            fs::write(&input, history)?;
            let seed = Command::new("git")
                .current_dir(source)
                .args(["pack-objects", "--stdout"])
                .stdin(fs::File::open(input)?)
                .output()?;
            assert!(seed.status.success());
            fs::write(&file, seed.stdout)?;
            let result = Command::new("git")
                .current_dir(receiver.path())
                .args([
                    "index-pack",
                    "--stdin",
                    "--strict",
                    "--check-self-contained-and-connected",
                ])
                .stdin(fs::File::open(&file)?)
                .output()?;
            assert!(
                result.status.success(),
                "{}",
                String::from_utf8_lossy(&result.stderr)
            );
        }
        fs::write(&file, bytes)?;
        let mut command = Command::new("git");
        command
            .current_dir(receiver.path())
            .args(["index-pack", "--stdin", "--strict"]);
        let result = command.stdin(fs::File::open(&file)?).output()?;
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        let result = command
            .arg("--check-self-contained-and-connected")
            .stdin(fs::File::open(&file)?)
            .output()?;
        // Git returns one for graph links supplied by the receiver's have set,
        // even though strict validation succeeds and all delta bases are in-pack.
        assert_eq!(
            result.status.code(),
            Some(i32::from(have.is_some() && !expected.is_empty())),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        super::tests::command(
            Some(receiver.path()),
            &["update-ref", "refs/heads/fetched", &target.to_string()],
        )?;
        super::tests::command(Some(receiver.path()), &["fsck", "--strict", "--no-reflogs"])?;
        Ok(())
    }

    #[tokio::test]
    async fn common_open_accepts_384_transaction_history() -> TestResult {
        let fixture = fixture(ObjectFormat::Sha1, b"history")?;
        let (log, _, _) = test_log("history-384").await?;
        publish_durable_pack(&log, &fixture, ObjectFormat::Sha1).await?;
        for index in 0..383 {
            let view = log.load().await?;
            let (expected, target) = if index % 2 == 0 {
                (None, Some(fixture.target))
            } else {
                (Some(fixture.target), None)
            };
            let bytes = Machine::new(ObjectFormat::Sha1).transaction(
                vec![RefUpdate::new("refs/tags/changing", expected, target)?],
                vec![],
            )?;
            let prepared = log.prepare(&view, TransactionId::new(), bytes, Bytes::new(), vec![])?;
            assert!(matches!(
                log.commit(prepared).await?,
                CommitStatus::Committed(_)
            ));
        }
        let pool = Pool::new(crate::pack::budget::LIVE_BYTES);
        let repository = Repository::open_with_pool(&log, ObjectFormat::Sha1, &pool).await?;
        assert_eq!(repository.view.tail().len(), 384);
        assert_eq!(
            repository.refs().get(&b"refs/heads/main"[..]),
            Some(&fixture.target)
        );
        drop(repository);
        assert!(pool.admit().is_ok());
        Ok(())
    }

    #[tokio::test]
    async fn materialization_rejects_before_tail_reads_without_resetting_budget() -> TestResult {
        let fixture = fixture(ObjectFormat::Sha1, b"preflight")?;
        let (log, faults, _) = test_log("materialize-preflight").await?;
        publish_durable_pack(&log, &fixture, ObjectFormat::Sha1).await?;
        let view = log.load().await?;
        faults.reset();
        let operation = Pool::new(0).admit()?;
        assert!(preflight_view(&operation, &view).is_err());
        assert_eq!(faults.metrics().operation(Operation::Get).requests, 0);
        assert_eq!(operation.calls(), view.tail().len());
        assert_eq!(operation.live_bytes(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn head_transfer_is_charged_before_read_and_after_retry() -> TestResult {
        let (log, faults, _) = test_log("head-budget").await?;
        let operation = Pool::new(crate::pack::budget::LIVE_BYTES).admit()?;
        operation.io(crate::pack::budget::TRANSFER_BYTES - log.options().max_head_bytes)?;
        faults.reset();
        let repository = Repository::open_attempt(&log, ObjectFormat::Sha1, &operation).await?;
        assert_eq!(faults.metrics().operation(Operation::Get).requests, 1);
        drop(repository);
        operation.retry()?;
        faults.reset();
        assert!(
            Repository::open_attempt(&log, ObjectFormat::Sha1, &operation)
                .await
                .is_err()
        );
        assert_eq!(faults.metrics().operation(Operation::Get).requests, 0);
        assert_eq!(operation.live_bytes(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn common_repository_retains_one_authenticated_exact_view() -> TestResult {
        let fixture = fixture(ObjectFormat::Sha1, b"common repository")?;
        let (log, faults, _) = test_log("repository-common-view").await?;
        publish_durable_pack(&log, &fixture, ObjectFormat::Sha1).await?;
        faults.reset();
        let pool = Pool::new(crate::pack::budget::LIVE_BYTES);
        let repository = Repository::open_with_pool(&log, ObjectFormat::Sha1, &pool).await?;
        assert_eq!(
            repository.refs().get(&b"refs/heads/main"[..]),
            Some(&fixture.target)
        );
        assert_eq!(repository.state.packs.len(), 1);
        assert!(
            faults.metrics().events.iter().all(|event| {
                event.operation != Operation::Get || !event.path.contains("/nodes/")
            })
        );
        assert!(
            faults.metrics().events.iter().all(|event| {
                event.operation != Operation::Get || !event.path.contains("/blobs/")
            })
        );

        let view = log.load().await?;
        let record = Machine::new(ObjectFormat::Sha1).transaction(
            vec![RefUpdate::new("refs/tags/v1", None, Some(fixture.target))?],
            vec![],
        )?;
        let prepared = log.prepare(&view, TransactionId::new(), record, Bytes::new(), vec![])?;
        assert!(matches!(
            log.commit(prepared).await?,
            CommitStatus::Committed(_)
        ));
        assert!(!repository.refs().contains_key(&b"refs/tags/v1"[..]));
        assert!(pool.admit().is_err());
        drop(repository);
        assert!(pool.admit().is_ok());
        Ok(())
    }

    #[tokio::test]
    async fn common_repository_preflights_state_and_releases_cancelled_load() -> TestResult {
        let fixture = fixture(ObjectFormat::Sha1, b"bounded repository")?;
        let (log, faults, _) = test_log("repository-common-bounds").await?;
        publish_durable_pack(&log, &fixture, ObjectFormat::Sha1).await?;

        faults.reset();
        let bounded = Pool::new(0);
        assert!(
            Repository::open_with_pool(&log, ObjectFormat::Sha1, &bounded)
                .await
                .is_err()
        );
        assert_eq!(faults.metrics().operation(Operation::Get).requests, 0);
        assert!(bounded.admit().is_ok());

        faults.reset();
        let mut pause = faults.pause_get_at(2, FailurePhase::Before);
        let pool = Pool::new(crate::pack::budget::LIVE_BYTES);
        {
            let opening = Repository::open_with_pool(&log, ObjectFormat::Sha1, &pool);
            tokio::pin!(opening);
            assert!(tokio::select! {
                entered = pause.wait_until_entered() => entered,
                _ = &mut opening => false,
            });
        }
        assert!(!pause.release());
        assert!(pool.admit().is_ok());
        Ok(())
    }

    #[tokio::test]
    async fn checkpoint_preserves_conflict_and_pending_outcomes() -> TestResult {
        let conflict_fixture = fixture(ObjectFormat::Sha1, b"checkpoint")?;
        let (log, _, _) = test_log("repository-checkpoint-status").await?;
        let initial = common_open(&log, ObjectFormat::Sha1).await?;
        let push = initial
            .prepare_receive(
                TransactionId::new(),
                receive_input(
                    ObjectFormat::Sha1,
                    &[RefUpdate::new(
                        "refs/heads/main",
                        None,
                        Some(conflict_fixture.target),
                    )?],
                    &fs::read(&conflict_fixture.pack)?,
                    true,
                ),
            )
            .await?;
        assert!(matches!(push.publish().await?, CommitStatus::Committed(_)));

        let first = common_open(&log, ObjectFormat::Sha1).await?;
        let second = common_open(&log, ObjectFormat::Sha1).await?;
        assert!(matches!(
            first.checkpoint().await?,
            CheckpointStatus::Published(_)
        ));
        assert!(matches!(
            second.checkpoint().await?,
            CheckpointStatus::Conflict(_)
        ));

        let second_fixture = fixture(ObjectFormat::Sha1, b"pending-checkpoint")?;
        let (second_log, second_faults, _) = test_log("repository-checkpoint-pending").await?;
        let initial = common_open(&second_log, ObjectFormat::Sha1).await?;
        let push = initial
            .prepare_receive(
                TransactionId::new(),
                receive_input(
                    ObjectFormat::Sha1,
                    &[RefUpdate::new(
                        "refs/heads/main",
                        None,
                        Some(second_fixture.target),
                    )?],
                    &fs::read(&second_fixture.pack)?,
                    true,
                ),
            )
            .await?;
        assert!(matches!(push.publish().await?, CommitStatus::Committed(_)));
        let checkpoint = common_open(&second_log, ObjectFormat::Sha1).await?;
        second_faults.reset();
        second_faults.schedule(Failure {
            operation: Operation::Put,
            occurrence: 2,
            phase: FailurePhase::After,
        });
        let CheckpointStatus::Pending(pending) = checkpoint.checkpoint().await? else {
            return Err("checkpoint response was not lost".into());
        };
        assert!(matches!(
            second_log.resolve_checkpoint(pending).await?,
            CheckpointResolution::Published(_)
        ));
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
