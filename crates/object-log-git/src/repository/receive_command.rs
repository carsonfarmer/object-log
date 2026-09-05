#[path = "receive_stream.rs"]
mod streamed;

use super::catalog_reuse::Reuse;
use std::io::Write as _;

use bytes::Bytes;
use gix_object::Kind;
use object_log::{CommitStatus, Resolution, TransactionId};

use super::{PreparedPush, Repository, pack_roots, wire_response};
use crate::{
    Error, ObjectId, ReceivePolicy, RefUpdate,
    closure::{CONNECTED, Closure, Edges},
    durable,
    pack::budget::{Operation, Reservation, hold},
    wire::{self, ReceiveControls, ReceiveStatus},
};

pub(super) struct Publication {
    // All allocations and the admission permit survive preparation/publication.
    _operation: Operation,
    _memory: Vec<Reservation>,
    responses: [Bytes; 4],
}

impl Repository {
    /// Advertises classic receive-pack refs and capabilities from one exact view.
    ///
    /// # Errors
    /// Returns an error for invalid durable state or an exhausted resource budget.
    #[allow(
        clippy::unused_async,
        reason = "uniform asynchronous command API for HTTP adapters"
    )]
    pub async fn receive_advertisement(self) -> Result<Bytes, Error> {
        let _memory = self.operation.reserve_state(
            self.state.refs.len() * std::mem::size_of::<wire::AdvertisedRef<'_>>(),
        )?;
        let refs = self
            .state
            .refs
            .iter()
            .map(|(name, &target)| wire::AdvertisedRef {
                name,
                target: Some(target),
                peeled: None,
                symref_target: None,
            })
            .collect::<Vec<_>>();
        wire_response(&self.operation, |out| {
            out.write_all(b"001f# service=git-receive-pack\n0000")?;
            wire::write_receive_advertisement(out, self.format, &refs)
        })
    }

    /// Validates and stages a classic receive-pack command for atomic publication.
    ///
    /// Existing branches require fast-forward updates. Use
    /// [`Self::prepare_receive_with_policy`] to explicitly allow rewritten history.
    ///
    /// The caller must open the repository before collecting a bounded request
    /// body. The complete input may contain at most 1 MiB of commands and 9 MiB
    /// of pack data. Failed validation can leave collectible immutable staging.
    ///
    /// # Errors
    /// Malformed controls return a protocol error. Valid commands rejected before
    /// publication return [`Error::ReceiveRejected`] with their report-status bytes.
    pub async fn prepare_receive(
        self,
        transaction_id: TransactionId,
        input: Bytes,
    ) -> Result<PreparedPush, Error> {
        self.prepare_receive_with_policy(transaction_id, input, ReceivePolicy::default())
            .await
    }

    /// Validates and stages receive-pack with an explicit server branch policy.
    ///
    /// Allowing non-fast-forward updates does not bypass old-ID comparison,
    /// connectivity, branch object-kind checks, or atomic publication. The policy
    /// applies to this request, including its one possible expired-view retry.
    /// Admission and input limits match [`Self::prepare_receive`].
    ///
    /// # Errors
    /// Returns the same malformed-input and rejected-command errors as
    /// [`Self::prepare_receive`].
    pub async fn prepare_receive_with_policy(
        mut self,
        transaction_id: TransactionId,
        input: Bytes,
        policy: ReceivePolicy,
    ) -> Result<PreparedPush, Error> {
        if input.len() > wire::MAX_RECEIVE_BYTES + crate::pack::MAX_RECEIVE_PACK_BYTES {
            return Err(Error::InvalidProtocol("receive input bytes"));
        }
        let operation = self.operation.clone();
        let input_memory = operation.reserve(input.len())?;
        // Names, commands, sorting scratch, and capacity growth in the parser.
        let command_memory =
            operation.reserve_state(input.len().min(wire::MAX_RECEIVE_BYTES) * 4 + 1024)?;
        operation.work(input.len())?;
        let parsed = wire::parse_receive(&input, self.format)?;
        let pack = parsed.pack;
        let request = ReceiveControls {
            updates: parsed.updates,
            report_status: parsed.report_status,
        };
        loop {
            match self
                .prepare_receive_attempt(transaction_id, &request, pack, policy)
                .await
            {
                Ok((prepared, prepared_memory)) => {
                    let result = self.finish_receive(&request, prepared, prepared_memory);
                    drop(input_memory);
                    drop(command_memory);
                    return result;
                }
                Err(Error::ObjectLog(object_log::Error::ViewExpired)) => {
                    operation.retry()?;
                    let log = self.log.clone();
                    let format = self.format;
                    drop(self);
                    self = Self::open_attempt(&log, format, &operation).await?;
                }
                Err(source) => return reject_receive(&operation, &request, source),
            }
        }
    }

    fn validate_receive_controls(
        &self,
        transaction_id: TransactionId,
        request: &ReceiveControls,
    ) -> Result<(), Error> {
        self.log.preflight(&self.view, transaction_id)?;
        for update in &request.updates {
            if self.state.refs.get(&update.name).copied() != update.expected {
                return Err(Error::StaleReference);
            }
        }
        validate_namespace(&self.operation, &self.state.refs, &request.updates)?;
        Ok(())
    }

    fn finish_receive(
        self,
        request: &ReceiveControls,
        prepared: object_log::PreparedCommit,
        prepared_memory: Reservation,
    ) -> Result<PreparedPush, Error> {
        let operation = self.operation.clone();
        let responses = [
            response(&operation, request, ReceiveStatus::Success)?,
            response(
                &operation,
                request,
                ReceiveStatus::Rejected(b"atomic ref conflict"),
            )?,
            response(
                &operation,
                request,
                ReceiveStatus::Rejected(b"publication pending"),
            )?,
            response(
                &operation,
                request,
                ReceiveStatus::Rejected(b"publication evidence expired"),
            )?,
        ];
        // Both publication and its one immediate resolution are admitted
        // before the head can change. Same-process proofs avoid pack reads.
        let options = self.log.options();
        let publication_memory =
            operation.reserve(publication_bytes(options, super::HEAD_DECODE_FACTOR)?)?;
        for _ in 0..2 {
            operation.work(options.max_commit_bytes + options.max_head_bytes * 2)?;
        }
        // A pending commit can read the same plan during resolution.
        drop(durable::publication_plan(&operation, &self.view)?);
        let plan_memory = durable::publication_plan(&operation, &self.view)?;
        let token_memory = operation.reserve(publication_bytes(options, 4)?)?;
        let recovery_token = hold(prepared.recovery_token()?, token_memory);
        Ok(PreparedPush {
            log: self.log,
            prepared,
            recovery_token,
            receive: Publication {
                _operation: operation,
                _memory: vec![
                    self.state_memory,
                    self.view_memory,
                    prepared_memory,
                    publication_memory,
                    plan_memory,
                ],
                responses,
            },
        })
    }

    async fn prepare_receive_attempt(
        &self,
        transaction_id: TransactionId,
        request: &ReceiveControls,
        pack: &[u8],
        policy: ReceivePolicy,
    ) -> Result<(object_log::PreparedCommit, Reservation), Error> {
        self.validate_receive_controls(transaction_id, request)?;
        let staged = if pack.is_empty() {
            None
        } else {
            let catalog = self.catalog().await?;
            let mut reader = durable::Reader::new(&self.log, &self.view, &catalog);
            let normalized =
                crate::receive::normalize(&self.operation, self.format, pack, &mut reader).await?;
            let reusable = self.reuse_catalog_objects(&normalized).await?;
            if normalized.bytes.get(8..12) == Some(&[0, 0, 0, 0])
                || matches!(reusable, Reuse::KnownObjects)
            {
                None
            } else {
                drop(reader);
                drop(catalog);
                Some(match reusable {
                    Reuse::Pack(location) => (location.descriptor, location.root),
                    Reuse::Stage => {
                        durable::stage(&self.operation, &self.log, &self.view, normalized).await?
                    }
                    Reuse::KnownObjects => unreachable!(),
                })
            }
        };
        self.prepare_staged_receive(transaction_id, request, staged, policy, None)
            .await
    }

    async fn prepare_staged_receive(
        &self,
        transaction_id: TransactionId,
        request: &ReceiveControls,
        staged: Option<(crate::format::PackDescriptor, object_log::StagedObject)>,
        policy: ReceivePolicy,
        certificate: Option<&crate::pack::ingest::ScanCertificate<'_>>,
    ) -> Result<(object_log::PreparedCommit, Reservation), Error> {
        self.validate_receive_controls(transaction_id, request)?;
        let mut objects = Vec::new();
        let mut descriptors = Vec::new();
        let mut tree = self.state.catalog_tree(self.format);
        if let Some((descriptor, root)) = staged {
            if let Some(current) = &tree {
                tree = Some(
                    super::catalog_migration::insert_pack(
                        current,
                        &self.log,
                        &self.view,
                        &self.operation,
                        descriptor,
                        root,
                    )
                    .await?,
                );
            } else if !self.state.packs.contains_key(&descriptor.id) {
                descriptors.push(descriptor);
                objects.push(root);
            }
        }
        // Read staged and existing packs through exactly the same authenticated
        // sparse catalog. Publication has not occurred and staging is collectible.
        let mut roots = pack_roots(&self.state);
        roots.extend(
            descriptors
                .iter()
                .cloned()
                .zip(objects.iter().map(|root| root.reference().clone())),
        );
        if request.updates.iter().any(|update| update.target.is_some()) {
            let catalog = if let Some(tree) = &tree {
                durable::Catalog::from_tree(&self.operation, self.format, tree.root().cloned())?
            } else {
                durable::load(&self.operation, &self.log, &self.view, self.format, &roots).await?
            };
            let mut reader = durable::Reader::new(&self.log, &self.view, &catalog)
                .with_scan_certificate(certificate);
            let _roots_memory = self
                .operation
                .reserve_state(request.updates.len() * std::mem::size_of::<ObjectId>())?;
            let targets = request
                .updates
                .iter()
                .filter_map(|update| update.target)
                .collect::<Vec<_>>();
            let mut closure = Closure::new(&self.operation)?;
            closure
                .walk(&mut reader, &targets, CONNECTED, Edges::All)
                .await?;
            closure.verify_all(&mut reader).await?;
            validate_branches(&mut closure, &mut reader, &request.updates, policy).await?;
        }
        let memory = self.operation.reserve(publication_bytes(
            self.log.options(),
            super::VIEW_RETAIN_FACTOR,
        )?)?;
        let record = self
            .state
            .transaction(self.format, request.updates.to_vec(), descriptors)?;
        self.operation.work(record.len())?;
        if let Some(tree) = tree {
            objects = tree.root().cloned().into_iter().collect();
        }
        let prepared =
            self.log
                .prepare(&self.view, transaction_id, record, Bytes::new(), objects)?;
        Ok((prepared, memory))
    }
}

impl PreparedPush {
    /// Publishes the exact prepared command and resolves one uncertain result.
    ///
    /// Success statuses are emitted only after confirmed publication. Retain
    /// [`Self::recovery_token`] before consuming this value for later recovery.
    ///
    /// # Errors
    /// Returns an error for invalid durable evidence.
    pub async fn publish_receive(self) -> Result<(Resolution, Bytes), Error> {
        let publication = self.receive;
        let resolution = match self.log.commit(self.prepared).await? {
            CommitStatus::Committed(view) => Resolution::Committed(view),
            CommitStatus::Conflict(view) => Resolution::NotCommitted(view),
            CommitStatus::Pending(pending) => self.log.resolve(pending).await?,
        };
        let index = match &resolution {
            Resolution::Committed(_) => 0,
            Resolution::NotCommitted(_) => 1,
            Resolution::StillPending(_) => 2,
            Resolution::Expired(_) => 3,
        };
        let bytes = publication.responses[index].clone();
        Ok((
            resolution,
            Bytes::from_owner(PublishedResponse {
                bytes,
                _publication: publication,
            }),
        ))
    }
}

fn reject_receive<T>(
    operation: &Operation,
    request: &ReceiveControls,
    source: Error,
) -> Result<T, Error> {
    let status = match &source {
        Error::InvalidPack(_) => ReceiveStatus::InvalidPack(b"invalid pack or resource limit"),
        Error::StaleReference => ReceiveStatus::Rejected(b"stale reference"),
        Error::NonFastForward => ReceiveStatus::Rejected(b"non-fast-forward"),
        _ => ReceiveStatus::Rejected(b"invalid update or storage failure"),
    };
    Err(Error::ReceiveRejected {
        response: response(operation, request, status)?,
        source: Box::new(source),
    })
}

fn response(
    operation: &Operation,
    request: &ReceiveControls,
    status: ReceiveStatus<'_>,
) -> Result<Bytes, Error> {
    wire_response(operation, |out| {
        if request.report_status {
            wire::write_receive_status(out, &request.updates, status)
        } else {
            Ok(())
        }
    })
}

async fn validate_branches(
    closure: &mut Closure,
    reader: &mut durable::Reader<'_>,
    updates: &[RefUpdate],
    policy: ReceivePolicy,
) -> Result<(), Error> {
    for update in updates {
        let Some(target) = update
            .target
            .filter(|_| update.name.starts_with(b"refs/heads/"))
        else {
            continue;
        };
        if closure.kind(target) != Some(Kind::Commit) {
            return Err(Error::InvalidReference);
        }
        let Some(expected) = update.expected else {
            continue;
        };
        if policy == ReceivePolicy::AllowNonFastForward {
            continue;
        }
        if !closure.reaches_commit(reader, target, expected).await? {
            return Err(Error::NonFastForward);
        }
    }
    Ok(())
}

struct PublishedResponse {
    bytes: Bytes,
    _publication: Publication,
}
impl AsRef<[u8]> for PublishedResponse {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl Repository {
    /// Checkpoints refs and the complete packs needed by their reachable objects.
    ///
    /// Unreachable packs are omitted so object-log collection can reclaim them.
    ///
    /// # Errors
    /// Returns an error for invalid reachable objects, exhausted limits, or storage
    /// failure. Uncertain publication remains explicit in the checkpoint result.
    pub async fn checkpoint(self) -> Result<object_log::CheckpointStatus, Error> {
        let operation = self.operation.clone();
        let log = self.log.clone();
        let format = self.format;
        match self.checkpoint_attempt().await {
            Err(Error::ObjectLog(object_log::Error::ViewExpired)) => {
                operation.retry()?;
                Self::open_attempt(&log, format, &operation)
                    .await?
                    .checkpoint_attempt()
                    .await
            }
            result => result,
        }
    }

    async fn checkpoint_attempt(self) -> Result<object_log::CheckpointStatus, Error> {
        if matches!(self.state.catalog, crate::state::CatalogState::Tree(_)) {
            return self.checkpoint_tree().await;
        }
        if self.view.tail().is_empty() {
            return Ok(object_log::CheckpointStatus::Published(self.view));
        }
        let _roots_memory = self.operation.reserve_state(
            self.state.refs.len() * std::mem::size_of::<ObjectId>()
                + self.state.packs.len() * (std::mem::size_of::<ObjectId>() * 4 + 32),
        )?;
        let _live_leaf = self
            .operation
            .reserve_state(12 * std::mem::size_of::<ObjectId>())?;
        let mut live = std::collections::BTreeSet::new();
        if !self.state.refs.is_empty() {
            let catalog = self.catalog().await?;
            let mut reader = durable::Reader::new(&self.log, &self.view, &catalog);
            let roots = self.state.refs.values().copied().collect::<Vec<_>>();
            let mut closure = Closure::new(&self.operation)?;
            closure
                .walk(&mut reader, &roots, CONNECTED, Edges::All)
                .await?;
            for (name, &id) in &self.state.refs {
                if name.starts_with(b"refs/heads/") && closure.kind(id) != Some(Kind::Commit) {
                    return Err(Error::InvalidReference);
                }
            }
            closure.verify_all(&mut reader).await?;
            for &id in closure.nodes.keys() {
                live.insert(catalog.containing_pack(id).ok_or(Error::InvalidReference)?);
            }
        }
        self.checkpoint_snapshot(|id| live.contains(id)).await
    }
}

// Git clients store refs in a file/directory namespace. Apply the entire batch
// logically before checking prefixes so atomic delete-and-replace remains valid.
fn validate_namespace(
    operation: &Operation,
    current: &crate::RefSnapshot,
    updates: &[RefUpdate],
) -> Result<(), Error> {
    let _memory = operation.reserve_state(
        (current.len() + updates.len()) * std::mem::size_of::<&[u8]>()
            + updates.len() * std::mem::size_of::<&RefUpdate>(),
    )?;
    let name_bytes = current.keys().map(Vec::len).sum::<usize>()
        + updates
            .iter()
            .map(|update| update.name.len())
            .sum::<usize>();
    operation.work(name_bytes * 64)?;
    let mut sorted = updates.iter().collect::<Vec<_>>();
    sorted.sort_unstable_by(|left, right| left.name.cmp(&right.name));
    let mut names = Vec::with_capacity(current.len() + updates.len());
    for name in current.keys() {
        let deleted = sorted
            .binary_search_by(|update| update.name.cmp(name))
            .ok()
            .is_some_and(|index| sorted[index].target.is_none());
        if !deleted {
            names.push(name.as_slice());
        }
    }
    names.extend(
        updates
            .iter()
            .filter(|update| update.target.is_some())
            .map(|update| update.name.as_slice()),
    );
    names.sort_unstable();
    names.dedup();
    for name in &names {
        for (index, byte) in name.iter().enumerate() {
            if *byte == b'/' {
                operation.work(index * 64)?;
                if names.binary_search(&&name[..index]).is_ok() {
                    return Err(Error::InvalidReference);
                }
            }
        }
    }
    Ok(())
}

fn publication_bytes(options: object_log::Options, head_factor: usize) -> Result<usize, Error> {
    super::memory_bound(options.max_commit_bytes, 4)?
        .checked_add(super::memory_bound(options.max_head_bytes, head_factor)?)
        .ok_or_else(|| Error::InvalidPack("Git publication exceeds memory".into()))
}
