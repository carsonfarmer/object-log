//! Versioned durable encoding.

use crate::log::Options;
use crate::store::{ImmutableKey, ImmutableKind};
use crate::{
    CheckpointRef, CommitRef, Digest, Error, LogId, ObjectKind, ObjectRef, ObservedState,
    PreparedCommit, RetentionId, StorageId, TransactionId, View,
};
use bytes::Bytes;
use minicbor::bytes::ByteVec;
use minicbor::{Decode, Encode};
use object_store::UpdateVersion;
use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;
use uuid::Uuid;

pub(crate) const FORMAT_VERSION: u32 = 1;
const DIGEST_LEN: usize = 32;
const UUID_LEN: usize = 16;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Head {
    pub log_id: LogId,
    pub incarnation: Uuid,
    pub options: Options,
    pub generation: u64,
    pub next_sequence: u64,
    pub checkpoint: Option<CheckpointRef>,
    pub tail: Vec<CommitRef>,
    pub recent_outcomes: Vec<CommitRef>,
    pub collection_epoch: u64,
    pub active_plan: Option<CollectionPlanRef>,
    pub retention_ids: BTreeSet<RetentionId>,
}

impl Head {
    pub(crate) fn empty(log_id: LogId, incarnation: Uuid, options: Options) -> Self {
        Self {
            log_id,
            incarnation,
            options,
            generation: 0,
            next_sequence: 0,
            checkpoint: None,
            tail: Vec::new(),
            recent_outcomes: Vec::new(),
            collection_epoch: 0,
            active_plan: None,
            retention_ids: BTreeSet::new(),
        }
    }

    pub(crate) fn tip(&self) -> Option<Digest> {
        self.tail
            .last()
            .map(|commit| commit.digest)
            .or_else(|| self.checkpoint.as_ref().map(|base| base.through_commit))
    }

    pub(crate) fn advance_generation(&mut self) -> Result<(), Error> {
        self.generation = self
            .generation
            .checked_add(1)
            .ok_or(Error::LimitExceeded("head generation"))?;
        Ok(())
    }

    pub(crate) fn validate(&self) -> Result<(), Error> {
        if self.tail.len() > self.options.max_tail_entries {
            return Err(Error::InvalidFormat(
                "head tail exceeds its durable limit".into(),
            ));
        }
        if self.recent_outcomes.len() > self.options.resolution_window {
            return Err(Error::InvalidFormat(
                "head outcomes exceed their durable limit".into(),
            ));
        }
        if self.generation < self.next_sequence {
            return Err(Error::InvalidFormat(
                "head generation precedes its commit sequence".into(),
            ));
        }
        self.validate_collection_state()?;
        let expected_start = match self.checkpoint.as_ref() {
            Some(checkpoint) => {
                if checkpoint.object.kind != ObjectKind::Checkpoint {
                    return Err(Error::InvalidFormat(
                        "head base names a non-checkpoint object".into(),
                    ));
                }
                checkpoint.through_sequence.checked_add(1).ok_or_else(|| {
                    Error::InvalidFormat("head base sequence cannot advance".into())
                })?
            }
            None => 0,
        };

        let identity_count = self.tail.len().saturating_add(self.recent_outcomes.len());
        let mut transaction_ids = HashSet::with_capacity(identity_count);
        validate_commit_sequence(&self.tail, expected_start, &mut transaction_ids)?;
        let tail_len = u64::try_from(self.tail.len())
            .map_err(|_| Error::InvalidFormat("tail length exceeds u64".into()))?;
        let expected_next = expected_start
            .checked_add(tail_len)
            .ok_or_else(|| Error::InvalidFormat("head next sequence cannot advance".into()))?;
        if self.next_sequence != expected_next {
            return Err(Error::InvalidFormat(
                "head next sequence does not follow its base and tail".into(),
            ));
        }

        let expected_outcome_start = match self.checkpoint.as_ref() {
            None if self.recent_outcomes.is_empty() => None,
            None => {
                return Err(Error::InvalidFormat(
                    "head outcomes exist without a base checkpoint".into(),
                ));
            }
            Some(checkpoint) => {
                let available = checkpoint.through_sequence.checked_add(1).ok_or_else(|| {
                    Error::InvalidFormat("head base sequence cannot advance".into())
                })?;
                let window = u64::try_from(self.options.resolution_window)
                    .map_err(|_| Error::InvalidFormat("outcome window exceeds u64".into()))?;
                let retained = u64::try_from(self.recent_outcomes.len())
                    .map_err(|_| Error::InvalidFormat("outcome count exceeds u64".into()))?;
                if retained != available.min(window) {
                    return Err(Error::InvalidFormat(
                        "head does not retain the required outcome suffix".into(),
                    ));
                }
                if self
                    .recent_outcomes
                    .last()
                    .is_some_and(|commit| commit.digest != checkpoint.through_commit)
                {
                    return Err(Error::InvalidFormat(
                        "head outcome suffix does not match its checkpoint".into(),
                    ));
                }
                Some(available - retained)
            }
        };
        validate_commit_sequence(
            &self.recent_outcomes,
            expected_outcome_start.unwrap_or(0),
            &mut transaction_ids,
        )?;
        Ok(())
    }

    fn validate_collection_state(&self) -> Result<(), Error> {
        if self.collection_epoch > self.generation {
            return Err(Error::InvalidFormat(
                "head collection epoch exceeds its generation".into(),
            ));
        }
        if self.retention_ids.len() > self.options.max_retention_ids {
            return Err(Error::InvalidFormat(
                "head retention IDs exceed their durable limit".into(),
            ));
        }
        if self.active_plan.is_some() && !self.retention_ids.is_empty() {
            return Err(Error::InvalidFormat(
                "head has an active plan and active retentions".into(),
            ));
        }
        if let Some(plan) = self.active_plan.as_ref() {
            let plan_len = usize::try_from(plan.len)
                .map_err(|_| Error::InvalidFormat("collection plan length exceeds usize".into()))?;
            if self.collection_epoch == 0 {
                return Err(Error::InvalidFormat(
                    "head has an active plan before the first collection epoch".into(),
                ));
            }
            if plan_len > self.options.max_collection_plan_bytes {
                return Err(Error::InvalidFormat(
                    "head collection plan exceeds its durable byte limit".into(),
                ));
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CollectionPlanRef {
    pub storage_id: StorageId,
    pub digest: Digest,
    pub len: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CollectionPlan {
    pub log_id: LogId,
    pub collection_epoch: u64,
    pub candidates: Vec<CollectionCandidate>,
}

impl CollectionPlan {
    fn validate(&self, options: Options) -> Result<(), Error> {
        if self.collection_epoch == 0 {
            return Err(Error::InvalidFormat(
                "collection plan epoch must be positive".into(),
            ));
        }
        if self.candidates.is_empty() {
            return Err(Error::InvalidFormat(
                "collection plan must contain a positive deletion set".into(),
            ));
        }
        if self.candidates.len() > options.max_collection_objects {
            return Err(Error::LimitExceeded("collection plan objects"));
        }
        if !self
            .candidates
            .windows(2)
            .all(|pair| pair[0].key < pair[1].key)
        {
            return Err(Error::InvalidFormat(
                "collection plan candidates are not strictly sorted".into(),
            ));
        }
        self.candidate_bytes()?;
        Ok(())
    }

    pub(crate) fn candidate_bytes(&self) -> Result<u64, Error> {
        self.candidates.iter().try_fold(0_u64, |total, candidate| {
            total
                .checked_add(candidate.bytes)
                .ok_or(Error::LimitExceeded("collection candidate bytes"))
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CollectionCandidate {
    pub key: ImmutableKey,
    pub bytes: u64,
}

fn validate_commit_sequence(
    commits: &[CommitRef],
    start: u64,
    transaction_ids: &mut HashSet<TransactionId>,
) -> Result<(), Error> {
    for (offset, commit) in commits.iter().enumerate() {
        let offset = u64::try_from(offset)
            .map_err(|_| Error::InvalidFormat("commit offset exceeds u64".into()))?;
        if commit.sequence
            != start
                .checked_add(offset)
                .ok_or_else(|| Error::InvalidFormat("head commit sequence cannot advance".into()))?
        {
            return Err(Error::InvalidFormat(
                "head commits are not contiguous".into(),
            ));
        }
        if !transaction_ids.insert(commit.transaction_id) {
            return Err(Error::InvalidFormat(
                "head contains a duplicate transaction".into(),
            ));
        }
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Commit {
    pub log_id: LogId,
    pub incarnation: Uuid,
    pub transaction_id: TransactionId,
    pub expected_tip: Option<Digest>,
    pub operation: Bytes,
    pub result: Bytes,
    pub objects: Vec<ObjectRef>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Checkpoint {
    pub log_id: LogId,
    pub incarnation: Uuid,
    pub through_sequence: u64,
    pub through_commit: Digest,
    pub snapshot: Bytes,
    pub objects: Vec<ObjectRef>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Node {
    pub payload: Bytes,
    pub children: Vec<ObjectRef>,
}

#[derive(Clone, Debug, Decode, Encode, PartialEq)]
#[cbor(map)]
struct EnvelopeWire {
    #[cbor(n(1), with = "minicbor::bytes")]
    payload: Vec<u8>,
    #[cbor(n(2), with = "minicbor::bytes")]
    digest: Vec<u8>,
}

#[derive(Clone, Debug, Decode, Encode, PartialEq)]
#[cbor(map)]
struct HeadWire {
    #[n(1)]
    format_version: u32,
    #[n(2)]
    log_id: String,
    #[n(3)]
    generation: u64,
    #[n(4)]
    next_sequence: u64,
    #[n(5)]
    checkpoint: Option<CheckpointRefWire>,
    #[n(6)]
    tail: Vec<CommitRefWire>,
    #[n(7)]
    recent_outcomes: Vec<CommitRefWire>,
    #[cbor(n(8), with = "minicbor::bytes")]
    incarnation: Vec<u8>,
    #[n(9)]
    options: OptionsWire,
    #[n(10)]
    collection_epoch: u64,
    #[n(11)]
    active_plan: Option<CollectionPlanRefWire>,
    #[n(12)]
    retention_ids: Vec<ByteVec>,
}

#[derive(Clone, Debug, Decode, Encode, PartialEq)]
#[cbor(map)]
struct OptionsWire {
    #[n(1)]
    max_tail_entries: u64,
    #[n(2)]
    resolution_window: u64,
    #[n(3)]
    max_inline_operation_bytes: u64,
    #[n(4)]
    max_inline_result_bytes: u64,
    #[n(5)]
    max_object_refs: u64,
    #[n(6)]
    max_commit_bytes: u64,
    #[n(7)]
    max_head_bytes: u64,
    #[n(8)]
    max_checkpoint_bytes: u64,
    #[n(9)]
    max_object_bytes: u64,
    #[n(10)]
    max_retention_ids: u64,
    #[n(11)]
    max_collection_objects: u64,
    #[n(12)]
    max_collection_plan_bytes: u64,
}

#[derive(Clone, Debug, Decode, Encode, PartialEq)]
#[cbor(map)]
struct CommitWire {
    #[n(1)]
    format_version: u32,
    #[n(2)]
    log_id: String,
    #[cbor(n(3), with = "minicbor::bytes")]
    transaction_id: Vec<u8>,
    #[cbor(n(4), with = "minicbor::bytes")]
    expected_tip: Option<Vec<u8>>,
    #[cbor(n(5), with = "minicbor::bytes")]
    operation: Vec<u8>,
    #[cbor(n(6), with = "minicbor::bytes")]
    result: Vec<u8>,
    #[n(7)]
    objects: Vec<ObjectRefWire>,
    #[cbor(n(8), with = "minicbor::bytes")]
    incarnation: Vec<u8>,
}

#[derive(Clone, Debug, Decode, Encode, PartialEq)]
#[cbor(map)]
struct CheckpointWire {
    #[n(1)]
    format_version: u32,
    #[n(2)]
    log_id: String,
    #[n(3)]
    through_sequence: u64,
    #[cbor(n(4), with = "minicbor::bytes")]
    through_commit: Vec<u8>,
    #[cbor(n(5), with = "minicbor::bytes")]
    snapshot: Vec<u8>,
    #[cbor(n(6), with = "minicbor::bytes")]
    incarnation: Vec<u8>,
    #[n(7)]
    objects: Vec<ObjectRefWire>,
}

#[derive(Encode)]
#[cbor(map)]
struct CheckpointEncodeWire<'a> {
    #[n(1)]
    format_version: u32,
    #[n(2)]
    log_id: &'a str,
    #[n(3)]
    through_sequence: u64,
    #[cbor(n(4), with = "minicbor::bytes")]
    through_commit: &'a [u8],
    #[cbor(n(5), with = "minicbor::bytes")]
    snapshot: &'a [u8],
    #[cbor(n(6), with = "minicbor::bytes")]
    incarnation: &'a [u8],
    #[n(7)]
    objects: Vec<ObjectRefWire>,
}

#[derive(Clone, Debug, Decode, Encode, PartialEq)]
#[cbor(map)]
struct NodeWire {
    #[n(1)]
    format_version: u32,
    #[cbor(n(2), with = "minicbor::bytes")]
    payload: Vec<u8>,
    #[n(3)]
    children: Vec<ObjectRefWire>,
}

#[derive(Clone, Debug, Decode, Encode, PartialEq)]
#[cbor(map)]
struct RecoveryTokenWire {
    #[n(1)]
    format_version: u32,
    #[cbor(n(2), with = "minicbor::bytes")]
    head: Vec<u8>,
    #[n(3)]
    e_tag: Option<String>,
    #[n(4)]
    storage_version: Option<String>,
    #[cbor(n(5), with = "minicbor::bytes")]
    transaction_id: Vec<u8>,
    #[cbor(n(6), with = "minicbor::bytes")]
    operation: Vec<u8>,
    #[cbor(n(7), with = "minicbor::bytes")]
    result: Vec<u8>,
    #[n(8)]
    objects: Vec<ObjectRefWire>,
    #[cbor(n(9), with = "minicbor::bytes")]
    storage_id: Vec<u8>,
}

#[derive(Clone, Debug, Decode, Encode, PartialEq)]
#[cbor(map)]
struct CommitRefWire {
    #[n(1)]
    sequence: u64,
    #[cbor(n(2), with = "minicbor::bytes")]
    transaction_id: Vec<u8>,
    #[cbor(n(3), with = "minicbor::bytes")]
    digest: Vec<u8>,
    #[n(4)]
    len: u64,
    #[cbor(n(5), with = "minicbor::bytes")]
    storage_id: Vec<u8>,
}

#[derive(Clone, Debug, Decode, Encode, PartialEq)]
#[cbor(map)]
struct CheckpointRefWire {
    #[n(1)]
    through_sequence: u64,
    #[cbor(n(2), with = "minicbor::bytes")]
    through_commit: Vec<u8>,
    #[n(3)]
    object: Option<ObjectRefWire>,
}

#[derive(Clone, Debug, Decode, Encode, PartialEq)]
#[cbor(map)]
struct ObjectRefWire {
    #[n(1)]
    kind: u8,
    #[cbor(n(2), with = "minicbor::bytes")]
    digest: Vec<u8>,
    #[n(3)]
    len: u64,
    #[cbor(n(4), with = "minicbor::bytes")]
    storage_id: Vec<u8>,
}

#[derive(Clone, Debug, Decode, Encode, PartialEq)]
#[cbor(map)]
struct CollectionPlanRefWire {
    #[cbor(n(1), with = "minicbor::bytes")]
    storage_id: Vec<u8>,
    #[cbor(n(2), with = "minicbor::bytes")]
    digest: Vec<u8>,
    #[n(3)]
    len: u64,
}

#[derive(Clone, Debug, Decode, Encode, PartialEq)]
#[cbor(map)]
struct CollectionPlanWire {
    #[n(1)]
    format_version: u32,
    #[n(2)]
    log_id: String,
    #[n(3)]
    collection_epoch: u64,
    #[n(4)]
    candidates: Vec<CollectionCandidateWire>,
}

#[derive(Clone, Debug, Decode, Encode, PartialEq)]
#[cbor(map)]
struct CollectionCandidateWire {
    #[cbor(n(1), with = "minicbor::bytes")]
    incarnation: Vec<u8>,
    #[n(2)]
    kind: u8,
    #[cbor(n(3), with = "minicbor::bytes")]
    storage_id: Vec<u8>,
    #[cbor(n(4), with = "minicbor::bytes")]
    digest: Vec<u8>,
    #[n(5)]
    bytes: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
enum ObjectKindWire {
    Blob = 1,
    Checkpoint = 2,
    Node = 3,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
enum ImmutableKindWire {
    Commit = 1,
    Blob = 2,
    Node = 3,
    Checkpoint = 4,
    CollectionPlan = 5,
}

pub(crate) fn encode_head(head: &Head) -> Result<Bytes, Error> {
    head.validate()?;
    encode_envelope(&HeadWire {
        format_version: FORMAT_VERSION,
        log_id: head.log_id.to_string(),
        generation: head.generation,
        next_sequence: head.next_sequence,
        checkpoint: head.checkpoint.as_ref().map(CheckpointRefWire::from),
        tail: head.tail.iter().map(CommitRefWire::from).collect(),
        recent_outcomes: head
            .recent_outcomes
            .iter()
            .map(CommitRefWire::from)
            .collect(),
        incarnation: head.incarnation.as_bytes().to_vec(),
        options: OptionsWire::try_from(head.options)?,
        collection_epoch: head.collection_epoch,
        active_plan: head.active_plan.as_ref().map(CollectionPlanRefWire::from),
        retention_ids: head
            .retention_ids
            .iter()
            .map(|id| ByteVec::from(id.as_uuid().as_bytes().to_vec()))
            .collect(),
    })
}

pub(crate) fn decode_head(bytes: &[u8]) -> Result<Head, Error> {
    let wire: HeadWire = decode_envelope(bytes)?;
    require_version(wire.format_version)?;
    let head = Head {
        log_id: LogId::new(wire.log_id)?,
        incarnation: uuid(&wire.incarnation, "log incarnation")?,
        options: Options::try_from(wire.options)?,
        generation: wire.generation,
        next_sequence: wire.next_sequence,
        checkpoint: wire.checkpoint.map(CheckpointRef::try_from).transpose()?,
        tail: wire
            .tail
            .into_iter()
            .map(CommitRef::try_from)
            .collect::<Result<_, _>>()?,
        recent_outcomes: wire
            .recent_outcomes
            .into_iter()
            .map(CommitRef::try_from)
            .collect::<Result<_, _>>()?,
        collection_epoch: wire.collection_epoch,
        active_plan: wire
            .active_plan
            .map(CollectionPlanRef::try_from)
            .transpose()?,
        retention_ids: retention_ids(wire.retention_ids)?,
    };
    head.validate()?;
    require_canonical(bytes, &encode_head(&head)?)?;
    Ok(head)
}

pub(crate) fn encode_commit(commit: &Commit) -> Result<Bytes, Error> {
    encode_envelope(&CommitWire {
        format_version: FORMAT_VERSION,
        log_id: commit.log_id.to_string(),
        transaction_id: commit.transaction_id.as_uuid().as_bytes().to_vec(),
        expected_tip: commit.expected_tip.map(|digest| digest.as_bytes().to_vec()),
        operation: commit.operation.to_vec(),
        result: commit.result.to_vec(),
        objects: commit.objects.iter().map(ObjectRefWire::from).collect(),
        incarnation: commit.incarnation.as_bytes().to_vec(),
    })
}

pub(crate) fn decode_commit(bytes: &[u8]) -> Result<Commit, Error> {
    let wire: CommitWire = decode_envelope(bytes)?;
    require_version(wire.format_version)?;
    let commit = Commit {
        log_id: LogId::new(wire.log_id)?,
        incarnation: uuid(&wire.incarnation, "log incarnation")?,
        transaction_id: transaction_id(&wire.transaction_id)?,
        expected_tip: wire.expected_tip.map(|value| digest(&value)).transpose()?,
        operation: Bytes::from(wire.operation),
        result: Bytes::from(wire.result),
        objects: wire
            .objects
            .into_iter()
            .map(ObjectRef::try_from)
            .collect::<Result<_, _>>()?,
    };
    require_canonical(bytes, &encode_commit(&commit)?)?;
    Ok(commit)
}

pub(crate) fn encode_checkpoint(checkpoint: &Checkpoint) -> Result<Bytes, Error> {
    encode_envelope(&CheckpointEncodeWire {
        format_version: FORMAT_VERSION,
        log_id: checkpoint.log_id.as_str(),
        through_sequence: checkpoint.through_sequence,
        through_commit: checkpoint.through_commit.as_bytes(),
        snapshot: &checkpoint.snapshot,
        incarnation: checkpoint.incarnation.as_bytes(),
        objects: checkpoint.objects.iter().map(ObjectRefWire::from).collect(),
    })
}

pub(crate) fn decode_checkpoint(bytes: &[u8]) -> Result<Checkpoint, Error> {
    let wire: CheckpointWire = decode_envelope(bytes)?;
    require_version(wire.format_version)?;
    let checkpoint = Checkpoint {
        log_id: LogId::new(wire.log_id)?,
        incarnation: uuid(&wire.incarnation, "log incarnation")?,
        through_sequence: wire.through_sequence,
        through_commit: digest(&wire.through_commit)?,
        snapshot: Bytes::from(wire.snapshot),
        objects: wire
            .objects
            .into_iter()
            .map(ObjectRef::try_from)
            .collect::<Result<_, _>>()?,
    };
    require_canonical(bytes, &encode_checkpoint(&checkpoint)?)?;
    Ok(checkpoint)
}

pub(crate) fn encode_node(node: &Node) -> Result<Bytes, Error> {
    encode_envelope(&NodeWire {
        format_version: FORMAT_VERSION,
        payload: node.payload.to_vec(),
        children: node.children.iter().map(ObjectRefWire::from).collect(),
    })
}

pub(crate) fn decode_node(bytes: &[u8]) -> Result<Node, Error> {
    let wire: NodeWire = decode_envelope(bytes)?;
    require_version(wire.format_version)?;
    let node = Node {
        payload: Bytes::from(wire.payload),
        children: wire
            .children
            .into_iter()
            .map(ObjectRef::try_from)
            .collect::<Result<_, _>>()?,
    };
    require_canonical(bytes, &encode_node(&node)?)?;
    Ok(node)
}

pub(crate) fn encode_recovery_token(prepared: &PreparedCommit) -> Result<Bytes, Error> {
    encode_envelope(&RecoveryTokenWire {
        format_version: FORMAT_VERSION,
        head: encode_head(prepared.view.head())?.to_vec(),
        e_tag: prepared.view.storage_version().e_tag.clone(),
        storage_version: prepared.view.storage_version().version.clone(),
        transaction_id: prepared.transaction_id.as_uuid().as_bytes().to_vec(),
        operation: prepared.operation.to_vec(),
        result: prepared.result.to_vec(),
        objects: prepared.objects.iter().map(ObjectRefWire::from).collect(),
        storage_id: prepared.storage_id.as_uuid().as_bytes().to_vec(),
    })
}

pub(crate) fn decode_recovery_token(bytes: &[u8]) -> Result<PreparedCommit, Error> {
    let wire: RecoveryTokenWire = decode_envelope(bytes)?;
    require_version(wire.format_version)?;
    let prepared = PreparedCommit {
        view: View {
            observed: Arc::new(ObservedState {
                head: decode_head(&wire.head)?,
                version: UpdateVersion {
                    e_tag: wire.e_tag,
                    version: wire.storage_version,
                },
            }),
        },
        staging_domain: Arc::new(crate::StagingDomain),
        transaction_id: transaction_id(&wire.transaction_id)?,
        storage_id: storage_id(&wire.storage_id)?,
        operation: Bytes::from(wire.operation),
        result: Bytes::from(wire.result),
        objects: wire
            .objects
            .into_iter()
            .map(ObjectRef::try_from)
            .collect::<Result<_, _>>()?,
    };
    require_canonical(bytes, &encode_recovery_token(&prepared)?)?;
    Ok(prepared)
}

pub(crate) fn encode_collection_plan(
    plan: &CollectionPlan,
    options: Options,
) -> Result<Bytes, Error> {
    plan.validate(options)?;
    let bytes = encode_envelope(&CollectionPlanWire {
        format_version: FORMAT_VERSION,
        log_id: plan.log_id.to_string(),
        collection_epoch: plan.collection_epoch,
        candidates: plan
            .candidates
            .iter()
            .map(CollectionCandidateWire::from)
            .collect(),
    })?;
    if bytes.len() > options.max_collection_plan_bytes {
        return Err(Error::LimitExceeded("encoded collection plan bytes"));
    }
    Ok(bytes)
}

pub(crate) fn decode_collection_plan(
    bytes: &[u8],
    options: Options,
) -> Result<CollectionPlan, Error> {
    if bytes.len() > options.max_collection_plan_bytes {
        return Err(Error::LimitExceeded("encoded collection plan bytes"));
    }
    let wire: CollectionPlanWire = decode_envelope(bytes)?;
    require_version(wire.format_version)?;
    let plan = CollectionPlan {
        log_id: LogId::new(wire.log_id)?,
        collection_epoch: wire.collection_epoch,
        candidates: wire
            .candidates
            .into_iter()
            .map(CollectionCandidate::try_from)
            .collect::<Result<_, _>>()?,
    };
    plan.validate(options)?;
    require_canonical(bytes, &encode_collection_plan(&plan, options)?)?;
    Ok(plan)
}

fn encode_envelope(message: &impl Encode<()>) -> Result<Bytes, Error> {
    let payload = minicbor::to_vec(message)
        .map_err(|error| Error::InvalidFormat(format!("CBOR encoding failed: {error}")))?;
    let digest = Digest::of(&payload);
    Ok(Bytes::from(
        minicbor::to_vec(EnvelopeWire {
            payload,
            digest: digest.as_bytes().to_vec(),
        })
        .map_err(|error| Error::InvalidFormat(format!("CBOR encoding failed: {error}")))?,
    ))
}

fn decode_envelope<M>(bytes: &[u8]) -> Result<M, Error>
where
    M: for<'bytes> Decode<'bytes, ()>,
{
    let envelope: EnvelopeWire = decode_exact(bytes)?;
    if digest(&envelope.digest)? != Digest::of(&envelope.payload) {
        return Err(Error::CorruptObject);
    }
    decode_exact(&envelope.payload)
}

fn decode_exact<M>(bytes: &[u8]) -> Result<M, Error>
where
    M: for<'bytes> Decode<'bytes, ()>,
{
    let mut decoder = minicbor::Decoder::new(bytes);
    let value = decoder
        .decode()
        .map_err(|error| Error::InvalidFormat(error.to_string()))?;
    if decoder.position() != bytes.len() {
        return Err(Error::InvalidFormat(
            "encoded object contains trailing bytes".into(),
        ));
    }
    Ok(value)
}

fn require_version(version: u32) -> Result<(), Error> {
    if version != FORMAT_VERSION {
        return Err(Error::InvalidFormat(format!(
            "unsupported format version {version}"
        )));
    }
    Ok(())
}

fn require_canonical(input: &[u8], canonical: &[u8]) -> Result<(), Error> {
    if input != canonical {
        return Err(Error::InvalidFormat(
            "encoded object is not canonical format version 1".into(),
        ));
    }
    Ok(())
}

fn digest(value: &[u8]) -> Result<Digest, Error> {
    let bytes: [u8; DIGEST_LEN] = value
        .try_into()
        .map_err(|_| Error::InvalidFormat("digest has an invalid length".into()))?;
    Ok(Digest(bytes))
}

fn transaction_id(value: &[u8]) -> Result<TransactionId, Error> {
    let value = uuid(value, "transaction ID")?;
    Ok(TransactionId::from_uuid(value))
}

fn storage_id(value: &[u8]) -> Result<StorageId, Error> {
    Ok(StorageId::from_uuid(uuid(value, "physical storage ID")?))
}

fn retention_ids(values: Vec<ByteVec>) -> Result<BTreeSet<RetentionId>, Error> {
    let mut previous = None;
    let mut ids = BTreeSet::new();
    for value in values {
        let id = RetentionId::from_uuid(uuid(value.as_ref(), "retention ID")?);
        if previous.is_some_and(|previous| previous >= id) {
            return Err(Error::InvalidFormat(
                "head retention IDs are not strictly sorted".into(),
            ));
        }
        previous = Some(id);
        ids.insert(id);
    }
    Ok(ids)
}

fn uuid(value: &[u8], name: &str) -> Result<Uuid, Error> {
    if value.len() != UUID_LEN {
        return Err(Error::InvalidFormat(format!(
            "{name} has an invalid length"
        )));
    }
    Uuid::from_slice(value)
        .map_err(|error| Error::InvalidFormat(format!("invalid {name}: {error}")))
}

impl From<&CommitRef> for CommitRefWire {
    fn from(value: &CommitRef) -> Self {
        Self {
            sequence: value.sequence,
            transaction_id: value.transaction_id.as_uuid().as_bytes().to_vec(),
            digest: value.digest.as_bytes().to_vec(),
            len: value.len,
            storage_id: value.storage_id.as_uuid().as_bytes().to_vec(),
        }
    }
}

impl TryFrom<CommitRefWire> for CommitRef {
    type Error = Error;

    fn try_from(value: CommitRefWire) -> Result<Self, Self::Error> {
        Ok(Self {
            sequence: value.sequence,
            transaction_id: transaction_id(&value.transaction_id)?,
            storage_id: storage_id(&value.storage_id)?,
            digest: digest(&value.digest)?,
            len: value.len,
        })
    }
}

impl From<&CheckpointRef> for CheckpointRefWire {
    fn from(value: &CheckpointRef) -> Self {
        Self {
            through_sequence: value.through_sequence,
            through_commit: value.through_commit.as_bytes().to_vec(),
            object: Some(ObjectRefWire::from(&value.object)),
        }
    }
}

impl TryFrom<CheckpointRefWire> for CheckpointRef {
    type Error = Error;

    fn try_from(value: CheckpointRefWire) -> Result<Self, Self::Error> {
        let object: ObjectRef = value
            .object
            .ok_or_else(|| Error::InvalidFormat("checkpoint object is missing".into()))?
            .try_into()?;
        if object.kind != ObjectKind::Checkpoint {
            return Err(Error::InvalidFormat(
                "checkpoint reference names a non-checkpoint object".into(),
            ));
        }
        Ok(Self {
            through_sequence: value.through_sequence,
            through_commit: digest(&value.through_commit)?,
            object,
        })
    }
}

impl From<&ObjectRef> for ObjectRefWire {
    fn from(value: &ObjectRef) -> Self {
        Self {
            kind: match value.kind {
                ObjectKind::Blob => ObjectKindWire::Blob as u8,
                ObjectKind::Node => ObjectKindWire::Node as u8,
                ObjectKind::Checkpoint => ObjectKindWire::Checkpoint as u8,
            },
            digest: value.digest.as_bytes().to_vec(),
            len: value.len,
            storage_id: value.storage_id.as_uuid().as_bytes().to_vec(),
        }
    }
}

impl TryFrom<ObjectRefWire> for ObjectRef {
    type Error = Error;

    fn try_from(value: ObjectRefWire) -> Result<Self, Self::Error> {
        let kind = match value.kind {
            value if value == ObjectKindWire::Blob as u8 => ObjectKind::Blob,
            value if value == ObjectKindWire::Checkpoint as u8 => ObjectKind::Checkpoint,
            value if value == ObjectKindWire::Node as u8 => ObjectKind::Node,
            _ => {
                return Err(Error::InvalidFormat("invalid object kind".into()));
            }
        };
        Ok(Self {
            kind,
            storage_id: storage_id(&value.storage_id)?,
            digest: digest(&value.digest)?,
            len: value.len,
        })
    }
}

impl From<&CollectionPlanRef> for CollectionPlanRefWire {
    fn from(value: &CollectionPlanRef) -> Self {
        Self {
            storage_id: value.storage_id.as_uuid().as_bytes().to_vec(),
            digest: value.digest.as_bytes().to_vec(),
            len: value.len,
        }
    }
}

impl TryFrom<CollectionPlanRefWire> for CollectionPlanRef {
    type Error = Error;

    fn try_from(value: CollectionPlanRefWire) -> Result<Self, Self::Error> {
        Ok(Self {
            storage_id: storage_id(&value.storage_id)?,
            digest: digest(&value.digest)?,
            len: value.len,
        })
    }
}

impl From<&CollectionCandidate> for CollectionCandidateWire {
    fn from(value: &CollectionCandidate) -> Self {
        Self {
            incarnation: value.key.incarnation.as_bytes().to_vec(),
            kind: match value.key.kind {
                ImmutableKind::Commit => ImmutableKindWire::Commit as u8,
                ImmutableKind::Blob => ImmutableKindWire::Blob as u8,
                ImmutableKind::Node => ImmutableKindWire::Node as u8,
                ImmutableKind::Checkpoint => ImmutableKindWire::Checkpoint as u8,
                ImmutableKind::CollectionPlan => ImmutableKindWire::CollectionPlan as u8,
            },
            storage_id: value.key.storage_id.as_uuid().as_bytes().to_vec(),
            digest: value.key.digest.as_bytes().to_vec(),
            bytes: value.bytes,
        }
    }
}

impl TryFrom<CollectionCandidateWire> for CollectionCandidate {
    type Error = Error;

    fn try_from(value: CollectionCandidateWire) -> Result<Self, Self::Error> {
        let kind = match value.kind {
            value if value == ImmutableKindWire::Commit as u8 => ImmutableKind::Commit,
            value if value == ImmutableKindWire::Blob as u8 => ImmutableKind::Blob,
            value if value == ImmutableKindWire::Node as u8 => ImmutableKind::Node,
            value if value == ImmutableKindWire::Checkpoint as u8 => ImmutableKind::Checkpoint,
            value if value == ImmutableKindWire::CollectionPlan as u8 => {
                ImmutableKind::CollectionPlan
            }
            _ => return Err(Error::InvalidFormat("invalid immutable kind".into())),
        };
        Ok(Self {
            key: ImmutableKey {
                incarnation: uuid(&value.incarnation, "object incarnation")?,
                kind,
                storage_id: storage_id(&value.storage_id)?,
                digest: digest(&value.digest)?,
            },
            bytes: value.bytes,
        })
    }
}

impl TryFrom<Options> for OptionsWire {
    type Error = Error;

    fn try_from(value: Options) -> Result<Self, Self::Error> {
        Ok(Self {
            max_tail_entries: option_to_u64(value.max_tail_entries)?,
            resolution_window: option_to_u64(value.resolution_window)?,
            max_inline_operation_bytes: option_to_u64(value.max_inline_operation_bytes)?,
            max_inline_result_bytes: option_to_u64(value.max_inline_result_bytes)?,
            max_object_refs: option_to_u64(value.max_object_refs)?,
            max_object_bytes: option_to_u64(value.max_object_bytes)?,
            max_commit_bytes: option_to_u64(value.max_commit_bytes)?,
            max_head_bytes: option_to_u64(value.max_head_bytes)?,
            max_checkpoint_bytes: option_to_u64(value.max_checkpoint_bytes)?,
            max_retention_ids: option_to_u64(value.max_retention_ids)?,
            max_collection_objects: option_to_u64(value.max_collection_objects)?,
            max_collection_plan_bytes: option_to_u64(value.max_collection_plan_bytes)?,
        })
    }
}

impl TryFrom<OptionsWire> for Options {
    type Error = Error;

    fn try_from(value: OptionsWire) -> Result<Self, Self::Error> {
        Ok(Self {
            max_tail_entries: option_to_usize(value.max_tail_entries)?,
            resolution_window: option_to_usize(value.resolution_window)?,
            max_inline_operation_bytes: option_to_usize(value.max_inline_operation_bytes)?,
            max_inline_result_bytes: option_to_usize(value.max_inline_result_bytes)?,
            max_object_refs: option_to_usize(value.max_object_refs)?,
            max_object_bytes: option_to_usize(value.max_object_bytes)?,
            max_commit_bytes: option_to_usize(value.max_commit_bytes)?,
            max_head_bytes: option_to_usize(value.max_head_bytes)?,
            max_checkpoint_bytes: option_to_usize(value.max_checkpoint_bytes)?,
            max_retention_ids: option_to_usize(value.max_retention_ids)?,
            max_collection_objects: option_to_usize(value.max_collection_objects)?,
            max_collection_plan_bytes: option_to_usize(value.max_collection_plan_bytes)?,
        })
    }
}

fn option_to_u64(value: usize) -> Result<u64, Error> {
    u64::try_from(value).map_err(|_| Error::LimitExceeded("durable option"))
}

fn option_to_usize(value: u64) -> Result<usize, Error> {
    usize::try_from(value).map_err(|_| Error::LimitExceeded("durable option"))
}

#[cfg(test)]
#[allow(clippy::panic)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::Arc;

    use super::{
        Checkpoint, CheckpointWire, CollectionCandidate, CollectionCandidateWire, CollectionPlan,
        CollectionPlanRef, CollectionPlanWire, Commit, EnvelopeWire, FORMAT_VERSION, Head,
        HeadWire, Node, ObjectRefWire, OptionsWire, decode_checkpoint, decode_collection_plan,
        decode_commit, decode_head, decode_node, decode_recovery_token, encode_checkpoint,
        encode_collection_plan, encode_commit, encode_envelope, encode_head, encode_node,
        encode_recovery_token,
    };
    use crate::store::{ImmutableKey, ImmutableKind};
    use crate::{CheckpointRef, CommitRef, Digest, Error, LogId, ObjectKind, ObjectRef, Options};
    use bytes::Bytes;

    fn log_id() -> LogId {
        LogId::new("tenant.resource").unwrap_or_else(|error| panic!("valid ID failed: {error}"))
    }

    fn incarnation() -> uuid::Uuid {
        uuid::Uuid::from_u128(1)
    }

    fn storage_id() -> crate::StorageId {
        crate::StorageId::from_uuid(uuid::Uuid::from_u128(2))
    }

    fn commit_ref(sequence: u64, data: &[u8]) -> CommitRef {
        CommitRef {
            sequence,
            transaction_id: crate::TransactionId::new(),
            storage_id: storage_id(),
            digest: Digest::of(data),
            len: u64::try_from(data.len()).unwrap_or_else(|_| panic!("test data is too large")),
        }
    }

    fn candidate(id: u128, bytes: u64) -> CollectionCandidate {
        CollectionCandidate {
            key: ImmutableKey::from_parts(
                incarnation(),
                ImmutableKind::Blob,
                uuid::Uuid::from_u128(id),
                Digest::of(&id.to_be_bytes()),
            ),
            bytes,
        }
    }

    fn head_wire(format_version: u32) -> HeadWire {
        HeadWire {
            format_version,
            log_id: log_id().to_string(),
            generation: 0,
            next_sequence: 0,
            checkpoint: None,
            tail: Vec::new(),
            recent_outcomes: Vec::new(),
            incarnation: incarnation().as_bytes().to_vec(),
            options: OptionsWire::try_from(Options::default())
                .unwrap_or_else(|error| panic!("options failed: {error}")),
            collection_epoch: 0,
            active_plan: None,
            retention_ids: Vec::new(),
        }
    }

    #[test]
    fn collection_plan_round_trip_preserves_exact_physical_keys() {
        let mut candidates = vec![candidate(2, 20), candidate(1, 10)];
        candidates.sort_by_key(|candidate| candidate.key);
        let plan = CollectionPlan {
            log_id: log_id(),
            collection_epoch: 3,
            candidates,
        };

        let encoded = encode_collection_plan(&plan, Options::default())
            .unwrap_or_else(|error| panic!("encode failed: {error}"));
        let decoded = decode_collection_plan(&encoded, Options::default())
            .unwrap_or_else(|error| panic!("decode failed: {error}"));

        assert_eq!(decoded, plan);
        assert_eq!(
            decoded
                .candidate_bytes()
                .unwrap_or_else(|error| panic!("sum failed: {error}")),
            30
        );
    }

    #[test]
    fn collection_plan_rejects_non_positive_or_non_canonical_sets() {
        let empty = CollectionPlan {
            log_id: log_id(),
            collection_epoch: 1,
            candidates: Vec::new(),
        };
        assert!(matches!(
            encode_collection_plan(&empty, Options::default()),
            Err(Error::InvalidFormat(_))
        ));

        let duplicate = candidate(1, 10);
        let unsorted = CollectionPlan {
            log_id: log_id(),
            collection_epoch: 1,
            candidates: vec![candidate(2, 20), duplicate, duplicate],
        };
        assert!(matches!(
            encode_collection_plan(&unsorted, Options::default()),
            Err(Error::InvalidFormat(_))
        ));
    }

    #[test]
    fn collection_plan_enforces_count_sum_and_encoded_byte_limits() {
        let plan = CollectionPlan {
            log_id: log_id(),
            collection_epoch: 1,
            candidates: vec![candidate(1, u64::MAX), candidate(2, 1)],
        };
        assert!(matches!(
            encode_collection_plan(&plan, Options::default()),
            Err(Error::LimitExceeded("collection candidate bytes"))
        ));

        let plan = CollectionPlan {
            log_id: log_id(),
            collection_epoch: 1,
            candidates: vec![candidate(1, 1), candidate(2, 2)],
        };
        let one_object = Options {
            max_collection_objects: 1,
            ..Options::default()
        };
        assert!(matches!(
            encode_collection_plan(&plan, one_object),
            Err(Error::LimitExceeded("collection plan objects"))
        ));

        let encoded = encode_collection_plan(&plan, Options::default())
            .unwrap_or_else(|error| panic!("encode failed: {error}"));
        let too_small = Options {
            max_collection_plan_bytes: encoded.len().saturating_sub(1),
            ..Options::default()
        };
        assert!(matches!(
            decode_collection_plan(&encoded, too_small),
            Err(Error::LimitExceeded("encoded collection plan bytes"))
        ));
        assert!(matches!(
            encode_collection_plan(&plan, too_small),
            Err(Error::LimitExceeded("encoded collection plan bytes"))
        ));
    }

    #[test]
    fn collection_plan_rejects_malformed_physical_keys() {
        let mut malformed = CollectionPlanWire {
            format_version: FORMAT_VERSION,
            log_id: log_id().to_string(),
            collection_epoch: 1,
            candidates: vec![CollectionCandidateWire {
                incarnation: incarnation().as_bytes().to_vec(),
                kind: 99,
                storage_id: vec![0; 15],
                digest: Digest::of(b"candidate").as_bytes().to_vec(),
                bytes: 1,
            }],
        };
        let encoded =
            encode_envelope(&malformed).unwrap_or_else(|error| panic!("encode failed: {error}"));
        assert!(matches!(
            decode_collection_plan(&encoded, Options::default()),
            Err(Error::InvalidFormat(_))
        ));
        malformed.candidates[0].kind = 2;
        let encoded =
            encode_envelope(&malformed).unwrap_or_else(|error| panic!("encode failed: {error}"));
        assert!(matches!(
            decode_collection_plan(&encoded, Options::default()),
            Err(Error::InvalidFormat(_))
        ));
    }

    #[test]
    fn head_round_trip_preserves_order_and_base() {
        let checkpoint_object = ObjectRef {
            kind: ObjectKind::Checkpoint,
            storage_id: storage_id(),
            digest: Digest::of(b"checkpoint"),
            len: 10,
        };
        let first = commit_ref(4, b"first");
        let second = commit_ref(5, b"second");
        let compacted_first = commit_ref(2, b"compacted-first");
        let compacted_second = commit_ref(3, b"compacted-second");
        let head = Head {
            log_id: log_id(),
            incarnation: incarnation(),
            options: Options {
                resolution_window: 2,
                ..Options::default()
            },
            generation: 9,
            next_sequence: 6,
            checkpoint: Some(CheckpointRef {
                through_sequence: 3,
                through_commit: compacted_second.digest,
                object: checkpoint_object,
            }),
            tail: vec![first.clone(), second.clone()],
            recent_outcomes: vec![compacted_first, compacted_second],
            collection_epoch: 0,
            active_plan: None,
            retention_ids: BTreeSet::new(),
        };

        let encoded = encode_head(&head).unwrap_or_else(|error| panic!("encode failed: {error}"));
        let decoded =
            decode_head(&encoded).unwrap_or_else(|error| panic!("decode failed: {error}"));
        assert_eq!(decoded, head);
        assert_eq!(decoded.tip(), head.tip());
    }

    #[test]
    fn head_round_trip_preserves_collection_state_and_enforces_exclusion() {
        let mut head = Head::empty(
            log_id(),
            incarnation(),
            Options {
                max_retention_ids: 1,
                ..Options::default()
            },
        );
        head.generation = 1;
        head.collection_epoch = 1;
        head.active_plan = Some(CollectionPlanRef {
            storage_id: storage_id(),
            digest: Digest::of(b"plan"),
            len: 100,
        });
        let encoded = encode_head(&head).unwrap_or_else(|error| panic!("encode failed: {error}"));
        let decoded =
            decode_head(&encoded).unwrap_or_else(|error| panic!("decode failed: {error}"));
        assert_eq!(decoded, head);

        head.retention_ids
            .insert(crate::RetentionId::from_uuid(uuid::Uuid::from_u128(3)));
        assert!(matches!(encode_head(&head), Err(Error::InvalidFormat(_))));
        head.active_plan = None;
        head.retention_ids
            .insert(crate::RetentionId::from_uuid(uuid::Uuid::from_u128(4)));
        assert!(matches!(encode_head(&head), Err(Error::InvalidFormat(_))));
    }

    #[test]
    fn commit_round_trip_preserves_opaque_bytes() {
        let commit = Commit {
            log_id: log_id(),
            incarnation: incarnation(),
            transaction_id: crate::TransactionId::new(),
            expected_tip: Some(Digest::of(b"prior")),
            operation: Bytes::from_static(b"operation"),
            result: Bytes::from_static(b"result"),
            objects: vec![ObjectRef {
                kind: ObjectKind::Blob,
                storage_id: storage_id(),
                digest: Digest::of(b"blob"),
                len: 4,
            }],
        };

        let encoded =
            encode_commit(&commit).unwrap_or_else(|error| panic!("encode failed: {error}"));
        let decoded =
            decode_commit(&encoded).unwrap_or_else(|error| panic!("decode failed: {error}"));
        assert_eq!(decoded, commit);
    }

    #[test]
    fn checkpoint_encoder_preserves_format_and_snapshot() {
        let checkpoint = Checkpoint {
            log_id: log_id(),
            incarnation: incarnation(),
            through_sequence: 3,
            through_commit: Digest::of(b"commit"),
            snapshot: Bytes::from_static(b"opaque snapshot"),
            objects: vec![ObjectRef {
                kind: ObjectKind::Blob,
                storage_id: storage_id(),
                digest: Digest::of(b"blob"),
                len: 4,
            }],
        };

        let encoded =
            encode_checkpoint(&checkpoint).unwrap_or_else(|error| panic!("encode failed: {error}"));
        let owned = encode_envelope(&CheckpointWire {
            format_version: FORMAT_VERSION,
            log_id: checkpoint.log_id.to_string(),
            through_sequence: checkpoint.through_sequence,
            through_commit: checkpoint.through_commit.as_bytes().to_vec(),
            snapshot: checkpoint.snapshot.to_vec(),
            incarnation: checkpoint.incarnation.as_bytes().to_vec(),
            objects: checkpoint.objects.iter().map(ObjectRefWire::from).collect(),
        })
        .unwrap_or_else(|error| panic!("owned encode failed: {error}"));
        let decoded =
            decode_checkpoint(&encoded).unwrap_or_else(|error| panic!("decode failed: {error}"));
        assert_eq!(encoded, owned);
        assert_eq!(decoded, checkpoint);
    }

    #[test]
    fn reference_node_round_trip_preserves_children() {
        let node = Node {
            payload: Bytes::from_static(b"page map"),
            children: vec![ObjectRef {
                kind: ObjectKind::Blob,
                storage_id: storage_id(),
                digest: Digest::of(b"page"),
                len: 4,
            }],
        };

        let encoded = encode_node(&node).unwrap_or_else(|error| panic!("encode failed: {error}"));
        let decoded =
            decode_node(&encoded).unwrap_or_else(|error| panic!("decode failed: {error}"));
        assert_eq!(decoded, node);
    }

    #[test]
    fn recovery_token_round_trip_preserves_the_exact_candidate() {
        let prepared = crate::PreparedCommit {
            view: crate::View {
                observed: Arc::new(crate::ObservedState {
                    head: Head::empty(log_id(), incarnation(), Options::default()),
                    version: object_store::UpdateVersion {
                        e_tag: Some("etag".to_owned()),
                        version: Some("version".to_owned()),
                    },
                }),
            },
            staging_domain: Arc::new(crate::StagingDomain),
            transaction_id: crate::TransactionId::new(),
            storage_id: storage_id(),
            operation: Bytes::from_static(b"operation"),
            result: Bytes::from_static(b"result"),
            objects: vec![ObjectRef {
                kind: ObjectKind::Blob,
                storage_id: storage_id(),
                digest: Digest::of(b"blob"),
                len: 4,
            }],
        };

        let encoded = encode_recovery_token(&prepared)
            .unwrap_or_else(|error| panic!("encode failed: {error}"));
        let decoded = decode_recovery_token(&encoded)
            .unwrap_or_else(|error| panic!("decode failed: {error}"));
        assert_eq!(decoded.view.head(), prepared.view.head());
        assert_eq!(
            decoded.view.storage_version(),
            prepared.view.storage_version()
        );
        assert_eq!(decoded.transaction_id, prepared.transaction_id);
        assert_eq!(decoded.storage_id, prepared.storage_id);
        assert_eq!(decoded.operation, prepared.operation);
        assert_eq!(decoded.result, prepared.result);
        assert_eq!(decoded.objects, prepared.objects);
    }

    #[test]
    fn recovery_token_excludes_staging_proof() {
        let mut prepared = crate::PreparedCommit {
            view: crate::View {
                observed: Arc::new(crate::ObservedState {
                    head: Head::empty(log_id(), incarnation(), Options::default()),
                    version: object_store::UpdateVersion {
                        e_tag: Some("etag".to_owned()),
                        version: Some("version".to_owned()),
                    },
                }),
            },
            staging_domain: Arc::new(crate::StagingDomain),
            transaction_id: crate::TransactionId::from_uuid(uuid::Uuid::from_u128(3)),
            storage_id: storage_id(),
            operation: Bytes::from_static(b"operation"),
            result: Bytes::from_static(b"result"),
            objects: vec![ObjectRef {
                kind: ObjectKind::Blob,
                storage_id: crate::StorageId::from_uuid(uuid::Uuid::from_u128(4)),
                digest: Digest::of(b"blob"),
                len: 4,
            }],
        };
        let without_proof = encode_recovery_token(&prepared)
            .unwrap_or_else(|error| panic!("encode failed: {error}"));
        prepared.staging_domain = Arc::new(crate::StagingDomain);
        let with_proof = encode_recovery_token(&prepared)
            .unwrap_or_else(|error| panic!("encode failed: {error}"));

        assert_eq!(with_proof, without_proof);
    }

    #[tokio::test]
    async fn recovery_token_cannot_change_the_durable_options() {
        use object_store::memory::InMemory;
        use object_store::path::Path;

        let id = log_id();
        let backend =
            crate::ValidatedBackend::new(Arc::new(InMemory::new()), Path::from("format-tests"))
                .await
                .unwrap_or_else(|error| panic!("backend validation failed: {error}"));
        let log = crate::Log::open(&backend, &id, Options::default())
            .await
            .unwrap_or_else(|error| panic!("open failed: {error}"));
        let view = log
            .load()
            .await
            .unwrap_or_else(|error| panic!("load failed: {error}"));
        let prepared = log
            .prepare(
                &view,
                crate::TransactionId::new(),
                Bytes::from_static(b"operation"),
                Bytes::new(),
                Vec::new(),
            )
            .unwrap_or_else(|error| panic!("prepare failed: {error}"));
        let mut tampered = decode_recovery_token(
            &prepared
                .recovery_token()
                .unwrap_or_else(|error| panic!("token failed: {error}")),
        )
        .unwrap_or_else(|error| panic!("decode failed: {error}"));
        Arc::get_mut(&mut tampered.view.observed)
            .unwrap_or_else(|| panic!("decoded view is unexpectedly shared"))
            .head
            .options
            .max_tail_entries = 1;
        let token = encode_recovery_token(&tampered)
            .unwrap_or_else(|error| panic!("encode failed: {error}"));

        assert!(matches!(
            log.resume(&token).await,
            Err(Error::ConfigurationMismatch("options"))
        ));
    }

    #[test]
    fn changed_envelope_is_corrupt() {
        let mut encoded = encode_commit(&Commit {
            log_id: log_id(),
            incarnation: incarnation(),
            transaction_id: crate::TransactionId::new(),
            expected_tip: None,
            operation: Bytes::from_static(b"operation"),
            result: Bytes::new(),
            objects: Vec::new(),
        })
        .unwrap_or_else(|error| panic!("encode failed: {error}"))
        .to_vec();
        let index = encoded.len().saturating_sub(1);
        encoded[index] ^= 1;

        assert!(matches!(decode_commit(&encoded), Err(Error::CorruptObject)));
    }

    #[test]
    fn head_rejects_sequence_gap() {
        let head = Head {
            log_id: log_id(),
            incarnation: incarnation(),
            options: Options::default(),
            generation: 1,
            next_sequence: 2,
            checkpoint: None,
            tail: vec![commit_ref(1, b"gap")],
            recent_outcomes: Vec::new(),
            collection_epoch: 0,
            active_plan: None,
            retention_ids: BTreeSet::new(),
        };
        assert!(matches!(encode_head(&head), Err(Error::InvalidFormat(_))));
    }

    #[test]
    fn head_rejects_checkpoint_sequence_overflow() {
        let head = Head {
            log_id: log_id(),
            incarnation: incarnation(),
            options: Options::default(),
            generation: 1,
            next_sequence: u64::MAX,
            checkpoint: Some(CheckpointRef {
                through_sequence: u64::MAX,
                through_commit: Digest::of(b"commit"),
                object: ObjectRef {
                    kind: ObjectKind::Checkpoint,
                    storage_id: storage_id(),
                    digest: Digest::of(b"checkpoint"),
                    len: 10,
                },
            }),
            tail: Vec::new(),
            recent_outcomes: Vec::new(),
            collection_epoch: 0,
            active_plan: None,
            retention_ids: BTreeSet::new(),
        };

        assert!(matches!(encode_head(&head), Err(Error::InvalidFormat(_))));
    }

    #[test]
    fn head_rejects_duplicate_transaction_across_base_and_tail() {
        let compacted = commit_ref(0, b"compacted");
        let mut active = commit_ref(1, b"active");
        active.transaction_id = compacted.transaction_id;
        let head = Head {
            log_id: log_id(),
            incarnation: incarnation(),
            options: Options::default(),
            generation: 2,
            next_sequence: 2,
            checkpoint: Some(CheckpointRef {
                through_sequence: 0,
                through_commit: compacted.digest,
                object: ObjectRef {
                    kind: ObjectKind::Checkpoint,
                    storage_id: storage_id(),
                    digest: Digest::of(b"checkpoint"),
                    len: 10,
                },
            }),
            tail: vec![active],
            recent_outcomes: vec![compacted],
            collection_epoch: 0,
            active_plan: None,
            retention_ids: BTreeSet::new(),
        };

        assert!(matches!(encode_head(&head), Err(Error::InvalidFormat(_))));
    }

    #[test]
    fn head_rejects_outcomes_without_a_checkpoint() {
        let active = commit_ref(0, b"active");
        let outcome = commit_ref(0, b"outcome");
        let head = Head {
            log_id: log_id(),
            incarnation: incarnation(),
            options: Options::default(),
            generation: 1,
            next_sequence: 1,
            checkpoint: None,
            tail: vec![active],
            recent_outcomes: vec![outcome],
            collection_epoch: 0,
            active_plan: None,
            retention_ids: BTreeSet::new(),
        };

        assert!(matches!(encode_head(&head), Err(Error::InvalidFormat(_))));
    }

    #[test]
    fn head_enforces_its_durable_bounds_and_sequence_evidence() {
        let compacted = commit_ref(0, b"compacted");
        let active = commit_ref(1, b"active");
        let head = Head {
            log_id: log_id(),
            incarnation: incarnation(),
            options: Options {
                max_tail_entries: 1,
                resolution_window: 1,
                ..Options::default()
            },
            generation: 2,
            next_sequence: 2,
            checkpoint: Some(CheckpointRef {
                through_sequence: 0,
                through_commit: compacted.digest,
                object: ObjectRef {
                    kind: ObjectKind::Checkpoint,
                    storage_id: storage_id(),
                    digest: Digest::of(b"checkpoint"),
                    len: 10,
                },
            }),
            tail: vec![active],
            recent_outcomes: vec![compacted],
            collection_epoch: 0,
            active_plan: None,
            retention_ids: BTreeSet::new(),
        };
        assert!(head.validate().is_ok());

        let mut too_many_tail_entries = head.clone();
        too_many_tail_entries.tail.push(commit_ref(2, b"extra"));
        too_many_tail_entries.next_sequence = 3;
        too_many_tail_entries.generation = 3;
        assert!(too_many_tail_entries.validate().is_err());

        let mut missing_evidence = head.clone();
        missing_evidence.recent_outcomes.clear();
        assert!(missing_evidence.validate().is_err());

        let mut generation_behind = head;
        generation_behind.generation = 1;
        assert!(generation_behind.validate().is_err());

        let mut mismatched_anchor = generation_behind;
        mismatched_anchor.generation = 2;
        mismatched_anchor
            .checkpoint
            .as_mut()
            .unwrap_or_else(|| panic!("test checkpoint is missing"))
            .through_commit = Digest::of(b"different commit");
        assert!(mismatched_anchor.validate().is_err());
    }

    #[test]
    fn empty_head_encoding_is_stable() {
        let encoded = encode_head(&Head::empty(log_id(), incarnation(), Options::default()))
            .unwrap_or_else(|error| panic!("encode failed: {error}"));
        assert_eq!(
            hex::encode(encoded),
            "a2015872aa0101026f74656e616e742e7265736f75726365030004000680078008500000000000000000000000000000000109ac0119040002190400031a000100000419100005190400061a00100000071a00040000081a01000000091a040000000a1904000b1a000186a00c1a010000000a000c80025820d66b6c9f2f2a86881c5a2254f9691643adbb7edb70ed057d47216fcda63d0067"
        );
    }

    #[test]
    fn future_format_version_fails_closed() {
        let encoded = encode_envelope(&head_wire(FORMAT_VERSION + 1))
            .unwrap_or_else(|error| panic!("encode failed: {error}"));

        assert!(matches!(
            decode_head(&encoded),
            Err(Error::InvalidFormat(_))
        ));
    }

    #[test]
    fn head_rejects_duplicate_and_unsorted_retention_ids() {
        for retention_ids in [
            vec![vec![2; 16].into(), vec![1; 16].into()],
            vec![vec![1; 16].into(); 2],
        ] {
            let mut wire = head_wire(FORMAT_VERSION);
            wire.retention_ids = retention_ids;
            let encoded =
                encode_envelope(&wire).unwrap_or_else(|error| panic!("encode failed: {error}"));
            assert!(matches!(
                decode_head(&encoded),
                Err(Error::InvalidFormat(_))
            ));
        }
    }

    #[test]
    fn unknown_field_is_not_canonical() -> std::result::Result<(), Box<dyn std::error::Error>> {
        let mut payload = Vec::new();
        let mut writer = minicbor::Encoder::new(&mut payload);
        writer
            .map(7)?
            .u8(1)?
            .u32(FORMAT_VERSION)?
            .u8(2)?
            .str(log_id().as_str())?
            .u8(3)?
            .u64(0)?
            .u8(4)?
            .u64(0)?
            .u8(6)?
            .array(0)?
            .u8(7)?
            .array(0)?
            .u8(99)?
            .null()?;
        let envelope_bytes = minicbor::to_vec(EnvelopeWire {
            digest: Digest::of(&payload).as_bytes().to_vec(),
            payload,
        })?;

        assert!(matches!(
            decode_head(&envelope_bytes),
            Err(Error::InvalidFormat(_))
        ));
        Ok(())
    }

    #[test]
    fn trailing_bytes_are_rejected() {
        let mut encoded = encode_head(&Head::empty(log_id(), incarnation(), Options::default()))
            .unwrap_or_else(|error| panic!("encode failed: {error}"))
            .to_vec();
        encoded.push(0);
        assert!(matches!(
            decode_head(&encoded),
            Err(Error::InvalidFormat(_))
        ));
    }
}
