# Protocol design

## Storage layout

```text
<prefix>/v1/logs/<log-id>/index.cbor
<prefix>/v1/logs/<log-id>/data/<incarnation>/<kind>/<storage-id>/<digest>
```

The library derives every key. A caller cannot supply a raw object path after
opening a log.

Only `index.cbor` is mutable. Its update version is an opaque token from the
object store. Every other object uses create-only publication. Its key contains
a random physical storage ID and a deterministic BLAKE3 content digest. A new
blob, reference node, checkpoint, or collection plan gets a new physical ID.
Exact commit recovery reuses only the physical ID in its recovery evidence.
This prevents an old delete from addressing a later write with the same content
digest.

This structure follows Cursor's mutable metadata plus immutable object model.
It also follows Micelio's concrete split between one index, ordered entry
pointers, content-addressed WAL entries, payload objects, and bases. The Rust
type `Head` is the decoded index. The Rust type `Commit` is one WAL entry.

The durable encoding is CBOR, not Protobuf. Each structure is a CBOR map with
stable positive integer keys. The exact schema is in
`schema/object-log-v1.cddl`. Encoders write keys in ascending order. Decoders
reject non-canonical bytes, unknown fields, and unsupported versions. Before
the first tagged release, the schema can change while its version remains 1.
The first release will not support mixed-version writers. This strict rule replaces Micelio's unknown-field
preservation rule because the selected CBOR codec does not retain unknown
fields during an index rewrite. The current `0.1.0` format is pre-release. It
rejects older development data and has no compatibility guarantee. After the
first tagged durable-format release, every incompatible schema change must use
a new format version.

## Head

The durable head contains these logical fields:

```text
format_version
log_identity
incarnation_id
options
generation
base_checkpoint
tail[]
recent_outcomes[]
collection_epoch
retention_ids[]
active_collection_plan
integrity_digest
```

`incarnation_id` is random and durable. It prevents a cursor from one deleted
or independent log from authorizing writes to another log with the same text
identifier. It is only a namespace salt. WAL entries, payloads, and bases use
deterministic BLAKE3 content identities within that namespace.

`options` records all limits that affect durable validation. Every writer must
open the log with the same options.

`generation` increases for every head update, including maintenance updates.
`tail` is ordered. Each element contains a sequence, transaction ID, entry
digest, and encoded entry length. As in Micelio, the immutable entry has no
assigned sequence. The mutable index pointer assigns its position. This keeps
the entry content-addressable before publication.

`recent_outcomes` is a bounded resolution window. It records enough data to
identify the result of a recent transaction after checkpointing removes its
commit from the active tail. Expiry must be explicit.

`collection_epoch` increases when a positive deletion plan becomes active.
`retention_ids` is a bounded sorted set. `active_collection_plan` is empty or
names one immutable positive deletion plan. Retentions and an active plan
cannot exist together.

The implementation must enforce encoded byte and entry-count limits. It must
request a checkpoint before the head becomes an unbounded manifest.

## Immutable commit

A commit contains:

```text
format_version
log_identity
incarnation_id
transaction_id
expected_tip
operation_bytes
result_bytes
object_refs[]
integrity_digest
```

The operation, result, encoded commit, object count, and referenced objects
have configured limits. Large data belongs in a blob that the caller stages
before it prepares the commit. The digest covers the canonical encoded bytes.
Object-store ETags are concurrency tokens and are not content-integrity hashes.

## Open and refresh

`ValidatedBackend::new` validates one backend and root once. It returns a typed
handle that derives tenant scopes without storage requests. `Log::open` creates
the initial index when needed. It does not probe the backend or load all log
data. This keeps tenant open and close cheap.

The capability probe writes and deletes one private object when the backend
handle is created. Provisioning and collection credentials need delete
permission. Only the capability probe and collection issue deletes.

`load` reads and validates only the index. `read_checkpoint` reads its base.
`read_tail` fetches active WAL entries concurrently because the index contains
their complete ordered references. It does not fetch referenced payloads or
nodes. An adapter reads only the objects that it needs. The materializer uses
these operations to restore the complete state.

`refresh` uses a conditional read. An unchanged index returns `NotModified`. A
changed index returns its new view. The caller then reads the base and active
tail that the view names.

## Commit

The caller prepares one candidate against one cursor.

1. Validate all sizes, references, log identities, and the expected cursor.
2. Verify the complete transitive object graph. This also proves that each
   referenced blob or node is durable and valid.
3. Create the immutable commit object.
4. Build a new head that appends the commit reference.
5. Conditionally replace the observed head version.

The result is:

- `Committed` when the conditional update returns success.
- `Conflict` when the store rejects the update and the winner can be read.
- `Pending` when the safe final view or classification is not available. This
  includes an ambiguous update result and a rejected update followed by a
  failed read of the winner.

The core never retries a candidate against a newer cursor. The application must
read the winning operations, validate its intent again, and prepare a new
candidate. The transaction ID can remain stable. The commit digest changes
because its expected position changes.

## Pending resolution

Resolution reads the current head:

- A matching transaction ID and commit digest proves success.
- The original head still present permits retry of the exact conditional write.
- A different winner directly after the expected head proves that the candidate
  did not publish.
- A checkpointed result is resolved from `recent_outcomes`.
- Missing evidence after resolution-window expiry returns `Expired`.
- Store unavailability returns `StillPending`.

An implementation must not report `NotCommitted` when history movement makes
the evidence incomplete. `Expired` is indeterminate. It does not prove that the
operation failed. An application must not submit a non-idempotent operation as
new work after this result.

`PreparedCommit::recovery_token` encodes the exact source cursor, operation,
result, object references, and transaction ID. The caller must persist this
token before publication if process-loss recovery is required. `Log::resume`
can stage the missing WAL object and retry only the original conditional head
update.

## Checkpoint

A checkpoint contains opaque snapshot bytes, explicit object roots, and the
exact covered tail position. Its digest binds all three. A root can be a blob or
a reference node. A reference node contains opaque adapter bytes and explicit
children. This forms a content-addressed tree with bounded fan-out and no
fixed depth.

Publication validates every WAL entry in the supplied view and every declared
checkpoint root. It also validates that the covered commit is in that view. The new index
replaces the base checkpoint, removes the covered tail prefix, preserves the
suffix, preserves the resolution window, and increments the generation.

A definite CAS failure returns `Conflict`. An uncertain update returns a
`PendingCheckpoint`. `resolve_checkpoint` retries only the exact original
checkpoint against its exact source view. Later head movement can make the
outcome `Expired`.

The core treats snapshot and node payload bytes as opaque. The adapter must put
every durable dependency in the checkpoint roots or a reference-node edge.
Opaque bytes must not hide another durable reachability graph. The materializer
must also prove that the snapshot is the correct state for the covered prefix.
These are the checkpoint trust boundaries.

## Garbage collection

Garbage collection follows the Cursor-style positive-plan model. The head is
the only mutable authority.

`start_collection` requires no retention and no active plan. It validates the
active tail, current checkpoint, and their complete transitive object graph.
It then lists one bounded log scope. Unknown entries count against the scan
limit but cannot enter the deletion set. If unreachable immutable objects
exist, the method writes one sorted positive plan and installs its reference
with the head CAS that increments the collection epoch. Candidate deletion
starts only after that fence is durable.

Every head update preserves an active plan. Commit and checkpoint publication
read that plan. They reject a direct or transitive reference to a planned key.
They also reject a planned commit key. A checkpoint staging call selects
another physical ID if its first new key is in the plan. Full graph validation
also runs without a fence. This prevents publication of an old node whose
child was deleted by an earlier collection.

`resume_collection` first loads the current head and confirms the exact plan.
It submits the complete positive set in batches of at most 1,000 keys. A
missing key is success. An error or cancellation leaves the plan active. A
retry submits the complete set again. After all candidate submissions succeed,
one head CAS clears the exact plan. The protocol has no progress bitmap,
collector lease, background worker, or second authority.

The plan object is not in its positive set. After a definite rejected fence
CAS or a successful clear, the library deletes the plan object on a
best-effort basis. A later collection can remove it if that cleanup fails.

A retention ID protects the full log namespace and has no automatic expiry.
Any retention blocks plan installation. An active plan blocks a new retention.
A caller reuses one ID only to resolve an uncertain retention update. It uses a
new ID after a confirmed release.

Object, node, tail, and checkpoint reads take a `View`. A missing object from a
view in an older collection epoch returns `ViewExpired`. A missing object from
the current epoch is corruption. An epoch can advance only when the fence CAS
observes no retention, so later reuse of the same retention ID cannot restore
an older view.

Compacted commit bodies are not live only because the head retains the complete
recent outcome evidence needed for commit resolution. The active tail, current
checkpoint, and their transitive object graph remain live. A valid
content-addressed cycle cannot be formed without breaking digest verification.

## Materializer

The optional helper has a narrow role. It receives the explicit object
references with each opaque snapshot or operation. It can keep those references
for lazy adapter reads.

```rust
trait Materializer {
    type State;
    type Error;

    fn empty(&self) -> Self::State;
    fn restore(
        &self,
        checkpoint: &[u8],
        objects: &[ObjectRef],
    ) -> Result<Self::State, Self::Error>;
    fn apply(
        &self,
        state: &mut Self::State,
        sequence: u64,
        operation: &[u8],
        objects: &[ObjectRef],
    ) -> Result<(), Self::Error>;
    fn checkpoint(&self, state: &Self::State) -> Result<Vec<u8>, Self::Error>;
}
```

The core log can be used without this trait. Domain transactions and query APIs
do not belong in the core.

## Preferred owner and group commit

Rendezvous hashing over the live process set selects a preferred process for a
log. Requests normally route to it. Ownership is advisory.

The owner keeps:

- The current cursor and materialized state.
- A bounded request queue.
- One active commit builder.
- A short batch timer and a maximum batch size.

It applies queued operations in order to a tentative state. Compatible
operations become one commit record. Admission starts publication in a
detached task so cancellation by one caller cannot stop an admitted batch.
After the commit publishes, the owner replies through one-shot channels. A
conflict discards the tentative state, refreshes, and validates the operations
again. Queue limits apply to request count and bytes.

If ownership changes, the new owner loads the current head. An old owner can
still attempt a write, but it cannot overwrite a newer head. It loses the CAS,
refreshes, and stops accepting ownership work.

This layer increases logical operation throughput. It does not change the
linearization point or allow acknowledgement before durable publication.
