# Protocol design

## Storage layout

```text
<prefix>/v1/logs/<log-id>/index.cbor
<prefix>/v1/logs/<log-id>/wal/<digest>.cbor
<prefix>/v1/logs/<log-id>/objects/<digest>
<prefix>/v1/logs/<log-id>/bases/<digest>.cbor
```

The library derives every key. A caller cannot supply a raw object path after
opening a log.

Only `index.cbor` is mutable. Its update version is an opaque token supplied by the
object store. Every other object uses create-only publication at a digest-based
key.

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

`open` validates the backend contract and namespace. It creates the initial
index when needed. It does not load all log data.

The current capability probe writes and deletes one private object on every
open. Backend credentials therefore need delete permission for the probe even
though the log protocol does not delete durable log data. Moving this probe to
an explicit provisioning result is an operability follow-on.

`load` reads and validates only the index. `read_checkpoint` reads its base.
`read_tail` fetches active WAL entries concurrently because the index contains
their complete ordered references. The materializer uses these operations to
restore the complete state.

`refresh` uses a conditional read. An unchanged index returns `NotModified`. A
changed index returns its new view. The caller then reads the base and active
tail that the view names.

## Commit

The caller prepares one candidate against one cursor.

1. Validate all sizes, references, log identities, and the expected cursor.
2. Verify that every referenced blob is already durable and valid.
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

A checkpoint contains opaque snapshot bytes and the exact covered tail
position. Its digest binds both.

Publication validates every WAL entry and referenced blob in the supplied view.
It also validates that the covered commit is in that view. The new index
replaces the base checkpoint, removes the covered tail prefix, preserves the
suffix, preserves the resolution window, and increments the generation.

A definite CAS failure returns `Conflict`. An uncertain update returns a
`PendingCheckpoint`. `resolve_checkpoint` retries only the exact original
checkpoint against its exact source view. Later head movement can make the
outcome `Expired`.

The core treats snapshot bytes as opaque. It proves which log prefix the bytes
claim to cover. The materializer must prove that the snapshot is the correct
state for that prefix. This is the checkpoint trust boundary.

The first release retains prior objects. A later garbage collector will need a
durable reachability and grace-period protocol.

## Materializer

The optional helper has a narrow role:

```rust
trait Materializer {
    type State;
    type Error;

    fn empty(&self) -> Self::State;
    fn restore(&self, checkpoint: &[u8]) -> Result<Self::State, Self::Error>;
    fn apply(
        &self,
        state: &mut Self::State,
        sequence: u64,
        operation: &[u8],
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
operations become one commit record. After the commit publishes, the owner
replies to all operations with their recorded results. A conflict discards the
tentative state, refreshes, and validates the operations again.

If ownership changes, the new owner loads the current head. An old owner can
still attempt a write, but it cannot overwrite a newer head. It loses the CAS,
refreshes, and stops accepting ownership work.

This layer increases logical operation throughput. It does not change the
linearization point or allow acknowledgement before durable publication.
