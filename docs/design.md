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
reject non-canonical bytes, unknown fields, and unsupported versions. Any
schema change requires a new format version. The first release does not support
mixed-version writers. This strict rule replaces Micelio's unknown-field
preservation rule because the selected CBOR codec does not retain unknown
fields during an index rewrite.

## Head

The durable head contains these logical fields:

```text
format_version
log_identity
generation
base_checkpoint
tail[]
recent_outcomes[]
integrity_digest
```

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
transaction_id
expected_generation
expected_tip
operation_bytes
result_bytes
object_refs[]
integrity_digest
```

The operation and result have configured size limits. Large data belongs in a
staged blob. The digest covers the canonical encoded bytes. Object-store ETags
are concurrency tokens and are not content-integrity hashes.

## Open and refresh

`open` validates the backend contract and namespace. It does not load all log
data.

`load` reads `head`, then loads the checkpoint and commit tail. Tail objects can
be fetched concurrently because the head contains their complete ordered
references.

`refresh` uses a conditional read when the backend supports it. An unchanged
head returns `NotModified`. A changed head returns the new view and the missing
tail or a new checkpoint when the prior cursor has fallen behind the base.

## Commit

The caller prepares one candidate against one cursor.

1. Validate all sizes, references, log identities, and the expected cursor.
2. Create each missing immutable blob.
3. Create the immutable commit object.
4. Build a new head that appends the commit reference.
5. Conditionally replace the observed head version.

The result is:

- `Committed` when the conditional update returns success.
- `Conflict` when the store returns a definite precondition failure.
- `Pending` when a timeout or transport failure can hide success.

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
the evidence incomplete.

## Checkpoint

A checkpoint contains opaque snapshot bytes and the exact covered tail
position. Its digest binds both.

Publication reads the current head and verifies that the covered commit is in
its history. The new head replaces the base checkpoint, removes the covered
tail prefix, preserves the suffix, preserves the resolution window, and
increments the generation. A CAS conflict causes revalidation against the new
head. It does not make the snapshot valid for a different prefix.

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
