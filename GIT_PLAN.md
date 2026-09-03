# Minimal serverless Git example plan

## Outcome

Build `object-log-git` as a separate demonstration crate. It must show how one
serverless request can open a repository, fetch or push Git data, publish one
atomic update, and discard all local state. The crate must use only the public
`object-log` API.

This goal follows SQLite and precedes the WASI filesystem adapter. It does not
block local SQLite completion.

## Storage model

- One object log owns one Git repository.
- Git objects stay in standard immutable pack files. Do not invent another
  object encoding.
- Each changed push stages zero or more packs and publishes one ordered ref
  transaction. The transaction contains each ref name, expected old object ID,
  and new object ID.
- The object-log head is the only mutable authority. Do not create mutable Git
  ref objects, a second manifest, a lease service, or a background coordinator.
- A checkpoint contains the complete direct-ref map and references the pack set
  required to serve it. Later commits contain incremental packs and ref
  transactions.
- A fetch or rebuild confirms the current head, retains the selected view,
  loads or materializes every pack that the view requires, and then releases
  retention. Garbage collection removes packs only after no current or retained
  view references them.
- A local bare repository, pack index, and object cache are disposable. A cold
  invocation can recover from the checkpoint and ordered tail.

This example uses Cursor's immutable WAL plus CAS publication model. It omits
Cursor's warm-owner routing, replication, batching, and physical pack
compaction. Checkpoints bound ref-log replay. Pack count and cold-recovery bytes
can grow without a fixed limit until a later pack-compaction stage.

## Minimal public API

```text
Repository::open(log) -> Repository
Repository::refs() -> RefSnapshot
Repository::materialize(path) -> MaterializedRepository
Repository::stage_push(transaction_id, commands, pack) -> PushStage
Repository::publish(staged) -> PushStatus
Repository::resume(recovery_token) -> PushResolution
Repository::checkpoint() -> GitCheckpointStatus
```

`stage_push` validates the pack, object IDs, ref names, expected old values,
object availability, and configured fast-forward policy before it stages any
publication. `publish` never reruns validation after a conflict. The caller can
refresh, check the commands against the new ref state, and stage a new logical
attempt.

The first crate supports direct refs under `refs/heads/` and `refs/tags/`. It
keeps `HEAD` as one configured symbolic ref. It rejects other symbolic refs,
replace refs, shallow state, alternates, hooks, and partial-clone promises until
their behavior is explicit.

## Serverless example

Provide one small HTTP example after the storage crate is stable. It can use
Git's standard stateless RPC boundary or a proven Git protocol library. The
handler must keep authentication and repository routing outside
`object-log-git`.

Each request must:

1. Derive one validated tenant and repository log ID.
2. Load or materialize the requested repository view.
3. Serve a fetch, or validate and stage one push.
4. Publish the push with object-log compare-and-swap.
5. Return success only after a definite commit result.

An uncertain publication returns a stable recovery token. A retry resolves the
same push. It must not accept the pack again as a new transaction.

## Required evidence

- Empty repository creation and cold recovery need no retained local files.
- Clone, fetch, branch creation, tag creation, fast-forward push, and ref delete
  work with an unmodified Git client.
- Invalid packs, missing objects, invalid ref names, non-fast-forward updates,
  and wrong expected object IDs fail before publication.
- Two pushes from one view have one object-log order. A conflict never changes
  refs and never publishes a rejected pack reference.
- Lost publication responses resolve to the original push.
- A checkpoint plus tail produces the same refs and reachable Git object IDs as
  full replay.
- Collection preserves packs used by the current or a retained view and removes
  unreachable incremental packs.
- Tests cover SHA-1 and SHA-256 repositories if the selected Git library
  supports both. Otherwise the crate rejects the unsupported object format.
- Benchmarks report cold clone, warm fetch, small push, large pack push,
  checkpoint, recovery, object-store request count, and byte amplification.
- One opt-in local MinIO flow uses a disposable bucket and leaves no process or
  container behind.

## Tasks

1. Select the smallest maintained Git pack and protocol library.
2. Define canonical ref-transaction and checkpoint records.
3. Add strict codec and pack-validation tests.
4. Implement stage, publish, recovery, and explicit conflict handling.
5. Implement cold materialization and checkpoint recovery.
6. Add the minimal standard Git client example.
7. Add race, failure, collection, and MinIO acceptance tests.
8. Add benchmarks and complete a correctness and deletion review.

## Limits

Keep Git policy outside the generic log. Start with one repository per log and
one push per publication. Do not add pack rewriting, global deduplication,
cross-repository transactions, a preferred owner, or provider-specific storage
behavior in this example.
