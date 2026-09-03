# Minimal serverless Git example plan

## Outcome

Build `object-log-git` as a separate demonstration crate. One serverless
request can recover a repository, run a standard Git fetch or push, publish one
atomic update, and discard all local files. The crate uses only the public
`object-log` API.

This goal follows the local SQLite demonstration. The cross-example API review
in issue #13 follows this goal.

## Git boundary

Use the installed upstream Git implementation as the Git engine. Do not
implement pack validation, object connectivity, ancestry, ref transactions, or
smart HTTP framing in this crate. Do not add `gix` or `git2` for the first
version. `gix` has no complete repository verification or receive-pack server.
`git2` adds a C FFI boundary and also has no receive-pack server.

The local HTTP example runs `git http-backend` once for each request. A fetch
uses a fresh recovered bare repository. A push also uses a fresh repository,
but the handler holds Git's response until object-log confirms the index CAS.
A conflict or unresolved result cannot return Git's success response.

The executable Git dependency is explicit. A cloud image must contain the
tested Git version and disposable local storage. This is not a direct WASI
component.

## Storage model

- One object log owns one Git repository.
- Git objects stay in standard immutable pack files.
- A large pack is a reference node over fixed-size blob chunks.
- A commit contains one ordered ref transaction and its new pack roots.
- The object-log index is the only mutable authority.
- A checkpoint contains the complete direct-ref map and the selected live pack
  descriptors. Its object roots align with those descriptors.
- A cold request rebuilds pack indexes and refs in a new bare repository.
- Local Git configuration, indexes, hooks, reflogs, and temporary files are not
  durable state.

Configure receive-pack to retain non-empty pushes as packs, check received
objects, reject non-fast-forward branch updates, permit deletes, disable hooks,
disable automatic maintenance, and avoid reflogs. Git repairs thin network
packs before the adapter reads the stored pack.

The pack store must keep the exact bytes at each immutable physical key until
object-log collection deletes that key. External expiry, deletion, or
overwrite violates this contract. A reopened handle verifies existing object
graphs before it republishes their references.

This design follows Cursor's immutable WAL plus CAS publication model. It does
not include Cursor's warm-owner routing, replication, batching, or physical
pack compaction. Checkpoints bound ref-log replay. Live pack count and recovery
bytes can still grow until a later pack-compaction stage.

## Public API target

```text
Repository::open(log, cache_path, config) -> Repository
Repository::path() -> Path
Repository::refs() -> RefSnapshot
Repository::stage_push(transaction_id) -> StageStatus
StagedPush::publish() -> CommitStatus
Repository::resume(recovery_token) -> Resolution
Repository::checkpoint() -> GitCheckpointStatus
```

`open` includes cold materialization. The HTTP adapter lets upstream Git change
the disposable repository, then calls `stage_push`. Staging checks the complete
repository, compares refs and packs with the materialized state, and stages one
candidate. The borrow-bound staged value prevents publication through another
repository instance.

The first crate supports SHA-1 and SHA-256 repositories, direct refs under
`refs/heads/` and `refs/tags/`, and one configured symbolic `HEAD`. It rejects
other symbolic refs, loose objects after receive, replace refs, shallow state,
alternates, hooks, and partial-clone state.

## Checkpoints and collection

A checkpoint keeps each pack that contains at least one object reachable from
the current refs. It can omit a pack that contains no reachable object. The
selected pack set must cover every reachable object before publication.

This is conservative pack selection. It is not pack rewriting. It can collect
a feature-only pack after all refs to that feature are deleted. It cannot
remove unreachable bytes from a pack that also contains live objects.

## Required evidence

- Empty creation and cold recovery need no retained local files.
- An unmodified client can clone, fetch, create a branch and tag, push a
  fast-forward update, and delete refs through loopback smart HTTP.
- Git rejects invalid packs, missing objects, invalid refs, and non-fast-forward
  branch updates before object-log publication.
- Two pushes from one view produce one winner. The loser changes no durable
  refs and publishes no pack reference.
- A lost publication response resolves the original transaction without
  another pack upload.
- A checkpoint plus tail produces the same refs and reachable object IDs as
  full replay.
- Collection preserves current and retained packs and removes an
  unreachable-only pack after checkpoint publication.
- The same lifecycle runs for SHA-1 and SHA-256.
- Benchmarks report cold clone, warm fetch, small push, large pack push,
  checkpoint, recovery, object-store requests, bytes, and disk use.
- One opt-in local MinIO flow uses a disposable bucket and leaves no process or
  container behind.

## Fixed tranche

1. Define strict ref, object-ID, pack, transaction, and checkpoint records.
2. Implement materialization with standard packs and rebuilt indexes.
3. Implement repository inspection and strict upstream Git validation.
4. Implement pack chunk staging and atomic ref publication.
5. Implement pending resolution without another pack upload.
6. Implement live-pack checkpoint selection.
7. Add focused state, corruption, race, and collection tests.
8. Add the loopback smart HTTP lifecycle test.
9. Add SHA-1 and SHA-256 coverage.
10. Add adapter benchmarks and request and byte accounting.
11. Add one opt-in pinned MinIO lifecycle.
12. Record local evidence and run an independent correctness and deletion
    review.

## Limits

Keep Git policy outside the generic log. Start with one repository for each log
and one push for each publication. Do not add pack rewriting, global
deduplication, cross-repository transactions, a preferred owner,
provider-specific behavior, Spin integration, or live AWS work.
