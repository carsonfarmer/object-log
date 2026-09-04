# Git storage adapter plan

## Outcome

`object-log` is the product. It is a small object-storage WAL for higher-level
storage systems. The `object-log-git` crate is one proof of its public API.

Phase 1 builds a native Git storage adapter. It accepts parsed ref commands and
a pack. It validates both inputs, publishes one atomic object-log update, and
recovers a standard bare Git repository from object storage. The adapter uses
only the public `object-log` API.

Smart HTTP follows this storage proof. Git protocol code and Git libraries do
not belong in the core WAL.

This goal follows the local SQLite demonstration. The cross-example API review
in issue #13 follows this goal.

## Library choice

Use `gix` and `gix-pack` with pure-Rust features. They provide pack parsing,
object access, reference validation, and repository materialization. The
adapter does not run a Git executable and does not use C FFI.

The first target is a native serverless function or container with disposable
local storage. This includes Linux services that provide a temporary directory.
The adapter can perform blocking pack and repository work on a bounded worker.

The current `gix` storage path does not work as a WASI guest. Its high-level
pack path uses memory maps, which `memmap2` does not support on WASI. The
`gix-pack` `wasm` feature also removes its high-level pack writer. A future WASI
adapter needs lower-level streaming pack APIs and a different object lookup.
Do not add that work to phase 1.

[`docs/evidence/git-library-selection-2026-09-03.md`](docs/evidence/git-library-selection-2026-09-03.md)
records the library review and compile checks.

## Adapter boundary

The storage adapter accepts domain values instead of Git wire data:

```text
GitRepository::open(log, cache_path, config) -> GitRepository
GitRepository::prepare_push(commands, pack) -> PreparedPush
PreparedPush::publish() -> CommitStatus
GitRepository::resume(recovery_token) -> Resolution
GitRepository::materialize(path) -> MaterializedRepository
GitRepository::checkpoint() -> GitCheckpointStatus
```

`prepare_push` parses and validates the pack. It checks each ref command against
the current snapshot. `PreparedPush` is opaque and belongs to the repository
handle that created it. This prevents callers from claiming that arbitrary pack
bytes passed validation.

`publish` stages new pack chunks and commits one ref transaction. A conflict or
unresolved result cannot report success. `resume` resolves a lost publication
response without another pack upload. `materialize` creates a standard bare
repository with standard pack files, indexes, refs, and `HEAD`.

The first crate supports SHA-1 and SHA-256 repositories. It supports direct refs
under `refs/heads/` and `refs/tags/`, plus one configured symbolic `HEAD`. It
rejects other symbolic refs, replace refs, shallow state, alternates, hooks,
partial-clone state, and invalid object graphs.

## Storage model

- One object log owns one Git repository.
- Git objects stay in standard immutable pack files.
- A large pack is a reference node over fixed-size blob chunks.
- A commit contains one ordered ref transaction and its new pack roots.
- The object-log index is the only mutable authority.
- A checkpoint contains the direct-ref map and selected live pack descriptors.
- A cold request rebuilds pack indexes and refs in a new bare repository.
- Local configuration, hooks, reflogs, and temporary files are cache state.

Normalize each thin input pack into a self-contained pack. Check pack checksums,
object IDs, deltas, object availability, and connectivity. Reject invalid ref
names and non-fast-forward branch updates.

The pack store keeps the exact bytes at each immutable physical key until
object-log collection deletes that key. External expiry, deletion, or overwrite
violates this contract. A reopened handle verifies existing object graphs before
it republishes their references.

The layout follows Cursor's immutable WAL and CAS publication model. It omits
warm-owner routing, replication, batching, and physical pack compaction.
Checkpoints bound ref-log replay. Live pack count and recovery bytes can still
grow until a later pack-compaction stage.

## Smart HTTP phase

Build smart HTTP only after phase 1 proves the storage API. Keep it in a separate
crate. The adapter parses packet lines, capabilities, ref commands, and pack
input. It passes parsed commands and pack bytes to `object-log-git`.

Authentication, tenant routing, and HTTP server policy stay outside both the
core WAL and the Git storage adapter. A push response waits for object-log to
confirm publication. A conflict or unresolved result returns a protocol error.

The HTTP phase must prove clone, fetch, and push with an unmodified Git client.
It can use a native loopback server. It must not use an installed Git executable
to implement server behavior. The test client can use the standard Git program.

Current Rust Git server crates are not dependencies. They combine HTTP,
authentication, repository discovery, protocol, and process policy. Their small
release history does not justify that trust or scope. Re-evaluate them only for
focused protocol code after the storage adapter works.

## Checkpoints and collection

A checkpoint keeps each pack that contains at least one object reachable from
the current refs. It can omit a pack with no reachable object. The selected pack
set must cover every reachable object before publication.

Conservative pack selection leaves each pack unchanged. Collection can remove a
feature-only pack after all refs to that feature are deleted. It cannot remove
unreachable bytes from a pack that also contains live objects.

## Required evidence

### Phase 1 storage proof

- Empty creation and cold recovery need no retained local files.
- Parsed ref commands and pack bytes publish one atomic update.
- Recovery creates a standard repository with the same refs and reachable IDs.
- Invalid packs, missing objects, invalid refs, and non-fast-forward updates fail
  before object-log publication.
- Two pushes from one view produce one winner. The loser publishes no pack ref.
- A lost response resolves the transaction without another pack upload.
- A checkpoint plus tail matches full replay.
- The same lifecycle passes for SHA-1 and SHA-256 repositories.

### Qualification and HTTP

- Collection preserves current packs and removes an unreachable-only pack after
  checkpoint publication.
- One opt-in MinIO flow uses a disposable bucket and leaves no process or
  container behind.
- Benchmarks report cold materialization, cold clone, warm fetch, small and large
  pack push, checkpoint, recovery, object-store requests, bytes, and disk use.
- An unmodified client can clone, fetch, create a branch and tag, push a
  fast-forward update, and delete refs through loopback smart HTTP.
- HTTP acceptance passes for SHA-1 and SHA-256 repositories.

## Fixed tranche

1. Define strict ref, object-ID, pack, transaction, and checkpoint records.
2. Add the pure-Rust `gix` and `gix-pack` feature set.
3. Implement pack validation and self-contained pack normalization.
4. Implement pack chunk staging and atomic ref publication.
5. Implement pending resolution without another pack upload.
6. Materialize standard bare repositories and rebuild pack indexes.
7. Implement live-pack checkpoint selection.
8. Add state, corruption, race, and collection tests.
9. Add SHA-1 and SHA-256 coverage.
10. Add adapter benchmarks and request and byte accounting.
11. Add one opt-in pinned MinIO lifecycle.
12. Record local evidence and run independent correctness and deletion reviews.

Smart HTTP starts as a separate tranche after these tasks pass.

## Limits

Keep Git policy outside the generic log. Start with one repository for each log
and one push for each publication. Defer pack rewriting, global deduplication,
cross-repository transactions, provider-specific behavior, Spin integration,
live AWS work, and a WASI Git adapter.

Preferred-owner routing and Durable-Object behavior are later performance and
deployment work. They are not a core object-log goal now.
