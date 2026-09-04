# Git storage adapter plan

## Outcome

`object-log` is the product. It is a small, generic, object-storage-backed WAL
for higher-level storage systems. The `object-log-git` crate is one proof of
its public API.

The native Git storage adapter accepts parsed ref commands and an optional pack
path. It validates both inputs, publishes one atomic object-log update, and
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
Repository::open(log, work_dir, object_format) -> Result<Repository, Error>
Repository::refs() -> &RefSnapshot
Repository::prepare_push(self, transaction_id, updates, Option<&Path>) -> Result<PreparedPush, Error>
PreparedPush::recovery_token() -> &Bytes
PreparedPush::publish(self) -> Result<CommitStatus, Error>
```

`open` loads the log and rebuilds a standard bare repository in an empty,
disposable directory. It installs durable packs, validates their reachable
object graph, and restores refs. Objects outside the recovered pack set cannot
support a ref update.

`prepare_push` validates each update against the current snapshot. It normalizes
an optional pack, validates the target graph against recovered and new pack
objects, stages pack chunks, and prepares one object-log commit. `PreparedPush`
is opaque. Its recovery token identifies the exact publication attempt.

`publish` performs the conditional publication. The core `Log::resume` method
resolves a lost response from the recovery token without another pack upload.

The first crate supports SHA-1 and SHA-256 repositories. It supports direct refs
under `refs/heads/` and `refs/tags/`, plus one configured symbolic `HEAD`. It
rejects other symbolic refs, shallow state, alternates, and invalid object
graphs. It disables replacement objects and does not run Git hooks.

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

The layout follows Cursor's immutable WAL and CAS publication model. Git
checkpoints and physical pack compaction are not implemented. Live pack count,
replay work, and recovery bytes can grow.

Durable Object behavior, tenancy, routing, and actor or service ownership are
out of scope. This proof does not add them to the WAL.

## Smart HTTP phase

Build smart HTTP only after phase 1 proves the storage API. Keep it in a separate
crate. The adapter parses packet lines, capabilities, ref commands, and pack
input. It passes parsed commands and pack bytes to `object-log-git`.

Authentication, routing, and HTTP server policy are out of scope. A push
response waits for object-log to confirm publication. A conflict or unresolved
result returns a protocol error.

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

## Current status

The native proof implements strict records, pure-Rust pack validation and
normalization, chunk storage, atomic ref publication, lost-response recovery,
bare-repository recovery, graph and pack provenance checks, conflict tests, and
SHA-1 and SHA-256 tests.

The remaining work is live-pack checkpoint selection, collection tests,
benchmarks and request accounting, one pinned MinIO lifecycle, and a local
evidence record. Smart HTTP starts after this storage proof is complete.

## Limits

Keep Git policy outside the generic log. Start with one repository for each log
and one push for each publication. Defer pack rewriting, global deduplication,
cross-repository transactions, provider-specific behavior, Spin integration,
live AWS work, and a WASI Git adapter.

Durable Object behavior, tenancy, routing, and actor or service ownership are
not goals for this project.
