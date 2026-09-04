# Git proof plan

## Outcome

`object-log-git` must prove that the generic WAL can support a complete Git
repository. The durable authority is object storage. Local files are optional
cache data.

The first runnable host can be native. The Git protocol, pack, object lookup,
and publication code must compile for `wasm32-wasip2`. A later Spin component
must only adapt HTTP and object-store input and output. It must not contain a
second Git implementation.

The core `object-log` crate remains independent from Git.

## Current reference implementation

The current native proof supports SHA-1 and SHA-256 pack storage, atomic ref
transactions, cold recovery, checkpoints, collection, and Git smart HTTP
protocol v0. It uses a disposable bare repository and high-level `gix` APIs.

This implementation is a temporary test reference. Its memory-mapped object
database and filesystem pack writer are not WASI-compatible. Its fetch path
also sends all reachable objects because it does not use the client's `have`
set.

The replacement must pass the current storage and client tests before the
native-only core is deleted.

## Required design

- One object log owns one Git repository.
- Standard immutable packs and indexes contain Git objects.
- Large pack and index data use bounded object-log chunks.
- One object-log publication applies one ordered ref transaction.
- The object-log head is the only mutable durable authority.
- A pinned view supplies refs and a pack catalog for one request.
- Object lookup reads standard indexes and only the required pack ranges.
- Push validates pack checksums, deltas, object IDs, connectivity, and ref
  rules before publication.
- Thin input packs become self-contained durable packs.
- Fetch returns a self-contained pack for
  `reachable(wants) - reachable(valid haves)`.
- Checkpoints retain packs that contain live objects. Collection removes dead
  pack and index chunks.

The pre-release storage format can change if a new shape removes code or makes
the runtime better. Do not add a compatibility reader for an earlier
development shape.

## Small host-neutral boundary

Keep the public API concrete and byte-oriented. Do not add a generic Git engine
trait. The core must accept bounded asynchronous input, write to bounded
asynchronous output, and return a publication result that distinguishes
success, rejection, pending evidence, and expiry.

Expose only values that an HTTP adapter or higher-level storage caller needs.
Keep packet parsing, pack normalization, graph traversal, and object-log state
inside `object-log-git`.

The core owns all protocol limits. HTTP adapters can add stricter transport
limits. A declared content length permits early rejection but never replaces
counting the received bytes.

## Git wire behavior

Upload-pack discovery and fetch use Git protocol version 2. The server supports
`ls-refs` and `fetch`. It does not fall back to a protocol v0 upload-pack
advertisement when the client requests version 2.

Push uses classic receive-pack. Git protocol version 2 does not define a new
push command. The receive-pack path keeps the current atomic publication and
per-ref result behavior.

The first protocol set excludes shallow clones, filters, ref-in-want, packfile
URIs, and sideband-all. Add a capability only with a test for its complete
behavior.

## Implementation tranches

### 1. WASI contract

- Separate the host-neutral core from the temporary native engine.
- Add only the service, protocol, and limit values required by later work.
- Reject unsupported service and protocol combinations.
- Compile `object-log-git` for `wasm32-wasip2` without native features.
- Keep Tokio filesystem, temporary files, memory maps, and high-level
  `gix::Repository` out of the WASI dependency graph.

### 2. Pack engine

- Use low-level Gitoxide crates for pack parsing, validation, delta resolution,
  normalization, and index generation.
- Support base objects, `OFS_DELTA`, in-pack `REF_DELTA`, and thin packs whose
  external bases exist in the pinned view.
- Bound input bytes, decoded object bytes, object count, work, and delta depth.
- Produce packs that pass `git index-pack --strict` and `git fsck --strict`.

The first implementation can hold one explicitly bounded incoming pack in
memory. Select its limit from measured native and WASI memory use. Do not infer
the limit from an object-store or Spin default.

### 3. Durable objects

- Store immutable packs and indexes through object-log reference trees.
- Load refs and the pack catalog without creating a local Git repository.
- Find blobs, trees, commits, and tags through standard indexes.
- Traverse commits, trees, and annotated tags with explicit bounds.
- Preserve object-log recovery, checkpoint, and collection behavior.

### 4. Protocol

- Implement protocol v2 discovery, `ls-refs`, and have-aware `fetch`.
- Implement classic receive-pack advertisement, command parsing, and status.
- Use `gix-packetline` where it removes code.
- Reject malformed or unsupported requests before object reads or publication.

### 5. Integration and deletion

- Connect protocol, pack, durable lookup, and publication through one engine.
- Keep Axum as a thin native routing and transport adapter.
- Add a thin `wasm32-wasip2` Spin example that calls the same engine.
- Compare the new engine with the native reference on the same fixtures.
- Delete the native repository materializer, protocol v0 upload-pack engine,
  and native-only core dependencies after acceptance passes.

## Acceptance

An unchanged Git client must:

- discover an empty and a populated repository with protocol v2;
- clone, fetch, and list refs;
- push a new branch, annotated tag, fast-forward update, and deletions;
- receive a clear rejection for stale and non-fast-forward updates; and
- pass `git fsck --strict` after cold recovery.

Packet traces must show `version 2`, `command=ls-refs`, and `command=fetch`.
An incremental fetch must subtract valid `have` objects and be materially
smaller than the current full-reachable response on the fixed fixture.

Two pushes from one view must have one durable winner. A lost response must be
recoverable after the host and all local cache data are removed. Rejected and
losing packs must become collectable.

The same checkpoint, collection, and cold-clone lifecycle must pass with memory
storage and local MinIO. Live AWS qualification remains separate.

Required build gates include:

```sh
cargo +1.97.1 check -p object-log-git --lib \
  --target wasm32-wasip2 --no-default-features
cargo +1.97.1 build -p object-log-git-spin \
  --target wasm32-wasip2 --release
```

## Performance and size control

Compare the native reference and replacement with the same revision, Git
client, machine, and fixtures. Measure p50 and p95 time, wire bytes, peak
memory, object-store requests, and transferred bytes for:

- a 4 KiB one-commit push and clone;
- an 8 MiB deterministic pack;
- a 384-commit full clone;
- one incremental fetch after those 384 commits; and
- a thin incremental push.

Use one warm-up and ten measured samples. Record raw results and limitations.
Cold recovery must not download complete pack bodies.

Target 1,000 to 1,540 new product lines and delete at least 1,100 native-only
product lines. Stop for a simplicity review if new product code exceeds 1,600
lines or one tranche exceeds its line budget by more than 25 percent. Run a
Rust review, an adversarial correctness review, a prose review, and a deletion
review at stable checkpoints.

GitHub issue [#17](https://github.com/carsonfarmer/object-log/issues/17) tracks
this work. Issue [#14](https://github.com/carsonfarmer/object-log/issues/14)
tracks only native-host hardening that remains useful after the shared engine
exists.
