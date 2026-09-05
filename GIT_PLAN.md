# Git proof plan

## Outcome

The standalone object-log library is the product. A complete, usable Git
implementation proves both its correctness and how simply an application can
use it. Keep the full Git behavior below. When integration grows complicated,
identify whether the cause is inherent Git behavior, a missing generic log
capability, or unnecessary adapter machinery. Improve the appropriate layer;
do not move Git rules into the generic log or cut behavior to reduce size.

`object-log-git` must prove that the generic WAL can support Git smart HTTP
discovery, clone, incremental fetch, atomic push, and cold recovery. Object
storage is the durable authority. Local files and memory are cache data.

Spin is the Git HTTP host. The protocol, pack, object lookup, and publication
path compile for `wasm32-wasip2`; the component adapts HTTP and object storage
without a second Git engine. The earlier native host is removed.

The core `object-log` crate remains independent from Git.

## Current state

The full [review and execution queue](docs/reviews/git-proof-2026-09-05.md)
records ordinary-workflow gaps and core API lessons. Issue #26 sets minimum
capacity at 50 MiB files and 1 GiB pushes. Streaming receive and fetch now meet
that minimum in both-hash Spin/MinIO lifecycles. Aggregate clone also combines
three separate 720 MiB pushes into a pack over 2,080 MiB. The real object-log
history fixture covers incremental updates and compaction/checkpoint/GC with
cold recovery. Ordinary receive, fetch and maintenance use an edge-free closure
walker, tested through a connected 32,770-object history and full maintenance for
both hashes. Stored packs retain their 32,768-object bound; shallow, filtered
and URI fetches retain the earlier graph path. Memory and operation budgets
still define a finite scale envelope.
Use ordinary Spin defaults for runtime
behavior; no host memory cap, pooling override, or one-instance wrapper is required.

Tasks 1–9 were accepted at `b4b05f3`. Tasks 10–12 now pass unchanged-client,
local-provider, functional, and resource qualification for the shared native
and Spin adapters. Independent reviews are recorded. The owner subsequently
authorized removal of the previous native implementation. That implementation
and the entire native HTTP crate are removed. Useful client coverage runs
through actual Spin; publication/recovery faults run in the portable library.
Installed Git remains the benchmark reference. At `e9e5bdc`, all 14 paired
benchmark cases pass object, pack-size, call and transfer checks. Streaming
SHA-1 8 MiB push remains above the timing-review threshold after 30 pairs:
1.361× Git at p50 and 1.270× at p95. This compares guest/InMemory command work
with native Git subprocess/filesystem work, not HTTP or S3 latency. Issue #23
tracks phase-level investigation; the other 13 cases stayed below the threshold.
The historical foundation counts below describe their stated revision, not current size.

Revision `b322985c616d948e7365739e3286baeaeb460acc` completes tasks 1 and 2 with
four private, host-neutral foundations:

| Module | Product lines | State |
| --- | ---: | --- |
| Pack normalization | 618 | Accepted |
| Durable staging, sparse reads, and fetch writing | 750 | Accepted |
| Git wire protocol | 616 | Accepted |
| Operation budgets | 205 | Accepted |
| Total | 2,189 | Accepted |

The counts are raw Rust lines before each module's `#[cfg(test)]` section. Task
2 added 141 product lines and 287 test lines. The
[architecture gate](docs/evidence/git-architecture-gate-2026-09-04.md) records
task-1 acceptance and the no-go decision at that revision. The
[pack](docs/evidence/git-wasi-pack-2026-09-04.md),
[durable reader](docs/evidence/git-wasi-durable-reader-2026-09-04.md), and
[wire](docs/evidence/git-wasi-wire-2026-09-04.md) records contain the foundation
behavior and local checks. The
[fetch-pack record](docs/evidence/git-fetch-pack-2026-09-04.md) covers the
task-2 writer. The modules support SHA-1 and SHA-256. The native crate and the
no-default-feature WASIp2 target pass standalone checks. The historical manifest included the native reference runtime; that dependency
has since been removed.

Tasks 3–9 connect durable state, command-local catalogs, graph traversal, exact
selection, upload commands, and receive preparation/publication through the
common `Repository`.

The earlier native proof used disposable bare repositories and high-level
`gix` APIs. Its fetch ignored client haves. The [historical baseline](docs/evidence/git-wasi-baseline-2026-09-04.md)
remains evidence of that implementation; its product code is now removed.

The earlier native HTTP host and its detached-task/shutdown machinery are
removed. Spin awaits publication inline; it does not promise the deleted host's
finish-after-disconnect behavior. An interrupted standard Git push has the usual
lost-reply ambiguity. Custom protocols can use exact-candidate recovery tokens.

The first bounded functional proof is complete, but the broader project is not.
The owner requires compaction and a useful scale demonstration (#19), operational
memory/admission qualification (#21), pooled transport investigation (#22),
performance review (#23), partial/filtered clones and packfile URIs (#24),
and simplification (#25). See `docs/follow-ons.md` for links and acceptance scope.

## Storage and consistency contract

- One object log owns one Git repository.
- Standard immutable packs and indexes contain Git objects.
- Large pack and index data use bounded object-log chunks.
- One object-log publication applies one ordered ref transaction.
- The object-log head is the only mutable durable authority.
- One operation reads one exact observed view. A `View` is not a retention
  lease. It can restart once if collection expires that view. It must finish
  validation before it writes response bytes.
- Object lookup reads standard indexes and only the required pack ranges.
- A push validates pack checksums, deltas, object IDs, connectivity, and ref
  rules before publication.
- A thin input becomes a self-contained durable pack.
- A fetch produces a self-contained output pack.
- Checkpoints retain packs that contain live objects. Collection removes dead
  pack and index chunks.

The durable layout is pre-release. It can change when another layout reduces
code or measured runtime cost.

## API contract

Task 3 replaces the native-only public `Repository` signatures with one common
public type for native and WASIp2. Its shared entry points are
`Repository::open(&Log, ObjectFormat)` and `refs(&self)`. The shared API has no
work directory or path output. `Repository` owns the exact `View`, refs,
authenticated pack roots and sizes, `Operation`, and retained-state
reservation. Standard indexes are command-local. Each object-reading command
creates one private `Catalog` and `Reader`. This lets
`ls-refs` avoid pack-index loads and avoids a self-reference. This is a
pre-release API correction.

Do not add public `Engine`, `Service`, `Outcome`, `Catalog`, or `Reader` types.
Packet parsing, graph traversal, pack creation, budgets, and object-log state
remain private. `prepare_receive` returns a `PreparedPush`; its
`publish_receive` returns resolution and bounded wire bytes. Retain its opaque
recovery token before publishing. `View::collection_plan_bytes` supplies generic
authenticated size metadata for publication accounting.

The Spin adapter calls the common `Repository` path. It can enforce smaller
transport limits, but cannot weaken the engine limits.

## Git protocol policy

Upload-pack discovery and fetch use protocol v2 with `ls-refs` and `fetch`.
Push uses classic receive-pack because Git protocol v2 does not define a new
push command.

The first engine accepts wants only when they are reachable from the refs in
the exact observed view. This project policy is stricter than protocol v2,
which permits an arbitrary object ID in `want`. The engine rejects an
unreachable want. For an accepted request, it returns:

```text
reachable(wants) - reachable(valid haves)
```

The engine validates haves against the same exact observed view. It acknowledges
common haves during negotiation. It sends a pack only after `done`. For
`include-tag`, it fully peels annotated tags and includes the complete applicable
tag chain, not only a direct target.

The original protocol set excluded shallow fetches and filters. Shallow clone,
absolute/relative deepening, unshallow, time cutoffs, and ref exclusions now have
both-hash unchanged-client coverage through Spin/MinIO. Filters and packfile URIs
remain required work in #24. `ref-in-want`, `sideband-all`, and progress are not
advertised. Add a capability only with a test for its complete behavior.

## Memory and operation budgets

The native process admits one active Git engine operation through an 88 MiB
shared live allocation pool. In WASI that static pool belongs to one component
instance. Spin controls host concurrency with its default configuration.

The shared engine retains its 88 MiB live pool and 24 MiB retained-state
allowance. These are library operation limits, not a Spin host-memory limit.
Do not force pooling, connection reuse, instance count, or a 128 MiB host cap.

Each operation uses one cumulative budget. A retry does not reset its counters.

| Operation resource | Limit |
| --- | ---: |
| Logical object-log I/O calls (serving) | 512 |
| Uploaded plus downloaded bytes | 96 MiB |
| Decode, graph, and pack work | 256 MiB |
| Thin-base resolution rounds | 32 |
| Restart after expired evidence | 1 |

Conservative metadata checkpointing uses 8,192 calls with the same live-memory,
retained-state, transfer, and work limits. It does not traverse or compact packs.

Counters currently precharge logical core operations. Checkpoint collision
attempts are included, but blob/node staging can still issue uncharged core
identity-collision retries (#36). Backend retries are a separate source of
physical calls. Record actual requests in provider evidence; do not claim these
logical counters prove a universal physical-request cap.

The engine must also record total calls, transferred bytes, and serial request
depth. There is no smaller request-depth performance threshold yet.

Task 1 applies these phase limits:

| Phase resource | Limit |
| --- | ---: |
| Catalog, view, and graph state | 24 MiB |
| Verified pack-chunk cache | 8 MiB |
| Receive control | 1 MiB |
| Incoming receive pack | 9 MiB |
| Raw fetch pack | 9,437,184 bytes |
| Framed fetch response | 9,437,926 bytes |
| Normalized durable pack | 16 MiB |
| One decoded object | 8 MiB |
| Standard pack index | 2 MiB |
| Objects in one pack or graph walk | 32,768 |
| Delta depth | 256 |

Receive control and the incoming pack have separate limits. Both are charged
to the same live pool.

Measure runtime resource use with the ordinary Spin configuration. Investigate
library costs before changing host settings or proposing Spin patches.

## Pack creation policy

Compressed-entry reuse is a P0 fetch requirement. The pack builder must reuse
validated compressed entries when possible. If a selected delta and its
base are both in the output, it must encode the relation as `REF_DELTA` against
the selected base ID and reuse the compressed delta stream.

If reuse cannot prove a valid selected base, the builder must materialize the
object, verify its ID, and write a full object. The output pack must contain
every base that it needs. The client must not need an external object to read
the response.

## Twelve sequential implementation tasks

1. Complete at `8d28839`: centralize the process pool, operation counters, and
   reduced limits. Replace
   `object_log::materialize` with `materialize(log, owned_view, materializer)`
   and add `CommitRef::len()`. This pre-release change lets the engine reserve
   exact checkpoint and tail bytes before concurrent reads and keeps one
   materialization path. Add exact-limit and limit-plus-one tests.
2. Complete at `b322985`: add the private bounded fetch-pack writer defined by
   the [source gate](docs/evidence/git-fetch-pack-source-gate-2026-09-04.md).
   Reuse validated compressed entries, materialize unsafe delta fallbacks, and
   produce self-contained SHA-1 and SHA-256 packs. The
   [implementation record](docs/evidence/git-fetch-pack-2026-09-04.md) contains
   the checks and line counts.
3. Complete: replace the native-only signatures with one common public `Repository` for
   native and WASIp2. Open it with `Repository::open(&Log, ObjectFormat)` and
   expose refs through `refs(&self)`. Retain the exact `View`, refs,
   authenticated pack roots and sizes, `Operation`, and retained-state
   reservation. Return no work directory or path. The accepted bridge retains
   a catalog; move it into object-reading commands in Task 4.
4. Complete: make catalogs command-local and add iterative commit, tree, and annotated-tag traversal. Enforce graph,
   object, work, call, transfer, and memory budgets in one place.
5. Complete: validate reachable wants and usable haves against one view. Compute the
   exact selected object set without reading unrelated blobs.
6. Complete: build the fetch pack from reused compressed entries and materialized
   fallbacks. Validate it with Git and enforce the raw and framed byte limits.
7. Complete: connect protocol-v2 discovery, `ls-refs`, negotiation, and fetch to
   `Repository`. Buffer the bounded response and allow one expired-view retry.
8. Complete: resolve receive-pack thin bases in at most 32 rounds. Normalize the pack,
   check connectivity and ref rules, and keep all counters cumulative.
9. Complete: prepare and publish the ordered receive ref transaction. Preserve current
   conflict, pending-result, lost-response, and per-ref status behavior.
10. Complete: change the native Axum host into a thin adapter and run unchanged-client
    parity against the earlier reference. Both native implementations have now
    been removed; useful tests migrated to the shared library and actual Spin.
11. Complete with documented runtime conditions: add the thin Spin WASIp2 adapter. Record imports and peak process memory,
    then run all memory-store acceptance and performance cases.
12. Client/provider qualification complete: run the same accepted cases against
    local MinIO. The owner authorized deletion of the previous native engine after
    qualification; useful tests and installed-Git comparisons remain. Rust,
    adversarial, prose, and deletion reviews remain part of acceptance.

Tasks are sequential because each task supplies the contract or evidence for
the next task. Local memory cases must pass before MinIO. The generic
`object_store::LocalFileSystem` backend lacks conditional compare-and-swap, so
the log rejects it. Add a filesystem acceptance case only if a filesystem
adapter supplies that operation.

## Functional acceptance

An unchanged Git client must:

- discover empty and populated SHA-1 and SHA-256 repositories with protocol v2;
- clone, fetch, and list refs;
- perform an incremental fetch that excludes every object reachable from an
  accepted have;
- push a branch, annotated tag, fast-forward update, and deletion;
- receive clear stale and non-fast-forward rejections; and
- pass `git fsck --strict` after cold recovery.

Packet traces must show `version 2`, `command=ls-refs`, and `command=fetch`.
Generated fetch packs must pass `git index-pack --strict`. Two pushes from one
view must have one durable winner. A lost response must be recoverable after
the host and all cache data are removed. Rejected and losing packs must become
collectable.

The same checkpoint, collection, and cold-clone lifecycle must pass with memory
storage and then local MinIO. A filesystem backend can join this sequence only
if it supplies conditional compare-and-swap. Live AWS qualification remains
separate.

The build gates include:

```sh
cargo +1.97.1 check -p object-log-git --lib \
  --target wasm32-wasip2 --no-default-features
cargo +1.97.1 build -p object-log-git-spin \
  --target wasm32-wasip2 --release
```

## Performance acceptance

Use the same revision, pinned Git 2.54 client, machine, and deterministic fixture
for the Git oracle and replacement. Run one warm-up and ten paired samples.
Record p50 and p95 time, raw and framed bytes, logical store calls, transferred
bytes, and observed logical serial request depth.

Measure:

- a 4 KiB one-commit push and clone;
- an 8 MiB deterministic pack;
- a 384-commit full clone;
- one incremental fetch after those 384 commits; and
- a thin incremental push.

The hard local gates are:

- The fetch pack contains exactly the expected object-ID set.
- Every fetch pack normalizes without external delta bases. Full clone packs
  pass `git index-pack --strict --check-self-contained-and-connected` in an
  empty receiver. Incremental packs pass `git index-pack --strict` in a receiver
  seeded only with accepted-have history, followed by `git fsck --strict` on
  the fetched target. The connectivity flag returns 1 when graph dependencies
  come from that history; this is expected for an incremental pack. See the
  [selection evidence](docs/evidence/git-selection-2026-09-04.md).
- For the 8 MiB full fetch and 384-commit incremental fetch, candidate pack
  bytes are at most 1.10 times a same-run `git pack-objects --stdout --revs`
  oracle.
- Every raw fetch pack is at most 9,437,184 bytes.
- Every framed fetch response is at most 9,437,926 bytes.
- The measured 8 MiB fixture succeeds.
- Every operation stays within 512 logical store calls and 96 MiB of combined
  transfer.

Ten samples do not define a hard latency gate. With ten samples, p95 is the
maximum observation. If candidate p50 or p95 exceeds 1.25 times an equivalent
Git baseline, run 30 paired samples and record the performance finding for
owner review. The owner subsequently authorized old-engine removal independently
of that finding; installed-Git comparisons remain. Record noisy results as inconclusive.

Measure fresh-process resource use with ordinary `spin up --from` and record
observations with verification results. No 128 MiB process or instance cap is
an acceptance requirement. Builds and runtime behavior remain separate concerns.

No tighter call-count or serial-depth performance gate exists yet. Measurement
code must add zero product lines; keep it in test and support code. A failed
hard gate blocks accepting a behavior-changing replacement.

## Source-size review thresholds

Count product and test lines separately from revision `2ee2174`.

| Tranche | Expected change | Review threshold (historical gates for completed tasks) |
| --- | ---: | ---: |
| Task 1: pack, durable, wire, and private budgets | Complete at 2,048 retained product lines | Retained product exceeds 2,050 |
| Task 2 compressed-entry reuse | Complete at +141 product lines and +287 test lines | Any new dependency, named crate feature, public API, format-name change, or line-limit excess |
| Repository, graph, hybrid fetch, and receive | Add 900–1,225 | Added product exceeds 1,275 |
| Native HTTP and Spin adapters | Add 180–280 | Added product exceeds 300 |
| Tests | Add 1,600–2,400 | Report separately |

At revision `b322985`, the private replacement foundation has 2,189 raw product
lines. The combined Git and HTTP implementation has 5,101 raw product lines under
consistent top-level test-module counting; the earlier 4,970 count accidentally
truncated production code at an inline test-only helper.
Task 2 stayed within its separate source-size gate. The historical native-deletion estimate
was 2,165 product lines, with an expected remaining total of 3,480–4,125 and
a review threshold of 4,150. Actual removal leaves 4,885 adjusted production
lines (Git 4,293, Spin 592); the architecture review signal
remains exceeded. See the [removal record](docs/evidence/git-native-removal-2026-09-04.md).

The [final architecture review](docs/evidence/git-final-architecture-2026-09-04.md)
records 7,490 adjusted product Rust lines at `0ed9b52`, before the final small
cleanup and qualification fixes. Required Git rules and bounded WASI transport
explain most growth; it does not justify moving domain rules into object-log.

Treat source-size thresholds, including the historical stop gates above, as
architecture review signals. Report material overage and explain its cause
before integration. Simplify unnecessary machinery, but never remove required
behavior to satisfy a line count. Behavioral, resource, and provider gates
remain acceptance requirements.

## Standards and prior art

- Git's [protocol-v2](https://git-scm.com/docs/protocol-v2) defines `ls-refs`,
  want/have negotiation, and fetch response sections. The reachable-only want
  rule above is a stricter project policy.
- Git's [pack format](https://git-scm.com/docs/gitformat-pack) defines
  `REF_DELTA`, `OFS_DELTA`, checksums, and self-contained stored packs.
- Git's [pack-objects](https://git-scm.com/docs/git-pack-objects) documents
  delta reuse and the standard pack output used as the performance oracle.
- [`gix-pack` 0.74.2](https://docs.rs/gix-pack/0.74.2/gix_pack/) supplies the
  low-level pack and index types used by the private foundation.
- Cursor's [Git at any scale](https://cursor.com/blog/git-at-any-scale) describes
  an object-storage WAL, standard Git repositories on local NVMe, conditional
  reads, CAS publication, and reuse of published compacted packs. Cursor and
  Micelio use native Git and local repositories.
- Walgit's [remote reader](https://github.com/tobi/walgit/blob/main/crates/walgit-wal/src/remote.rs)
  is prior art for indexed range reads and a process-wide block cache over
  object storage.
- Cloudflare documents a [128 MB per-isolate memory limit](https://developers.cloudflare.com/workers/platform/limits/),
  including WebAssembly allocations. That platform-specific limit does not
  configure or constrain this Spin proof.
- The `git-server` package in `imjasonh/playground` targets Cloudflare Workers.
  It is not a Cloudflare-owned project. Its root license is unclear, so do not
  copy its source or structure.

GitHub issue [#17](https://github.com/carsonfarmer/object-log/issues/17) tracks
this work. Issue [#14](https://github.com/carsonfarmer/object-log/issues/14)
records the superseded native-host work. Spin runtime and protocol follow-ons
are tracked in #21–#25 and compaction in #19.
