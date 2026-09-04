# Git proof plan

## Outcome

`object-log-git` must prove that the generic WAL can support Git smart HTTP
discovery, clone, incremental fetch, atomic push, and cold recovery. Object
storage is the durable authority. Local files and memory are cache data.

The first integrated host can remain native. The protocol, pack, object lookup,
and publication path must compile for `wasm32-wasip2`. A later Spin component
must adapt HTTP and object storage without a second Git engine.

The core `object-log` crate remains independent from Git.

## Current state

Revision `2ee21742d7acf64e7ceb6ddb79c6407c0462bcf5` has three private,
host-neutral foundations:

| Module | Product lines | State |
| --- | ---: | --- |
| Pack normalization | 500 | Complete locally |
| Durable staging and sparse reads | 578 | Complete locally |
| Git wire protocol | 606 | Complete locally |
| Total | 1,684 | Complete locally |

The counts are raw Rust lines before each module's `#[cfg(test)]` section.
The [pack](docs/evidence/git-wasi-pack-2026-09-04.md),
[durable reader](docs/evidence/git-wasi-durable-reader-2026-09-04.md), and
[wire](docs/evidence/git-wasi-wire-2026-09-04.md) records contain the foundation
behavior and local checks. The modules support SHA-1 and SHA-256 and pass
native and WASIp2 checks without default features.

The native proof remains the client and storage oracle. It uses a disposable
bare repository and high-level `gix` APIs. It supports atomic ref transactions,
cold recovery, checkpoints, collection, and protocol-v0 smart HTTP. Its fetch
path sends all reachable objects because it ignores the client's `have` set.
The [baseline](docs/evidence/git-wasi-baseline-2026-09-04.md) records its size,
protocol trace, storage requests, and local latency.

Do not delete the native oracle until the replacement passes the same client
and storage cases.

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

Keep the current public `Repository` and `PreparedPush` integration surface.
Reuse the current public value types. Do not add public `Engine`, `Service`, or
`Outcome` types. Packet parsing, graph traversal, pack creation, budgets, and
object-log state remain private.

The native HTTP and Spin adapters must call the same `Repository` path. The
adapters can enforce smaller transport limits, but they cannot weaken the
engine limits.

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

The first protocol set excludes shallow fetches, filters, `ref-in-want`,
packfile URIs, `sideband-all`, and progress. Add a capability only with a test
for its complete behavior.

## Memory and operation budgets

The process admits one active Git engine operation. All repositories share one
88 MiB live allocation pool. All requests in the process share this limit.

| Process memory | Budget |
| --- | ---: |
| Git live pool | 88 MiB |
| Runtime allowance | 24 MiB |
| Safety reserve | 16 MiB |
| WASI host model | 128 MiB |
| Provisional observed peak target | 120 MiB |

Each operation uses one cumulative budget. A retry does not reset its counters.

| Operation resource | Limit |
| --- | ---: |
| Logical object-log I/O calls | 512 |
| Uploaded plus downloaded bytes | 96 MiB |
| Decode, graph, and pack work | 256 MiB |
| Thin-base resolution rounds | 32 |
| Restart after expired evidence | 1 |

These counters conservatively charge object-log I/O issued by the engine.
Backend retries can add physical calls. Record physical retries in MinIO
evidence instead of adding product instrumentation.

The engine must also record total calls, transferred bytes, and serial request
depth. There is no smaller request-depth performance threshold yet.

The first implementation task must replace the larger foundation defaults with
these phase limits:

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

These limits are provisional until the WASIp2 adapter measures peak process
memory. If the runtime needs more than 24 MiB, reduce the live pool or a phase
limit. Do not assign the 16 MiB reserve to a phase.

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

1. Centralize the process pool, operation counters, and reduced limits. Replace
   `object_log::materialize` with `materialize(log, owned_view, materializer)`
   and add `CommitRef::len()`. This pre-release change lets the engine reserve
   exact checkpoint and tail bytes before concurrent reads and keeps one
   materialization path. Add exact-limit and limit-plus-one tests.
2. Add compressed-entry inspection and reuse. Cover selected-base `REF_DELTA`,
   full-object fallback, checksums, order, and self-contained output.
3. Load one repository view through the existing `Repository` surface. Retain
   refs, authenticated pack proofs, standard indexes, and one sparse reader.
4. Add iterative commit, tree, and annotated-tag traversal. Enforce graph,
   object, work, call, transfer, and memory budgets in one place.
5. Validate reachable wants and usable haves against one view. Compute the
   exact selected object set without reading unrelated blobs.
6. Build the fetch pack from reused compressed entries and materialized
   fallbacks. Validate it with Git and enforce the raw and framed byte limits.
7. Connect protocol-v2 discovery, `ls-refs`, negotiation, and fetch to
   `Repository`. Buffer the bounded response and allow one expired-view retry.
8. Resolve receive-pack thin bases in at most 32 rounds. Normalize the pack,
   check connectivity and ref rules, and keep all counters cumulative.
9. Prepare and publish the ordered receive ref transaction. Preserve current
   conflict, pending-result, lost-response, and per-ref status behavior.
10. Change the native Axum host into a thin adapter and run unchanged-client
    parity against the native oracle. Keep the oracle available for comparison.
11. Add the thin Spin WASIp2 adapter. Record imports and peak process memory,
    then run all memory-store and filesystem-store acceptance and performance
    cases.
12. Run the same accepted cases against local MinIO. Delete the native oracle
    only after client and storage parity, all hard gates, and required owner
    reviews pass. Finish with Rust, adversarial, prose, and deletion reviews.

Tasks are sequential because each task supplies the contract or evidence for
the next task. Local memory and filesystem cases must pass before MinIO.

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
storage, filesystem storage, and then local MinIO. Live AWS qualification
remains separate.

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
- `git index-pack --strict --check-self-contained-and-connected` accepts the
  fetch pack.
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
Git oracle, run 30 paired samples and require owner performance review before
native deletion. Record noisy results as inconclusive.

After the Spin component exists, measure a fresh process with
`--max-instance-memory 134217728` and `/usr/bin/time -l`. A peak at or below
120 MiB is a provisional target, not a hard gate. A later Linux test must
enforce a 128 MiB cgroup and prove that the host survives the workload.

No tighter call-count or serial-depth performance gate exists yet. Measurement
code must add zero product lines; keep it in test and support code. A failed
hard gate blocks native-oracle deletion.

## Source-size gates

Count product and test lines separately from revision `2ee2174`.

| Tranche | Expected change | Stop gate |
| --- | ---: | ---: |
| Pack, durable, wire, and private budgets | Add 260–400; delete 80–160 | Retained product exceeds 2,050 |
| Repository, graph, hybrid fetch, and receive | Add 900–1,225 | Added product exceeds 1,275 |
| Native HTTP and Spin adapters | Add 180–280 | Added product exceeds 300 |
| Tests | Add 1,600–2,400 | Report separately |

The current combined Git and HTTP implementation has 4,465 product lines. The
native deletion target is 2,165 product lines. After that deletion, the
expected Git, HTTP, and Spin total is 3,480–4,125 product lines. Stop if it
exceeds 4,150.

Do not continue past a missed intermediate gate because a later deletion might
offset it. Reduce the current tranche before integration.

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
- Cursor's [Git at any scale](https://cursor.com/blog/git-at-any-scale) supports
  the object-storage WAL, immutable pack, atomic publication, and disposable
  cache model.
- Walgit's [remote reader](https://github.com/tobi/walgit/blob/main/crates/walgit-wal/src/remote.rs)
  is prior art for indexed range reads and a process-wide block cache over
  object storage.
- Cloudflare documents a [128 MB per-isolate memory limit](https://developers.cloudflare.com/workers/platform/limits/),
  including WebAssembly allocations. This plan uses a 128 MiB process model
  and a lower 120 MiB provisional target.

GitHub issue [#17](https://github.com/carsonfarmer/object-log/issues/17) tracks
this work. Issue [#14](https://github.com/carsonfarmer/object-log/issues/14)
tracks native-host hardening that remains useful after the shared path exists.
