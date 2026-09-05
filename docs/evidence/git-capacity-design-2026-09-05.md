# Git capacity design and first range-processing prerequisite

Issue: https://github.com/carsonfarmer/object-log/issues/26

Source baseline: `03684540024ba9248e47bcef0aa5ea46a8fd9ceb`.
Exclusive worktree branch: `cf/capacity-range-fetch`.

The required outcome remains 50 MiB decoded regular Git files and at least
1 GiB incoming packs, followed by usable clone/fetch, through actual Spin and
local MinIO with both hashes. This tranche does **not** establish that capacity.
It removes one allocation that scales with compressed entry size and leaves
all policy constants unchanged. The 128 MiB target applies to serving with
prepared executable code; Cargo builds and Wasmtime compilation/cache setup
are separate provisioning work.

## Current choke points

| Stage and source | Current retained data or limit | Required change |
| --- | --- | --- |
| Spin `body`/`decode_body` | Complete HTTP body `Vec`; gzip can retain compressed and decoded bodies | Incremental control parsing, bounded decompression and pack ingestion with backpressure; independent overhead limit |
| `Repository::prepare_receive`, `wire::parse_receive` | Complete `Bytes` request; borrowed pack slice; same request retained for retry | A replayable bounded pack source, with controls held separately |
| `pack::scan` | Whole input, copied compressed bytes per entry, largest inflated object `Vec` | Scan a range source incrementally; keep offsets/CRC/size metadata, stream inflation and hashing |
| `resolve_external`, `receive::normalize` | Compressed entry vectors, whole external bases, compression buffers; repeated full normalization for thin-base discovery | Resolve the dependency graph once; stage missing bases and resolved results in bounded scratch ranges |
| `pack::index` | Complete normalized pack; index `Vec`; reservation includes twice total resolved bytes, largest object, three times largest delta instructions, and 1,536 bytes per object | Bounded delta traversal and index writing driven by verified metadata, without whole-pack gix traversal allocations |
| `durable::stage` | Complete normalized `Vec` before 1 MiB chunks are written; standard index embedded in root | Incremental chunk sink, bounded fanout tree, separately addressable index sections |
| `durable::load` | Every live pack root and standard index, merged OID directory, offsets arrays | Coordinate #19's private authenticated OID-to-location tree; no catalog/state changes in this tranche |
| `Reader::entry`, `find`, `apply_delta` | Compressed entry and decoded object; retained delta instructions and full base/result | Incremental verified leaf scanning; range-backed delta instructions, base copies, and output |
| `Graph` and `Repository::fetch_pack_or_ack` | Bounded graph, then `find` fully decodes every unverified selected leaf | Verify blob kind/size/OID without retaining decoded bytes; stream tree parsing or retain a separate structural-object cap |
| `Reader::fetch_pack` | Complete raw pack output; materialized fallback object when a delta base is unselected | This tranche streams reused compressed entry ranges into that private output; subsequent work must stage output incrementally and stream fallback objects |
| `wire::write_fetch`, `wire_response`, Spin response | Complete raw and framed pack buffers overlap | Incremental sideband framing over a fully validated, replayable response source |
| `budget`, Spin connector ledger | 512 calls, 96 MiB transfer, 256 MiB work; cache metadata capacity tied to call quota | Separate bounded resident cache metadata from cumulative calls; checked large transfer/work counters across retries |

Other capacity boundaries: 8 MiB decoded objects, 9 MiB incoming and raw fetch
packs, 16 MiB normalized packs, 2 MiB indexes, 32,768 pack/graph objects,
256 delta depth, and 32 thin-base rounds. Internal pack offsets/lengths are
`u32`, pack/chunk selectors are `u16`. The WAL defaults to 1,024 children per
node and 100,000 collection objects. A flat 1 MiB chunk list reaches its
1 GiB ceiling before any thin-base expansion. Merely widening one field or
raising one quota will expose the next bound.

## Dependency-ordered implementation

1. **Authenticated ranges and verified leaf scanning.** Land this tranche's
   range visitor and compressed-entry copy. Next feed authenticated slices to
   incremental zlib decoding with a fixed output window and incremental Git
   object hashing. Enforce exact stream end, declared size, checksum, and
   expansion work. Return verified kind/size for blob connectivity and fetch
   validation. Both hashes and malformed stream boundaries need tests. Keep
   full materialization only where graph parsing or the existing delta path
   still requires it; do not raise the object limit yet.
2. **Replayable bounded scratch and ingest.** Add a private byte-source/sink
   abstraction using immutable WAL chunks and bounded fanout nodes. Stage
   received bytes as they arrive; retain only controls and compact entry
   metadata. A failed upload must never publish refs. A collected/expired
   staging view must abort or restart with the same cumulative operation
   counters. Crash leftovers remain collectible. A decoded scratch result
   must support random ranges for Git delta copy instructions. Cap scratch
   growth and use backpressure before reading another transport chunk.
3. **Bounded normalization and delta resolution.** Scan entry headers/zlib
   streams from the replayable source; verify the incoming trailer while
   ingesting. Keep entry ranges, dependencies, CRCs and resolved OIDs rather
   than compressed `Vec`s. Resolve internal OFS/REF chains and external thin
   bases with explicit depth, work, cycle, duplicate-OID and scratch limits.
   Stream validated compressed entries into the normalized sink; insert
   external bases through streaming deflate. Replace repeated whole-pack
   normalization attempts with a bounded dependency schedule. No ref
   transaction becomes publishable before complete validation/connectivity.
4. **Index and durable layout.** Write standard indexes from verified
   `(OID, CRC, offset)` records. The first large-file workload may retain the
   existing 32,768-object envelope with explicitly reserved metadata; larger
   many-object workloads need bounded sorted runs and sparse index sections.
   Use checked `u64` logical offsets/lengths; convert only chunk-local sizes to
   `usize`. Support standard large-offset index tables when needed. Use bounded
   fanout for pack and index bytes, coordinated with #19's lookup tree and GC
   reachability. Incoming and normalized limits must be separate: a thin
   input can require much more durable output. Reject expansion over its own
   documented quota before publication.
5. **Validated fetch and transport.** Preserve exact want-minus-have selection
   and the self-contained pack rule. Finish the selected pack and trailer in
   bounded scratch before exposing response bytes; then frame sideband packets
   incrementally. The response owns admission until consumption/cancellation.
   A response source needs a lifetime decision: a `View` is not a lease and
   staged objects can be collected during a slow response. The concrete first
   option is a disposable file spool on provisioned non-tmpfs storage, read
   through the common bounded source interface. It is never recovery authority;
   losing it truncates that request, and a new request rebuilds it from WAL.
   Verify WASIp2/Spin support and quota/cleanup for that storage before adopting
   this deployment envelope. If a deployment cannot supply such scratch, an
   object-backed response needs an explicitly reviewed crash-recoverable
   retention policy through the existing head. Existing permanent namespace
   retention is not an automatic per-request solution: crashes can leak it.
   Neither path adds another durable authority. Storage/transport failure after
   response start can still truncate HTTP; never restart and append a new pack
   to an already-started response.
6. **Quotas, host wiring, and qualification.** Only after stages 1–5 have
   bounded allocations should engine/transport capacities rise together.
   A 1 GiB upload at 1 MiB chunks already needs 1,024 writes; one full reread
   needs another 1,024 calls and 1 GiB transferred, before normalization,
   metadata, retries, or output. Derive finite call/transfer/work budgets from
   measured pass counts and a declared supported workload, including one
   expired-view retry; retain separate expansion, scratch, object-count and
   structural-object limits. Use checked wide cumulative counters on WASIp2.
   Cache slots must depend on resident byte/entry budgets, not total allowed
   calls. Keep rejected work charged. Record payload versus host-buffer memory
   separately and preserve physical retry accounting.

A candidate resident envelope for design testing is 24 MiB metadata/graph,
8 MiB compressed cache, 4 MiB decoded scratch cache, 4 MiB combined active
ingest/output windows, 1 MiB codec state, and 4 MiB ancillary bookkeeping:
45 MiB within the existing 88 MiB live pool. This is an allocation plan, not a
measured RSS claim or permission to spend the safety reserve. Concurrent PUT
buffers, core node encoding/decoding, active collection plans, and host HTTP
buffers must fit their measured phase budgets. Start with serial/bounded
transfers rather than multiplying windows by unqualified concurrency.

## Reusable WAL improvements from actual friction

The WAL already supplies immutable blobs, authenticated child references,
tree nodes, exact views and staging/publication proofs. No new authority
protocol is needed. Keep Git pack/index formats and delta policy in Git.

- `durable::root_bytes` duplicates CBOR envelope sizes to reserve before PUT.
  Core-owned exact node-size preflight belongs to #31; this task does not edit it.
- A bounded authenticated byte-sequence cursor/sink could remove repeated
  chunk geometry, tree navigation and staging-proof management across Git and
  other adapters. First implement the private Git need on `put_object`,
  `put_node`, `read_object` and `read_node`; extract only the demonstrated
  byte-oriented common contract. Raw partial reads of a content-hashed blob
  cannot be called authenticated unless the complete blob or a proof is checked.
- Every chunk PUT currently checks/decodes an active collection plan. Large
  staging needs measured plan-read amplification and memory accounting. Any
  reusable bounded staging context must preserve the fence/proof semantics;
  it cannot skip the check because a cached view previously passed.
- A stream source alone does not solve retention. Review source lifetime and
  crash cleanup before adding a generic retained stream API.

## First local tranche and evidence

`durable.rs` now visits authenticated chunk slices without gathering a whole
range. Fetch parses at most 42 prefix bytes (u64 header plus SHA-256 base ID),
keeps canonical-header/object-size checks, computes CRC over the **original**
entry while copying only its compressed payload, and rewrites selected delta
headers as before. The raw output remains private and bounded; late CRC errors
discard it. Missing selected bases still take the verified materialization
fallback. Multi-chunk buffered reads share the visitor. Empty/out-of-bounds
range handling is explicit. No API, dependency, durable format, catalog, head,
retry policy, or quota changes. The raw pre-test product section of
`durable.rs` grows from 835 to 896 lines (+61); there are no public API or
dependency additions.

New focused tests cover:

- Both-hash reuse of a 1,200,002-byte blob fixture spanning two chunks, with
  enough pool room for output/selection but no contiguous compressed-entry
  copy. Warm fetch performs zero GETs; exact pack bytes and installed Git
  strict validation pass. The fixture uses compression level zero and is an
  allocation regression test, not the required incompressible capacity proof.
- Empty, aligned, partial-tail and crossing-boundary ranges, invalid ranges,
  and consumer failure stopping before a second chunk GET.
- Both-hash CRC mismatch after copying, with all output reservations released.

Existing tests retain both-hash REF/OFS reuse and materialized fallback,
authentication/corruption, exact output limits, work failure before GET,
cancellation/admission release, collection and sparse reads.

Independent read-only Rust correctness/simplification review approved the
tranche with no actionable findings. Optional future coverage is a SHA-256
REF_DELTA header crossing a chunk boundary. The review also independently
identified the index reservation, blob materialization, fanout and response
lifetime blockers recorded above.

`make check` passes: 325 tests passed, 12 opt-in tests ignored; formatting,
strict all-target/all-feature workspace Clippy, Git WASIp2 check, and strict
Spin WASIp2 Clippy all pass. Raw output:
[`git-capacity-check-2026-09-05.txt`](git-capacity-check-2026-09-05.txt).
The memory-store tests and installed-Git filesystem receivers run in this gate.
No MinIO test was enabled for this local prerequisite.

Environment: macOS arm64, Apple M4 Pro, Rust 1.97.1, installed Apple Git 2.54.0.
Independent review rechecked the final helper extraction and this design and
approved both with no actionable findings. These local checks do not establish actual Spin/MinIO latency,
128 MiB serving headroom, 50 MiB blob support, or 1 GiB push support. #26 remains
open. The next independent tranche is incremental verified blob scanning.


The opt-in release `shared_git_performance_acceptance` also passes: 14
both-hash cases, 10 measured pairs plus one warmup each, no 1.25x review flags.
The existing harness alternates order and checks exact objects, pack sizes and
operation accounting. It covers 4 KiB and 8 MiB push/fetch, 384-commit history,
385-commit incremental fetch, and thin push. Command:

```text
OBJECT_LOG_GIT_PERFORMANCE_OUTPUT=/tmp/object-log-capacity-shared-performance.jsonl cargo test --locked --release -p object-log-git --test shared_performance -- --ignored --exact shared_git_performance_acceptance --nocapture
```

Raw [JSONL](git-capacity-shared-performance-2026-09-05.jsonl) and
[command output](git-capacity-shared-performance-2026-09-05.txt) are retained.
The JSONL revision is the baseline HEAD because the tested patch was still
uncommitted; the run includes this document's source patch. This is native
macOS memory-store acceptance against installed Git subprocesses, with different
whole-command scopes, not actual WASIp2/MinIO performance. It does not resolve
#23's historical WASIp2 SHA-1 finding and has no before/after speedup claim.
Compilation took 18.58 seconds; the test took 125.89 seconds. No serving cgroup
was imposed on compilation or this native test.
