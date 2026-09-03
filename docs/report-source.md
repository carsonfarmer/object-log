# Object-store log research report

Audience: object-log and Spin storage-factor implementers  
Date: 2026-09-02  
Scope: object-store WALs, key-value storage, SQLite, and `wasi:filesystem`

## Direct answer

The design is viable. A small log with immutable entries and one conditional
object-store update can be the durable authority for one tenant resource. A
local owner can materialize state, serialize operations, and batch them. A new
owner can rebuild from a checkpoint and the active tail. This gives Spin apps
the main storage properties of a durable object: per-resource ordering,
durable state, disposable compute, and safe failover.

It does not provide a complete durable-object platform. Request routing,
advisory ownership, alarms, execution limits, and transactions across resources
remain outside the log. A single object-store compare-and-swap also limits
strict commit throughput for one resource.

The implemented `object-log` protocol is a reasonable independent core. Do not
replace it with one surveyed project now. Use `wal3`, WalTier, Micelio, objwal,
and Graft as design and test sources for the next tranches.

## The common durable shape

The strongest projects converge on the same structure:

1. Upload immutable data before publication.
2. Publish ordered references with one conditional mutable object.
3. Treat local state as a disposable materialized cache.
4. Use batching to amortize the serialized publication operation.
5. Compact old history into an immutable checkpoint or snapshot.

Cursor describes this shape directly. It uploads each push as a separate WAL
object and publishes it by adding a pointer to a CAS-updated index. Any node can
write safely. A rendezvous-selected node is preferred only to reduce retries.
Cursor reports up to 120 pushes/s on S3 Standard and more than 300 pushes/s on
S3 Express One Zone for its Git workload. [Cursor, “Git at any
scale”](https://cursor.com/blog/git-at-any-scale).

Apache Arrow's Rust `object_store` crate is the correct provider seam. It
supports conditional updates through `PutMode::Update` and an opaque
`UpdateVersion`. Its documentation presents this operation as a way to build
optimistic transactions over object storage. [object_store conditional-put
documentation](https://docs.rs/object_store/latest/object_store/#conditional-put).
AWS documents the matching S3 `If-None-Match` and `If-Match` behavior.
[AWS S3 conditional writes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html).

## Project comparison

### WalTier

WalTier is the closest small embeddable Rust project. It supports a state
machine, snapshots, replicas, conditional reads, conflict reconciliation, and
batch writes. It is useful source material for compaction and a preferred
single-writer API. [WalTier repository](https://github.com/danthegoodman1/waltier).

The whole-live-log rewrite is confirmed in both documentation and code. Its
WAL image contains the current snapshot pointer and live entries. Every write
encodes that image and sends the complete bytes through `put_if_match`.
[WalTier write implementation](https://raw.githubusercontent.com/danthegoodman1/waltier/main/src/wal.rs).
This gives one-read bootstrap, but its write bytes grow with the active image.
WalTier also documents ambiguous writes as at-least-once and asks applications
to make entries idempotent or detect duplicates. That contract is weaker than
the exact pending-resolution contract needed for generic storage factors.

### Chroma wal3

Chroma's `wal3` is the closest high-throughput architecture. It batches records
into immutable Parquet fragments, publishes fragments through a CAS manifest,
uses immutable manifest snapshots as a tree, supports cursor-aware garbage
collection, and schedules critical writes so task cancellation cannot stop
progress. Its design explicitly prefers one in-process writer while retaining
correct multi-writer failure behavior. [wal3 design](https://github.com/chroma-core/chroma/blob/f60fe42cdad202a92acad55a1f0fbf8ce757c8b1/rust/wal3/README.md).

The detailed comparison changed four local decisions. Checkpoints now declare
content roots. Canonical reference nodes make large object graphs traversable.
Normal replay reads WAL metadata but leaves payload reads lazy. One validated
backend handle opens many tenant logs without repeating the capability probe.
These changes copy WAL3 invariants without copying its fixed metadata tree.

WAL3 also confirms that safe deletion needs a positive durable deletion plan.
For generic object graphs, the plan must be installed as a head-CAS fence.
Publication while that fence is active must reject any transitive reference to
its candidate set. A grace period alone permits an old verified object to be
deleted before a later CAS makes it live. [wal3 garbage-collection
protocol](https://github.com/chroma-core/chroma/blob/f60fe42cdad202a92acad55a1f0fbf8ce757c8b1/rust/wal3/README.md#garbage-collection).

It is not a clean reusable dependency. The crate is part of the Chroma
workspace and depends on Chroma types, configuration, storage, telemetry,
Parquet, Arrow, gRPC, and Google Cloud Spanner packages.
[wal3 Cargo manifest](https://raw.githubusercontent.com/chroma-core/chroma/main/rust/wal3/Cargo.toml).
Its fragment tree and garbage-collection protocol are important sources for
the fast follow-on work. Direct reuse remains a poor fit. At the reviewed
revision, the crate depends on Arrow, Parquet, Chroma packages, telemetry,
tonic, and Google Cloud Spanner.

### Micelio

Micelio closely implements the Cursor Git design. Its durable schema separates
immutable entries, content-addressed packs, a base, and one CAS index. It leaves
the sequence number out of the immutable entry and assigns order in the index.
The index repeats selected entry metadata to reduce catch-up reads.
[Micelio WAL schema](https://github.com/tuist/micelio/blob/main/priv/proto/micelio/wal/v1/wal.proto).

The project is Git-specific and uses Protobuf. The useful parts are the object
layout, sequence assignment, base-plus-tail model, and explicit durable-schema
discipline. `object-log` adopts those concepts with canonical CBOR maps and a
CDDL schema. It does not need Protobuf.

### objwal and walgit

The Go objwal project shows a practical group-commit path. It buffers records,
uploads immutable segments in parallel, publishes ordered segment references
through one manifest CAS, and resolves durability waiters after that CAS. It
uses one epoch-fenced primary and read-only replicas, so it is not the arbitrary
writer contract used by `object-log`. [objwal
design](https://github.com/JayJamieson/objwal).

walgit contains the same useful Git-level ideas: immutable packs, a CAS
manifest, checkpoint plus tail recovery, conditional reads, and group commit.
It also includes leases, policy, bundles, remote pack readers, a web product,
and several mutable objects. It is too broad and Git-specific to use as the
generic storage core. [walgit architecture](https://github.com/tobi/walgit).

### SlateDB and its transactional object

SlateDB is a mature direction for a complete object-store key-value engine. It
uses the same Rust `object_store` interface and makes the latency and request
cost trade-off explicit. [SlateDB](https://github.com/slatedb/slatedb).

The smaller `slatedb_txn_obj` crate is directly relevant. It provides a generic
transactional object with conditional update, sequenced versions, and optional
epoch fencing. It can replace some low-level CAS plumbing, but it does not
provide the immutable WAL, exact pending-result evidence, or application
checkpoint contract required here. [slatedb_txn_obj
documentation](https://docs.rs/slatedb-txn-obj/latest/slatedb_txn_obj/).

### SQLite and filesystem projects

Graft is the strongest SQLite research target. It provides transactional page
storage, immutable snapshots, lazy partial replication, read-your-write state,
and conditional remote commits. Its SQLite extension is already built on this
volume model. It is alpha software and brings a larger storage engine, so the
next tranche should compare its page and changeset choices before selecting an
adapter. [Graft architecture](https://graft.rs/docs/internals/).

Turbolite is useful for page grouping, compression, range reads, prefetch, and
cache design. Its documented standalone contract has one safe writer. Direct
multi-writer use can corrupt its manifest. It is not the ordering authority for
multi-tenant factors. [Turbolite](https://github.com/russellromney/turbolite).

`s3-wasi-fs` is a useful host-binding and conformance reference. It separates a
Wasmtime-free filesystem core from Wasmtime `wasi:filesystem` bindings and has
a MinIO SQLite demonstration. Its documented limits include non-atomic rename,
open-unlink differences, and last-writer-wins concurrent writes. It must not be
used as the durable multi-writer authority. [s3-wasi-fs compatibility
summary](https://github.com/aruokhai/s3-wasi-fs#compatibility-matrix).

Crab is less direct. Its useful ideas are content-defined immutable chunks,
hash verification, lazy hydration, and the rule that immutable data must exist
before a conditional mutable reference update. It does not provide the generic
ordered state-machine log. [Crab](https://github.com/crabbuild/crab).

## Why the host boundary matters

A Spin factor can keep object-store credentials in the host. A guest-level S3
library would instead need network authority and provider configuration. A
host `wasi:filesystem` implementation avoids that credential problem, but it
still has a larger semantic problem than a key-value factor.

Filesystem calls use descriptors, offsets, directory streams, rename, links,
open-unlink behavior, and partial writes. SQLite adds page locking, journal
ordering, sync, and crash expectations. Mapping each call to an object request
is slow. Caching is not only a byte cache: it must preserve coherent handle and
metadata behavior across updates. The `s3-wasi-fs` limitations show why direct
object mapping is not enough for safe concurrent tenants.

The clean order is therefore:

1. Keep the WAL as the only durable ordering authority.
2. Prove key-value operations and checkpoints.
3. Add safe garbage collection.
4. Select a SQLite representation after comparing page objects, SQLite session
   changesets, and VFS journal records.
5. Implement `wasi:filesystem` only after its metadata transaction model is
   explicit and tested.

## Throughput and the preferred owner

One total order has one serialized publication point. Strictly durable,
unbatched commits to one resource therefore cannot exceed the latency of that
authority. Cursor's published S3 figures are hundreds, not thousands, of
pushes per second. Thousands of independent resources can scale by partition,
and thousands of logical operations on one resource can use group commit.

A preferred in-memory owner is an optimization, not a new authority:

- Rendezvous hashing selects the normal process for each resource.
- That process retains the current cursor and materialized state.
- It accepts work into a bounded queue.
- It validates and applies operations in queue order to tentative state.
- It writes one WAL entry for a bounded batch and performs one index CAS.
- Admission starts publication in a detached task. Caller cancellation cannot
  stop an admitted batch.
- It replies to each caller through a one-shot channel only after the CAS
  succeeds, using results stored in the entry.
- On conflict, it discards tentative state, refreshes, and validates again.
- Another process can take over by loading the durable checkpoint and tail.
- A stale owner remains safe because it cannot pass the object-store CAS.

This can produce thousands of logical operations per second when a commit
contains many operations. To produce thousands of separately acknowledged,
unbatched commits per second on one resource, the design needs a lower-latency
linearizable authority, such as a replicated memory/NVMe log, or it must split
the resource into independent logs. Object storage can remain the long-term
WAL and checkpoint store in that design.

## Implemented decision

The current library keeps one mutable `index.cbor`. WAL entries, payloads,
reference nodes, checkpoints, and collection plans are immutable. Each
deletable key combines a random physical ID with its deterministic BLAKE3
content identity. The random durable incarnation prevents a cursor from one
log lifetime from authorizing another lifetime.

Checkpoints expose their declared roots. Reference nodes have opaque payloads
and explicit children. This permits adapter-specific trees without hiding GC
reachability. Replay verifies the WAL chain and loads payloads on demand. A
validated backend/root handle performs one capability probe and then derives
tenant scopes without more probe requests.

The public result model distinguishes committed, definite conflict, pending,
and expired evidence. A recovery token preserves the exact candidate before
publication. The local key-value module proves atomic commands and recorded
results. Cursor-style bounded garbage collection is locally complete. SQLite
is next, then `wasi:filesystem`. Live AWS qualification is separate.

The final all-feature local gate passes 135 tests. A pinned MinIO rerun passed
one integrated test, including 1,001 collection candidates. The Criterion
matrix measures in-memory append, recovery, contention, collection graph
shapes, fence lookup, and complete-set resume. These results do not prove S3
latency, multi-process behavior, or the full fault matrix. See [the initial
baseline](evidence/local-baseline-2026-09-02.md), [GC
evidence](evidence/gc-local-2026-09-03.md), and [test
gaps](testing.md#current-matrix-gaps).

## Material limits and disagreements

- Cursor publishes production results but not its implementation. Its report
  supports the architecture and measured envelope, not a reusable library.
- WalTier is small and reusable, but its whole-image rewrite and at-least-once
  ambiguity are not the chosen contract.
- `wal3` has the best surveyed high-throughput and garbage-collection design,
  but its workspace coupling makes direct reuse expensive. Its recovery can
  adopt an unreferenced next-sequence fragment. `object-log` deliberately
  requires exact transaction evidence instead.
- Graft, Micelio, Turbolite, and `s3-wasi-fs` describe themselves as alpha,
  experimental, or semantically limited. They are evidence, not production
  qualification for this project.
- No live AWS test has run. No claim in this report treats MinIO as proof of S3
  performance or complete provider compatibility.

## Claim-to-source ledger

All sources were accessed on 2026-09-02.

- Cursor, “Git at any scale”: WAL/index design, preferred-owner routing,
  compaction, and published S3 throughput. https://cursor.com/blog/git-at-any-scale
- Dan Goodman, WalTier README and `src/wal.rs`: whole-image CAS, batching,
  replicas, compaction, and ambiguous-write semantics.
  https://github.com/danthegoodman1/waltier
- Chroma, wal3 README and Cargo manifest: fragment tree, batching, garbage
  collection, cancellation safety, and workspace coupling.
  https://github.com/chroma-core/chroma/tree/main/rust/wal3
- Tuist, Micelio WAL schema: immutable entry, index pointer, base, and index
  format. https://github.com/tuist/micelio/blob/main/priv/proto/micelio/wal/v1/wal.proto
- Apache Arrow, `object_store` Rust documentation: provider abstraction and
  conditional update API. https://docs.rs/object_store/latest/object_store/
- Amazon Web Services, S3 conditional-write documentation: `If-Match` and
  `If-None-Match`. https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html
- Jay Jamieson, objwal README: segment batching, group commit, primary fencing,
  and replica materialization. https://github.com/JayJamieson/objwal
- SlateDB project and `slatedb_txn_obj` documentation: object-store LSM and
  reusable conditional transactional object. https://github.com/slatedb/slatedb
- Graft documentation: transactional page volumes, snapshot reads, and remote
  conditional commits. https://graft.rs/docs/internals/
- Russell Romney, Turbolite README: SQLite page/cache design and single-writer
  limitation. https://github.com/russellromney/turbolite
- Aruokhai, s3-wasi-fs README: WASI host split, MinIO SQLite demonstration, and
  filesystem semantic limits. https://github.com/aruokhai/s3-wasi-fs
- Crab project README: immutable chunks, hash verification, lazy hydration,
  and conditional mutable references. https://github.com/crabbuild/crab
- Tobi, walgit README: Git-specific WAL, CAS manifest, group commit, and broad
  product surface. https://github.com/tobi/walgit
