# Authenticated Git catalog proposal

Design baseline: `03f1761`. This is a proposal for issue #19, with the root
reviewer's direction to develop an authenticated catalog. It does not authorize
a durable-format cutover. Implementation and migration require their own review.
The generic object-log WAL remains the product; Git object ordering and pack
semantics remain private to `object-log-git`.

## Problem and bounded outcome

The current catalog reads every live pack root, including its full standard
index, and constructs an in-memory combined directory. Repeated small pushes
therefore increase cold lookup requests and memory even for a narrow fetch.
Checkpointing removes wholly unnecessary packs but does not consolidate packs.
Repacking into bounded-size packs alone cannot remove this dependence.

The proposed catalog maps an object ID to its authenticated pack location by
reading a bounded tree path. Lookup cost grows with tree depth and requested
objects, not with a scan of unrelated packs. It does not by itself remove
whole-history graph traversal, provide 1 GiB ingest, or solve collection's
namespace-size ceiling.

## Private tree and existing packs

Use a conventional immutable copy-on-write B+ tree, encoded in canonical CBOR
inside existing object-log reference nodes. There are two node forms:

- A branch carries ordered child lower-bound keys. Its authenticated children
  are catalog nodes.
- A leaf carries ordered `(object_id, pack_slot, index_position)` entries and
  a local table of `(pack_id, pack_bytes)` descriptors. The table aligns with
  authenticated pack-root children; duplicate pack roots within a leaf share
  one slot.

Pack roots retain the existing standard Git index and authenticated chunk
references. The initial reader loads a selected pack's index lazily and keeps
its existing validation, delta decoding, and sparse range reads. This avoids
coupling the catalog to a replacement pack format.

A modest fanout ceiling, provisionally 64, limits arrays and allocation. Exact
encoded node size is also a split condition: entries spread across many packs
need more child references than entries sharing one pack. Do not assume a
fixed entry count fits the existing 8,240-byte object limit. Minimum occupancy,
split rules, and root exceptions must be specified in codec tests before the
format is accepted. No new generic tree abstraction or research data structure
is needed for this tranche.

Validation must cover format version, SHA-1/SHA-256 key width, strict key order,
nonempty non-root nodes, bounded height/fanout, child/table alignment, valid
slots, and child levels. Check lower bounds and key ranges when reading each
child. A leaf location is usable only after its selected standard index verifies
that the recorded position contains the requested OID. Existing pack/index,
CRC, zlib, and object-ID validation remain required.

## One publication authority and explicit migration

The eventual Git state is refs plus an optional authenticated catalog-root
proof. A transaction carries ref updates and a replacement catalog root; a
checkpoint carries the ref snapshot and root. These are ordinary log objects
and records. No independently mutable catalog manifest or head is introduced.

A push stages its pack and changed tree paths, validates connectivity, then
publishes refs and the replacement root through the existing conditional head
write. Older roots remain dependencies of retained WAL records until normal
checkpointing and collection permit reclamation. The collector traverses
catalog branch, leaf, pack-root, and chunk edges without knowing Git semantics.

Prefer an explicit v1-to-v2 maintenance migration. Keep v1 decoding and the
original refs/pack state usable until migration publishes successfully. Build
the v2 tree against one exact observed v1 view and publish a versioned migration
record through the existing head CAS. A conflict must preserve the winner and
require revalidation/restart; a lost reply must be resolved as an exact candidate.
Never interpret an unsupported version as an empty repository.

The reviewed migration design must specify how mixed v1 history and the v2
transition materialize, how a later checkpoint represents the result, and which
reader versions can open each state. Test interruption before publication,
conflict, uncertain publication, cold restart, and GC around the transition.
Preserve old-state/head-history evidence until normal retention and resolution
rules allow deletion. Do not ship an irreversible cutover as part of a tree
codec or lookup patch.

A versioned Git maintenance record also allows later catalog changes without
changing refs. This matters because the current checkpoint API requires a
through-commit in the active tail: checkpointing an already empty tail cannot
publish another representation by itself.

## Lazy reader integration

Provisional private entry points are `Catalog::open`, asynchronous `lookup`,
and batched `insert_pack`. Opening stores the exact view/root and operation
budget without reading pack indexes. Lookup returns a pack descriptor, its
authenticated root, and an index position. The reader loads only touched packs.

Initially retain touched packs within the existing state budget and return an
explicit limit error when exhausted. A cache eviction policy can follow measured
need. Delete the all-pack directory rather than rebuilding it elsewhere.

Graph scheduling currently calls synchronous `Reader::contains`. Tree lookup
is asynchronous. Replace that assumption with ordered or batched asynchronous
existence checks while preserving validation of referenced leaves. Removing
those checks would weaken connectivity validation. Keep this integration
separate from capacity work on streaming blobs and packs.

## Scoped fetch traversal is separate

The current fetch path starts graph traversal from every ref. A catalog tree
alone therefore does not prevent unrelated-history amplification.

A first compatible fast path can authorize wants that exactly match advertised
ref targets using the exact ref snapshot, then traverse only their closures.
Valid haves and negotiation behavior must remain correct for that observed view.
Handle include-tag by examining and peeling tag chains without traversing every
unrelated tag target's graph.

Non-tip wants still need exact reachable-from-refs validation under the current
policy. Preserve the existing fallback until a reachability index or policy
change is reviewed. Acceptance must distinguish the scoped fast path from the
fallback; do not claim universal narrow-fetch scaling.

## Core dependencies and failure behavior

Issue #31 supplies exact core-owned node sizing before writes. Issue #33 evaluates
child-proof traversal: reading an authenticated same-epoch parent should permit
reuse of its unchanged child subtrees without recursive restaging. Any accepted
API must preserve log provenance, epoch and collection fences, and must not grant
proofs from arbitrary object references. These are separate reviewed prerequisites,
not a ratified API embedded in this proposal.

For the initial catalog update, a head conflict returns conflict/restart. A root
built from stale state must never overwrite a concurrent push's mappings or refs.
Future reuse of completed compaction outputs requires explicit input validation
and merging against the current root.

Existing namespace retention is a safe interim guard for long maintenance: it
protects the entire namespace indefinitely through the same head. Confirm it
before staging; retain its recovery identity; release it only after confirmed
publication or abandonment. Crash recovery and confirmed release need tests and
an operator path. This is not a fine-grained staging lease. Consider durable job
roots only if measured maintenance needs justify them.

Current collection marks/scans the namespace with a 100,000-object ceiling and
materializes its deletion plan. Catalog improvements do not remove those bounds.
Expose them in scale evidence and evolve collection separately when required.

## Implementation order and acceptance

1. Implement private codec, splits, and lookup with both hashes, exact size
   limits, malformed nodes, and unchanged small-object-limit tests.
2. Review #31/#33 and prove that COW updates read/write changed paths without
   recursively verifying untouched subtrees. Counters remain cumulative.
3. Implement and independently review the explicit maintenance migration,
   publication, cold reconstruction, lost-response resolution, conflicts, and
   collection of losing outputs. Verify old-state/head-history preservation.
4. Integrate lazy reads, preserving existing exact pack/OID/delta and graph
   validation tests. A cold narrow lookup must read zero unrelated pack indexes.
5. Add repeated-small-push experiments across increasing pack counts. Record
   catalog path reads, bytes, memory, and durable growth with raw evidence.
6. Add scoped fetch tests with a small wanted branch beside a substantially
   larger unrelated history. Preserve and measure non-tip fallback behavior.
7. Run memory tests and installed-Git filesystem receivers before actual
   Spin/MinIO lifecycles; publish reproducible sustained measurements.
8. Add bounded physical pack replacement using the established publication and
   GC edges, including concurrent pushes, interrupted staging, restart, and
   eventual reclamation of losing outputs.

All tranches retain SHA-1/SHA-256, sparse ranges, explicit uncertain results,
GC safety, bounded cumulative quotas and WASIp2 compatibility. The 128 MiB budget
applies to serving, not builds or executable-cache preparation. Issue #26's
50 MiB files and 1 GiB pushes remain a separate, coordinated acceptance target.

Only the project's own issue tracker is used for this proposal. No upstream
report or external communication is part of it. Later SQLite, production KV,
and G-trees-based verifiable KV remain outside this Git tranche.
