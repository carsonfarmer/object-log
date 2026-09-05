# Authenticated catalog foundation

Issue #19; exclusive branch `cf/git-catalog-tree`, based on `24c6ede` plus
approved child-proof traversal `67a2d79` (locally cherry-picked as `803c5dc`).

This is a private foundation compiled only by the Git library's test module.
Repository state and the serving component do not use it yet. It is not a
catalog migration, a durable-format cutover, lazy-reader acceptance, pack
compaction, or completion of #19. The implementation follows
`docs/git-catalog-proposal.md`; root publication still uses ordinary object-log
reference nodes and the existing head.

## Representation and update rules

`CatalogTree` owns an object format and optional root publication proof. Root
height and lower bounds are read from authenticated payloads, never trusted from
external metadata. `lookup` follows lower-bound keys to a leaf and returns the
pack descriptor, authenticated pack-root proof, and standard-index position.
The eventual reader must validate the selected index and cross-check that OID
and position; the tree alone does not replace pack/index/content validation.

Canonical CBOR payloads distinguish levels: leaf level zero carries ordered
OID keys, a sorted local pack table aligned with authenticated children, and
parallel slot/index arrays. Branches carry lower-bound keys aligned with
catalog child nodes. Arrays have at most 64 entries; the decoder rejects counts
before allocating. Every node is also limited to the smaller of 16 KiB and the
log's configured object-byte limit. The tested log limit remains 8,240 bytes.

Insertion merges a sorted pack batch only into affected leaves. Duplicate OIDs
choose the lowest pack ID, as the existing catalog does. It reuses child proofs
for unchanged subtrees through `read_staged_node`; it does not recursively
restage those subtrees. Splitting halves a group until both the entry ceiling
and exact encoded node-size check pass. All leaves stay at one depth, with a
maximum root level of eight. Non-root groups may contain one entry because
byte-size constraints vary; there is no deletion/rebalancing policy in this
append-only foundation. A newly assembled root must reduce the number of child
roots, otherwise construction rejects a limit too small to hold two children.

Lower bounds, levels, and inherited ancestor upper bounds are checked when
children are loaded. Leaf pack tables reject duplicates, unused slots, invalid
kinds, mismatched formats, and invalid index-position bounds. This validates the
visited path; it does not scan untouched subtrees to prove global correctness.

Construction reserves scratch before allocating merged batches, split work,
and local tables. Node reads reserve bounded decoder/state space before I/O;
node writes use core `node_size` before PUT. Logical reads/writes and work
consume the supplied cumulative operation, including failures. The helpers do
not open fresh operation budgets or retry on their own. The caller owns retained-state accounting for returned root/lookup metadata and
the publication lifecycle. It must pair the root with its exact materialized
view when preparing an update; epoch/provenance checks alone do not prove that
an arbitrary staged tree is the current Git state.

## Evidence

Tests use packs and indexes produced by installed Git and normalized/staged
through the existing Git implementation. Both hash formats are covered.

- 128 objects split into two leaves. Cold first/last lookups each issue two
  parent/path GETs and no PUTs, without reading pack indexes or pack chunks.
- Adding one OID to that full leaf reads two catalog nodes, writes three, and
  reuses the untouched sibling's exact authenticated reference.
- Forty actual Git packs with 100 OIDs each grow a balanced tree to root level
  two. Sample lookups from every batch issue three GETs, independent of scanning
  the forty pack roots.
- A leaf with distinct pack roots splits on encoded bytes before exceeding its
  64-entry ceiling; no test increases the 8,240-byte log limit.
- Duplicate OIDs choose the same lowest pack ID in either insertion order.
  Invalid sorted batches fail without storage requests.
- Authenticated malformed payloads, huge array counts, wrong levels/lower bounds,
  trailing bytes, invalid slots, and descendants crossing ancestor bounds fail.
- Memory pressure rejects before I/O; dropping a paused insertion releases its
  operation reservations.
- Generic test materialization/checkpoint/GC preserves catalog-to-pack-to-chunk
  dependencies. Cold lookup resolves a real pack and the existing reader checks
  its object after collection. Stale-epoch proofs are rejected.
- Competing catalog candidates have one head winner. A lost winning reply is
  resolved by a separately opened log; checkpoint/GC removes the losing root,
  and cold lookup retains the winner's mappings only.

These are memory-store and installed-Git filesystem-fixture measurements, not
Spin/MinIO, RSS, remote-provider performance, or public service qualification.
The ordinary WASIp2 product check excludes this intentionally test-only module;
its eventual production wiring needs an explicit WASIp2 gate.

## Verification result

All ten focused catalog tests pass, as do all 130 Git library tests and strict
all-target/all-feature Git Clippy. Formatting and diff checks pass. Independent
Rust correctness/simplification review reran the ten catalog tests and found
no blockers. It suggested an additional isolated lower-key-mismatch negative
case as future coverage; current tests independently cover wrong levels and
inherited upper bounds, and the implementation checks exact lower-key equality.

Raw results are retained in `git-catalog-foundation-2026-09-05/`. These runs use
this worktree's exclusive Cargo target directory. An initial attempt to reuse
another worktree's cache produced a stale-core compilation failure; that failed
attempt is not qualification evidence.

## Remaining integration

Lookup is deliberately cold/stateless in this foundation. Production command
integration needs a bounded decoded-node and selected-index cache: repeatedly
reading the root for each object could exhaust existing call budgets despite
bounded lookup depth. Cache entries must remain tied to the exact view/proofs
and their retained allocation reservations.

Review and implement the explicit v1-to-v2 maintenance migration before adopting
this root in Git state. Preserve old state until successful CAS, uncertain-result
evidence, mixed-history replay, and GC safety. Replace the existing all-pack
catalog only through independently reviewed reader integration. Synchronous
membership checks in graph construction need an asynchronous equivalent without
weakening connectivity. Scoped fetch traversal and non-tip reachability fallback
remain separate work. Larger packs, streaming ingest/index construction,
compaction replacement, and production maintenance are outside this foundation.
