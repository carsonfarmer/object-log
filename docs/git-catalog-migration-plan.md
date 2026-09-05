# Catalog migration and lazy reader integration

This is an implementation proposal for issue #19, not an activated format
migration. The catalog foundation and command cache remain test-only until the
state machine, reader, receive path, and maintenance operations use them together.
The generic log head remains the only mutable durable authority.

## Ownership and order

1. Catalog worker: authenticated tree and bounded command-local node cache.
2. Capacity worker: selected-pack index cache and sparse object reads in
   `durable.rs`. A selected index must prove that the catalog's OID and position
   agree before its offsets, CRC, or size are trusted.
3. Metadata/state owner and catalog worker agree the versioned record schema.
   Neither changes `state.rs`, `format.rs`, or maintenance concurrently.
4. Catalog worker: explicit migration command and versioned replay/checkpoint
   wiring. Capacity worker: reader construction from the materialized root.
5. Receive owner: new pack insertion and atomic refs/root publication. Graph
   owner: asynchronous existence checks, retaining missing-leaf validation.
6. Whole-service tests, independent review, provider gates, then activation.

The cache interface is `CatalogCache::new(tree, log, view, operation)` followed by
`lookup(id).await`. A successful lookup returns the authenticated pack descriptor,
actual pack-root staging proof, and standard-index position. The cache borrows
one exact log, view, and operation; it cannot be moved between expired-view
retries. A retry constructs a new cache using the existing operation, so all work
and transfer counters remain cumulative. Returned metadata is owned by the
caller and must be accounted for if retained.

## Proposed record evolution

Keep the v1 decoder and exact canonical encoding for existing records. Introduce
an explicitly decoded v2 record rather than interpreting new fields as v1.
Preserve keys 0–4 (version, checkpoint, hash format, refs, legacy pack descriptors)
and reserve key 5 for catalog operation and key 6 for repository metadata. This
allocation is agreed with the issue #30 owner, pending root approval.
The proposed catalog tags are 0 LegacySnapshot, 1 TreeSnapshot, 2 Unchanged,
3 Migrate, and 4 Replace. Initial metadata implementation recognizes only 0 and 2;
it rejects the reserved tree tags until their dependency validation is implemented.
Every v2 record includes key 5. Metadata tags are 0 Unchanged, 1 Snapshot(target),
and 2 Update(expected target, new target).

The catalog operation distinguishes unchanged, migrate-to-tree, and replace-tree.
A nonempty tree is represented by exactly one WAL dependency proof; an empty tree
has none. No serialized staging token or independently writable root object is
introduced. Legacy pack descriptors remain meaningful only for legacy state;
they must be empty after migration. Dependency cardinality and each operation's
allowed previous storage mode are validated before mutating materialized state.
A metadata-only upgrade may enter v2 while retaining legacy packs; migration must
accept that state as well as v1. All subsequent writers emit v2, and replay rejects
a later v1 transaction.
A v2 checkpoint explicitly identifies its storage mode and root, so it can be
restored without old process state.

Repository metadata is orthogonal to catalog mode. Reserve an explicit metadata
record for the persisted symbolic HEAD target planned in #30: a checkpoint stores
the current target; a change stores expected-old and new targets. Legacy v1
replay defaults to `refs/heads/main`. Its validation and unset/default semantics
belong to #30. Reserving a field is not permission to silently accept arbitrary
unknown keys: unsupported versions or mandatory metadata operations fail closed.
Optional future metadata needs a deliberately specified canonical extension rule
before implementation; do not assume minicbor's unknown-field skipping provides
forward compatibility.

## Explicit maintenance migration

A migration reads a materialized legacy-pack view, enumerates its authenticated standard
pack indexes within the admitted maintenance budget, and builds the catalog using
actual pack-root proofs. Index enumeration needs a bounded streaming or per-pack
interface from the capacity owner; collecting every index together is forbidden.
Each catalog insertion preserves the existing deterministic duplicate-OID winner.

It then appends one migration transaction against that exact view. Ref contents
and repository metadata remain unchanged. The existing head CAS establishes the
expected source state; replay separately enforces that migration occurs only
from legacy-pack mode, whether its record version is v1 or v2. A conflict reloads state and rebuilds or deliberately aborts;
it does not attach a candidate to a different view by assumption. An uncertain
result returns the generic pending token and is resolvable by a cold process.
The v1 head remains authoritative until publication succeeds. Candidates and
superseded history remain subject to existing staging fences and checkpoint/GC
retention rules.

A new reader must accept a v1 checkpoint followed by migration and v2 transactions,
and also a standalone v2 checkpoint. Old v1 histories remain readable. An old
binary may reject a post-migration history; do not claim rollback compatibility.
Activation therefore requires an explicit operator action and a documented binary
compatibility boundary, not an automatic migration on open.

## Reader, receive, and maintenance consequences

The selected-pack cache must stay bound to the same command context as the node
cache, reserve decode memory before allocation, and evict retained index state
under pressure. It must not eagerly load unrelated indexes. Chunk range reads,
CRC/OID validation, and both object formats remain unchanged.

Graph membership checks become asynchronous or batched. A filtered or shallow
response may not hide a missing referenced leaf merely because its bytes are
omitted. Scoped traversal improvements are separate: replacing directory lookup
alone does not make an all-refs graph walk proportional to the requested refs.

Receive publishes ref changes and the replacement catalog root in the same WAL
transaction. Removing the full pack map also removes its convenient pack-ID
membership test. Preserve or explicitly resolve the current duplicate-pack reuse
contract before cutover: an OID lookup alone cannot establish that a pack exists
when a different pack wins all its OID mappings. Do not reintroduce a whole pack
map solely to conceal this design gap, and do not claim redundant-pack staging
has been eliminated without a test.

Conservative checkpoints can retain the catalog root and its transitive pack
references without enumerating indexes. Pruning/compaction needs a separate
reachable-object rebuild and atomic replacement; this migration does not implement
pack compaction. Catalog leaves may reference a pack multiple times, but generic
GC follows authenticated node children and deduplicates physical object identities.

## Required acceptance evidence

- SHA-1 and SHA-256 legacy fixtures replay unchanged; v1 checkpoint plus migration
  and v2 tail matches a cold v2 checkpoint, including refs and symbolic HEAD.
- Migration conflict, lost publication reply, cold token resolution, interruption
  during staging, and collection between staging/publication preserve one winner.
- Old retained history survives until eligible for ordinary checkpoint/GC;
  unsuccessful candidate nodes and obsolete pack roots become collectible.
- Small 8,240-byte log-object limits remain unchanged. Malformed nodes, catalog
  positions, dependency counts, unsupported versions, and invalid mode transitions
  fail before visible state mutation.
- Repeated bounded fetches across growing unrelated pack history load only tree
  paths and selected indexes. Warm command lookups reuse nodes; cache eviction and
  expired-view retry keep cumulative work, calls, and transfers.
- New receive, delete-only receive, duplicate pack, empty repository, both hash
  formats, checkpoint, pruning, and actual Git clients work in both storage modes.
- Filesystem and local MinIO gates, WASIp2 compilation, and Spin serving evidence
  include the integrated code. The 128 MiB budget applies to serving, not builds.

Passing the node/cache tests alone does not close #19. Required later evidence
includes repeated pushes, long history, bounded catalog work, safe pack compaction,
and sustained Spin/MinIO measurements. These should demonstrate the WAL's useful
properties while keeping Git policy outside the generic library.
