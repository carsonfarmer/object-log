# Garbage-collection tranche

## Outcome

Add bounded and restart-safe deletion for one log namespace. Keep the mutable
head as the only authority. Do not start SQLite or WASI filesystem work in this
tranche.

## Safety contract

1. BLAKE3 stays the deterministic content identity. Each deletable object also
   gets a private random physical storage ID. The physical key includes the log
   incarnation, object kind, storage ID, and content digest.
2. A new staging call gets a new physical storage ID. An exact retry uses the
   same ID. No public API can recreate an old physical key from bytes alone.
3. The head contains a monotonic collection epoch, zero or one active plan,
   and a bounded sorted set of retention IDs.
4. A retention ID conservatively protects the complete log namespace. Any
   active retention blocks plan installation. An active plan blocks new
   retention acquisition. Retention has no TTL or automatic release.
5. `start_collection` verifies the complete live graph from one exact view,
   lists the bounded namespace, writes a sorted positive deletion plan, and
   installs its reference with the same head CAS that increments the collection
   epoch. It does not delete objects.
6. The caller controls the grace interval between `start_collection` and
   `resume_collection`. Retention, not elapsed time, is the proof that a reader
   must finish.
7. Every head update preserves the active plan. Commit and checkpoint
   publication load that plan and reject their own immutable object or any
   direct or transitive reference in its deletion set.
8. `resume_collection` first confirms the exact active plan. It deletes only
   the plan's immutable physical keys in batches of at most 1,000. Missing keys
   are successful deletes.
9. A delete error or cancellation leaves the plan active. A later call repeats
   the complete positive set. The collector stores no mutable progress map.
10. After every delete has a definite success or missing result, the collector
    clears the exact plan with a head CAS. A concurrent append can cause a
    retry, but its data remains in the next head.
11. A delayed delete cannot affect new data because a new staging call cannot
    reuse the deleted physical storage ID. A plan from an old incarnation
    cannot address a replacement log's data.
12. Object and node reads take a `View`. A missing object from an older
    collection epoch returns explicit view expiry. Missing data from the current
    epoch remains corruption.

## Public API boundary

The implementation must keep these operations small and explicit:

```text
RetentionId::new() -> RetentionId
View::collection_epoch() -> u64
View::has_retention(id) -> bool
Log::retain(view, id) -> RetentionStatus
Log::release_retention(view, id) -> RetentionStatus
Log::start_collection(view) -> CollectionStart
Log::resume_collection(view) -> CollectionFinish
Log::read_object(view, reference) -> bytes
Log::read_node(view, reference) -> ReferenceNode
```

`CollectionStart` distinguishes an empty plan, an installed fence, a head
conflict, and an active retention. `CollectionFinish` distinguishes completion
from a head conflict. A collection report contains candidate count, candidate
bytes, and delete attempts. Fields stay private and have getters.

No API accepts a raw object path. No deletion type can represent the mutable
head. The core does not add a collector trait, background task, distributed
lease, mutable deletion bitmap, Bloom filter, or provider-specific branch.

## Required evidence

- Append, checkpoint, retention, and collection races have both CAS orderings.
- A direct deletion-set reference and a nested node reference cannot publish.
- The newly encoded commit or checkpoint object cannot be in the deletion set.
- Missing, corrupt, oversized, cyclic, or over-budget live graphs fail before
  fence installation or deletion.
- Cancellation before and after fence installation and before and after a
  visible delete is restart-safe.
- Two collectors can repeat deletes, but only one clears the exact plan.
- A delayed delete after fence clearing cannot remove newly staged identical
  content.
- An old-incarnation delete cannot affect a replacement incarnation.
- Retention versus fence installation has one winner. A lost retention update
  resolves from the stable retention ID.
- Older missing reads return view expiry. Current missing reads return
  corruption.
- Listing counts unknown entries toward its limit and never deletes them.
- Memory and filesystem deletion are repeatable. The pinned MinIO flow covers
  listing and a 1,000-object bulk-delete boundary.
- Benchmarks measure 1,000, 10,000, and 100,000 listed objects, several live
  ratios, graph shapes, fence checks, and partial-resume cases.

## Tasks

- [x] Adversarially review the fence and delayed-delete protocol.
- [x] Review the smallest backend list and delete boundary.
- [x] Review the Rust API, test matrix, and line budget.
- [ ] Add physical storage identity to durable references and keys.
- [ ] Add the collection epoch, active-plan reference, and retention IDs.
- [ ] Add canonical collection-plan encoding and strict limits.
- [ ] Add namespace-safe list and immutable-only batch delete.
- [ ] Add retention acquisition and release.
- [ ] Add bounded live-graph marking and positive plan creation.
- [ ] Add fenced publication, repeatable deletion, and plan clearing.
- [ ] Add deterministic race, fault, cancellation, and model tests.
- [ ] Add collection benchmarks and update local evidence.
- [ ] Update the README and protocol documentation with measured behavior.
- [ ] Complete independent correctness and strict line reviews.

The product-code review threshold for this tranche is 650 net new physical
lines. More than 750 lines requires a reduction review before integration.
