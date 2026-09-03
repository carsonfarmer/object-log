# Garbage-collection tranche

## Outcome

Bounded and restart-safe deletion is complete for one log namespace. The
mutable head remains the only authority. SQLite is the next product goal.

## Safety contract

1. BLAKE3 stays the deterministic content identity. Each deletable object also
   gets a private random physical storage ID. The physical key includes the log
   incarnation, object kind, storage ID, and content digest.
2. A new payload or node staging call gets a new physical storage ID. It has no
   caller-visible retry token. Internal recovery within that one awaited call
   can reuse the ID. A random-ID collision causes allocation of another ID;
   existing bytes are not accepted as a new staging result.
3. The head contains a monotonic collection epoch, zero or one active plan,
   and a bounded sorted set of retention IDs.
4. A retention ID conservatively protects the complete log namespace. Any
   active retention blocks plan installation. An active plan blocks new
   retention acquisition. Retention has no TTL or automatic release.
5. `start_collection` verifies the complete live graph from one exact view,
   lists the bounded namespace, writes a sorted positive deletion plan, and
   installs its reference with the same head CAS that increments the collection
   epoch. Candidate deletion starts only after that fence is active.
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
10. After all deletes issued by that invocation have a definite success or
    missing result, the collector clears the exact plan with a head CAS. A
    concurrent append can cause a conflict, but its data remains in the next
    head. A prior duplicate delete can finish later; physical-ID non-reuse
    makes it harmless.
11. A delayed delete cannot affect new data because a new staging call cannot
    reuse the deleted physical storage ID. A plan from an old incarnation
    cannot address a replacement log's data.
12. Object and node reads take a `View`. When a read is missing, the supplied
    view and current head determine expiry. An unretained view from an older
    collection epoch returns explicit expiry. A retained view or current-epoch
    view reports missing data as corruption.
13. Exact commit recovery can recreate its WAL object only as part of the
    original non-rebased source-head CAS. Checkpoint resolution requires its
    staged checkpoint object to remain durable. A new logical attempt allocates
    a new physical ID and restages collected payload references.
14. The plan object is not part of its own positive set. After a definite
    rejected fence CAS or a completed clear, the library deletes that plan
    object on a best-effort basis. A later collection can remove it if cleanup
    fails.

## Public API boundary

The implementation must keep these operations small and explicit:

```text
RetentionId::new() -> RetentionId
View::collection_epoch() -> u64
Log::retain(view, id) -> RetentionStatus
Log::release_retention(view, id) -> RetentionStatus
Log::start_collection(view) -> CollectionStart
Log::resume_collection(view) -> CollectionFinish
Log::read_object(view, reference) -> bytes
Log::read_node(view, reference) -> ReferenceNode
```

`RetentionStatus` distinguishes an applied state, an active-collection block,
a head conflict, and an uncertain update. `CollectionStart` distinguishes an
empty plan, an active fence, a head conflict, an active retention, and an
uncertain fence update. `CollectionFinish` distinguishes completion, a head
conflict, and an uncertain clear or delete. A collection report contains
candidate count, candidate bytes, and the number of candidate keys submitted
for deletion. Fields stay private and have getters.

An installed plan reference is durable recovery evidence in the head. After a
`Pending` start, the caller loads the head: an active plan is resumed, while an
unchanged source can start again. After a `Pending` finish, the caller loads the
head: the same plan is resumed, while no plan proves completion. No public
pending-operation or plan-ID type is required.

No API accepts a raw object path. No deletion type can represent the mutable
head. The core does not add a collector trait, background task, distributed
lease, mutable deletion bitmap, Bloom filter, or provider-specific branch.

## Required evidence

- Append, checkpoint, retention, and collection races have both CAS orderings.
- A direct deletion-set reference and a nested node reference cannot publish.
- The newly encoded commit or checkpoint object cannot be in the deletion set.
- Missing, corrupt, oversized, or over-budget live graphs fail before fence
  installation or deletion. A valid content-addressed cycle cannot be built
  without breaking digest verification.
- Cancellation before and after fence installation and before and after a
  visible delete is restart-safe.
- Two collectors can repeat deletes, but only one clears the exact plan.
- A delayed delete after fence clearing cannot remove newly staged identical
  content.
- A forced random-ID collision allocates a different physical key rather
  than accepting an existing object.
- An old-incarnation delete cannot affect a replacement incarnation.
- Retention versus fence installation has one winner. A lost retention update
  resolves from the stable retention ID.
- Older missing reads return view expiry. Current missing reads return
  corruption.
- Listing counts unknown entries toward its limit and never deletes them.
- Memory and filesystem deletion are repeatable. The pinned MinIO flow covers
  listing and 1,001 candidates across the 1,000-key bulk-delete boundary.
- Benchmarks measure 1,000, 10,000, and 100,000 listed objects, several live
  ratios, graph shapes, fence checks, and partial-resume cases.

## Tasks

- [x] Adversarially review the fence and delayed-delete protocol.
- [x] Review the smallest backend list and delete boundary.
- [x] Review the Rust API, test matrix, and line budget.
- [x] Add physical storage identity to durable references and keys.
- [x] Add the collection epoch, active-plan reference, and retention IDs.
- [x] Add canonical collection-plan encoding and strict limits.
- [x] Add namespace-safe list and immutable-only batch delete.
- [x] Add retention acquisition and release.
- [x] Add bounded live-graph marking and positive plan creation.
- [x] Add fenced publication, repeatable deletion, and plan clearing.
- [x] Add deterministic race, fault, cancellation, and model tests.
- [x] Add collection benchmarks and update local evidence.
- [x] Add large end-to-end collection acceptance and a completion deadline.
- [x] Update the README and protocol documentation with measured behavior.
- [x] Complete independent correctness and strict line reviews.

The product-code review threshold for this tranche is 650 net new physical
lines. More than 750 lines requires a reduction review before integration.

## Completion record

Revision `5419e9aa793bf94fad77e22da75fb96c346ccb28` passes 135 local
tests. The focused GC suite has 27 tests. The independent correctness review
found no remaining P0 or P1 defect. The Rust simplification change removed 80
net lines of code and tests by deleting redundant validation and allocation
without changing the safety contract.

From baseline `825447c` through evidence revision `5419e9a`, GC added 1,344
product lines, 2,601 test and support lines, 163 benchmark lines, 169
documentation lines, and no operator or infrastructure lines. At `5419e9a`,
the repository contains 4,857 product, 6,812 test and support, 464 benchmark,
1,637 documentation and schema, and 138 operator and infrastructure lines. The
total is 13,908 lines without `Cargo.lock`.

The local MinIO flow passed one test in 2.22 seconds. It deleted 1,001
candidates and left only `index.cbor` in the GC log. The benchmark evidence is
in [the GC local record](docs/evidence/gc-local-2026-09-03.md).

Revision `576093b7b94177e7f12bab7bf5e16e13c6d9213f` adds the opt-in
large acceptance target. Its timed collection phase removed 100,000
memory-backed candidates in 1.717 seconds and 10,001 local MinIO candidates in
1.609 seconds. Both cases kept the exact live graph, removed the plan, and
reported no work on a second collection. See
[the large GC acceptance record](docs/evidence/gc-acceptance-2026-09-03.md).
