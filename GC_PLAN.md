# Garbage-collection contract

Garbage collection is implemented. This file records the safety boundary that
future changes must preserve; executable tests are the acceptance record.

## Authority

- The mutable head contains a monotonic collection epoch, bounded retention
  IDs, and at most one active plan.
- A plan is an immutable, sorted, bounded list of physical object keys.
- Installing the plan and advancing the epoch is one conditional head update.
- The plan is the complete positive deletion set. Listing alone never
  authorizes deletion.
- No progress bitmap, lease, background worker, or second mutable authority is
  part of the protocol.

## Safety

1. Every new immutable object gets a random physical storage ID. Rewriting the
   same content cannot reuse an old deletion target.
2. Collection validates the checkpoint, tail, and complete reachable graph
   from one view before installing a plan.
3. Any reader retention blocks plan installation. An active plan blocks new
   retentions.
4. Publication preserves the active plan and rejects direct or transitive
   references to its deletion set.
5. Deletion is repeatable. A failed or cancelled attempt leaves the exact plan
   active, and the next attempt submits its complete set again.
6. The head clears the exact plan only after all deletion submissions have a
   definite successful or missing result.
7. Missing data from an older unretained epoch is view expiry. Missing data in
   a current or retained view is corruption.
8. Lost retention IDs are cleared only after the operator stops ingress and
   drains all readers.

## Bounds

Collection limits the namespace scan, plan entries, plan bytes, object graph,
node fan-out, reference depth, total decoded bytes, and delete batch size. A
limit failure occurs before plan installation or deletion. Delete batches
contain at most 1,000 keys.

## Required tests

The suite must continue to cover:

- both CAS orderings for append, checkpoint, retention, and collection races;
- direct and transitive fence rejection;
- pending installation and clearing, cancellation, and repeated deletion;
- two collectors racing to clear one exact plan;
- physical-ID collision handling and delayed deletes;
- old-incarnation isolation;
- retained and expired reads;
- corrupt, oversized, cyclic, and over-budget graphs;
- unknown namespace entries and bounded listings;
- memory, filesystem, and MinIO deletion; and
- 100,000 memory-backed candidates and 10,001 MinIO candidates.

Benchmarks cover representative namespace sizes, live ratios, graph shapes,
fence checks, and resumed deletion. Machine-specific transcripts stay outside
the repository.
