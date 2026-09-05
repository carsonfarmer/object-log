# Git repository Task 3

Date: 2026-09-04

Implementation: `f20a8d9`, `1eabf9c`, and `c578b22`.

The common Repository opens an exact object-log view on native and WASIp2.
The native oracle uses the same durable representation through open_native.
The obsolete storage module is removed. No public core-log API was added.

## Repairs and evidence

Pack staging chooses min(1 MiB, max_object_bytes). Readers derive width from
first-child length in the authenticated root, validate all complete chunks and
the final remainder, and retain that geometry only in memory. Root encoding and
reference/cache reservations account for the actual chunk count. Sparse range
reads remain sparse. Both hash formats cover 8,240 bytes, 16 KiB, and 1 MiB;
malformed geometry fails before blob reads. The GC race retains its collection
and retry assertions; its obsolete expectation of a partially written file was
updated for buffered native recovery. Small object limits were not increased.

Repository recovery now reserves before loading/decoding state. Head reads are
charged conservatively at max_head_bytes, including across retries. Transient
scratch uses the 88 MiB live pool; retained view/state/catalog share a 24 MiB
phase guard. The native bridge reads already-normalized packs under the 16 MiB
storage limit rather than reapplying the 9 MiB network limit. Its bounded read
uses an exact allocation and one stack byte to detect growth. A >9 MiB stored
pack and exact/next-byte read cases pass.

The generic node decoder now rejects a child count that cannot fit in the
remaining encoded bytes before allocating the child vector. This is a lesson
from Git's budgeted sparse reader that improves the core for every consumer.

## Gates

`mise exec -- make check` passed: 303 tests passed, 0 failed, 9 opt-in tests
ignored. This includes 3 GC integration tests, 15 repository tests, memory and
filesystem coverage, strict native workspace Clippy, formatting, and locked
wasm32-wasip2 compilation. Separate locked WASIp2 strict Clippy passed.
A 384-transaction history opens successfully. Independent Rust correctness and
simplification reviews identified the allocation and normalized-pack defects;
all reported blockers were resolved before acceptance.

## Limits and next work

Scratch admission is deliberately conservative and implementation-dependent:
64 times configured head bytes, then 8 times retained; 128 times authenticated
checkpoint-plus-tail bytes during materialization; 4 times populated state
storage plus fixed BTreeMap root-leaf allowances afterward. Dense malformed
encodings and current Rust type sizes have regression checks. These are
admission bounds, not measured allocations. Large configured heads or histories
can hit a limit even when their eventual state is small; checkpointing reduces
the tail. The 384-transaction regression guards the planned history fixture.

Native oracle instances keep their original separate pools for concurrent
oracle tests. These do not establish common-engine process admission or Spin
runtime memory. Task 4 will move the retained catalog into object-reading
commands. Protocol-v2 client parity, replacement MinIO qualification, measured
performance, and Spin execution remain future gates. No remote performance or
live AWS qualification is claimed.
