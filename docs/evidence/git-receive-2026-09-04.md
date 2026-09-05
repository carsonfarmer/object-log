# Common receive and checkpoint acceptance

Date: 2026-09-04. Core implementation: `6ce252c`, Rust 1.97.1, Git 2.54.

The same public `Repository` prepares classic receive requests on native and
WASIp2. Thin packs resolve authenticated external bases in at most 32 rounds,
retain cumulative budgets, and become self-contained durable packs. Pack
checksums, object identities, graph connectivity, branch target kinds,
fast-forward ancestry, stale updates, and ref namespace conflicts are checked
before publication. Rejected staged data remains collectible.

`PreparedPush::publish_receive` publishes the ordered ref transaction and
encodes per-ref status. Its recovery token preserves the exact candidate;
uncertain publication never returns a successful Git status. Responses and
prepared state retain their memory reservations. Shared checkpointing retains
packs containing live objects and their internal delta bases.

The generic API addition `View::collection_plan_bytes` exposes authenticated
size metadata so applications can reserve active collection-plan reads and
scratch before writes. It adds no Git rules or durable authority. Checked
publication arithmetic rejects extreme valid Options values without overflow.

## Verification

- `mise exec -- make check`: 339 passed, 9 opt-in tests ignored; formatting,
  strict native workspace Clippy, workspace tests, locked WASIp2 check pass.
- Locked WASIp2 strict Clippy passes separately.
- Focused tests cover both hashes, mixed thin chains, corrupt and duplicate
  packs, cancellation, invalid graphs, non-fast-forward and stale updates,
  ref namespace conflicts, pending publication, recovery, checkpoint and GC.
- Independent Rust correctness and simplification reviews completed. Findings
  led to collection-plan accounting, snapshot bounds based on retained state,
  and checked publication arithmetic. The native oracle remains.

HTTP adapters, real client fault cases, local MinIO replacement parity, and
Spin process-memory qualification are separate later acceptance tranches.
