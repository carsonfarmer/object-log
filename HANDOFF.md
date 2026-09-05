# object-log handoff

Updated: 2026-09-04 18:07 PDT

## Start here

Read these files in order:

1. `AGENTS.md`
2. `GIT_PLAN.md`
3. `docs/evidence/git-fetch-pack-2026-09-04.md`
4. This file

Task 3 was integrated at `ae65d80`. Its preserved local
worktree is:

```text
/Users/carsonfarmer/Developer/Personal/.object-log-worktrees/git-repository-task3
```

## Goal

Build a small, fast object-storage WAL for higher-level storage systems. Keep
the core byte-oriented and independent from Spin. The KV, SQLite, and Git
crates prove the public API.

Owner clarification: the examples must be complete and usable, and must prove
ease of integration as well as correctness. Git is a real use case from which
to improve the standalone log API. Classify complexity as domain behavior,
a missing generic capability, or unnecessary integration machinery, and fix
the appropriate layer. Cursor's "Git at any scale" is the design inspiration.

The Git proof must keep its full target:

- Git protocol-v2 discovery, clone, and have-aware fetch;
- classic receive-pack push;
- SHA-1 and SHA-256;
- a WASI-compatible core and a later Spin adapter;
- memory, filesystem, and local MinIO acceptance;
- recovery, collection, limits, benchmarks, and unchanged Git client tests.

Do not remove a required feature to meet a source-size estimate. The 5,000-line
core target and 2,000-line proof target are architecture review signals. Report
and justify material overage. Correct behavior and measured performance have
priority.

## Accepted foundations

Task 2 is accepted at `b322985`. Its private fetch-pack writer supports both
hash formats, validated compressed-entry reuse, and full-object fallback when
a delta base is absent from the selected set. It passed Git 2.54 strict pack
validation, native checks, and locked WASIp2 checks. The native oracle remains
until replacement client and storage parity is proven.

## Task 3 acceptance

Task 3 now passes all local gates after authenticated variable chunk geometry
and repository allocation/accounting repairs. The focused fixes are `1eabf9c`
(geometry) and `c578b22` (bounds). The current branch also contains the latest
main documentation. See `docs/evidence/git-repository-2026-09-04.md`.

- 303 workspace tests passed, 9 opt-in tests ignored.
- All 3 Git GC tests preserve their original 8,240-byte/16 KiB object limits.
- Strict native Clippy and locked WASIp2 check/Clippy passed.
- Independent Rust correctness and simplification reviews completed; findings
  were fixed, including pre-allocation rejection of impossible core node counts.

## Task 4 acceptance

Task 4 adds command-local catalogs and bounded iterative graph traversal.
Seven focused graph tests and the full gate pass (311 passed, 9 ignored).
Independent correctness/simplification review approved after fixing tag-blob
reads and nonadjacent duplicate tree names. See
`docs/evidence/git-graph-2026-09-04.md`.

Hosted Linux CI run 33935170712 passed at `f210cc7`, including the native
loopback that previously failed. The flush fix remains a source-proven repair;
we did not reproduce the earlier hosted failure locally.

## Tasks 5 and 6 acceptance

Exact want-minus-have selection and complete include-tag chains now produce
both-hash packs checked in fresh receivers containing only accepted-have
history. Selected leaves verify their declared type; unrelated leaves remain
unread. Independent correctness review approved after those repairs. The full
gate passes (313 passed, 9 opt-in tests ignored), including native strict Clippy
and locked WASIp2 check/Clippy. See `docs/evidence/git-selection-2026-09-04.md`.

## Task 7 acceptance

Protocol-v2 discovery, ls-refs, ACK-only negotiation, and done-fetch use the
common repository. Responses retain memory reservations through transmission;
one expired-view retry shares counters with open. Matching tag refs are fully
peeled with actual-kind checks. Independent review found and repaired the final
target-kind gap. The full workspace gate passes (318 passed, 9 ignored), with
strict native and WASIp2 checks. See `docs/evidence/git-upload-2026-09-04.md`.

## Next actions

Tasks 8–10 are assembled for integration in `.object-log-worktrees/git-native-shared`.
The common receive/checkpoint path and native adapter are passing focused real
Git client tests for both hashes. Full gates, fault tests, performance evidence,
and replacement MinIO acceptance are running before integration. Thin helper
and receive branches remain preserved. Spin adapter work is isolated in
`.object-log-worktrees/git-spin-adapter`.

The owner reaffirmed Spin serverless compatibility as a design constraint.
A separate `cf/spin-transport-probe` worktree is testing Spin SDK 5.2 with the
established object_store S3 implementation and a WASI HTTP connector. This is
a probe, not an accepted adapter. Keep native-only dependencies out of the
common libraries and verify actual component imports as well as compilation.

## Decisions and risks

- The object-log head remains the only mutable durable authority.
- Immutable objects use deterministic content IDs and random physical IDs for
  safe deletion.
- Keep the sparse range-read design. Do not replace it with whole-pack loading
  only to reduce source lines.
- Keep all operation counters cumulative across one expired-view retry.
- Buffer and validate a response before returning bytes.
- Each live Git pack adds one catalog-root GET. Issue #19 tracks compaction.
  This does not block a small trial, but it blocks a scale claim.
- `sley-protocol` 0.5.2 compiles for Rust 1.97 and WASIp2 and could reduce wire
  code. It is not adopted. Reconsider it only after the current path works and
  a measured simplification review shows a clear net benefit.
- `gitserver-core` is filesystem-bound and does not compile for the accepted
  WASIp2 path. Do not use it.

## Tracking

- Git replacement: https://github.com/carsonfarmer/object-log/issues/17
- Git pack compaction: https://github.com/carsonfarmer/object-log/issues/19
- Live AWS qualification: https://github.com/carsonfarmer/object-log/issues/10
- Roadmap and limitations: https://github.com/carsonfarmer/object-log/issues/11

No live AWS test has run. Local MinIO qualification remains a later gate.
