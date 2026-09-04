# object-log handoff

Updated: 2026-09-04 16:36 PDT

## Start here

Read these files in order:

1. `AGENTS.md`
2. `GIT_PLAN.md`
3. `docs/evidence/git-fetch-pack-2026-09-04.md`
4. This file

The active implementation branch is `cf/git-repository-task3`. Its local
worktree is:

```text
/Users/carsonfarmer/Developer/Personal/.object-log-worktrees/git-repository-task3
```

## Goal

Build a small, fast object-storage WAL for higher-level storage systems. Keep
the core byte-oriented and independent from Spin. The KV, SQLite, and Git
crates prove the public API.

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

## Accepted state on main

`main` is clean and pushed at
`1e0c0d35bbcd7bf5b1ce8d555c9f66d16f24ca2c`.

Task 2 is accepted at `b322985c616d948e7365739e3286baeaeb460acc`.
It provides a private bounded fetch-pack writer with:

- SHA-1 and SHA-256 output;
- validated full-object, OFS-delta, and REF-delta reuse;
- full-object fallback when the selected base is absent;
- exact output, memory, call, transfer, work, and retry bounds;
- Git 2.54 strict pack validation; and
- native and locked `wasm32-wasip2` checks.

The full local gate passed with 294 tests passed and 9 opt-in tests ignored.
Hosted CI also passed:

- `b322985`: https://github.com/carsonfarmer/object-log/actions/runs/33927234897
- `1e0c0d3`: https://github.com/carsonfarmer/object-log/actions/runs/33928228253

The current strict product-source counts are:

| Crate | Product lines |
| --- | ---: |
| `object-log` | 4,713 |
| `object-log-kv` | 434 |
| `object-log-sqlite` | 1,460 |
| `object-log-git` | 4,043 |
| `object-log-git-http` | 1,023 |

Tests, benchmarks, documentation, generated code, and vendored code are not in
these counts. The Git count includes the temporary native oracle and the new
replacement path. Keep the oracle until replacement parity is proven.

## Task 3 checkpoint

The branch `cf/git-repository-task3` is clean and pushed at
`f20a8d92155e21589d5b0de5d3096c5cb94c890b`. This is a checkpoint, not an
accepted integration commit.

The branch adds one common `Repository::open(&Log, ObjectFormat)` and keeps the
native oracle behind `open_native`. It connects one exact object-log view,
refs, authenticated pack roots, operation accounting, and durable pack reads.
It deletes the obsolete `storage.rs` path. The net product change is 6 lines.

Evidence on this branch:

- focused repository tests: 10 passed;
- native strict Clippy: passed;
- locked `wasm32-wasip2` check and strict Clippy: passed;
- `git diff --check`: passed;
- full `mise exec -- make check`: failed in the three Git GC integration tests.

## Blocking defect

`durable::stage` uses fixed 1 MiB chunks. The Git GC tests open logs with
`max_object_bytes` values of 8,240 bytes or 16 KiB. A valid native pack then
fails during staging with:

```text
ObjectLog(LimitExceeded("object bytes"))
```

The next change must support the object-log contract instead of increasing the
test limits. Prefer this shape:

1. Stage with `min(1 MiB, log.options().max_object_bytes)`.
2. Derive and validate chunk geometry from the authenticated root children.
3. Avoid a new durable field unless child lengths cannot define one canonical
   layout.
4. Keep random access bounded when a pack uses smaller chunks.
5. Test 8,240-byte, 16 KiB, and 1 MiB chunk limits.

The checkpoint already fixes two related bridge defects. Native file reads
reserve the receive buffer before allocation. Native cold recovery reads the
authenticated stored pack under the 16 MiB durable limit instead of using the
9,437,184-byte network fetch limit.

## Next actions

1. Continue in the existing Task 3 worktree. Do not reset or discard it.
2. Fix and test variable durable chunk geometry.
3. Run `cargo test -p object-log-git --test gc`.
4. Run the focused repository tests, native strict Clippy, locked WASIp2 check,
   and `mise exec -- make check`.
5. Run one independent Rust correctness and simplification review.
6. Commit the focused fix on `cf/git-repository-task3`.
7. Integrate the Task 3 commits into current `main`, push, and update issue
   #17 and the README.
8. Continue with Task 4 in `GIT_PLAN.md`. Do not stop after Task 3.

## Decisions and risks

- The object-log head remains the only mutable durable authority.
- Immutable objects use deterministic content IDs and random physical IDs for
  safe deletion.
- Keep the sparse range-read design. Do not replace it with whole-pack loading
  only to reduce source lines.
- Keep all operation counters cumulative across one expired-view retry.
- Buffer and validate a response before returning bytes.
- Task 3 currently retains `Catalog` in `Repository`. An earlier review
  preferred command-local catalogs so `ls-refs` avoids index loads. Review this
  after the GC blocker is fixed. Do not expand Task 3 for it unless evidence
  shows a correctness or measured performance defect.
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
