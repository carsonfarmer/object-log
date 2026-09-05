# object-log handoff

Updated: 2026-09-04, final adapter qualification.

## Intent

The standalone, byte-oriented object-storage log is the product. Complete,
usable examples prove its correctness and ease of integration. Git is a real
use case from which to improve generic capabilities; keep Git rules in the Git
crate. Cursor's “Git at any scale” is the design inspiration. The core remains
independent from Spin, while the same library runs in a Spin WASIp2 adapter.

Read `AGENTS.md`, `GIT_PLAN.md`, and the evidence linked below. Source-size
thresholds are architecture review signals; required behavior is preserved.

## Accepted main and active work

Main is pushed through `b4b05f3` (Tasks 1–9). Hosted Linux CI run
33938325701 passed. Task 3's authenticated variable geometry preserves all
three original small-object-limit GC tests. Its original worktree and branch
remain preserved. Later accepted work adds command-local catalogs, bounded
iterative traversal, exact have-aware selection, protocol-v2 upload commands,
thin receive normalization, atomic ref publication, and shared checkpointing.

The combined adapter work is in:

```text
/Users/carsonfarmer/Developer/Personal/.object-log-worktrees/git-native-shared
cf/git-native-shared
```

Native and Spin adapters use the same common repository for both hashes.
Classic receive supports default Git's flush-only HTTP probe before chunked
large pushes. Both adapters preserve exact recovery tokens on uncertain
publication, including invalid resolution evidence after a hidden successful
head write. The native path finishes admitted publication after disconnect
and drains its task tracker on shutdown.

## Current evidence

- Combined workspace gate: 359 passed, 12 opt-in ignored; formatting, strict
  native Clippy, and locked WASIp2 check passed. Separate core and Spin WASIp2
  strict checks and release component builds pass.
- Unchanged native clients pass both hashes, v2 traces, 384-commit negotiation,
  default 8 MiB push/clone, rejection, checkpoint, GC, and cold recovery.
- Seven native fault tests cover cancellation, real TCP disconnect, pending and
  expired tokens, corrupt resolution evidence, admission, and body/header limits.
- Actual WASIp2 memory-store lifecycle passes both hashes for 4 KiB, 8 MiB,
  and 384-commit plus thin update fixtures, with exact OIDs and strict Git checks.
- Native/shared/Spin local MinIO lifecycles pass. The final Spin run includes
  collection followed by a fresh host and strict cold clone, with unpooled
  outbound HTTP. No live AWS run is claimed.
- Preserved extended SQLite recovery, staged request accounting, native Git
  request audit, workspace Criterion, and large memory/MinIO GC gates pass.
- Independent Rust correctness, simplification, architecture, quota, and
  adversarial reviews completed; findings were repaired. Final WASIp2 paired
  performance evidence and prose review are being completed before integration.

Evidence:

- `docs/evidence/git-receive-2026-09-04.md`
- `docs/evidence/git-adapter-regression-2026-09-04.md`
- `docs/evidence/git-shared-performance-2026-09-04.md`
- `docs/evidence/git-wasip2-memory-2026-09-04.md`
- `docs/evidence/git-spin-linux-2026-09-04.md`
- `docs/evidence/git-final-architecture-2026-09-04.md`

## Spin deployment constraint

A fresh Linux Spin process using a prepared executable cache passes the
both-hash workload inside a hard 128 MiB cgroup with swap disabled. Both runs
reach the cap and trigger reclaim; no spare process-memory margin is established.
Empty-cache compilation is OOM-killed under 128 MiB. Prepare the exact component's
cache on the deployment platform outside the serving cgroup using
`crates/object-log-git-spin/prewarm_cache.py`, then serve with `run.sh --cache`.
Cache setup peaked at roughly 228–231 MB in the recorded runs.

The compiled-code cache contains no repository authority. Losing it requires
recompilation; repository recovery still needs only the object-log head and
immutable store objects. `run.sh` forces one pooled component instance and
disables outbound connection pooling. Earlier pooled transport runs had an
intermittent WASI protocol error; its cause is not proven. Successful unpooled
runs and the prior failure are both recorded.

## Preserved boundaries and follow-ons

- The head remains the only mutable durable authority; local state is disposable.
- Sparse range reads remain. Catalog, graph, pack, transfer, and retry accounting
  stay bounded; all engine counters accumulate across one expired-view retry.
- Spin's handler-wide connector budget also includes backend probes and log open:
  512 outgoing attempts and 96 MiB accepted/sent payload. Headers and bytes
  already buffered by the remote transport are outside that payload ledger.
- The native oracle remains selectable. Its deletion is deferred until runtime
  and performance qualification and any required owner review are complete.
- Each live pack requires a catalog-root read. Issue #19 tracks compaction;
  the current proof does not establish arbitrary repository scale.
- Generic local filesystem storage lacks conditional compare-and-swap. Its
  rejection tests and native disposable-filesystem oracle tests remain.

Git replacement: https://github.com/carsonfarmer/object-log/issues/17

Compaction: https://github.com/carsonfarmer/object-log/issues/19

Live AWS: https://github.com/carsonfarmer/object-log/issues/10
