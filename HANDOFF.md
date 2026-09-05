# object-log handoff

Updated: 2026-09-05, Git proof review and issue-driven implementation.

## Intent

The standalone, byte-oriented object-storage log is the product. Complete,
usable examples prove its correctness and ease of integration. Git is a real
use case from which to improve generic capabilities; keep Git rules in the Git
crate. Cursor's “Git at any scale” is the design inspiration. The core remains
independent from Spin, while the same library runs in a Spin WASIp2 adapter.

Read `AGENTS.md`, `GIT_PLAN.md`, and the evidence linked below. Source-size
thresholds are architecture review signals; required behavior is preserved.

## Current implementation

The complete shared proof was accepted at `c3273b0`; hosted Linux CI
33939435517 passed. The owner subsequently requested removal of the previous
native Git implementation. That implementation, its `native-oracle` feature,
filesystem repository API, and protocol-v0 host path are removed.

Both the previous native engine and the entire `object-log-git-http` crate are
removed. Spin is the HTTP host. Installed Git remains the independent test and
benchmark reference. Useful client tests run against real Spin/MinIO; uncertain
publication, corrupt evidence, and expired token tests run in the portable Git
library. The removed native host's TaskTracker/disconnect/shutdown behavior is
not a Spin guarantee: Spin awaits publication inline.

Removal was integrated and pushed at `0368454`; hosted Linux CI 33941521276
passed. Do not interpret historical evidence as claiming deleted APIs still exist.

## Removal verification

The final combined removal gate passes 322 tests, with 12 opt-in tests ignored
in that ordinary run. Formatting, strict native linting, default-feature core
WASIp2 checking, and strict Spin WASIp2 linting pass. Actual Spin/provider tests
are run separately; the final removal evidence records their exact results.
Independent correctness and simplification/deletion reviews cover the changes.

Current adjusted product Rust is 4,885 lines: Git 4,293 and Spin 592. A
consistent raw preamble count falls from 7,526 to 4,955; see the evidence for
test-helper and historical blank-line accounting. The lockfile drops 54 packages without version bumps. Generic object-log
source is unchanged. See `docs/evidence/git-native-removal-2026-09-04.md`.

## Owner's clarified follow-on scope

The first bounded functional proof is implemented; the broader project is not
complete until the generic WAL approach demonstrates useful Git scale.
Prioritize compaction/scale (#19), Spin memory/startup/concurrency (#21), pooled
HTTP investigation (#22), SHA-1 large-push performance (#23), shallow/partial/
filtered clones and packfile URIs (#24), and simplification (#25). All have
explicit GitHub acceptance criteria. Keep the core generic and independent of
Spin; implement Git semantics in the Git crate. See `docs/follow-ons.md`.

## Qualification before removal

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
  performance and prose reviews are complete. All functional/resource gates pass;
  one latency result remains recorded for performance review.

Evidence:

- `docs/evidence/git-receive-2026-09-04.md`
- `docs/evidence/git-adapter-regression-2026-09-04.md`
- `docs/evidence/git-shared-performance-2026-09-04.md`
- `docs/evidence/git-wasip2-memory-2026-09-04.md`
- `docs/evidence/git-spin-linux-2026-09-04.md`
- `docs/evidence/git-spin-performance-2026-09-04.md`
- `docs/evidence/git-final-workspace-2026-09-04.txt`
- `docs/evidence/git-final-architecture-2026-09-04.md`

## Spin deployment constraint

A fresh Linux Spin process using a prepared executable cache passes the
both-hash workload inside a hard 128 MiB cgroup with swap disabled. The runs
reach the cap and trigger reclaim; the extended workload also shows small
temporary accounting overshoots permitted by Linux. No spare margin is established.
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
- The owner authorized old-engine removal independently of the performance
  finding: actual WASIp2 SHA-1 8 MiB push was 1.655× p50 and 1.634× p95 against
  installed Git after 30 pairs. That benchmark remains; deletion does not
  claim a speed improvement.
- Each live pack requires a catalog-root read. Issue #19 tracks compaction;
  the current proof does not establish arbitrary repository scale.
- Generic local filesystem storage lacks conditional compare-and-swap. Its
  rejection tests remain. Git-client fixtures still exercise filesystem receivers,
  while the product engine requires no local Git repository.

Git replacement: https://github.com/carsonfarmer/object-log/issues/17

Compaction: https://github.com/carsonfarmer/object-log/issues/19

Live AWS: https://github.com/carsonfarmer/object-log/issues/10

The pooled HTTP error is now reproduced with a standalone Spin SDK component
without object-log, object_store or the custom bridge. Pooled GETs fail with
IncompleteMessage; two unpooled runs pass 1,000 invocations each. Exact fault
ownership remains unproven. See `docs/evidence/spin-pooling-2026-09-05.md` and #22.

## Current review and execution queue

Read `docs/reviews/git-proof-2026-09-05.md`. GitHub #11 is the queue and #17 the
Git outcome. New findings #27–#35 cover authenticated read bounds, repeated packs,
normal refs/default branch, maintenance, client authorization, node sizing,
proof-preserving traversal and bounded materialization. #26 requires at least
50 MiB regular files and 1 GiB pushes with working fetch/recovery/maintenance.
The owner accepts 128 MiB as a serving-runtime budget; builds and Wasmtime
compilation/cache preparation are outside it. Do not call their OOM under the
serving cap a product acceptance failure.

The first reviewed queue batch has source revision `5c037c3`: #27/#28 fixes,
Spin read-only policy/operator instructions, and authenticated range copying
for fetch. Combined gates pass 327 ordinary tests (12 opt-in ignored), strict
native/WASIp2 checks, separate Git/Spin MinIO tests, request accounting, and six
actual-WASI memory fixtures. See the review's accepted-batch section and raw
`docs/evidence/git-proof-queue-2026-09-05/` logs. Existing capacity limits and
#23's actual-WASIp2 latency finding remain unchanged.

Root integrates in exclusive `cf/git-proof-queue`. Next subagents own bounded
materialization (#34) and general ref namespaces (#29); sidebar tasks own
incremental blob verification (#26) and maintenance design (#32). They deliver
commits and evidence; only root integrates main. Do not overlap their files.

No upstream reports or external communications without explicit approval of
the concrete contents. The prior upstream report was withdrawn and its original
revision removed. Do not restore upstream links or cross-references. Local #22
remains an investigation. Keep later SQLite, production KV and the owner's
G-trees-based verifiable KV work out of the present Git tranche.
