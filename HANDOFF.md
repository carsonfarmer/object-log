# object-log handoff

The goal is a small, powerful, generic object-storage WAL. A useful Git consumer
proves its API; domain rules stay outside the core and the conditional head is
the only mutable durable authority. Read AGENTS.md and GIT_PLAN.md.

The library-backed Go Git consumer lives at `examples/git`, with the Rust WAL
component at `examples/wal-component`. The custom Rust Git engine, native
maintenance command and old Git harnesses are removed.
Local replacement qualification passes; the old implementation remains available in Git history.

Local development uses the upstream go-git v6 prerelease pin in `go.mod`,
without a workspace override. Partial-clone filters are deferred. Leave the
reviewed `carsonfarmer/go-git` branch `cf/partial-clone-filters` parked until the
owner decides whether to resume it.

The Go consumer delegates protocols, formats and packs to go-git. It retains
sparse object lookup, atomic refs, cold recovery, authentication, read-only mode,
safe checkpoint/collection and a small once-only expired-read retry. Before
long tails fill, pushes invoke existing maintenance and reopen before accepting
new updates. The installed Git executable remains the independent test oracle.

Large-file lifecycles have passed at 16, 64 and 513 MiB for both hashes on local
Spin/MinIO. Request byte/object limits and cooperative deadlines are documented
in the Git README. They do not bound total memory or cross-instance concurrency.
Incoming delta bases/results still need full buffers, and outgoing
packs omit delta compression. The temporary pinned component-build adapter patch
fixes Go GC host calls; Spin and the Go collector remain unchanged. Do not claim
a fixed memory ceiling or production readiness. Use a fresh prefix because the
old Git catalog format is incompatible.

Both hashes pass 1,025 ordinary pushes with automatic cleanup and cold history
recovery, shallow/deepen/unshallow, annotated tags and fetch visibility checks.
The full provider suite also passes with frequent Go GC. Concurrent atomic
pushes/readers, interrupted uploads, lost responses, cleanup and forced-restart
recovery pass for both hashes. Issue #41 was traced to MinIO writing conflicting
PUT responses twice, then silently closing a reusable connection. A provider
patch confirmed the cause, but is not an accepted solution. The bridge now retries
an identical conditional storage PUT once after a connection failure, accepting
only a successful replay and otherwise preserving the first uncertain outcome.
Released MinIO and ordinary Spin pass concurrent Git tests; injected response
loss confirms that a successful write followed by a rejected retry stays pending,
with published refs visible after refresh. Both attempts retain the same budget.
Conditional uploads also send `Expect: 100-continue`, making early MinIO
rejections advertise connection closure while successful connections remain
reusable. Use default Spin HTTP pooling; no extra bootstrap retry or provider
patch is required. Git pushes are never replayed. No upstream report or
submission is authorized.
Outgoing delta tests show smaller packs but much more
allocation; the released library exposes no byte-bounded selection through its
transport, so full-object streaming remains the default.
The independent maintenance model, immutable-create fault points, golden bytes
and complete supported-backend MinIO protocol matrix now cover issue #5.
Follow `examples/git/README.md`; use ordinary Spin and isolated local MinIO. No remote
deployment or upstream posts are authorized. Do not restart shared Docker.

Use exclusive worktrees; root alone integrates main. Preserve sparse reads,
explicit uncertain outcomes and cumulative retry counters. Keep reports short;
verification belongs in tests and Git history rather than new evidence archives.
After Git settles, return to the KV design scoped in issue #39.

Mature-tail checkpointing reuses complete verification on an exact local view;
reopened handles and recovery tokens still verify. SQLite recovery uses one
ordered 32-chunk window across records. Cold metadata recovery still reads the
whole tail. Licensing and dependency provenance are in THIRD_PARTY.md.

Graph verification keeps the 32-read window filled as objects finish, and Git
catalog pruning avoids copying unchanged maps. Existing authentication, fencing
and automatic cleanup remain intact. The full Go WAL experiment stays separate
on `cf/go-wal-experiment`: its WASI concurrency trap and timeout/cancellation gaps
block replacement (issue #42). Keep Rust as the accepted core; no runtime fork.

The Git catalog now holds compressed loose objects up to 512 bytes directly in
authenticated leaves, reducing history and collection reads without new core
APIs or custom Git traversal. Larger objects keep sparse WAL chunks. Current validated catalogs remain supported; original array leaves and
unvalidated experimental roots are rejected. There is no migration reader.
The unpatched adapter still traps on ordinary 16 MiB pushes with this layout.
See the Git README for concurrent large-file checks and observed memory use.

Commit encoding borrows opaque payloads and checkpoint encoding shares its
schema with decoding. Durable bytes and owned recovery results are unchanged.

Git now uses generic WAL byte writers/readers for both stored objects and
temporary incoming packs. Chunk sizes, lists and reconstruction belong to the
core; the existing head still publishes the finished root. Use a fresh prefix
for this byte-stream representation. Small inline catalog objects remain inline.
