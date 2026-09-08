# object-log handoff

The goal is a small, powerful, generic object-storage WAL. A useful Git consumer
proves its API; domain rules stay outside the core and the conditional head is
the only mutable durable authority. Read AGENTS.md and GIT_PLAN.md.

The library-backed Go Git consumer lives at `examples/git`, with the Rust WAL
component at `examples/wal-component`. The custom Rust Git engine, native
maintenance command and old Git harnesses are removed.
The core, KV and SQLite implementations are unchanged. Local replacement
qualification passes; the old implementation remains available in Git history.

Local development uses the existing published go-git module pin in `go.mod`,
without a workspace override. Partial-clone filters are deferred. Leave the
reviewed `carsonfarmer/go-git` branch `cf/partial-clone-filters` parked until the
owner decides whether to resume it.

The Go consumer delegates protocols, formats and packs to go-git. It retains
sparse object lookup, atomic refs, cold recovery, authentication, read-only mode,
safe checkpoint/collection and a small once-only expired-read retry. Before
long tails fill, pushes invoke existing maintenance and reopen before accepting
new updates. The installed Git executable remains the independent test oracle.

Large-file lifecycles have passed at 16, 64 and 513 MiB for both hashes on local
Spin/MinIO. Incoming delta bases/results still need full buffers, and outgoing
packs omit delta compression. The temporary pinned component-build adapter patch
fixes Go GC host calls; Spin and the Go collector remain unchanged. Do not claim
a fixed memory ceiling or production readiness. Use a fresh prefix because the
old Git catalog format is incompatible.

Both hashes pass 1,025 ordinary pushes with automatic cleanup and cold history
recovery, shallow/deepen/unshallow, annotated tags and fetch visibility checks.
The full provider suite also passes with frequent Go GC. A parallel long run
hit one transient MinIO HTTP protocol error before a push; the unchanged SHA-1
rerun passed. No concrete runtime bug or reason for a Spin patch was found.
Follow `examples/git/README.md`; use ordinary Spin and isolated local MinIO. No remote
deployment or upstream posts are authorized. Do not restart shared Docker.

Use exclusive worktrees; root alone integrates main. Preserve sparse reads,
explicit uncertain outcomes and cumulative retry counters. Keep reports short;
verification belongs in tests and Git history rather than new evidence archives.
After Git settles, return to the KV design scoped in issue #39.
