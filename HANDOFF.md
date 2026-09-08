# object-log handoff

The product is a small, powerful, generic object-storage WAL. A fully usable Git
service proves its API. Keep domain rules outside the core and the conditional
head as the only mutable durable authority. Read AGENTS.md and GIT_PLAN.md.

The Git functional burndown is integrated at 160820f. Full local checks and
both-hash provider tests cover concurrent Spin clients, large pushes, shallow,
filtered and URI histories, compaction and cold recovery. Collection now drains
stale backlogs in bounded batches; repeat until empty. The live graph and
operation budgets remain finite.

The owner rejected the Git implementation's size (~11,883 production Rust
lines) and the previous reduction pass as insufficient. Functional coverage does
not establish a successful simplicity proof. Stop feature expansion. The current
acceptance target is to remove at least half the Git implementation through
established libraries and architectural simplification, preserving client
behavior. Investigate WAL API friction as well as Git-specific duplication.
Do not meet the target by moving code, compressing formatting, or cutting tests.
If library substitution cannot meet it, explain concrete, tested incompatibilities
and the remaining choices. Existing implementation remains the regression baseline.
KV work waits behind this correction.

The smaller Go consumer is on `cf/git-architecture-reduction` under
`experiments/git-go`, using the unchanged Rust WAL through `wal-component`.
Roughly 1,900 production/interface/config lines now cover basic service policy,
compressed loose objects, splitting sparse indexes and seekable incoming packs.
The owner prioritizes ordinary complete Git over copying rarely used extensions.
The runtime crash is fixed by a small local component-adapter patch, built from
pinned, checksum-verified upstream source. It pauses both clock imports and handles
the immediate timer poll used by Go GC. Spin and Go's collector are unchanged.
The full experiment provider suite passes both hashes, including the 2 MiB clone
regression, with default GC and GOGC=1 stress. Strict adapter Clippy and core
memory/filesystem conformance pass. The reproduction remains in provider tests.
Storage cleanup now uses the existing WAL checkpoint and fenced collection APIs:
POST the repository's `/maintenance` endpoint until complete. Independent review,
focused tests, native/WASIp2 Clippy and the workspace gate pass. The provider suite
covers retained history and unreachable-object pruning for both hashes. Ordinary
16, 64 and 513 MiB push/clone/edit/fetch lifecycles pass both hashes on local
Spin/MinIO with unchanged Git client settings. Fetch
streams full objects instead of building outgoing deltas; incoming delta bases
and results still need full buffers. The exact 512 MiB fsck failure is a native
Git threshold-equality bug, confirmed by matching content hashes and successful
streaming verification with the threshold one byte lower.
Do not integrate this branch as the complete replacement yet. Expired-view retry,
fetch visibility policy and large-delta memory remain; see the experiment README.
Fetch currently accepts known unreachable IDs within the authenticated repository
until pruning. Use public library codecs for any policy fix and preserve the
ref-tip sparse fast path; do not add a whole-history walk to every fetch.
No upstream post or remote deployment occurred.

Use exclusive worktrees; root alone integrates main. Preserve sparse reads,
exact recovery, cumulative retry counters and provider tests. Use ordinary Spin
with default runtime settings. Avoid Spin patches unless essential. Verification
belongs in tests, commits and concise issue updates, not new evidence archives.

No upstream communications or repository links are authorized. The old Spin
issue was withdrawn; do not recreate it. Do not restart the shared Docker
service without the owner's reply. Native MinIO is available for local testing.
