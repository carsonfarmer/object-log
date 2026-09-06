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

Use exclusive worktrees; root alone integrates main. Preserve sparse reads,
exact recovery, cumulative retry counters and provider tests. Use ordinary Spin
with default runtime settings. Avoid Spin patches unless essential. Verification
belongs in tests, commits and concise issue updates, not new evidence archives.

No upstream communications or repository links are authorized. The old Spin
issue was withdrawn; do not recreate it. Do not restart the shared Docker
service without the owner's reply. Native MinIO is available for local testing.
