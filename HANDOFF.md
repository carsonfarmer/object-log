# object-log handoff

The product is a small, powerful, generic object-storage WAL. A fully usable Git
service proves its API. Keep domain rules outside the core and the conditional
head as the only mutable durable authority. Read AGENTS.md and GIT_PLAN.md.

The Git functional burndown is integrated at 160820f. Full local checks and
both-hash provider tests cover concurrent Spin clients, large pushes, shallow,
filtered and URI histories, compaction and cold recovery. Collection now drains
stale backlogs in bounded batches; repeat until empty. The live graph and
operation budgets remain finite.

The owner-requested Rust reduction and prose cleanup pass is complete.
The combined local gate and final provider checks pass. Git completion and
review results are tracked in #17 and #25. Scale #19, reliability #21 and
performance #23 are closed.
KV follow-on design is in #39; leave SQLite and verifiable KV for later.

Use exclusive worktrees; root alone integrates main. Preserve sparse reads,
exact recovery, cumulative retry counters and provider tests. Use ordinary Spin
with default runtime settings. Avoid Spin patches unless essential. Verification
belongs in tests, commits and concise issue updates, not new evidence archives.

No upstream communications or repository links are authorized. The old Spin
issue was withdrawn; do not recreate it. Do not restart the shared Docker
service without the owner's reply. Native MinIO is available for local testing.
