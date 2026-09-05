# object-log handoff

The product is a small, powerful, generic object-storage WAL. A fully usable Git
service proves its API. Keep domain rules outside the core and the conditional
head as the only mutable durable authority. Read AGENTS.md and GIT_PLAN.md.

## Current work

The Git proof now supports explicit authenticated catalog migration, sparse
selected-pack lookup, persisted default branches, cumulative request guards,
bounded head/collection decoding, and installed-collection resumption. Native
operator and WASI transports share one safe-read retry policy; native reqwest
retains its existing protocol-level retry behavior. Writes retain pending-result
semantics. The native Git engine and HTTP host are removed; installed Git remains
the independent oracle.

Use workspace/native/WASIp2 checks and ordinary Spin with isolated local MinIO.
Check hosted CI after each push. Verification belongs in tests and commits.

## Next implementation

- Authenticated packfile URIs are implemented, including range resume and cold
  recovery. Preserve this path while adding catalog and streaming consumers.
- Integrate reviewed live-pack compaction and its operator command, then prove
  repeated pushes, compaction, checkpoint/GC, and cold Git recovery.
- Finish bounded streaming receive AND fetch/clone for the capacity target:
  at least 50 MiB files and 1 GiB pushes. Do not accept an unusable push-only cap.
- Continue the GitHub queue, especially #19, #23, #25, #26 and #32.

Use exclusive implementation worktrees; root alone integrates main. Keep useful
regression tests, sparse authenticated reads, cumulative retry counters, recovery,
GC and provider coverage. Use ordinary Spin: no forced instance/pooling/memory
wrapper and no Spin patches unless absolutely necessary. Do not add evidence
archives or reports; record verification in tests, commits and issues.

No upstream communications or repository links are authorized. The old Spin
issue was withdrawn; do not recreate it. Docker was unresponsive and restart
approval was requested; do not restart the shared service without a reply.
