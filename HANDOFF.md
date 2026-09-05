# object-log handoff

The product is a small, powerful, generic object-storage WAL. A fully usable Git
service proves its API. Keep domain rules outside the core and the conditional
head as the only mutable durable authority. Read AGENTS.md and GIT_PLAN.md.

## Current work

The current batch includes persisted default branches, cumulative request guards,
bounded head/collection decoding, installed-collection resumption, and plain Spin
launches. One safe storage GET/HEAD retry handles typed interrupted responses;
writes are never retried by the transport. The previous native Git engine and
HTTP host are removed; installed Git remains the independent oracle.

Validation: 442 workspace tests pass, 21 opt-in ignored; strict native/WASIp2,
all seven separately run local provider targets, actual WASI lifecycle fixtures,
SQLite recovery and large memory/MinIO GC pass. Tests use ordinary Spin defaults
and isolated native MinIO. Hosted CI is checked after each push.

## Next implementation

- Authenticated packfile URIs are implemented, including range resume and cold
  recovery. Preserve this path while adding catalog and streaming consumers.
- Integrate the actual catalog Reader/publication consumers after the final
  replica-content checkpoint check. Capacity and catalog workers own this work.
- Finish bounded streaming receive AND fetch/clone for the capacity target:
  at least 50 MiB files and 1 GiB pushes. Do not accept an unusable push-only cap.
- Continue the GitHub queue, especially #19, #23, #24, #25, #26 and #32.

Use exclusive implementation worktrees; root alone integrates main. Keep useful
regression tests, sparse authenticated reads, cumulative retry counters, recovery,
GC and provider coverage. Use ordinary Spin: no forced instance/pooling/memory
wrapper and no Spin patches unless absolutely necessary. Do not add evidence
archives or reports; record verification in tests, commits and issues.

No upstream communications or repository links are authorized. The old Spin
issue was withdrawn; do not recreate it. Docker was unresponsive and restart
approval was requested; do not restart the shared service without a reply.
