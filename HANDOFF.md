# object-log handoff

The product is a small, powerful, generic object-storage WAL. A fully usable Git
service proves its API. Keep domain rules outside the core and the conditional
head as the only mutable durable authority. Read AGENTS.md and GIT_PLAN.md.

## Current work

The Git proof now supports explicit authenticated catalog migration, sparse
selected-pack lookup, persisted default branches, cumulative request guards,
bounded head/collection decoding, and fresh or resumed operator collection. Native
operator and WASI transports share one safe-read retry policy; native reqwest
retains its existing protocol-level retry behavior. Writes retain pending-result
semantics. The native Git engine and HTTP host are removed; installed Git remains
the independent oracle.

Use workspace/native/WASIp2 checks and ordinary Spin with isolated local MinIO.
Check hosted CI after each push. Verification belongs in tests and commits.

## Next implementation

- Authenticated packfile URIs are implemented, including range resume and cold
  recovery. Preserve this path while adding catalog and streaming consumers.
- Live-pack compaction and its operator command preserve refs and symbolic HEAD;
  repeated push/checkpoint/GC cycles and cold Git recovery are tested. Extend
  this beyond the tested 1,100 file-changing pushes and 35 maintenance/cold-clone
  cycles per hash; compaction still traverses the live graph.
- Streaming receive supports 1 GiB blobs and 1,040 MiB incoming packs. Both-hash
  Spin/MinIO tests cover large-file push, clone/fetch and maintenance, plus cold
  clone combining three independent 720 MiB pushes into one pack over 2,080 MiB.
- The real object-log history fixture now passes push, cold clone, incremental
  update, migration, compaction/checkpoint/GC and final cold verification. Private
  encoded-chunk sharing and verified small-delta reuse reduce repeated reads
  and decoding without raising operation quotas or changing the core API.
- Continue the GitHub queue, especially #19, #21, #23, #25 and #26.

Use exclusive implementation worktrees; root alone integrates main. Keep useful
regression tests, sparse authenticated reads, cumulative retry counters, recovery,
GC and provider coverage. Use ordinary Spin: no forced instance/pooling/memory
wrapper and no Spin patches unless absolutely necessary. Do not add evidence
archives or reports; record verification in tests, commits and issues.

No upstream communications or repository links are authorized. The old Spin
issue was withdrawn; do not recreate it. Docker was unresponsive and restart
approval was requested; do not restart the shared service without a reply.
