# object-log handoff

Build a small generic object-storage WAL, using a useful Git service to prove
its API. Domain rules stay outside the core; the conditional head is the only
mutable durable authority. Read AGENTS.md and GIT_PLAN.md.

The accepted implementation is `examples/git` (go-git consumer) and
`examples/wal-component` (Rust WAL bridge). The custom Rust Git engine and native
maintenance service are removed. Installed Git remains the independent oracle.
KV, SQLite and the Go WAL experiment are deferred. Do not resume them or perform
remote provider testing without owner direction.

## Current behavior

Both hashes support ordinary v2 clone/have-aware fetch, classic push, shallow
history, tags, authentication, read-only serving and cold recovery. Ref updates
and the sparse catalog publish atomically. Branches require fast-forward updates;
partial filters and packfile URIs are deferred.

The WAL owns authenticated chunking and sparse byte reads. Git stores small
compressed objects in catalog leaves and streams larger objects. Incoming delta
bases/results stream through go-git; outgoing packs use full objects because its
delta selector does not bound bytes. Automatic checkpoint/cleanup runs at 64 tail
entries. Collection retains the existing fencing and uncertain-outcome protocol.

Request limits cover input bytes, object and metadata sizes, pack entry counts,
catalog decoding and cooperative deadlines. They are not a process-memory limit.
Retries retain cumulative counters and decoding charges. An expired read can
reopen once before output; a recorded storage failure stops further output.
A single identical conditional storage PUT may retry after a connection failure;
a rejected replay preserves the first uncertain outcome. Git pushes never replay.

## Dependencies and operation

Use ordinary Spin and unmodified local MinIO. No instance, pooling or memory
wrapper. go-git is pinned to our streaming-fix fork at `6060178b` (upstream #2379).
The adapter builds directly from our reviewed Wasmtime commit
`c8e24c308754f784fbb4a08205a2a9c08c461d00` (upstream #14319), with an archive
checksum; the redundant local patch is removed. componentize-go remains upstream
0.4.2; its draft #78 regression awaits the adapter fix. New upstream submissions
require owner review. Dependency provenance is in THIRD_PARTY.md.

Use a fresh storage prefix for incompatible development catalogs; do not add a
migration reader. See the Git README for build, client, large-file, failure and
restart commands and the tested operating envelope. Keep tests and concise
commits, not new evidence archives. Do not restart shared Docker or MinIO.

## Qualification

Issue #43 tracks the final local qualification. Workspace/native/WASIp2 gates,
the full core MinIO matrix, 1,025 mixed-history pushes per hash with concurrent
fetch/fsck, malformed-input rejection, access controls, small configured limits,
concurrent writes/reads/collection and forced-restart recovery have passed.
Concurrent 513 MiB push/clone/edit/fetch lifecycles pass for both hashes, with
exact contents and native Git integrity checks.
An intermittent native-client rejection occurred before upload. The retained
source could not read its newest commit; the server's advertised prior commit
was valid. A subsequent native HTTP control using Apple Git 2.54.0 lost an
acknowledged server commit during repeated pushes, without the WAL or moving
the server directory. Its pack files remain valid but omit the missing commit.
The exact cause and relationship between these failures remain unproven.
The repeated-push test waits for each client's own maintenance to finish before
subsequent commands and artifact relocation; maintenance remains enabled.
This workaround is not a fix. Local qualification remains open under issue #43.
Failure artifacts remain available with `go test -artifacts`.
Remote latency, provider behavior, deployment
security, aggregate admission and operational recovery still need remote testing.

Root alone integrates main. Implement in exclusive worktrees, request independent
correctness/simplification reviews, and run applicable gates before integration.
Keep the generic WAL small; new core APIs need concrete consumer benefit.
