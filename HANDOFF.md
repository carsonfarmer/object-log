# object-log handoff

Build a small generic object-storage WAL, using a useful Git service to prove
its API. Domain rules stay outside the core; the conditional head is the only
mutable durable authority. Read AGENTS.md and GIT_PLAN.md.

The accepted implementation is `examples/git` (go-git consumer) and
`examples/wal-component` (Rust WAL bridge). The custom Rust Git engine and native
maintenance service are removed. Installed Git remains the independent oracle.
KV, SQLite and the Go WAL experiment are deferred. Do not resume them.

## Current behavior

Both hashes support ordinary v2 clone/have-aware fetch, classic push, shallow
history, tags, authentication, read-only serving and cold recovery. Ref updates
and the sparse catalog publish atomically. Branches require fast-forward updates;
partial filters and packfile URIs are deferred.

The WAL owns authenticated chunking and sparse byte reads. Git stores small
compressed objects in catalog leaves and streams larger objects. Incoming delta
bases/results stream through go-git; outgoing packs use full objects because its
delta selector does not bound bytes. At 64 tail entries, push admission
checkpoints the current authenticated catalog without walking Git history or
deleting objects. The maintenance endpoint prunes unreachable Git objects and
runs one bounded collection batch. Operators run it periodically and after ref
deletion, repeating `more` until `complete` and retrying `pending` or `conflict`;
`retained` means a reader still blocks collection. Collection retains the
existing fencing and uncertain-outcome protocol.

Full-object outgoing packs do not change Git correctness or negotiation: have-aware
fetch still omits objects the client already has. They can make clones and fetches
larger and slower, increase network-egress cost, and reduce concurrency when network
bandwidth is the bottleneck. Repositories with many similar revisions of large files
are most affected. They avoid the memory and CPU cost of generating deltas. Live
qualification must measure network egress for its own repository mix.

Request limits cover input bytes, object and metadata sizes, pack entry counts,
catalog decoding and cooperative deadlines. They are not a process-memory limit.
Retries retain cumulative counters and decoding charges. A complete-object read
may make three fresh core attempts after generic storage failures; each is
admitted and counted. Spin may make two HTTP attempts per core attempt, for six
at most. An expired view can reopen once before output. A single identical
conditional storage PUT may retry after a connection failure; a rejected replay
preserves the first uncertain outcome. Git pushes never replay.

## Dependencies and operation

Use ordinary Spin and unmodified local MinIO. No instance, pooling or memory
wrapper. go-git is pinned to our fork at `a37a9c5b`. It contains the streamed
parser work in [go-git PR #2379](https://github.com/go-git/go-git/pull/2379)
plus small receive-pack, empty SHA-256 advertisement and deterministic first-ref
fixes retained only on our fork pending owner review.
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

Issue #43 records the completed local qualification. Workspace/native/WASIp2 gates,
the full core MinIO matrix, 1,025 mixed-history pushes per hash with concurrent
fetch/fsck, malformed-input rejection, access controls, small configured limits,
concurrent writes/reads/collection and forced-restart recovery have passed.
Concurrent 513 MiB push/clone/edit/fetch lifecycles pass for both hashes, with
exact contents and native Git integrity checks.
The earlier native-client rejection was traced outside the service. In installed
Git 2.54, detached maintenance drops its lock while work continues. Concurrent
cleanup can then treat an unfinished pack as durable, remove a loose object, and
lose the only copy when that unfinished pack is discarded. Two deterministic
native tests fail on the installed binary and pass with a small local Git fix;
the current upstream source still has both paths. The endurance test sets
`maintenance.autoDetach=false`, preserving normal maintenance while avoiding
that client-side race. No Git patch has been submitted upstream.
Failure artifacts remain available with `go test -artifacts`.
The full loopback-Spin/live AWS S3 qualification passed at exact runtime revision
`57643eb6b155811f39d990fe8379964d3dcc4c6d` with composed component SHA-256
`e239c0234c3b9a2af4d709a1ce9c6d3997b4415e82df3a0e80063363947ee6bc`.
All phases completed in 6,319 seconds, including mature maintenance for both
hashes and concurrent 513 MiB lifecycles. Sampled Spin trigger RSS peaked at
1,562,608 KiB during those performance workloads; this observation is not a
fixed memory ceiling. Exact-prefix and infrastructure teardown left no objects,
versions, markers or multipart uploads. Terraform state was empty, and the
bucket, user, local config, credentials, processes and listeners were absent.
The reusable workflow is in `examples/git/qualification/aws`.
Deployed HTTPS, host admission and TLS remain for the chosen production host.

Root alone integrates main. Implement in exclusive worktrees, request independent
correctness/simplification reviews, and run applicable gates before integration.
Keep the generic WAL small; new core APIs need concrete consumer benefit.
