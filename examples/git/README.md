# Git on object-log

A Go Git service backed by the existing Rust WAL. go-git handles Git protocols,
formats and packs; the sibling Rust component provides authenticated object
storage, atomic publication, checkpoints and garbage collection. Refs and the
sparse object catalog share one WAL head. No local repository cache is needed.
Local Spin/MinIO qualification passes. Issue #10 tracks fresh
loopback-Spin/live AWS S3 qualification for the current revision. Deployed HTTPS
remains a hosting qualification before public rollout. Development pins our
go-git fork at `a37a9c5b` through `go.mod`. Its v6 APIs provide both hashes,
protocol-v2 serving, shallow history and streamed object writes. The streaming
work is represented by [go-git PR #2379](https://github.com/go-git/go-git/pull/2379); additional
small server fixes remain only on our fork pending owner review. Partial-clone filters
remain deferred.

## Build and run locally

Install Go 1.26.3, the repository's pinned Rust toolchain, Spin, `wac`, MinIO and
its `mc` client. From the repository root:

```sh
rustup target add wasm32-unknown-unknown wasm32-wasip2
make git-check
make git-build
```

The Go Makefile generates bindings and builds the component. Dependencies and
the build adapter are pinned; generated files and binaries are ignored. The
component reads its mapped application variables through the standard
`wasi:config/store` interface supplied by Spin 4.
Start a disposable local MinIO instance in another terminal:

```sh
MINIO_ROOT_USER=objectlog MINIO_ROOT_PASSWORD=local-test-secret \
  minio server /tmp/object-log-git-minio --address 127.0.0.1:19090
mc alias set local-git http://127.0.0.1:19090 objectlog local-test-secret
mc mb --ignore-existing local-git/wal-proof
```

Then create a protected Spin 4 variables file and use a new prefix and boot ID
for each isolated test run:

```sh
cat >/tmp/object-log-git-vars.toml <<'EOF'
wal_prefix = "your-fresh-test-prefix"
wal_access_key = "objectlog"
wal_secret_key = "local-test-secret"
git_boot_id = "local-boot-1"
EOF
chmod 600 /tmp/object-log-git-vars.toml
cd examples/git
spin up --listen 127.0.0.1:19100 --variable @/tmp/object-log-git-vars.toml
```

Repositories are `/sha1.git` and `/sha256.git`. The manifest defaults to the
local endpoint, bucket and region. Put `wal_endpoint`, `wal_bucket`, and
`wal_region` in the same file to override all three; the endpoint also narrows
the outbound allowlist. The file accepts `wal_session_token`, `git_password`,
`git_read_only`, and `wal_default_branch`. Its credential variables are marked
secret in the manifest. Keep the file mode at 0600 and pass only its path to
Spin.
Authentication is off by default; keep this configuration on loopback.
Branches require fast-forward updates; force and force-with-lease cannot rewrite them.
Use a fresh prefix: previous chunk-list objects, retired custom catalogs and
unvalidated experimental roots are unsupported. Catalogs contain inline objects
and WAL byte streams.

## Test

Run `make git-spin-config-test` from the repository root to build the component
and verify Spin passes the boot ID, storage target and password to the guest.

From the root, against the running local host:

```sh
GIT_PROBE_URL=http://127.0.0.1:19100 make git-provider-test
```

Set matching `GIT_PROBE_PASSWORD` and `GIT_PROBE_BRANCH` when configured. If the
host drops HTTP counter trailers, set `GIT_PROBE_LOG` to the absolute
`examples/git/.spin/logs/git_stderr.txt` path. The log exposes the same cumulative
storage counters as the trailers.

The tests use installed Git as an independent oracle. Opt-in extensions:

- `GIT_REPEATED_PUSHES=1`: 1,025 pushes per hash mixing text edits, sparse edits to a
  1 MiB binary, and binary additions/deletions, with concurrent fetch/integrity
  checks, automatic tail checkpointing, and final cold history and byte
  verification. Failed writer clients and packet diagnostics are retained with
  `-artifacts`. Each
  client runs its own maintenance synchronously before subsequent commands. This
  avoids an independently reproduced lock bug in installed Git 2.54 while still
  exercising client maintenance. Run with
  `go test -race -artifacts ./tests -run '^TestRepeatedPushes$' -count=1 -parallel=4 -v -timeout=20m`.
  It reports client latency percentiles in 256-push windows, including
  negotiation, transfer and tail checkpointing. Use an isolated prefix and keep
  competing workloads off the host when measuring. These timings do not include
  component compilation.
- `GIT_LARGE_OBJECT_MIB=513`: larger push/clone/edit/fetch lifecycle.
- `GIT_COLLECTION_CAPACITY=1`: against a fresh host configured with
  `wal_max_collection_objects=32`, grows each repository to its authenticated
  physical-object boundary, verifies the rejected push does not change the ref,
  then completes maintenance and a cold clone.
- `GIT_COLD_CATALOG=1`: against a fresh host configured with
  `git_max_catalog_bytes=16384`, creates a split catalog bucket and proves a
  cold update reads only its changed path and publishes the requested tip.
- `GIT_CONCURRENT_LARGE=1`: overlap that lifecycle for both hashes; use
  `go test ./tests -run '^TestLargeBlob$' -parallel=2 -count=1 -v`.
- `GIT_PROBE_PERSISTED_HEAD=true`: run `TestPersistedHead` after restarting Spin
  with the same prefix to verify saved default-branch recovery.
- `GIT_FAILURE_DRILLS=prepare GIT_DRILL_STATE=/tmp/git-drill-state.json`: run
  `go test -race ./tests -run '^TestFailureDrills$' -count=1 -parallel=4` from this directory
  with `GIT_PROBE_URL` set. It checks concurrent writers, readers, collection and interrupted
  pushes. Stop and restart Spin with the same prefix, then rerun with
  `GIT_FAILURE_DRILLS=verify` to check the saved expectations after recovery.

## Request limits

Each request has explicit limits. Invalid settings fail closed.

| Setting | Default | What it limits |
| --- | --- | --- |
| `git_max_push_bytes` | 2 GiB | Incoming push body |
| `git_max_negotiation_bytes` | 8 MiB | Negotiation, push commands and expanded gzip |
| `git_max_object_bytes` | 1 GiB | Each decoded Git object |
| `git_max_metadata_bytes` | 16 MiB | Each commit, tree or tag |
| `git_max_pack_objects` | 1,000,000 | Entries declared by an incoming pack |
| `git_max_catalog_bytes` | 64 MiB | Catalog bucket JSON decoded across the request |
| `git_request_timeout` | `5m` | Cooperative request deadline |
| `wal_max_collection_objects` | 100,000 | Physical WAL objects retained by one repository, including its checkpoint |
| `wal_recover_retentions_after_drain` | `false` | Exclusive recovery mode for IDs lost by stopped instances |

Use positive byte/count values and a positive duration. Blobs stream; structured
objects need the smaller decoding limit. Pack counts are checked before entry
allocation or decoded-object writes. The incoming pack is staged temporarily
first. Catalog cache hits are free, and refreshed stores retain the request's
charges. WAL envelopes, object proofs and Go allocation overhead are additional;
these settings do not promise a total-process memory ceiling.

`git_max_pack_objects` limits one incoming pack. Separately,
`wal_max_collection_objects` limits the physical graph retained by one
repository. Streamed objects can own several WAL chunks, so these counts are not
interchangeable. The WAL reports a finished stream's storage-object count and
the authenticated Git catalog rolls it up without knowing the chunk geometry.
An update that would leave no room for the checkpoint required by collection is
rejected before publication. The collection limit is durable, and changing it
requires a fresh prefix. This count-aware catalog also requires a fresh prefix;
earlier development catalogs are rejected rather than migrated.

Cancellation is checked between storage calls and before publication. An
already-running synchronous WASI call must finish; its publication outcome is
preserved even after the deadline. Delta bases, instructions and results stream;
backward copies can reread a base. A recorded read failure stops further pack
output, including when a library traversal suppresses the original error.

On local Spin 4.0.2/MinIO, three alternating, sequential 64 MiB lifecycle
comparisons reduced median peak worker RSS from 570 to 321 MiB with streaming.
Time and storage traffic were similar. RSS was sampled every 100 ms, excluding
builds and MinIO/client processes; this is a workload measurement, not a bound.

Automatic checkpointing runs after 64 tail entries. It checkpoints the current
authenticated catalog without walking Git history or deleting objects. The
1,025-push test crosses this boundary repeatedly and checks exact cold history
and file contents. `TestMaintenance` separately exercises Git pruning and bounded
WAL collection on the mature repositories. See issue #6 for broader benchmark
work.

Fetch visibility checks current ref trees before older history, deduplicates
shared work and stops when wants and relevant haves are proven. The sparse blob
fetch remains subject to the unchanged 768 KiB read budget after mature history.
Run the full provider suite with `GIT_REPEATED_PUSHES=1` on a fresh prefix to
exercise that combined check. Shared-parent traversal allocated 10.70 MB instead
of 18.94 MB in the retained 128-tip/256-parent benchmark.

Host-wide concurrent-request admission is a hosting concern and remains deferred;
this example adds no instance limiter or additional durable coordination.

A complete object read may be restarted by the core at most twice after its
initial attempt. For each core attempt, the Spin HTTP adapter may retry one
bodyless request, allowing at most six HTTP attempts for one logical read. Every
core admission and HTTP attempt remains cumulative. A rejected conditional-write
retry preserves the first uncertain outcome for WAL recovery; it cannot turn a
lost success into a definite conflict. Git pushes are never replayed.

To check small limits, start a fresh-prefix host with push=131072,
negotiation=4096 and object=65536, then run
`GIT_PROBE_LIMITS=1 GIT_PROBE_URL=http://127.0.0.1:19100 go test ./tests -run TestConfiguredLimits`
from this directory. The ordinary suite uses the defaults.

## Live S3 qualification

`make git-remote-rehearse` prints the complete offline sequence. For a live
campaign, copy `remote-qualification.env.example` outside the repository, fill
in its non-secret record, source it, and export fresh `AWS_ACCESS_KEY_ID`,
`AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`, and `GIT_PROBE_PASSWORD` values.
The reusable [AWS qualification setup](qualification/aws) provisions the
dedicated bucket and least-privilege identity and issues temporary credentials
with a four-hour expiry ceiling. Their values do not enter Terraform state. The
runner
requires a dedicated bucket with versioning disabled, no lifecycle rules,
reviewed default encryption, and an empty campaign prefix. Its policy needs
bucket configuration reads and list/list-versions on that prefix, plus get, put
and delete object access beneath the prefix; it must not grant access to
production data.

Run `make git-remote-qualification REMOTE_QUALIFICATION_PHASE=...` with these
phases in order: `start`, `backend`, `protocol`, `standard`, `recovery`,
`read-only`, `limits`, `performance`, and `teardown`. Restart the unchanged
standard profile with a new boot ID before `recovery`; redeploy the read-only
and limits profiles before their phases; then restore standard before
performance. Set `GIT_PROBE_BOOT_ID` to the active profile's `git_boot_id`.
`TestAccess` checks the response boot ID and the service-computed fingerprint
of the endpoint, region, bucket and exact profile prefix. Recovery requires the
saved standard boot ID to change. The performance phase runs `TestMaintenance`
to completion on both mature repositories after repeated pushes and before the
concurrent 513 MiB lifecycles.

Standard, recovery, read-only and performance use
`wal_prefix=$GIT_QUALIFICATION_PREFIX/git`; limits uses the fresh
`$GIT_QUALIFICATION_PREFIX/git-limits` prefix. Every profile sets the recorded
endpoint, bucket and region, all three temporary credential values, and
`git_password`. Read-only additionally sets `git_read_only=true`. Limits sets
`git_max_push_bytes=131072`, `git_max_negotiation_bytes=4096`, and
`git_max_object_bytes=65536`. Keep `wal_default_branch` equal to
`GIT_PROBE_BRANCH` across the standard restart.

Create a 0600 Spin 4 TOML variables file outside the repository containing:

```toml
wal_endpoint = "https://s3.REGION.amazonaws.com"
wal_bucket = "DEDICATED-BUCKET"
wal_region = "REGION"
wal_prefix = "CAMPAIGN-PREFIX/git"
wal_access_key = "TEMPORARY-ACCESS-KEY"
wal_secret_key = "TEMPORARY-SECRET-KEY"
wal_session_token = "TEMPORARY-SESSION-TOKEN"
wal_default_branch = "main"
git_password = "TEMPORARY-GIT-PASSWORD"
git_boot_id = "standard-boot-1"
git_read_only = "false"
git_max_push_bytes = "2147483648"
git_max_negotiation_bytes = "8388608"
git_max_object_bytes = "1073741824"
git_max_metadata_bytes = "16777216"
git_max_pack_objects = "1000000"
git_max_catalog_bytes = "67108864"
git_request_timeout = "5m"
wal_max_collection_objects = "100000"
wal_recover_retentions_after_drain = "false"
```

For local Spin connected to live S3, pass only that protected path:

```sh
spin up --listen 127.0.0.1:19100 --variable @/absolute/path/qualification-spin.toml
```

Set `GIT_PROBE_LOG` to the absolute `.spin/logs/git_stderr.txt` path for this
loopback qualification so the runner can read Spin's cumulative WAL counters.

For recovery, change `git_boot_id` and restart or redeploy. The read-only
profile sets `git_read_only = "true"`. The limits profile uses the `git-limits`
prefix and sets push=131072, negotiation=4096, and object=65536. Apply the same
variables through the deployment host's secret/config facility.

The runner enforces credential expiry and a three-hour campaign safety ceiling,
ordered tests, failure lockout, exact-prefix teardown, and a zero residual check.
Tests start immediately; there is no soak or scheduled run window. It writes one
concise log per phase in the protected state directory, including the frozen
plan digest, timestamps, status, and start-time tool versions. Review request
counts and provider cost at each manual phase stop using the declared counter
source; no counter API is wired, so the runner cannot enforce those two
ceilings. The repeated-history test reports p50/p95/p99 client latency and
durable push throughput. A loopback Git URL qualifies live S3
behavior; only a deployed HTTPS URL adds inbound TLS, authentication and host
admission evidence. Teardown remains available until credential expiry. After a
failed phase, inspect
the phase log, run `teardown`, and have the owner run `review`; that persists a
`failed-reviewed` state. A later campaign cannot start while a failed campaign
still needs teardown or this explicit review.

## Cleanup and limits

Push admission checkpoints the current authenticated catalog after 64 tail
entries. This bounds the WAL tail; it does not prune Git objects or collect old
storage. Operators must send an authenticated `POST /sha1.git/maintenance` and
`POST /sha256.git/maintenance` periodically and after ref deletion. Each request
prunes unreachable Git objects and collects one bounded batch. Repeat `more` until
`complete`; retry `pending` or `conflict` with a fresh request. `retained` means a
WAL retention blocks collection. Counts are deletion candidates, not unique
deleted objects. Unchanged catalog nodes reuse their original proofs and maps;
filtering copies maps only when needed.

Every upload-pack request acquires WAL retention before reading its catalog and
releases it after its last response write. If an instance ends before confirming
release, stop ingress and wait for every reader to finish. Then start one
authenticated instance with `wal_recover_retentions_after_drain = "true"`.
This exclusive mode rejects Git and normal maintenance requests. Send
authenticated `POST` requests to `/sha1.git/recover-retentions-after-drain` and
`/sha256.git/recover-retentions-after-drain`, stop that instance, disable the
setting, and resume service. Never use this action while a reader may still be
running.

Retention acquire and release can overlap a receive-pack publication without
rejecting the single Git writer. The WAL preserves the current retention set
while publishing against the same logical repository state; competing Git or
collection updates remain conflicts.

Compressed loose objects of at most 512 bytes live directly in authenticated
catalog leaves, avoiding separate reads during history traversal and collection.
Larger objects and temporary incoming packs use the WAL byte-stream API, which owns
chunk geometry, authenticated reconstruction and offset reads. Git retains its
splitting sparse index and compression. Incoming packs remain unpublished;
go-git checks and decodes them while a small importer coordinates dependency
resolution and streams decoded objects into the WAL. Fetch streams full objects
without making deltas, trading larger transfers for lower memory and CPU use.
Have-aware negotiation still omits objects the client already has, so this changes
the encoding of required objects rather than fetch correctness. Clones and fetches
can be slower and incur more network-egress cost, and bandwidth limits can reduce
concurrent throughput. Repositories with many similar revisions of large files are
most affected; already-compressed or substantially different files may see little
change. Live qualification records response behavior, latency and throughput on
its representative histories. This does not establish a fixed process-memory
ceiling or predict every repository's egress.
Ordinary large-file lifecycles
have passed at 16, 64 and 513 MiB for both hashes on local Spin/MinIO.
Both hashes pass shallow clone, deepen, unshallow, annotated tags and 1,025
consecutive pushes with automatic tail checkpointing and cold recovery. Partial
filters and packfile URIs are not replacement requirements.

`go test ./tests -run TestDeltaEncoderGitCompatibility` checks go-git's full and
delta encoders against Git; `go test ./tests -run '^$' -bench
BenchmarkOutgoingDeltas -benchmem` demonstrates the size and allocation tradeoff.
These tests compare library behavior; the service uses full-object pack entries.
go-git's HTTP upload-pack path does not expose a byte-bounded delta selector, and
the service does not carry a private transport hook for one.
Conditional S3 uploads use `Expect: 100-continue` so early rejections can
advertise connection closure. This avoids reusing MinIO connections whose
request bodies were not consumed; successful connections remain reusable.
See [issue #41](https://github.com/carsonfarmer/object-log/issues/41).

`make build` builds the adapter from a checksum-verified commit in our Wasmtime
fork, containing [the fix under review](https://github.com/bytecodealliance/wasmtime/pull/14319).
It handles GC clock and immediate timer calls during canonical allocation.
There is no local patch file; Spin and Go remain unchanged. Return to a standard
adapter once it passes the retained regression and frequent-GC tests.
`GODEBUG=gctrace=1` can itself call the host during canonical allocation; sample
host RSS instead. No Spin pooling or memory-limit wrapper is used.

## Live S3 evidence

Issue #10 tracks the current live S3 campaign. Do not claim remote qualification
until the full ordered run, mature-repository maintenance, and exact-prefix
teardown pass at the current revision. A public host still needs its own HTTPS,
authentication, routing and host-wide admission tests.
