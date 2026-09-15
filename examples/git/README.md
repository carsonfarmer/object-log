# Git on object-log

A Go Git service backed by the existing Rust WAL. go-git handles Git protocols,
formats and packs; the sibling Rust component provides authenticated object
storage, atomic publication, checkpoints and garbage collection. Refs and the
sparse object catalog share one WAL head. No local repository cache is needed.
Local Spin/MinIO qualification passes. Remote provider and deployment testing
remain before a production rollout. Development pins our go-git fork at
`a37a9c5b` through `go.mod`. Its v6 APIs provide both hashes, protocol-v2 serving,
shallow history and streamed object writes. The streaming work is represented
by [go-git PR #2379](https://github.com/go-git/go-git/pull/2379); additional
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
  checks, automatic cleanup, and final cold history and byte verification. Failed
  writer clients and packet diagnostics are retained with `-artifacts`. Each
  client runs its own maintenance synchronously before subsequent commands. This
  avoids an independently reproduced lock bug in installed Git 2.54 while still
  exercising client maintenance. Run with
  `go test -race -artifacts ./tests -run '^TestRepeatedPushes$' -count=1 -parallel=4 -v -timeout=20m`.
  It reports client latency percentiles in 256-push windows, including negotiation,
  transfer and cleanup. Use an isolated prefix and keep competing workloads off
  the host when measuring. These timings do not include component compilation.
- `GIT_LARGE_OBJECT_MIB=513`: larger push/clone/edit/fetch lifecycle.
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

Use positive byte/count values and a positive duration. Blobs stream; structured
objects need the smaller decoding limit. Pack counts are checked before entry
allocation or decoded-object writes. The incoming pack is staged temporarily
first. Catalog cache hits are free, and refreshed stores retain the request's
charges. WAL envelopes, object proofs and Go allocation overhead are additional;
these settings do not promise a total-process memory ceiling.

Cancellation is checked between storage calls and before publication. An
already-running synchronous WASI call must finish; its publication outcome is
preserved even after the deadline. Delta bases, instructions and results stream;
backward copies can reread a base. A recorded read failure stops further pack
output, including when a library traversal suppresses the original error.

On local Spin 4.0.2/MinIO, three alternating, sequential 64 MiB lifecycle
comparisons reduced median peak worker RSS from 570 to 321 MiB with streaming.
Time and storage traffic were similar. RSS was sampled every 100 ms, excluding
builds and MinIO/client processes; this is a workload measurement, not a bound.

Automatic checkpointing now runs after 64 tail entries. Sequential comparisons
of 1,025 mixed-history pushes per hash, with concurrent readers, used 478,139
storage calls and 2.17 GB versus 720,654 calls and 1.91 GB at 128 entries.
For the same 1,027 receive requests per hash, calls averaged 80 instead of 108;
more frequent cleanup increases transferred bytes. Reader request counts varied,
and shared-host timings are observational. The tests check exact cold history
and file contents, not only throughput. See issue #6 for broader benchmark work.

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
Use the recorded least-privilege STS role. The runner requires a dedicated
bucket with versioning disabled, no lifecycle rules, reviewed default
encryption, and an empty campaign prefix. Its policy needs bucket configuration
reads and list/list-versions on that prefix, plus get, put and delete object
access beneath the prefix; it must not grant access to production data.

Run `make git-remote-qualification REMOTE_QUALIFICATION_PHASE=...` with these
phases in order: `start`, `backend`, `protocol`, `standard`, `recovery`,
`read-only`, `limits`, `performance`, and `teardown`. Restart the unchanged
standard profile with a new boot ID before `recovery`; redeploy the read-only
and limits profiles before their phases; then restore standard before
performance. Set `GIT_PROBE_BOOT_ID` to the active profile's `git_boot_id`.
`TestAccess` checks the response boot ID and the service-computed fingerprint
of the endpoint, region, bucket and exact profile prefix. Recovery requires the
saved standard boot ID to change.

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
```

For local Spin connected to live S3, pass only that protected path:

```sh
spin up --listen 127.0.0.1:19100 --variable @/absolute/path/qualification-spin.toml
```

For recovery, change `git_boot_id` and restart or redeploy. The read-only
profile sets `git_read_only = "true"`. The limits profile uses the `git-limits`
prefix and sets push=131072, negotiation=4096, and object=65536. Apply the same
variables through the deployment host's secret/config facility.

The runner enforces the 08:00–20:00 Pacific window, credential and campaign
deadlines, one campaign per day, ordered tests, failure lockout, exact-prefix
teardown, and a zero residual check. It writes one concise log per phase in the
protected state directory, including the frozen plan digest, timestamps,
status, and start-time tool versions. The configured campaign duration must fit
entirely before 20:00 Pacific. Review request counts and provider cost at each
manual phase stop using the declared counter source; no counter API is wired,
so the runner cannot enforce those two ceilings. The repeated-history test reports p50/p95/p99 client
latency and durable push throughput. A loopback Git URL qualifies live S3
behavior; only a deployed HTTPS URL adds inbound TLS, authentication and host
admission evidence. Teardown has its own deadline at credential expiry, so it
remains available after the campaign deadline. After a failed phase, inspect
the phase log, run `teardown`, and have the owner run `review`; that persists a
`failed-reviewed` state. A later campaign cannot start while a failed campaign
still needs teardown or this explicit review.

## Cleanup and limits

Push admission checkpoints after 64 tail entries, including safe cleanup. Send an authenticated
`POST /sha1.git/maintenance` (or `/sha256.git/maintenance`) to prune unreachable
objects and collect one bounded batch. Repeat `more` until `complete`; retry
`pending` or `conflict` with a fresh request. `retained` means a WAL retention
blocks collection. Counts are deletion candidates, not unique deleted objects.
Maintenance still walks reachable history. Unchanged catalog nodes reuse their
original proofs and maps; filtering copies maps only when needed.

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
change. Remote qualification must measure response-pack bytes, latency and throughput
on representative histories. This does not establish a fixed process-memory ceiling.
Ordinary large-file lifecycles
have passed at 16, 64 and 513 MiB for both hashes on local Spin/MinIO.
Both hashes pass shallow clone, deepen, unshallow, annotated tags and 1,025
consecutive pushes with automatic cleanup and cold recovery. Partial filters
and packfile URIs are not replacement requirements.

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
