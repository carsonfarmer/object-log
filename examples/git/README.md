# Git on object-log

A Go Git service backed by the existing Rust WAL. go-git handles Git protocols,
formats and packs; the sibling Rust component provides authenticated object
storage, atomic publication, checkpoints and garbage collection. Refs and the
sparse object catalog share one WAL head. No local repository cache is needed.
Local replacement checks pass; this is not production ready.
Development temporarily pins our go-git fork at `6060178b` through `go.mod`.
It includes the position and failed-reopen fixes under review in upstream PR #2379. Its v6 APIs provide both hashes, protocol-v2
serving, shallow history and streamed object writes. Partial-clone filters remain deferred.

## Build and run locally

Install Go 1.26.3, the repository's pinned Rust toolchain, Spin, `wac`, MinIO and
its `mc` client. From the repository root:

```sh
rustup target add wasm32-unknown-unknown wasm32-wasip2
make git-check
make git-build
```

The Go Makefile generates bindings and builds the component. Dependencies and
the build adapter are pinned; generated files and binaries are ignored.
Start a disposable local MinIO instance in another terminal:

```sh
MINIO_ROOT_USER=objectlog MINIO_ROOT_PASSWORD=local-test-secret \
  minio server /tmp/object-log-git-minio --address 127.0.0.1:19090
mc alias set local-git http://127.0.0.1:19090 objectlog local-test-secret
mc mb --ignore-existing local-git/wal-proof
```

Then start ordinary Spin, using a new prefix for each isolated test run:

```sh
cd examples/git
spin up --listen 127.0.0.1:19100 \
  --env WAL_PREFIX=your-fresh-test-prefix \
  --env WAL_ACCESS_KEY=objectlog --env WAL_SECRET_KEY=local-test-secret
```

Repositories are `/sha1.git` and `/sha256.git`. `spin.toml` contains the local S3
endpoint, bucket and region; edit its outbound host when changing the endpoint.
Use `--env GIT_PASSWORD=...` for HTTP Basic authentication (any username),
`--env GIT_READ_ONLY=true` to reject pushes and maintenance, and
`--env WAL_DEFAULT_BRANCH=...` for a new repository's persisted default branch.
Authentication is off by default; keep this configuration on loopback.
Branches require fast-forward updates; force and force-with-lease cannot rewrite them.
Use a fresh prefix: previous chunk-list objects, retired custom catalogs and
unvalidated experimental roots are unsupported. Catalogs contain inline objects
and WAL byte streams.

## Test

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
  checks, automatic cleanup, and final cold history and byte verification. Run with
  `go test -race ./tests -run '^TestRepeatedPushes$' -count=1 -parallel=4 -v -timeout=20m`.
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

Defaults are 2 GiB per push, 8 MiB per negotiation (including expanded gzip),
1 GiB per accepted object, and a five-minute request deadline. Override with
`GIT_MAX_PUSH_BYTES`, `GIT_MAX_NEGOTIATION_BYTES`, `GIT_MAX_OBJECT_BYTES` (positive
byte counts), and `GIT_REQUEST_TIMEOUT` (for example `2m`). Invalid settings fail
closed. Push command headers share the negotiation bound.

Cancellation is checked between storage calls and before publication. An
already-running synchronous WASI call must finish; its publication outcome is
preserved even after the deadline. Incoming object and delta-result sizes are
checked before decoding into storage. Base objects, delta instructions and results
stream through fixed-size buffers; pack metadata still grows with object count.
Backward copies can reread a base. These limits do not promise a process-memory ceiling.
On local Spin 4.0.2/MinIO, three alternating comparisons of the buffered importer
and streaming importer ran the 64 MiB push/clone/edit/fetch lifecycle sequentially
for both hashes, with fresh prefixes and warmed HTTP workers. Median peak worker
RSS fell from 570 to 321 MiB; elapsed time was 38.79 versus 39.39 seconds, with
834 storage requests in both cases and effectively unchanged transferred bytes.
RSS was sampled every 100 ms, excluding builds and MinIO/client processes.
The mixed-history test completed 2,050 updates with concurrent fetches in
150 seconds, peaking at 217 MiB. Its busy readers and writers together issued
714,430 storage calls and transferred 1.90 GB: request cost remains a limitation.
After this history, the existing single-blob fetch test exceeded its unchanged
768 KiB read budget (1.7–2.0 MB). Explicit blob visibility checks currently walk
published history; this resource gap remains open in issue #6. These are workload
measurements, not upper bounds.

Host-wide concurrent-request admission is a hosting concern and remains deferred;
this example adds no instance limiter or additional durable coordination.

A connection failure may retry one bodyless storage read or one identical
conditional storage write. A rejected write retry preserves the first uncertain
outcome for WAL recovery; it cannot turn a lost success into a definite conflict.
Both attempts count toward the same storage budget. Git pushes are never replayed.

To check small limits, start a fresh-prefix host with push=131072,
negotiation=4096 and object=65536, then run
`GIT_PROBE_LIMITS=1 GIT_PROBE_URL=http://127.0.0.1:19100 go test ./tests -run TestConfiguredLimits`
from this directory. The ordinary suite uses the defaults.

## Cleanup and limits

Push admission checkpoints long tails automatically. Send an authenticated
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
a small importer resolves pack dependencies and streams go-git's delta decoder
into WAL-backed objects. Fetch streams full objects
without making deltas, trading larger transfers for lower memory use. This does
not establish a fixed process-memory ceiling. Ordinary large-file lifecycles
have passed at 16, 64 and 513 MiB for both hashes on local Spin/MinIO.
Both hashes pass shallow clone, deepen, unshallow, annotated tags and 1,025
consecutive pushes with automatic cleanup and cold recovery. Partial filters
and packfile URIs are not replacement requirements.

`go test ./tests -run TestDeltaEncoderGitCompatibility` checks full and delta packs against Git;
`go test ./tests -run '^$' -bench BenchmarkOutgoingDeltas -benchmem` compares size
and allocation costs. Delta selection currently saves transfer bytes at the
cost of whole-object buffering; go-git's transport offers no byte-bounded selector.
Conditional S3 uploads use `Expect: 100-continue` so early rejections can
advertise connection closure. This avoids reusing MinIO connections whose
request bodies were not consumed; successful connections remain reusable.
See [issue #41](https://github.com/carsonfarmer/object-log/issues/41).

`make build` applies `adapter.patch` to checksum-verified upstream source. This
temporary build-tool fix handles Go GC clock and immediate timer calls during
canonical allocation. Spin and Go's collector are unchanged. Remove it when the
standard adapter passes the retained regression and frequent-GC tests. No
upstream post has been made. Retesting the stock pinned adapter with the current
inline catalog still traps on 16 MiB pushes at default GC settings, for both
hashes. `GODEBUG=gctrace=1` also traps because GC logging calls the host during
canonical allocation; sample host RSS instead. No Spin pooling or memory-limit
wrapper is used.
