# Git on object-log

A Go Git service backed by the existing Rust WAL. go-git handles Git protocols,
formats and packs; the sibling Rust component provides authenticated object
storage, atomic publication, checkpoints and garbage collection. Refs and the
sparse object catalog share one WAL head. No local repository cache is needed.
Final replacement qualification is in progress; this is not production ready.

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
Use a fresh prefix: the retired custom Git catalog is incompatible.

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

- `GIT_REPEATED_PUSHES=1`: repeated ordinary pushes and cold history recovery.
- `GIT_LARGE_OBJECT_MIB=513`: larger push/clone/edit/fetch lifecycle.
- `GIT_PROBE_PERSISTED_HEAD=true`: run `TestPersistedHead` after restarting Spin
  with the same prefix to verify saved default-branch recovery.

## Cleanup and limits

Push admission checkpoints long tails automatically. Send an authenticated
`POST /sha1.git/maintenance` (or `/sha256.git/maintenance`) to prune unreachable
objects and collect one bounded batch. Repeat `more` until `complete`; retry
`pending` or `conflict` with a fresh request. `retained` means a WAL retention
blocks collection. Counts are deletion candidates, not unique deleted objects.

Objects use compressed loose-object bodies in 1 MiB chunks and a splitting
sparse index. Incoming packs are staged as seekable WAL chunks; individual delta
bases/results still need whole-object buffers. Fetch streams full objects
without making deltas, trading larger transfers for lower memory use. This does
not establish a fixed process-memory ceiling. Ordinary large-file lifecycles
have passed at 16, 64 and 513 MiB for both hashes on local Spin/MinIO.
Shallow/tag and other remaining provider gates must pass before acceptance;
partial filters and packfile URIs are not replacement requirements.

`make build` applies `adapter.patch` to checksum-verified upstream source. This
temporary build-tool fix handles Go GC clock and immediate timer calls during
canonical allocation. Spin and Go's collector are unchanged. Remove it when the
standard adapter passes the retained regression and frequent-GC tests. No
upstream post has been made. No Spin pooling or memory-limit wrapper is used.
