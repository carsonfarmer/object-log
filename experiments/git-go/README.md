# Go Git consumer experiment

This tests go-git against the existing Rust WAL through a WASIp2 component
interface. It is a replacement feasibility experiment, not the complete Git proof.
The WAL head publishes the object index and refs together; Go has no separate
durable authority or local repository cache.

Use stock Go 1.26.3, install `rustup target add wasm32-unknown-unknown`,
and run `make build`. The sibling `../wal-component` supplies the
shared WIT contract. The Makefile generates bindings and builds `main.wasm`;
generated files and binaries are ignored. Dependencies are pinned in `go.mod`:
go-git `e9e5820fe0d2`, Bytecode Alliance go-pkg `91f6c4863e67`, and componentize-go
0.4.2. This uses the synchronous WASIp2 HTTP adapter, without Spin patches.

Compose the component with `wal-component` and start ordinary Spin against a
fresh, isolated local MinIO prefix. The paths are `/sha1.git` and `/sha256.git`.
The host supplies `WAL_ENDPOINT`, `WAL_BUCKET`, `WAL_REGION`, `WAL_ACCESS_KEY`,
`WAL_SECRET_KEY`, and `WAL_PREFIX` as environment variables. Bind to loopback;
authentication is optional as described below.

Run the opt-in tests against that host:

```sh
GIT_PROBE_URL=http://127.0.0.1:19100 \
GIT_PROBE_LOG=/absolute/path/to/component_stderr.txt \
go test ./tests -v -timeout 3m
```

The tests cover both hashes, atomic push, rejected ref-prefix collisions,
clone/fetch/fsck, a verified external REF_DELTA thin push, and exact sparse blob
retrieval. The byte bound uses actual WAL transport counters. `GIT_PROBE_LOG`
is only needed when the host drops HTTP trailers; it reads the same counters
from the corresponding request's log line. Run each suite with a fresh prefix.

Objects use the library's compressed loose-object format in 1 MiB chunks.
Crowded index leaves split, keeping lookup sparse. Incoming packs are staged as
seekable WAL chunks so the library can release decoded bodies as it parses;
individual delta bases/results still require full buffers. Temporary input packs
are not published roots and need later collection. Packed storage with deltas,
maintenance/GC, expired-view retry, and full fetch visibility policy remain open.

The replacement is **not complete**. Both-hash provider tests pass with ordinary
Spin, including the 32-file clone regression, malformed input, competing pushes,
sparse retrieval and a stress run with `--env GOGC=1` (frequent Go collection).
Index/codec native tests and strict adapter Clippy also pass. After restarting
Spin, run `GIT_PROBE_PERSISTED_HEAD=true go test ./tests -run TestPersistedHead`
with the same connection settings to check the saved default branch.

`make build` applies `adapter.patch` to checksum-verified, pinned upstream source.
This temporary component-build fix caches both clocks during canonical allocation
and handles Go's immediate timer poll without calling the host. Other paused polls
are rejected; it does not invent file-descriptor readiness. Spin itself and the
Go collector remain unchanged. Remove this patch when the standard adapter passes
the retained regression and stress tests. No upstream post has been made.

Local configuration is in `spin.toml`. After building both components, compose:

```sh
wac plug --plug ../wal-component/target/wasm32-wasip2/release/wal_component_probe.wasm main.wasm -o git.wasm
spin up --listen 127.0.0.1:19100 \
  --env WAL_PREFIX=your-fresh-test-prefix \
  --env WAL_ACCESS_KEY=your-local-minio-key \
  --env WAL_SECRET_KEY=your-local-minio-secret
```

Use `--env GIT_PASSWORD=...` for Git HTTP Basic authentication (any username),
`--env GIT_READ_ONLY=true` to reject pushes, and `--env WAL_DEFAULT_BRANCH=...`
for a new repository's default branch. That branch is saved with the repository.
Set matching `GIT_PROBE_PASSWORD` and `GIT_PROBE_BRANCH` when testing those settings.
Use HTTPS when credentials leave loopback. No authentication is enabled by default.
