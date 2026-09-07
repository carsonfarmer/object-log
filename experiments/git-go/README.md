# Go Git consumer experiment

This tests go-git against the existing Rust WAL through a WASIp2 component
interface. It is a replacement feasibility experiment, not the complete Git proof.
The WAL head publishes the object index and refs together; Go has no separate
durable authority or local repository cache.

Use stock Go 1.26.3 and `make build`. The sibling `../wal-component` supplies the
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

The replacement is **not accepted**. Ordinary both-hash client tests passed with
access control, persisted default branches, tags/deletion, thin pushes and
conflicting atomic updates. Index and codec tests pass with the race detector.
The retained `TestManyObjects` pushes 32 files (about 2 MiB), but cloning traps
inside Go's canonical allocator when GC completion calls the wall clock. The
adapter pauses its monotonic clock only. This also occurred in a small push;
size is not a safe workaround. No GC disabling, Spin patch, or binding fork is
included. An intermittent S3 initialization-probe HTTP error also failed one
concurrent-push run. The full provider gate remains red.

The related upstream [GC issue](https://github.com/bytecodealliance/componentize-go/issues/56)
describes the monotonic-clock case; our observed stack uses `runtime.walltime1`.
Do not treat its closure as proof that this toolchain works. The failing provider
test is the reproduction. No upstream issue or comment has been posted.

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
