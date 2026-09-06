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
this experiment does not provide authentication.

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

Objects use 1 MiB chunks and an immutable bucketed index, not stored Git packs.
The WAL currently limits each bucket to 1,024 object children. Upstream's parser
retains inflated objects for non-seekable receive input, so chunked storage does
not establish bounded receive memory. Packed storage/range reuse, full protocol
extension parity, maintenance/GC, and expired-view retry remain outside this
experiment. It does not replace the existing implementation yet.
