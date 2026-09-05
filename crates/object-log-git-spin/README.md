# Git on Spin

This HTTP adapter runs the shared `object-log-git::Repository` engine in a
WASIp2 component. The S3 backend uses `object_store`'s AWS implementation with
its HTTP and signing interfaces adapted to Spin SDK 5.2 and RustCrypto. Git
parsing, selection, pack generation, publication, and recovery stay in the
shared engine; the object-log head remains the only mutable durable authority.

Build from the workspace root:

```sh
cargo build --locked -p object-log-git-spin --target wasm32-wasip2 --release
```

Configure Spin variables `endpoint`, `bucket`, `access_key`, and `secret_key`
using Spin's variable provider. The bucket must already exist. Optional
variables are `region` (default `us-east-1`), `prefix` (`object-log-git`),
`log_id` (`repository`), and `object_format` (`sha1` or `sha256`). Use separate
log IDs for different repositories and formats. Then run:

```sh
crates/object-log-git-spin/run.sh
```

The repository URL is `http://localhost:3000/repo`. Upload uses protocol v2;
receive uses classic receive-pack. Storage access is configured independently
of client HTTP access. No filesystem preopens are needed. Repository state
survives fresh Spin instances in S3, not in a process-local cache.

The adapter validates request headers before backend access and acquires a
repository operation before reading a command body. Both transmitted and
gzip-expanded bodies are limited to 10 MiB. During decompression the two host
buffers can coexist (20 MiB); these belong to the runtime allowance until the
engine charges the command input. Response bytes retain their engine owner
through the final stream write. Spin's per-instance engine admission does not
bound aggregate memory across concurrent host instances. The launch command
forces Spin's pooling allocator with `SPIN_WASMTIME_POOLING=1` and limits it
to one live component instance. Do not disable pooling. Unsupported hosts must
fail startup rather than silently use the on-demand allocator; run the
concurrent fixture on each qualification host to verify refusal of a second
live instance. These
limits follow [Spin 4.0.2's pooling configuration](https://github.com/spinframework/spin/blob/v4.0.2/crates/core/src/lib.rs#L92).

The S3 connector streams request and response bodies. It applies a five-second
connect timeout and thirty-second first-byte and between-byte timeouts. It
rejects an overall timeout because WASI HTTP does not expose equivalent
semantics. Automatic object-store retries are disabled; uncertain publication
uses the core recovery contract. An uncertain receive publication returns HTTP
503 with an `application/octet-stream` body containing the opaque recovery
token. An operator can retain those exact binary bytes and use `Log::resume`;
the token is not written to logs. Confirmed acceptance and rejection retain
normal Git response framing. Each invocation validates the backend and
opens the log, so measurements must include that fixed provider work.

A local signed HTTP fixture tests the transport independently of a provider:

```sh
cargo build --locked -p object-log-git-spin --example transport_probe --target wasm32-wasip2 --release
python3 crates/object-log-git-spin/tests/check_transport.py
python3 crates/object-log-git-spin/tests/check_http.py
```

The fixture checks SigV4 signatures, conditional creation and update, conflict
mapping, full and ranged reads, listing, deletion, and bounded 503 propagation.
It is not MinIO qualification or evidence of unchanged-client parity; those
belong to the workspace's provider and Git acceptance gates.

See [initial adapter evidence](EVIDENCE.md) for exact local gates and their limits.

The supported runtime configuration disables outbound HTTP connection pooling.
A pooled provider run produced a transient protocol error; the unpooled
configuration passed the recorded Linux workload. The cause is not proven.
`run.sh` rejects competing `--runtime-config-file` and `RUNTIME_CONFIG_FILE`
settings instead of silently discarding them.

Linux serving under a hard 128 MiB process limit requires an executable cache
prepared with the same Spin version, platform, and component. Cold compilation
was OOM-killed under that limit; cache setup peaked at 228–231 MB across two runs. Prepare
the cache outside the serving cgroup, then retain it for fresh serving processes:

```sh
python3 crates/object-log-git-spin/prewarm_cache.py --directory /deployment/wasmtime-cache
crates/object-log-git-spin/run.sh --cache /deployment/wasmtime-cache/wasmtime-cache.toml
```

This is a compiler cache, not repository state. See the
[Linux qualification evidence](../../docs/evidence/git-spin-linux-2026-09-04.md)
for exact limits, raw counters, provider conditions, and the cold-start failure.
