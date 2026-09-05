# Linux Spin memory qualification — 2026-09-04

Fresh Spin processes using an explicit, precompiled Wasmtime cache passed the
Git workload for SHA-1 and SHA-256 in a hard 128 MiB cgroup with swap disabled.
**Empty-cache compilation did not pass that limit:** Linux OOM-killed Spin
before readiness. The native oracle remains available.

| Run | Cgroup limit | Peak bytes | OOM kills | Outcome |
| --- | ---: | ---: | ---: | --- |
| Empty compilation cache | 134,217,728 | 134,217,728 | 1 | Startup killed, exit 137 |
| Explicit cache setup | 536,870,912 | 230,866,944 | 0 | Component compiled and discovery succeeded |
| Fresh SHA-1 process, compiled cache | 134,217,728 | 134,217,728 | 0 | Client workload passed |
| Fresh SHA-256 process, compiled cache | 134,217,728 | 134,217,728 | 0 | Client workload passed |

Both serving runs reached their hard memory cap: `memory.events:max` was 25
for SHA-1 and 33 for SHA-256. Reclaim occurred; this evidence establishes no
spare memory margin. Their `oom` and `oom_kill` counters were zero, and
`memory.swap.max` was zero. Metrics were collected after Spin exited; normal
serving runs were deliberately terminated (exit 143) after the workload.

Each workload used unchanged Git 2.54.0 clients outside the measured cgroup:
initial push and clone, an exact 8 MiB deterministic incompressible object
push, have-aware fetch, strict fsck, tag creation/deletion, and an oversized
object rejection followed by an unchanged-ref and health check. Git used its
default HTTP post buffer; the large push used its flush-only probe followed
by a chunked upload. These are provider/client and process-memory results,
not latency benchmarks or the complete recovery/GC acceptance suite.

Conditions: official Linux aarch64 Spin 4.0.2 (`bfc7543`), Docker runtime image
`rust:1.97.1-bookworm` (local image ID
`sha256:0e2bcaef56d041a486784e54104a81aebe0da44bd03019bd70bc0401e42e4a97`),
one forced pooled component instance, and the exact WASM hash in
[raw results](git-spin-linux-2026-09-04/result.json). MinIO ran outside the
serving cgroup using the digest pinned by `scripts/test-minio.sh`. All owned
containers and networks were verified removed. A prior full replay also passed
both warm-cache workloads; its [result](git-spin-linux-2026-09-04/previous-result.json)
records a 227,618,816-byte cache setup peak. Both runs failed empty-cache startup.

The qualified runtime configuration disables outbound HTTP connection
pooling. An earlier pooled run returned a transient `HttpProtocolError` on
an index GET, also observed on macOS; its [log is preserved](git-spin-linux-2026-09-04/pooled-transport-failure.log).
Fresh-instance teardown interacting with pooled connections is a hypothesis,
not a proven root cause. The successful configuration is established by this
run, not by retrying failed Git commands. Spin documents the
[outbound pooling setting](https://spinframework.dev/v4/dynamic-configuration).

Reproduce from the workspace root after building the component:

```sh
python3 crates/object-log-git-spin/tests/check_linux_memory.py \
  --spin /path/to/official-linux-spin/spin \
  --wasm target/wasm32-wasip2/release/object_log_git_spin.wasm \
  --output /tmp/object-log-linux-qualification-new
```

For deployment, run `prewarm_cache.py` on the deployment OS/architecture with
the exact Spin binary and component, outside the 128 MiB serving cgroup.
Retain its generated cache directory, then pass its configuration to `run.sh`
inside the serving limit. The executable cache is disposable and independent
of repository state; deleting it requires recompilation, while repository
recovery still depends exclusively on the object-log head and immutable S3
objects. The qualification script performs that setup in an explicit 512 MiB
container and then starts new 128 MiB serving processes using the same cache.
