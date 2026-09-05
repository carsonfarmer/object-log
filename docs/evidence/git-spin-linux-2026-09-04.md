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

## Extended fixture replay

The final replay uses isolated Git 2.54.0 configuration (no global/system
settings, signing, or automatic GC). It extends each hash's workload to a
4 KiB first object, the unchanged default-buffer 8 MiB push, 384 commits with
a changing 16 KiB blob, a full-history clone, and a small subsequent edit
followed by successful incremental push and fetch. Strict fsck, expected ref
OID, and 384/385 commit counts pass. Tag deletion and oversized-object rejection
still leave the host healthy and the durable ref unchanged.

A separate `git pack-objects --thin` invocation for exactly the final update's
want-minus-base closure fails `git index-pack --stdin --fix-thin` in an empty
receiver with one unresolved delta, for both hashes. This proves the generated
thin fixture needs an external base; it does not capture the HTTP client's
actual pack bytes. The successful HTTP push remains an unchanged Git command.

| Extended run | Configured memory.max | Reported memory.peak | oom / oom_kill |
| --- | ---: | ---: | --- |
| Empty cache | 134,217,728 | 134,217,728 | 1 / 1 |
| Cache setup | 536,870,912 | 227,434,496 | 0 / 0 |
| SHA-1 serving | 134,217,728 | 134,332,416 | 0 / 0 |
| SHA-256 serving | 134,217,728 | 134,230,016 | 0 / 0 |

Serving `memory.events:max` counters were 56 and 61. The kernel reported peaks
114,688 and 12,288 bytes above the configured maximum; the measurements are
preserved without rounding them down. Linux documents that `memory.max` can
[temporarily be exceeded](https://www.kernel.org/doc/html/v6.8/admin-guide/cgroup-v2.html#memory-interface-files).
Both processes survived every workload
with `memory.swap.max=0`, and neither was OOM-killed. This supports workload
survival under the configured cgroup constraint, with no spare margin; it is
not evidence that every sampled accounting value remains below 128 MiB.
Empty-cache startup still failed, and prepared-cache deployment remains required.

[Extended raw results](git-spin-linux-2026-09-04/extended/result.json) include
the component hash and cleanup verification. The same directory retains cgroup
metrics, Git fetch/clone/fsck/ref traces, large-push transport traces, and the
external-base diagnostics. Git and pinned MinIO again ran outside the measured
cgroup. This fixture replay does not replace the separate recovery/GC gates.
