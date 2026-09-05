# Spin outbound pooling investigation

The failure is reproducible without object-log, object_store, signing, or our
custom WASI transport. Its exact root cause is not yet established.

On macOS 27.0 (26A5425a), Apple M4 Pro arm64, Spin 4.0.2
(`bfc7543`, 2026-06-23), a component using only published spin-sdk 5.2.0 and
anyhow performs five unsigned conditional PUT/GET pairs per incoming request
against the pinned local MinIO image. PUT writes 15 bytes with
`If-None-Match: *`; after the first write it returns 412. GET validates the
original bytes. The SDK buffers both request and response bodies.

| SDK-only run | Outbound pooling | Incoming attempts | Observed result |
|---|---|---:|---|
| First | enabled | 294 | 10 `HttpProtocolError` responses; stopped at 10 errors |
| First | disabled | 1,000 | All passed |
| Repeat | enabled | 469 | Attempt 459 timed out at 45 seconds; subsequent nine attempts were rejected while that instance remained live |
| Repeat | disabled | 1,000 | All passed |

The ten primary errors in the first run all name GET and log:

```text
hyper_util::client::legacy::Error(SendRequest, hyper::Error(IncompleteMessage))
Handler returned an error: ErrorCode::HttpProtocolError
```

Do not interpret the nine post-timeout rejections as independent transport
failures. The committed harness now stops a phase immediately on timeout so
this secondary effect cannot inflate its failure count. There are no retries.

The signed custom-transport fixture against a Python HTTP/1.1 server passed
1,000 fresh invocations with pooling enabled and another 1,000 disabled. A
separate actual Git component/MinIO run reproduced the original index/probe
GET errors. This narrows the reproducer to the SDK/runtime/provider boundary;
it does not establish whether Spin, its Hyper client, or MinIO owns the bug.
A wire trace or a smaller server reproducer is still needed to assign cause.

## Reproduce

Prerequisites: the versions above, Rust 1.97.1 with `wasm32-wasip2`, Python 3,
Docker, and AWS CLI. From the repository root:

```sh
python3 crates/object-log-git-spin/tests/check_pooling.py \
  --sdk-only --output /tmp/spin-pooling-repro --attempts 1000
```

The script builds a standalone minimal component with the retained Cargo.lock,
starts a disposable pinned MinIO container and anonymous test bucket, tests
both settings in separate Spin processes, saves results/logs, then removes
the container. Its generated Rust source contains no project dependencies.
`--wasm <component.wasm>` instead exercises the actual Git adapter's empty
ls-refs/bootstrap path. The HTTP listening port is selected dynamically.

[First results](spin-pooling-2026-09-05/sdk-result.json),
[repeat results](spin-pooling-2026-09-05/sdk-repeat-result.json),
[locked dependencies](spin-pooling-2026-09-05/sdk-Cargo.lock), and compressed raw
[first pooled](spin-pooling-2026-09-05/sdk-pooled.log.gz),
[first unpooled](spin-pooling-2026-09-05/sdk-unpooled.log.gz), and
[repeat pooled](spin-pooling-2026-09-05/sdk-repeat-pooled.log.gz) logs are retained.
The first compiled component SHA-256 is in its result JSON.

## Source findings and limits

Spin 4.0.2 keeps outbound WASI clients in app state and clones them into
instances, so connections are intentionally shared across incoming requests.
Its [HTTP client construction](https://github.com/spinframework/spin/blob/v4.0.2/crates/factor-outbound-http/src/wasi.rs#L650-L681)
uses Hyper's legacy client; disabling pooling sets `pool_max_idle_per_host(0)`.
Its [error conversion](https://github.com/spinframework/spin/blob/v4.0.2/crates/factor-outbound-http/src/wasi.rs#L957-L969)
logs the underlying error and maps otherwise-unclassified failures to
`HttpProtocolError`. Spin pins hyper-util 0.1.20 and Hyper 1.8.1 in its lockfile.
This explains the error surface, not the broken connection's origin.

[Spin issue #3363](https://github.com/spinframework/spin/issues/3363) tracks
replacing the legacy pooling implementation; it does not report this
reproducer or establish a fix. Searching all Spin issues for
`IncompleteMessage` and `HttpProtocolError` found no exact report.

These macOS runs have a 128 MiB per-instance linear-memory ceiling and one live
instance, but no process cgroup limit. The transport failure therefore also
occurs outside the constrained Linux cold-compilation scenario. Compiler
cache preparation and serving-process RSS belong to a separate memory issue.
No production transport change or speculative retry was made.
