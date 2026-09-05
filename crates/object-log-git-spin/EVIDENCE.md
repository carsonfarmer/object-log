# Initial adapter evidence — 2026-09-04

Local host: Darwin arm64; Rust 1.97.1 (`8bab26f4f`), Cargo 1.97.1,
Spin 4.0.2 (`bfc7543`). This is adapter qualification. It does not establish
unchanged-client lifecycle parity, MinIO compatibility, performance, or a
128 MiB process RSS bound.

Passed commands from the workspace root:

```sh
cargo fmt --all --check
cargo test --locked -p object-log-git-spin --all-targets
cargo clippy --locked -p object-log-git-spin --all-targets -- -D warnings
cargo clippy --locked -p object-log-git-spin --target wasm32-wasip2 -- -D warnings
cargo build --locked -p object-log-git-spin --target wasm32-wasip2 --release --lib --example transport_probe
python3 crates/object-log-git-spin/tests/check_transport.py
python3 crates/object-log-git-spin/tests/check_http.py
cargo run --locked -p object-log-git-spin --example imports -- target/wasm32-wasip2/release/object_log_git_spin.wasm
```

Four library tests pass: append rejects before exceeding its allocation,
gzip expansion is bounded, concatenated gzip members survive, and unsupported
overall HTTP deadlines are rejected. The example repeats the timeout test.

The transport fixture independently checks every SigV4 signature and payload
hash, including chunked bulk deletion. Its raw result was:

```json
{"host_admission":"second concurrent instance rejected","result":"signed conditional put/get/range/list/delete transport passed","failure":"503 propagated without retry","calls":[["PUT","create"],["PUT","create"],["PUT","update"],["GET",null],["GET","bytes=2-6"],["GET","list"],["POST","delete"],["GET","503"],["GET","held"]]}
```

The last case holds one component's outbound request, attempts a second
inbound request, verifies HTTP failure with no second provider request, and
then releases the first request successfully. Both host instance-count
variables are one. This verifies that the pooling restriction was active on
this host; it does not measure host memory.

The actual application manifest and launch script pass SHA-1 and SHA-256
protocol-v2 discovery, content type, malformed-protocol rejection, malformed
command content type rejection, and unknown route checks with no provider
listening. Static discovery therefore does not accidentally initialize S3.

The built application imports `fermyon:spin/variables@2.0.0` and WASI 0.2.9
interfaces for HTTP, streams/poll, clocks/random, CLI, and filesystem. It
exports `wasi:http/incoming-handler@0.2.0`. There are no socket or WASI P3
imports. Filesystem interfaces are linked by Rust dependencies; both runtime
fixtures pass without preopens. The SDK export macro is enabled only on Wasm,
so native workspace builds do not try to link a WASI ABI export.

Follow-up transport regressions pass with the same local toolchain:

- An 8 MiB signed PUT receives HTTP 307 while the fixture keeps its socket
  open and never reads the body. The adapter cancels the upload and preserves
  the redirect error. Restoring the old `< 400` condition makes this test time
  out, so it exercises actual backpressure rather than only a status branch.
- Bulk-delete requests must carry `Content-Length` and cannot use chunked
  encoding. The connector obtains the exact size from the object-store body
  before converting the request into WASI HTTP resources.
- The actual application accepts uppercase `GZIP` and mixed-case `Identity`
  content-encoding tokens on both object formats.
