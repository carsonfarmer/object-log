# Git WASI replacement baseline

Date: 2026-09-04

Code revision: `e0a22dd45aa7be19016024238c3677b58b63aa94`

This record captures the native reference before issue #17 replaces its Git core.
The tests use Git 2.54.0, Apple Git-157. The host is an Apple `Mac16,8` with 14
logical CPUs and 48 GiB of memory.

## Source size

Physical lines in inline `#[cfg(test)]` modules count as test lines.

| Area | Product | Tests |
| --- | ---: | ---: |
| `object-log-git/src/lib.rs` | 247 | 21 |
| `format.rs` | 155 | 70 |
| `state.rs` | 102 | 115 |
| `storage.rs` | 173 | 191 |
| `git.rs` | 416 | 203 |
| `git/local.rs` | 369 | 252 |
| `repository.rs` | 380 | 498 |
| Core integration tests and support | 0 | 741 |
| **`object-log-git`** | **1,842** | **2,091** |
| `object-log-git-http/src/lib.rs` | 579 | 143 |
| `server.rs` | 370 | 149 |
| `main.rs` | 74 | 0 |
| HTTP loopback tests | 0 | 404 |
| **`object-log-git-http`** | **1,023** | **696** |
| **Combined** | **2,865** | **2,787** |

The Git benchmark has 193 lines. Git-specific documentation has 592 lines.

## Current WASI build failure

This command fails:

```sh
cargo +1.97.1 check -p object-log-git --lib --target wasm32-wasip2
```

Tokio first reports that its `fs` feature is not supported on WASI. The current
core also uses local directories, blocking tasks, high-level `gix` repository
discovery and object databases, memory maps, Git reference locks, temporary
files, and `gix_pack::Bundle::write_to_directory`.

The `gix-pack` `wasm` feature removes that bundle writer. Cargo feature
unification means the project cannot enable this feature in the common graph
while the native reference still calls the writer. The first replacement
tranche therefore isolates the native code without adding the low-level pack
engine.

The generic `object-log` crate passes the same `wasm32-wasip2` check.

## Client oracle

Run the loopback suite with:

```sh
cargo +1.97.1 test -p object-log-git-http --test loopback
```

The native HTTP suite passed three loopback tests in 8.55 seconds. One local
MinIO test remained ignored.

The native client fixtures cover:

- empty discovery and the first push;
- clone, fast-forward push, and fetch;
- atomic branch and annotated-tag creation and deletion;
- a stale forced-push rejection;
- two concurrent pushes with one winner; and
- final content, ref state, and `git fsck --strict`.

The MinIO fixture stops the first host, opens a new log and scratch directory,
then clones and validates the durable repository.

The lower-level tests cover SHA-1 and SHA-256, thin-pack normalization,
corruption and object-count rejection, ref policy, lost publication responses,
checkpoints, collection, view expiry, and current-view corruption.

## Protocol trace

The client requests protocol version 2, but the current upload-pack response
uses the protocol-v0 service preamble and `want`/`have` exchange. It contains no
`version 2`, `command=ls-refs`, or `command=fetch` packets.

The have-aware fixture creates 385 commits, clones them, adds one commit, then
fetches. The incremental request sent 385 `have` lines. The server ignored them
and returned every reachable object. The review used:

```sh
GIT_TRACE_PACKET=1 cargo +1.97.1 test -p object-log-git-http \
  --test loopback unmodified_git_pushes_clones_fetches_and_rejects_stale_updates \
  -- --exact
GIT_TRACE_PACKET=1 cargo +1.97.1 test -p object-log-git-http \
  --test loopback large_fetch_uses_gzip_multi_round_requests_and_chunked_output \
  -- --exact
```

The review retained the observations but not the raw trace.

The replacement trace must contain `version 2`, `command=ls-refs`,
`command=fetch`, valid `have` lines, and a protocol v2 `packfile` section. It
must not contain the protocol v0 upload-pack service preamble.

Receive-pack remains the classic push path. Its current trace contains the
service preamble, ref commands, pack input, `unpack ok`, and per-ref `ok` lines.
A rejected stale update returns `unpack rejected` and a per-ref `ng` line. The
replacement must retain these patterns and must not invent `command=push`.

## Object-store request baseline

The focused request audit passed in 3.76 seconds with:

```sh
make git-performance-acceptance
```

| Case and phase | Requests | Uploaded | Downloaded |
| --- | ---: | ---: | ---: |
| 4 KiB publication | 4 PUT | 4,879 B | 0 B |
| 4 KiB checkpoint | 3 GET, 2 PUT | 579 B | 4,654 B |
| 4 KiB recovery | 4 GET | 0 B | 4,996 B |
| 8 MiB publication | 36 PUT | 8,393,931 B | 0 B |
| 8 MiB checkpoint | 35 GET, 2 PUT | 583 B | 8,393,706 B |
| 8 MiB recovery | 36 GET | 0 B | 8,394,049 B |

The small pack is 4,311 bytes. The large pack is 8,391,374 bytes and uses 33
pack chunks. The fixture uses deterministic pseudo-random file bytes, fixed Git
identity and timestamps, and `git pack-objects --all --stdout`.

## Local latency baseline

Criterion used ten samples, a one-second warm-up, an eight-second measurement,
and an in-memory object store. Run it with:

```sh
make git-bench
```

The retained [raw slope estimates](git-criterion-2026-09-03.tsv) use revision
`bfa3cd7603ff1a82dab8af88380f78b366d111e0`. The table below is the later
baseline run reported for this review. Its raw Criterion directory was not
retained.

| Operation | Pack | Slope estimate | 95% interval |
| --- | ---: | ---: | ---: |
| Publication | 4,311 B | 987.44 us | 963.01-1,024.73 us |
| Publication | 8,391,374 B | 67.279 ms | 66.822-68.212 ms |
| Checkpoint | 4,311 B | 23.400 us | 22.921-24.170 us |
| Checkpoint | 8,391,374 B | 3.743 ms | 3.510-3.958 ms |
| Cold recovery | 4,311 B | 3.173 ms | 3.116-3.242 ms |
| Cold recovery | 8,391,374 B | 51.815 ms | 50.892-52.932 ms |

## Current bounds

- Encoded HTTP request body: 513 MiB.
- Input and output pack: 512 MiB each.
- Upload control input: 8 MiB.
- Receive control input: 1 MiB.
- Wants or ref commands: 1,024.
- Haves: 65,536.
- Repository objects: 1,000,000.
- Decoded object allocation: 256 MiB.
- Durable pack chunks: 8 MiB.
- Concurrent chunk transfers: eight.
- Default active Git operations: four.
- HTTP request and response idle timeout: one minute.

The current suite tests fragmented input and command-count overflow. It does
not measure HTTP wire bytes, peak process memory, or aggregate scratch use.
The latency and byte results are local. They do not predict remote object-store
performance.
