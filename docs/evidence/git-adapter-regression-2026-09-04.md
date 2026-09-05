# Git adapter and preserved workspace regression evidence

The common Git engine has two HTTP adapters: native Axum and Spin WASIp2.
Both use the same `Repository` upload and receive commands. The native oracle
remains selectable; none of its recovery, filesystem, GC, or benchmark tests
were removed.

## Client and failure behavior

The native adapter passes unchanged-client lifecycle tests for both SHA-1 and
SHA-256: protocol-v2 discovery and trace, clone, have-aware fetch, branch/tag
creation, fast-forward, deletion, rejection, checkpoint, collection, and fresh
host clone with strict fsck. A 384-commit negotiation fixture proves gzip,
multiple request rounds, and chunked output. The default 8 MiB push test proves
both an HTTP probe and chunked request, then validates a clone byte-for-byte.
No `http.postBuffer` override is used.

Git's [`remote-curl.c` probe](https://github.com/git/git/blob/v2.54.0/remote-curl.c)
sends an exact flush-only POST before a large receive upload. Both adapters
answer this transport no-op with an empty successful receive-result body;
common transaction preparation still rejects missing ref commands.

Seven deterministic native fault tests cover pending/expired recovery tokens,
cancelled handlers, real TCP disconnect during publication, shutdown draining,
process admission through body collection and response delivery, body/header
limits, and corrupt resolution evidence after a hidden successful head write.
The latter regression reproduced HTTP 500 before repair and verifies HTTP 503
with a recoverable exact token after repair, for both hashes. Restoring the
valid head and reopening the log classifies that candidate as committed.
Spin has the same post-preparation token-preserving error policy.

## Preserved local gates

The extended run includes 1,000 SQLite WAL transactions recovered without their
cache, staged-object request accounting, and the native Git request/byte audit.
Raw output: [extended acceptance](extended-acceptance-2026-09-04.txt).

Pinned local MinIO passed the core conditional protocol, SQLite lifecycle,
native Git checkpoint/collection/recovery, and native HTTP cold-clone cases.
The large GC timed phase took 1.846 seconds for 100,000 memory objects and
1.545 seconds for 10,001 MinIO objects, both below the unchanged 30-second gate.
Raw output: [provider regression](local-provider-regression-2026-09-04.txt).
The shared-native both-hash MinIO lifecycle also has a passing run; actual
Spin MinIO and cgroup evidence are recorded separately.

`make bench` completed all workspace Criterion suites, retaining raw output:
[Criterion](workspace-criterion-2026-09-04.txt). The benchmarked Git core is
`c094ae8`; later adapter token handling and test-support additions do not change
that core. The run used Rust 1.97.1, Git 2.54.0, macOS arm64, Apple M4 Pro,
48 GiB RAM, Docker 29.7.2, optimized Cargo bench profile, and in-memory stores.
Each suite retains its declared warm-up, sample count, and measurement time
in the raw output. Other compilation and local provider qualification overlapped
part of this run, so these timings are regression observations, not isolated
comparative performance or remote-storage measurements. The separately paired
Git performance evidence supplies the pack-size and latency comparison.

No live AWS qualification is claimed. The generic local-filesystem backend
still lacks conditional compare-and-swap; its rejection tests remain part of
the workspace gate, alongside native disposable filesystem oracle tests.
