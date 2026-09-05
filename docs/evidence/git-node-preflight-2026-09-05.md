# Git uses core-owned exact node-size preflight

The Git staging path now calls `Log::node_size` with the actual standard-index
length and each pack chunk length, including a short final chunk, before any
PUT. The copied exact CBOR envelope/reference formula is deleted. Root memory,
work and I/O reservations still use the exact encoded size and remain alive
through the root PUT. Core size errors propagate without child uploads.

Dependency: approved core commit `55cc5fce774853d68ff73d204ac991b859b5a32c`
was cherry-picked as `42b2bdb` onto this exclusive worktree. Root already has
the API; integrate only the subsequent Git commit. No core source edits belong
to this Git tranche. The independent Git pack-root/catalog upper bounds and
all existing policy limits remain unchanged.

The exact-reservation test compares preflight with the written root's actual
encoded length and still succeeds at the exact pool capacity, keeps that
reservation during the paused root PUT, and rejects one byte below capacity
with zero PUTs. A new oversized-root test permits the index payload itself at
the configured maximum but rejects its encoded node before any child PUT.
Both failure controls release their reservations.

Variable-chunk tests retain both hashes and widths 8,240, 16,384 and 1,048,576
bytes, comparing actual written length against core preflight. Original
`tests/gc.rs` cases retain their 8,240/16,384-byte settings unchanged.

Verification on macOS arm64, Apple M4 Pro, Rust 1.97.1, installed Git 2.54.0:

- Focused durable tests: 31 passed.
- `make check`: 336 passed, 12 opt-in tests ignored; formatting, strict native
  workspace Clippy, Git WASIp2 check and strict Spin WASIp2 Clippy pass.
- Original three Git GC tests pass inside that full gate.
- Independent Rust correctness/simplification review approved with no findings.

Raw gate output:
[`git-node-preflight-check-2026-09-05.txt`](git-node-preflight-check-2026-09-05.txt).
No new performance claim or provider qualification is made for this size-only
integration. Memory and installed-Git filesystem tests ran; MinIO remains opt-in.

This resolves the Git-side exact-format leakage identified under #31 and the
#26 capacity design. It does not add streaming ingestion or increase capacity.
The next receive-source prototype must use owned bounded controls and replayable
authenticated entry ranges; existing `Bytes` request ownership, thin retries,
compressed vectors and whole-pack indexing still block removal of whole input
retention. That work needs coordination with the receive/protocol owners before
changing their APIs; see [the receive-source contract](git-receive-scan-2026-09-05.md).
