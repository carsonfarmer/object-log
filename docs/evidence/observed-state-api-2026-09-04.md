# Observed-state API evidence

## Result

This change compares base revision `6bd4649` with implementation revision
`006f802`. It replaces `View` plus `Cursor` with one `View` backed by shared
immutable state. It also makes `ScopedStore` internal, changes `refresh` to
return `Option<View>`, and adds a preflight check for Git and SQLite. The
key-value, SQLite, Git, and Git HTTP crates use the new API.

The durable v1 encoding did not change. The recovery-token fields, numeric
keys, and field order are the same. The existing canonical and recovery tests
passed.

## Local measurements

The temporary example used an in-memory object store and a log with 1,024 tail
entries. Each `cargo run --release --example api_tranche_measure` invocation
measured 2,000 `View` clones, 2,000 successful preflight operations, and 20
complete `read_tail` operations. The baseline used an empty `prepare` call in
place of preflight. Each revision ran three invocations. The table reports the
median total elapsed time for each batch.

| Batch | Baseline | Final | Change |
|---|---:|---:|---:|
| 2,000 `View` clones | 2,807,208 ns | 6,250 ns | -99.78% |
| 2,000 preflight operations | 24,121,333 ns | 596,000 ns | -97.53% |
| 20 complete tail reads | 41,238,042 ns | 38,973,583 ns | -5.49% |

The final medians are 3.125 ns per clone, 298 ns per successful preflight, and
1.949 ms per tail read. `View` clone now increments one `Arc`. Successful
preflight checks the observed state without I/O, allocation, encoding, or a new
storage ID.

The tail-read change is small. It removes temporary collections and one clone
per reference, but storage reads and decoding still dominate this in-memory
test. The 5.49% elapsed-time change is directional evidence, not a remote-store
performance claim.

The release measurements ran on arm64 macOS 27.0 with Rust and Cargo 1.97.1.
The temporary measurement example was removed before commit.

## Line count

These physical-line counts compare the same revisions. Product counts exclude
the `#[cfg(test)]` tails in library files.

| Area | Baseline | Final | Change |
|---|---:|---:|---:|
| Product | 9,231 | 9,164 | -67 |
| Tests | 13,256 | 13,237 | -19 |
| Benchmarks | 1,060 | 1,055 | -5 |
| Documentation | 3,775 | 3,782 | +7 |

The implementation snapshot has 84 fewer lines in total.

## Verification

The following gates passed at `006f802`:

- `cargo fmt --all --check`.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`.
- `cargo test --workspace --all-features`: 230 passed and nine opt-in tests
  remained ignored.
- `cargo test --all-features --test protocol --test model --test checkpoint`:
  all 61 tests passed.
- `cargo test -p object-log-sqlite --all-features --test recovery`: four
  passed and the 1,000-transaction acceptance test remained ignored.
- `cargo test -p object-log-git --all-features repository::tests::lost_response_resumes_without_restaging_the_pack -- --exact`:
  the focused recovery test passed.
- `cargo test --workspace --all-features --doc`: all five crates passed. The
  crates contain no doctest examples.

An independent rust-skills review returned `ACCEPT`. It found no unresolved
issue #16 defect after the final allocation and ownership corrections.

## Limits

The elapsed-time measurement did not count allocator calls. Source inspection
shows that successful preflight does not allocate. A rejected preflight can
allocate an owned diagnostic string.

Preflight does not reserve a log position. A concurrent publication can make
the view stale before `prepare`. `read_tail` still returns an owned `Vec` of
records. This pre-release change breaks the old source API. We did not run the
large opt-in suites, MinIO, or AWS.
