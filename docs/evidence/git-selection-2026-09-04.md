# Git selected-fetch evidence

The shared engine validates wants and haves against one authenticated view,
computes exact reachable-wants minus reachable-haves, and includes full applicable
annotated-tag chains. Pack writing retains compressed-entry reuse and full-object
fallback. Selected declared blob leaves are authenticated and type-checked before
output; unrelated leaves remain unread.

Two focused tests cover both SHA-1 and SHA-256: full clone, incremental fetch,
duplicate wants/haves, unreachable have exclusion and want rejection, complete tag
chains, empty selection, and wrong-kind tree/tag leaves. Output IDs match Git's
`rev-list` oracle. Every emitted pack normalizes without external delta bases.
Fresh bare receivers contain only the accepted-have closure, install the incoming
pack with strict index validation, and pass strict fsck after updating the target.

The original plan conflated delta independence and full graph connectivity.
[Git 2.54 index-pack](https://raw.githubusercontent.com/git/git/v2.54.0/builtin/index-pack.c)
counts graph dependencies supplied by an existing object database as foreign and
returns 1 with `--check-self-contained-and-connected`. Full clones must return 0;
incremental fixtures return 1 where their ancestors come from accepted history.
Exact set comparison, standalone delta decoding, and the have-only receiver fsck
preserve the intended correctness gate.

Independent Rust correctness review found the missing selected-leaf kind check
and source-repository validation weakness. Both were repaired and approved.

Validation on macOS ARM64, Rust 1.97.1, Git 2.54:

- `mise exec -- make check`: 313 passed, 9 opt-in tests ignored; formatting,
  workspace strict native Clippy, and locked WASIp2 check pass.
- Locked no-default-feature WASIp2 strict Clippy passes.
- Raw logs: `/tmp/object-log-selection-full.log` and
  `/tmp/object-log-selection-wasi.log`.
- Hosted Linux run 33935170712 passed the preceding Task 4 revision.

This is selected-pack acceptance, not wire-client or provider parity. The native
oracle remains. Thin-pack helper 35ddfb9 has separate reviews and awaits integration.
