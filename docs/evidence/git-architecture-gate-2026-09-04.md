# Git architecture gate

Date: 2026-09-04

Revision: `8d28839b27c0d0f6122f981f13f79412ca11e233`

## Decision

| Surface | Result |
| --- | --- |
| Core with memory storage | PASS |
| Core with local MinIO | PASS in the prior qualification; not rerun in this review |
| Replacement Git v2 with MinIO, collection, and Spin | NOT YET |

The replacement modules remain disconnected. A real Git v2 trial is NO-GO
until `Repository` uses the private pack, durable-reader, wire, and budget
modules.

The prior [local MinIO qualification](local-baseline-2026-09-02.md#local-minio-evidence)
covers conditional create and update, conditional read, uncertain-result
recovery, checkpoint publication, reopen, and base recovery. This architecture
review did not rerun MinIO.

## Verified contract

The read-only architecture review verified:

- one compare-and-swap head update as the publication point;
- deterministic content identity, unique physical deletion keys, and garbage
  collection reachability;
- backend and log isolation;
- recovery after an uncertain publication result;
- a positive garbage collection fence;
- a private replacement API; and
- compilation for `wasm32-wasip2` without default features.

## Task 1B acceptance

Task 1B is accepted at this revision. The private foundation has 2,048 product
lines:

| Module | Product lines |
| --- | ---: |
| Pack normalization | 617 |
| Durable staging and sparse reads | 610 |
| Git wire protocol | 616 |
| Operation budgets | 205 |

These are raw Rust lines before each module's top-level `#[cfg(test)] mod tests`
section. The 2,048-line result is below the 2,050-line stop gate.

`mise exec -- make check` passed at `8d28839`. It ran Rust formatting, strict
Clippy, all workspace tests, and the locked no-default-feature
`wasm32-wasip2` Git check.

The hosted [Rust CI run](https://github.com/carsonfarmer/object-log/actions/runs/33924250916)
also passed for `8d28839`.
