# Local staged-object evidence

## Result

The staged-object API removes immutable payload read-back from new publication.
`put_object` and `put_node` return opaque `StagedObject` values. A `Log` handle
and its clones can use those values in the same collection epoch. Existing
references require `stage_objects`, which deduplicates and verifies the
complete graph once per call.

Recovery tokens do not encode this process-local proof. `resume` and a
separately opened handle verify the complete durable graph before a head
update.

The format still uses version 1. Its pre-release byte layout is not an
acceptance constraint. A simpler or better current layout can replace it
without a compatibility reader.

## Revisions and environment

- Staged-object implementation: `e9617287416fe848f81756adedd826e8993127e8`.
- Rust simplification: `679d22834454c5eb7ff0b2c827a5edf3c5e4f0e0`.
- Public storage contract: `59bf73debe97507d57dfd87d4b5358711c0ab00f`.
- Repeatable accounting target: `582f2bb62a6c5fd4d79e8ebe162151bae97ddff1`.
- Recovery regressions: `455c86dfcbaa9fe3cb11f2101122537acd4b9fa6`.
- Transitive recovery correction:
  `7610f904378c05a8cffc8a8e42aea6c3926a1b3e`.
- Nested collection-fence regression:
  `7ce187b26c5fdb23327158dee51d721ea04e88cc`.
- Head-publication assertion correction:
  `1040cd241cae5b941ffce76adb91fd3dc21f1897`.
- Initial documentation update:
  `35fe1bed47b302f09645392d1409a604d5eb1537`.
- Date: 2026-09-03, America/Vancouver.
- Host: Apple `Mac16,8`, 14 logical CPUs, 48 GiB memory.
- Operating system: macOS 27.0, build 26A5421a.
- Rust: 1.97.1, aarch64-apple-darwin.
- Accounting backend: `object_store::memory::InMemory`.
- Compatibility backend:
  `minio/minio@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`
  on an ephemeral loopback port.

The request and byte results measure one local process. The MinIO runs check
compatibility, not service latency. Nothing here measures remote S3
performance.

## Repeatable request accounting

Run:

```sh
make staged-performance-acceptance
```

The opt-in target creates each fixture before it resets the counters. It then
measures only the named operation.

| Operation | Requests | Uploaded | Downloaded | Blob GETs |
|---|---:|---:|---:|---:|
| 1 MiB SQLite update | 1 GET, 3 PUT | 1,059,503 B | 0 B | 0 |
| 100 MiB SQLite checkpoint | 2 GET, 4 PUT | 104,968,796 B | 249 B | 0 |

The test asserts request counts and the absence of blob GETs. It prints exact
byte totals but does not assert encoded sizes. This lets the pre-release layout
change without weakening the I/O-shape check.

## Causal comparison

A same-host audit ran the same workloads at the immediate parent `104b609` and
at the staged-object implementation `d84cc670`. The product, test, and
benchmark files at `d84cc670` match the integrated `e961728`. The
simplification commit did not change storage operations.

| Workload | Metric | Before | With staged proof | Change |
|---|---|---:|---:|---:|
| 1 MiB update | GET requests | 2 | 1 | -1 |
| 1 MiB update | Downloaded | 1,058,840 B | 0 B | -1,058,840 B |
| 1 MiB update | PUT requests | 3 | 3 | 0 |
| 1 MiB update | Uploaded | 1,059,486 B | 1,059,486 B | 0 |
| 100 MiB checkpoint | GET requests | 4 | 2 | -2 |
| 100 MiB checkpoint | Downloaded | 104,968,429 B | 237 B | -104,968,192 B |
| 100 MiB checkpoint | PUT requests | 4 | 4 | 0 |
| 100 MiB checkpoint | Uploaded | 104,968,771 B | 104,968,771 B | 0 |

The remaining checkpoint reads were one zero-byte conditional head read and
one 237-byte commit read. The exact byte totals differ from the repeatable
target because the audit used its existing benchmark fixtures. Each before and
after pair used the same fixture.

## Correctness evidence

The regular model suite covers these cases:

- A staged blob and node publish without dependency GETs.
- Existing shared graphs are verified once when staged.
- Another open handle rejects a foreign staged proof for new work.
- A collection-epoch change invalidates an earlier proof.
- Same-handle pending resolution reads no blob, node, or commit dependency.
- A decoded recovery token reads and verifies its referenced blob.
- A missing recovery dependency returns an invalid-format error after one blob
  GET and before any head PUT.
- Changed recovery bytes return `CorruptObject` after one blob GET and before
  any head PUT.
- Decoded commit recovery and reopened checkpoint resolution traverse a
  node-to-blob graph. A missing or changed descendant fails before any head
  PUT.
- A collection race cannot stage a new root whose child appears in the active
  positive deletion plan.

The public API prevents callers from constructing `StagedObject` values. The
proof binds the object reference to one open-log domain and collection epoch.

## Storage contract

After a create-only immutable write succeeds, the backend must return the exact
bytes from that physical key until object-log garbage collection deletes it.
Lifecycle expiry, administrative deletion, or overwrite by another tool breaks
this contract. A finite capability probe cannot prove future retention.

This contract is what makes the fast path safe. If an external actor breaks it
between create and publication, same-process publication can expose a missing
or changed reference. Later full verification detects the damage, but it
cannot prevent that first publication. Operators must isolate the object-log
namespace and disable conflicting lifecycle rules.

## Line count

At `7ce187b`, before the separate CI change, the repository contains:

| Category | Lines | Change from `104b609` |
|---|---:|---:|
| Product | 6,494 | +123 |
| Test and support | 10,247 | +635 |
| Benchmark | 868 | +14 |
| Documentation | 2,923 | +254 |
| Schema | 184 | 0 |
| Operator and infrastructure | 232 | +3 |

The strict Rust simplification pass removed 36 product lines and one test line
from the first implementation. It removed duplicate validation, unreachable
branches, and optional proof state. It kept one opaque domain token.

## Local gates

- `make check` passed 189 regular tests. Six opt-in tests remained ignored.
- `make staged-performance-acceptance` passed the two measured workloads.
- The core and SQLite loopback MinIO flows passed. Their test binaries reported
  2.54 seconds and 0.25 seconds.
- The 100,000-object memory GC acceptance completed its timed work in 1.83
  seconds. The 10,001-object MinIO case completed its timed work in 1.62
  seconds. No test container remained.
- Rust documentation built with warnings denied.
- All five benchmark executables compiled in the optimized profile.
- No cloud test ran for this change.

## Limits

- The accounting target uses an in-memory backend. It does not predict remote
  latency or throughput.
- An active collection fence can require collection-plan reads.
- Recovery and separately opened handles still pay the full verification cost.
- The request-count assertions describe the current protocol shape. A better
  pre-release protocol can update the assertions and this evidence together.
- Live AWS qualification remains in issue #10.
