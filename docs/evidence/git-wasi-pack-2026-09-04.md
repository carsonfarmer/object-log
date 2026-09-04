# Git WASI pack engine evidence

Date: 2026-09-04

Revision: `468e4913ee671a61dd49a292e57afe26b9cc99ff`

This issue #17 tranche adds one private, bounded pack normalizer. It uses
Gitoxide for pack parsing, compression, delta application, hashing, and
standard index generation. It does not add a second public Git API.

## Result

The normalizer accepts standard pack version 2 for SHA-1 and SHA-256. It
supports base objects, `OFS_DELTA`, forward and backward in-pack `REF_DELTA`,
and thin `REF_DELTA` packs.

Thin-pack callers supply the exact set of bases that are outside the incoming
pack. Each base includes its Git kind, bytes, and object ID. The normalizer
checks the ID before use. An unused base or a base that is also present in the
pack is an error. This private contract avoids a second lookup algorithm. The
receive path still needs a preparation step that identifies unresolved base
IDs before it calls the normalizer.

The output contains a self-contained pack and its standard version 2 index.
The implementation rejects duplicate object IDs. It passes the exact entry
values produced by pack writing to index generation, so index construction
does not scan and inflate the complete pack again.

## Limits

| Resource | Limit |
| --- | ---: |
| Input pack | 32 MiB |
| Normalized pack | 64 MiB |
| One decoded object | 16 MiB |
| Decoded work | 256 MiB |
| Objects after thin-pack completion | 65,535 |
| Standard index | 4 MiB |
| Delta depth | 4,095 |

The work limit counts both required inflations and each materialized delta
result. The depth limit matches the maximum accepted by Git's pack generator.
The depth and cycle checks are iterative.

External-base insertion uses ordered maps for one
`O((objects + bases) log(objects + bases))` pass. It does not use Gitoxide's
lookup wrapper, which can rescan the supplied base set for each unresolved
reference.

## Verification

Nine focused pack tests pass without default features. They cover:

- SHA-1 and SHA-256;
- empty, base, `OFS_DELTA`, forward and backward `REF_DELTA`, and thin packs;
- checksums, truncation, trailing data, object counts, offsets, canonical
  headers, duplicate objects, and external-base evidence;
- every byte and object limit; and
- depth 4,095, depth 4,096, and a dependency cycle.

The generated packs pass `git index-pack --strict` and `git fsck --strict`.
Native strict Clippy, the WASIp2 no-default check and strict Clippy, and the
complete workspace gate pass. An independent Rust review found no remaining
correctness issue, input-reachable panic, unsafe code, or quadratic object
walk.

The WASIp2 dependency graph excludes high-level `gix`, `gix-tempfile`, and
Tokio filesystem support. `memmap2` remains a transitive `gix-pack` dependency,
but it selects its unsupported-platform stub on WASI. This module uses only
in-memory Gitoxide entry points.

## Local timing and size

An indicative release run normalized the same 193-byte REF pack with three
50,002-byte decoded blobs 2,000 times. The final implementation took 227.094
microseconds per call. The earlier form, which inflated the pack once more
during index construction, took 236.110 microseconds per call. This single run
suggests a 3.8% improvement. It is not retained Criterion evidence.

The tranche adds 494 product and integration lines, 559 test lines, 11
dependency-configuration lines, and five generated lockfile lines. The pack
module accounts for 489 product lines. Five lines connect the private module.

## Remaining work

The normalizer is private until the durable repository and protocol engine use
it. It holds its bounded input and output in memory. It does not yet expose an
inspection phase for unresolved thin-pack bases. Runtime WASI memory and import
inspection require a callable component entry point. No result in this record
qualifies remote object-store performance.
