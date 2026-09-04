# Git WASI durable reader evidence

Date: 2026-09-04

Current main revision: `223849e34287768862701fc6639035745b5305a9`

Implementation revision: `c90a2aa07fcb04f5437ec9afd6a549cd00d181c8`

This issue #17 tranche adds private pack staging, catalog loading, and sparse
object reads. It does not add a public Git API.

## Result

`stage` splits one normalized pack into 1 MiB immutable objects. It stores the
standard Git version 2 index in the root node. It returns the pack descriptor
and the object-log staging proof for that root.

`load` reads pack roots with at most eight concurrent transfers. It validates
the node shape, chunk lengths, descriptor, standard index, object order,
fan-out table, offsets, CRC table, pack checksum, and index checksum. It builds
one in-memory directory over the live indexes. If two packs contain the same
object, the directory selects the lowest pack ID. Input root order does not
change that result.

`Reader::find` searches the directory before it reads pack data. A miss makes
no object-store request. A hit reads only the 1 MiB chunks that contain the
indexed pack entry. A one-chunk entry uses a `Bytes` slice. An entry that
crosses a chunk boundary copies only that entry range.

The reader supports base objects, `OFS_DELTA`, and in-pack `REF_DELTA`. It
checks entry CRC32, the canonical pack header, the exact zlib stream, delta
sizes, cycles, depth, and the final Git object ID. The pack normalizer makes a
thin input self-contained before staging.

The durable pack-root layout is pre-release. It can change when another layout
reduces code or improves runtime behavior.

## Request and cache behavior

The SHA-1 and SHA-256 round-trip test records these request counts:

- Staging uses one PUT for each pack chunk and one PUT for the root.
- The uploaded bytes equal the pack bytes plus the root bytes.
- Loading one pack catalog uses one GET for the root and does not read a pack
  chunk.
- Looking up an absent ID uses zero GETs and downloads zero bytes.
- A verified chunk is cached by pack and chunk number in that `Reader`.

The chunk-boundary test first downloads one complete 1 MiB chunk. A later
two-byte read across the boundary downloads only the uncached final chunk. A
subsequent object read from those cached chunks uses zero GETs.

The cache is local to one `Reader`. It admits complete verified chunks until
its 32 MiB limit is full. It has no eviction policy.

## Hard limits

| Resource | Limit |
| --- | ---: |
| Pack chunk | 1 MiB |
| One durable pack | 64 MiB |
| Standard index | 4 MiB |
| Pack root | 4 MiB + 4,022 bytes |
| Charged catalog memory | 64 MiB |
| Reader cache | 32 MiB |
| Concurrent stage or load transfers | 8 |
| Objects in one pack | 65,535 |
| Packs in one catalog | 65,535 |
| One decoded object | 16 MiB |
| Charged read work per `Reader` | 256 MiB |
| Pack GETs per `Reader` | 256 |
| Delta depth | 4,095 |

Read work charges pack-entry ranges, decoded entries, loaded chunks, and delta
results. These limits reject work before an unbounded read or allocation.

## Tests and build proof

Nine focused tests pass. They cover:

- SHA-1 and SHA-256 staging, loading, sparse lookup, and an empty pack;
- deterministic duplicate-object selection;
- exact chunk-boundary reads and cache hits;
- malformed roots, descriptors, indexes, checksums, fan-out tables, IDs,
  offsets, CRC values, zlib streams, and trailing entry bytes;
- base objects and multi-level `OFS_DELTA` and `REF_DELTA` chains;
- request, work, object, catalog, and depth limits; and
- collection of an unpublished pack tree while a published tree remains
  readable.

The native tests use Git `2.54.0 (Apple Git-157)` to generate SHA-1 and
SHA-256 packs and indexes. The product path does not invoke Git or use a local
repository.

These gates passed after integration:

```sh
cargo test -p object-log-git --lib durable::tests
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo +1.97.1 check -p object-log-git --lib \
  --target wasm32-wasip2 --no-default-features
cargo +1.97.1 clippy -p object-log-git --lib \
  --target wasm32-wasip2 --no-default-features -- -D warnings
make check
```

The WASIp2 checks compile the durable module and its common dependencies. They
do not prove runtime behavior in a WASI host.

## Source size

| Current durable module | Raw lines | Nonblank lines |
| --- | ---: | ---: |
| Product | 610 | 576 |
| Tests | 836 | 777 |

The implementation commit also added 11 and removed three product glue lines.
It added two and removed two dependency-configuration lines. The locked common
versions used here include `futures` 0.3.34, `gix-features` 0.49.1,
`gix-pack` 0.74.2, and `gix-zlib` 0.1.0.

## Authorities

- Git's [`gitformat-pack`](https://git-scm.com/docs/gitformat-pack) defines
  pack entries, object IDs, checksums, deltas, and the version 2 index.
- [`gix-pack` 0.74.2](https://docs.rs/gix-pack/0.74.2/gix_pack/) supplies the
  standard index reader and pack entry parser.
- [`gix-zlib` 0.1.0](https://docs.rs/gix-zlib/0.1.0/gix_zlib/) supplies bounded
  one-stream inflation.
- Local Git `2.54.0 (Apple Git-157)` supplies the pack and index oracles used
  by the tests.

## Remaining limits

The module is private and is not connected to the wire protocol or repository
engine. It reads one object; commit, tree, and tag traversal remain engine
work. Catalog loading reads every live index and builds the complete directory
in memory. The 64 MiB catalog charge does not include all allocator and pack
metadata overhead. The cache has no eviction. The reader has no WASI runtime,
Spin, remote object-store, or MinIO performance result.
