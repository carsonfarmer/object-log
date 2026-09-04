# Git fetch-pack task 2 source gate

Date: 2026-09-04

Plan revision reviewed: `bd04a725b73baa416b1beb1d7de1db49df924920`

This record defines the design gate for task 2. It does not record implemented
behavior, passing product tests, or WASIp2 execution.

## Accepted design

Task 2 can add private stored-entry inspection and a bounded fetch-pack writer.
The durable format remains named v1. Task 2 should avoid unrelated layout work.
It can change the v1 layout when that change reduces code or improves measured
performance. It must not add public API.

The writer must meet these requirements:

- Build one complete private pack before the wire layer writes output.
- Reserve the output allocation before allocation. Keep that reservation with
  the returned bytes through wire framing.
- Limit the complete raw pack to 9,437,184 bytes. The limit includes the pack
  header and trailer. Reject byte 9,437,185 before response output.
- Write pack version 2. Use the repository object format for its SHA-1 or
  SHA-256 trailer. Write the exact checked object count. Do not write an index.
- Resolve each selected object to the current canonical occurrence. Sort by
  pack ID, source offset, and object ID. Duplicate and reordered input must
  produce the same bytes.
- Inspect and emit one stored entry at a time through the existing chunk cache.
  Do not retain all compressed entries in a second collection.
- For a full object, write a canonical new entry header and copy only the
  stored zlib payload.
- For an OFS_DELTA or REF_DELTA, reuse its zlib delta payload only when its
  immediate base object is also selected. Write a REF_DELTA against that base
  object ID.
- Materialize the target as a full object when its immediate base is not
  selected. A base that the client has but the output omits does not qualify.
  The output pack must be self-contained.
- Resolve an OFS_DELTA base through the source pack offset table. Resolve a
  REF_DELTA base through the same source pack index. Reject a missing or
  external durable base.
- Before reuse, verify the CRC over the complete stored entry. Parse the entry
  header, require its canonical encoding, and prove the exact payload range.
  The no-inflate reuse path depends on the existing rule that staging accepts
  only normalized immutable packs.
- Pass the uncompressed delta-instruction size to a reused delta header. Do
  not pass the reconstructed target size.
- For fallback output, use the existing reader, verify the reconstructed object
  ID, and use deterministic compression.
- Charge reads, decoded input, compression input, and emitted output to the
  same operation. A retry must not reset a counter.
- During pack construction, hold the catalog, selected locations, bounded
  output, chunk cache, and one current stored entry or decoded object. Do not
  retain graph state or a framed output buffer.
- Give the wire layer output only after pack construction and all checks pass.

Tests must cover SHA-1 and SHA-256, full-object reuse, OFS-to-REF conversion,
REF reuse, client-known-only base materialization, selected delta chains,
corrupt CRCs, missing bases, duplicate occurrences, stable order, deterministic
output, an empty pack, 9,437,184 bytes, and 9,437,185 bytes. Representative
packs must pass Git 2.54 `index-pack --strict
--check-self-contained-and-connected` and contain the exact selected object IDs.

## Source evidence

Git `v2.54.0`, commit `94f057755b7941b321fd11fec1b2e3ca5313a4e0`,
defines the required pack behavior:

- `Documentation/gitformat-pack.adoc:29-130` defines hash-format trailers,
  CRC coverage, pack entries, delta sizes, and self-contained stored packs.
- `builtin/pack-objects.c:626-710` checks a version 2 index CRC, writes a new
  entry header, and copies the stored compressed payload.
- `builtin/pack-objects.c:1083-1131` converts OFS_DELTA to REF_DELTA and keeps
  the compressed delta payload.

The current Gitoxide crates supply the low-level WASIp2-compatible operations:

| Crate | Version and source revision | Required path |
| --- | --- | --- |
| `gix-pack` | `0.74.2`, `3ebca8b66017ab2dd02a38f75f78f485bee1ded8` | `src/index/access.rs:14-26`, `src/data/entry/decode.rs:21-63`, `src/data/entry/mod.rs:31-68`, `src/data/entry/header.rs:75-107`, `src/data/header.rs:23-36` |
| `gix-hash` | `0.26.2`, `e52fe9d03e82437a25bdfb1098e7046ec7e1b558` | `src/io.rs:87-129` |
| `gix-zlib` | `0.1.0`, `842bc447e3aeacf5d9d36f7f8a01068eda4b7999` | `src/stream/deflate.rs:153-224` |

Use `gix-pack` for index data, header parsing, checked OFS base offsets, pack
headers, and entry headers. Use `gix-hash` for the streaming pack checksum. Use
`gix-zlib` for fallback compression. Its current path uses pure Rust `zlib-rs`.
`gix-pack::data::Entry::from_bytes` parses the header. It does not validate the
zlib stream or prove the object ID.

Do not enable the `gix-pack` `generate` feature for WASIp2. Its
`data::output::FromEntriesIter` path adds graph, diff, locking, and table code.
Do not use `data::input::EntriesToBytesIter` for this writer. It requires owned
compressed `Vec` values and `Read + Write + Seek`, then reads the full output
again to calculate its checksum.

Cursor's [Git at any scale](https://cursor.com/blog/git-at-any-scale) describes
an object-storage WAL, standard Git repositories on local NVMe, conditional
reads, CAS publication, and reuse of published compacted packs across replicas.
It does not describe reuse of stored zlib entry streams in a fetch pack. Git's
`builtin/pack-objects.c` supplies the source evidence for that technique.

Micelio commit `980f1d94c1bfc3ed0500e936c0e60b4d3cee4af2`
also uses native Git and local repositories. `lib/micelio/git.ex:1-9` assigns
pack meaning and optimization to Git. `lib/micelio/git.ex:548-588` calls
`git pack-objects`. `docs/architecture.md:64-81` streams packs through local
files. Micelio does not supply a WASIp2 pack-writer API.

Walgit source at commit `6d8fa54ba0f83072a1a50317bb6c8c1afa5a3cd1`
reports a self-contained fetch design. Comments at
`crates/walgit-git/src/upload_gix.rs:388-400` report large offset-to-object-ID
table costs in the Gitoxide thin-pack path, and the code disables thin output.
Its high-level generator uses repository, thread, and runtime facilities that
task 2 must not add.

The `git-server` package in `imjasonh/playground`, commit
`f4693f58d6dc4b705cae3cd41c9b0fd6593cd800`, targets Cloudflare Workers. Its
`git-server/src/pack/write.rs:1-142` and
`git-server/src/repo.rs:1490-1535` report stored zlib payload reuse and
OFS_DELTA-to-REF_DELTA conversion. It is not a Cloudflare-owned project. Its
package manifest declares MIT, but the repository root has no clear license
file. Use it as design evidence only. Do not copy its source or structure. Its
SHA-1-only, streamed, optional-thin writer has a different contract from this
bounded SHA-1 and SHA-256 writer.

## License limits

| Source | License evidence | Rule for task 2 |
| --- | --- | --- |
| Git 2.54 | GPL-2.0-only `COPYING` | Use behavior and documentation. Do not copy source. |
| Gitoxide crates | MIT OR Apache-2.0 package metadata and license files | Call the public APIs. Apache-2.0 is compatible with this repository. |
| Walgit | MIT `LICENSE` | Use source-reported behavior and design evidence. No copied code is required. |
| Micelio | MPL-2.0 `LICENSE` | Use architecture evidence. Do not copy source into Apache-2.0 files. |
| `imjasonh/playground` | Package says MIT; repository root license is unclear | Do not copy source or structure. |
| Cursor article | Published article, not a code license | Use the stated design principles only. |

## Size and dependency gate

Task 2 has these stop limits:

| Change | Maximum |
| --- | ---: |
| Net product code | +160 lines |
| Test code | +450 lines |
| Dependencies | 0 |
| Cargo features | 0 |
| Public API additions | 0 |
| Durable format-name changes | 0 |

Factor the existing range, CRC, header, and canonical checks out of the current
reader. Do not add a second offset map, checksum implementation, zlib
implementation, header encoder, streaming response state, output index, file
path, process call, or runtime adapter. Stop task 2 for a smaller design review
if an implementation exceeds a limit.
