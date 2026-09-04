# Git library selection

Date: 2026-09-03

Revision reviewed: `fc71ecc`

## Decision

Use the `gix` umbrella crate and `gix-pack` for Phase 1 of the native
`object-log-git` storage adapter. Select pure-Rust features. Do not run a Git
executable or use C FFI.

`object-log` remains independent of these crates. The Git adapter depends on
the public object-log API. A later smart HTTP crate depends on the Git adapter.

## Required work

The storage adapter must:

- accept parsed ref commands and an input pack;
- validate pack checksums, object IDs, deltas, connectivity, and ref rules;
- normalize thin input into a self-contained pack;
- publish new pack roots and one ref transaction through object-log;
- resolve an uncertain publication without uploading the pack again; and
- recover standard packs, indexes, refs, and `HEAD` in a bare repository.

The adapter owns validation. Callers cannot create the opaque prepared-push
value. This keeps untrusted HTTP input outside the durable authority boundary.

## Candidates

### gix and gix-pack

The review used `gix 0.87.1` and `gix-pack 0.74.2`. Gitoxide describes `gix` as
the application entry point and exposes lower-level crates for pack, object,
reference, and validation work. A pure-Rust feature set uses `zlib-rs` and does
not need libgit2, OpenSSL, or libcurl.

The selected `gix-pack` version is newer than the fix for
[GHSA-x494-mj8g-cj27](https://github.com/GitoxideLabs/gitoxide/security/advisories/GHSA-x494-mj8g-cj27).
That advisory affects versions through 0.68.0 and marks 0.69.0 as the first
fixed version. Input byte, object count, object size, delta depth, work, and
temporary-disk limits are still required.

The feature flags compile with both SHA-1 and SHA-256 support. Acceptance tests
must prove both repository formats. A feature flag is not evidence of complete
behavior.

### git2

`git2` has a smaller Rust dependency graph and a mature API. It binds to
libgit2. Static builds also compile C code. This conflicts with the required
pure-Rust adapter and adds a native toolchain and FFI boundary.

### Rust server crates

The current `gitserver-core 0.0.1` release and its GitHub server workspace cover
repository discovery, HTTP, authentication, protocol handling, process policy,
and benchmarks. They do not provide the narrow pack-validation and storage
boundary needed here. Their release history is too small for this project to
trust that wider surface.

Keep the `gix` repository API for storage work. Re-evaluate server code only
when the separate smart HTTP phase starts. Do not make a current server crate
part of the durable storage boundary.

### Git executable

An installed Git program can validate and materialize repositories. It makes
the deployment depend on a host executable and process policy. Phase 1 excludes
that dependency. The eventual loopback acceptance can still use Git as the
external test client.

## Compile checks

A disposable crate enabled these features:

```toml
gix = { version = "0.87.1", default-features = false, features = ["sha1", "sha256"] }
gix-pack = { version = "0.74.2", default-features = false, features = ["streaming-input", "sha1", "sha256"] }
```

The project lock selects `tinyvec 1.12.0`. A fresh resolution selected
`tinyvec 1.13.0`, which did not compile with the local Rust 1.97 toolchain
because its `vec!` macro was not in scope. The project must keep its resolved
graph pinned and reviewed.

The minimal `gix` plus `gix-pack` WASI check had 106 distinct active package
versions. A focused `gix-pack`, `gix-hash`, and `gix-validate` check had 62.
These counts include the disposable root crate.

## Dependency and build footprint

The selected `gix` and `gix-pack` features have 109 normal third-party
packages on macOS arm64. A lower-bound component set has 86. That set uses
`gix-config`, `gix-features`, `gix-hash`, `gix-object`, `gix-odb`, `gix-pack`,
`gix-ref`, and `gix-revision`.

The component set removes 23 packages. It does not replace
`gix::ThreadSafeRepository::init`, `gix::open`, or `gix::Repository`. The
adapter would have to own repository creation, configuration, hash selection,
object and ref store assembly, and revision graph wiring. Phase 1 keeps the
umbrella crate instead of adding that Git plumbing.

Clean release builds used Rust 1.97.0 on macOS arm64. The umbrella build
directory used 163.7 MiB. The component build used 124.9 MiB. All release
rlibs used 85.1 MiB and 62.0 MiB, respectively. Gitoxide-family rlibs used
27.0 MiB and 14.9 MiB. Empty linked binaries were both 431,312 bytes because
the linker removed unused code. These numbers measure build footprint. They
do not measure the final adapter binary.

## WASI result

The check used installed `wasm32-wasip1` and `wasm32-wasip2` targets. Rust now
uses `wasm32-wasip1` as the name for the former `wasm32-wasi` target.

Without `gix-pack`'s `wasm` feature, both targets failed. The high-level bundle
writer imports `gix-tempfile`, while that dependency is disabled on `wasm32`.

With the `wasm` feature, the dependency check passed for both targets. That
feature removes `gix_pack::bundle::write`, including
`Bundle::write_to_directory`. The high-level `gix` object database also uses
`memmap2`. On WASI, its map calls return `ErrorKind::Unsupported`. A successful
dependency build therefore does not provide a working WASI pack store.

A future WASI adapter can combine lower-level streaming APIs such as
`BytesToEntriesIter`, `LookupRefDeltaObjectsIter`, `EntriesToBytesIter`, and
`index::write_data_iter_to_stream`. It also needs a bounded object lookup that
does not use memory maps. This work adds a new validation path and needs a
separate review.

## Deployment result

The selected libraries compile for native Linux targets. Phase 1 targets
serverless functions and containers with bounded CPU, memory, and disposable
disk. Each request will recover a bare repository, validate and publish one
update, and delete its local files. Object storage and the object-log index
remain the durable authority.

## Sources

- [Gitoxide repository and crate map](https://github.com/GitoxideLabs/gitoxide)
- [`gix` 0.87.1 documentation](https://docs.rs/gix/0.87.1/gix/)
- [`gix-pack` 0.74.2 documentation](https://docs.rs/gix-pack/0.74.2/gix_pack/)
- [`memmap2` 0.9.11 mapping documentation](https://docs.rs/memmap2/0.9.11/memmap2/struct.MmapOptions.html)
- [Rust `wasm32-wasip1` target notes](https://doc.rust-lang.org/stable/rustc/platform-support/wasm32-wasip1.html)
- [`gitserver-core` 0.0.1 API](https://docs.rs/gitserver-core/0.0.1/gitserver_core/)
- [Git smart HTTP server behavior](https://git-scm.com/docs/git-http-backend)

## Limits

This is a library and compile review. It does not prove runtime performance,
untrusted-pack safety, SHA-256 completeness, MinIO behavior, garbage collection,
or Git client compatibility. The plan requires separate evidence for each item.
