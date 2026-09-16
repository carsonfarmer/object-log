# Third-party software

Project code is Apache-2.0; see [LICENSE](LICENSE). `object-log` is the only
publishable Cargo package. The key-value example and WASIp2 component are also
Apache-2.0 and set `publish = false`. Dependencies retain their own licenses.

## Retained adapter source

The Git build uses Wasmtime's `wasi-preview1-component-adapter` from
[revision `c8e24c3`](https://github.com/carsonfarmer/wasmtime/tree/c8e24c308754f784fbb4a08205a2a9c08c461d00),
which contains the fix proposed in
[Wasmtime #14319](https://github.com/bytecodealliance/wasmtime/pull/14319).
The build script verifies the downloaded source archive checksum. The adapter's
Apache-2.0 WITH LLVM-exception license is reproduced in
[`licenses/wasmtime.txt`](licenses/wasmtime.txt) and must accompany redistributed
adapter source or binaries.

The Git example pins a [go-git fork](https://github.com/carsonfarmer/go-git/compare/d15c624290f14f5eec6f51a660b20b8449ee7d98...26cbbf6c051e0f72063eebb2f0d07a4181e4bb7c)
with streaming pack import, request and object limits, error propagation, thin-pack
resolution, version 3 packs, receive-pack corrections, reference advertisement
fixes, and bounded reuse of existing deltas.
[go-git #2379](https://github.com/go-git/go-git/pull/2379) proposes only the streaming
decoder rewind and failed-reopen fixes; it does not cover the complete fork.
The comparison above shows every retained change from its upstream ancestor.

The Go component bindings pin a [go-pkg revision](https://github.com/carsonfarmer/go-pkg/commit/b0c40df4c02bb780994cedee093376abe9144ba2)
that releases imported buffers after lifting them into Go values. The build
compiles upstream [componentize-go v0.4.3](https://github.com/bytecodealliance/componentize-go/commit/148dba505f8c6c64ad84db777cfde5e34e25098b)
with a Cargo dependency override for the corresponding
[binding-generator correction](https://github.com/carsonfarmer/wit-bindgen/commit/fd8f26d9b019b853770c0446ab27530567e51c39).
These ownership fixes have not been submitted upstream. Source revisions are
pinned in `examples/git/Makefile` and `go.mod`; their upstream licenses apply.

## Design acknowledgements

The protocol is informed by Cursor's
[*Git at any scale*](https://cursor.com/blog/git-at-any-scale),
[Micelio](https://github.com/tuist/micelio), and Git's object model. These are
design references, not incorporated source. Git itself is GPL-2.0-only;
Micelio is MPL-2.0.

## Dependency inventories

- [`licenses/rust-direct.tsv`](licenses/rust-direct.tsv) records resolved direct
  Rust dependency names, versions, and declared license expressions.
- [`licenses/go.csv`](licenses/go.csv) records the Go packages used by the Git
  example. Its Rust component build tools use Apache-2.0 WITH LLVM-exception.
- Lockfiles remain authoritative for exact dependency versions.

Audit the resolved dependencies when lockfiles change:

```sh
cargo install cargo-license --version 0.7.0 --locked
for manifest in Cargo.toml crates/object-log-kv/Cargo.toml examples/wal-component/Cargo.toml; do
    cargo license --manifest-path "$manifest" --all-features --direct-deps-only --tsv
done

cd examples/git
make bindings
go run github.com/google/go-licenses/v2@v2.0.1 report \
    --ignore object-log-git-proof ./...
```

These commands emit raw reports. Review, normalize, and deduplicate their
output into the tracked inventories, preserving fork-specific revision URLs.

Before distributing the composed Git component, collect dependency license and
notice files from the same locked build inputs and include this project's
license plus the retained adapter license.
