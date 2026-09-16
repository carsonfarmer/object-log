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

The Git example uses a reviewed go-git fork revision through `go.mod`. Its
streaming decoder changes are proposed in
[go-git #2379](https://github.com/go-git/go-git/pull/2379). Go, WIT, and WASI
dependencies retain their upstream licenses.

## Design acknowledgements

The protocol is informed by Cursor's
[*Git at any scale*](https://cursor.com/blog/git-at-any-scale),
[Micelio](https://github.com/tuist/micelio), and Git's object model. These are
design references, not incorporated source. Git itself is GPL-2.0-only;
Micelio is MPL-2.0.

## Dependency inventories

- [`licenses/rust-direct.tsv`](licenses/rust-direct.tsv) records resolved direct
  Rust dependency names, versions, and declared license expressions.
- [`licenses/go.csv`](licenses/go.csv) records the Go packages and build tool
  used by the Git example.
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
    --ignore object-log-git-proof ./... github.com/bytecodealliance/componentize-go
```

These commands emit raw reports. Review, normalize, and deduplicate their
output into the tracked inventories, preserving fork-specific revision URLs.

Before distributing the composed Git component, collect dependency license and
notice files from the same locked build inputs and include this project's
license plus the retained adapter license.
