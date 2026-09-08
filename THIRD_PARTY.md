# Licensing and provenance

Project code is Apache-2.0; see [LICENSE](LICENSE). The only publishable Cargo
package is `object-log`. The KV and SQLite examples also declare Apache-2.0 and
have `publish = false`; the WASI component also declares Apache-2.0 and is
unpublished.
Dependency licenses remain their own.

## Retained source

[examples/git/adapter.patch](examples/git/adapter.patch) modifies Wasmtime's
`crates/wasi-preview1-component-adapter/src/lib.rs` at commit
[`668016926adfd1b8a79dbce894f1e203d8892599`](https://github.com/bytecodealliance/wasmtime/tree/668016926adfd1b8a79dbce894f1e203d8892599).
It changes cached clock reads and immediate timer polling during canonical
allocation. Its upstream license is Apache-2.0 WITH LLVM-exception, reproduced
in [licenses/wasmtime.txt](licenses/wasmtime.txt). The build script pins and
verifies the source archive before applying this patch. Keep that attribution
and license with the patch and any redistributed modified source.

The Git proof uses go-git (Apache-2.0) and Bytecode Alliance's Go/WASI packages
(Apache-2.0 WITH LLVM-exception) as dependencies. Generated WIT bindings use the
local interface in `examples/wal-component/wit/wal.wit` and the pinned generator.

## Design references

[Git](https://github.com/git/git/blob/master/COPYING) (GPL-2.0-only),
[Micelio](https://github.com/tuist/micelio/blob/main/LICENSE) (MPL-2.0), and
[Cursor's article](https://cursor.com/blog/git-at-any-scale) are behavior or
design references. Referencing them does not license their source or prose
under this project's Apache license. Source reuse requires a separate owner
license decision and a record of the affected files and upstream revision.

[Gitoxide](https://github.com/GitoxideLabs/gitoxide) (MIT OR Apache-2.0),
[Walgit](https://github.com/tobi/walgit/blob/main/LICENSE) (MIT), and
[Chroma WAL3](https://github.com/chroma-core/chroma/blob/f60fe42cdad202a92acad55a1f0fbf8ce757c8b1/LICENSE)
(Apache-2.0) are not current dependencies. The comparisons in
[docs/report-source.md](docs/report-source.md) describe design ideas, not
incorporated source. Record actual source reuse by file and revision if added.

## Dependency inventory

[licenses/rust-direct.tsv](licenses/rust-direct.tsv) records direct dependency
names, resolved versions, and declared license expressions for the workspace
and WASI example, including development and optional dependencies.
[licenses/go.csv](licenses/go.csv) is the Go package and build-tool license
report, including imported transitive packages. Lockfiles remain authoritative
for versions. These inventories are not substitutes for license texts in a
binary distribution.

Regenerate with `cargo-license 0.7.0` and `go-licenses v2.0.1`:

```sh
cargo install cargo-license --version 0.7.0 --locked
for manifest in Cargo.toml crates/object-log-kv/Cargo.toml crates/object-log-sqlite/Cargo.toml examples/wal-component/Cargo.toml; do
    cargo license --manifest-path "$manifest" --all-features --direct-deps-only --tsv
done

cd examples/git
make bindings
go run github.com/google/go-licenses/v2@v2.0.1 report --ignore object-log-git-proof ./... github.com/bytecodealliance/componentize-go
```

The Rust table retains the name/version/license columns, removes local packages,
and deduplicates rows. `cargo-license` can include multiple resolved versions
of a directly named crate. The Go report excludes this module's own code because
its license is at the repository root; dependencies are still inspected.
`go-licenses` warns that assembly cannot be inspected for further dependencies;
the Wasmtime adapter is accounted for separately above.

When shipping the Git component, collect the Go dependency license and notice
files using the same build inputs:

```sh
go run github.com/google/go-licenses/v2@v2.0.1 save --ignore object-log-git-proof --save_path release-licenses ./...
```

Include the project license, adapter license, and the Rust component's dependency
license/notice texts as well. Review changed dependencies and retained source
before distributing a new artifact; do not infer its notices from an older
binary or this direct-dependency inventory.
