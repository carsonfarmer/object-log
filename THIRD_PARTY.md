# Third-party software

Project code is Apache-2.0; see [LICENSE](LICENSE). `object-log` is the only
publishable Cargo package. The WASIp2 component is also Apache-2.0 and sets
`publish = false`. Dependencies retain their own licenses.

## Git component dependencies

The build uses the preview1 adapter bundled with unmodified
[componentize-go v0.4.3](https://github.com/bytecodealliance/componentize-go/releases/tag/v0.4.3),
installed from that tag's [revision `148dba5`](https://github.com/bytecodealliance/componentize-go/commit/148dba505f8c6c64ad84db777cfde5e34e25098b).
The adapter's Apache-2.0 WITH LLVM-exception license is reproduced in
[`licenses/wasmtime.txt`](licenses/wasmtime.txt) and must accompany redistributed
adapter binaries.

The Git example uses unmodified upstream [go-git at revision `0f3a0a2`](https://github.com/go-git/go-git/commit/0f3a0a2c25513f2666ac9b88a6745f7b2382f572).
It uses the public pack parser, object storage, stored-delta, and receive-hook
interfaces. Incoming deltas use go-git's normal buffering. Receive-pack
advertises `no-thin`, so ordinary Git clients include the bases their packs need.
The [v6.0.0-alpha.5 release](https://github.com/go-git/go-git/releases/tag/v6.0.0-alpha.5)
lacks `plumbing.ValidateBranchName`, which `repositories.go` calls. Keep the
reviewed upstream revision until a v6 release provides that API and passes the
Git build and provider suites.
The selected go-git revision requires the prerelease `go-billy/v6`
`v6.0.0-alpha.2`; `examples/git/go.mod` and `go.sum` record its exact module
version and checksums.

The Go SDK pins [go-pkg PR #13](https://github.com/bytecodealliance/go-pkg/pull/13)
at its contributor's exact [revision `af8c737`](https://github.com/ricochet/go-pkg/commit/af8c737ad573d76dd08cf2927baf2660475c5c01).
The change postpones garbage collection during canonical allocation, allowing
the stock adapter to run the Go component. The PR is unmerged; the module
replacement uses that revision unchanged, with no project-specific SDK patch.
The compatible unmodified SDK revision traps on ordinary pushes when garbage
collection starts in `cabi_realloc`. Upstream v0.3.0 changes an SDK API used by
the service but does not include PR #13. Remove the replacement when an
upstream release provides equivalent behavior and the composed Git provider
suite passes. The binding generator is the version bundled with componentize-go.

The WASIp2 component uses `object_store` 0.14.2 from its Cargo.lock; its
manifest accepts compatible 0.14 releases. `spin-sdk` 5.2.0 is used only by
the credential-test guest, not the reusable WAL component, and is also locked
by that workspace's Cargo.lock.

The service serializes allocating component calls and releases imported-buffer
pins through go-pkg's public `Unpin` function after the bindings return Go
values. Build inputs are recorded in `examples/git/Makefile` and `go.mod`;
their upstream licenses apply.

## Optional TypeScript repository viewer

The viewer uses Bun, TypeScript, Biome, Preact, `fflate` and the Spin JavaScript
SDK. Its `examples/git/viewer/bun.lock` records exact versions
and package integrity. Their upstream licenses apply.

The compiler uses a temporary package based on official ComponentizeJS 0.23.0,
with the binding splicer rebuilt from
[revision `a47770b`](https://github.com/carsonfarmer/ComponentizeJS/commit/a47770bd1b63a7497d785c5b32e841812de16700).
[Upstream PR #357](https://github.com/bytecodealliance/ComponentizeJS/pull/357)
fixes missing classes for imported resources without methods, reported in
[issue #221](https://github.com/bytecodealliance/ComponentizeJS/issues/221).
The packaged JavaScript bindings and all engine/cache files remain identical
to the official release; only the compiled binding splicer changes. Source and
build details accompany the
[temporary package](https://github.com/carsonfarmer/ComponentizeJS/releases/tag/opaque-resource-v0.23.0).
Remove the package override once an upstream release includes this fix.
The compiler change adds no viewer behavior or Git-specific code.

## Hosted Spin runtime

The optional Git host runs unmodified [Spin v4.1.0](https://github.com/spinframework/spin/releases/tag/v4.1.0),
which is Apache-2.0 WITH LLVM-exception.

Spin 4.1 transitively resolves the OpenTelemetry SDK version affected by
[GHSA-w9wp-h8wv-79jx](https://github.com/open-telemetry/opentelemetry-rust/security/advisories/GHSA-w9wp-h8wv-79jx).
The optional hosted Git example constrains
untrusted `baggage` headers in Caddy pending the coordinated upstream upgrade in
[spinframework/spin#3598](https://github.com/spinframework/spin/issues/3598).

## Design acknowledgements

The protocol is informed by Cursor's
[*Git at any scale*](https://cursor.com/blog/git-at-any-scale),
[Micelio](https://github.com/tuist/micelio), and Git's object model. These are
design references, not incorporated source. Git itself is GPL-2.0-only;
Micelio is MPL-2.0.

## Dependency inventories

- [`licenses/rust-direct.tsv`](licenses/rust-direct.tsv) records resolved direct
  Rust dependency names, versions, and reviewed license expressions across all
  Rust packages in two workspaces.
- [`licenses/go.csv`](licenses/go.csv) records the Go packages used by the Git
  example. Its Rust component build tools use Apache-2.0 WITH LLVM-exception.
- Lockfiles remain authoritative for exact dependency versions.

Audit the resolved dependencies when lockfiles change:

```sh
cargo install cargo-license --version 0.7.0 --locked
for manifest in Cargo.toml examples/wal-component/Cargo.toml; do
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
license plus the bundled adapter license.
