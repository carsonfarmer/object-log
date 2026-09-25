# Third-party software

Project code is Apache-2.0; see [LICENSE](LICENSE). `object-log` is the only
publishable Cargo package. The key-value example and WASIp2 component are also
Apache-2.0 and set `publish = false`. Dependencies retain their own licenses.

## Git component dependencies

The build uses the preview1 adapter bundled with unmodified
[componentize-go v0.4.3](https://github.com/bytecodealliance/componentize-go/releases/tag/v0.4.3).
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

The service serializes allocating component calls and releases imported-buffer
pins through go-pkg's public `Unpin` function after the bindings return Go
values. Build inputs are recorded in `examples/git/Makefile` and `go.mod`;
their upstream licenses apply.

## Native Spin provider

The isolated [Spin key-value provider](integrations/spin-key-value/README.md)
uses unmodified [Spin v4.1.0](https://github.com/spinframework/spin/releases/tag/v4.1.0).
Spin is Apache-2.0 WITH LLVM-exception. The opt-in guest test fetches and compiles
that release's unmodified key-value test component and helper; it does not
vendor them into this repository. The provider is Apache-2.0 and unpublished.
Its separate Cargo.lock records its native and development dependencies.

Spin 4.1 transitively resolves the OpenTelemetry SDK version affected by
[GHSA-w9wp-h8wv-79jx](https://github.com/open-telemetry/opentelemetry-rust/security/advisories/GHSA-w9wp-h8wv-79jx).
The provider has no HTTP boundary. The optional hosted Git example constrains
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
  Rust packages in three workspaces.
- [`licenses/go.csv`](licenses/go.csv) records the Go packages used by the Git
  example. Its Rust component build tools use Apache-2.0 WITH LLVM-exception.
- Lockfiles remain authoritative for exact dependency versions.

Audit the resolved dependencies when lockfiles change:

```sh
cargo install cargo-license --version 0.7.0 --locked
for manifest in Cargo.toml crates/object-log-kv/Cargo.toml examples/wal-component/Cargo.toml integrations/spin-key-value/Cargo.toml; do
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

Before distributing a runtime that embeds the native Spin provider, collect
licenses and notices from its separate locked workspace as well. Include this
project's license and Spin's Apache-2.0 WITH LLVM-exception terms.
