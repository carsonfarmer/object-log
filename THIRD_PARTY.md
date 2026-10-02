# Third-party dependencies

Project source is [Apache-2.0](LICENSE). Dependencies retain their upstream
licenses. This repository contains source and build instructions; compiled
components and browser bundles are generated locally. Cargo, Go and Bun
lockfiles record the resolved dependencies.

## Git example

The example uses unmodified upstream [go-git revision `6bcff7e`](https://github.com/go-git/go-git/commit/6bcff7e44598c9a22a5b3ca79e5394c4b005206a),
which includes the delta-reader rewind fix from [PR #2379](https://github.com/go-git/go-git/pull/2379).
The v6.0.0-alpha.5 release lacks the `plumbing.ValidateBranchName` API it uses.
`examples/git/go.mod` also records the compatible go-billy v6 prerelease.

The build uses unmodified [componentize-go v0.4.3](https://github.com/bytecodealliance/componentize-go/releases/tag/v0.4.3)
and its bundled preview1 adapter. The Go SDK replacement selects the unchanged
[go-pkg PR #13](https://github.com/bytecodealliance/go-pkg/pull/13) contributor
[revision `af8c737`](https://github.com/ricochet/go-pkg/commit/af8c737ad573d76dd08cf2927baf2660475c5c01).
It postpones garbage collection during canonical allocation, where the released
SDK can trap. Remove this replacement when an upstream version provides the
fix and passes the composed Git tests.

## Repository viewer

The viewer's compiler override is based on ComponentizeJS 0.23.0 with the binding
splicer rebuilt from [revision `a47770b`](https://github.com/carsonfarmer/ComponentizeJS/commit/a47770bd1b63a7497d785c5b32e841812de16700).
[PR #357](https://github.com/bytecodealliance/ComponentizeJS/pull/357) fixes missing
classes for imported resources without methods, reported in
[issue #221](https://github.com/bytecodealliance/ComponentizeJS/issues/221).
Only the binding splicer changes; the packaged JavaScript and engine/cache files
match the official release. The [temporary package](https://github.com/carsonfarmer/ComponentizeJS/releases/tag/opaque-resource-v0.23.0)
includes its source and build details. Remove the override when an upstream
release includes the fix.

## Optional host

The AWS example runs unmodified Spin 4.1.0. Its Caddy configuration limits
`baggage` headers while Spin's OpenTelemetry dependency is affected by
[GHSA-w9wp-h8wv-79jx](https://github.com/open-telemetry/opentelemetry-rust/security/advisories/GHSA-w9wp-h8wv-79jx).
The coordinated dependency upgrade is tracked in
[spinframework/spin#3598](https://github.com/spinframework/spin/issues/3598).

## Design references

The WAL design draws on Cursor's [Git at any scale](https://cursor.com/blog/git-at-any-scale)
and [Micelio](https://github.com/tuist/micelio). These are design references;
the repository does not incorporate their source code.
