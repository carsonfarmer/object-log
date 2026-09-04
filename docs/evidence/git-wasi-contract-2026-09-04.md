# Git WASI contract evidence

Date: 2026-09-04

Revision: `b6f2086e48aeaf46755dd16e8a258d10d57fbe6d`

Issue #17 tranche 1 establishes a build and dependency boundary. It does not
add a second Git engine or a speculative protocol API.

## Result

`object-log-git` compiles for `wasm32-wasip2` without default features. Its
direct common dependencies are:

- `bstr`, with only allocation and standard-library support;
- `bytes`;
- `gix-validate`;
- `minicbor`;
- `object-log`; and
- `thiserror`.

The common graph has no direct high-level `gix`, `gix-pack`, or Tokio
dependency. The existing filesystem repository, pack writer, blocking tasks,
and native-only error variants are behind the default `native-oracle` feature.
The `aws` feature selects that oracle because the current MinIO and AWS
qualification still use it.

Ref validation now calls `gix-validate` directly. It preserves the current
UTF-8, `refs/heads/`, and `refs/tags/` policy without pulling in the high-level
Git reference stack.

No public service, protocol-version, request, response, limit, host, or
replacement repository type was added. Protocol methods and limits remain
deferred until code enforces them.

## CI gate

`rust-toolchain.toml` installs `wasm32-wasip2`. The standard `make check` target
now runs:

```sh
cargo +1.97.1 check -p object-log-git --lib \
  --target wasm32-wasip2 --no-default-features
```

GitHub CI already runs `make check`, so every push and pull request now checks
this target.

## Local verification

The integrated `make check` completed in 26.4 seconds. It passed:

- formatting;
- strict workspace Clippy for all targets and features;
- 230 regular workspace tests;
- nine expected ignored tests;
- all documentation tests; and
- the WASIp2 no-default build.

Focused checks also passed for the native no-default library, the all-feature
Git library, and WASIp2 Clippy. The native Git tests passed 27 tests and ignored
two opt-in tests.

An independent Rust review accepted the feature boundary, dependency graph,
ref validator, build gate, helper count, and public API shape without a finding.

## Line change

| Category | Added | Removed |
| --- | ---: | ---: |
| Product | 21 | 6 |
| Tests | 1 | 1 |
| Build and CI configuration | 16 | 6 |
| Documentation | 0 | 0 |

The product increase is 15 net lines. Most additions are feature gates around
the temporary native modules and error variants.

## Limits

This result proves compilation and dependency isolation. It does not prove a
working WASI pack engine, protocol v2, a linked Spin component, or WASI runtime
memory use. Those are later issue #17 tranches.

The common pack dependency remains deferred. Enabling `gix-pack`'s `wasm`
feature in the same native build would remove the bundle-writer API used by the
oracle. The pack tranche must use target-specific feature selection or delete
the oracle path before it enables that feature.
