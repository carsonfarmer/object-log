# object-log WAL component

This unpublished WASIp2 component exposes the generic Rust WAL to the Go Git
example. It contains no Git policy and introduces no second durable authority.
Sessions, object handles, and prepared candidates retain the core library's
authenticated staging proofs.

The component uses the established `object_store` S3 client through a WASI HTTP
transport. `latest-complete-state` verifies the checkpoint and active tail,
returning the latest complete record and exact tail length while referenced
application objects remain lazy. Byte writers and readers delegate chunk
geometry, authenticated length, bounded offset reads, and garbage-collection
reachability to the core.

Build and compose it from the repository root:

```sh
make git-check
make git-build
```

The second command creates the final `examples/git/git.wasm`. See the
[Git example guide](../git/README.md) for local Spin, MinIO, configuration, and
provider tests.

This adapter configures WAL objects up to 2 MiB. With the current authenticated
chunk geometry, one byte stream can represent up to 2 GiB. Incoming Git packs
use the same API without publishing their temporary stream roots.

The build uses componentize-go's bundled adapter; dependency revisions are in
[THIRD_PARTY.md](../../THIRD_PARTY.md). Native tests cover the component library;
strict native and WASIp2 Clippy checks run as part of `make git-check`.
