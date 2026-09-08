# WAL component

A small WASIp2 interface to the existing Rust WAL for the Go Git consumer.
Sessions, object handles and prepared candidates retain the core's authenticated
staging proofs. Refresh shares cumulative transport counters. The WAL format,
publication point, checkpoint safety and garbage collection stay in the core.

Build and compose from the repository root with `make git-build`; see the
[sibling Git example](../git/README.md) for ordinary Spin and local MinIO setup.
The component reuses the established object_store S3 client through a WASI HTTP
transport. The core itself has no Spin dependency. WAL objects are limited to
2 MiB here; the Go consumer uses 1 MiB chunks for sparse reads.

`make git-check` runs native library tests and strict native/WASIp2 checks.
Native tests use `--lib`: the component's HTTP exports are intended for WASI,
not a native shared-library host. Local provider tests exercise the composed
component. The client README records remaining limits and the temporary
component-build adapter patch.
