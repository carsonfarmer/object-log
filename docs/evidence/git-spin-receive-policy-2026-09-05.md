# Spin receive policy and notes acceptance

The Spin host now exposes `allow_non_fast_forward`, a strict boolean with
`false` as its manifest default. Setting it to `true` selects the shared
`ReceivePolicy::AllowNonFastForward` API introduced at `ccfafcf`. Existing
old-ID checks, object/connectivity validation, and conditional WAL publication
remain in the Git library. The host does not infer authorization from a client
force flag. This is a server write policy, not authentication.

`read_only=true` rejects receive discovery and POSTs with 403 before the rewrite
policy, request body, or repository is loaded. Invalid boolean configuration
fails closed. Ordinary writes remain fast-forward-only unless explicitly enabled.

## Conditions and results

Local macOS, Git 2.54.0 (Apple Git-157), Spin 4.0.2, Rust 1.97.1, the repository's
pinned MinIO container, and a release WASIp2 component. The normal launcher
uses its existing single-instance and unpooled outbound HTTP configuration.
No live AWS, deployment, concurrency, memory-headroom, or authentication claim
is made by this fixture.

The unchanged-client MinIO test passes for SHA-1 and SHA-256:

- A correct explicit `--force-with-lease` rewind is rejected by the default
  server policy and the remote branch remains unchanged.
- After restarting with rewrites enabled, the same lease and rewind succeed.
- A later stale explicit lease is rejected by Git; the remote remains unchanged.
  Shared-engine tests separately exercise stale incoming old IDs and CAS races.
- Ordinary Git pushes and fetches preserve notes and `refs/archive/saved`.
- A cold read-only host clones the rewound exact HEAD, retrieves the note, and
  passes strict Git validation. It rejects a correctly leased push with 403,
  despite rewrites being enabled; the durable head is unchanged.

The actual-component HTTP fixture passes 12 both-hash policy combinations,
including omitted/default and enabled rewrite configuration, invalid booleans,
and read-only precedence. Its existing 50 ms inter-request gap isolates policy
checks from the separately tracked #21 instance-release admission race.

Seven native Spin library tests, strict native all-target Clippy, strict WASIp2
Clippy, formatting, and the release component build pass. Root integration and
independent review remain separate acceptance steps.

Commands:

```sh
mise exec -- cargo build --locked -p object-log-git-spin --target wasm32-wasip2 --release
mise exec -- python3 crates/object-log-git-spin/tests/check_http.py
mise exec -- ./scripts/test-minio.sh minio spin_minio_force_with_lease_and_notes_obey_host_policy object-log-git-spin ''
mise exec -- cargo test --locked -p object-log-git-spin --lib
mise exec -- cargo clippy --locked -p object-log-git-spin --all-targets -- -D warnings
mise exec -- cargo clippy --locked -p object-log-git-spin --target wasm32-wasip2 -- -D warnings
```

Raw output is preserved in `git-spin-receive-policy-2026-09-05/`.
