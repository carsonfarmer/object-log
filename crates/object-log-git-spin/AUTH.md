# Private single-repository Git access

Spin defaults to `auth_mode = "basic"` and refuses known Git routes until at
least one HTTP token is configured. The username is `git`. Each token is exactly
64 hexadecimal characters representing 32 random bytes. A read token permits
clone/fetch; a write token also permits push. Configure distinct read/write
tokens. Empty tokens disable that role. These HTTP credentials are separate from
S3 credentials. The local maintenance operator remains privileged through S3.

Use `auth_mode = "disabled"` only for an explicitly trusted local fixture. That
mode rejects nonempty token settings. `read_only = "true"` denies receive-pack,
including discovery and its authentication probe, even for a writer. All auth,
object-format, and boolean policy settings are validated before storage or body
consumption. Invalid configuration returns a static 500; missing, malformed or
incorrect credentials receive 401 and a Basic challenge; insufficient scope
receives 403. Unknown Git routes return 404. The adapter issues no redirects.

## HTTPS deployment recipe

No public deployment is performed or approved by this document. For a reviewed
private deployment, terminate HTTPS on a trusted reverse proxy and bind Spin
only to `127.0.0.1`, on the same protected host. Restrict host/network access so
clients cannot bypass HTTPS or read the private backend traffic. Basic
credentials are reusable secrets: they must never cross an untrusted plaintext
connection. Provision a valid certificate using the deployment's existing TLS
process before admitting Git clients. A minimal Caddy routing fragment is:

```caddyfile
https://git.example.internal {
    reverse_proxy 127.0.0.1:3000
}
```

Replace the hostname with the reviewed private deployment name and provide its
trusted TLS configuration. The proxy must preserve Authorization and
Git-Protocol headers, reject ambiguous duplicate Authorization values, and must
not log Authorization, request bodies, or credential-bearing URLs. The only
trusted backend is the loopback Spin listener; do not proxy this credential to
another origin. Configure Git remotes directly with the final HTTPS URL, without
credentials and without relying on HTTP-to-HTTPS redirects.

Store repository variables in a mode-0600 file outside the checkout. Generate
random values directly into private files (for example Python's
`secrets.token_hex(32)`); do not print them, commit them, put them in shell
history, or pass them as `--variable auth_write_token=...` arguments. Set
`auth_mode`, `auth_read_token`, and `auth_write_token` in that private repository
TOML alongside the existing storage/policy settings. Invoke:

```sh
spin up --from spin.toml --listen 127.0.0.1:3000 --variable @/private/repository.toml
```

Use a Git credential helper that retrieves the HTTP token from your existing
secret manager or a private mode-0600 file. Enable path scoping:

```sh
git config --global credential.useHttpPath true
git clone https://git.example.internal/repo
```

The helper returns `username=git` and `password=<token>` through Git's helper
pipe after checking the protocol, hostname, and repository path. It must not
return the credential for other origins/paths. Use the read token for clients
that only need clone/fetch. The acceptance fixture in `tests/check_auth_git.py`
uses this real helper flow with a mode-0600 token file; it does not inject an
Authorization header through `http.extraHeader`.

Rotate by generating replacement tokens, updating the private server variables,
and restarting **all** serving hosts with the new configuration. Update client
secret stores separately. Requests already admitted under the previous token
may finish. A rolling restart leaves the previous token valid on old instances
until they stop; stop all old hosts to complete revocation. S3/operator
credentials need their own rotation procedure. No user database, tenant model,
or core WAL authorization protocol is introduced.

## Local verification

Build the release component first. From the repository root:

```sh
cargo build --locked -p object-log-git-spin --target wasm32-wasip2 --release
python3 crates/object-log-git-spin/tests/check_auth.py
./scripts/test-minio.sh auth_minio auth_minio object-log-git-spin ''
```

The HTTP fixture counts backend calls and sends no POST body bytes despite a
nonzero Content-Length. The MinIO fixture uses unchanged Git for both SHA-1 and
SHA-256, writer push, reader clone/fetch and denied push, cold restart, token
rotation, read-only denial, and exact head bytes/ETag preservation on rejection.
Tests bind only to loopback and use ordinary Spin runtime settings;
builds and Wasmtime cache preparation remain outside the serving budget.
