# Optional remote Git host

Run the Git service on one disposable EC2 host with HTTPS, Cognito and S3.

Use the protected external directory and Terraform commands in [README.md](README.md).
Build the integrated service and stage its archive outside the checkout:

```sh
make git-build
tar -C examples/git -czf "$qualification_state/git.tar.gz" spin.toml git.wasm
```

Enable the commented `host_*` inputs in `terraform.tfvars.example`. For a domain,
supply an unused `host_name` in an existing public `host_route53_zone_id`.
Alternatively, leave both empty to use the Elastic IP with a publicly trusted
certificate; no registered domain or DNS zone is needed.
Choose an existing public subnet with internet routing. Empty `host_subnet_id`
selects a default subnet in the account's default VPC; accounts without one need
an explicit subnet. The host has an Elastic IP and accepts ports 80/443 only.
Management uses SSM, with no SSH port or key.

The configurable default is `t3.xlarge` (16 GiB) for the concurrent large-object
qualification fixtures. Two concurrent 513 MiB push/clone/update/fetch lifecycles
passed on this host size with a 5.30 GiB service cgroup peak. This is a tested
workload, not an application memory limit or an arbitrary-concurrency guarantee.
The service now defaults to 64 MiB objects. Set
`host_git_max_object_bytes = 537919488` for the 513 MiB qualification fixture;
use that larger limit only on a host with measured headroom.

`host_repositories` provisions canonical names, unique stable WAL IDs, SHA-1 or
SHA-256 formats, default branches, and independent read/write/admin groups.
Declare all referenced groups in `host_groups`. Users and membership are managed
in Cognito outside Terraform; self-registration is disabled. Adding a repository
requires configuration, not rebuilding. An authorized first-push discovery
materializes it. Do not change a stored repository's format or identity to migrate it.

The artifact is staged under this run's `artifacts/` prefix and SHA-256 checked
at boot. Data uses the separate `git/` prefix. The EC2 role can read that artifact
and read/write/delete only that data prefix. IMDSv2 is required. No static S3 key
or OAuth secret enters Spin variables or EC2 user data. The SSM core policy's
broad Parameter Store reads are denied outside the worker's secret parameter.
The role needs conditional writes even if Git pushes are disabled: clone and
fetch register and release readers in the WAL head to prevent concurrent
collection from deleting objects they use. On each Spin start, the maintenance
identity authenticates one loopback probe against the same backend configuration
before Caddy forwards traffic.
The probe and later collection require list and delete access. A local runtime
readiness file is cleared before every start and written only after validation;
it is never a durable WAL authority. Caddy blocks the internal probe route, and
its loopback listener gates every maintenance request during Spin restarts.
The maintenance secret still exists in **Terraform state and saved plans**;
protect those local files as credentials and never commit them.

Review and apply the plan using the README commands. Caddy obtains and renews
HTTPS certificates after the Elastic IP is associated and, for domain hosting,
DNS points at the host. The IP path requires Caddy 2.11.4 or later and uses
Let's Encrypt's `shortlived` profile with [160-hour certificates](https://letsencrypt.org/2026/01/15/6day-and-ip-general-availability).
Allow bootstrap and certificate issuance to finish before testing. Use the
`host_service_url` output with normal certificate verification. Artifact or
configuration changes replace this disposable host; this is not a rolling
deployment. Drain readers before replacement. Lost retentions require the
service's explicit drained recovery procedure.

```sh
instance_id="$(terraform -chdir="$terraform_dir" output -raw host_instance_id)"
region="$(terraform -chdir="$terraform_dir" output -raw aws_region)"
aws --profile "$admin_profile" --region "$region" ssm start-session --target "$instance_id"
# In the session:
sudo cloud-init status --wait
sudo journalctl -u object-log-git -u caddy -u object-log-maintenance
sudo systemctl list-timers object-log-maintenance.timer
```

Spin binds to `127.0.0.1:3000`; Caddy serves public HTTPS. The local maintenance
worker calls Caddy's `127.0.0.1:8081` listener with its Cognito token, so cleanup does not
depend on the Git service's public DNS or certificate renewal. The systemd timer runs
after boot, then `host_maintenance_interval` seconds after each completed run,
without overlap. A run starts with `/maintenance` per repository, then uses
`/collect` after `more` or `retained`. Every repository has an absolute
`host_maintenance_budget_seconds` deadline; systemd also limits the total run to
that budget times the repository count plus 65 seconds for setup/cleanup.
Unmaterialized repositories are skipped; an error does not prevent other
repositories from being serviced. There is no job database or detached component
work. Graph scans and S3 costs still need measurement on the deployed workload.

Caddy rejects an inbound W3C `baggage` header above 8,192 bytes or 64 list
members with HTTP 431 before proxying it to Spin. This bounds the availability
issue in [GHSA-w9wp-h8wv-79jx](https://github.com/open-telemetry/opentelemetry-rust/security/advisories/GHSA-w9wp-h8wv-79jx)
while Spin 4.1 still resolves the affected OpenTelemetry SDK. Remove this gateway
rule only after upgrading to a fixed Spin release and rerunning hosted
qualification. The coordinated upstream upgrade is tracked in
[spinframework/spin#3598](https://github.com/spinframework/spin/issues/3598).

By default, conflicts, pending publication, and retained readers wait until the
next timer. Continuous traffic can prevent collection from progressing. Set
`host_maintenance_pause_seconds` explicitly to enable an optional ingress gap,
bounded by the repository budget. During each repository's gap, Caddy returns
503 with Retry-After for new ordinary requests; existing requests continue.
Exact configured POST admin paths still reach Spin and require its authentication.
The worker retries retained/conflict/pending outcomes within the gap, switching
permanently to physical collection after pruning. Each gap ends when that pass
finishes or its deadline expires. Sequential repositories can produce consecutive
gaps; the maximum is their count times the configured pause. A `finally` removes
the marker, and systemd's runtime directory cleanup handles a killed worker.
Progress depends on admitted requests finishing inside the gap and on configured
operation deadlines. Lost retentions still require manual drained recovery;
the worker never clears them.

## Interactive Git authentication

Use released `git-credential-oauth` v0.17.2 and Git 2.45 or newer for expiry and
refresh support. For HTTP/2, Git's linked libcurl must include the
[upload EOF fix](https://github.com/curl/curl/commit/6a095da1f3c836fa7e06510720555746b1353506),
released in curl 8.5.0; a backport also suffices. libcurl 7.88.1 can hang after a
streamed Git upload. Stock Git 2.47.3 with libcurl 8.14.1 passed that transfer.
Check the library linked to Git; upgrading the standalone `curl` command alone
may leave Git using the older library.

Keep a secure credential storage helper configured **before** `oauth`.
Read `terraform output -json host_cognito` and substitute its client and
endpoint values below. Use the `host_service_url` output in place of
`https://git.example.com`, including when connecting by IP.

```sh
git config --global credential.https://git.example.com.oauthClientId CLIENT_ID
git config --global credential.https://git.example.com.oauthScopes 'openid git/access'
git config --global credential.https://git.example.com.oauthAuthURL https://COGNITO_DOMAIN/oauth2/authorize
git config --global credential.https://git.example.com.oauthTokenURL https://COGNITO_DOMAIN/oauth2/token
git config --global credential.https://git.example.com.oauthRedirectURL http://localhost:53119
git config --global --add credential.https://git.example.com.helper oauth
# After an authorized writer has pushed to this repository:
git clone https://git.example.com/team/project.git
```

A fresh configured repository returns 404 to reads until a writer's first push
materializes it. Push from a local repository with the configured object format
and initial branch before trying to clone it.

The helper uses authorization-code S256 PKCE and sends the access token as an
ordinary Basic password. Its fixed `http://localhost:53119` callback matches the
registered public client; the random loopback-port default is unsuitable.
Request both scopes so Cognito includes the `cognito:groups` claim used for
repository permissions. The service requires an access token with `git/access`.
Cognito supports PKCE but this client setting does not require it server-side.
Refresh uses the token endpoint; the issuer/JWKS hostname is a different endpoint.
For the finite live expiry test, set `host_access_token_minutes = 5` through
Terraform. The default is 60 minutes. The provider suite captures one fixed
access token and does not refresh it during a run. For longer finite tests,
temporarily choose a lifetime that covers the planned run and obtain a fresh
token after applying it. Restore the default when testing is finished.

The separate confidential maintenance client has only the client-credentials
grant and `git/access git/maintenance` scopes. Its tokens authorize administrative
service actions only, never Git reads or writes.

These settings follow the [helper's tagged source](https://github.com/hickford/git-credential-oauth/blob/v0.17.2/main.go)
and Cognito's [authorization](https://docs.aws.amazon.com/cognito/latest/developerguide/authorization-endpoint.html)
and [token](https://docs.aws.amazon.com/cognito/latest/developerguide/token-endpoint.html)
contracts. For interactive users, repository read, write and admin groups are
independent; an empty group list denies that action. Local JWT checks cannot
immediately detect revocation of either interactive or maintenance tokens.

## Test the hosted service

Use a disposable user with read/write/admin groups for the test repositories.
Qualify the separate permission levels and credential helper before running the
full provider suite, using a different repository for those checks. Keep the
service URL public HTTPS throughout. The provider fixtures must have fresh WAL
IDs or a fresh prefix before the first run and every rerun.

The suite uses `sha1.git` (SHA-1) and `sha256.git` (SHA-256), each configured with
default branch `main` and a distinct WAL ID. To include repository isolation,
also configure `alpha/project.git` and `beta/project.git` as SHA-1 and
`hash256/project.git` as SHA-256, with separate IDs and the same test permissions;
set `GIT_MULTI_REPOSITORIES=1`. These are fixture names, not service restrictions.

The deterministic provider tests assert exact cleanup counts. In the SSM host
session, stop the timer and let any running worker finish before starting them:

```sh
sudo systemctl stop object-log-maintenance.timer
sudo systemctl show object-log-maintenance.service --property=ActiveState
```

Wait for `inactive` or `failed`; do not kill a worker in the middle of collection.
After the suite, start `object-log-maintenance.timer` again and test automatic
cleanup separately with ordinary pushes/ref deletions and idle repositories.
Stopping the timer for deterministic assertions does not qualify automation.

Stock Spin may omit response trailers. Use the response's `X-Request-ID` to match
a Git storage operation to its component usage record, formatted as
`wal POST /repository.git/git-upload-pack id=... calls=... bytes=...`.
When trailers are absent, the provider tests use `GIT_PROBE_LOG` and wait up to
10 seconds for that request's complete log line. Only complete lines with the
matching request ID supply its counters.

Stream the component logs through SSM to the client running the tests. In a
separate terminal, with the AWS Session Manager plugin installed:

```sh
umask 077
aws --profile "$admin_profile" --region "$region" ssm start-session \
  --target "$instance_id" --document-name AWS-StartInteractiveCommand \
  --parameters '{"command":["sudo journalctl -u object-log-git.service --follow --lines=0 --output=cat --no-pager"]}' \
  >> "$qualification_state/spin.log"
```

After interactive login has configured the credential helper, obtain the token
without printing it and run the existing tests from the repository root:

```sh
export GIT_PROBE_URL="$(terraform -chdir="$terraform_dir" output -raw host_service_url)"
export GIT_PROBE_LOG="$qualification_state/spin.log"
GIT_PROBE_PASSWORD="$(printf 'url=%s\n\n' "$GIT_PROBE_URL" | git credential fill | sed -n 's/^password=//p')"
export GIT_PROBE_PASSWORD
test -n "$GIT_PROBE_PASSWORD" && make git-provider-test
unset GIT_PROBE_PASSWORD
```

Start the log stream before testing and keep it running without rotating the
file. Confirm complete usage lines arrive promptly; stalled SSM delivery prevents
the counter assertions from passing. SSM carries only logs; Git traffic goes
through public HTTPS. Avoid unrelated requests to those repositories during
counter assertions.

## Local validation and teardown

```sh
terraform -chdir="$terraform_dir" fmt -check -recursive
terraform -chdir="$terraform_dir" validate
terraform -chdir="$terraform_dir" test # Mock provider only; no AWS resources.
(cd "$terraform_dir" && python3 -m unittest -v test_maintenance.py)
```

For teardown, stop the timer and Git service through SSM, drain traffic, then
follow the README's prefix cleanup and destroy procedure. The same Terraform
state owns the host, Elastic IP, optional DNS record, instance role, Cognito
pool/clients, SSM parameter, artifact, and bucket. The existing domain and subnet
are retained.
