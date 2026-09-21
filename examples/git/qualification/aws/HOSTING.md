# Optional remote Git host

This setup defines the remote qualification environment. Live Cognito login,
TLS, S3 credential renewal, and capacity remain untested until the deployed
service passes the gate in `GIT_PLAN.md`.

Use the protected external directory and Terraform commands in [README.md](README.md).
Build the integrated service and stage its archive outside the checkout:

```sh
make git-build
tar -C examples/git -czf "$qualification_state/git.tar.gz" spin.toml git.wasm
```

Enable the commented `host_*` inputs in `terraform.tfvars.example`. Supply an
unused hostname in an **existing public Route53 zone**; no domain is registered.
Choose an existing public subnet with internet routing. Empty `host_subnet_id`
selects a default subnet in the account's default VPC; accounts without one need
an explicit subnet. The host has an Elastic IP and accepts ports 80/443 only.
Management uses SSM, with no SSH port or key.

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
The maintenance secret still exists in **Terraform state and saved plans**;
protect those local files as credentials and never commit them.

Review and apply the plan using the README commands. Caddy obtains and renews
HTTPS certificates after DNS points at the host. Allow bootstrap and DNS
propagation to finish. Artifact or configuration changes replace this disposable
host; this is not a rolling deployment. Drain readers before replacement. Lost
retentions require the service's explicit drained recovery procedure.

```sh
instance_id="$(terraform -chdir="$terraform_dir" output -raw host_instance_id)"
region="$(terraform -chdir="$terraform_dir" output -raw aws_region)"
aws --profile "$admin_profile" --region "$region" ssm start-session --target "$instance_id"
# In the session:
sudo cloud-init status --wait
sudo journalctl -u object-log-git -u caddy -u object-log-maintenance
sudo systemctl list-timers object-log-maintenance.timer
```

Spin binds to `127.0.0.1:3000`; Caddy serves public HTTPS. The systemd timer runs
after boot, then `host_maintenance_interval` seconds after each completed run,
without overlap. A run calls `/maintenance` once per repository, then `/collect`
while the response is `more`. Every repository has an absolute
`host_maintenance_budget_seconds` deadline; systemd also limits the total run to
that budget times the repository count plus 65 seconds for setup/cleanup.
Unmaterialized repositories are skipped; an error does not prevent other
repositories from being serviced. There is no job database or detached component
work. Graph scans and S3 costs still need measurement on the deployed workload.

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
refresh support. Keep a secure credential storage helper configured **before**
`oauth`. Read `terraform output -json host_cognito`, substitute its client and
endpoint values below, and use the configured service hostname:

```sh
git config --global credential.https://git.example.com.oauthClientId CLIENT_ID
git config --global credential.https://git.example.com.oauthScopes git/access
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
Cognito supports PKCE but this client setting does not require it server-side.
Refresh uses the token endpoint; the issuer/JWKS hostname is a different endpoint.
The separate confidential maintenance client has only the client-credentials
grant and `git/access git/maintenance` scopes. Its tokens authorize administrative
service actions only, never Git reads or writes.

These settings follow the [helper's tagged source](https://github.com/hickford/git-credential-oauth/blob/v0.17.2/main.go)
and Cognito's [authorization](https://docs.aws.amazon.com/cognito/latest/developerguide/authorization-endpoint.html)
and [token](https://docs.aws.amazon.com/cognito/latest/developerguide/token-endpoint.html)
contracts. Browser login, Basic delivery, refresh, group permissions, signing-key
rotation, and IMDS renewal still need live tests. Signed test tokens do not prove
Cognito interoperability. Local JWT checks cannot immediately detect revocation.

## Local validation and teardown

```sh
terraform -chdir="$terraform_dir" fmt -check -recursive
terraform -chdir="$terraform_dir" validate
terraform -chdir="$terraform_dir" test # Mock provider only; no AWS resources.
(cd "$terraform_dir" && python3 -m unittest -v test_maintenance.py)
```

For teardown, stop the timer and Git service through SSM, drain traffic, then
follow the README's prefix cleanup and destroy procedure. The same Terraform
state owns the host, Elastic IP, DNS record, instance role, Cognito pool/clients,
SSM parameter, artifact, and bucket. The existing domain and subnet are retained.
