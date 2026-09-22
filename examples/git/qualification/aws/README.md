# Disposable AWS S3 qualification

This Terraform root provisions an isolated backend for the Git, WAL, and KV
integration tests. It creates:

- one private, unversioned, AES-256-encrypted S3 bucket;
- public-access blocking and bucket-owner-enforced ownership;
- one IAM user restricted to the configured test prefix; and
- no long-lived credential in Terraform state when hosting is disabled.

The optional qualification runner is an egress-only Amazon Linux EC2 instance
managed through SSM. It receives a source archive from the test prefix and uses
an instance role restricted to that prefix. It opens no inbound ports and is
separate from the hosted Git service.

Optional [remote hosting](HOSTING.md) adds one Ubuntu EC2 host, Caddy HTTPS,
stock Spin 4.0.2, restricted workload identity, Cognito, and periodic maintenance.
That option stores a generated maintenance client secret in sensitive Terraform
state and an SSM SecureString parameter. Follow the hosting guide's
[client requirements](HOSTING.md#interactive-git-authentication) for OAuth and
HTTP/2, including Git's linked libcurl version.

`force_destroy = false` prevents Terraform from deleting an unexpected non-empty
bucket. Never point this setup at production data.

## Provision

Use an authenticated AWS CLI profile and keep Terraform state, plans, variables,
and credentials outside the repository:

```sh
terraform_dir=examples/git/qualification/aws
qualification_state=/absolute/private/path/object-log-qualification
admin_profile=YOUR_AWS_PROFILE
region=us-west-2 # Match aws_region in terraform.tfvars.
umask 077
mkdir -p "$qualification_state"
chmod 700 "$qualification_state"
cp "$terraform_dir/terraform.tfvars.example" \
  "$qualification_state/terraform.tfvars"
$EDITOR "$qualification_state/terraform.tfvars"

aws sso login --profile "$admin_profile"
terraform -chdir="$terraform_dir" init -reconfigure \
  -backend-config="path=$qualification_state/terraform.tfstate"
terraform -chdir="$terraform_dir" validate
terraform -chdir="$terraform_dir" plan \
  -var-file="$qualification_state/terraform.tfvars" \
  -out="$qualification_state/qualification.tfplan"
terraform -chdir="$terraform_dir" apply \
  "$qualification_state/qualification.tfplan"
```

For same-region native qualification, create a source archive outside the
repository and add these settings before planning:

```sh
git ls-files --cached --others --exclude-standard -z |
  while IFS= read -r -d '' path; do
    test ! -e "$path" || printf '%s\0' "$path"
  done |
  tar --null -T - -czf "$qualification_state/source.tar.gz"
runner_ami="$(AWS_PROFILE="$admin_profile" aws --region "$region" ssm get-parameter \
  --name /aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64 \
  --query Parameter.Value --output text)"
cat >>"$qualification_state/terraform.tfvars" <<EOF
qualification_runner = true
qualification_runner_artifact_path = "$qualification_state/source.tar.gz"
qualification_runner_ami_id = "$runner_ami"
EOF
```

Read `qualification_runner_instance_id` after apply and use SSM Run Command or
Session Manager to run the KV commands below from `/opt/object-log`. The runner
role supplies S3 credentials through EC2 instance metadata; do not copy the
temporary IAM-user session onto the host. Capture `/usr/bin/time -v` with the
test output when measuring whole-process memory. Record the Terraform outputs,
`/etc/os-release`, `uname -a`, `rustc -Vv`, and the installed package versions
with the result. The runner terminates itself after three hours by default and
is removed by the same destroy command as the bucket.

The `.terraform/` directory contains ignored provider initialization data. The
managed state and saved plan stay in the protected external directory.

## Issue temporary credentials

The test IAM user normally has no access key. `issue-session.sh` creates a key,
uses it to request a four-hour STS session, deletes the key, and writes only the
session credentials to a new mode-0600 file. The helper refuses in-repository or
existing output paths.

```sh
qualification_user="$(terraform -chdir="$terraform_dir" output \
  -raw qualification_user_name)"
"$terraform_dir/issue-session.sh" \
  "$admin_profile" "$qualification_user" \
  "$qualification_state/session.json"
```

Load the session without printing it:

```sh
session_file="$qualification_state/session.json"
export AWS_ACCESS_KEY_ID="$(jq -er .AccessKeyId "$session_file")"
export AWS_SECRET_ACCESS_KEY="$(jq -er .SecretAccessKey "$session_file")"
export AWS_SESSION_TOKEN="$(jq -er .SessionToken "$session_file")"
```

## Configure and test

Read the Terraform outputs:

```sh
region="$(terraform -chdir="$terraform_dir" output -raw aws_region)"
bucket="$(terraform -chdir="$terraform_dir" output -raw bucket_name)"
prefix="$(terraform -chdir="$terraform_dir" output -raw object_prefix)"
```

Create a protected Spin variables file using the table in the
[Git service guide](../../README.md#configuration). Set:

```toml
wal_endpoint = "https://s3.REGION.amazonaws.com"
wal_bucket = "DEDICATED-BUCKET"
wal_region = "REGION"
wal_prefix = "ISOLATED-PREFIX/git"
wal_access_key = "TEMPORARY-ACCESS-KEY"
wal_secret_key = "TEMPORARY-SECRET-KEY"
wal_session_token = "TEMPORARY-SESSION-TOKEN"
git_password = "TEMPORARY-TEST-PASSWORD"
git_boot_id = "aws-test-1"
```

Build the final composed component immediately before starting Spin. Run Spin
in its own terminal:

```sh
make git-build
(cd examples/git && spin up --listen 127.0.0.1:19100 \
  --variable @"$qualification_state/spin.toml" \
  >"$qualification_state/spin.log" 2>&1)
```

From the repository root in another terminal, run the ordinary provider suite
first, followed by any opt-in large workloads. For extended remote runs, add an
explicit Go test `-timeout` that covers the planned workload and obtain temporary
credentials with enough remaining lifetime before starting.

```sh
export GIT_PROBE_URL=http://127.0.0.1:19100
export GIT_PROBE_PASSWORD=TEMPORARY-TEST-PASSWORD
export GIT_PROBE_LOG="$qualification_state/spin.log"
make git-provider-test

cd examples/git
GIT_REPEATED_PUSHES=1 go test -race -count=1 -parallel=4 -timeout=60m \
  -run '^TestRepeatedPushes$' -v ./tests
GIT_LARGE_OBJECT_MIB=513 GIT_CONCURRENT_LARGE=1 \
  go test -race -count=1 -parallel=2 -timeout=30m -run '^TestLargeBlob$' -v ./tests
```

To qualify the core directly, export the same backend and temporary credentials
under the test variable names, using a separate prefix:

```sh
export OBJECT_LOG_MINIO_ENDPOINT="https://s3.${region}.amazonaws.com"
export OBJECT_LOG_MINIO_BUCKET="$bucket"
export OBJECT_LOG_MINIO_REGION="$region"
export OBJECT_LOG_MINIO_PREFIX="${prefix}/core"
export OBJECT_LOG_MINIO_ACCESS_KEY="$AWS_ACCESS_KEY_ID"
export OBJECT_LOG_MINIO_SECRET_KEY="$AWS_SECRET_ACCESS_KEY"
export OBJECT_LOG_MINIO_SESSION_TOKEN="$AWS_SESSION_TOKEN"

cargo test --features aws,test-util --test store_conformance \
  minio_backend_conformance -- --ignored --nocapture
cargo test --features aws,test-util --test protocol \
  minio_protocol_matrix -- --ignored --nocapture
cargo test --features aws,test-util --test immutable_faults \
  minio_immutable_create_faults -- --ignored --nocapture
cargo test --features aws,test-util --test maintenance_model \
  minio_maintenance_model -- --ignored --nocapture
cargo test --features aws,test-util --test minio \
  minio_passes_recovery_checkpoint_and_gc_flow -- --ignored --nocapture
```

The large 10,001-object remote collection case is optional:

```sh
cargo test --features aws,test-util --test gc_acceptance \
  minio_gc_removes_10001_objects -- --ignored --nocapture
```

To qualify the KV library and native Spin provider against the same disposable
S3 backend, use separate child prefixes. A local process uses the temporary
session above. On the qualification runner, leave the AWS credential variables
unset so the same commands use its instance role:

```sh
export OBJECT_LOG_AWS_BUCKET="$bucket"
export OBJECT_LOG_AWS_REGION="$region"
export OBJECT_LOG_AWS_PREFIX="${prefix}/kv"
export SPIN_KV_AWS_BUCKET="$bucket"
export SPIN_KV_AWS_REGION="$region"
export SPIN_KV_AWS_PREFIX="${prefix}/spin-kv"

cargo test -p object-log-kv --features aws --test kv \
  aws_correctness_matrix -- --ignored --nocapture
cargo test -p object-log-kv --features aws --test qualification \
  aws_growth_and_contention -- --ignored --nocapture
cargo test -p object-log-kv --features aws --test qualification \
  aws_value_size_envelope -- --ignored --nocapture
(cd integrations/spin-key-value && \
  cargo test --locked --test aws -- --ignored --nocapture)
make spin-kv-guest-test
```

These tests create random descendants below the supplied prefixes. They run
the fault/recovery matrix, growth and contention workload, progressive value
sizes, the native provider, and upstream's unchanged Spin guest. They are
explicit remote tests and never run in the ordinary workspace gate.

A loopback Spin URL qualifies the application against live S3. It does not test
a deployment host's inbound TLS, routing, authentication integration, or
host-wide resource admission.

## Destroy

For a hosted service, first stop `object-log-maintenance.timer` and
`object-log-git.service` through SSM and drain traffic. Stop any local Spin too.
Delete only the configured prefix with the temporary identity:

```sh
aws --region "$region" s3 rm "s3://$bucket/$prefix/" --recursive
aws --region "$region" s3api list-objects-v2 \
  --bucket "$bucket" --prefix "$prefix/"
```

The final listing must be empty. Unset the temporary session, then use the
administrator profile to inspect and abort any incomplete multipart uploads for
the same prefix. `ListBucketMultipartUploads` is intentionally absent from the
temporary user's prefix policy because AWS scopes that operation to the bucket.

```sh
unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN
unset OBJECT_LOG_MINIO_ENDPOINT OBJECT_LOG_MINIO_BUCKET OBJECT_LOG_MINIO_REGION
unset OBJECT_LOG_MINIO_PREFIX OBJECT_LOG_MINIO_ACCESS_KEY
unset OBJECT_LOG_MINIO_SECRET_KEY OBJECT_LOG_MINIO_SESSION_TOKEN
AWS_PROFILE="$admin_profile" aws --region "$region" \
  s3api list-multipart-uploads --bucket "$bucket" --prefix "$prefix/"
```

After the object and multipart checks are empty, destroy the infrastructure:

```sh
AWS_PROFILE="$admin_profile" terraform -chdir="$terraform_dir" destroy \
  -var-file="$qualification_state/terraform.tfvars"
terraform -chdir="$terraform_dir" state list
```

The final state list must be empty. Remove the external credentials, plan, state,
and Spin variables only after destruction succeeds. A non-empty-bucket failure
requires inspection; do not enable `force_destroy` to bypass it.
