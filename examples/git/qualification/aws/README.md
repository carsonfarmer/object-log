# AWS live qualification setup

This Terraform root supports the live qualification proof; it is not part of
the generic `object-log` contract. It creates a dedicated private, unversioned,
AES-256-encrypted S3 bucket and an IAM user whose S3 access is restricted to one
campaign prefix. `force_destroy = false` prevents cleanup from widening beyond
that prefix.

The administrator profile used for this campaign is an IAM Identity Center
role session. AWS caps a role assumed from that session at one hour because it
is [role chaining](https://docs.aws.amazon.com/IAM/latest/UserGuide/troubleshoot_roles.html#troubleshoot_roles_cant-assume-role).
For the required 12 hours, `issue-session.sh` briefly creates an IAM user access
key, calls
[`GetSessionToken`](https://docs.aws.amazon.com/STS/latest/APIReference/API_GetSessionToken.html),
deletes the long-lived key, and writes the temporary credentials to a protected
file. Terraform never receives or outputs credential values.

## Provision and issue credentials

From the repository root:

```sh
terraform_dir=examples/git/qualification/aws
qualification_state=/absolute/path/outside/repository/qualification-state
umask 077
mkdir -p "${qualification_state}"
chmod 700 "${qualification_state}"
cp "${terraform_dir}/terraform.tfvars.example" "${terraform_dir}/terraform.tfvars"
$EDITOR "${terraform_dir}/terraform.tfvars"
aws sso login --profile AdministratorAccess-ACCOUNT_ID
terraform -chdir="${terraform_dir}" init -reconfigure \
  -backend-config=path="${qualification_state}/terraform.tfstate"
terraform -chdir="${terraform_dir}" plan \
  -out="${qualification_state}/qualification.tfplan"
terraform -chdir="${terraform_dir}" apply \
  "${qualification_state}/qualification.tfplan"

admin_profile=AdministratorAccess-ACCOUNT_ID
qualification_user="$(terraform -chdir="${terraform_dir}" output \
  -raw qualification_user_name)"
"${terraform_dir}/issue-session.sh" "${admin_profile}" "${qualification_user}" \
  "${qualification_state}/session-credentials.json"
```

The partial local backend keeps managed state and the saved plan in the
protected directory; `.terraform/` contains only ignored initialization data.

The helper refuses an in-repository destination, an existing output, or a
bootstrap user that already has an access key. It requests exactly 43,200
seconds, disables shell tracing, recovers a returned key ID before cleanup, and
publishes with a no-clobber hard link. If key creation returns no usable ID, the
next run detects the key and stops for administrator cleanup. Keep the IAM user
and policy until testing ends because AWS evaluates them on each session request.

Load the session without printing it or putting values in process arguments:

```sh
session_file="${qualification_state}/session-credentials.json"
export AWS_ACCESS_KEY_ID="$(jq -er .AccessKeyId "${session_file}")"
export AWS_SECRET_ACCESS_KEY="$(jq -er .SecretAccessKey "${session_file}")"
export AWS_SESSION_TOKEN="$(jq -er .SessionToken "${session_file}")"
credential_expires_at="$(jq -er .Expiration "${session_file}")"
```

Copy `examples/git/remote-qualification.env.example` beside the session file,
fill its non-secret record, and source it. Set its expiry to
`credential_expires_at` and credential source to
`iam-user:<qualification_user>:GetSessionToken`. Create the Spin variable file
from the [existing template](../../README.md#live-s3-qualification) in the same
0700 directory, set its mode to 0600, and pass only its path to Spin.

## Qualify and destroy

Run `make git-remote-qualification REMOTE_QUALIFICATION_PHASE=<phase>` from the
repository root in this order:

```text
start → backend → protocol → standard → recovery → read-only → limits
      → performance → teardown
```

Follow the [Git README](../../README.md#live-s3-qualification) for the required
profile changes, recovery restart, prefix split, and failure-review flow.

After runner teardown, capture the Terraform outputs, unset the temporary
session, use the administrator profile to abort incomplete uploads under the
exact prefix, and destroy the boundary. `ListBucketMultipartUploads` is
bucket-wide, so it is deliberately absent from the temporary user's policy.

```bash
set -euo pipefail
: "${admin_profile:?set the administrator profile}"
: "${qualification_state:?set the protected state directory}"
: "${terraform_dir:?set the Terraform directory}"
umask 077
bucket="$(terraform -chdir="${terraform_dir}" output -raw bucket_name)"
region="$(terraform -chdir="${terraform_dir}" output -raw aws_region)"
prefix="$(terraform -chdir="${terraform_dir}" output -raw object_prefix)"
unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN
multipart_file="$(mktemp "${qualification_state}/multipart-uploads.XXXXXX")"

list_uploads() {
  AWS_PROFILE="${admin_profile}" aws s3api list-multipart-uploads \
    --region "${region}" --bucket "${bucket}" --prefix "${prefix}/" \
    --output json >"${multipart_file}"
}
for attempt in 1 2 3; do
  list_uploads
  uploads_remaining="$(jq -er '(.Uploads // []) | length' "${multipart_file}")"
  (( uploads_remaining == 0 )) && break
  jq -r '.Uploads[] | [.Key, .UploadId] | @tsv' "${multipart_file}" |
    while IFS=$'\t' read -r key upload_id; do
      AWS_PROFILE="${admin_profile}" aws s3api abort-multipart-upload \
        --region "${region}" --bucket "${bucket}" \
        --key "${key}" --upload-id "${upload_id}"
    done
  echo "multipart abort pass ${attempt} complete" >&2
done
list_uploads
[[ "$(jq -er '(.Uploads // []) | length' "${multipart_file}")" == 0 ]] || {
  echo "incomplete multipart uploads remain under ${prefix}/" >&2
  exit 1
}

AWS_PROFILE="${admin_profile}" terraform -chdir="${terraform_dir}" destroy
rm -f "${multipart_file}" "${qualification_state}/session-credentials.json" \
  "${qualification_state}/qualification-spin.toml"
```

Remove the protected files only after destroy succeeds. A non-empty-bucket
failure requires inspection; do not enable `force_destroy`.
