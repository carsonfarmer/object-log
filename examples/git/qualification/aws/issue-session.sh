#!/usr/bin/env bash
case $- in *x*) set +x ;; esac
set -euo pipefail

fail() {
  echo "issue-session: $*" >&2
  exit 1
}

[[ $# -eq 3 ]] || fail "usage: $0 AWS_PROFILE IAM_USER /absolute/session.json"
profile="$1"
iam_user="$2"
output="$3"
[[ "${output}" == /* ]] || fail "output must be an absolute path outside the repository"
for dependency in aws jq; do
  command -v "${dependency}" >/dev/null || fail "${dependency} is required"
done

admin_aws() {
  env -u BASHOPTS -u SHELLOPTS \
    -u AWS_ACCESS_KEY_ID -u AWS_SECRET_ACCESS_KEY \
    -u AWS_SESSION_TOKEN -u AWS_SECURITY_TOKEN \
    aws --no-cli-pager --profile "${profile}" "$@"
}

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
repository_root="$(git -C "${script_dir}" rev-parse --show-toplevel)"
output_parent="$(dirname "${output}")"
[[ -d "${output_parent}" ]] || fail "output directory does not exist: ${output_parent}"
output="$(cd "${output_parent}" && pwd -P)/$(basename "${output}")"
case "${output}" in
  "${repository_root}" | "${repository_root}"/*) fail "output must be outside the repository" ;;
esac
[[ ! -e "${output}" && ! -L "${output}" ]] || fail "refusing to overwrite ${output}"

umask 077
bootstrap_file="$(mktemp "${TMPDIR:-/tmp}/object-log-bootstrap.XXXXXX")"
session_file="$(mktemp "${output}.XXXXXX")"
bootstrap_key_id=""
bootstrap_secret=""

cleanup() {
  local code=$?
  set +x
  trap - EXIT INT TERM HUP
  if [[ -z "${bootstrap_key_id}" && -s "${bootstrap_file}" ]]; then
    bootstrap_key_id="$(jq -er '.AccessKeyId | select(type == "string" and test("^[A-Za-z0-9_]{16,128}$"))' \
      "${bootstrap_file}" 2>/dev/null || :)"
  fi
  unset bootstrap_secret
  rm -f "${bootstrap_file}" "${session_file}"
  if [[ -n "${bootstrap_key_id}" ]]; then
    admin_aws iam delete-access-key --user-name "${iam_user}" \
      --access-key-id "${bootstrap_key_id}" >/dev/null || code=1
  fi
  exit "${code}"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
trap 'exit 129' HUP

[[ "$(admin_aws iam list-access-keys --user-name "${iam_user}" \
  --query 'length(AccessKeyMetadata)' --output text)" == 0 ]] ||
  fail "bootstrap user already has an access key; inspect it before continuing"

admin_aws iam create-access-key --user-name "${iam_user}" \
  --query AccessKey --output json >"${bootstrap_file}"
bootstrap_key_id="$(jq -er .AccessKeyId "${bootstrap_file}")"
bootstrap_secret="$(jq -er .SecretAccessKey "${bootstrap_file}")"

issue_session() {
  AWS_ACCESS_KEY_ID="${bootstrap_key_id}" \
  AWS_SECRET_ACCESS_KEY="${bootstrap_secret}" \
  AWS_SESSION_TOKEN='' AWS_SECURITY_TOKEN='' \
    env -u BASHOPTS -u SHELLOPTS \
      aws --no-cli-pager sts get-session-token --duration-seconds 14400 \
      --query Credentials --output json
}
for attempt in 1 2 3 4 5 6; do
  if issue_session >"${session_file}"; then break; fi
  (( attempt < 6 )) || fail "could not issue temporary session credentials"
  sleep 2
done
jq -e '.AccessKeyId and .SecretAccessKey and .SessionToken and .Expiration' \
  "${session_file}" >/dev/null
unset bootstrap_secret

admin_aws iam delete-access-key --user-name "${iam_user}" \
  --access-key-id "${bootstrap_key_id}" >/dev/null
: >"${bootstrap_file}"
bootstrap_key_id=""
chmod 600 "${session_file}"
ln -n "${session_file}" "${output}" || fail "refusing to overwrite ${output}"
rm -f "${session_file}"
session_file=""
echo "Wrote temporary session credentials to ${output}"
