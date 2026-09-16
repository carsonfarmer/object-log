#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
helper="${script_dir}/issue-session.sh"
test_root="$(mktemp -d "${TMPDIR:-/tmp}/object-log-session-test.XXXXXX")"
trap 'rm -r "${test_root}"' EXIT
mkdir "${test_root}/bin"

cat >"${test_root}/bin/aws" <<'FAKE_AWS'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"${FAKE_AWS_LOG:?}"
case " $* " in
  *" iam list-access-keys "*) printf '0\n' ;;
  *" iam create-access-key "*)
    printf '%s\n' '{"AccessKeyId":"TEST1111111111111111","SecretAccessKey":"BOOTSTRAP_SECRET_MARKER"}'
    case "${FAKE_MODE:-success}" in
      create-failure) exit 23 ;;
      signal) kill -TERM "${PPID}"; exit 143 ;;
    esac
    ;;
  *" sts get-session-token "*)
    [[ "${AWS_ACCESS_KEY_ID:-}" == TEST1111111111111111 ]]
    [[ "${AWS_SECRET_ACCESS_KEY:-}" == BOOTSTRAP_SECRET_MARKER ]]
    [[ " $* " == *" --duration-seconds 14400 "* ]]
    case "${FAKE_MODE:-success}" in
      transient)
        attempts=0
        [[ ! -s "${FAKE_STS_ATTEMPTS:?}" ]] || attempts="$(<"${FAKE_STS_ATTEMPTS}")"
        attempts=$((attempts + 1))
        printf '%s\n' "${attempts}" >"${FAKE_STS_ATTEMPTS}"
        if (( attempts < 3 )); then echo 'InvalidClientTokenId' >&2; exit 25; fi
        ;;
      race) printf 'racer\n' >"${FAKE_RACE_OUTPUT:?}" ;;
      race-symlink) ln -s "${FAKE_RACE_TARGET:?}" "${FAKE_RACE_OUTPUT:?}" ;;
    esac
    printf '%s\n' '{"AccessKeyId":"TEST2222222222222222","SecretAccessKey":"SESSION_SECRET_MARKER","SessionToken":"SESSION_TOKEN_MARKER","Expiration":"2030-01-01T00:00:00Z"}'
    ;;
  *" iam delete-access-key "*) : ;;
  *) exit 24 ;;
esac
FAKE_AWS
cat >"${test_root}/bin/sleep" <<'FAKE_SLEEP'
#!/bin/sh
exit 0
FAKE_SLEEP
chmod +x "${test_root}/bin/aws" "${test_root}/bin/sleep"

mode() {
  stat -c '%a' "$1" 2>/dev/null || stat -f '%Lp' "$1"
}

run_failure() {
  local scenario="$1" output="$2" log="$3" capture="$4" code
  set +e
  PATH="${test_root}/bin:${PATH}" FAKE_MODE="${scenario}" \
    FAKE_AWS_LOG="${log}" FAKE_RACE_OUTPUT="${output}" \
    FAKE_RACE_TARGET="${test_root}/race-target" \
    "${helper}" admin-profile qualification-user "${output}" >"${capture}" 2>&1
  code=$?
  set -e
  [[ ${code} -ne 0 ]]
}

success_output="${test_root}/success.json"
success_log="${test_root}/success.log"
PATH="${test_root}/bin:${PATH}" FAKE_AWS_LOG="${success_log}" \
  "${helper}" admin-profile qualification-user "${success_output}" >/dev/null
jq -e '.AccessKeyId == "TEST2222222222222222"' "${success_output}" >/dev/null
[[ "$(mode "${success_output}")" == 600 ]]
grep -Fq -- '--access-key-id TEST1111111111111111' "${success_log}"
[[ "$(grep -Fc ' iam delete-access-key ' "${success_log}")" == 1 ]]

transient_output="${test_root}/transient.json"
transient_log="${test_root}/transient.log"
PATH="${test_root}/bin:${PATH}" FAKE_MODE=transient \
  FAKE_STS_ATTEMPTS="${test_root}/transient-attempts" \
  FAKE_AWS_LOG="${transient_log}" \
  "${helper}" admin-profile qualification-user "${transient_output}" \
  >/dev/null 2>"${test_root}/transient.capture"
jq -e '.AccessKeyId == "TEST2222222222222222"' "${transient_output}" >/dev/null
[[ "$(<"${test_root}/transient-attempts")" == 3 ]]
[[ "$(grep -Fc ' iam delete-access-key ' "${transient_log}")" == 1 ]]

failure_log="${test_root}/failure.log"
run_failure create-failure "${test_root}/failure.json" "${failure_log}" \
  "${test_root}/failure.capture"
grep -Fq -- '--access-key-id TEST1111111111111111' "${failure_log}"
[[ ! -e "${test_root}/failure.json" ]]

signal_log="${test_root}/signal.log"
run_failure signal "${test_root}/signal.json" "${signal_log}" \
  "${test_root}/signal.capture"
grep -Fq -- '--access-key-id TEST1111111111111111' "${signal_log}"
[[ ! -e "${test_root}/signal.json" ]]

race_output="${test_root}/race.json"
run_failure race "${race_output}" "${test_root}/race.log" \
  "${test_root}/race.capture"
[[ "$(<"${race_output}")" == racer ]]

mkdir "${test_root}/race-target"
run_failure race-symlink "${test_root}/race-symlink.json" \
  "${test_root}/race-symlink.log" "${test_root}/race-symlink.capture"
[[ -L "${test_root}/race-symlink.json" ]]
[[ -z "$(find "${test_root}/race-target" -mindepth 1 -print -quit)" ]]

ln -s "${test_root}/missing" "${test_root}/dangling.json"
run_failure success "${test_root}/dangling.json" "${test_root}/dangling.log" \
  "${test_root}/dangling.capture"
[[ -L "${test_root}/dangling.json" ]]

trace="${test_root}/trace.capture"
PATH="${test_root}/bin:${PATH}" FAKE_AWS_LOG="${test_root}/trace.log" \
  bash -x "${helper}" admin-profile qualification-user \
    "${test_root}/trace.json" >"${trace}" 2>&1
for marker in BOOTSTRAP_SECRET_MARKER SESSION_SECRET_MARKER SESSION_TOKEN_MARKER; do
  if grep -Fq "${marker}" "${trace}"; then
    echo "secret marker reached xtrace output" >&2
    exit 1
  fi
done

echo "issue-session success, retry, failure, signal, race, symlink, and xtrace tests passed"
