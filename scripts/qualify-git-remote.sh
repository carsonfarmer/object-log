#!/usr/bin/env bash
set -euo pipefail

phase="${1:-rehearse}"
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

fail() { echo "remote qualification: $*" >&2; return 1; }
need() { [[ -n "${!1:-}" ]] || fail "set $1"; }

rehearse() {
  cat <<'EOF'
Offline rehearsal; no network or state changes:
  start       validate revision, temporary credentials, reviewed limits, S3
              settings, one-run window, and an empty isolated prefix
  backend     remote S3 backend conformance
  protocol    conditional-write, ambiguity, fault, recovery, and maintenance tests
  standard    authenticated Git correctness plus failure-drill preparation
  recovery    verify restart with the same standard prefix
  read-only   verify the same prefix under the read-only service profile
  limits      verify the fresh small-limit service profile
  performance run 1,025 pushes/hash and concurrent 513 MiB lifecycles last
  teardown    delete the exact prefix and require zero residual objects

Standard/recovery/read-only/performance use PREFIX/git. Limits uses
PREFIX/git-limits. Core tests use PREFIX/core. HTTP loopback proves live S3
behavior only; an HTTPS deployment also exercises inbound TLS and auth.
EOF
}

usage() {
  echo "usage: scripts/qualify-git-remote.sh {rehearse|start|backend|protocol|standard|recovery|read-only|limits|performance|teardown|status}" >&2
}

if [[ "${phase}" == rehearse ]]; then rehearse; exit 0; fi
case "${phase}" in start|backend|protocol|standard|recovery|read-only|limits|performance|teardown|status) ;; *) usage; exit 2 ;; esac

for name in \
  GIT_QUALIFICATION_ID GIT_QUALIFICATION_REVISION GIT_QUALIFICATION_REGION \
  GIT_QUALIFICATION_S3_ENDPOINT GIT_QUALIFICATION_BUCKET GIT_QUALIFICATION_PREFIX \
  GIT_QUALIFICATION_ENCRYPTION GIT_QUALIFICATION_CREDENTIAL_SOURCE \
  GIT_QUALIFICATION_CREDENTIAL_EXPIRES_AT GIT_QUALIFICATION_REQUEST_LIMIT \
  GIT_QUALIFICATION_COST_LIMIT_USD GIT_QUALIFICATION_TIME_LIMIT_SECONDS \
  GIT_QUALIFICATION_COUNTER_SOURCE GIT_QUALIFICATION_ADMISSION \
  GIT_QUALIFICATION_STATE_DIR GIT_QUALIFICATION_TEST_TIMEOUT \
  GIT_PROBE_URL GIT_PROBE_PASSWORD GIT_PROBE_BRANCH
do
  need "${name}"
done

id="${GIT_QUALIFICATION_ID}"
state_dir="${GIT_QUALIFICATION_STATE_DIR}"
state="${state_dir}/${id}.state"
[[ "${id}" =~ ^[A-Za-z0-9._-]+$ ]] || fail "campaign ID must be filename-safe"
[[ "${GIT_QUALIFICATION_S3_ENDPOINT}" == https://* ]] || fail "S3 endpoint must use HTTPS"
[[ "${state_dir}" == /* ]] || fail "state directory must be absolute"
case "${state_dir}" in "${root}"|"${root}"/*) fail "state directory must be outside the repository" ;; esac
case "${GIT_QUALIFICATION_PREFIX}" in /*|*/|*..*) fail "prefix must be a normalized relative path" ;; esac
[[ "${GIT_QUALIFICATION_PREFIX}" == *"${id}"* ]] || fail "prefix must contain the campaign ID"
[[ "${GIT_QUALIFICATION_ENCRYPTION}" == AES256 || "${GIT_QUALIFICATION_ENCRYPTION}" == aws:kms ]] || fail "encryption must be AES256 or aws:kms"
[[ "${GIT_QUALIFICATION_REQUEST_LIMIT}" =~ ^[1-9][0-9]*$ ]] || fail "request limit must be positive"
[[ "${GIT_QUALIFICATION_COST_LIMIT_USD}" =~ ^[0-9]+([.][0-9]+)?$ ]] || fail "cost limit must be a decimal"
[[ "${GIT_QUALIFICATION_TIME_LIMIT_SECONDS}" =~ ^[1-9][0-9]*$ ]] || fail "time limit must be positive seconds"
if [[ "${GIT_PROBE_URL}" != https://* ]]; then
  case "${GIT_PROBE_URL}" in http://127.0.0.1:*|http://localhost:*) ;; *) fail "Git URL must be HTTPS or explicit loopback" ;; esac
fi
[[ "$(git -C "${root}" rev-parse HEAD)" == "${GIT_QUALIFICATION_REVISION}" ]] || fail "checked-out revision differs from plan"
[[ -z "$(git -C "${root}" status --porcelain)" ]] || fail "qualification requires a clean revision"

value() { awk -F= -v key="$1" '$1 == key {print $2; exit}' "${state}"; }
save() {
  mkdir -p "${state_dir}"
  local temporary
  temporary="$(mktemp "${state_dir}/.${id}.XXXXXX")"
  printf 'id=%s\nrevision=%s\nbucket=%s\nprefix=%s\ndate=%s\nepoch=%s\nphase=%s\nresult=%s\n' \
    "${id}" "${GIT_QUALIFICATION_REVISION}" "${GIT_QUALIFICATION_BUCKET}" \
    "${GIT_QUALIFICATION_PREFIX}" "$3" "$4" "$1" "$2" >"${temporary}"
  chmod 600 "${temporary}"
  mv "${temporary}" "${state}"
}

aws_s3api() {
  aws --region "${GIT_QUALIFICATION_REGION}" --endpoint-url "${GIT_QUALIFICATION_S3_ENDPOINT}" s3api "$@"
}
aws_credentials() {
  need AWS_ACCESS_KEY_ID; need AWS_SECRET_ACCESS_KEY; need AWS_SESSION_TOKEN
  command -v aws >/dev/null || fail "aws CLI is required"
}
counts() {
  local current versions markers
  current="$(aws_s3api list-objects-v2 --bucket "${GIT_QUALIFICATION_BUCKET}" --prefix "${GIT_QUALIFICATION_PREFIX}/" --query 'length(Contents)' --output text)"
  versions="$(aws_s3api list-object-versions --bucket "${GIT_QUALIFICATION_BUCKET}" --prefix "${GIT_QUALIFICATION_PREFIX}/" --query 'length(Versions)' --output text)"
  markers="$(aws_s3api list-object-versions --bucket "${GIT_QUALIFICATION_BUCKET}" --prefix "${GIT_QUALIFICATION_PREFIX}/" --query 'length(DeleteMarkers)' --output text)"
  printf '%s %s %s\n' "${current}" "${versions}" "${markers}"
}

check_window() {
  local hour elapsed
  hour="$(TZ=America/Vancouver date +%H)"
  (( 10#${hour} >= 8 && 10#${hour} < 20 )) || fail "live phases run only 08:00-20:00 Pacific"
  [[ "$(value date)" == "$(TZ=America/Vancouver date +%F)" ]] || fail "campaign must finish on its Pacific start date"
  elapsed=$(($(date +%s) - $(value epoch)))
  (( elapsed < GIT_QUALIFICATION_TIME_LIMIT_SECONDS )) || fail "campaign time limit elapsed"
}
guard() {
  [[ -f "${state}" ]] || fail "run start first"
  [[ "$(value result)" == active ]] || fail "campaign is locked; only teardown is allowed"
  [[ "$(value phase)" == "$1" ]] || fail "expected phase $1, found $(value phase)"
  [[ "$(value revision)" == "${GIT_QUALIFICATION_REVISION}" ]] || fail "state revision changed"
  [[ "$(value bucket)" == "${GIT_QUALIFICATION_BUCKET}" && "$(value prefix)" == "${GIT_QUALIFICATION_PREFIX}" ]] || fail "state storage target changed"
  check_window
}
advance() { save "$1" active "$(value date)" "$(value epoch)"; }
run_phase() {
  local expected="$1" next="$2"; shift 2
  guard "${expected}"
  if "$@"; then advance "${next}"; else
    local code=$?
    save "${next}" failed "$(value date)" "$(value epoch)"
    echo "Campaign ${id} failed in ${next}; teardown is still required." >&2
    return "${code}"
  fi
}

rust_test() {
  local target="$1" test="$2" output code
  output="$(mktemp "${TMPDIR:-/tmp}/object-log-qualification.XXXXXX")"
  set +e
  cargo test -p object-log --features aws,test-util --test "${target}" "${test}" -- --ignored --exact --nocapture 2>&1 | tee "${output}"
  code=${PIPESTATUS[0]}
  set -e
  if [[ "${code}" != 0 ]] || ! grep -Fq "test ${test} ... ok" "${output}" || ! grep -Fq '1 passed; 0 failed' "${output}"; then code=1; else code=0; fi
  rm -f "${output}"
  return "${code}"
}
core_env() {
  aws_credentials
  export OBJECT_LOG_MINIO_ENDPOINT="${GIT_QUALIFICATION_S3_ENDPOINT}"
  export OBJECT_LOG_MINIO_ACCESS_KEY="${AWS_ACCESS_KEY_ID}"
  export OBJECT_LOG_MINIO_SECRET_KEY="${AWS_SECRET_ACCESS_KEY}"
  export OBJECT_LOG_MINIO_SESSION_TOKEN="${AWS_SESSION_TOKEN}"
  export OBJECT_LOG_MINIO_BUCKET="${GIT_QUALIFICATION_BUCKET}"
  export OBJECT_LOG_MINIO_REGION="${GIT_QUALIFICATION_REGION}"
  export OBJECT_LOG_MINIO_PREFIX="${GIT_QUALIFICATION_PREFIX}/core"
}
backend() { core_env; rust_test store_conformance minio_backend_conformance; }
protocol() {
  core_env
  rust_test protocol minio_protocol_matrix
  rust_test immutable_faults minio_immutable_create_faults
  rust_test maintenance_model minio_maintenance_model
  rust_test minio minio_passes_recovery_checkpoint_and_gc_flow
}

go_tests() {
  local pattern="$1" output code expected; shift
  output="$(mktemp "${TMPDIR:-/tmp}/object-log-qualification.XXXXXX")"
  set +e
  (cd "${root}/examples/git" && go test -count=1 -parallel=4 -timeout="${GIT_QUALIFICATION_TEST_TIMEOUT}" -run "${pattern}" -v ./tests) 2>&1 | tee "${output}"
  code=${PIPESTATUS[0]}
  set -e
  if [[ "${code}" != 0 ]] || grep -Fq -- '--- SKIP:' "${output}"; then rm -f "${output}"; return 1; fi
  for expected in "$@"; do grep -Fq -- "--- PASS: ${expected} " "${output}" || { rm -f "${output}"; return 1; }; done
  rm -f "${output}"
}
standard() {
  unset GIT_PROBE_READ_ONLY GIT_PROBE_LIMITS GIT_REPEATED_PUSHES GIT_LARGE_OBJECT_MIB GIT_CONCURRENT_LARGE GIT_PROBE_PERSISTED_HEAD
  go_tests '^(TestAccess|TestLargeBlob|TestMaintenance|TestWALGit|TestManyObjects|TestShallowAndTags|TestFetchVisibility)$' \
    TestAccess TestLargeBlob TestMaintenance TestWALGit TestManyObjects TestShallowAndTags TestFetchVisibility
  export GIT_FAILURE_DRILLS=prepare GIT_DRILL_STATE="${state_dir}/${id}-drill.json"
  go_tests '^TestFailureDrills$' TestFailureDrills
}
recovery() {
  export GIT_FAILURE_DRILLS=verify GIT_DRILL_STATE="${state_dir}/${id}-drill.json" GIT_PROBE_PERSISTED_HEAD=true
  go_tests '^(TestFailureDrills|TestPersistedHead)$' TestFailureDrills TestPersistedHead
}
read_only() { export GIT_PROBE_READ_ONLY=true; go_tests '^TestAccess$' TestAccess; }
limits() { export GIT_PROBE_LIMITS=1; go_tests '^(TestConfiguredLimits|TestAuthenticateProbeRequest)$' TestConfiguredLimits TestAuthenticateProbeRequest; }
performance() {
  unset GIT_PROBE_READ_ONLY GIT_PROBE_LIMITS GIT_FAILURE_DRILLS GIT_PROBE_PERSISTED_HEAD
  export GIT_REPEATED_PUSHES=1
  go_tests '^TestRepeatedPushes$' TestRepeatedPushes
  unset GIT_REPEATED_PUSHES
  export GIT_LARGE_OBJECT_MIB=513 GIT_CONCURRENT_LARGE=1
  go_tests '^TestLargeBlob$' TestLargeBlob
}

case "${phase}" in
  status) [[ -f "${state}" ]] || fail "campaign state does not exist"; cat "${state}" ;;
  start)
    aws_credentials
    hour="$(TZ=America/Vancouver date +%H)"; (( 10#${hour} >= 8 && 10#${hour} < 20 )) || fail "campaigns start only 08:00-20:00 Pacific"
    mkdir -p "${state_dir}"; [[ ! -e "${state}" ]] || fail "campaign ID already exists"
    day="$(TZ=America/Vancouver date +%F)"; [[ ! -e "${state_dir}/day-${day}" ]] || fail "one campaign already started today"
    for old in "${state_dir}"/*.state; do
      [[ -e "${old}" ]] || continue
      if grep -q '^result=failed' "${old}"; then
        old_id="$(awk -F= '$1 == "id" {print $2}' "${old}")"
        [[ "${GIT_QUALIFICATION_REVIEWED_FAILURE:-}" == "${old_id}" ]] || fail "failed campaign ${old_id} needs owner review"
      fi
    done
    printf '%s\n' "${id}" >"${state_dir}/day-${day}"; save starting active "${day}" "$(date +%s)"
    actual_version="$(aws_s3api get-bucket-versioning --bucket "${GIT_QUALIFICATION_BUCKET}" --query Status --output text)"
    [[ "${actual_version}" == None ]] || { save start failed "${day}" "$(value epoch)"; fail "use a dedicated bucket with versioning disabled"; }
    actual_encryption="$(aws_s3api get-bucket-encryption --bucket "${GIT_QUALIFICATION_BUCKET}" --query 'ServerSideEncryptionConfiguration.Rules[0].ApplyServerSideEncryptionByDefault.SSEAlgorithm' --output text)"
    [[ "${actual_encryption}" == "${GIT_QUALIFICATION_ENCRYPTION}" ]] || { save start failed "${day}" "$(value epoch)"; fail "bucket encryption differs from plan"; }
    if lifecycle="$(aws_s3api get-bucket-lifecycle-configuration --bucket "${GIT_QUALIFICATION_BUCKET}" 2>&1)"; then
      save start failed "${day}" "$(value epoch)"; fail "use a dedicated bucket without lifecycle rules"
    fi
    [[ "${lifecycle}" == *NoSuchLifecycleConfiguration* ]] || { save start failed "${day}" "$(value epoch)"; fail "could not verify bucket lifecycle settings"; }
    read -r current versions markers <<<"$(counts)"
    [[ "${current}" == 0 && "${versions}" == 0 && "${markers}" == 0 ]] || { save start failed "${day}" "$(value epoch)"; fail "prefix is not empty"; }
    advance start
    printf 'campaign=%s revision=%s region=%s bucket=%s prefix=%s\n' "${id}" "${GIT_QUALIFICATION_REVISION}" "${GIT_QUALIFICATION_REGION}" "${GIT_QUALIFICATION_BUCKET}" "${GIT_QUALIFICATION_PREFIX}"
    printf 'storage_class=STANDARD versioning=Disabled encryption=%s lifecycle=none consistency=strong-read-after-write-and-list\n' "${actual_encryption}"
    printf 'credential=%s expires=%s request_limit=%s cost_limit_usd=%s time_limit_seconds=%s\n' "${GIT_QUALIFICATION_CREDENTIAL_SOURCE}" "${GIT_QUALIFICATION_CREDENTIAL_EXPIRES_AT}" "${GIT_QUALIFICATION_REQUEST_LIMIT}" "${GIT_QUALIFICATION_COST_LIMIT_USD}" "${GIT_QUALIFICATION_TIME_LIMIT_SECONDS}"
    printf 'counter_source=%s admission=%s scope=%s\n' "${GIT_QUALIFICATION_COUNTER_SOURCE}" "${GIT_QUALIFICATION_ADMISSION}" "$(if [[ "${GIT_PROBE_URL}" == https://* ]]; then echo deployed-https; else echo live-s3-only; fi)"
    ;;
  backend) run_phase start backend backend ;;
  protocol) run_phase backend protocol protocol ;;
  standard) run_phase protocol standard standard ;;
  recovery) run_phase standard recovery recovery ;;
  read-only) run_phase recovery read-only read_only ;;
  limits) run_phase read-only limits limits ;;
  performance) run_phase limits performance performance ;;
  teardown)
    [[ -f "${state}" ]] || fail "campaign state does not exist"
    case "$(value result)" in active|failed) ;; *) fail "campaign is already closed" ;; esac
    aws_credentials
    aws --region "${GIT_QUALIFICATION_REGION}" --endpoint-url "${GIT_QUALIFICATION_S3_ENDPOINT}" s3 rm "s3://${GIT_QUALIFICATION_BUCKET}/${GIT_QUALIFICATION_PREFIX}/" --recursive
    read -r current versions markers <<<"$(counts)"
    [[ "${current}" == 0 && "${versions}" == 0 && "${markers}" == 0 ]] || fail "teardown left current=${current} versions=${versions} markers=${markers}"
    result=failed-cleaned
    if [[ "$(value result)" == active && "$(value phase)" == performance ]]; then result=complete; fi
    save complete "${result}" "$(value date)" "$(value epoch)"; rm -f "${state_dir}/${id}-drill.json"
    echo "teardown residual current=0 versions=0 markers=0"
    ;;
esac
