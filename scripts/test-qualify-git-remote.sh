#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
scratch="$(mktemp -d)"
trap 'rm -rf "${scratch}"' EXIT
mkdir -p "${scratch}/bin" "${scratch}/state"
cat >"${scratch}/bin/mock" <<'EOF'
#!/bin/sh
case "${0##*/}:$*" in
  date:*"-j -f"*|date:*"-d "*) echo 2000000000 ;;
  date:*"+%Y-%m-%dT%H:%M:%SZ"*) echo 2030-03-17T10:00:00Z ;;
  date:*"+%H"*) echo 10 ;; date:*"+%F"*) echo 2030-03-17 ;; date:*"+%s"*) echo 1900000000 ;;
  git:*"rev-parse HEAD"*) echo fake-revision ;; git:*"status --porcelain"*) : ;;
  aws:*list-objects-v2*) if [ "${AWS_MODE:-}" = hang ]; then sh -c 'trap "" TERM; sleep 30' & echo $! >"${AWS_CHILD_PID_FILE}"; wait; fi; echo null ;;
  aws:*) echo null ;;
  go:*test*) for test in TestAccess TestLargeBlob TestMaintenance TestWALGit TestManyObjects TestShallowAndTags TestFetchVisibility TestFailureDrills; do echo "--- PASS: ${test} (0.00s)"; done; echo PASS; echo ok ;;
  shasum:*) cat >/dev/null; echo 'planhash  -' ;;
  *) exit 1 ;;
esac
EOF
for command in date git go aws shasum; do ln -s mock "${scratch}/bin/${command}"; done
cat >"${scratch}/bin/cargo" <<'EOF'
#!/bin/sh
if [ "${CARGO_MODE:-}" = hang ]; then
  sh -c 'trap "" TERM; sleep 30' &
  echo $! >"${CHILD_PID_FILE}"
  wait
fi
if [ ! -e "${FAIL_MARKER}" ]; then : >"${FAIL_MARKER}"; exit 1; fi
: >"${SECOND_MARKER}"
for test in minio_immutable_create_faults minio_maintenance_model minio_passes_recovery_checkpoint_and_gc_flow; do
  echo "test ${test} ... ok"
done
echo 'test result: ok. 1 passed; 0 failed; 0 ignored'
EOF
chmod +x "${scratch}/bin/"*
cat >"${scratch}/state/phase-failure.state" <<'EOF'
id=phase-failure
revision=fake-revision
region=test
endpoint=https://s3.example.invalid
bucket=test-bucket
prefix=qualification/phase-failure
scope=live-s3-only
plan=planhash
service=
date=2030-03-17
epoch=1900000000
phase=backend
result=active
EOF
export PATH="${scratch}/bin:${PATH}" FAIL_MARKER="${scratch}/failed" SECOND_MARKER="${scratch}/second"
export GIT_QUALIFICATION_ID=phase-failure GIT_QUALIFICATION_REVISION=fake-revision
export GIT_QUALIFICATION_REGION=test GIT_QUALIFICATION_S3_ENDPOINT=https://s3.example.invalid
export GIT_QUALIFICATION_BUCKET=test-bucket GIT_QUALIFICATION_PREFIX=qualification/phase-failure
export GIT_QUALIFICATION_ENCRYPTION=AES256 GIT_QUALIFICATION_CREDENTIAL_SOURCE=test-role
export GIT_QUALIFICATION_CREDENTIAL_EXPIRES_AT=2030-03-18T00:00:00Z
export GIT_QUALIFICATION_REQUEST_LIMIT=1 GIT_QUALIFICATION_COST_LIMIT_USD=1
export GIT_QUALIFICATION_TIME_LIMIT_SECONDS=35000 GIT_QUALIFICATION_COUNTER_SOURCE=manual
export GIT_QUALIFICATION_ADMISSION=reviewed GIT_QUALIFICATION_STATE_DIR="${scratch}/state"
export GIT_PROBE_URL=http://127.0.0.1:19100 GIT_PROBE_PASSWORD=test
export GIT_PROBE_BRANCH=main GIT_PROBE_BOOT_ID=boot-1
export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_SESSION_TOKEN=test
if "${root}/scripts/qualify-git-remote.sh" protocol >/dev/null 2>&1; then
  echo "protocol phase masked its first failed command" >&2
  exit 1
fi
[[ -e "${FAIL_MARKER}" && ! -e "${SECOND_MARKER}" ]]
grep -q '^phase=protocol$' "${scratch}/state/phase-failure.state"
grep -q '^result=failed$' "${scratch}/state/phase-failure.state"
grep -q 'phase=protocol status=failed' "${scratch}/state/phase-failure-protocol.log"
sed -e 's/phase-failure/prefix-failure/g' -e 's/result=failed/result=active/' \
  "${scratch}/state/phase-failure.state" >"${scratch}/state/prefix-failure.state"
export GIT_QUALIFICATION_ID=prefix-failure GIT_QUALIFICATION_PREFIX=qualification/prefix-failure
if "${root}/scripts/qualify-git-remote.sh" standard >/dev/null 2>&1; then
  echo "standard phase masked its failed prefix binding" >&2
  exit 1
fi
grep -q '^result=failed$' "${scratch}/state/prefix-failure.state"
sed 's/phase-failure/phase-timeout/g' "${scratch}/state/phase-failure.state" |
  sed -e 's/phase=protocol/phase=backend/' -e 's/result=failed/result=active/' >"${scratch}/state/phase-timeout.state"
export GIT_QUALIFICATION_ID=phase-timeout GIT_QUALIFICATION_PREFIX=qualification/phase-timeout
export GIT_QUALIFICATION_TIME_LIMIT_SECONDS=1 CARGO_MODE=hang CHILD_PID_FILE="${scratch}/child-pid"
started="$(/bin/date +%s)"
if "${root}/scripts/qualify-git-remote.sh" protocol >/dev/null 2>&1; then
  echo "protocol phase exceeded its deadline" >&2
  exit 1
fi
elapsed=$(( $(/bin/date +%s) - started ))
child_pid="$(cat "${CHILD_PID_FILE}")"
(( elapsed < 5 ))
if kill -0 "${child_pid}" 2>/dev/null; then
  echo "deadline left a child holding the phase pipeline" >&2
  exit 1
fi
grep -q '^result=failed$' "${scratch}/state/phase-timeout.state"
grep -q 'phase=protocol status=failed' "${scratch}/state/phase-timeout-protocol.log"
sed 's/phase-timeout/phase-signal/g' "${scratch}/state/phase-timeout.state" |
  sed -e 's/phase=protocol/phase=backend/' -e 's/result=failed/result=active/' >"${scratch}/state/phase-signal.state"
export GIT_QUALIFICATION_ID=phase-signal GIT_QUALIFICATION_PREFIX=qualification/phase-signal
export GIT_QUALIFICATION_TIME_LIMIT_SECONDS=35000 CHILD_PID_FILE="${scratch}/signal-child"
"${root}/scripts/qualify-git-remote.sh" protocol >/dev/null 2>&1 &
runner_pid=$!
for _ in {1..100}; do [[ -s "${CHILD_PID_FILE}" ]] && break; /bin/sleep .05; done
[[ -s "${CHILD_PID_FILE}" ]] || { echo "deadline child did not start" >&2; exit 1; }
kill -TERM "${runner_pid}"
if wait "${runner_pid}"; then echo "externally terminated deadline succeeded" >&2; exit 1; fi
child_pid="$(cat "${CHILD_PID_FILE}")"
if kill -0 "${child_pid}" 2>/dev/null; then echo "external termination left a child running" >&2; exit 1; fi
grep -q '^result=failed$' "${scratch}/state/phase-signal.state"
sed 's/phase-failure/aws-signal/g' "${scratch}/state/phase-failure.state" |
  sed 's/result=failed/result=active/' >"${scratch}/state/aws-signal.state"
export GIT_QUALIFICATION_ID=aws-signal GIT_QUALIFICATION_PREFIX=qualification/aws-signal
export AWS_MODE=hang AWS_CHILD_PID_FILE="${scratch}/aws-child"
"${root}/scripts/qualify-git-remote.sh" standard >/dev/null 2>&1 &
runner_pid=$!
for _ in {1..100}; do [[ -s "${AWS_CHILD_PID_FILE}" ]] && break; /bin/sleep .05; done
[[ -s "${AWS_CHILD_PID_FILE}" ]] || { echo "deadline AWS child did not start" >&2; exit 1; }
kill -TERM "${runner_pid}"
if wait "${runner_pid}"; then echo "externally terminated AWS deadline succeeded" >&2; exit 1; fi
child_pid="$(cat "${AWS_CHILD_PID_FILE}")"
if kill -0 "${child_pid}" 2>/dev/null; then echo "external termination left AWS child running" >&2; exit 1; fi
if compgen -G "${scratch}/state/.qualification-aws.*" >/dev/null; then echo "AWS output file survived termination" >&2; exit 1; fi
grep -q '^result=failed$' "${scratch}/state/aws-signal.state"
sed 's/phase-failure/cleanup/g' "${scratch}/state/phase-failure.state" |
  sed -e 's/epoch=1900000000/epoch=1899990000/' >"${scratch}/state/cleanup.state"
export GIT_QUALIFICATION_ID=cleanup GIT_QUALIFICATION_PREFIX=qualification/cleanup
export GIT_QUALIFICATION_TIME_LIMIT_SECONDS=1 CARGO_MODE='' AWS_MODE=''
"${root}/scripts/qualify-git-remote.sh" teardown >/dev/null
grep -q '^result=failed-cleaned$' "${scratch}/state/cleanup.state"
"${root}/scripts/qualify-git-remote.sh" review >/dev/null
grep -q '^result=failed-reviewed$' "${scratch}/state/cleanup.state"
echo "phase failures, deadline cleanup, teardown, and review rehearsal passed"
