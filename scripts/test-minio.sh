#!/usr/bin/env bash
set -euo pipefail

image="minio/minio@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e"
container="object-log-minio-$(uuidgen | tr '[:upper:]' '[:lower:]')"
access_key="objectlog"
secret_key="objectlog-local-test-secret"
bucket="object-log-test"
test_target="${1:-minio}"
test_filter="${2:-minio_passes_recovery_checkpoint_and_gc_flow}"
package="${3:-object-log}"
features="${4-aws,test-util}"
container_started=0
native_pid=""
native_data=""
native_binary="${OBJECT_LOG_MINIO_BINARY:-}"

cleanup() {
  local status=$?
  local cleanup_failed=0
  trap - EXIT INT TERM

  if [[ -n "${native_pid}" ]]; then
    kill "${native_pid}" 2>/dev/null || true
    for _ in $(seq 1 40); do
      if ! kill -0 "${native_pid}" 2>/dev/null; then break; fi
      sleep 0.05
    done
    if kill -0 "${native_pid}" 2>/dev/null; then kill -KILL "${native_pid}" 2>/dev/null || true; fi
    wait "${native_pid}" 2>/dev/null || true
    if ! python3 -c 'import errno,socket,sys; s=socket.socket(); s.settimeout(1); sys.exit(0 if s.connect_ex(("127.0.0.1",int(sys.argv[1]))) == errno.ECONNREFUSED else 1)' "${port}"; then
      echo 'Native MinIO listener did not close.' >&2
      cleanup_failed=1
    fi
    if [[ "${status}" != "0" ]]; then cat "${native_data}/server.log" >&2 || true; fi
  fi
  if [[ -n "${native_data}" ]]; then
    rm -r -- "${native_data}" || cleanup_failed=1
  fi

  if [[ "${container_started}" == "1" ]]; then
    if docker container inspect "${container}" >/dev/null 2>&1; then
      if ! docker rm --force "${container}" >/dev/null; then
        echo "failed to remove MinIO test container ${container}" >&2
        cleanup_failed=1
      fi
    fi
    if docker container inspect "${container}" >/dev/null 2>&1; then
      echo "MinIO test container ${container} remains after cleanup" >&2
      cleanup_failed=1
    elif ! docker info >/dev/null 2>&1; then
      echo "cannot verify MinIO test container cleanup" >&2
      cleanup_failed=1
    fi
  fi

  if [[ "${status}" == "0" && "${cleanup_failed}" == "1" ]]; then
    status=1
  fi
  exit "${status}"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

if [[ -n "${native_binary}" ]]; then
  [[ -x "${native_binary}" ]] || { echo 'OBJECT_LOG_MINIO_BINARY must name an executable.' >&2; exit 2; }
  command -v lsof >/dev/null || { echo 'Native MinIO mode requires lsof to verify listener ownership.' >&2; exit 2; }
  "${native_binary}" --version
  shasum -a 256 "${native_binary}"
  native_data="$(mktemp -d "${TMPDIR:-/tmp}/object-log-minio.XXXXXX")"
  port="$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')"
  endpoint="http://127.0.0.1:${port}"
  MINIO_ROOT_USER="${access_key}" MINIO_ROOT_PASSWORD="${secret_key}" MINIO_BROWSER=off \
    "${native_binary}" server "${native_data}/data" --address "127.0.0.1:${port}" \
    >"${native_data}/server.log" 2>&1 &
  native_pid=$!
else
  docker info >/dev/null
  container_started=1
  docker run --detach --rm \
    --name "${container}" \
    --publish 127.0.0.1::9000 \
    --env "MINIO_ROOT_USER=${access_key}" \
    --env "MINIO_ROOT_PASSWORD=${secret_key}" \
    "${image}" server /data >/dev/null
  published="$(docker port "${container}" 9000/tcp)"
  endpoint="http://${published}"
fi

ready=0
for _ in $(seq 1 60); do
  if [[ -n "${native_pid}" ]] && ! kill -0 "${native_pid}" 2>/dev/null; then
    echo 'Owned native MinIO exited during startup.' >&2
    exit 1
  fi
  if curl --connect-timeout 1 --max-time 2 --fail --silent \
    "${endpoint}/minio/health/ready" >/dev/null; then
    if [[ -n "${native_pid}" ]] && [[ "$(lsof -a -p "${native_pid}" -iTCP:"${port}" -sTCP:LISTEN -t 2>/dev/null || true)" != "${native_pid}" ]]; then
      sleep 0.1
      continue
    fi
    ready=1
    break
  fi
  sleep 1
done
if [[ "${ready}" != "1" ]]; then
  if [[ -z "${native_binary}" ]]; then docker logs "${container}" >&2; fi
  exit 1
fi

if [[ -n "${native_pid}" ]] && { ! kill -0 "${native_pid}" 2>/dev/null || [[ "$(lsof -a -p "${native_pid}" -iTCP:"${port}" -sTCP:LISTEN -t 2>/dev/null || true)" != "${native_pid}" ]]; }; then
  echo 'Owned native MinIO no longer owns the test listener.' >&2
  exit 1
fi

AWS_ACCESS_KEY_ID="${access_key}" \
AWS_SECRET_ACCESS_KEY="${secret_key}" \
AWS_DEFAULT_REGION="us-east-1" \
aws --endpoint-url "${endpoint}" s3api create-bucket --bucket "${bucket}" >/dev/null

test_command=(cargo test --package "${package}")
if [[ -n "${features}" ]]; then
  test_command+=(--features "${features}")
fi
test_command+=(--test "${test_target}" "${test_filter}" -- --ignored --nocapture)

OBJECT_LOG_MINIO_ENDPOINT="${endpoint}" \
OBJECT_LOG_MINIO_ACCESS_KEY="${access_key}" \
OBJECT_LOG_MINIO_SECRET_KEY="${secret_key}" \
OBJECT_LOG_MINIO_BUCKET="${bucket}" \
"${test_command[@]}"
