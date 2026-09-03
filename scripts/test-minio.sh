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
features="${4:-aws,test-util}"
container_started=0

cleanup() {
  local status=$?
  local cleanup_failed=0
  trap - EXIT INT TERM

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

ready=0
for _ in $(seq 1 60); do
  if curl --connect-timeout 1 --max-time 2 --fail --silent \
    "${endpoint}/minio/health/ready" >/dev/null; then
    ready=1
    break
  fi
  sleep 1
done
if [[ "${ready}" != "1" ]]; then
  docker logs "${container}" >&2
  exit 1
fi

AWS_ACCESS_KEY_ID="${access_key}" \
AWS_SECRET_ACCESS_KEY="${secret_key}" \
AWS_DEFAULT_REGION="us-east-1" \
aws --endpoint-url "${endpoint}" s3api create-bucket --bucket "${bucket}" >/dev/null

OBJECT_LOG_MINIO_ENDPOINT="${endpoint}" \
OBJECT_LOG_MINIO_ACCESS_KEY="${access_key}" \
OBJECT_LOG_MINIO_SECRET_KEY="${secret_key}" \
OBJECT_LOG_MINIO_BUCKET="${bucket}" \
cargo test --package "${package}" --features "${features}" \
  --test "${test_target}" "${test_filter}" -- --ignored --nocapture
