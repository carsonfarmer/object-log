#!/usr/bin/env bash
set -euo pipefail

image="minio/minio@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e"
container="object-log-minio-$(uuidgen | tr '[:upper:]' '[:lower:]')"
access_key="objectlog"
secret_key="objectlog-local-test-secret"
bucket="object-log-test"

cleanup() {
  docker rm --force "${container}" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

docker info >/dev/null
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
  if curl --fail --silent "${endpoint}/minio/health/ready" >/dev/null; then
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
cargo test --features aws,test-util --test minio -- --ignored --nocapture
