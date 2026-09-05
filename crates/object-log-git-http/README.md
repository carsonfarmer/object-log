# object-log-git-http

This temporary native reference host preserves HTTP fault, disconnect, and local
provider coverage while equivalent Spin coverage is developed. It contains no
previous native Git implementation.

The native server hosts one repository at `/repo` using the same
`object-log-git::Repository` as the Spin component. The shared engine supports
protocol-v2 discovery, clone, have-aware fetch, and classic receive-pack push
for SHA-1 and SHA-256. Product code does not run Git or link to C Git;
acceptance tests use unchanged Git clients.

The routes are `GET /repo/info/refs?service=git-upload-pack`, the corresponding
`git-receive-pack` discovery, and `POST /repo/git-upload-pack` and
`POST /repo/git-receive-pack`. Upload requests require `Git-Protocol: version=2`.
Identity and gzip bodies, fixed-length and chunked uploads, and Git's
flush-only probe before a large push are supported. Responses are completely
validated before HTTP success and retain their engine reservation through
transmission. No local Git repository is needed by the shared engine.

Run against an existing S3 bucket:

```sh
OBJECT_LOG_STORE_URL=s3://bucket/prefix \
OBJECT_LOG_GIT_FORMAT=sha256 \
OBJECT_LOG_LISTEN=127.0.0.1:3000 \
cargo run --release -p object-log-git-http
```

`OBJECT_LOG_GIT_FORMAT` defaults to `sha1`. Keep different repository formats
in separate object-log namespaces. The `object_store` S3 builder reads standard
`AWS_*` settings. S3-compatible services can set `AWS_ENDPOINT`, `AWS_ALLOW_HTTP`,
and `AWS_VIRTUAL_HOSTED_STYLE_REQUEST`. Startup probes conditional store behavior.
`HEAD` is exposed as `refs/heads/main`; accepted wants must be reachable from
refs in the command's exact view, and valid haves exclude their complete closure.

A successful receive report follows confirmed durable publication. Ref rejection
uses normal Git framing with HTTP 200. Uncertain publication, including invalid
resolution evidence after preparation, returns HTTP 503 and an
`application/octet-stream` body containing the exact opaque recovery token.
A caller that retains those bytes can use `Log::resume`. Tokens are not logged.
A disconnected Git client must refresh refs; it cannot assume a failed HTTP
request means the transaction did not commit. Unpublished staging remains
eligible for collection. Once the complete receive body arrives, publication
runs under a task tracker even if its HTTP handler disconnects; shutdown waits.

One process-wide engine pool admits one operation. Body collection occurs after
admission; encoded and decoded request limits, idle timeouts, pack limits, and
engine budgets still apply. Responses hold admission until consumed or dropped.

Authentication, TLS, tenant routing, shallow/partial clone, and live AWS are
separate deployment or follow-on work. Put the server behind a proxy that bounds
request headers. Local filesystem storage lacks the required conditional update;
its rejection tests remain. Use local `MinIO` for persistent acceptance.
