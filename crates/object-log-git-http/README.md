# object-log-git-http

This crate provides Git smart HTTP protocol v0 and a runnable native server
for one repository at `/repo`. It uses `object-log-git` for Git state and
`object-log` for durable publication. Product code does not run Git or link to
C Git. The acceptance tests use an unchanged Git client.

The server supports these routes:

- `GET /repo/info/refs?service=git-upload-pack`
- `GET /repo/info/refs?service=git-receive-pack`
- `POST /repo/git-upload-pack`
- `POST /repo/git-receive-pack`

It accepts identity and gzip request bodies. Axum and Hyper handle fixed-length
and chunked requests. Upload responses stream from an anonymous temporary file
after pack generation succeeds. This prevents a false `200` response when pack
generation fails. Discovery and receive reports stay below their protocol
limits and use memory.

Run the server with an existing S3 bucket:

```sh
OBJECT_LOG_STORE_URL=s3://bucket/prefix \
OBJECT_LOG_LISTEN=0.0.0.0:3000 \
cargo run --release -p object-log-git-http
```

The `object_store` S3 builder reads standard `AWS_*` settings. S3-compatible
services can also set `AWS_ENDPOINT`, `AWS_ALLOW_HTTP`, and
`AWS_VIRTUAL_HOSTED_STYLE_REQUEST`. `OBJECT_LOG_SCRATCH` selects disposable
local storage. `OBJECT_LOG_CONCURRENCY` defaults to four active operations.
The server probes conditional create, update, and read behavior before it
listens.

The repository uses SHA-1 and exposes `HEAD` as `refs/heads/main`. The fetch
path sends the complete reachable object set. It accepts a requested object
that was reachable in the current durable repository even when a concurrent
push changed the advertised ref before the POST arrived.

A receive result stays successful only after durable publication. A rejected
push returns a Git protocol rejection with HTTP `200`. An uncertain or expired
publication returns HTTP `503`. The client must fetch fresh refs before it
decides whether its requested ref transaction became visible. The server does
not retain the object-log recovery token. This loses exact historical attempt
classification, but it does not report false success. Staged objects that did
not publish remain eligible for object-log garbage collection.

The host limits active Git work, encoded and decoded request data, packet-line
counts, pack bytes, and idle body time. Active operations continue under a task
tracker if the HTTP handler is canceled. Shutdown waits for those operations.
An upload response keeps its concurrency permit and anonymous file until the
client consumes or drops the body.

Authentication, TLS, tenant routing, protocol v2, SHA-256 HTTP, and live AWS
qualification remain deployment or follow-on work. Put this server behind a
proxy that limits request header bytes. The application cannot prevent Hyper
from allocating parsed headers before route middleware runs. Local filesystem
storage does not pass object-log's conditional-update probe. Use local MinIO
for persistent local tests.
