# object-log-git-http

This crate is a verified protocol service proof for `object-log-git`, not a
deployable HTTP server. It keeps web framework, routing, repository selection,
and authentication outside the library.

`SmartHttp` implements these protocol operations:

- upload-pack discovery
- receive-pack discovery
- upload-pack POST
- receive-pack POST

The crate supports Git protocol v0 and SHA-1 repositories. It uses
`gix-packetline` for framing and `gix-pack` for pack output. Product code does
not run a Git executable. The integration test uses an unmodified Git client
as an external compatibility check.

Receive-pack delegates validation and publication to `object-log-git`. It does
not report a successful ref update until the object-log commit is confirmed.
Each request uses a new disposable local repository.

Upload-pack accepts direct advertised object IDs and peeled annotated-tag IDs.
It currently ignores `have` lines and returns the complete reachable object
set after the client sends `done`. Pack input and output are limited to 512
MiB.

A host must map the four Git routes to the corresponding methods and apply the
media types and cache policy from `Service`. It must also provide bounded gzip
decoding, chunked transfer, HTTP error mapping, and service-level resource
limits.
