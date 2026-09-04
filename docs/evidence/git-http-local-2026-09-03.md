# Local Git smart HTTP proof

## Result

At `e05df91`, `object-log-git-http` implements the four Git smart HTTP
protocol v0 operations for SHA-1 repositories. Product code uses `gix`,
`gix-pack`, and `gix-packetline`. It does not invoke Git or a C Git library.

The loopback test starts a native HTTP service and uses an unmodified Git 2.54.0
client. It proves:

- empty repository discovery
- initial push and clone
- fetch and fast-forward update
- atomic branch and annotated-tag creation and deletion
- non-fast-forward rejection
- final content and ref state
- `git fsck --strict`

Five protocol unit tests and the loopback test pass. The full workspace gate
passes with 219 tests and eight opt-in tests ignored.

## Durable publication

Receive-pack parses and bounds the ref commands and optional pack. It delegates
pack validation and ref policy to `object-log-git`. It writes a success report
only after object-log confirms the conditional publication or resolves its
recovery token as committed.

Client errors produce Git `ng` results. Object-store, durable-state, local Git,
and blocking-task errors return to the HTTP host. This lets the host distinguish
a rejected push from an internal failure.

## Upload negotiation

The proof uses a protocol v0 advertisement even when the client requests
protocol v2. Git accepts this fallback.

An upload request without `done` receives only `NAK`. The final request receives
`NAK` and a raw, self-contained pack. The server advertises no sideband or thin
pack capability. A focused test rejects unsupported capabilities and trailing
request data.

The fetch pack contains the complete object set reachable from current refs.
The implementation validates each requested object ID against the advertised
refs or a peeled annotated tag. It ignores `have` lines. This is correct, but it
uses more transfer bandwidth than an incremental pack.

## Bounds

- At most 1,024 wants or ref commands.
- At most 65,536 `have` lines.
- At most 8 MiB of upload control data.
- At most 1 MiB of receive control data.
- At most 512 MiB for an input or output pack.
- At most 10 million repository objects through the storage adapter's existing
  graph limit.

Pack input uses bounded disk spooling. Pack output is written to a bounded
temporary file before the response starts. A regression test proves that the
input accepts a `PACK` header split across one-byte reads.

## Size

The smart HTTP tranche adds 660 nonblank, non-comment product lines and 361 test
lines. It adds 31 README lines and 61 manifest or lockfile lines, with one
manifest deletion. The product count includes the Git adapter methods and pack
writer used by the protocol crate.

## Deployment limits

This is a verified Git protocol service, not a deployable HTTP server. The
loopback HTTP fixture buffers request and response bodies. A production host
must provide:

- authentication, routing, and repository selection
- bounded gzip request decoding
- chunked request and response transfer
- header and HTTP status handling
- request cancellation and service-level resource limits

The upload oracle confirmed that Git uses gzip for large, multi-round
negotiation requests. Gzip therefore belongs in the future HTTP host, not in the
protocol parser.

The current proof does not cover simultaneous pushes or a dropped HTTP response
after publication. The storage adapter separately proves one CAS winner and
lost-response recovery. Protocol v2, SHA-256 HTTP, incremental pack selection,
and a runnable host remain follow-on work. GitHub issue #14 tracks the
deployable host.
