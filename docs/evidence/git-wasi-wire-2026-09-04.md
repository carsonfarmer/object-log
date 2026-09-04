# Git WASI wire protocol evidence

Date: 2026-09-04

Current main revision: `223849e34287768862701fc6639035745b5305a9`

Integrated revisions:

- `296c56b70e4d51aa8a35bda33ba31522df4dfc88` added the bounded wire module.
- `f5a5296043269aceb504004792235931f5e5d66c` corrected request ordering,
  object-format defaults, receive reporting, pack rules, and output checks.
- `223849e34287768862701fc6639035745b5305a9` added the final pack-output and
  status-reason bounds.

This issue #17 tranche adds one private, host-neutral Git wire module. It has
seven operations for advertisement, parsing, and response writing. It has no
HTTP, async runtime, filesystem, repository, or public API.

## Result

Upload-pack advertises protocol v2 with `ls-refs`, `fetch`, `unborn`, agent,
and object format. It parses bounded `ls-refs` requests with `peel`, `symrefs`,
`unborn`, and repeated `ref-prefix` values. An empty prefix is valid.

Fetch parsing accepts `want`, `have`, and `done` in the order allowed by Git.
It retains `thin-pack`, `ofs-delta`, and `include-tag`. It accepts
`no-progress` and emits no progress. Duplicate options, data after `done`,
unsupported features, malformed packet lines, and truncated input fail before
engine work.

Fetch output has two valid states. Negotiation output contains an
acknowledgments section with all common IDs or `NAK`. Pack output contains a
direct packfile section. The module does not emit `ready`. The first engine
will acknowledge common haves during negotiation and send a pack only after
the client sends `done`.

The packfile writer uses channel 1 and chunks pack data into at most 65,515
bytes per packet. It rejects output packs over 64 MiB before it writes the
section header.

Receive-pack uses the classic protocol. Advertisement validation checks the
complete input before output. It requires valid targets, rejects unborn refs,
rejects duplicates, requires bytewise C-locale order, and requires `HEAD` to
be first when present. The request parser reads the first-NUL capability set
and ordered ref commands through the flush packet, then borrows the remaining
pack bytes.

SHA-1 is the default when a request omits `object-format`. SHA-256 requests
must select `object-format=sha256`. A create or update requires nonempty bytes
that start with `PACK`. A delete-only request must not include a pack. This is
the current strict project policy. The parser retains the client's optional
`report-status` selection so the adapter can omit a report when it was not
requested.

## Hard limits

| Resource | Limit |
| --- | ---: |
| Upload control input | 8 MiB |
| Receive control input | 1 MiB |
| Incoming receive pack | 32 MiB |
| Outgoing fetch pack | 64 MiB |
| Wants | 1,024 |
| Ref prefixes | 1,024 |
| Receive commands and statuses | 1,024 |
| Haves | 65,535 |
| Advertised refs | 65,535 |
| Acknowledgments | 65,535 |
| Text before its line feed | 65,515 bytes |
| Pack data in one channel-1 packet | 65,515 bytes |

All semantic checks and output-size checks run before response bytes are
written. A later writer I/O failure can still leave partial bytes.

## Fixtures and build proof

Eighteen focused tests pass. They compare exact bytes for:

- SHA-1 and SHA-256 protocol-v2 advertisements;
- Git 2.54 `ls-refs` and receive requests;
- fetch argument order, flags, and deduplicated object IDs;
- `ls-refs` responses;
- negotiation-only ACK and NAK responses;
- direct packfile responses and maximum channel-1 chunks;
- SHA-1 and SHA-256 classic receive advertisements and requests;
- receive success, rejection, and invalid-pack status; and
- valid zero-object SHA-1 and SHA-256 packs from `git pack-objects --stdout`.

The tests also cover optional line feeds, command ordering, exact maximum and
maximum-plus-one boundaries, mixed object formats, duplicate inputs,
malformed input, unsupported capabilities, empty status sets, invalid status
reasons, and fail-before-write behavior. They generate small fixture bytes in
the tests instead of keeping opaque fixture files.

These gates passed at the current main revision:

```sh
cargo test -p object-log-git --lib wire::tests
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo +1.97.1 check -p object-log-git --lib \
  --target wasm32-wasip2 --no-default-features
cargo +1.97.1 clippy -p object-log-git --lib \
  --target wasm32-wasip2 --no-default-features -- -D warnings
make check
```

The WASIp2 checks compile this module without default features. The wire path
uses `gix-packetline` 0.22.2 with only its blocking I/O feature. This proves
host-neutral compilation. It does not prove execution in a WASI host.

## Source size

| Current wire module | Raw lines | Nonblank lines |
| --- | ---: | ---: |
| Product | 613 | 573 |
| Tests | 956 | 903 |

The first wire commit added one manifest line, one lockfile line, and two
module-glue lines. The two correction commits changed shared object-ID and pack
limit code so the wire and normalizer use one contract.

## Authorities

- Git's [`protocol-v2`](https://git-scm.com/docs/protocol-v2) defines command
  requests, `ls-refs`, fetch negotiation, response sections, and packfile
  sidebands.
- Git's [`gitprotocol-pack`](https://git-scm.com/docs/gitprotocol-pack)
  defines classic pack negotiation and receive command framing.
- Git's
  [`gitprotocol-capabilities`](https://git-scm.com/docs/gitprotocol-capabilities)
  defines first-line NUL capabilities and receive-pack capability rules.
- Git's [`gitformat-pack`](https://git-scm.com/docs/gitformat-pack) defines
  `PACK`, SHA-1, SHA-256, and the zero-object pack shape.
- [`gix-packetline` 0.22.2](https://docs.rs/gix-packetline/0.22.2/gix_packetline/)
  supplies packet-line decoding and blocking response encoding.
- Local Git `2.54.0 (Apple Git-157)` stateless RPC and pack output supply the
  exact byte oracles.

## Remaining limits

The module is private and is not connected to the durable reader, graph walk,
pack builder, publication path, HTTP adapter, or Spin component. It excludes
shallow fetches, filters, `ref-in-want`, packfile URIs, `sideband-all`, and
progress. It checks the receive pack prefix and byte limit at this boundary;
the pack normalizer must validate its contents before publication. No result
here proves an unchanged-client loopback, WASI runtime behavior, or remote
object-store performance.
