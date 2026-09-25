# Spin key-value provider

`object-log-spin-key-value` is a native Rust provider for Spin's existing
`KeyValueFactor`. Consumers link it into their custom Spin runtime and register
it at compile time. Guests keep using the standard Spin key-value interface.
This crate does not ship a runtime, CLI, trigger, guest transport, or Spin fork.

Spin 4.1 resolves an OpenTelemetry SDK affected by
[GHSA-w9wp-h8wv-79jx](https://github.com/open-telemetry/opentelemetry-rust/security/advisories/GHSA-w9wp-h8wv-79jx).
This provider has no HTTP boundary at which to constrain untrusted propagation
headers. Until Spin ships the coordinated upgrade tracked in
[spinframework/spin#3598](https://github.com/spinframework/spin/issues/3598), an
HTTP host must reject `baggage` headers above 8,192 bytes or 64 list members at
its gateway. The optional Git host demonstrates and tests that boundary in
[`HOSTING.md`](../../examples/git/qualification/aws/HOSTING.md).

The provider implements upstream `StoreManager`, `Store`, `Cas`, and
`MakeKeyValueStore`. It pins unmodified Spin **4.1.0**, revision
`c0b3726aa4857961e20cf8616a0df5f0741af73d`. It is pre-release and unpublished;
use a reviewed repository revision. Its separate Cargo workspace keeps Spin and
native S3 dependencies outside the portable object-log and KV cores.

## Register in your runtime

```toml
[dependencies]
object-log-spin-key-value = { git = "https://github.com/carsonfarmer/object-log", rev = "<reviewed-object-log-revision>" }
spin-factor-key-value = { git = "https://github.com/spinframework/spin", rev = "c0b3726aa4857961e20cf8616a0df5f0741af73d" }
```

Add the provider to the resolver your runtime uses **before** resolving its TOML:

```rust
use object_log_spin_key_value::ObjectLogKeyValueStore;
use spin_factor_key_value::runtime_config::spin::RuntimeConfigResolver;

let mut resolver = RuntimeConfigResolver::new();
resolver.register_store_type(ObjectLogKeyValueStore)?;
let key_value_config = resolver.resolve(Some(&runtime_toml))?;
```

Install `key_value_config` as the runtime configuration for the existing
`KeyValueFactor` in your `RuntimeFactors` set. If your host already has a resolver
with other providers, register with that resolver instead of replacing it.
`Config` also implements `Serialize`, so upstream `add_default_store` works.
[`examples/register.rs`](examples/register.rs) is a runnable registration and
configuration example. [`tests/guest.rs`](tests/guest.rs) demonstrates assigning
that configuration to an actual upstream factor set.

All Spin crates must resolve to the **same Cargo source and revision** as your
runtime. For a consumer building Spin from local source, patch this crate's Git
dependency in the consumer's root manifest:

```toml
[patch."https://github.com/spinframework/spin"]
spin-factor-key-value = { path = "path/to/spin/crates/factor-key-value" }
```

The path must contain the compatible pinned API. An upgrade to a different Spin
API requires adapting and revalidating the provider. No changes to upstream Spin
APIs are required. The stock Spin executable cannot load arbitrary native store
providers from TOML: its host must link and register this provider. See upstream's
[provider construction API](https://github.com/spinframework/spin/blob/v4.1.0/crates/factor-key-value/src/runtime_config/spin.rs)
and [built-in registration](https://github.com/spinframework/spin/blob/v4.1.0/crates/runtime-config/src/lib.rs).

## Host and guest configuration

Host `runtime-config.toml`:

```toml
[key_value_store.default]
type = "object-log"
bucket = "my-bucket"
region = "us-east-1"
prefix = "my-deployment/app-a"

[key_value_store.audit]
type = "object-log"
bucket = "my-bucket"
region = "us-east-1"
prefix = "my-deployment/app-a"
```

The ordinary guest manifest grants labels as usual:

```toml
[component.my-component]
source = "my-component.wasm"
key_value_stores = ["default", "audit"]
```

Spin enforces component permissions and unknown-label configuration errors.
The manager derives one WAL identity from the BLAKE3 digest of each label.
Different labels within a prefix are isolated; the same bucket, prefix, and
label deliberately share state across hosts. Different applications/tenants
must receive distinct, canonical, nonempty prefixes from the host operator.
A label rename selects a different store. Every writer of a namespace must use
this adapter's conventions; direct KV writers can introduce invalid UTF-8 keys
or incompatible counter bytes.

S3 credentials stay in the host's native `object_store::aws::AmazonS3Builder`
credential chain (environment credentials, web identity, container identity, or
EC2 instance identity). The provider imports only credential-source variables;
ambient endpoint, HTTP, signing, and transport variables cannot override its
TOML configuration. There are no credential fields in the guest manifest or
provider TOML. The guest needs no outbound S3 permission, WASI HTTP adapter, or
composed storage component. Configure the host's TLS crypto provider as normal
for your Spin embedding.

The bucket must already exist. Backend validation probes conditional reads and
writes and deletes its probe object. Grant the host read, write, list, and delete
permissions under its root prefix, and prevent external lifecycle deletion or
overwrite of protocol objects. Native requests have a 30-second client timeout,
at most two client retries, and a 60-second retry budget. Inject an independently
configured native `ObjectStore` through `Manager::new` for other transport policy.

For local MinIO, also set `endpoint = "http://127.0.0.1:9000"` and
`allow_http = true`. For ephemeral local development, replace the S3 fields with
`memory = true`; each constructed manager owns a fresh memory backend. The
filesystem backend is rejected because `LocalFileSystem` lacks conditional
updates. It is not a durable fallback.

## Behavior and limits

Each logical store has one conditional-write head. All trees, commits,
checkpoints, and collection plans use object-log's immutable objects. There is
no local database, durable queue, owner lease, or second publication authority.
Reads reconstruct an exact snapshot from the store. Each configured manager
shares one disposable write owner per open namespace. The owner collects
currently queued set/delete calls after one scheduler yield and publishes them
in order as one WAL batch when they fit. Other managers can write concurrently;
the conditional head decides which publication wins. Restart recovery needs
only S3 and the same configuration.

- Get distinguishes absence from an empty value. Deleting an absent key succeeds.
- Multi-get preserves input order, duplicates, and missing-value positions.
  Set/delete batches are atomic; repeated keys observe earlier commands.
  A batch in which no command changes a value succeeds without a WAL commit or
  tail slot. At the checkpoint threshold it may still publish a checkpoint.
- Counters use little-endian signed 64-bit bytes, matching Spin's default backend.
  Incrementing an absent key by zero creates it. Invalid encodings and overflow
  return `Error::Other` rather than trapping.
- CAS captures an observed log generation at creation or `current`. Swapping
  checks that exact generation, preventing absent-key races and ABA. Unrelated
  writes, checkpoints, and retention changes can conservatively cause CAS failure;
  Spin supplies the refreshed CAS handle through its existing dispatch.
- A confirmed conflict waits for a small randomized, bounded delay, then
  revalidates against a fresh snapshot within the attempt budget. This keeps
  equal-latency writers from repeatedly colliding in lockstep. An uncertain
  publication resolves the exact candidate. It is never
  replayed with a new transaction identity. Unresolved or expired mutation
  outcomes become `Error::Other` with an explicit **outcome unknown** message.
- Spin's guest interface has no pending token or request identity. Consequently
  this adapter cannot promise exactly-once retries after an unknown result,
  process loss, or lost response. Guests must reconcile their application state
  instead of blindly repeating an increment. Durable KV data still recovers.
- Admitted work runs in a Tokio task and keeps its permit after guest cancellation.
  Set/delete calls enter a bounded per-namespace queue. The manager admission
  or input allowance rejects overload immediately; work that has waited too long fails before it
  starts. `Manager::drain(deadline)` stops admission and waits for admitted work;
  a timeout leaves that work running. Hosts call it before runtime shutdown.
  An abrupt process loss can still interrupt admitted work.

Optional `[key_value_store.<label>.limits]` fields and defaults:

| Field | Default | Scope |
| --- | ---: | --- |
| `key_bytes` | 256 | UTF-8 key bytes |
| `value_bytes` | 65,536 | One value |
| `batch_entries` | 128 | Commands or requested keys |
| `batch_bytes` | 1,048,576 | Input key/value bytes |
| `page_entries` | 128 | Internal scan page |
| `response_bytes` | 1,048,576 | Returned bytes plus collection headers and element storage |
| `tree_bytes` | 33,554,432 | KV traversal/staging work per call or scan page |
| `list_keys` | 4,096 | Complete key listing; overflow is an error |
| `concurrent_operations` | 16 | Per manager, shared across its open handles |
| `owner_count` | 128 | Distinct live namespace owners per manager |
| `owner_input_bytes` | 16,777,216 | Input bytes across admitted set/delete calls |
| `owner_wait_ms` | 1,000 | Maximum admission-to-start wait for a queued write |
| `attempts` | 8 | Conflict attempts and exact pending resolutions |
| `requests` | 100,000 | Cumulative logical WAL client calls per operation |
| `checkpoint_entries` | 64 | Automatic checkpoint threshold before mutations |
| `collection_candidates` | 1,024 | One collection plan |

The WAL's separate `max_collection_objects` option defaults to 100,000. Each
write publishes a complete KV root, so this bounds total tree objects and can
stop growth even when each individual call fits the KV byte limits. Key capacity
depends on tree shape. Guests receive the distinct message
`object-log total-state object limit exceeded`; the failed write changes
nothing. Set
`[key_value_store.<label>.wal].max_collection_objects` for a larger state when
creating the store. This durable option cannot be changed in place; every
opener must retain it. Collection also bounds distinct live objects by it. Each
new plan walks the live graph and scans the namespace. Each pass deletes up to
`collection_candidates` objects, so repeated passes repeat this work. Measure
the `requests` allowance and host maintenance time as the state grows instead
of only raising the object limit.

`get_keys` returns a complete bounded listing, never silent truncation.
Collection response accounting includes the outer `Vec` header, even for an
empty result, as well as each element's headers and payload bytes.
The async interface buffers the same bounded result. Internal KV scans also
read values: their transient page allowance is the larger of `response_bytes`
and `key_bytes + value_bytes`. Existence checks read a value without copying it
into a returned buffer. These limits are not a whole-process RSS cap: guest
lifting, caller inputs, WAL decoding, native clients, allocator overhead, and
concurrency add memory. The embedding also owns guest memory, instance count,
and Spin resource-table limits. `owner_count` bounds distinct live store names,
but does not cap handles to one name or CAS handles. The request guard applies
to a whole grouped publication, including conflict retries. If group preparation
hits a limit, each original write is retried with its own guard; storage errors
do not trigger that fallback. The guard excludes backend probing/opening and
retries internal to object_store, listing pagination, and delete batching.

Optional `[key_value_store.<label>.wal]` fields are the twelve public
`object_log::Options` fields, with the core defaults. They are durable: all
openers must agree. The initial format is pre-release; use a fresh prefix after
an incompatible format change. Host limit changes can make existing data
unreadable if reduced below its required bounds.

In a 64-write hot-namespace sample on local native MinIO (Apple M4 Pro,
optimized Rust test build, 64-byte values), eight concurrent clients completed
about 104 writes/s before grouping and 504–565 writes/s with the owner across
two runs. The p95 write latency fell from 240 ms to 16–35 ms; logical
object-store calls fell from 3,524 to 202–231, and HTTP conditional-write
conflicts from 32 to zero. One client remained near 140 writes/s. Reads retain
their independent path. The conditions and raw results are recorded in issue
#57; these numbers do not predict remote S3 throughput.

## Maintenance and diagnostics

Writes checkpoint automatically before the configured tail threshold. Physical
collection is explicit host work through `Manager::maintain(label)`. The standard
resolver exposes a concrete manager through `StoreManager::metadata`:

```rust
use anyhow::Context;
use object_log_spin_key_value::Manager;

let manager = key_value_config.get_store_manager("default")
    .context("no default store")?;
let host = manager.metadata().downcast_ref::<Manager>()
    .context("default store is not an object-log provider")?.clone();
let outcome = host.maintain("default").await?;
```

The cloned host handle shares the same backend validation and admission limit
as the configured manager. It can be retained by the host for later maintenance.
The standard Spin `Store` trait intentionally has no admin API.

One call checkpoints and processes at most one bounded deletion plan. It resumes
an existing plan before materializing a tree. `More` means call again within a
host-controlled maintenance budget; `Complete` means that pass found no garbage.
`Contended`, `Retained`, and `Pending` require later host attention/retry. Reader
retentions are never cleared automatically. For predictable availability, drain
all hosts before collection; concurrent operations remain fenced and safe but
can fail with expired-view or collection errors. No scheduler or HTTP admin
endpoint is provided.

The provider preserves the current tracing span, emits host-side failure details,
and reports completed collection delete attempts. Spin's existing factor spans
cover guest operations. Guests receive sanitized storage errors; detailed native
provider errors stay in host logs. Configure host telemetry and log access policy
there. Request counts do not represent billed S3 operations or network bytes.

## Verification

From the repository root:

```sh
make spin-kv-check
make spin-kv-guest-test
```

The second command fetches pinned, unmodified upstream Spin test sources, builds
its standard key-value guest for `wasm32-wasip2`, and runs it through upstream
Spin factors and the HTTP trigger in-process. It tests standard guest CRUD and
permission denial with no storage imports or credentials. The test embedding
is confined to development dependencies and does not ship a runtime.

Memory tests cover independent managers, cold reopening, concurrent increments,
CAS races, limits, registration, cancellation, publication uncertainty,
checkpoint replacement, retained collection, and collection restart. The
filesystem capability test runs before opt-in local MinIO qualification:

```sh
cd integrations/spin-key-value
../../scripts/test-minio.sh minio minio_native_provider object-log-spin-key-value ''
# Without Docker, use an installed native MinIO:
OBJECT_LOG_MINIO_BINARY=/absolute/path/to/minio \
  ../../scripts/test-minio.sh minio minio_native_provider object-log-spin-key-value ''
```

The script creates isolated local storage and verifies cleanup. Tests reject
remote endpoints. Native S3 qualification covers factory construction with host
credentials, namespace isolation, concurrent writers, cold recovery, limits,
and collection. These are correctness tests, not remote-S3 performance claims.
The native integration is not a WASIp2 target; the unchanged portable cores are
checked separately for WASIp2 by `make spin-kv-check`.

For opt-in AWS qualification, provision the disposable backend described in
[`examples/git/qualification/aws/README.md`](../../examples/git/qualification/aws/README.md),
then set `SPIN_KV_AWS_BUCKET`, `SPIN_KV_AWS_REGION`, and a unique
`SPIN_KV_AWS_PREFIX`. Use its temporary session for a local process, or leave
credential variables unset on the same-region runner so it uses the instance
role. Then run:

```sh
cargo test --locked --test aws -- --ignored --nocapture
cd ../..
make spin-kv-guest-test
```

The first command covers the native provider's default profile, concurrent
writers, cold recovery, maintenance, and an explicitly configured value-size
profile through 8 MiB. The second runs upstream's unchanged Spin guest against
the same native S3 provider when those variables are present. Neither command
runs without the explicit ignored-test or remote configuration opt-in.

Commit `a1d4568` was qualified on 2026-09-22 from a same-region `t3.xlarge`
Amazon Linux 2023 runner in `us-west-2`. The default and explicit 8 MiB native
profiles passed. The large profile admitted two simultaneous 8 MiB writes to
independent logical stores and immediately rejected a third through the shared
manager limit. The original 8 MiB point profile survived a cold reopen and
collection. The two native test cases ran in parallel and the combined process
peaked at 155 MiB RSS. The pinned, unmodified Spin guest also passed through the
S3-backed provider and peaked at 155 MiB in its separately timed process. These
are qualification figures for this host and test mix, not a runtime-wide memory
limit.
