# Spin key-value provider

`object-log-spin-key-value` is a native Rust provider for Spin's existing
`KeyValueFactor`. Consumers link it into their custom Spin runtime and register
it at compile time. Guests keep using the standard Spin key-value interface.
This crate does not ship a runtime, CLI, trigger, guest transport, or Spin fork.

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
credential chain (environment credentials or supported workload identity).
There are no credential fields in the guest manifest or provider TOML. The guest
needs no outbound S3 permission, WASI HTTP adapter, or composed storage component.
Configure the host's TLS crypto provider as normal for your Spin embedding.

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
Every operation reconstructs an exact snapshot from the store. Restart recovery
needs only S3 and the same configuration.

- Get distinguishes absence from an empty value. Deleting an absent key succeeds.
- Multi-get preserves input order, duplicates, and missing-value positions.
  Set/delete batches are atomic; repeated keys observe earlier commands.
- Counters use little-endian signed 64-bit bytes, matching Spin's default backend.
  Incrementing an absent key by zero creates it. Invalid encodings and overflow
  return `Error::Other` rather than trapping.
- CAS captures an observed log generation at creation or `current`. Swapping
  checks that exact generation, preventing absent-key races and ABA. Unrelated
  writes, checkpoints, and retention changes can conservatively cause CAS failure;
  Spin supplies the refreshed CAS handle through its existing dispatch.
- A confirmed conflict revalidates against a fresh snapshot within the attempt
  budget. An uncertain publication resolves the exact candidate. It is never
  replayed with a new transaction identity. Unresolved or expired mutation
  outcomes become `Error::Other` with an explicit **outcome unknown** message.
- Spin's guest interface has no pending token or request identity. Consequently
  this adapter cannot promise exactly-once retries after an unknown result,
  process loss, or lost response. Guests must reconcile their application state
  instead of blindly repeating an increment. Durable KV data still recovers.
- Admitted work runs in a Tokio task and keeps its permit after guest cancellation.
  Process/runtime shutdown can still interrupt it. Overload fails immediately;
  there is no adapter queue. Hosts own overall admission and graceful shutdown.

Optional `[key_value_store.<label>.limits]` fields and defaults:

| Field | Default | Scope |
| --- | ---: | --- |
| `key_bytes` | 256 | UTF-8 key bytes |
| `value_bytes` | 65,536 | One value |
| `batch_entries` | 128 | Commands or requested keys |
| `batch_bytes` | 1,048,576 | Input key/value bytes |
| `page_entries` | 128 | Internal scan page |
| `response_bytes` | 1,048,576 | Returned bytes plus String/Vec element storage |
| `tree_bytes` | 33,554,432 | KV traversal/staging work per call or scan page |
| `list_keys` | 4,096 | Complete key listing; overflow is an error |
| `concurrent_operations` | 16 | Per manager, shared across its open handles |
| `attempts` | 8 | Conflict attempts and exact pending resolutions |
| `requests` | 100,000 | Cumulative logical WAL client calls per operation |
| `checkpoint_entries` | 64 | Automatic checkpoint threshold before mutations |
| `collection_candidates` | 1,024 | One collection plan |

`get_keys` returns a complete bounded listing, never silent truncation.
The async interface buffers the same bounded result. Internal KV scans also
read values: their transient page allowance is the larger of `response_bytes`
and `key_bytes + value_bytes`. Existence checks read a value without copying it
into a returned buffer. These limits are not a whole-process RSS cap: guest
lifting, caller inputs, WAL decoding, native clients, allocator overhead, and
concurrency add memory. The embedding also owns guest memory, instance count,
and Spin resource-table limits; `HostLimits` does not cap open store/CAS handles.
The request guard excludes backend probing/opening and
provider-internal retries, listing pagination, and delete batching.

Optional `[key_value_store.<label>.wal]` fields are the twelve public
`object_log::Options` fields, with the core defaults. They are durable: all
openers must agree. The initial format is pre-release; use a fresh prefix after
an incompatible format change. Host limit changes can make existing data
unreadable if reduced below its required bounds.

## Maintenance and diagnostics

Writes checkpoint automatically before the configured tail threshold. Physical
collection is explicit host work through `Manager::maintain(label)`. Retain the
concrete manager when constructing/registering it programmatically if your host
needs this method; the standard Spin `Store` trait intentionally has no admin API.

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

Source size: **840 production Rust lines**, **27 registration-example lines**,
and **794 test lines** (physical lines including comments/blank lines; generated
code, lockfiles, and upstream sources excluded). Reproduce with:

```sh
wc -l integrations/spin-key-value/src/*.rs
wc -l integrations/spin-key-value/examples/*.rs integrations/spin-key-value/tests/*.rs
```
