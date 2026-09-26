# Repository explorer

A small repository browser with its own TypeScript Spin API and Preact UI.
It shows branches, directories, file previews and recent commits for configured
repositories. The Go Git service and the WAL are unchanged.

The API checks access through ordinary Git discovery, then reads the repository's
existing WAL catalog directly. Reads follow only the selected history and paths;
opening a large file summary does not download the file. SHA-1 and SHA-256 both
work. Storage settings come from the same Spin variables as the Git example.

## Run locally

Install the [Git prerequisites](../README.md), [Bun](https://bun.com/), and Node.js.
From the repository root:

```sh
cd examples/git/viewer
bun install --frozen-lockfile
bun run check
bun test
bun run build
```

Follow the [Git example's local setup](../README.md#run-locally), adding
`-f spin.viewer.toml` to its `spin up` command. Push a repository, then open
`http://127.0.0.1:19100/browse?repo=sha256.git` and enter the configured password.
Other configured paths, such as `team/project.git`, work too. Cognito access
tokens use the Git service's existing read permissions. Credentials stay in page
memory and clear on reload. Branch and path selections stay in the URL.

Hosted proxies must forward `/browse`, `/browse/api`, `/browse/assets/client.js`
and `/browse/assets/style.css` to Spin. The API is `GET /browse/api` with a
configured `repo` and optional `ref` and `path` query parameters.

## Build and checks

Bun bundles the browser and API; `j2w` builds the WASI component, then `wac` links
it with the reusable WAL component. The build generates `viewer/dist/viewer.wasm`
and `spin.viewer.toml` from the existing manifest. Rebuild after changing
`spin.toml`. Normal Git builds do not require Bun.

`bun run check` runs Biome and strict TypeScript checks; `bun test` checks catalog
reads, object validation and authorization. `bun run format` applies formatting
and safe lint fixes. CI checks, tests and builds with the frozen Bun lockfile.

The compiler currently needs the small fix for
[ComponentizeJS #221](https://github.com/bytecodealliance/ComponentizeJS/issues/221),
which otherwise omits the classes for opaque WAL handles. Exact build provenance
is listed in [THIRD_PARTY.md](../../../THIRD_PARTY.md).

## Scope

Only branch tips are selectable. The UI shows eight first-parent commits,
500 entries per directory and UTF-8 text previews up to 256 KiB. Binary and
larger files show a summary. Symlinks show their stored target; submodules show
their commit ID. Non-UTF-8 filenames appear as byte escapes without a browse link; valid sibling
files remain browsable. Names and file content render as text. Git metadata reads are
limited to 16 MiB per object and 64 MiB of catalog data per request. An expired
view retries once, preserving cumulative read accounting.
