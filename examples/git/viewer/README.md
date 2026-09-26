# Repository explorer

A small read-only UI for the Git example, built with TypeScript and Preact. It
browses branches, directories, text files and recent first-parent commits. A
separate Spin component serves the page; the existing Git service supplies the
data through its read authorization, retention and retry path. The WAL is
unchanged.

## Run locally

Install the [Git prerequisites](../README.md) and [Bun](https://bun.com/).
From the repository root:

```sh
cd examples/git/viewer
bun install --frozen-lockfile
bun run check
bun run build
cd ../../..
OBJECT_LOG_GIT_LOCAL_MANIFEST=spin.viewer.toml make git-local
```

Push to a printed Git URL, then open
`http://127.0.0.1:19100/browse?repo=sha256.git` and enter the printed password.
`/browse` also accepts a configured path such as `team/project.git`. Cognito
access tokens work with the same repository permissions. Credentials stay in
page memory and clear on reload. Branch and path selections stay in the URL;
Back/Forward and ordinary modified link clicks work.

For an existing backend, use `spin up -f spin.viewer.toml` from `examples/git`
with your protected variables file. Hosted proxies must forward the three exact
viewer routes and `/<repository>/_browse` to the service.

## Build and checks

Bun bundles the browser and static handler; Spin's standard `j2w` compiler makes
the WASI component. `bun run check` runs Biome linting/formatting and strict
TypeScript checks. `bun run format` applies formatting and safe lint fixes. CI
checks and builds the viewer with the frozen Bun lockfile.

The build generates `viewer/dist/viewer.wasm` and `spin.viewer.toml` from the
existing manifest. Rebuild after changing `spin.toml`. Normal Git builds do not
require Bun. The component has no storage credentials or outbound hosts.

## Scope

Only branch tips are selectable. The UI shows eight first-parent commits,
500 entries per directory and UTF-8 text previews up to 256 KiB. Binary and
larger files show a summary. Symlinks show their stored target; submodules show
their commit ID. Names and file content render as text. The adapter's tests are
included in `make git-check`.
