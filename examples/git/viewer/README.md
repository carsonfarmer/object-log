# Repository explorer

A small repository browser with creation through its TypeScript Spin API and Preact UI.
It shows branches, directories, file previews and recent commits for authorized
repositories. The API and UI live in a separate component; the WAL is unchanged.

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
bun start
```

This builds both components, starts disposable local MinIO and Spin, and prints
the Git URL. Open `http://127.0.0.1:19100/browse?repo=team/project.git`, sign in
with password `local-git-password`, choose SHA-1 or SHA-256 and click
**Create repository**. Then push an existing repository of that format:

```sh
git push http://127.0.0.1:19100/team/project.git HEAD:main
```

When Git prompts, use username `git` and password `local-git-password`.
Press Ctrl-C in the startup terminal to stop both services and remove their
disposable data. Each restart begins with empty storage.

The default `"*"` policy allows authorized creation of any repository through
the API or UI, without per-repository TOML or a restart. Creation records its
Git format; reads never create repositories. Bare names such as `team/project` identify the same
repository as `team/project.git`. Exact configuration entries override the
whole wildcard policy and may pin a WAL identity or Git format. Cognito access
tokens use the Git service's existing read permissions. Credentials stay in page
memory and clear on reload. Branch and path selections stay in the URL.

For an existing backend, use `bun run build`, then follow the
[Git example's configuration](../README.md#configuration) and add
`-f spin.viewer.toml` to its `spin up` command.

Hosted proxies must forward `/browse`, `/browse/api`, `/browse/assets/client.js`
and `/browse/assets/style.css` to Spin. `GET /browse/api` takes `repo` and
optional `ref` and `path` query parameters. `POST /browse/api?repo=<name>`
forwards creation to the Git service, using its write permission:

```sh
curl --fail-with-body -u git:local-git-password \
  -H 'Content-Type: application/json' -d '{"format":"sha1"}' \
  'http://127.0.0.1:19100/browse/api?repo=team/project.git'
```

Creation defaults to SHA-1 and `main`; `default_branch` is optional. A successful
creation returns 201; creating an existing repository returns 409.

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
