# Repository explorer

A small read-only repository UI implemented in TypeScript as its own Spin
component. It browses branches, directories and text files, and shows the eight
most recent commits along the first parent. The browser uses no framework or
third-party runtime library.

## Run

Install the [Git example prerequisites](../README.md) plus Node.js 24 or later.
Build the optional viewer and start both components from a checkout:

```sh
cd examples/git/viewer
npm ci
npm run check
npm run build
cd ../../..
OBJECT_LOG_GIT_LOCAL_MANIFEST=spin.viewer.toml make git-local
```

The starter builds Git, starts an isolated MinIO backend, validates it, and
prints the Git URLs and password. Push a repository, then open:

```text
http://127.0.0.1:19100/browse?repo=sha256.git
```

For an existing backend, build Git with `make git-build` and pass
`-f spin.viewer.toml` to the Git guide's normal `spin up` command, using the same
protected variables file.

The `/browse` landing page also accepts a repository path such as
`team/project.git`. Sign in with the Git password or a Cognito access token.
The page keeps the credential in memory and clears it on reload; it never puts
it in a URL, cookie, or browser storage. Directory links preserve the selected
branch, and browser Back/Forward works.

`npm run build` creates `viewer/dist/viewer.wasm` and the ignored
`examples/git/spin.viewer.toml`. That manifest copies the authoritative
`spin.toml` and adds three exact UI/asset routes. Rebuild the viewer after
changing the base manifest. The normal Git build and manifest require no Node
installation. A hosted reverse proxy must forward those exact viewer routes
and `/<repository>/_browse` through its existing backend admission rules.

## Data and limits

The TypeScript component serves only the page and its assets. It receives no
storage credentials, has no outbound hosts, and does not read the WAL. The
existing Git component supplies a narrow `GET /<repository>/_browse` adapter
because Git smart HTTP has no tree or file browsing API. That adapter uses the
same read authorization, existing repository open, reader retention, and retry
path as fetch. The core log is unchanged.

Only branch tips are selectable. Tags and arbitrary object IDs are not exposed.
The prototype returns up to 500 entries in one directory, eight first-parent
commits, and UTF-8 text previews up to 256 KiB. Binary and larger files show a
summary; symlinks show their stored target without following it. Submodules
show their commit ID. Commit titles are bounded to 512 bytes. Paths are bounded
to 64 segments and 4096 UTF-8 bytes. File content is rendered as plain text.

Run the adapter tests and Git gates from the repository root:

```sh
make git-check
```

Check the UI source and locked build dependencies from this directory:

```sh
npm run format:check
npm run check
npm audit
npm run build
```

The build pins `componentize-js` consistently across Spin's compiler packages
and explicitly supplies its preview shim. All compiler packages run at build
time; the resulting component needs only Spin at runtime.
