import { render } from "preact";
import { useLayoutEffect, useRef, useState } from "preact/hooks";
import { LocationProvider, useLocation } from "preact-iso/router";

import { repositoryName } from "./names";
import type { Commit, Snapshot } from "./repository";

const messages: Record<number, string> = {
  400: "Invalid repository, branch, commit or path.",
  401: "Authentication required.",
  403: "This credential cannot read this repository.",
  404: "Repository, commit or path not found.",
};

function Icon({ kind }: { kind: "folder" | "file" | "code" | "chevron" }) {
  const paths = {
    folder: "M2 4h5l2 2h5v7H2z",
    file: "M4 2h5l3 3v9H4zM9 2v4h3",
    code: "M6 4 2 8l4 4m4-8 4 4-4 4",
    chevron: "m4 6 4 4 4-4",
  };
  return (
    <svg aria-hidden="true" width="16" height="16" viewBox="0 0 16 16" fill="none">
      <path d={paths[kind]} stroke="currentColor" strokeWidth="1.5" strokeLinejoin="round" />
    </svg>
  );
}

const href = (query: Record<string, string>) => `/?${new URLSearchParams(query)}`;

function CommitLink({ commit, branch }: { commit: Commit; branch: string }) {
  const { query } = useLocation();
  return (
    <a class="row" href={href({ repo: query.repo, ref: branch, commit: commit.id })}>
      <code>{commit.id.slice(0, 7)}</code>
      <span class="commit-title" title={commit.title}>
        {commit.title || "Untitled commit"}
      </span>
    </a>
  );
}

function History({ view }: { view: Snapshot }) {
  return (
    <section aria-label="Recent commits">
      <p class="muted">
        Recent commits · first parent · select a commit to explore earlier history
      </p>
      <div class="panel">
        {view.history.map((commit) => (
          <article key={commit.id}>
            <CommitLink commit={commit} branch={view.branch} />
            <p class="muted">
              {commit.author || "Unknown author"} committed on{" "}
              <time dateTime={commit.date}>{new Date(commit.date).toLocaleDateString()}</time>
            </p>
          </article>
        ))}
        {!view.history.length && <p class="empty">No commits yet.</p>}
      </div>
    </section>
  );
}

const previewMessages: Record<string, string> = {
  binary: "Binary file · preview unavailable",
  large: "This file exceeds the 256 KiB preview limit.",
};

function Code({ view }: { view: Snapshot }) {
  const { query } = useLocation();
  const link = (label: string, path: string) => <a href={href({ ...query, path })}>{label}</a>;
  const file = view.file;
  const entries = [...view.entries].sort(
    (a, b) =>
      Number(b.kind === "directory") - Number(a.kind === "directory") ||
      a.name.localeCompare(b.name),
  );
  return (
    <section aria-label="Code">
      <nav class="breadcrumbs" aria-label="File path">
        {link(query.repo.replace(/\.git$/, ""), "")}
        {(view.path ? view.path.split("/") : []).map((part, i, parts) => (
          <span key={parts.slice(0, i + 1).join("/")}>
            {" "}
            / {link(part, parts.slice(0, i + 1).join("/"))}
          </span>
        ))}
      </nav>
      <div class="panel">
        {view.history[0] && (
          <article class="row tip">
            <CommitLink commit={view.history[0]} branch={view.branch} />
          </article>
        )}
        {file ? (
          <>
            <article class="row tip">
              <span>{file.size.toLocaleString()} bytes</span>
              <code>{file.id.slice(0, 12)}</code>
            </article>
            {file.state === "text" ? (
              <pre>{file.text}</pre>
            ) : (
              <p class="empty">{previewMessages[file.state] ?? `Submodule · ${file.id}`}</p>
            )}
          </>
        ) : (
          <>
            {view.path && (
              <article class="row">
                <Icon kind="folder" />
                {link("..", view.path.split("/").slice(0, -1).join("/"))}
              </article>
            )}
            {entries.map((entry) => (
              <article class="row" key={`${entry.unavailable ? "bytes" : "text"}:${entry.name}`}>
                <Icon kind={entry.kind === "directory" ? "folder" : "file"} />
                {entry.unavailable ? (
                  <span title="This filename is not UTF-8">{entry.name}</span>
                ) : (
                  link(entry.name, [view.path, entry.name].filter(Boolean).join("/"))
                )}
                <code>{entry.id.slice(0, 7)}</code>
              </article>
            ))}
            {!entries.length && (
              <p class="empty">
                {view.history.length
                  ? "This directory is empty."
                  : "This repository is empty. Push a branch to start exploring."}
              </p>
            )}
            {view.more && <p class="empty">Showing the first 500 entries.</p>}
          </>
        )}
      </div>
    </section>
  );
}

function App() {
  const { url, query, route } = useLocation();
  const [authorization, setAuthorization] = useState({ header: "" });
  const [result, setResult] = useState<{ view?: Snapshot; error?: string; status?: number }>({});
  const [tab, setTab] = useState("code");
  const [copied, setCopied] = useState(false);
  const cloneInput = useRef<HTMLInputElement>(null);
  const repository = query.repo ?? "";
  const valid = !!repositoryName(repository, location.origin);
  const view = result.view;
  const cloneURL = `${location.origin}/${repository}`;

  useLayoutEffect(() => {
    document.title = repository ? `${repository} · object-log` : "Repository · object-log";
    setResult({});
    if (!valid) return;
    const request = new AbortController();
    void (async () => {
      try {
        const response = await fetch(`/_viewer/api${new URL(url, location.origin).search}`, {
          headers: authorization.header ? { Authorization: authorization.header } : {},
          credentials: "omit",
          cache: "no-store",
          signal: request.signal,
        });
        const next = response.ok
          ? { view: (await response.json()) as Snapshot }
          : {
              error: messages[response.status] ?? "Repository unavailable. Try again.",
              status: response.status,
            };
        if (!request.signal.aborted) setResult(next);
      } catch (error) {
        if (!request.signal.aborted)
          setResult({ error: error instanceof Error ? error.message : "Repository unavailable." });
      }
    })();
    return () => request.abort();
  }, [url, repository, valid, authorization]);

  return (
    <>
      <header class="row site-header">
        <a href="/">◈ object-log</a>
        <span>Repository explorer</span>
      </header>
      <main>
        <h1>{repository.replace(/\.git$/, "") || "Repositories"}</h1>
        {valid && !view && (
          <>
            <p role="status">{result.error ?? "Loading repository…"}</p>
            {result.status === 404 && !query.ref && !query.commit && !query.path && (
              <p class="panel">
                Push your first branch to create this repository:
                <br />
                <code>git push {cloneURL} HEAD</code>
              </p>
            )}
          </>
        )}
        {(!valid || (!view && result.error)) && (
          <form
            class="panel"
            onSubmit={(event) => {
              event.preventDefault();
              const data = new FormData(event.currentTarget);
              if (!valid) {
                const name = String(data.get("repo")).trim();
                if (repositoryName(name, location.origin)) route(href({ repo: name }));
                else setResult({ error: "Enter a repository path such as team/project.git." });
                return;
              }
              const credential = String(data.get("credential"));
              event.currentTarget.reset();
              const encoded = btoa(
                String.fromCharCode(...new TextEncoder().encode(`git:${credential}`)),
              );
              setAuthorization({
                header: data.get("mode") === "bearer" ? `Bearer ${credential}` : `Basic ${encoded}`,
              });
            }}
          >
            {valid && (
              <>
                <h2>Sign in to read this repository</h2>
                <label>
                  Credential
                  <select name="mode">
                    <option value="basic">Git password</option>
                    <option value="bearer">Cognito access token</option>
                  </select>
                </label>
              </>
            )}
            <label>
              {valid ? "Password or token" : "Repository name"}
              <input
                key={valid ? "credential" : "repo"}
                name={valid ? "credential" : "repo"}
                type={valid ? "password" : "text"}
                autoComplete="off"
                maxLength={valid ? 8192 : 4096}
                placeholder={valid ? "" : "team/project"}
                required
              />
            </label>
            <button type="submit">{valid ? "Sign in" : "Open repository"}</button>
            <p class="muted">
              {valid
                ? "Used for this page only."
                : "Open a repository. A first push creates it at the same Git URL."}
            </p>
            {!valid && <p role="status">{result.error}</p>}
          </form>
        )}
        {valid && view && (
          <>
            <nav class="row tabs" aria-label="Repository views">
              {["code", "commits"].map((name) => (
                <button
                  type="button"
                  key={name}
                  aria-pressed={tab === name}
                  onClick={() => setTab(name)}
                >
                  {name === "code" ? "Code" : `Commits ${view.history.length}`}
                </button>
              ))}
            </nav>
            <div class="row toolbar">
              <label class="row">
                Branch
                <select
                  value={view.branch}
                  disabled={!view.branches.length}
                  onChange={(event) =>
                    route(href({ repo: repository, ref: event.currentTarget.value }))
                  }
                >
                  {view.branches.map((ref) => (
                    <option key={ref} value={ref}>
                      {ref.replace(/^refs\/heads\//, "")}
                    </option>
                  ))}
                </select>
              </label>
              <span class="muted">
                {view.branches.length} {view.branches.length === 1 ? "branch" : "branches"}
              </span>
              {"commit" in query && (
                <a href={href({ repo: repository, ref: view.branch })}>Branch tip</a>
              )}
              <details>
                <summary class="row">
                  <Icon kind="code" /> Code <Icon kind="chevron" />
                </summary>
                <div class="panel clone-box">
                  <label>
                    Clone URL
                    <input ref={cloneInput} value={cloneURL} readOnly />
                  </label>
                  <button
                    type="button"
                    onClick={async () => {
                      try {
                        await navigator.clipboard.writeText(cloneURL);
                        setCopied(true);
                      } catch {
                        cloneInput.current?.select();
                      }
                    }}
                  >
                    {copied ? "Copied" : "Copy URL"}
                  </button>
                </div>
              </details>
            </div>
            {tab === "commits" ? <History view={view} /> : <Code view={view} />}
          </>
        )}
      </main>
    </>
  );
}

render(
  <LocationProvider scope="/?">
    <App />
  </LocationProvider>,
  document.body,
);
