import { render } from "preact";
import { useLayoutEffect, useState } from "preact/hooks";
import { LocationProvider, useLocation } from "preact-iso/router";

import { HistoryPages } from "./history";
import { repositoryName } from "./names";
import type { Commit, Snapshot } from "./repository";

const messages: Record<number, string> = {
  400: "Invalid repository, branch, commit or path.",
  401: "Authentication required.",
  403: "This credential cannot read this repository.",
  404: "Repository, commit or path not found.",
};

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

function History({
  view,
  pages,
  query,
}: {
  view: Snapshot;
  pages: HistoryPages;
  query: Record<string, string>;
}) {
  const first = view.history[0]?.id;
  const origin =
    query.origin?.length === first?.length && /^(?:[0-9a-f]{40}|[0-9a-f]{64})$/.test(query.origin)
      ? query.origin
      : first;
  const prefix = JSON.stringify([query.repo.replace(/\.git$/, ""), view.branch, origin]);
  const previous = first ? pages.previous(prefix + first) : undefined;
  const current = { ...query, ref: view.branch, commit: first ?? "", origin: origin ?? "" };
  if (first && view.next) pages.remember(prefix + view.next, first);
  return (
    <section aria-label="Commits">
      <div class="panel">
        {view.history.map((commit) => (
          <article key={commit.id}>
            <CommitLink commit={commit} branch={view.branch} />
            <p>
              {commit.author || "Unknown author"} committed on{" "}
              <time dateTime={commit.date}>{new Date(commit.date).toLocaleDateString()}</time>
            </p>
          </article>
        ))}
        {!view.history.length && <p>No commits yet.</p>}
        {(previous || view.next) && (
          <article class="row tip pager">
            {previous ? (
              <a href={href({ ...current, commit: previous })}>← Newer commits</a>
            ) : (
              <span />
            )}
            {view.next && (
              <a
                href={href({ ...current, commit: view.next })}
                onClick={(event) => {
                  if (
                    event.button === 0 &&
                    !event.ctrlKey &&
                    !event.metaKey &&
                    !event.altKey &&
                    !event.shiftKey
                  )
                    history.replaceState(null, "", href(current));
                }}
              >
                Older commits →
              </a>
            )}
          </article>
        )}
      </div>
    </section>
  );
}

const previewMessages: Record<string, string> = {
  binary: "Binary file · preview unavailable",
  large: "This file exceeds the 256 KiB preview limit.",
};

function Code({ view, query }: { view: Snapshot; query: Record<string, string> }) {
  const link = (label: string, path: string) => <a href={href({ ...query, path })}>{label}</a>;
  const file = view.file;
  const entries = [...view.entries].sort(
    (a, b) =>
      Number(b.kind === "directory") - Number(a.kind === "directory") ||
      a.name.localeCompare(b.name),
  );
  if (!file && view.path) entries.unshift({ name: "..", kind: "directory", id: "" });
  return (
    <section aria-label="Code">
      <nav aria-label="File path">
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
        {file && (
          <article class="row tip">
            <span>{file.size.toLocaleString()} bytes</span>
          </article>
        )}
        {file &&
          (file.state === "text" ? (
            <pre>{file.text}</pre>
          ) : (
            <p>{previewMessages[file.state] ?? `Submodule · ${file.id}`}</p>
          ))}
        {entries.map((entry) => (
          <article class="row" key={`${entry.unavailable ? "bytes" : "text"}:${entry.name}`}>
            <span aria-hidden="true">{entry.kind === "directory" ? "📁" : "📄"}</span>
            {entry.unavailable ? (
              <span title="This filename is not UTF-8">{entry.name}</span>
            ) : (
              link(
                entry.name,
                entry.name === ".."
                  ? view.path.split("/").slice(0, -1).join("/")
                  : [view.path, entry.name].filter(Boolean).join("/"),
              )
            )}
          </article>
        ))}
        {!file && !view.entries.length && (
          <p>
            {view.history.length
              ? "This directory is empty."
              : "This repository is empty. Push a branch to start exploring."}
          </p>
        )}
        {view.more && <p>Showing the first 500 entries.</p>}
      </div>
    </section>
  );
}

function App() {
  const { url, query: requested, route } = useLocation();
  const [authorization, setAuthorization] = useState({ header: "" });
  const [result, setResult] = useState<{
    url?: string;
    query?: Record<string, string>;
    view?: Snapshot;
    error?: string;
    status?: number;
  }>({});
  const [copy, setCopy] = useState({ url: "", label: "Copy URL" });
  const [pages] = useState(() => new HistoryPages());
  const repository = requested.repo ?? "";
  const valid = !!repositoryName(repository, location.origin);
  const shown = result.query?.repo === repository ? result : undefined;
  const query = shown?.query ?? requested;
  const view = shown?.view;
  const tab = query.view === "commits" ? "commits" : "code";
  const loading = valid && !result.error && result.url !== url;
  const cloneURL = `${location.origin}/${repository}`;

  useLayoutEffect(() => {
    document.title = repository ? `${repository} · object-log` : "Repository · object-log";
    setResult((current) =>
      current.query?.repo === repository ? { ...current, url: undefined } : {},
    );
    if (!valid) return;
    const request = new AbortController();
    void (async () => {
      try {
        const selectors = new URL(url, location.origin).searchParams;
        selectors.delete("origin");
        const response = await fetch(`/_viewer/api?${selectors}`, {
          headers: authorization.header ? { Authorization: authorization.header } : {},
          credentials: "omit",
          cache: "no-store",
          signal: request.signal,
        });
        const next = response.ok
          ? { url, query: requested, view: (await response.json()) as Snapshot }
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
      <header class="row">
        <a href="/">◈ object-log</a>
        <span role="status">
          {loading && <span class="loading" role="img" aria-label="Loading repository" />}
        </span>
      </header>
      <main>
        <h1>{repository.replace(/\.git$/, "") || "Repositories"}</h1>
        {valid && !view && result.error && (
          <>
            <p role="status">{result.error}</p>
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
              setResult({});
              setAuthorization({
                header: data.get("mode") === "bearer" ? `Bearer ${credential}` : `Basic ${encoded}`,
              });
            }}
          >
            {valid && (
              <>
                <h2>Sign in to read this repository</h2>
                <select name="mode" aria-label="Credential type">
                  <option value="basic">Git password</option>
                  <option value="bearer">Cognito access token</option>
                </select>
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
            {!valid && <p role="status">{result.error}</p>}
          </form>
        )}
        {valid && view && (
          <>
            <nav class="row tabs" aria-label="Repository views">
              {["code", "commits"].map((name) => (
                <a
                  key={name}
                  aria-current={tab === name ? "page" : undefined}
                  href={href({ ...query, view: name })}
                >
                  {name === "code" ? "Code" : "Commits"}
                </a>
              ))}
            </nav>
            <div class="row toolbar">
              <label class="row">
                Branch
                <select
                  value={view.branch}
                  disabled={!view.branches.length}
                  onChange={(event) =>
                    route(href({ repo: repository, ref: event.currentTarget.value, view: tab }))
                  }
                >
                  {view.branches.map((ref) => (
                    <option key={ref} value={ref}>
                      {ref.replace(/^refs\/heads\//, "")}
                    </option>
                  ))}
                </select>
              </label>
              {"commit" in query && (
                <a href={href({ repo: repository, ref: view.branch, view: tab })}>Branch tip</a>
              )}
              <button
                type="button"
                title={cloneURL}
                onClick={async () => {
                  try {
                    await navigator.clipboard.writeText(cloneURL);
                    setCopy({ url: cloneURL, label: "Copied" });
                  } catch {
                    setCopy({ url: cloneURL, label: "Copy failed" });
                  }
                }}
              >
                {copy.url === cloneURL ? copy.label : "Copy URL"}
              </button>
              {copy.url === cloneURL && copy.label === "Copy failed" && <code>{cloneURL}</code>}
            </div>
            {tab === "commits" ? (
              <History view={view} pages={pages} query={query} />
            ) : (
              <Code view={view} query={query} />
            )}
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
