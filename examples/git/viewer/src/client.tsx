import { render, type VNode } from "preact";
import { useEffect, useLayoutEffect, useRef, useState } from "preact/hooks";

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

function validRepository(name: string): boolean {
  return (
    name !== "" &&
    name !== "*" &&
    name !== ".git" &&
    !/[\s%?#\\]/.test(name) &&
    name.split("/").every((part) => part !== "" && part !== "." && part !== "..") &&
    new URL(`/${name}`, location.origin).pathname === `/${name}`
  );
}

function History({ commits, link }: { commits: Commit[]; link: (commit: Commit) => VNode }) {
  return (
    <section aria-label="Recent commits">
      <p class="muted">
        Recent commits · first parent · select a commit to explore earlier history
      </p>
      <div class="panel">
        {commits.length ? (
          commits.map((commit) => (
            <article key={commit.id}>
              <div>
                {link(commit)}
                <p class="muted">
                  {commit.author || "Unknown author"} committed on{" "}
                  <time dateTime={commit.date}>{new Date(commit.date).toLocaleDateString()}</time>
                </p>
              </div>
            </article>
          ))
        ) : (
          <p>No commits yet.</p>
        )}
      </div>
    </section>
  );
}

function Code({
  view,
  repository,
  link,
  commitLink,
}: {
  view: Snapshot;
  repository: string;
  link: (label: string, path: string) => VNode;
  commitLink: (commit: Commit) => VNode;
}) {
  return (
    <section aria-label="Code">
      <nav class="breadcrumbs" aria-label="File path">
        {link(repository.replace(/\.git$/, ""), "")}
        {(view.path ? view.path.split("/") : []).map((part, i, parts) => (
          <span key={parts.slice(0, i + 1).join("/")}>
            {" "}
            / {link(part, parts.slice(0, i + 1).join("/"))}
          </span>
        ))}
      </nav>
      <div class="panel">
        {view.history[0] && <article class="tip">{commitLink(view.history[0])}</article>}
        {view.file ? (
          <>
            <article class="tip">
              <span>{view.file.size.toLocaleString()} bytes</span>
              <code>{view.file.id.slice(0, 12)}</code>
            </article>
            {view.file.state === "text" ? (
              <pre>{view.file.text}</pre>
            ) : (
              <p class="empty">
                {view.file.state === "binary"
                  ? "Binary file · preview unavailable"
                  : view.file.state === "large"
                    ? "This file exceeds the 256 KiB preview limit."
                    : `Submodule · ${view.file.id}`}
              </p>
            )}
          </>
        ) : !view.history.length ? (
          <p class="empty">This repository is empty. Push a branch to start exploring.</p>
        ) : (
          <>
            {view.path && (
              <article>
                <Icon kind="folder" />
                {link("..", view.path.split("/").slice(0, -1).join("/"))}
              </article>
            )}
            {[...view.entries]
              .sort(
                (a, b) =>
                  Number(b.kind === "directory") - Number(a.kind === "directory") ||
                  a.name.localeCompare(b.name),
              )
              .map((entry) => (
                <article key={`${entry.unavailable ? "bytes" : "text"}:${entry.name}`}>
                  <Icon kind={entry.kind === "directory" ? "folder" : "file"} />
                  {entry.unavailable ? (
                    <span title="This filename is not UTF-8">{entry.name}</span>
                  ) : (
                    link(entry.name, [view.path, entry.name].filter(Boolean).join("/"))
                  )}
                  <code>{entry.id.slice(0, 7)}</code>
                </article>
              ))}
            {!view.entries.length && <p class="empty">This directory is empty.</p>}
            {view.more && <p class="empty">Showing the first 500 entries.</p>}
          </>
        )}
      </div>
    </section>
  );
}

function App() {
  const [search, setSearch] = useState(location.search);
  const [authorization, setAuthorization] = useState({ header: "" });
  const [result, setResult] = useState<{ view?: Snapshot; error?: string; status?: number }>({});
  const [tab, setTab] = useState("code");
  const [copied, setCopied] = useState(false);
  const cloneInput = useRef<HTMLInputElement>(null);
  const query = new URLSearchParams(search);
  const repository = query.get("repo") ?? "";
  const valid = validRepository(repository);
  const view = result.view;
  const cloneURL = `${location.origin}/${repository}`;

  useEffect(() => {
    const update = () => setSearch(location.search);
    addEventListener("popstate", update);
    return () => removeEventListener("popstate", update);
  }, []);

  useLayoutEffect(() => {
    document.title = repository ? `${repository} · object-log` : "Repository · object-log";
    if (!valid) return;
    const request = new AbortController();
    setResult({});
    void (async () => {
      try {
        const params = new URLSearchParams(search);
        const response = await fetch(`/_viewer/api?${params}`, {
          headers: authorization.header ? { Authorization: authorization.header } : {},
          credentials: "omit",
          cache: "no-store",
          signal: request.signal,
        });
        if (!response.ok) {
          if (!request.signal.aborted)
            setResult({
              error: messages[response.status] ?? "Repository unavailable. Try again.",
              status: response.status,
            });
          return;
        }
        const snapshot: Snapshot = await response.json();
        if (!request.signal.aborted) setResult({ view: snapshot });
      } catch (error) {
        if (!request.signal.aborted)
          setResult({ error: error instanceof Error ? error.message : "Repository unavailable." });
      }
    })();
    return () => request.abort();
  }, [search, repository, valid, authorization]);

  function navigate(params: URLSearchParams) {
    history.pushState(null, "", `?${params}`);
    setSearch(location.search);
  }
  function link(params: URLSearchParams, label: string | VNode, className?: string) {
    return (
      <a
        class={className}
        href={`?${params}`}
        onClick={(event) => {
          if (event.button || event.ctrlKey || event.metaKey || event.shiftKey || event.altKey)
            return;
          event.preventDefault();
          navigate(params);
        }}
      >
        {label}
      </a>
    );
  }
  function pathLink(label: string, path: string) {
    const params = new URLSearchParams(search);
    params.set("path", path);
    return link(params, label);
  }
  function commitLink(commit: Commit) {
    return link(
      new URLSearchParams({ repo: repository, ref: view?.branch ?? "", commit: commit.id }),
      <>
        <code>{commit.id.slice(0, 7)}</code>
        <span class="commit-title" title={commit.title}>
          {commit.title || "Untitled commit"}
        </span>
      </>,
      "commit-link",
    );
  }

  return (
    <>
      <header>
        <a href="/">◈ object-log</a>
        <span>Repository explorer</span>
      </header>
      <main>
        <h1>{repository.replace(/\.git$/, "") || "Repositories"}</h1>
        {!valid ? (
          <form
            class="panel"
            onSubmit={(event) => {
              event.preventDefault();
              const name = String(new FormData(event.currentTarget).get("repo")).trim();
              if (validRepository(name)) navigate(new URLSearchParams({ repo: name }));
              else setResult({ error: "Enter a repository path such as team/project.git." });
            }}
          >
            <label>
              Repository name
              <input name="repo" placeholder="team/project" required />
            </label>
            <button type="submit">Open repository</button>
            <p class="muted">Open a repository. A first push creates it at the same Git URL.</p>
            <p role="status">{result.error}</p>
          </form>
        ) : !view ? (
          <>
            <p role="status">{result.error ?? "Loading repository…"}</p>
            {result.status === 404 &&
              !query.get("ref") &&
              !query.get("commit") &&
              !query.get("path") && (
                <p class="panel">
                  Push your first branch to create this repository:
                  <br />
                  <code>git push {cloneURL} HEAD</code>
                </p>
              )}
            {result.error && (
              <form
                class="panel"
                onSubmit={(event) => {
                  event.preventDefault();
                  const data = new FormData(event.currentTarget);
                  const credential = String(data.get("credential"));
                  event.currentTarget.reset();
                  const encoded = btoa(
                    Array.from(new TextEncoder().encode(`git:${credential}`), (byte) =>
                      String.fromCharCode(byte),
                    ).join(""),
                  );
                  setAuthorization({
                    header:
                      data.get("mode") === "bearer" ? `Bearer ${credential}` : `Basic ${encoded}`,
                  });
                }}
              >
                <h2>Sign in to read this repository</h2>
                <label>
                  Credential
                  <select name="mode">
                    <option value="basic">Git password</option>
                    <option value="bearer">Cognito access token</option>
                  </select>
                </label>
                <label>
                  Password or token
                  <input
                    name="credential"
                    type="password"
                    autoComplete="off"
                    maxLength={8192}
                    required
                  />
                </label>
                <button type="submit">Sign in</button>
                <p class="muted">Used for this page only.</p>
              </form>
            )}
          </>
        ) : (
          <>
            <nav class="tabs" aria-label="Repository views">
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
            <div class="toolbar">
              <label>
                Branch
                <span class="select-control">
                  <select
                    value={view.branch}
                    disabled={!view.branches.length}
                    onChange={(event) =>
                      navigate(
                        new URLSearchParams({ repo: repository, ref: event.currentTarget.value }),
                      )
                    }
                  >
                    {view.branches.map((ref) => (
                      <option key={ref} value={ref}>
                        {ref.replace(/^refs\/heads\//, "")}
                      </option>
                    ))}
                  </select>
                  <Icon kind="chevron" />
                </span>
              </label>
              <span class="muted">
                {view.branches.length} {view.branches.length === 1 ? "branch" : "branches"}
              </span>
              {query.has("commit") &&
                link(new URLSearchParams({ repo: repository, ref: view.branch }), "Branch tip")}
              <details>
                <summary>
                  <Icon kind="code" /> Code <Icon kind="chevron" />
                </summary>
                <div class="clone-box">
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
            {tab === "commits" ? (
              <History commits={view.history} link={commitLink} />
            ) : (
              <Code view={view} repository={repository} link={pathLink} commitLink={commitLink} />
            )}
          </>
        )}
      </main>
    </>
  );
}

const root = document.getElementById("app");
if (!root) throw new Error("Missing app root");
render(<App />, root);
