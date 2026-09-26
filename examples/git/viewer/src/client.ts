export {};

interface Commit {
  id: string;
  title: string;
  author: string;
  date: string;
}
interface Entry {
  name: string;
  kind: string;
  id: string;
}
interface Snapshot {
  branch: string;
  branches: string[];
  path: string;
  history: Commit[];
  entries: Entry[];
  more: boolean;
  file?: { id: string; size: number; state: string; text?: string };
}

function get<T extends HTMLElement>(id: string): T {
  return document.getElementById(id) as T;
}
function node<K extends keyof HTMLElementTagNameMap>(
  tag: K,
  text = "",
  className = "",
): HTMLElementTagNameMap[K] {
  const element = document.createElement(tag);
  element.textContent = text;
  element.className = className;
  return element;
}
function icon(kind: string): SVGSVGElement {
  const svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
  svg.setAttribute("viewBox", "0 0 16 16");
  svg.setAttribute("aria-hidden", "true");
  const path = document.createElementNS(svg.namespaceURI, "path");
  path.setAttribute(
    "d",
    kind === "directory"
      ? "M1 2h5l2 2h7v9H1Z"
      : "M3 1h6l4 4v10H3Zm6 1v4h3M5 9h6M5 12h6",
  );
  if (kind === "directory") svg.classList.add("directory-icon");
  svg.append(path);
  return svg;
}
function validRepository(name: string): boolean {
  return (
    name.endsWith(".git") &&
    !/[\s%?#\\]/.test(name) &&
    name
      .split("/")
      .every((part) => part !== "" && part !== "." && part !== "..") &&
    new URL("/" + name, location.origin).pathname === "/" + name
  );
}

const repository = new URLSearchParams(location.search).get("repo") ?? "";
const statusMessage = get("status");
const auth = get<HTMLFormElement>("auth");
const content = get("content");
const branch = get<HTMLSelectElement>("branch");
let authorization = "";
let pending: AbortController | undefined;

function link(label: string, path: string): HTMLAnchorElement {
  const anchor = node("a", label);
  const query = new URLSearchParams(location.search);
  query.set("path", path);
  anchor.href = "?" + query.toString();
  anchor.addEventListener("click", (event) => {
    if (event.ctrlKey || event.metaKey || event.shiftKey || event.altKey)
      return;
    event.preventDefault();
    history.pushState(null, "", anchor.href);
    void load();
  });
  return anchor;
}
function date(value: string): string {
  return new Date(value).toLocaleDateString(undefined, {
    month: "short",
    day: "numeric",
    year: "numeric",
  });
}
function render(view: Snapshot): void {
  branch.replaceChildren();
  for (const ref of view.branches) {
    const option = node("option", ref.replace(/^refs\/heads\//, ""));
    option.value = ref;
    branch.append(option);
  }
  branch.value = view.branch;
  branch.disabled = view.branches.length === 0;
  get("branch-count").textContent =
    `${view.branches.length} ${view.branches.length === 1 ? "branch" : "branches"}`;
  get("commit-count").textContent = String(view.history.length);
  const breadcrumbs = get("breadcrumbs");
  breadcrumbs.replaceChildren(link(repository.replace(/\.git$/, ""), ""));
  const parts = view.path ? view.path.split("/") : [];
  parts.forEach((part, i) =>
    breadcrumbs.append(
      node("span", "/", "muted"),
      link(part, parts.slice(0, i + 1).join("/")),
    ),
  );
  const tip = get("tip");
  tip.replaceChildren();
  const latest = view.history[0];
  tip.hidden = !latest;
  if (latest)
    tip.append(
      node("span", latest.author.slice(0, 1).toUpperCase() || "G", "avatar"),
      node("span", latest.title || "Untitled commit", "title"),
      node("span", latest.id.slice(0, 7), "hash"),
    );
  const files = get("files");
  files.replaceChildren();
  if (view.file) {
    const header = node("div", "", "file-header");
    header.append(
      node("span", `${view.file.size.toLocaleString()} bytes`, "muted"),
      node("span", view.file.id.slice(0, 12), "hash"),
    );
    files.append(header);
    const messages: Record<string, string> = {
      binary: "Binary file · preview unavailable",
      large: "This file exceeds the 256 KiB preview limit.",
      submodule: "Submodule · " + view.file.id,
    };
    files.append(
      view.file.state === "text"
        ? node("pre", view.file.text ?? "")
        : node(
            "div",
            messages[view.file.state] ?? "Preview unavailable",
            "empty",
          ),
    );
  } else if (!view.history.length) {
    files.append(
      node(
        "div",
        "This repository is empty. Push a branch to start exploring.",
        "empty",
      ),
    );
  } else {
    const entries = [...view.entries].sort(
      (a, b) =>
        Number(b.kind === "directory") - Number(a.kind === "directory") ||
        a.name.localeCompare(b.name),
    );
    for (const entry of entries) {
      const row = node("div", "", "file-row");
      row.append(
        icon(entry.kind),
        link(entry.name, [...parts, entry.name].join("/")),
        node("span", entry.id.slice(0, 7), "hash"),
      );
      files.append(row);
    }
    if (!entries.length)
      files.append(node("div", "This directory is empty.", "empty"));
    if (view.more)
      files.append(node("div", "Showing the first 500 entries.", "empty"));
  }
  const commits = get("history");
  commits.replaceChildren();
  for (const commit of view.history) {
    const row = node("div", "", "history-row");
    const info = node("div", "", "history-info");
    info.append(
      node("div", commit.title || "Untitled commit", "history-title"),
      node(
        "div",
        `${commit.author || "Unknown author"} committed on ${date(commit.date)}`,
        "muted",
      ),
    );
    row.append(
      node("span", commit.author.slice(0, 1).toUpperCase() || "G", "avatar"),
      info,
      node("span", commit.id.slice(0, 7), "hash"),
    );
    commits.append(row);
  }
  if (!view.history.length)
    commits.append(node("div", "No commits yet.", "empty"));
}

async function load(): Promise<void> {
  pending?.abort();
  const request = new AbortController();
  pending = request;
  statusMessage.hidden = false;
  statusMessage.textContent = "Loading repository…";
  auth.hidden = true;
  content.hidden = true;
  try {
    const query = new URLSearchParams(location.search);
    query.delete("repo");
    const response = await fetch(`/${repository}/_browse?${query}`, {
      headers: authorization ? { Authorization: authorization } : {},
      credentials: "omit",
      cache: "no-store",
      signal: request.signal,
    });
    if (response.status === 401) {
      statusMessage.textContent = "Authentication required.";
      auth.hidden = false;
      get<HTMLInputElement>("credential").focus();
      return;
    }
    if (!response.ok) {
      const messages: Record<number, string> = {
        403: "This credential does not have read access.",
        404: "Repository or path not found. Check the path and push a branch before browsing.",
        400: "This branch or path query is invalid.",
      };
      throw new Error(
        messages[response.status] ??
          "Repository unavailable. Try refreshing this page.",
      );
    }
    const view = (await response.json()) as Snapshot;
    if (pending !== request) return;
    render(view);
    statusMessage.hidden = true;
    content.hidden = false;
  } catch (error) {
    if (request.signal.aborted) return;
    statusMessage.textContent =
      error instanceof Error ? error.message : "Repository unavailable.";
    auth.hidden = false;
  }
}

auth.addEventListener("submit", (event) => {
  event.preventDefault();
  const field = get<HTMLInputElement>("credential");
  const credential = field.value;
  field.value = "";
  const bytes = new TextEncoder().encode("git:" + credential);
  authorization =
    get<HTMLSelectElement>("auth-mode").value === "bearer"
      ? "Bearer " + credential
      : "Basic " +
        btoa(Array.from(bytes, (byte) => String.fromCharCode(byte)).join(""));
  void load();
});
branch.addEventListener("change", () => {
  const query = new URLSearchParams({ repo: repository, ref: branch.value });
  history.pushState(null, "", "?" + query);
  void load();
});
for (const name of ["code", "commits"]) {
  get(name + "-tab").addEventListener("click", () => {
    for (const panel of ["code", "commits"]) {
      const active = panel === name;
      get(panel + "-view").hidden = !active;
      get(panel + "-tab").classList.toggle("active", active);
      get(panel + "-tab").setAttribute("aria-pressed", String(active));
    }
  });
}
get<HTMLFormElement>("open-repo").addEventListener("submit", (event) => {
  event.preventDefault();
  const name = get<HTMLInputElement>("repo-name").value.trim();
  if (validRepository(name))
    location.href = "/browse?" + new URLSearchParams({ repo: name });
  else
    statusMessage.textContent =
      "Enter a repository path such as team/project.git.";
});
get("copy-url").addEventListener("click", async () => {
  try {
    await navigator.clipboard.writeText(
      get<HTMLInputElement>("clone-url").value,
    );
    get("copy-url").textContent = "Copied";
  } catch {
    get<HTMLInputElement>("clone-url").select();
  }
});
addEventListener("popstate", () => void load());
if (validRepository(repository)) {
  get("repository").textContent = repository.replace(/\.git$/, "");
  document.title = repository + " · object-log";
  get<HTMLInputElement>("clone-url").value = location.origin + "/" + repository;
  void load();
} else {
  get<HTMLFormElement>("open-repo").hidden = false;
  statusMessage.textContent = "Enter the path of a configured Git repository.";
}
