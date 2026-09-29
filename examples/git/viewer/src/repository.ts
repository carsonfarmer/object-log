import { type Catalog, hex, MissingObject, NotFound } from "./wal";

export { NotFound };

export type Commit = ReturnType<typeof commit>["summary"];
export type Snapshot = ReturnType<typeof snapshot>;
type TreeEntry = ReturnType<typeof tree>[number];
interface FilePreview {
  id: string;
  size: number;
  state: string;
  text?: string;
}
const text = new TextDecoder();
const strictText = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });
const previewBytes = 256 << 10;
const metadataBytes = 16 << 20;

// Decode only loose commit/tree objects; packs and deltas remain the Git service's concern.
function commit(catalog: Catalog, id: string) {
  const raw = text.decode(catalog.object(id, 1, metadataBytes).bytes);
  const split = raw.indexOf("\n\n"),
    header = raw.slice(0, split).split("\n");
  const fields = (name: string) =>
    header.find((line) => line.startsWith(`${name} `))?.slice(name.length + 1);
  const author = /^(.*?) <.*>[ \t]+(-?\d+)[ \t]+[+-]\d{4}$/.exec(fields("author") ?? "");
  const tree = fields("tree");
  if (split < 0 || !tree || !author) throw new Error("Invalid Git commit");
  return {
    tree,
    parent: fields("parent") ?? "",
    summary: {
      id,
      title: raw
        .slice(split + 2)
        .split("\n", 1)[0]
        .slice(0, 512),
      author: author[1].slice(0, 512),
      date: new Date(Number(author[2]) * 1000).toISOString(),
    },
  };
}

function tree(catalog: Catalog, id: string) {
  const bytes = catalog.object(id, 2, metadataBytes).bytes;
  if (!bytes) throw new Error("Tree exceeds metadata limit");
  const width = catalog.root.Format === "sha256" ? 32 : 20,
    result = [];
  for (let offset = 0; offset < bytes.length; ) {
    const space = bytes.indexOf(32, offset),
      nul = bytes.indexOf(0, space + 1);
    if (space < offset || nul <= space || nul + 1 + width > bytes.length)
      throw new Error("Invalid Git tree");
    const mode = text.decode(bytes.subarray(offset, space)),
      rawName = bytes.subarray(space + 1, nul);
    let name: string,
      unavailable = false;
    try {
      name = strictText.decode(rawName);
    } catch {
      name = hex(rawName).replace(/../g, "\\x$&");
      unavailable = true;
    }
    if (!name || name === "." || name === ".." || name.includes("/"))
      throw new Error("Invalid Git name");
    result.push({
      name,
      kind: mode === "40000" ? "directory" : mode === "160000" ? "submodule" : "file",
      id: hex(bytes.subarray(nul + 1, nul + 1 + width)),
      ...(unavailable ? { unavailable: true as const } : {}),
    });
    offset = nul + 1 + width;
  }
  return result;
}

export function snapshot(catalog: Catalog, query: URLSearchParams) {
  const history = query.get("view") === "commits";
  const branches = Object.keys(catalog.root.Refs)
    .filter((ref) => ref.startsWith("refs/heads/"))
    .sort();
  const branch =
    query.get("ref") ||
    (branches.includes(catalog.root.Head) ? catalog.root.Head : branches[0]) ||
    "";
  const view = {
    branch,
    branches,
    path: query.get("path") ?? "",
    history: [] as Commit[],
    next: "",
    entries: [] as TreeEntry[],
    more: false,
    file: undefined as FilePreview | undefined,
  };
  const selected = query.get("commit");
  if (!branches.length && !query.get("ref") && !selected && !view.path) return view;
  if (!branches.includes(branch)) throw new NotFound();
  if (selected && selected.length !== (catalog.root.Format === "sha256" ? 64 : 40))
    throw new NotFound();
  let id = selected || catalog.root.Refs[branch],
    tip = "";
  try {
    for (let i = 0; id && i < (history ? 20 : 1); i++) {
      const current = commit(catalog, id);
      if (i === 0) tip = current.tree;
      view.history.push(current.summary);
      id = current.parent;
    }
  } catch (error) {
    if (!view.history.length && selected && error instanceof MissingObject) throw new NotFound();
    throw error;
  }
  if (history) {
    view.next = id;
    return view;
  }
  let entries = tree(catalog, tip);
  const parts = view.path ? view.path.split("/") : [];
  for (const [index, part] of parts.entries()) {
    const entry = entries.find((value) => !value.unavailable && value.name === part);
    if (!entry) throw new NotFound();
    if (entry.kind === "directory") {
      entries = tree(catalog, entry.id);
      continue;
    }
    if (index !== parts.length - 1) throw new NotFound();
    view.file = { id: entry.id, size: 0, state: "submodule" };
    if (entry.kind !== "submodule") {
      const blob = catalog.object(entry.id, 3, previewBytes);
      view.file.size = blob.size;
      view.file.state = blob.bytes ? "binary" : "large";
      try {
        if (blob.bytes && !blob.bytes.includes(0))
          Object.assign(view.file, { state: "text", text: strictText.decode(blob.bytes) });
      } catch {
        /* Binary preview. */
      }
    }
    return view;
  }
  view.more = entries.length > 500;
  view.entries = entries.slice(0, 500);
  return view;
}
