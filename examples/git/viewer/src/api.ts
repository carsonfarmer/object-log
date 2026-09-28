import { canonicalName, repositoryName } from "./names";
import { NotFound, snapshot } from "./repository";
import * as wal from "./wal";

export async function browse(request: Request): Promise<Response> {
  const query = new URL(request.url).searchParams;
  const path = query.get("path") ?? "";
  const parts = path ? path.split("/") : [];
  if (
    [...query].some(
      ([key, value]) =>
        !["repo", "ref", "commit", "path", "view"].includes(key) ||
        query.getAll(key).length !== 1 ||
        value.length > 4096,
    ) ||
    (query.has("view") && !["code", "commits"].includes(query.get("view") ?? "")) ||
    (query.has("commit") && !/^(?:[0-9a-f]{40}|[0-9a-f]{64})$/.test(query.get("commit") ?? "")) ||
    parts.length > 64 ||
    parts.some((part) => !part || part === "." || part === ".." || part.includes("\0"))
  )
    return new Response("Invalid browse query", { status: 400 });
  const name = repositoryName(query.get("repo") ?? "", request.url);
  if (!name) return new Response("Repository not found", { status: 404 });
  const repositories: Record<string, Partial<wal.Repository>> = JSON.parse(
    wal.variable("git_repositories"),
  );
  const policy =
    Object.entries(repositories).find(([key]) => key !== "*" && canonicalName(key) === name)?.[1] ??
    repositories["*"];
  if (!policy) return new Response("Repository not found", { status: 404 });
  const repository = { ...policy, log_id: policy.log_id || wal.automaticLogId(name) };
  // Standard Git discovery enforces the unchanged service's repository permissions.
  const authorized = await fetch(
    `http://git.spin.internal/${name}/info/refs?service=git-upload-pack`,
    {
      headers: {
        Authorization: request.headers.get("Authorization") ?? "",
        "Git-Protocol": "version=2",
      },
      redirect: "manual",
    },
  );
  const body = authorized.body?.getReader();
  try {
    let bytes = 0;
    for (let part = await body?.read(); part && !part.done; part = await body?.read()) {
      bytes += part.value.length;
      if (bytes > 8 << 20) throw new Error("Git discovery exceeds 8 MiB");
    }
  } finally {
    await body?.cancel();
    body?.releaseLock();
  }
  if (authorized.status !== 200)
    return new Response("Repository access denied", {
      status: [401, 403, 404, 503].includes(authorized.status) ? authorized.status : 502,
    });
  const type = authorized.headers.get("Content-Type") ?? "";
  if (!type.startsWith("application/x-git-upload-pack-advertisement"))
    return new Response("Invalid Git discovery response", { status: 502 });
  // Successful discovery has validated this same declaratively configured backend.
  let session = wal.openExisting(wal.settings(repository));
  const budget = { bytes: 0 };
  try {
    for (let attempt = 0; ; attempt++) {
      let catalog: wal.Catalog | undefined;
      try {
        catalog = new wal.Catalog(session, repository.format, budget);
        const view = snapshot(catalog, query),
          usage = session.usage();
        return Response.json(view, {
          headers: {
            "Cache-Control": "no-store",
            "X-Wal-Calls": String(usage.calls),
            "X-Wal-Bytes": String(usage.bytes),
          },
        });
      } catch (error) {
        if (error instanceof NotFound)
          return new Response("Branch, commit or path not found", { status: 404 });
        if (attempt === 0 && (error as { payload?: { tag: string } }).payload?.tag === "expired") {
          const fresh = session.refresh();
          wal.drop(session);
          session = fresh;
        } else throw error;
      } finally {
        catalog?.[Symbol.dispose]();
      }
    }
  } finally {
    wal.drop(session);
  }
}
