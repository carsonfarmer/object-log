import { NotFound, snapshot } from "./repository";
import { Catalog, drop, openExisting, type Repository, settings, variable } from "./wal";

export async function browse(request: Request): Promise<Response> {
  const query = new URL(request.url).searchParams;
  const path = query.get("path") ?? "";
  if (
    [...query.keys()].some(
      (key) =>
        !["repo", "ref", "path"].includes(key) ||
        query.getAll(key).length !== 1 ||
        (query.get(key)?.length ?? 0) > 4096,
    ) ||
    (path &&
      (path.split("/").length > 64 ||
        path
          .split("/")
          .some((part) => !part || part === "." || part === ".." || part.includes("\0"))))
  )
    return new Response("Invalid browse query", { status: 400 });
  const name = query.get("repo") ?? "";
  const repositories: Record<string, Repository> = JSON.parse(variable("git_repositories"));
  if (!Object.hasOwn(repositories, name))
    return new Response("Repository not found", { status: 404 });
  // Standard Git discovery enforces the unchanged service's repository permissions.
  const authorized = await fetch(
    `http://git.spin.internal/${name}/info/refs?service=git-upload-pack`,
    {
      headers: { Authorization: request.headers.get("Authorization") ?? "" },
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
  if (
    !authorized.headers
      .get("Content-Type")
      ?.startsWith("application/x-git-upload-pack-advertisement")
  )
    return new Response("Invalid Git discovery response", { status: 502 });
  // Successful discovery has validated this same declaratively configured backend.
  let session = openExisting(settings(repositories[name]));
  const budget = { bytes: 0 };
  try {
    for (let attempt = 0; ; attempt++) {
      let catalog: Catalog | undefined;
      try {
        catalog = new Catalog(session, repositories[name].format, budget);
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
          return new Response("Branch or path not found", { status: 404 });
        if (attempt === 0 && (error as { payload?: { tag: string } }).payload?.tag === "expired") {
          const fresh = session.refresh();
          drop(session);
          session = fresh;
        } else throw error;
      } finally {
        catalog?.close();
      }
    }
  } finally {
    drop(session);
  }
}
