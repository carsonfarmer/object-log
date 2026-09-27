import { NotFound, snapshot } from "./repository";
import {
  automaticLogId,
  Catalog,
  canonicalName,
  drop,
  openExisting,
  type Repository,
  settings,
  variable,
} from "./wal";

async function boundedBody(message: Request | Response, limit: number): Promise<string> {
  const reader = message.body?.getReader(),
    decoder = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });
  let bytes = 0,
    text = "";
  try {
    for (let part = await reader?.read(); part && !part.done; part = await reader?.read()) {
      bytes += part.value.length;
      if (bytes > limit) throw new Error("HTTP body exceeds limit");
      text += decoder.decode(part.value, { stream: true });
    }
    return text + decoder.decode();
  } finally {
    await reader?.cancel();
    reader?.releaseLock();
  }
}

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
  const input = query.get("repo") ?? "",
    name = canonicalName(input);
  if (
    !input ||
    input === "*" ||
    name === ".git" ||
    name.length > 4096 ||
    /[\s%?#\\]/.test(input) ||
    input.split("/").some((part) => !part || part === "." || part === "..") ||
    new URL(`/${name}`, request.url).pathname !== `/${name}`
  )
    return new Response("Repository not found", { status: 404 });
  const repositories: Record<string, Partial<Repository>> = JSON.parse(
    variable("git_repositories"),
  );
  const policy =
    Object.entries(repositories).find(([key]) => key !== "*" && canonicalName(key) === name)?.[1] ??
    repositories["*"];
  if (!policy) return new Response("Repository not found", { status: 404 });
  const repository = { ...policy, log_id: policy.log_id || automaticLogId(name) };
  if (request.method === "POST") {
    if (query.has("ref") || query.has("path"))
      return new Response("Invalid create query", { status: 400 });
    let body: string;
    try {
      body = await boundedBody(request, 4096);
    } catch (error) {
      return new Response("Invalid creation request", {
        status: error instanceof Error && error.message === "HTTP body exceeds limit" ? 413 : 400,
      });
    }
    const created = await fetch(`http://git.spin.internal/${name}/create`, {
      method: "POST",
      headers: {
        Authorization: request.headers.get("Authorization") ?? "",
        "Content-Type": "application/json",
      },
      redirect: "manual",
      body,
    });
    body = await boundedBody(created, 65536);
    if (
      created.status === 201 &&
      !created.headers.get("Content-Type")?.startsWith("application/json")
    )
      return new Response("Invalid creation response", { status: 502 });
    const status = [201, 400, 401, 403, 404, 408, 409, 413, 503].includes(created.status)
      ? created.status
      : 502;
    return new Response(status === 502 ? "Repository creation unavailable" : body, {
      status,
      headers: {
        "Content-Type": status === 201 ? "application/json" : "text/plain; charset=utf-8",
        "Cache-Control": "no-store",
      },
    });
  }
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
  let session = openExisting(settings(repository));
  const budget = { bytes: 0 };
  try {
    for (let attempt = 0; ; attempt++) {
      let catalog: Catalog | undefined;
      try {
        catalog = new Catalog(session, repository.format, budget);
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
