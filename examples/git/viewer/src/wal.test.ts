import { afterEach, expect, mock, test } from "bun:test";
import type { Config, Session } from "object-log:storage/wal@0.1.0";
import { zlibSync } from "fflate";

let opened = 0;
const defaultPolicies = '{"team/demo.git":{"log_id":"demo","format":"sha256"}}';
let policies = defaultPolicies;
let openSession: (config: Config) => Session = () => {
  throw new Error("Unexpected storage access");
};
mock.module("object-log:storage/wal@0.1.0", () => ({
  openExisting: (config: Config) => {
    opened++;
    return openSession(config);
  },
}));
mock.module("@spinframework/spin-variables", () => ({
  get: (name: string) => (name === "git_repositories" ? policies : ""),
}));
const { Catalog } = await import("./wal");
const { browse } = await import("./api");
const { snapshot } = await import("./repository");
const originalFetch = globalThis.fetch;
afterEach(() => {
  globalThis.fetch = originalFetch;
  policies = defaultPolicies;
  openSession = () => {
    throw new Error("Unexpected storage access");
  };
});

function emptySession(format: string, overrides: Record<string, unknown> = {}): Session {
  const dispose = () => undefined;
  let next = true;
  return {
    recover: () => ({
      next: () => {
        if (!next) return undefined;
        next = false;
        return { tag: "checkpoint", val: { objects: [{ [Symbol.dispose]: dispose }] } };
      },
      readNode: () => ({
        data: new TextEncoder().encode(
          JSON.stringify({
            Validated: true,
            Format: format,
            Head: "refs/heads/main",
            Refs: {},
            Buckets: [],
            ...overrides,
          }),
        ),
        objects: [],
      }),
      [Symbol.dispose]: dispose,
    }),
    usage: () => ({ calls: 0n, bytes: 0n }),
    [Symbol.dispose]: dispose,
  } as unknown as Session;
}

const discovery = () =>
  new Response("refs", {
    headers: { "Content-Type": "application/x-git-upload-pack-advertisement" },
  });

test("catalog requires a persisted branch HEAD and accepts an unborn branch", () => {
  for (const format of ["sha1", "sha256"]) {
    for (const Head of [undefined, "", "refs/tags/main", "refs/heads/"])
      expect(() => new Catalog(emptySession(format, { Head }), "", { bytes: 0 })).toThrow(
        "Invalid repository catalog",
      );
    const catalog = new Catalog(emptySession(format), "", { bytes: 0 });
    expect(snapshot(catalog, new URLSearchParams()).branches).toEqual([]);
    catalog.close();
  }
});

test("wildcard repositories recover their stored format and canonical WAL identity", async () => {
  policies = '{"*":{}}';
  for (const format of ["sha1", "sha256"]) {
    policies = format === "sha1" ? '{"*":{}}' : '{"*":{},"team/project":{"log_id":"","format":""}}';
    for (const name of ["team/project", "team/project.git", "team/project.GIT"]) {
      const canonical = name === "team/project.GIT" ? "team/project.GIT.git" : "team/project.git";
      let authorized = false;
      globalThis.fetch = mock(async (url: RequestInfo | URL) => {
        expect(String(url)).toBe(
          `http://git.spin.internal/${canonical}/info/refs?service=git-upload-pack`,
        );
        authorized = true;
        return discovery();
      }) as unknown as typeof fetch;
      openSession = (config) => {
        expect(authorized).toBe(true);
        expect(config.logId).toBe(
          `auto-${new Bun.CryptoHasher("sha256").update(canonical).digest("hex")}`,
        );
        return emptySession(format);
      };
      expect(
        (await browse(new Request(`https://viewer.test/_viewer/api?repo=${name}`))).status,
      ).toBe(200);
    }
  }
});

test("exact aliases override the whole wildcard policy and enforce pinned formats", async () => {
  globalThis.fetch = mock(async () => discovery()) as unknown as typeof fetch;
  for (const key of ["team/project", "team/project.git"]) {
    policies = JSON.stringify({
      "*": { format: "sha256" },
      [key]: { log_id: "custom", format: "" },
    });
    openSession = (config) => {
      expect(config.logId).toBe("custom");
      return emptySession("sha1");
    };
    for (const name of ["team/project", "team/project.git"])
      expect(
        (await browse(new Request(`https://viewer.test/_viewer/api?repo=${name}`))).status,
      ).toBe(200);
  }
  policies = '{"*":{"format":"sha256"}}';
  openSession = () => emptySession("sha1");
  await expect(
    browse(new Request("https://viewer.test/_viewer/api?repo=team/project")),
  ).rejects.toThrow("Invalid repository catalog");
  policies = '{"*":{}}';
  openSession = () => emptySession("md5");
  await expect(
    browse(new Request("https://viewer.test/_viewer/api?repo=team/project")),
  ).rejects.toThrow("Invalid repository catalog");
});

function fixture(
  format: "sha1" | "sha256",
  raw: Uint8Array,
  encoded: Uint8Array,
  storage: "inline" | "full" | "delta" = "inline",
) {
  const id = new Bun.CryptoHasher(format).update(raw).digest("hex");
  let drops = 0,
    reads = 0;
  const handle = () => ({
    [Symbol.dispose]() {
      drops++;
    },
  });
  const root = handle(),
    leaf = handle(),
    wrapper = handle(),
    full = handle(),
    delta = handle();
  const item = {
    ID: id,
    Kind: 3,
    Size: 3,
    StoredSize: encoded.length,
    Encoding: "zlib",
    ...(storage === "inline"
      ? { Inline: btoa(String.fromCharCode(...encoded)) }
      : storage === "delta"
        ? { Delta: { StoredSize: 12 } }
        : {}),
  };
  const entry = (value: unknown, children: unknown[]) => ({
    data: new TextEncoder().encode(JSON.stringify(value)),
    objects: children,
  });
  let history = true;
  const recovery = {
    next() {
      if (!history) return undefined;
      history = false;
      return { tag: "checkpoint", val: { objects: [root] } };
    },
    readNode(value: unknown) {
      if (value === root)
        return entry(
          {
            Validated: true,
            Format: format,
            Head: "refs/heads/main",
            Refs: {},
            Buckets: [id.slice(0, 2)],
          },
          [leaf],
        );
      if (value === leaf)
        return entry(
          { Items: [item] },
          storage === "inline" ? [] : storage === "full" ? [full] : [wrapper],
        );
      return { data: new Uint8Array(), objects: [full, delta] };
    },
    openBytes(value: unknown) {
      expect(value).toBe(full);
      return {
        length: () => BigInt(encoded.length),
        readAt(offset: bigint, length: number) {
          reads++;
          return encoded.slice(Number(offset), Number(offset) + Math.min(3, length));
        },
        [Symbol.dispose]() {
          drops++;
        },
      };
    },
    [Symbol.dispose]() {
      drops++;
    },
  };
  const catalog = new Catalog({ recover: () => recovery } as unknown as Session, format, {
    bytes: 0,
  });
  return { catalog, id, counts: () => ({ drops, reads }) };
}

for (const format of ["sha1", "sha256"] as const) {
  test(`${format}: inline and chunked full objects with a retained delta`, () => {
    const raw = new TextEncoder().encode("blob 3\0abc"),
      encoded = zlibSync(raw);
    for (const storage of ["inline", "full", "delta"] as const) {
      const sample = fixture(format, raw, encoded, storage);
      try {
        expect(new TextDecoder().decode(sample.catalog.object(sample.id, 3, 256).bytes)).toBe(
          "abc",
        );
      } finally {
        sample.catalog.close();
      }
      expect(sample.counts().drops).toBe(storage === "inline" ? 3 : storage === "full" ? 5 : 7);
      expect(sample.counts().reads).toBe(storage === "inline" ? 0 : Math.ceil(encoded.length / 3));
    }
  });
}

test("inflation rejects excess decoded bytes and truncated input", () => {
  const raw = new TextEncoder().encode("blob 3\0abc");
  for (const encoded of [
    zlibSync(new TextEncoder().encode("blob 3\0abcEXTRA")),
    zlibSync(raw).slice(0, 5),
  ]) {
    const sample = fixture("sha256", raw, encoded);
    try {
      expect(() => sample.catalog.object(sample.id, 3, 256)).toThrow();
    } finally {
      sample.catalog.close();
    }
    expect(sample.counts().drops).toBe(3);
  }
});

test("large preview reads only metadata", () => {
  const raw = new TextEncoder().encode("blob 3\0abc"),
    sample = fixture("sha256", raw, zlibSync(raw), "delta");
  try {
    expect(sample.catalog.object(sample.id, 3, 2)).toEqual({ size: 3 });
  } finally {
    sample.catalog.close();
  }
  expect(sample.counts().reads).toBe(0);
});

test("Git authorization completes before viewer storage is opened", async () => {
  const baseline = opened;
  globalThis.fetch = mock(async (input: RequestInfo | URL, options?: RequestInit) => {
    expect(String(input)).toBe(
      "http://git.spin.internal/team/demo.git/info/refs?service=git-upload-pack",
    );
    expect(options?.headers).toEqual({ Authorization: "Bearer test-token" });
    expect(options?.redirect).toBe("manual");
    return new Response("Denied", { status: 403 });
  }) as unknown as typeof fetch;
  const response = await browse(
    new Request("https://viewer.test/_viewer/api?repo=team/demo.git", {
      headers: { Authorization: "Bearer test-token" },
    }),
  );
  expect(response.status).toBe(403);
  expect(opened).toBe(baseline);
  globalThis.fetch = mock(async () => {
    throw new Error("Unexpected HTTP call");
  }) as unknown as typeof fetch;
  expect(
    (await browse(new Request("https://viewer.test/_viewer/api?repo=missing.git"))).status,
  ).toBe(404);
  expect(
    (await browse(new Request("https://viewer.test/_viewer/api?repo=team/demo.git&path=..")))
      .status,
  ).toBe(400);
  expect(opened).toBe(baseline);
});

test("unusual filenames remain listed without hiding valid siblings", () => {
  const encode = (value: string) => new TextEncoder().encode(value);
  const commitId = "11".repeat(20),
    treeId = "22".repeat(20),
    blobId = "33".repeat(20);
  const names = [
    Uint8Array.of(255),
    encode("�"),
    encode("\\xff"),
    encode("\uFEFFREADME"),
    encode("README"),
  ];
  const tree = new Uint8Array(names.reduce((sum, name) => sum + 8 + name.length + 20, 0));
  let offset = 0;
  for (const [index, name] of names.entries()) {
    tree.set(encode("100644 "), offset);
    offset += 7;
    tree.set(name, offset);
    offset += name.length;
    tree[offset++] = 0;
    tree.fill(0x33 + index, offset, offset + 20);
    offset += 20;
  }
  for (const spacing of [" ", "  ", " \t"]) {
    const catalog = {
      root: { Format: "sha1", Head: "refs/heads/main", Refs: { "refs/heads/main": commitId } },
      object(id: string) {
        const bytes =
          id === commitId
            ? encode(`tree ${treeId}\nauthor A <a@b>${spacing}1700000000 +0000\n\nTitle\n`)
            : id === treeId
              ? tree
              : encode("safe");
        return { size: bytes.length, bytes };
      },
    } as unknown as InstanceType<typeof Catalog>;
    const view = snapshot(catalog, new URLSearchParams());
    expect(view.history[0].title).toBe("Title");
    expect(view.entries).toEqual([
      { name: "\\xff", kind: "file", id: blobId, unavailable: true },
      { name: "�", kind: "file", id: "34".repeat(20) },
      { name: "\\xff", kind: "file", id: "35".repeat(20) },
      { name: "\uFEFFREADME", kind: "file", id: "36".repeat(20) },
      { name: "README", kind: "file", id: "37".repeat(20) },
    ]);
    expect(snapshot(catalog, new URLSearchParams({ path: "�" })).file?.text).toBe("safe");
    expect(snapshot(catalog, new URLSearchParams({ path: "\\xff" })).file?.id).toBe(
      "35".repeat(20),
    );
  }
});

test("expired reads refresh once, close both views, and retain cumulative usage", async () => {
  let calls = 0,
    bytes = 0,
    refreshes = 0;
  const closed: string[] = [];
  const makeSession = (fresh: boolean): Session =>
    ({
      recover() {
        let next = true;
        const root = {
          [Symbol.dispose]() {
            closed.push(`root:${fresh}`);
          },
        };
        return {
          next() {
            if (!next) return undefined;
            next = false;
            return { tag: "checkpoint", val: { objects: [root] } };
          },
          readNode() {
            calls++;
            bytes += 100;
            if (!fresh) throw { payload: { tag: "expired" } };
            return {
              objects: [],
              data: new TextEncoder().encode(
                JSON.stringify({
                  Validated: true,
                  Format: "sha256",
                  Head: "refs/heads/main",
                  Refs: {},
                  Buckets: [],
                }),
              ),
            };
          },
          [Symbol.dispose]() {
            closed.push(`recovery:${fresh}`);
          },
        };
      },
      refresh() {
        refreshes++;
        return makeSession(true);
      },
      usage: () => ({ calls: BigInt(calls), bytes: BigInt(bytes) }),
      [Symbol.dispose]() {
        closed.push(`session:${fresh}`);
      },
    }) as unknown as Session;
  openSession = () => makeSession(false);
  globalThis.fetch = mock(
    async () =>
      new Response("refs", {
        headers: { "Content-Type": "application/x-git-upload-pack-advertisement" },
      }),
  ) as unknown as typeof fetch;
  const response = await browse(new Request("https://viewer.test/_viewer/api?repo=team/demo.git"));
  expect(response.status).toBe(200);
  expect((await response.json()).history).toEqual([]);
  expect(refreshes).toBe(1);
  expect(response.headers.get("X-Wal-Calls")).toBe("2");
  expect(response.headers.get("X-Wal-Bytes")).toBe("200");
  expect(closed.sort()).toEqual([
    "recovery:false",
    "recovery:true",
    "root:false",
    "root:true",
    "session:false",
    "session:true",
  ]);
});
