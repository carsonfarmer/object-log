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
const { Catalog, MissingObject, drop } = await import("./wal");
const { browse } = await import("./api");
const { NotFound, snapshot } = await import("./repository");
const originalFetch = globalThis.fetch;
afterEach(() => {
  globalThis.fetch = originalFetch;
  policies = defaultPolicies;
  openSession = () => {
    throw new Error("Unexpected storage access");
  };
});

function emptySession(
  format: string,
  overrides: Record<string, unknown> = {},
  children = 0,
): Session {
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
        objects: Array.from({ length: children }, () => ({ [Symbol.dispose]: dispose })),
      }),
      [Symbol.dispose]: dispose,
    }),
    usage: () => ({ calls: 0n, bytes: 0n }),
    [Symbol.dispose]: dispose,
  } as unknown as Session;
}

const readAccess = () => new Response(null, { status: 204 });

test("catalog requires a persisted branch HEAD and accepts an unborn branch", async () => {
  for (const format of ["sha1", "sha256"]) {
    for (const Head of [undefined, "", "refs/tags/main", "refs/heads/"])
      expect(() => new Catalog(emptySession(format, { Head }), "", { bytes: 0 })).toThrow(
        "Invalid repository catalog",
      );
    const catalog = new Catalog(emptySession(format), "", { bytes: 0 });
    expect((await snapshot(catalog, new URLSearchParams())).branches).toEqual([]);
    drop(catalog);
  }
});

test("catalog validates root fields, references and sorted bucket prefixes", async () => {
  for (const format of ["sha1", "sha256"]) {
    const id = "11".repeat(format === "sha1" ? 20 : 32);
    const invalid: [Record<string, unknown>, number][] = [
      [{ Validated: 1 }, 0],
      [{ Extra: true }, 0],
      [{ Head: "refs/heads/a..b" }, 0],
      [{ Head: "refs/heads/\u200c./nested" }, 0],
      [{ Refs: [] }, 0],
      [{ Refs: { "refs/heads/main.lock": id } }, 0],
      [{ Refs: { "refs/heads/main": 1 } }, 0],
      [{ Refs: { "refs/heads/main": id.slice(2) } }, 0],
      [{ Refs: { "refs/heads/main": "00".repeat(id.length / 2) } }, 0],
      [{ Refs: { "refs/heads/main": "AA".repeat(id.length / 2) } }, 0],
      [{ Refs: { "refs/heads/a": id, "refs/heads/a/b": id } }, 0],
      [{ Buckets: {} }, 0],
      [{ Buckets: ["0G"] }, 1],
      [{ Buckets: ["aa", "aa"] }, 2],
      [{ Buckets: ["bb", "aa"] }, 2],
    ];
    for (const [fields, children] of invalid)
      expect(() => new Catalog(emptySession(format, fields, children), "", { bytes: 0 })).toThrow(
        "Invalid repository catalog",
      );
    for (const Head of ["refs/heads/@", "refs/heads/feature/日本語", "refs/heads/a\u200cb"])
      drop(
        new Catalog(
          emptySession(format, { Head, Refs: { [Head]: id }, Buckets: ["00", "ff"] }, 2),
          "",
          { bytes: 0 },
        ),
      );
    const empty = new Catalog(emptySession(format, { Refs: null, Buckets: null }), "", {
      bytes: 0,
    });
    expect((await snapshot(empty, new URLSearchParams())).branches).toEqual([]);
    drop(empty);
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
        expect(String(url)).toBe(`http://git.spin.internal/${canonical}/authorize-read`);
        authorized = true;
        return readAccess();
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
  globalThis.fetch = mock(async () => readAccess()) as unknown as typeof fetch;
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
  storage: "inline" | "inline-delta" | "full" | "delta" = "inline",
  size = 3,
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
    Size: size,
    StoredSize: encoded.length,
    Encoding: "zlib",
    ...(storage.startsWith("inline")
      ? {
          Inline: btoa(String.fromCharCode(...encoded)),
          ...(storage === "inline-delta" ? { Delta: { Data: "AA==" } } : {}),
        }
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
          storage.startsWith("inline") ? [] : storage === "full" ? [full] : [wrapper],
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
  test(`${format}: object digest and Git header are checked independently`, async () => {
    const encode = (value: string) => new TextEncoder().encode(value);
    for (const [expected, stored] of [
      ["blob 3\0abc", "blob 3\0abd"],
      ["tree 3\0abc", "tree 3\0abc"],
    ]) {
      const sample = fixture(format, encode(expected), zlibSync(encode(stored)));
      try {
        await expect(sample.catalog.object(sample.id, 3, 256)).rejects.toThrow(
          "Git object differs from its catalog",
        );
      } finally {
        drop(sample.catalog);
      }
      expect(sample.counts()).toEqual({ reads: 0, drops: 3 });
    }
  });

  test(`${format}: inline and chunked full objects with a retained delta`, async () => {
    const raw = new TextEncoder().encode("blob 3\0abc"),
      encoded = zlibSync(raw);
    for (const storage of ["inline", "inline-delta", "full", "delta"] as const) {
      const sample = fixture(format, raw, encoded, storage);
      try {
        expect(
          new TextDecoder().decode((await sample.catalog.object(sample.id, 3, 256)).bytes),
        ).toBe("abc");
      } finally {
        drop(sample.catalog);
      }
      expect(sample.counts().drops).toBe(
        storage.startsWith("inline") ? 3 : storage === "full" ? 5 : 7,
      );
      expect(sample.counts().reads).toBe(
        storage.startsWith("inline") ? 0 : Math.ceil(encoded.length / 3),
      );
    }
  });

  test(`${format}: inline compressed length boundary`, async () => {
    for (const compressedSize of [2048, 2049]) {
      const size = compressedSize - 21,
        header = new TextEncoder().encode(`blob ${size}\0`),
        raw = new Uint8Array(header.length + size);
      raw.set(header);
      const encoded = zlibSync(raw, { level: 0 }),
        sample = fixture(format, raw, encoded, "inline-delta", size);
      expect(encoded.length).toBe(compressedSize);
      try {
        if (compressedSize === 2048)
          expect((await sample.catalog.object(sample.id, 3, 4096)).bytes).toEqual(
            raw.subarray(header.length),
          );
        else
          await expect(sample.catalog.object(sample.id, 3, 4096)).rejects.toThrow(
            "Invalid stored object length",
          );
      } finally {
        drop(sample.catalog);
      }
      expect(sample.counts()).toEqual({ reads: 0, drops: 3 });
    }
  });
}

test("inflation rejects excess decoded bytes and truncated input", async () => {
  const raw = new TextEncoder().encode("blob 3\0abc");
  for (const encoded of [
    zlibSync(new TextEncoder().encode("blob 3\0abcEXTRA")),
    zlibSync(raw).slice(0, 5),
  ]) {
    const sample = fixture("sha256", raw, encoded);
    try {
      await expect(sample.catalog.object(sample.id, 3, 256)).rejects.toThrow();
    } finally {
      drop(sample.catalog);
    }
    expect(sample.counts().drops).toBe(3);
  }
});

test("large preview reads only metadata", async () => {
  const raw = new TextEncoder().encode("blob 3\0abc"),
    sample = fixture("sha256", raw, zlibSync(raw), "delta");
  try {
    expect(await sample.catalog.object(sample.id, 3, 2)).toEqual({ size: 3 });
  } finally {
    drop(sample.catalog);
  }
  expect(sample.counts().reads).toBe(0);
});

test("a catalogued blob cannot be selected as a commit", async () => {
  const raw = new TextEncoder().encode("blob 3\0abc"),
    sample = fixture("sha256", raw, zlibSync(raw));
  sample.catalog.root.Refs["refs/heads/main"] = "22".repeat(32);
  try {
    await expect(
      snapshot(sample.catalog, new URLSearchParams({ commit: sample.id })),
    ).rejects.toThrow(NotFound);
  } finally {
    drop(sample.catalog);
  }
  expect(sample.counts()).toEqual({ reads: 0, drops: 3 });
});

test("Git authorization completes before viewer storage is opened", async () => {
  const baseline = opened;
  globalThis.fetch = mock(async (input: RequestInfo | URL, options?: RequestInit) => {
    expect(String(input)).toBe("http://git.spin.internal/team/demo.git/authorize-read");
    expect(options?.headers).toEqual({
      Authorization: "Bearer test-token",
    });
    expect(options?.redirect).toBe("manual");
    return new Response("Denied", { status: 403 });
  }) as unknown as typeof fetch;
  const response = await browse(
    new Request(`https://viewer.test/_viewer/api?repo=team/demo.git&commit=${"11".repeat(32)}`, {
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
  for (const view of ["", "unknown", "commits&view=code"])
    expect(
      (await browse(new Request(`https://viewer.test/_viewer/api?repo=team/demo.git&view=${view}`)))
        .status,
    ).toBe(400);
  expect(opened).toBe(baseline);
});

test("access-check errors cancel their bodies without opening storage", async () => {
  const baseline = opened;
  for (const status of [200, 302, 401, 403, 404, 503]) {
    let cancelled = false;
    const body = new ReadableStream<Uint8Array>({
      cancel() {
        cancelled = true;
      },
    });
    globalThis.fetch = mock(async () => new Response(body, { status })) as unknown as typeof fetch;
    const response = await browse(
      new Request("https://viewer.test/_viewer/api?repo=team/demo.git"),
    );
    expect(response.status).toBe([200, 302].includes(status) ? 502 : status);
    expect(cancelled).toBe(true);
    expect(body.locked).toBe(false);
  }
  expect(opened).toBe(baseline);
});

test("authorized missing repositories return 404, while invalid durable state remains an error", async () => {
  globalThis.fetch = mock(async () => readAccess()) as unknown as typeof fetch;
  const request = new Request("https://viewer.test/_viewer/api?repo=team/demo.git");
  openSession = () => {
    throw { payload: { tag: "missing" } };
  };
  expect((await browse(request)).status).toBe(404);
  for (const count of [0, 2]) {
    let dropped = 0;
    const dispose = () => dropped++;
    openSession = () =>
      ({
        recover: () => {
          let next = true;
          return {
            next: () => {
              if (!next) return undefined;
              next = false;
              return {
                tag: "checkpoint",
                val: {
                  objects: Array.from({ length: count }, () => ({ [Symbol.dispose]: dispose })),
                },
              };
            },
            [Symbol.dispose]: dispose,
          };
        },
        [Symbol.dispose]: dispose,
      }) as unknown as Session;
    if (count === 0) expect((await browse(request)).status).toBe(404);
    else await expect(browse(request)).rejects.toThrow("Invalid repository root count");
    expect(dropped).toBe(count + 2);
  }
  const failure = { payload: { tag: "other" } };
  openSession = () => {
    throw failure;
  };
  await expect(browse(request)).rejects.toBe(failure);
});

test("commit queries reject malformed selectors before authorization and return 404 for missing IDs", async () => {
  const baseline = opened;
  globalThis.fetch = mock(async () => {
    throw new Error("Unexpected authorization");
  }) as unknown as typeof fetch;
  for (const selector of [
    "",
    "1234567",
    "AA".repeat(20),
    "11".repeat(21),
    `${"11".repeat(20)}&commit=${"22".repeat(20)}`,
  ])
    expect(
      (
        await browse(
          new Request(`https://viewer.test/_viewer/api?repo=team/demo.git&commit=${selector}`),
        )
      ).status,
    ).toBe(400);
  expect(opened).toBe(baseline);
  globalThis.fetch = mock(async () => readAccess()) as unknown as typeof fetch;
  openSession = () => emptySession("sha256", { Refs: { "refs/heads/main": "22".repeat(32) } });
  for (const selector of ["11".repeat(32), "11".repeat(20)])
    expect(
      (
        await browse(
          new Request(`https://viewer.test/_viewer/api?repo=team/demo.git&commit=${selector}`),
        )
      ).status,
    ).toBe(404);
});

for (const format of ["sha1", "sha256"] as const) {
  test(`${format}: Code reads the selected path and Commits reads only its history page`, async () => {
    const width = format === "sha1" ? 20 : 32,
      id = (n: number) => n.toString(16).padStart(width * 2, "0"),
      encode = (value: string) => new TextEncoder().encode(value);
    const objects = new Map<string, { kind: number; bytes: Uint8Array }>();
    for (let i = 1; i <= 100; i++)
      objects.set(id(i), {
        kind: 1,
        bytes: encode(
          `tree ${id(101)}\n${i > 1 ? `parent ${id(i - 1)}\n` : ""}author A <a@b> 1700000000 +0000\n\nCommit ${i}\n`,
        ),
      });
    const tree = (mode: string, name: string, target: number) => {
      const header = encode(`${mode} ${name}\0`),
        bytes = new Uint8Array(header.length + width);
      bytes.set(header);
      bytes[bytes.length - 1] = target;
      return bytes;
    };
    objects.set(id(101), { kind: 2, bytes: tree("40000", "src", 102) });
    objects.set(id(102), { kind: 2, bytes: tree("100644", "README", 103) });
    objects.set(id(103), { kind: 3, bytes: encode("selected content") });
    const reads: string[] = [];
    const catalog = {
      root: { Format: format, Head: "refs/heads/main", Refs: { "refs/heads/main": id(100) } },
      object(key: string, kind: number) {
        reads.push(key);
        const object = objects.get(key);
        if (!object || object.kind !== kind) throw new MissingObject();
        return { size: object.bytes.length, bytes: object.bytes };
      },
    } as unknown as InstanceType<typeof Catalog>;
    const view = await snapshot(
      catalog,
      new URLSearchParams({ commit: id(25), path: "src/README" }),
    );
    expect(view.branch).toBe("refs/heads/main");
    expect(view.history.map((commit) => commit.title)).toEqual(["Commit 25"]);
    expect(view.file?.text).toBe("selected content");
    expect(reads).toEqual([id(25), id(101), id(102), id(103)]);
    reads.length = 0;
    const page = await snapshot(
      catalog,
      new URLSearchParams({ commit: id(25), path: "src/README", view: "commits" }),
    );
    expect(page.history.map((commit) => commit.id)).toEqual(
      Array.from({ length: 20 }, (_, i) => id(25 - i)),
    );
    expect(page.file).toBeUndefined();
    expect(page.entries).toEqual([]);
    expect(page.next).toBe(id(5));
    expect(reads).toEqual(page.history.map((commit) => commit.id));
    reads.length = 0;
    const older = await snapshot(
      catalog,
      new URLSearchParams({ commit: page.next, view: "commits" }),
    );
    expect(older.history.map((commit) => commit.id)).toEqual([5, 4, 3, 2, 1].map(id));
    expect(older.next).toBe("");
    expect(reads).toEqual(older.history.map((commit) => commit.id));
    reads.length = 0;
    expect(
      (await snapshot(catalog, new URLSearchParams({ commit: id(2), path: "src" }))).entries[0]
        ?.name,
    ).toBe("README");
    expect(reads).toEqual([id(2), id(101), id(102)]);
    for (const commit of [id(104), id(103)])
      await expect(snapshot(catalog, new URLSearchParams({ commit }))).rejects.toThrow(NotFound);
    await expect(
      snapshot(catalog, new URLSearchParams({ ref: "refs/heads/missing", commit: id(25) })),
    ).rejects.toThrow(NotFound);
    const starts: string[] = [];
    let start = id(100);
    while (start) {
      starts.push(start);
      reads.length = 0;
      const page = await snapshot(catalog, new URLSearchParams({ commit: start, view: "commits" }));
      expect(reads).toEqual(page.history.map((commit) => commit.id));
      expect(reads).toHaveLength(20);
      start = page.next;
    }
    expect(starts).toEqual([100, 80, 60, 40, 20].map(id));
    for (const start of [...starts].reverse()) {
      reads.length = 0;
      const page = await snapshot(catalog, new URLSearchParams({ commit: start, view: "commits" }));
      expect(reads).toEqual(page.history.map((commit) => commit.id));
      expect(reads).toHaveLength(20);
      expect(page.history[0].id).toBe(start);
    }
  });
}

test("unusual filenames remain listed without hiding valid siblings", async () => {
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
    const view = await snapshot(catalog, new URLSearchParams());
    expect(view.history[0].title).toBe("Title");
    expect(view.entries).toEqual([
      { name: "\\xff", kind: "file", id: blobId, unavailable: true },
      { name: "�", kind: "file", id: "34".repeat(20) },
      { name: "\\xff", kind: "file", id: "35".repeat(20) },
      { name: "\uFEFFREADME", kind: "file", id: "36".repeat(20) },
      { name: "README", kind: "file", id: "37".repeat(20) },
    ]);
    expect((await snapshot(catalog, new URLSearchParams({ path: "�" }))).file?.text).toBe("safe");
    expect((await snapshot(catalog, new URLSearchParams({ path: "\\xff" }))).file?.id).toBe(
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
  globalThis.fetch = mock(async () => readAccess()) as unknown as typeof fetch;
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
