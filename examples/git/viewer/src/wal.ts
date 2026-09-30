import * as wal from "object-log:storage/wal@0.1.0";
import { get } from "@spinframework/spin-variables";
import { unzlibSync } from "fflate";

export const variable = (name: string) => get(name) ?? "";
export const disposable = <T>(resource: T) => resource as T & Disposable;
export const drop = (resource: unknown) => disposable(resource)[Symbol.dispose]();
const decode = new TextDecoder("utf-8", { fatal: true });
export const json = <T>(data: Uint8Array): T => JSON.parse(decode.decode(data));
export const hex = (bytes: Uint8Array) => bytes.toHex();

export const automaticLogId = async (name: string) =>
  `auto-${hex(new Uint8Array(await crypto.subtle.digest("SHA-256", new TextEncoder().encode(name))))}`;
export class MissingObject extends Error {}
export class NotFound extends Error {}
interface Bucket {
  Items?: {
    ID: string;
    Kind: number;
    Size: number;
    StoredSize: number;
    Encoding: string;
    Inline?: string;
    Delta?: { StoredSize: number };
  }[];
  Prefixes?: string[];
}
export interface Root {
  Validated: boolean;
  Format: string;
  Head: string;
  Refs: Record<string, string>;
  Buckets: string[];
}

function validRef(name: string): boolean {
  return (
    typeof name === "string" &&
    name.startsWith("refs/") &&
    // biome-ignore lint/suspicious/noControlCharactersInRegex: Git refnames forbid ASCII controls.
    !/[\u0000-\u0020\u007f~^:?*[\\]|\.\.|@\{|\.$/.test(name) &&
    name
      .split("/")
      .every(
        (part) =>
          part &&
          !part.startsWith(".") &&
          !part.endsWith(".lock") &&
          !/^\.\.?$/.test(part.replace(/[\u200c-\u200f\u202a-\u202e\u206a-\u206f\ufeff]/g, "")),
      )
  );
}

export function settings(logId: string): wal.Config {
  return {
    endpoint: variable("wal_endpoint"),
    bucket: variable("wal_bucket"),
    region: variable("wal_region"),
    credentialMode:
      variable("wal_credential_mode") === "instance-role" ? "instance-role" : "static-credentials",
    accessKey: variable("wal_access_key"),
    secretKey: variable("wal_secret_key"),
    sessionToken: variable("wal_session_token") || undefined,
    prefix: variable("wal_prefix"),
    logId,
    logLimits: {
      maxTailEntries: 1024n,
      resolutionWindow: 1024n,
      maxInlineOperationBytes: 65536n,
      maxInlineResultBytes: 4096n,
      maxObjectRefs: 1024n,
      maxObjectBytes: 2097152n,
      maxCommitBytes: 1048576n,
      maxHeadBytes: 262144n,
      maxCheckpointBytes: 16777216n,
      maxRetentionIds: 1024n,
      maxCollectionObjects: BigInt(variable("wal_max_collection_objects")),
      maxCollectionPlanBytes: 16777216n,
    },
    transportLimits: { maxCalls: 25984n, maxBytes: 26180845568n },
  };
}

// Own one exact recovered view; no cache or local repository becomes authority.
export class Catalog {
  readonly recovery: wal.Recovery;
  readonly root: Root;
  private owned: wal.Object[] = [];
  private readonly buckets: Map<string, wal.Object>;
  private nodes = new Map<wal.Object, wal.Entry>();

  constructor(
    session: wal.Session,
    private budget: { bytes: number },
  ) {
    this.recovery = session.recover();
    try {
      for (let record = this.recovery.next(); record; record = this.recovery.next()) {
        for (const root of this.owned) drop(root);
        this.owned = record.val.objects;
      }
      if (!this.owned.length) throw new NotFound("Repository has no published root");
      if (this.owned.length !== 1) throw new Error("Invalid repository root count");
      const node = this.node(this.owned[0]);
      this.root = json<Root>(node.data);
      this.root.Refs ??= {};
      this.root.Buckets ??= [];
      const idPattern = new RegExp(`^[0-9a-f]{${this.root.Format === "sha1" ? 40 : 64}}$`);
      if (
        Object.keys(this.root).some(
          (key) => !["Validated", "Format", "Head", "Refs", "Buckets"].includes(key),
        ) ||
        this.root.Validated !== true ||
        !["sha1", "sha256"].includes(this.root.Format) ||
        !validRef(this.root.Head) ||
        !this.root.Head.startsWith("refs/heads/") ||
        typeof this.root.Refs !== "object" ||
        Array.isArray(this.root.Refs) ||
        Object.entries(this.root.Refs).some(
          ([name, id]) =>
            !validRef(name) ||
            typeof id !== "string" ||
            !idPattern.test(id) ||
            /^0+$/.test(id) ||
            name
              .split("/")
              .some((_, i, parts) => Object.hasOwn(this.root.Refs, parts.slice(0, i).join("/"))),
        ) ||
        !Array.isArray(this.root.Buckets) ||
        this.root.Buckets.length !== node.objects.length ||
        this.root.Buckets.some(
          (prefix, i, all) =>
            typeof prefix !== "string" ||
            !/^[0-9a-f]{2}$/.test(prefix) ||
            (i > 0 && all[i - 1] >= prefix),
        )
      )
        throw new Error("Invalid repository catalog");
      this.buckets = new Map(this.root.Buckets.map((prefix, i) => [prefix, node.objects[i]]));
    } catch (error) {
      this[Symbol.dispose]();
      throw error;
    }
  }

  private node(root: wal.Object): wal.Entry {
    const cached = this.nodes.get(root);
    if (cached) return cached;
    const entry = this.recovery.readNode(root);
    this.owned.push(...entry.objects);
    this.budget.bytes += entry.data.length;
    if (this.budget.bytes > 64 << 20) throw new Error("Catalog read exceeds 64 MiB");
    this.nodes.set(root, entry);
    return entry;
  }

  async object(
    id: string,
    kind: number,
    maxBytes: number,
  ): Promise<{ size: number; bytes?: Uint8Array }> {
    const hash = this.root.Format === "sha256" ? "SHA-256" : "SHA-1";
    if (!new RegExp(`^[0-9a-f]{${hash === "SHA-256" ? 64 : 40}}$`).test(id))
      throw new Error("Invalid Git object ID");
    let prefix = id.slice(0, 2),
      root = this.buckets.get(prefix);
    while (root) {
      const node = this.node(root),
        bucket = json<Bucket>(node.data);
      if (bucket.Prefixes?.length) {
        prefix = id.slice(0, prefix.length + 1);
        if (prefix.length >= id.length || bucket.Prefixes.length !== node.objects.length) break;
        root = node.objects[bucket.Prefixes.indexOf(prefix)];
        continue;
      }
      let child = 0;
      for (const item of bucket.Items ?? []) {
        const value = item.Inline ? undefined : node.objects[child++];
        if (item.ID !== id) continue;
        if (item.Kind !== kind) throw new MissingObject("Git object has a different kind");
        if (item.Encoding !== "zlib" || !Number.isSafeInteger(item.Size) || item.Size < 0)
          throw new Error("Invalid Git object metadata");
        if (item.Size > maxBytes) {
          if (kind !== 3) throw new Error("Git object exceeds metadata limit");
          return { size: item.Size };
        }
        let compressed: Uint8Array<ArrayBuffer>;
        if (item.Inline) compressed = Uint8Array.fromBase64(item.Inline);
        else {
          if (!value) throw new Error("Missing object reference");
          const full = item.Delta?.StoredSize ? this.node(value).objects[0] : value;
          using reader = disposable(this.recovery.openBytes(full));
          if (reader.length() !== BigInt(item.StoredSize) || item.StoredSize > maxBytes + 65536)
            throw new Error("Invalid stored object length");
          compressed = new Uint8Array(item.StoredSize);
          for (let offset = 0; offset < compressed.length; ) {
            const chunk = reader.readAt(BigInt(offset), compressed.length - offset);
            if (!chunk.length || chunk.length > compressed.length - offset)
              throw new Error("Invalid object read length");
            compressed.set(chunk, offset);
            offset += chunk.length;
          }
        }
        if (compressed.length !== item.StoredSize || (item.Inline && compressed.length > 2048))
          throw new Error("Invalid stored object length");
        const names: Record<number, string> = { 1: "commit", 2: "tree", 3: "blob" };
        const header = new TextEncoder().encode(`${names[kind]} ${item.Size}\0`);
        const expected = header.length + item.Size;
        const content = unzlibSync(compressed, { out: new Uint8Array(expected + 1) });
        if (
          content.length !== expected ||
          hex(new Uint8Array(await crypto.subtle.digest(hash, content))) !== id ||
          !header.every((byte, i) => content[i] === byte)
        )
          throw new Error("Git object differs from its catalog");
        return { size: item.Size, bytes: content.subarray(header.length) };
      }
      break;
    }
    throw new MissingObject("Git object not found");
  }

  [Symbol.dispose]() {
    for (const resource of this.owned.reverse()) drop(resource);
    drop(this.recovery);
  }
}

export const openExisting = wal.openExisting;
