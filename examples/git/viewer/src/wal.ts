import type {
  Config,
  Entry,
  Recovery,
  Session,
  Object as WalObject,
} from "object-log:storage/wal@0.1.0";
import * as wal from "object-log:storage/wal@0.1.0";
import { sha1 } from "@noble/hashes/legacy.js";
import { sha256 } from "@noble/hashes/sha2.js";
import { get } from "@spinframework/spin-variables";
import { unzlibSync } from "fflate";

export const variable = (name: string) => get(name) ?? "";
export const drop = (resource: unknown) => (resource as Disposable)[Symbol.dispose]();
const decode = new TextDecoder("utf-8", { fatal: true });
export const json = <T>(data: Uint8Array): T => JSON.parse(decode.decode(data));
export const hex = (bytes: Uint8Array) =>
  Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");

export interface Repository {
  log_id: string;
  format?: "sha1" | "sha256" | "";
}
export const canonicalName = (name: string) => (name.endsWith(".git") ? name : `${name}.git`);
export const automaticLogId = (name: string) =>
  `auto-${hex(sha256(new TextEncoder().encode(name)))}`;
export class MissingObject extends Error {}
interface Item {
  ID: string;
  Kind: number;
  Size: number;
  StoredSize: number;
  Encoding: string;
  Inline?: string;
  Delta?: { StoredSize: number };
}
interface Bucket {
  Items?: Item[];
  Prefixes?: string[];
}
export interface Root {
  Validated: boolean;
  Format: string;
  Head: string;
  Refs: Record<string, string>;
  Buckets: string[];
}

export function settings(repository: Repository): Config {
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
    logId: repository.log_id,
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
  readonly recovery: Recovery;
  readonly root: Root;
  private owned: WalObject[] = [];
  private buckets = new Map<string, WalObject>();
  private nodes = new Map<WalObject, Entry>();

  constructor(
    session: Session,
    format: string | undefined,
    private budget: { bytes: number },
  ) {
    this.recovery = session.recover();
    try {
      let roots: WalObject[] = [];
      for (let record = this.recovery.next(); record; record = this.recovery.next()) {
        for (const root of roots) drop(root);
        roots = record.val.objects;
        this.owned = roots;
      }
      if (roots.length !== 1) throw new Error("Repository has no published root");
      const node = this.node(roots[0]);
      this.root = json<Root>(node.data);
      if (
        !this.root.Validated ||
        !["sha1", "sha256"].includes(this.root.Format) ||
        typeof this.root.Head !== "string" ||
        !/^refs\/heads\/.+$/.test(this.root.Head) ||
        (format && this.root.Format !== format) ||
        this.root.Buckets.length !== node.objects.length
      )
        throw new Error("Invalid repository catalog");
      this.root.Buckets.forEach((prefix, i) => {
        this.buckets.set(prefix, node.objects[i]);
      });
    } catch (error) {
      this.close();
      throw error;
    }
  }

  private node(root: WalObject): Entry {
    const cached = this.nodes.get(root);
    if (cached) return cached;
    const entry = this.recovery.readNode(root);
    this.owned.push(...entry.objects);
    this.budget.bytes += entry.data.length;
    if (this.budget.bytes > 64 << 20) throw new Error("Catalog read exceeds 64 MiB");
    this.nodes.set(root, entry);
    return entry;
  }

  object(id: string, kind: number, maxBytes: number): { size: number; bytes?: Uint8Array } {
    const hashBytes = this.root.Format === "sha256" ? 32 : 20;
    if (!new RegExp(`^[0-9a-f]{${hashBytes * 2}}$`).test(id))
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
      const items = bucket.Items ?? [];
      let child = 0;
      for (const item of items) {
        const value = item.Inline ? undefined : node.objects[child++];
        if (item.ID !== id) continue;
        if (item.Kind !== kind) throw new MissingObject("Git object has a different kind");
        if (item.Encoding !== "zlib" || !Number.isSafeInteger(item.Size) || item.Size < 0)
          throw new Error("Invalid Git object metadata");
        if (item.Size > maxBytes) {
          if (kind !== 3) throw new Error("Git object exceeds metadata limit");
          return { size: item.Size };
        }
        let compressed: Uint8Array;
        if (item.Inline)
          compressed = Uint8Array.from(atob(item.Inline), (char) => char.charCodeAt(0));
        else {
          if (!value) throw new Error("Missing object reference");
          const full = item.Delta?.StoredSize ? this.node(value).objects[0] : value;
          const reader = this.recovery.openBytes(full);
          try {
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
          } finally {
            drop(reader);
          }
        }
        if (compressed.length !== item.StoredSize || (item.Inline && compressed.length > 512))
          throw new Error("Invalid stored object length");
        const names: Record<number, string> = { 1: "commit", 2: "tree", 3: "blob" };
        const header = new TextEncoder().encode(`${names[kind]} ${item.Size}\0`);
        const expected = header.length + item.Size;
        const output = new Uint8Array(expected + 1);
        const content = unzlibSync(compressed, { out: output });
        const hash = this.root.Format === "sha256" ? sha256(content) : sha1(content);
        if (
          content.length !== expected ||
          hex(hash) !== id ||
          !header.every((byte, i) => content[i] === byte)
        )
          throw new Error("Git object differs from its catalog");
        return { size: item.Size, bytes: content.subarray(header.length) };
      }
      break;
    }
    throw new MissingObject("Git object not found");
  }

  close() {
    for (const resource of this.owned.reverse()) drop(resource);
    drop(this.recovery);
  }
}

export const openExisting = wal.openExisting;
