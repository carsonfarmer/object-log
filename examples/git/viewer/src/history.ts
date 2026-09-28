type PageStore = Pick<Storage, "getItem" | "setItem">;

export class HistoryPages {
  private edges = new Map<string, string>();
  private storage?: PageStore;

  constructor(storage?: PageStore) {
    try {
      this.storage = storage ?? sessionStorage;
      const saved = JSON.parse(this.storage.getItem("object-log.history-pages") ?? "[]");
      if (Array.isArray(saved))
        for (const entry of saved.slice(-256))
          if (
            Array.isArray(entry) &&
            entry.length === 2 &&
            typeof entry[0] === "string" &&
            typeof entry[1] === "string" &&
            /^(?:[0-9a-f]{40}|[0-9a-f]{64})$/.test(entry[1])
          )
            this.edges.set(entry[0], entry[1]);
    } catch {
      // Paging still works when browser storage is unavailable.
    }
  }

  previous(key: string) {
    return this.edges.get(key);
  }

  remember(key: string, previous: string) {
    if (this.edges.get(key) === previous) return;
    this.edges.set(key, previous);
    if (this.edges.size > 256) this.edges.delete(this.edges.keys().next().value as string);
    try {
      this.storage?.setItem("object-log.history-pages", JSON.stringify([...this.edges]));
    } catch {
      // Keep the in-memory trail when storage is disabled or full.
    }
  }
}
