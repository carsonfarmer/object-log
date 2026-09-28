import { expect, test } from "bun:test";

import { HistoryPages } from "./history";

const first = "a".repeat(40);

test("paging survives reload without retaining unbounded history", () => {
  let saved = "[]";
  const storage = {
    getItem: () => saved,
    setItem: (_: string, value: string) => {
      saved = value;
    },
  };
  const pages = new HistoryPages(storage);
  for (let i = 0; i < 300; i++) pages.remember(`page-${i}`, first);
  const reloaded = new HistoryPages(storage);
  expect(reloaded.previous("page-0")).toBeUndefined();
  expect(reloaded.previous("page-299")).toBe(first);
  expect(JSON.parse(saved)).toHaveLength(256);
});

test("disabled browser storage still permits returning to a previous page", () => {
  const pages = new HistoryPages({
    getItem: () => {
      throw new Error("Storage disabled");
    },
    setItem: () => {
      throw new Error("Storage disabled");
    },
  });
  pages.remember("older", first);
  expect(pages.previous("older")).toBe(first);
});

test("malformed saved navigation is ignored instead of breaking the viewer", () => {
  for (const saved of ["{", '{"older":"not-a-commit"}', '[["older", "not-a-commit"]]']) {
    const pages = new HistoryPages({ getItem: () => saved, setItem: () => {} });
    expect(pages.previous("older")).toBeUndefined();
    pages.remember("older", first);
    expect(pages.previous("older")).toBe(first);
  }
});
