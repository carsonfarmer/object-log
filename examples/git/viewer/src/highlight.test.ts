import { expect, test } from "bun:test";
import { highlight } from "./highlight";

test("filename aliases select the registered grammars; unknown files stay plain", () => {
  for (const path of [
    "a.RS",
    "a.go",
    "a.ts",
    "a.tsx",
    "a.js",
    "a.jsx",
    "a.json",
    "README.md",
    "notes.MD",
    "a.sh",
    "a.yaml",
    "a.toml",
    "a.html",
    "a.css",
  ])
    expect(highlight("", path)).toBe("");
  for (const path of ["README", "notes.txt", "a.constructor", "a.__proto__", "src.rs/README"])
    expect(highlight("plain <notes>", path)).toBeUndefined();
  expect(highlight('pub fn greet() { println!("hello"); }', "src/main.rs")).toContain(
    'class="hljs-keyword"',
  );
  expect(highlight("const value: string = 'hello';", "src/app.ts")).toContain(
    'class="hljs-built_in"',
  );
  expect(highlight("# Heading\n\n**bold** and `code`", "README.md")).toContain(
    'class="hljs-section"',
  );
});

for (const path of ["index.html", "README.md"])
  test(`${path}: highlighting preserves source markup as text`, () => {
    const html = highlight('<section title="notes">a & b 日本語</section>', path) ?? "";
    expect(html).toContain("&lt;");
    expect(html).toContain("&amp;");
    expect(html).not.toContain("<section");
    expect(
      html
        .replace(/<\/?span(?: [^>]*)?>/g, "")
        .replaceAll("&lt;", "<")
        .replaceAll("&gt;", ">")
        .replaceAll("&quot;", '"')
        .replaceAll("&amp;", "&"),
    ).toBe('<section title="notes">a & b 日本語</section>');
  });
