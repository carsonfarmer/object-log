import hljs from "highlight.js/lib/core";
import bash from "highlight.js/lib/languages/bash";
import css from "highlight.js/lib/languages/css";
import go from "highlight.js/lib/languages/go";
import ini from "highlight.js/lib/languages/ini";
import javascript from "highlight.js/lib/languages/javascript";
import json from "highlight.js/lib/languages/json";
import markdown from "highlight.js/lib/languages/markdown";
import rust from "highlight.js/lib/languages/rust";
import typescript from "highlight.js/lib/languages/typescript";
import xml from "highlight.js/lib/languages/xml";
import yaml from "highlight.js/lib/languages/yaml";

for (const [name, grammar] of Object.entries({
  bash,
  css,
  go,
  ini,
  javascript,
  json,
  markdown,
  rust,
  typescript,
  xml,
  yaml,
}))
  hljs.registerLanguage(name, grammar);

export function highlight(text: string, path: string) {
  const language = path.split("/").at(-1)?.split(".").at(-1) ?? "";
  return hljs.getLanguage(language) ? hljs.highlight(text, { language }).value : undefined;
}
