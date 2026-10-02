const client = await Bun.build({
  entrypoints: ["src/client.tsx"],
  target: "browser",
  minify: true,
});
if (!client.success) throw new AggregateError(client.logs, "Browser build failed");
const component = await Bun.build({
  entrypoints: ["src/index.ts"],
  target: "browser",
  external: ["object-log:*", "fermyon:*"],
  outdir: "build",
  naming: "component.js",
  define: {
    CLIENT_SCRIPT: JSON.stringify(await client.outputs[0].text()),
    STYLES: JSON.stringify(
      (await Bun.file("node_modules/highlight.js/styles/github.css").text()) +
        (await Bun.file("src/style.css").text()),
    ),
    PAGE: JSON.stringify(await Bun.file("src/page.html").text()),
  },
});
if (!component.success) throw new AggregateError(component.logs, "Spin bundle failed");
const manifest = await Bun.file("../spin.toml").text();
const variables = [...manifest.matchAll(/^(wal_\w+) = "\{\{ (\w+) \}\}"$/gm)]
  .map((match) => `${match[1]} = "{{ ${match[2]} }}"\n`)
  .join("");
const routes = ["/", "/_viewer/api", "/_viewer/client.js", "/_viewer/style.css"];
await Bun.write(
  "../spin.viewer.toml",
  manifest +
    routes
      .map((route) => `\n[[trigger.http]]\nroute = "${route}"\ncomponent = "viewer"\n`)
      .join("") +
    '\n[component.viewer]\nsource = "viewer/dist/viewer.wasm"\nallowed_outbound_hosts = ["http://git.spin.internal", "{{ wal_endpoint }}", "http://169.254.169.254"]\n[component.viewer.variables]\n' +
    variables,
);

export {};
