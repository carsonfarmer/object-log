const client = await Bun.build({
  entrypoints: ["src/client.tsx"],
  target: "browser",
  minify: true,
});
if (!client.success) throw new AggregateError(client.logs, "Browser build failed");
const component = await Bun.build({
  entrypoints: ["src/index.ts"],
  target: "browser",
  outdir: "build",
  naming: "component.js",
  define: {
    CLIENT_SCRIPT: JSON.stringify(await client.outputs[0].text()),
    STYLES: JSON.stringify(await Bun.file("src/style.css").text()),
    PAGE: JSON.stringify(await Bun.file("src/page.html").text()),
  },
});
if (!component.success) throw new AggregateError(component.logs, "Spin bundle failed");
const routes = ["/browse", "/browse/assets/client.js", "/browse/assets/style.css"];
await Bun.write(
  "../spin.viewer.toml",
  (await Bun.file("../spin.toml").text()) +
    routes
      .map((route) => `\n[[trigger.http]]\nroute = "${route}"\ncomponent = "viewer"\n`)
      .join("") +
    '\n[component.viewer]\nsource = "viewer/dist/viewer.wasm"\nallowed_outbound_hosts = []\n',
);

export {};
