import { browse } from "./api";

declare const CLIENT_SCRIPT: string;
declare const STYLES: string;
declare const PAGE: string;

async function handle(request: Request): Promise<Response> {
  const path = new URL(request.url).pathname;
  if (request.method !== "GET" && request.method !== "HEAD") {
    return new Response("Method not allowed", {
      status: 405,
      headers: { Allow: "GET, HEAD" },
    });
  }
  if (path === "/_viewer/api") return browse(request);
  const assets: Record<string, [string, string]> = {
    "/": [PAGE, "text/html; charset=utf-8"],
    "/_viewer/client.js": [CLIENT_SCRIPT, "text/javascript; charset=utf-8"],
    "/_viewer/style.css": [STYLES, "text/css; charset=utf-8"],
  };
  if (!assets[path]) return new Response("Not found", { status: 404 });
  const [body, type] = assets[path];
  return new Response(request.method === "HEAD" ? null : body, {
    headers: {
      "Content-Type": type,
      "Cache-Control": "no-cache",
      "X-Content-Type-Options": "nosniff",
      "Referrer-Policy": "no-referrer",
      "Content-Security-Policy":
        "default-src 'none'; script-src 'self'; style-src 'self'; img-src data:; connect-src 'self'; base-uri 'none'; frame-ancestors 'none'",
    },
  });
}

addEventListener("fetch", (event: Event) => {
  const request = event as FetchEvent;
  request.respondWith(
    handle(request.request).catch((error) => {
      console.error(error);
      return new Response("Repository unavailable", { status: 503 });
    }),
  );
});
