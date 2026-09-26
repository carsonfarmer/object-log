export {};

declare const CLIENT_SCRIPT: string;
declare const STYLES: string;
declare const PAGE: string;

function handle(request: Request): Response {
  if (request.method !== "GET" && request.method !== "HEAD") {
    return new Response("Method not allowed", {
      status: 405,
      headers: { Allow: "GET, HEAD" },
    });
  }
  const path = new URL(request.url).pathname;
  if (
    path !== "/browse" &&
    path !== "/browse/assets/client.js" &&
    path !== "/browse/assets/style.css"
  ) {
    return new Response("Not found", { status: 404 });
  }
  const assets: Record<string, [string, string]> = {
    "/browse/assets/client.js": [
      CLIENT_SCRIPT,
      "text/javascript; charset=utf-8",
    ],
    "/browse/assets/style.css": [STYLES, "text/css; charset=utf-8"],
  };
  const [body, type] = assets[path] ?? [PAGE, "text/html; charset=utf-8"];
  return new Response(request.method === "HEAD" ? null : body, {
    headers: {
      "Content-Type": type,
      "Cache-Control": "no-cache",
      "X-Content-Type-Options": "nosniff",
      "Referrer-Policy": "no-referrer",
      "Content-Security-Policy":
        "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; base-uri 'none'; frame-ancestors 'none'",
    },
  });
}

addEventListener("fetch", (event: Event) => {
  const request = event as FetchEvent;
  request.respondWith(handle(request.request));
});
