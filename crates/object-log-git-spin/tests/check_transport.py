"""Local HTTP transport fixture; not S3/MinIO qualification."""
import concurrent.futures
import os
import hashlib
import hmac
import http.server
import json
import pathlib
import subprocess
import threading
import time
import urllib.error
import urllib.request
import urllib.parse

ROOT = pathlib.Path(__file__).resolve().parent
calls = []
exists = False
held = threading.Event()
release = threading.Event()

class Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_args):
        pass

    def process(self):
        global exists
        if self.headers.get("Transfer-Encoding") == "chunked":
            parts = []
            while True:
                size = int(self.rfile.readline().strip(), 16)
                if size == 0:
                    assert self.rfile.readline() == b"\r\n"
                    break
                parts.append(self.rfile.read(size))
                assert self.rfile.read(2) == b"\r\n"
            body = b"".join(parts)
        else:
            body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
        authorization = self.headers["Authorization"]
        assert authorization.startswith("AWS4-HMAC-SHA256 ")
        fields = dict(part.split("=", 1) for part in authorization[17:].split(", "))
        access, scope = fields["Credential"].split("/", 1)
        assert access == "probe-access"
        signed = fields["SignedHeaders"]
        canonical_headers = "".join(name + ":" + " ".join(self.headers[name].split()) + "\n" for name in signed.split(";"))
        payload_hash = hashlib.sha256(body).hexdigest()
        assert self.headers["x-amz-content-sha256"] == payload_hash
        url = urllib.parse.urlsplit(self.path)
        query = "&".join(urllib.parse.quote(k, safe="-_.~") + "=" + urllib.parse.quote(v, safe="-_.~") for k, v in sorted(urllib.parse.parse_qsl(url.query, keep_blank_values=True)))
        canonical = "\n".join([self.command, url.path, query, canonical_headers, signed, payload_hash])
        string_to_sign = "\n".join(["AWS4-HMAC-SHA256", self.headers["x-amz-date"], scope, hashlib.sha256(canonical.encode()).hexdigest()])
        key = b"AWS4probe-secret"
        for value in scope.split("/"):
            key = hmac.new(key, value.encode(), hashlib.sha256).digest()
        assert hmac.new(key, string_to_sign.encode(), hashlib.sha256).hexdigest() == fields["Signature"]
        if self.path == "/probe/held":
            calls.append((self.command, "held"))
            held.set()
            assert release.wait(10), "held request timed out"
            self.send_response(200)
            self.send_header("Content-Length", "0")
            self.send_header("Last-Modified", "Fri, 04 Sep 2026 00:00:00 GMT")
            self.send_header("ETag", '"fixture"')
            self.end_headers()
            return
        if self.path == "/probe/failure":
            calls.append((self.command, "503"))
            self.send_response(503)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        if url.path == "/probe" and self.command == "POST":
            assert url.query == "delete" and b"probe-object" in body
            exists = False
            output = b'<DeleteResult><Deleted><Key>probe-object</Key></Deleted></DeleteResult>'
            status = 200
            operation = "delete"
        elif url.path == "/probe":
            assert self.command == "GET" and "list-type=2" in url.query
            output = b'<ListBucketResult><Name>probe</Name><IsTruncated>false</IsTruncated><Contents><Key>probe-object</Key><LastModified>2026-09-04T00:00:00Z</LastModified><ETag>"fixture"</ETag><Size>15</Size><StorageClass>STANDARD</StorageClass></Contents></ListBucketResult>'
            status = 200
            operation = "list"
        else:
            assert url.path == "/probe/probe-object"
            operation = self.headers.get("Range")
            if self.command == "PUT":
                assert body == b"transport-probe"
                output = b""
                status = 200
                if self.headers.get("If-None-Match") == "*":
                    operation = "create"
                    if exists:
                        status = 412
                    exists = True
                else:
                    assert self.headers["If-Match"] == '"fixture"'
                    operation = "update"
            elif self.command == "DELETE":
                exists = False
                output = b""
                status = 204
            else:
                output = b"transport-probe"
                status = 200
                if self.headers.get("Range"):
                    assert self.headers["Range"] == "bytes=2-6"
                    output = output[2:7]
                    status = 206
        calls.append((self.command, operation))
        self.send_response(status)
        self.send_header("Content-Length", str(len(output)))
        self.send_header("Last-Modified", "Fri, 04 Sep 2026 00:00:00 GMT")
        self.send_header("ETag", '"fixture"')
        if status == 206:
            self.send_header("Content-Range", "bytes 2-6/15")
        self.end_headers()
        self.wfile.write(output)

    do_PUT = process
    do_GET = process
    do_DELETE = process
    do_POST = process

server = http.server.ThreadingHTTPServer(("127.0.0.1", 19171), Handler)
threading.Thread(target=server.serve_forever, daemon=True).start()
with (ROOT / "runtime.log").open("w") as log:
    process = subprocess.Popen(["spin", "up", "--from", str(ROOT / "spin.toml"), "--listen", "127.0.0.1:19172", "--max-instance-memory", "134217728"], stdout=log, stderr=subprocess.STDOUT, env={**os.environ, "SPIN_MAX_INSTANCE_COUNT": "1", "SPIN_WASMTIME_INSTANCE_COUNT": "1"})
    try:
        for attempt in range(100):
            try:
                with urllib.request.urlopen("http://127.0.0.1:19172/.well-known/spin/health", timeout=10) as response:
                    response.read()
                break
            except urllib.error.URLError:
                if process.poll() is not None:
                    raise RuntimeError((ROOT / "runtime.log").read_text())
                time.sleep(.1)
        else:
            raise RuntimeError("Spin failed to respond")
        with urllib.request.urlopen("http://127.0.0.1:19172/", timeout=10) as response:
            result = response.read().decode()
        assert result == "signed conditional put/get/range/list/delete transport passed\n", result
        assert calls == [("PUT", "create"), ("PUT", "create"), ("PUT", "update"), ("GET", None), ("GET", "bytes=2-6"), ("GET", "list"), ("POST", "delete")], calls
        with urllib.request.urlopen("http://127.0.0.1:19172/failure", timeout=10) as response:
            assert response.read() == b"bounded failure passed\n"
        assert calls[-1] == ("GET", "503") and len(calls) == 8, calls
        def get_held():
            with urllib.request.urlopen("http://127.0.0.1:19172/held", timeout=15) as response:
                return response.read()
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            first = executor.submit(get_held)
            assert held.wait(10), "first instance did not reach provider"
            try:
                try:
                    urllib.request.urlopen("http://127.0.0.1:19172/failure", timeout=5)
                    raise AssertionError("second live instance was admitted")
                except urllib.error.HTTPError as error:
                    assert error.code >= 500, error.code
                assert calls[-1] == ("GET", "held") and len(calls) == 9, calls
            finally:
                release.set()
            assert first.result() == b"held request released\n"
        print(json.dumps({"host_admission": "second concurrent instance rejected","result": result.strip(), "failure": "503 propagated without retry", "calls": calls}))
    finally:
        process.terminate()
        process.wait(timeout=10)
        server.shutdown()
