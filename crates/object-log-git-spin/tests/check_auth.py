"""Actual Spin auth rejection: counting backend and deliberately unsent bodies."""
import base64
import contextlib
import http.client
import http.server
import json
import pathlib
import secrets
import socket
import subprocess
import tempfile
import threading
import time

ROOT = pathlib.Path(__file__).resolve().parent.parent


class Backend(http.server.BaseHTTPRequestHandler):
    calls = 0

    def do_GET(self):
        type(self).calls += 1
        self.send_response(500)
        self.end_headers()

    do_HEAD = do_GET
    do_POST = do_GET
    do_PUT = do_GET
    do_DELETE = do_GET

    def log_message(self, *_):
        pass


def basic(token):
    return "Basic " + base64.b64encode(("git:" + token).encode()).decode()


def request(port, path, headers, post=False):
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
    connection.putrequest("POST" if post else "GET", path)
    for name, value in headers:
        connection.putheader(name, value)
    if post:
        # Never send the declared body. A response proves auth did not await it.
        connection.putheader("Content-Length", str(1024 * 1024))
    connection.endheaders()
    response = connection.getresponse()
    result = response.status, dict(response.getheaders()), response.read()
    connection.close()
    time.sleep(.05)  # Separate policy checks from the known host admission race.
    return result


def port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


@contextlib.contextmanager
def host(variables, directory):
    config = pathlib.Path(directory) / "repository.toml"
    config.touch(mode=0o600, exist_ok=True)
    config.write_text("".join(f"{key} = {json.dumps(value)}\n" for key, value in variables.items()))
    address = port()
    with (pathlib.Path(directory) / "runtime.log").open("w") as log:
        process = subprocess.Popen([str(ROOT / "run.sh"), "--listen", f"127.0.0.1:{address}", "--variable", "@" + str(config)], stdout=log, stderr=subprocess.STDOUT)
        try:
            for _ in range(200):
                if process.poll() is not None:
                    raise RuntimeError("Spin exited during auth fixture startup")
                try:
                    with socket.create_connection(("127.0.0.1", address), timeout=.1):
                        break
                except OSError:
                    time.sleep(.1)
            else:
                raise RuntimeError("Spin auth fixture startup timeout")
            yield address
        finally:
            process.terminate()
            process.wait(timeout=10)
    output = (pathlib.Path(directory) / "runtime.log").read_text()
    for key in ["auth_read_token", "auth_write_token", "secret_key"]:
        value = variables.get(key)
        if value:
            assert value not in output, "secret appeared in runtime output"
            assert base64.b64encode(("git:" + value).encode()).decode() not in output, "encoded credential appeared in runtime output"


def main():
    reader, writer = secrets.token_hex(32), secrets.token_hex(32)
    backend = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Backend)
    threading.Thread(target=backend.serve_forever, daemon=True).start()
    try:
        with tempfile.TemporaryDirectory() as directory:
            defaults = dict(endpoint=f"http://127.0.0.1:{backend.server_port}", bucket="unused", access_key="fixture", secret_key=secrets.token_hex(32))
            configs = [
                ({}, 500),
                (dict(auth_mode="unknown"), 500),
                (dict(auth_read_token="bad"), 500),
                (dict(auth_mode="disabled", auth_write_token=writer), 500),
                (dict(auth_read_token=reader, auth_write_token=reader.upper()), 500),
                (dict(auth_write_token=writer, read_only="invalid"), 500),
                (dict(auth_write_token=writer, read_only="true", allow_non_fast_forward="invalid"), 500),
                (dict(auth_write_token=writer, object_format="invalid"), 500),
                (dict(auth_read_token=reader, auth_write_token=writer), 401),
                (dict(auth_read_token=reader, auth_write_token=writer, read_only="true"), 401),
            ]
            for object_format in ["sha1", "sha256"]:
                for config, expected in configs:
                    with host(defaults | dict(object_format=object_format) | config, directory) as address:
                        routes = [("/repo/info/refs?service=git-upload-pack", False, False), ("/repo/git-upload-pack", True, False), ("/repo/info/refs?service=git-receive-pack", False, True), ("/repo/git-receive-pack", True, True)]
                        for path, post, write_scope in routes:
                            for auth in [[], [("Authorization", basic(secrets.token_hex(32)))], [("Authorization", "Basic !!!")], [("Authorization", basic(writer)), ("Authorization", basic(writer))], [("Authorization", "x" * 129)], [("Authorization", basic(writer) + ", " + basic(writer))]]:
                                status, headers, body = request(address, path, auth, post)
                                assert status == expected, (status, expected)
                                assert body == (b"Git request failed\n" if status == 500 else b"authentication required\n")
                                assert "location" not in {k.lower() for k in headers}
                                if status == 401:
                                    assert headers.get("www-authenticate") == 'Basic realm="object-log Git"'
                            if expected == 500 or write_scope:
                                # Readers on writes and read-only writers must be denied.
                                tokens = [reader, writer] if expected == 500 or config.get("read_only") == "true" else [reader]
                                for token in tokens:
                                    status, headers, body = request(address, path, [("Authorization", basic(token))], post)
                                    assert status == (500 if expected == 500 else 403)
                                    assert body == (b"Git request failed\n" if status == 500 else b"access forbidden\n")
                                    assert "location" not in {k.lower() for k in headers}
                        status, headers, _ = request(address, "/repo/unknown", [("Authorization", basic(writer))])
                        assert status == 404 and "location" not in {k.lower() for k in headers}
                        assert Backend.calls == 0, "rejected request reached backend"
                print(f"{object_format}: invalid config and auth rejected before backend/body; challenge and routes verified", flush=True)
    finally:
        backend.shutdown()
        backend.server_close()


if __name__ == "__main__":
    main()
