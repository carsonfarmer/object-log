#!/usr/bin/env python3
"""Opt-in local test: stock Spin, composed WAL, and a strict IMDS/S3 wire fixture.

No AWS calls or credentials. The test-only bridge feature fixes metadata to this
loopback listener; artifacts have a separate target directory from normal builds.
This proves credential acquisition/refresh in WASI, not S3 backend conformance.
"""

import hashlib
import http.client
import http.server
import json
import pathlib
import re
import socket
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET


class Fixture(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    objects = {}
    mode = "obtain"
    issued = 0
    metadata = []
    signatures = []
    errors = []

    def log_message(self, *_):
        pass

    def reply(self, code, data=b"", **headers):
        self.send_response(code)
        self.send_header("Content-Length", str(len(data)))
        for name, value in headers.items():
            self.send_header(name.replace("_", "-"), value)
        self.end_headers()
        self.wfile.write(data)

    def dispatch(self):
        body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
        if self.path.startswith("/latest/"):
            Fixture.metadata.append((self.command, self.path))
            assert "Authorization" not in self.headers
            if self.command == "PUT":
                assert self.path == "/latest/api/token"
                assert self.headers["X-aws-ec2-metadata-token-ttl-seconds"] == "600"
                if Fixture.mode == "slow-metadata":
                    time.sleep(2)
                    return self.reply(200, b"fixture-token")
                if Fixture.mode == "unavailable" or (Fixture.mode == "renewal-unavailable" and Fixture.issued):
                    return self.reply(403, b"metadata unavailable")
                return self.reply(200, b"fixture-token")
            assert self.command == "GET"
            assert self.headers["X-aws-ec2-metadata-token"] == "fixture-token"
            if self.path == "/latest/meta-data/iam/security-credentials/":
                return self.reply(200, b"fixture-role")
            assert self.path == "/latest/meta-data/iam/security-credentials/fixture-role"
            Fixture.issued += 1
            generation = Fixture.issued
            credentials = {
                "AccessKeyId": f"key-{generation}",
                "SecretAccessKey": f"secret-{generation}",
                "Token": f"session-{generation}",
                "Expiration": "2000-01-01T00:00:00Z" if generation == 1 else "2100-01-01T00:00:00Z",
            }
            return self.reply(200, json.dumps(credentials).encode())

        assert self.path.startswith("/fixture/credentials/") or (self.command == "POST" and self.path == "/fixture?delete")
        authorization = self.headers["Authorization"]
        match = re.search(r"Credential=key-(\d+)/\d+/us-west-2/s3/aws4_request", authorization)
        assert match, "missing role credential or wrong signing region"
        generation = int(match.group(1))
        assert self.headers["X-amz-security-token"] == f"session-{generation}"
        Fixture.signatures.append(generation)
        if self.command == "POST":
            result = ET.Element("DeleteResult")
            for key in ET.fromstring(body).findall(".//{*}Key"):
                assert key.text.startswith("credentials/")
                Fixture.objects.pop("/fixture/" + key.text, None)
                ET.SubElement(ET.SubElement(result, "Deleted"), "Key").text = key.text
            return self.reply(200, ET.tostring(result))
        previous = Fixture.objects.get(self.path)
        etag = '"' + hashlib.sha256(previous).hexdigest() + '"' if previous is not None else None
        if self.command == "PUT":
            if self.headers.get("If-None-Match") == "*" and previous is not None:
                return self.reply(412)
            if "If-Match" in self.headers and self.headers["If-Match"] != etag:
                return self.reply(412)
            Fixture.objects[self.path] = body
            return self.reply(200, ETag='"' + hashlib.sha256(body).hexdigest() + '"')
        if self.command == "DELETE":
            Fixture.objects.pop(self.path, None)
            return self.reply(204)
        assert self.command == "GET"
        if previous is None:
            return self.reply(404)
        if self.headers.get("If-None-Match") == etag:
            return self.reply(304)
        return self.reply(200, previous, ETag=etag, Last_Modified="Sun, 20 Sep 2026 00:00:00 GMT")

    def handle_fixture(self):
        try:
            self.dispatch()
        except (BrokenPipeError, ConnectionResetError):
            pass  # Expected when a WASI metadata deadline cancels the exchange.
        except Exception as error:
            Fixture.errors.append(repr(error))
            self.reply(500, b"fixture assertion failed")

    do_GET = do_PUT = do_DELETE = do_POST = handle_fixture


def main():
    component = pathlib.Path(__file__).resolve().parents[1]
    target = component / "target" / "credential-test"
    subprocess.run([
        "cargo", "build", "--locked", "--release", "--manifest-path", str(component / "Cargo.toml"),
        "--target", "wasm32-wasip2", "--target-dir", str(target), "--features", "test-imds",
        "--lib", "--example", "credential-client",
    ], check=True)
    artifacts = target / "wasm32-wasip2" / "release"
    # Binding must fail rather than replacing another process's listener.
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 19092), Fixture)
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()
    try:
        with tempfile.TemporaryDirectory(prefix="object-log-imds-") as directory:
            directory = pathlib.Path(directory)
            subprocess.run([
                "wac", "plug", "--plug", str(artifacts / "object_log_component.wasm"),
                str(artifacts / "examples" / "credential_client.wasm"), "-o", str(directory / "probe.wasm"),
            ], check=True)
            (directory / "spin.toml").write_text('''spin_manifest_version = 2
[application]
name = "wal-credential-test"
version = "0.0.0"
[[trigger.http]]
route = "/..."
component = "probe"
[component.probe]
source = "probe.wasm"
allowed_outbound_hosts = ["http://127.0.0.1:19092"]
''')
            with (directory / "spin.log").open("w+") as log:
                with socket.socket() as listener:
                    listener.bind(("127.0.0.1", 0))
                    spin_port = listener.getsockname()[1]
                spin = subprocess.Popen(["spin", "up", "--from", str(directory / "spin.toml"), "--listen", f"127.0.0.1:{spin_port}"], stdout=log, stderr=log)
                try:
                    for _ in range(100):
                        if spin.poll() is not None:
                            raise RuntimeError("Spin exited before readiness")
                        try:
                            connection = http.client.HTTPConnection("127.0.0.1", spin_port, timeout=0.1)
                            connection.connect()
                            connection.close()
                            break
                        except OSError:
                            time.sleep(0.1)
                    else:
                        raise RuntimeError("Spin did not listen")
                    for scenario in ("obtain", "existing", "missing", "unavailable", "renewal-unavailable", "slow-metadata"):
                        Fixture.mode, Fixture.issued = scenario, 0
                        Fixture.metadata, Fixture.signatures = [], []
                        started = time.monotonic()
                        try:
                            with urllib.request.urlopen(f"http://127.0.0.1:{spin_port}/{scenario}", timeout=10) as response:
                                assert response.read() == b"ok"
                        except urllib.error.HTTPError as error:
                            raise RuntimeError(f"{scenario}: {error.read().decode()}; fixture={Fixture.errors}") from error
                        assert not Fixture.errors, Fixture.errors
                        if scenario in ("unavailable", "slow-metadata"):
                            assert Fixture.metadata and not Fixture.signatures
                            assert all(method == "PUT" for method, _ in Fixture.metadata), "IMDSv1 fallback"
                            assert time.monotonic() - started < 6, "metadata deadline was not enforced"
                        elif scenario == "renewal-unavailable":
                            assert Fixture.issued == 1 and Fixture.signatures == [1]
                        else:
                            assert Fixture.issued == 2, f"expiry renewal not observed: {Fixture.issued}"
                            assert Fixture.signatures[0] == 1 and set(Fixture.signatures[1:]) == {2}
                        assert not any(path.endswith("/missing/index.cbor") for path in Fixture.objects)
                        print(f"PASS {scenario}: metadata={len(Fixture.metadata)} signed-storage={len(Fixture.signatures)} credential-generations={Fixture.issued}")
                except Exception:
                    log.flush()
                    log.seek(0)
                    print(log.read())
                    raise
                finally:
                    spin.terminate()
                    try:
                        spin.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        spin.kill()
                        spin.wait()
    finally:
        server.shutdown()
        server.server_close()
        worker.join()


if __name__ == "__main__":
    main()
