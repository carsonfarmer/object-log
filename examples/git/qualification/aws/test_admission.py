"""Opt-in stock Caddy test: GIT_HOST_ADMISSION_TEST=1 python3 test_admission.py.

Docker is required. GIT_HOST_SOURCE may name another qualification/aws directory.
GIT_SYSTEMD_TEST_CONTAINER additionally enables real systemd cleanup checks in a
disposable Linux container with systemd as PID 1 and Python 3 installed.
"""

import contextlib
import http.client
import http.server
import importlib.util
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import threading
import time
import unittest
import urllib.request
import uuid


SOURCE = Path(os.getenv("GIT_HOST_SOURCE", Path(__file__).parent))


def command(*args, **kwargs):
    kwargs.setdefault("timeout", 30)
    return subprocess.check_output(args, text=True, stderr=subprocess.STDOUT, **kwargs).strip()


def template_section(start, end):
    template = (SOURCE / "host-bootstrap.sh.tftpl").read_text()
    return template.split(start, 1)[1].split(end, 1)[0]


def wait_for(predicate, seconds=10):
    until = time.monotonic() + seconds
    while time.monotonic() < until:
        if predicate():
            return
        time.sleep(0.05)
    raise AssertionError("fixture did not reach the expected state")


class Backend(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    release = threading.Event()
    calls = []
    retained = False
    payload = b"existing fetch\n" * 16384

    def log_message(self, *_):
        pass

    def handle_request(self):
        self.rfile.read(int(self.headers.get("Content-Length", 0)))
        Backend.calls.append((self.command, self.path))
        admin = self.path.endswith(("/maintenance", "/collect"))
        status = 401 if admin and self.headers.get("Authorization") != "Bearer fixture" else 200
        data = b"git"
        if admin:
            state = "complete"
            if Backend.retained:
                state = "more" if self.path.endswith("/maintenance") else "retained"
            data = json.dumps({"state": state}).encode()
        slow = self.headers.get("X-Test-Slow") == "yes"
        if slow:
            data = self.payload
        self.send_response(status)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        if slow:
            self.wfile.write(data[:65536])
            self.wfile.flush()
            if not self.release.wait(10):
                raise AssertionError("test did not release existing request")
            data = data[65536:]
        self.wfile.write(data)

    do_GET = do_POST = handle_request


@unittest.skipUnless(os.getenv("GIT_HOST_ADMISSION_TEST") == "1", "opt-in Caddy fixture")
class AdmissionTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.cleanup = contextlib.ExitStack()
        cls.addClassCleanup(cls.cleanup.close)
        cls.directory = Path(cls.cleanup.enter_context(tempfile.TemporaryDirectory()))
        cls.marker_dir = cls.directory / "run"
        cls.marker_dir.mkdir()
        cls.marker = cls.marker_dir / "pause"
        backend = http.server.ThreadingHTTPServer(("0.0.0.0", 0), Backend)
        cls.cleanup.callback(backend.server_close)
        cls.cleanup.callback(backend.shutdown)
        threading.Thread(target=backend.serve_forever, daemon=True).start()
        snippet = template_section("<<'CADDY'\n", "\nCADDY")
        # This local HTTP fixture omits the public-IP certificate configuration.
        snippet = re.sub(r"%\{ if public_ip_https ~\}.*?%\{ endif ~\}\n", "", snippet, flags=re.S)
        paths = [f"/{name}/{operation}" for name in ("alpha/project.git", "star*project.git")
                 for operation in ("maintenance", "collect", "recover-retentions-after-drain")]
        snippet = snippet.replace("${hostname}", "http://:8080")
        snippet = snippet.replace("${admin_paths}", json.dumps(paths)).replace("${retry_after}", "2")
        snippet = snippet.replace("127.0.0.1:3000", f"host.docker.internal:{backend.server_port}")
        if "${" in snippet:
            raise AssertionError("unrendered Caddy template field")
        (cls.directory / "Caddyfile").write_text(snippet)
        name = "object-log-caddy-test-" + uuid.uuid4().hex[:8]
        command("docker", "run", "--detach", "--name", name, "--add-host", "host.docker.internal:host-gateway",
                "--publish", "127.0.0.1::8080", "--volume", f"{cls.directory / 'Caddyfile'}:/etc/caddy/Caddyfile:ro",
                "--volume", f"{cls.marker_dir}:/run/object-log-maintenance:ro", "caddy:2.10.2-alpine")
        cls.cleanup.callback(command, "docker", "rm", "--force", name)
        cls.port = int(command("docker", "port", name, "8080").rsplit(":", 1)[1])

        def ready():
            try:
                return cls.request("GET", "/ready")[0] == 200
            except OSError:
                return False

        try:
            wait_for(ready)
        except Exception:
            print(command("docker", "logs", name))
            raise
        spec = importlib.util.spec_from_file_location("admission_maintenance", SOURCE / "maintenance.py")
        cls.worker = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cls.worker)

    def setUp(self):
        self.marker.unlink(missing_ok=True)
        Backend.calls = []
        Backend.retained = False
        Backend.release.clear()
        self.addCleanup(Backend.release.set)
        self.addCleanup(self.marker.unlink, missing_ok=True)

    @classmethod
    def request(cls, method, path, headers=None):
        connection = http.client.HTTPConnection("127.0.0.1", cls.port, timeout=5)
        try:
            connection.request(method, path, headers=headers or {})
            response = connection.getresponse()
            return response.status, response.read(), response.getheader("Retry-After")
        finally:
            connection.close()

    def test_existing_request_survives_and_admin_paths_are_exact(self):
        existing = http.client.HTTPConnection("127.0.0.1", self.port, timeout=10)
        self.addCleanup(existing.close)
        existing.request("POST", "/alpha/project.git/git-upload-pack", headers={"X-Test-Slow": "yes"})
        response = existing.getresponse()
        first = response.read(1)
        control = http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)
        self.addCleanup(control.close)
        control.request("GET", "/alpha/project.git/info/refs?service=git-upload-pack")
        self.assertEqual(control.getresponse().read(), b"git")
        self.marker.touch()
        wait_for(lambda: self.request("GET", "/alpha/project.git/info/refs?service=git-upload-pack")[0] == 503)
        # Admission applies to a new request on an already-open connection too.
        control.request("POST", "/alpha/project.git/git-receive-pack")
        paused = control.getresponse()
        self.assertEqual(paused.status, 503)
        self.assertEqual(paused.getheader("Retry-After"), "2")
        paused.read()
        for method, path in [("POST", "/alpha/project.git/git-upload-pack"),
                             ("GET", "/alpha/project.git/maintenance"),
                             ("POST", "/unknown.git/maintenance"),
                             ("POST", "/starXproject.git/maintenance")]:
            self.assertEqual(self.request(method, path)[0], 503, path)
        for path in ("/alpha/project.git/maintenance", "/alpha/project.git/collect", "/star*project.git/maintenance"):
            self.assertEqual(self.request("POST", path)[0], 401, path)
            self.assertEqual(self.request("POST", path, {"Authorization": "Bearer fixture"})[0], 200, path)
        Backend.release.set()
        self.assertEqual(response.status, 200)
        self.assertEqual(first + response.read(), Backend.payload)
        self.marker.unlink()
        self.assertEqual(self.request("GET", "/alpha/project.git/info/refs?service=git-upload-pack")[0], 200)

    def test_retained_deadline_reopens_without_reader_recovery(self):
        Backend.retained = True

        def post(repository, operation, deadline):
            self.assertTrue(self.marker.exists())
            request = urllib.request.Request(f"http://127.0.0.1:{self.port}/{repository}/{operation}",
                                             data=b"", headers={"Authorization": "Bearer fixture"})
            return self.worker.request_json(request, deadline)["state"]

        started = time.monotonic()
        self.assertEqual(self.worker.maintain(["alpha/project.git"], post, 2, 0.2, self.marker), 1)
        self.assertLess(time.monotonic() - started, 2)
        self.assertFalse(self.marker.exists())
        self.assertEqual(Backend.calls, [("POST", "/alpha/project.git/maintenance"),
                                         ("POST", "/alpha/project.git/collect")])
        self.assertEqual(self.request("GET", "/alpha/project.git/info/refs?service=git-upload-pack")[0], 200)

    def test_unexpected_worker_failure_removes_marker(self):
        def broken(*_):
            self.assertTrue(self.marker.exists())
            raise RuntimeError("fixture failure")

        with self.assertRaises(RuntimeError):
            self.worker.maintain(["alpha/project.git"], broken, 2, 1, self.marker)
        self.assertFalse(self.marker.exists())


@unittest.skipUnless(os.getenv("GIT_SYSTEMD_TEST_CONTAINER"), "optional real Linux systemd fixture")
class SystemdCleanupTest(unittest.TestCase):
    def test_stop_kill_deadline_and_restart_remove_runtime_marker(self):
        container = os.environ["GIT_SYSTEMD_TEST_CONTAINER"]

        def execute(*args):
            return command("docker", "exec", container, *args)

        execute("/bin/sh", "-c", "id object-log-git >/dev/null 2>&1 || useradd --system object-log-git")
        execute("mkdir", "-p", "/opt/object-log-admission")
        command("docker", "cp", str(SOURCE / "maintenance.py"), f"{container}:/opt/object-log-admission/maintenance.py")
        with tempfile.TemporaryDirectory() as directory:
            fixture = Path(directory) / "fixture.py"
            fixture.write_text('''import os, pathlib, signal, time
import maintenance
def post(*args):
    mode = pathlib.Path("/opt/object-log-admission/mode").read_text().strip()
    if mode == "timeout":
        signal.signal(signal.SIGALRM, signal.SIG_IGN)
    time.sleep(30)
maintenance.maintain(["repo.git"], post, 30, 30)
''')
            command("docker", "cp", str(fixture), f"{container}:/opt/object-log-admission/fixture.py")
            unit = template_section("cat > /etc/systemd/system/object-log-maintenance.service <<'UNIT'\n", "\nUNIT")
            unit = unit.replace("${run_timeout}", "2")
            unit = re.sub(r"^ExecStart=.*$", "ExecStart=/usr/bin/python3 /opt/object-log-admission/fixture.py", unit, flags=re.M)
            path = Path(directory) / "object-log-admission-test.service"
            path.write_text(unit)
            command("docker", "cp", str(path), f"{container}:/etc/systemd/system/{path.name}")
        name = "object-log-admission-test.service"
        self.addCleanup(execute, "rm", "-f", f"/etc/systemd/system/{name}")
        self.addCleanup(execute, "systemctl", "stop", name)
        execute("systemctl", "daemon-reload")

        def marker_exists():
            return execute("/bin/sh", "-c", "test -f /run/object-log-maintenance/pause && echo yes || echo no") == "yes"

        for mode in ("stop", "kill", "timeout", "restart"):
            with self.subTest(mode=mode):
                execute("/bin/sh", "-c", f"echo {mode} >/opt/object-log-admission/mode")
                execute("systemctl", "start", "--no-block", name)
                wait_for(marker_exists)
                if mode == "stop":
                    execute("systemctl", "stop", name)
                elif mode == "kill":
                    execute("systemctl", "kill", "--signal=KILL", "--kill-whom=main", name)
                elif mode == "restart":
                    execute("touch", "/run/object-log-maintenance/stale")
                    execute("systemctl", "restart", "--no-block", name)
                    wait_for(lambda: marker_exists() and execute("/bin/sh", "-c", "test ! -e /run/object-log-maintenance/stale && echo yes || echo no") == "yes")
                    execute("systemctl", "stop", name)
                wait_for(lambda: not marker_exists())
                self.assertEqual(execute("/bin/sh", "-c", "test ! -e /run/object-log-maintenance && echo yes || echo no"), "yes")
                if mode in {"kill", "timeout"}:
                    expected = "signal" if mode == "kill" else "timeout"
                    self.assertEqual(execute("systemctl", "show", "--property=Result", "--value", name), expected)
                execute("systemctl", "reset-failed", name)


if __name__ == "__main__":
    unittest.main(verbosity=2)
