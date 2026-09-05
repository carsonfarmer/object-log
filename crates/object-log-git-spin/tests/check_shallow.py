"""Opt-in unchanged Git shallow clients against a fresh local Spin/MinIO pair.

Run after the release WASIp2 build. Uses only loopback endpoints and destroys
its MinIO container on exit. Host compilation is outside the serving budget.
"""
import json
import errno
import signal
import os
import pathlib
import socket
import subprocess
import tempfile
import time
import urllib.request
import uuid

ROOT = pathlib.Path(__file__).resolve().parent.parent
IMAGE = "minio/minio@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e"
CONTAINER = "object-log-shallow-" + uuid.uuid4().hex
ENV = dict(os.environ, GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL="/dev/null",
           GIT_AUTHOR_NAME="Test", GIT_AUTHOR_EMAIL="test@example.invalid",
           GIT_COMMITTER_NAME="Test", GIT_COMMITTER_EMAIL="test@example.invalid",
           AWS_ACCESS_KEY_ID="objectlog", AWS_SECRET_ACCESS_KEY="objectlog-local-test-secret",
           AWS_DEFAULT_REGION="us-east-1")


def run(args, cwd=None, env=None):
    result = subprocess.run(args, cwd=cwd, env=env or ENV, capture_output=True, text=True, timeout=120)
    if result.returncode:
        raise RuntimeError(f"{args}: {result.stderr}")
    return result.stdout.strip()


def git(path, *args):
    # See existing HTTP fixture: isolate known single-instance admission race.
    time.sleep(.05)
    return run(["git", "-c", "protocol.version=2", *args], cwd=path)


def ready(url, process=None):
    for _ in range(300):
        try:
            with urllib.request.urlopen(url, timeout=1):
                return
        except OSError:
            if process is not None and process.poll() is not None:
                raise RuntimeError("Spin exited during startup")
            time.sleep(.1)
    raise RuntimeError("server readiness timeout")


def stop(host, port):
    # Spin launches an HTTP child. Terminate this fixture's private group and
    # prove the old listener is gone before a cold restart or maintenance.
    try:
        os.killpg(host.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    try:
        host.wait(timeout=10)
    except subprocess.TimeoutExpired:
        os.killpg(host.pid, signal.SIGKILL)
        host.wait(timeout=10)
    for attempt in range(200):
        with socket.socket() as probe:
            probe.settimeout(.2)
            if probe.connect_ex(("127.0.0.1", port)) == errno.ECONNREFUSED:
                return
        if attempt == 100:
            try:
                os.killpg(host.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        time.sleep(.05)
    raise RuntimeError(f"old Spin listener {port} survived group termination")


def verify(path, count):
    assert git(path, "rev-list", "--count", "HEAD") == str(count)
    git(path, "fsck", "--strict", "--no-reflogs")


try:
    run(["docker", "run", "--detach", "--rm", "--name", CONTAINER,
         "--publish", "127.0.0.1::9000", "--env", "MINIO_ROOT_USER=objectlog",
         "--env", "MINIO_ROOT_PASSWORD=objectlog-local-test-secret", IMAGE, "server", "/data"])
    endpoint = "http://" + run(["docker", "port", CONTAINER, "9000/tcp"])
    ready(endpoint + "/minio/health/ready")
    run(["aws", "--endpoint-url", endpoint, "s3api", "create-bucket", "--bucket", "object-log-test"])
    for name in ["sha1", "sha256"]:
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            with socket.socket() as sock:
                sock.bind(("127.0.0.1", 0))
                port = sock.getsockname()[1]
            url = f"http://127.0.0.1:{port}/repo"
            variables = dict(endpoint=endpoint, bucket="object-log-test", access_key="objectlog",
                             secret_key="objectlog-local-test-secret", prefix="shallow-" + name,
                             object_format=name, auth_mode="disabled")
            config = root / "config.toml"
            config.write_text("".join(f"{key} = {json.dumps(value)}\n" for key, value in variables.items()))
            log_path = ROOT / "tests" / ("shallow-" + name + ".log")
            with log_path.open("w") as log:
                def start():
                    host = subprocess.Popen([str(ROOT / "run.sh"), "--listen", f"127.0.0.1:{port}",
                                             "--variable", "@" + str(config)], stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
                    try:
                        ready(f"http://127.0.0.1:{port}/.well-known/spin/health", host)
                    except BaseException:
                        stop(host, port)
                        raise
                    return host
                host = start()
                try:
                    source = root / "source"
                    git(root, "init", "--quiet", "-b", "main", "--object-format=" + name, str(source))
                    for n in range(1, 9):
                        (source / "file").write_text(str(n))
                        git(source, "add", "file")
                        dated = dict(ENV, GIT_AUTHOR_DATE=f"2000-01-{n:02}T00:00:00Z", GIT_COMMITTER_DATE=f"2000-01-{n:02}T00:00:00Z")
                        run(["git", "commit", "--quiet", "-m", str(n)], source, dated)
                        if n == 3:
                            git(source, "branch", "old")
                    git(source, "push", "--quiet", url, "main", "old")
                    clone = root / "clone"
                    git(root, "clone", "--quiet", "--depth=1", url, str(clone))
                    verify(clone, 1)
                    git(clone, "fetch", "--quiet", "--deepen=2")
                    verify(clone, 3)
                    git(clone, "fetch", "--quiet", "--depth=5")
                    verify(clone, 5)
                    stop(host, port)
                    host = start()
                    git(clone, "fetch", "--quiet", "--unshallow")
                    verify(clone, 8)
                    assert git(clone, "rev-parse", "--is-shallow-repository") == "false"
                    for option, count, label in [("--shallow-since=2000-01-05T00:00:00Z", 4, "since"),
                                                  ("--shallow-exclude=old", 5, "exclude")]:
                        target = root / label
                        git(root, "clone", "--quiet", option, url, str(target))
                        verify(target, count)
                        git(target, "fetch", "--quiet", "--unshallow")
                        verify(target, 8)
                    incremental = root / "incremental"
                    git(root, "clone", "--quiet", "--depth=1", url, str(incremental))
                    (source / "file").write_text("9")
                    git(source, "commit", "--quiet", "-am", "9")
                    git(source, "push", "--quiet", url, "main")
                    git(incremental, "fetch", "--quiet")
                    git(incremental, "reset", "--hard", "origin/main")
                    verify(incremental, 2)
                    git(incremental, "fetch", "--quiet", "--unshallow")
                    verify(incremental, 9)
                    git(source, "checkout", "--quiet", "-b", "side", "old")
                    for n in range(2):
                        (source / "side").write_text(str(n))
                        git(source, "add", "side")
                        git(source, "commit", "--quiet", "-m", "side" + str(n))
                    git(source, "checkout", "--quiet", "main")
                    git(source, "merge", "--quiet", "--no-ff", "side", "-m", "merge")
                    git(source, "push", "--quiet", url, "main", "side")
                    merged = root / "merged"
                    git(root, "clone", "--quiet", "--depth=2", url, str(merged))
                    verify(merged, 3)
                    git(merged, "fetch", "--quiet", "--deepen=2")
                    git(merged, "fsck", "--strict", "--no-reflogs")
                    git(merged, "fetch", "--quiet", "--unshallow")
                    verify(merged, 12)
                    git(source, "checkout", "--quiet", "side")
                    git(source, "commit", "--quiet", "--allow-empty", "-m", "diverge")
                    git(source, "push", "--quiet", url, "side")
                    head_cut = root / "head-cut"
                    git(root, "clone", "--quiet", "--branch", "side", "--shallow-exclude=HEAD", url, str(head_cut))
                    verify(head_cut, 1)
                    print(name + ": depth/deepen/absolute-depth/cold-unshallow/since/exclude/HEAD/incremental/merge strict client acceptance passed", flush=True)
                finally:
                    stop(host, port)
except Exception:
    for path in ROOT.glob("tests/shallow-*.log"):
        print(path.read_text())
    raise
finally:
    subprocess.run(["docker", "rm", "--force", CONTAINER], capture_output=True, check=False, timeout=20)
