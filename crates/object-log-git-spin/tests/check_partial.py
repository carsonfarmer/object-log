"""Opt-in unchanged Git partial clients across cold checkpoint/GC on Spin/MinIO.

Run after the release WASIp2 build. Uses only loopback endpoints and destroys
its owned MinIO container on exit. Alternatively set OBJECT_LOG_MINIO_ENDPOINT,
BUCKET, ACCESS_KEY and SECRET_KEY (all with OBJECT_LOG_MINIO_ prefix) for an
existing loopback service/bucket, which is never deleted. Host compilation is
outside the serving budget.
"""
import json
import ipaddress
import errno
import signal
import os
import pathlib
import socket
import subprocess
import tempfile
import time
import urllib.request
import urllib.parse
import uuid

ROOT = pathlib.Path(__file__).resolve().parent.parent
IMAGE = "minio/minio@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e"
CONTAINER = "object-log-partial-" + uuid.uuid4().hex
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
            listener_closed = probe.connect_ex(("127.0.0.1", port)) == errno.ECONNREFUSED
        try:
            os.killpg(host.pid, 0)
            group_gone = False
        except ProcessLookupError:
            group_gone = True
        except PermissionError:
            # macOS can transiently report EPERM while the group exits.
            group_gone = False
        if listener_closed and group_gone:
            return
        if attempt == 100:
            try:
                os.killpg(host.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        time.sleep(.05)
    raise RuntimeError(f"old Spin process group or listener {port} survived termination")


def verify(path, count):
    assert git(path, "rev-list", "--count", "HEAD") == str(count)
    git(path, "fsck", "--strict", "--no-reflogs")


def missing(path):
    # rev-list reports promised missing OIDs without faulting them in.
    return {line[1:] for line in git(path, "rev-list", "--objects", "--all", "--missing=print").splitlines() if line.startswith("?")}


def present(path, oid):
    entries = git(path, "cat-file", "--batch-all-objects", "--batch-check=%(objectname)")
    return oid in entries.splitlines()


def external_minio():
    endpoint = os.environ.get("OBJECT_LOG_MINIO_ENDPOINT")
    if endpoint is None:
        return None
    parsed = urllib.parse.urlsplit(endpoint)
    hostname = parsed.hostname or ""
    loopback = hostname == "localhost"
    if not loopback:
        try:
            loopback = ipaddress.ip_address(hostname).is_loopback
        except ValueError:
            pass
    if (not loopback or parsed.scheme not in ("http", "https")
            or parsed.username or parsed.password or parsed.path not in ("", "/")
            or parsed.query or parsed.fragment):
        raise ValueError("external MinIO endpoint must be a loopback HTTP(S) origin")
    values = [os.environ.get("OBJECT_LOG_MINIO_" + key)
              for key in ("BUCKET", "ACCESS_KEY", "SECRET_KEY")]
    if not all(values):
        raise ValueError("external MinIO requires BUCKET, ACCESS_KEY and SECRET_KEY")
    return endpoint.rstrip("/"), *values


external = external_minio()
owned_container = external is None
try:
    if external:
        endpoint, bucket, access_key, secret_key = external
        ENV.update(AWS_ACCESS_KEY_ID=access_key, AWS_SECRET_ACCESS_KEY=secret_key)
        ready(endpoint + "/minio/health/ready")
    else:
        bucket, access_key, secret_key = "object-log-test", "objectlog", "objectlog-local-test-secret"
        run(["docker", "run", "--detach", "--rm", "--name", CONTAINER,
             "--publish", "127.0.0.1::9000", "--env", "MINIO_ROOT_USER=" + access_key,
             "--env", "MINIO_ROOT_PASSWORD=" + secret_key, IMAGE, "server", "/data"])
        endpoint = "http://" + run(["docker", "port", CONTAINER, "9000/tcp"])
        ready(endpoint + "/minio/health/ready")
        run(["aws", "--endpoint-url", endpoint, "s3api", "create-bucket", "--bucket", bucket])
    for name in ["sha1", "sha256"]:
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            with socket.socket() as sock:
                sock.bind(("127.0.0.1", 0))
                port = sock.getsockname()[1]
            url = f"http://127.0.0.1:{port}/repo"
            prefix = "partial-fixture-" + uuid.uuid4().hex
            variables = dict(endpoint=endpoint, bucket=bucket, access_key=access_key,
                             secret_key=secret_key, prefix=prefix, object_format=name, auth_mode="disabled")
            config = root / "config.toml"
            config.write_text("".join(f"{key} = {json.dumps(value)}\n" for key, value in variables.items()))
            log_path = ROOT / "tests" / ("partial-" + name + ".log")
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
                    for n in range(3):
                        (source / "small").write_text("small" + str(n))
                        (source / "large").write_text("x" * 8192 + str(n))
                        git(source, "add", ".")
                        git(source, "commit", "--quiet", "-m", str(n))
                    small = git(source, "rev-parse", "HEAD:small")
                    large = git(source, "rev-parse", "HEAD:large")
                    tip = git(source, "rev-parse", "HEAD")
                    git(source, "tag", "-a", "v1", "-m", "v1")
                    git(source, "push", "--quiet", url, "main", "v1")
                    clones = {}
                    for filter_spec in ["blob:none", "blob:limit=1024"]:
                        target = root / filter_spec.replace(":", "-").replace("=", "-")
                        git(root, "clone", "--quiet", "--no-checkout", "--filter=" + filter_spec, url, str(target))
                        assert git(target, "config", "remote.origin.promisor") == "true"
                        assert git(target, "config", "remote.origin.partialclonefilter") == filter_spec
                        assert list((target / ".git/objects/pack").glob("*.promisor"))
                        assert large in missing(target)
                        assert present(target, small) == (filter_spec != "blob:none")
                        git(target, "fsck", "--strict")
                        clones[filter_spec] = target
                    shallow = root / "shallow"
                    git(root, "clone", "--quiet", "--no-checkout", "--depth=1", "--filter=blob:none", url, str(shallow))
                    assert large in missing(shallow)
                    verify(shallow, 1)
                    stop(host, port)
                    unavailable = subprocess.run(["git", "show", "HEAD:large"], cwd=clones["blob:none"], env=ENV, capture_output=True, timeout=10)
                    assert unavailable.returncode != 0
                    assert not present(clones["blob:none"], large)
                    maintenance_env = dict(ENV, OBJECT_LOG_MINIO_ENDPOINT=endpoint,
                        OBJECT_LOG_MINIO_ACCESS_KEY=access_key, OBJECT_LOG_MINIO_SECRET_KEY=secret_key,
                        OBJECT_LOG_MINIO_BUCKET=bucket, OBJECT_LOG_PARTIAL_PREFIX=prefix, OBJECT_LOG_PARTIAL_FORMAT=name)
                    print(run(["cargo", "test", "--locked", "-p", "object-log-git", "--features", "aws", "--test", "partial_maintenance",
                               "--", "--ignored", "--nocapture"], ROOT.parent.parent, maintenance_env), flush=True)
                    host = start()
                    for target in clones.values():
                        assert host.poll() is None, f"Spin exited after restart: {host.returncode}"
                        ready(f"http://127.0.0.1:{port}/.well-known/spin/health", host)
                        before = missing(target)
                        assert git(target, "show", "HEAD:large") == "x" * 8192 + "2"
                        assert present(target, large)
                        assert missing(target) < before
                        git(target, "checkout", "--quiet", "main")
                        assert (target / "small").read_text() == "small2"
                        assert (target / "large").read_text() == "x" * 8192 + "2"
                        git(target, "fsck", "--strict")
                    git(shallow, "fetch", "--quiet", "--deepen=1")
                    verify(shallow, 2)
                    assert git(shallow, "show", "HEAD:large") == "x" * 8192 + "2"
                    git(shallow, "fetch", "--quiet", "--unshallow")
                    verify(shallow, 3)
                    (source / "large").write_text("y" * 8192)
                    git(source, "commit", "--quiet", "-am", "incremental")
                    git(source, "push", "--quiet", url, "main")
                    new_large = git(source, "rev-parse", "HEAD:large")
                    for target in clones.values():
                        git(target, "fetch", "--quiet")
                        assert not present(target, new_large)
                        assert git(target, "show", "origin/main:large") == "y" * 8192
                        git(target, "fsck", "--strict")
                    refetch = clones["blob:none"]
                    git(refetch, "fetch", "--quiet", "--refetch", "--filter=blob:limit=16384")
                    assert not missing(refetch)
                    git(refetch, "fsck", "--strict")
                    print(name + ": promisor omissions, thresholds, lazy show/checkout, cold checkpoint/GC, shallow/deepen/unshallow incremental fetch, filter refetch and unavailable-remote recovery passed", flush=True)
                finally:
                    stop(host, port)
except Exception:
    for path in ROOT.glob("tests/partial-*.log"):
        print(path.read_text())
    raise
finally:
    if owned_container:
        subprocess.run(["docker", "rm", "--force", CONTAINER], capture_output=True, check=False, timeout=20)
