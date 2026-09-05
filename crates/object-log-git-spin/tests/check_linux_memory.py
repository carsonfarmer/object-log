"""Opt-in Linux Spin process-memory qualification; Git and MinIO run outside its cgroup."""
import argparse
import hashlib
import json
import os
import pathlib
import random
import shutil
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
import uuid

MINIO = "minio/minio@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e"
RUNTIME = "rust:1.97.1-bookworm"
ROOT = pathlib.Path(__file__).resolve().parent.parent
LIMIT = 128 * 1024 * 1024


def run(*args, **kwargs):
    return subprocess.run(args, check=True, capture_output=True, text=True, **kwargs).stdout.strip()


def http(url, headers=None):
    with urllib.request.urlopen(urllib.request.Request(url, headers=headers or {}), timeout=3) as response:
        return response.read()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--spin", type=pathlib.Path, required=True, help="Official Linux aarch64 Spin binary")
    parser.add_argument("--wasm", type=pathlib.Path, required=True)
    parser.add_argument("--output", type=pathlib.Path, required=True)
    args = parser.parse_args()
    git_version = run("git", "--version")
    assert git_version.startswith("git version 2.54."), git_version
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    shutil.copyfile(args.wasm, output / "component.wasm")
    cache = output / "cache"
    cache.mkdir()
    shutil.copyfile(ROOT / "runtime-config.toml", output / "runtime-config.toml")
    (output / "cache.toml").write_text('[cache]\ndirectory = "/cache/artifacts"\n')
    manifest = (ROOT / "spin.toml").read_text().replace('../../target/wasm32-wasip2/release/object_log_git_spin.wasm', '/app/component.wasm')
    (output / "spin.toml").write_text(manifest)
    token = "object-log-linux-" + uuid.uuid4().hex[:12]
    network, minio = token + "-network", token + "-minio"
    owned = []
    records = []
    report = {"git_version": git_version, "wasm_sha256": hashlib.sha256(args.wasm.read_bytes()).hexdigest(), "runtime_image": RUNTIME, "runtime_image_id": run("docker", "image", "inspect", RUNTIME, "--format", "{{.Id}}"), "spin_version": run("docker", "run", "--rm", "--mount", f"type=bind,src={args.spin.resolve().parent},dst=/spin,readonly", RUNTIME, "/spin/spin", "--version"), "minio_image": MINIO, "limit_bytes": LIMIT, "outbound_connection_pooling": False, "runs": records}
    def save():
        (output / "result.json").write_text(json.dumps(report, indent=2) + "\n")
    def start(label, memory, object_format, prefix):
        evidence = output / label
        evidence.mkdir()
        variables = {"endpoint": "http://minio:9000", "bucket": "object-log-test", "region": "us-east-1", "access_key": "objectlog", "secret_key": "objectlog-local-test-secret", "prefix": prefix, "log_id": "repository", "object_format": object_format}
        (evidence / "variables.json").write_text(json.dumps(variables))
        name = token + "-" + label
        # Keep a small shell alive after Spin exits so OOM counters can be captured.
        wrapper = '''/spin/spin up --from /app/spin.toml --listen 0.0.0.0:3000 --variable @/evidence/variables.json --cache /app/cache.toml --runtime-config-file /app/runtime-config.toml --max-instance-memory 134217728 --state-dir /state --log-dir /state/logs &
pid=$!
trap 'kill -TERM "$pid" 2>/dev/null || true' TERM INT
wait "$pid"
code=$?
for file in memory.peak memory.events memory.stat memory.max memory.swap.max; do cat "/sys/fs/cgroup/$file" > "/evidence/$file"; done
exit "$code"
'''
        owned.append(name)
        run("docker", "run", "--detach", "--name", name, "--network", network, "--publish", "127.0.0.1::3000", "--memory", str(memory), "--memory-swap", str(memory), "--env", "SPIN_WASMTIME_POOLING=1", "--env", "SPIN_MAX_INSTANCE_COUNT=1", "--env", "SPIN_WASMTIME_INSTANCE_COUNT=1", "--mount", f"type=bind,src={args.spin.resolve().parent},dst=/spin,readonly", "--mount", f"type=bind,src={output},dst=/app,readonly", "--mount", f"type=bind,src={cache},dst=/cache", "--mount", f"type=bind,src={evidence},dst=/evidence", RUNTIME, "sh", "-c", wrapper)
        endpoint = "http://" + run("docker", "port", name, "3000/tcp")
        record = {"label": label, "limit_bytes": memory, "container": name, "ready": False}
        records.append(record)
        for _ in range(600):
            if run("docker", "inspect", "--format", "{{.State.Running}}", name) != "true":
                break
            try:
                http(endpoint + "/.well-known/spin/health")
                record["ready"] = True
                break
            except (OSError, urllib.error.URLError):
                time.sleep(.1)
        save()
        return name, endpoint, record
    def finish(name, record):
        if run("docker", "inspect", "--format", "{{.State.Running}}", name) == "true":
            run("docker", "stop", "--time", "5", name)
        logs = subprocess.run(["docker", "logs", name], capture_output=True, text=True, check=True)
        (output / record["label"] / "runtime.log").write_text(logs.stdout + logs.stderr)
        state = json.loads(run("docker", "inspect", name))[0]["State"]
        record["state"] = state
        peak = output / record["label"] / "memory.peak"
        record["memory_peak_bytes"] = int(peak.read_text()) if peak.exists() else None
        events = output / record["label"] / "memory.events"
        record["memory_events"] = dict((key, int(value)) for key, value in (line.split() for line in events.read_text().splitlines())) if events.exists() else None
        save()
        print(json.dumps(record), flush=True)
    try:
        run("docker", "network", "create", network)
        run("docker", "run", "--detach", "--name", minio, "--network", network, "--network-alias", "minio", "--publish", "127.0.0.1::9000", "--env", "MINIO_ROOT_USER=objectlog", "--env", "MINIO_ROOT_PASSWORD=objectlog-local-test-secret", MINIO, "server", "/data")
        owned.append(minio)
        endpoint = "http://" + run("docker", "port", minio, "9000/tcp")
        for _ in range(100):
            try:
                http(endpoint + "/minio/health/ready")
                break
            except (OSError, urllib.error.URLError):
                time.sleep(.1)
        env = {**os.environ, "AWS_ACCESS_KEY_ID": "objectlog", "AWS_SECRET_ACCESS_KEY": "objectlog-local-test-secret", "AWS_DEFAULT_REGION": "us-east-1"}
        run("aws", "--endpoint-url", endpoint, "s3api", "create-bucket", "--bucket", "object-log-test", env=env)
        name, endpoint, record = start("empty-cache", LIMIT, "sha1", "cold-start")
        if record["ready"]:
            http(endpoint + "/repo/info/refs?service=git-upload-pack", {"Git-Protocol": "version=2"})
        finish(name, record)
        report["empty_cache_128m_startup_passed"] = record["ready"] and not record["state"]["OOMKilled"]
        # Compile cache setup is explicit and outside the measured 128 MiB runs.
        name, endpoint, record = start("cache-setup", 512 * 1024 * 1024, "sha1", "unused-setup")
        if not record["ready"]:
            finish(name, record)
            raise RuntimeError((output / "cache-setup/runtime.log").read_text())
        http(endpoint + "/repo/info/refs?service=git-upload-pack", {"Git-Protocol": "version=2"})
        finish(name, record)
        for object_format in ["sha1", "sha256"]:
            name, endpoint, record = start(object_format + "-warm", LIMIT, object_format, object_format)
            try:
                assert record["ready"], "warm-cache startup failed"
                with tempfile.TemporaryDirectory(prefix="object-log-linux-client-") as directory:
                    source = pathlib.Path(directory) / "source"
                    clone = pathlib.Path(directory) / "clone"
                    command_number = 0
                    def git(*arguments, cwd=source, check=True):
                        nonlocal command_number
                        command_number += 1
                        result = subprocess.run(["git", "-c", "commit.gpgsign=false", "-c", "gc.auto=0", "-c", "protocol.version=2", "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", *arguments], cwd=cwd, capture_output=True, text=True, env={**os.environ, "GIT_CONFIG_NOSYSTEM": "1", "GIT_CONFIG_GLOBAL": "/dev/null", "GIT_CONFIG_COUNT": "0", "GIT_TRACE_CURL": "1", "GIT_TRACE_CURL_NO_DATA": "1"})
                        (output / record["label"] / f"git-{command_number:02d}.log").write_text(repr(arguments) + "\n" + result.stdout + result.stderr)
                        if check and result.returncode != 0:
                            raise RuntimeError(result.stderr)
                        return result
                    git("init", "--object-format=" + object_format, "-b", "main", str(source), cwd=directory)
                    (source / "small").write_bytes(b"a" * 4096)
                    git("add", "."); git("commit", "-m", "first")
                    url = endpoint + "/repo"
                    git("push", url, "main")
                    git("clone", url, str(clone), cwd=directory)
                    (source / "large").write_bytes(random.Random(20260904).randbytes(8 * 1024 * 1024))
                    git("add", "."); git("commit", "-m", "eight MiB")
                    git("push", url, "main")
                    git("fetch", "origin", cwd=clone)
                    git("fsck", "--full", "--strict", cwd=clone)
                    expected = git("rev-parse", "HEAD").stdout.strip()
                    assert git("rev-parse", "origin/main", cwd=clone).stdout.strip() == expected
                    git("tag", "proof"); git("push", url, "refs/tags/proof")
                    git("push", url, ":refs/tags/proof")
                    # Keep the large object while exercising full history and external-base deltas.
                    mutable = bytearray(random.Random(384).randbytes(16 * 1024))
                    for revision in range(382):
                        mutable[:4] = revision.to_bytes(4, "little")
                        (source / "mutable").write_bytes(mutable)
                        git("add", "mutable"); git("commit", "-m", f"history {revision}")
                    assert git("rev-list", "--count", "HEAD").stdout.strip() == "384"
                    git("push", url, "main")
                    history_clone = pathlib.Path(directory) / "history-clone"
                    git("clone", url, str(history_clone), cwd=directory)
                    assert git("rev-list", "--count", "HEAD", cwd=history_clone).stdout.strip() == "384"
                    git("fsck", "--full", "--strict", cwd=history_clone)
                    base = git("rev-parse", "HEAD").stdout.strip()
                    mutable[100] ^= 1
                    (source / "mutable").write_bytes(mutable)
                    git("add", "mutable"); git("commit", "-m", "thin update")
                    # Independent Git fixture proves the intended incremental object set requires
                    # a base outside its pack. The HTTP push remains an unchanged Git command.
                    thin = subprocess.run(["git", "pack-objects", "--stdout", "--revs", "--thin"], input=f"HEAD\n^{base}\n".encode(), cwd=source, capture_output=True, check=True, env={**os.environ, "GIT_CONFIG_NOSYSTEM": "1", "GIT_CONFIG_GLOBAL": "/dev/null", "GIT_CONFIG_COUNT": "0"}).stdout
                    empty = pathlib.Path(directory) / "empty.git"
                    git("init", "--bare", "--object-format=" + object_format, str(empty), cwd=directory)
                    checked = subprocess.run(["git", "index-pack", "--stdin", "--fix-thin"], input=thin, cwd=empty, capture_output=True, env={**os.environ, "GIT_CONFIG_NOSYSTEM": "1", "GIT_CONFIG_GLOBAL": "/dev/null", "GIT_CONFIG_COUNT": "0"})
                    assert checked.returncode != 0 and b"unresolved delta" in checked.stderr, checked.stderr
                    (output / record["label"] / "thin-external-base.log").write_bytes(checked.stderr)
                    git("push", url, "main")
                    git("fetch", "origin", cwd=history_clone)
                    git("fsck", "--full", "--strict", cwd=history_clone)
                    expected = git("rev-parse", "HEAD").stdout.strip()
                    assert git("rev-parse", "origin/main", cwd=history_clone).stdout.strip() == expected
                    assert git("rev-list", "--count", "origin/main", cwd=history_clone).stdout.strip() == "385"
                    # Over-limit object rejection must leave the process and head intact.
                    with (source / "large").open("ab") as file: file.write(b"x")
                    git("add", "."); git("commit", "-m", "over object limit")
                    assert git("push", url, "main", check=False).returncode != 0
                    assert git("ls-remote", url, "refs/heads/main").stdout.split()[0] == expected
                    assert http(endpoint + "/.well-known/spin/health")
                    record["client_flow"] = "4KiB push/clone, 8MiB push, have-aware fetch, 384-commit push/clone, externally based thin update/fetch, fsck, tag create/delete, oversized rejection and head survival passed"
            finally:
                finish(name, record)
            assert record["memory_events"] is not None and record["memory_events"]["oom_kill"] == 0
            assert not record["state"]["OOMKilled"]
        report["warm_cache_128m_workloads_passed"] = True
        save()
    finally:
        failures = []
        for name in reversed(owned):
            subprocess.run(["docker", "rm", "--force", name], check=False, capture_output=True)
            if subprocess.run(["docker", "container", "inspect", name], capture_output=True).returncode == 0:
                failures.append(name)
        subprocess.run(["docker", "network", "rm", network], check=False, capture_output=True)
        if subprocess.run(["docker", "network", "inspect", network], capture_output=True).returncode == 0:
            failures.append(network)
        report["cleanup_remaining_resources"] = failures
        save()
        assert not failures, failures


if __name__ == "__main__":
    main()
