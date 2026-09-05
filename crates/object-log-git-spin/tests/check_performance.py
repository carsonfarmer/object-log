"""Opt-in paired WASI command/Git pack+ref measurements; retains the native oracle."""
import json
import math
import platform
import hashlib
import os
import pathlib
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request

import check_memory as m


def verify(directory, fixture, fmt, replies):
    source, base, tip, seed, thin, _ = fixture
    assert b"unpack ok" in replies[0] and b"ok refs/heads/main" in replies[0]
    assert b"ng refs/heads/main" in replies[4]
    assert replies[6].startswith(b"checkpoint and collection passed:")
    packs = [("initial", base, replies[1], b"", None),
             ("recovered", tip, replies[5], b"", None)]
    if thin:
        assert b"unpack ok" in replies[2] and b"ok refs/heads/main" in replies[2]
        packs.append(("incremental", tip, replies[3], seed, base))
    else:
        assert replies[2] == replies[3] == b""
    for name, target, reply, seed_pack, have in packs:
        checked = directory / name
        checked.mkdir()
        m.check_pack(checked, source, fmt, target, m.unpack(reply), seed_pack, have)


def oracle(directory, fixture, fmt, case):
    source, base, tip, seed, thin, _ = fixture
    incremental = case in ("incremental-fetch", "thin-push")
    target = tip if incremental else base
    if case.endswith("push"):
        receiver = directory / "oracle"
        m.init(receiver, fmt)
        if incremental:
            m.git(receiver, "index-pack", "--stdin", "--strict", data=seed)
            m.git(receiver, "update-ref", "refs/heads/main", base)
        args = ["index-pack", "--stdin", "--strict"] + (["--fix-thin"] if incremental else [])
        start = time.monotonic_ns()
        m.git(receiver, *args, data=thin if incremental else seed)
        m.git(receiver, "update-ref", "refs/heads/main", target)
        elapsed = time.monotonic_ns() - start
        m.git(receiver, "fsck", "--strict", "--no-progress")
        assert m.git(receiver, "rev-parse", "refs/heads/main").decode().strip() == target
        return {"elapsed_ns": elapsed, "pack_bytes": len(thin if incremental else seed)}
    revisions = target + "\n" + (f"^{base}\n" if incremental else "")
    start = time.monotonic_ns()
    pack = m.git(source, "pack-objects", "--stdout", "--revs", data=revisions.encode())
    elapsed = time.monotonic_ns() - start
    checked = directory / "oracle"
    checked.mkdir()
    m.check_pack(checked, source, fmt, target, pack, seed if incremental else b"", base if incremental else None)
    return {"elapsed_ns": elapsed, "pack_bytes": len(pack)}


def candidate(url, directory, fixture, fmt, case):
    start = time.monotonic_ns()
    with urllib.request.urlopen(urllib.request.Request(url + "/performance", data=fixture[-1]), timeout=180) as response:
        body = response.read(20*1024*1024+1)
    http_ns = time.monotonic_ns() - start
    replies = m.frames(body, 8)
    measurements = json.loads(replies[7])
    key = {"thin-push": "thin_push_ns", "incremental-fetch": "incremental_fetch_ns"}.get(
        case, "initial_push_ns" if case.endswith("push") else "initial_fetch_ns")
    verify(directory, fixture, fmt, replies)
    frame = 3 if case == "incremental-fetch" else 1
    return {"elapsed_ns": measurements[key], "http_lifecycle_ns": http_ns,
            "guest_stages_ns": {k: v for k, v in measurements.items() if k != "io"},
            "guest_io": measurements["io"][{"initial_push_ns": 0, "initial_fetch_ns": 1, "thin_push_ns": 2, "incremental_fetch_ns": 3}[key]], "response_bytes": len(body),
            "framed_fetch_bytes": len(replies[frame]) if case.endswith("fetch") else None,
            "pack_bytes": len(m.unpack(replies[frame])) if case.endswith("fetch") else len(fixture[4] if case == "thin-push" else fixture[3])}


def percentile(values, quantile):
    return sorted(values)[math.ceil(len(values)*quantile)-1]


def run():
    assert m.git(m.ROOT, "--version").startswith(b"git version 2.54.")
    output = pathlib.Path(os.environ.get("OBJECT_LOG_SPIN_PERFORMANCE_OUTPUT", str(pathlib.Path(tempfile.gettempdir()) / "object-log-spin-performance.jsonl")))
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w") as raw, tempfile.TemporaryDirectory(prefix="object-log-spin-performance-") as temporary:
        temp = pathlib.Path(temporary)
        def record(value):
            line = json.dumps(value, sort_keys=True)
            raw.write(line + "\n")
            raw.flush()
            if value["kind"] != "sample":
                print(line, flush=True)
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            port = listener.getsockname()[1]
        url = f"http://127.0.0.1:{port}"
        record({"kind": "conditions", "git": m.git(m.ROOT, "--version").decode().strip(),
                "spin": subprocess.check_output(["spin", "--version"], text=True).strip(),
                "revision": m.git(m.ROOT, "rev-parse", "HEAD").decode().strip(),
                "source_diff_sha256": hashlib.sha256(m.git(m.ROOT, "diff", "HEAD")).hexdigest(),
                "machine": platform.platform(), "processor": platform.processor(),
                "driver_sha256": hashlib.sha256(pathlib.Path(__file__).read_bytes()).hexdigest(),
                "runtime_configuration": "Spin defaults", "warmups": 1, "initial_pairs": 10, "escalated_pairs": 30,
                "candidate_scope": "Guest common command: repository open, log reads/writes, graph/pack/ref work; InMemory provider. Whole fresh lifecycle HTTP and runtime startup recorded separately. No transport or JIT in guest timer.",
                "oracle_scope": "Native Git subprocess pack-objects fetch or strict index-pack plus update-ref push; fixture creation, seed import and verification excluded. Filesystem provider; no log work.",
                "io_scope": "Each guest command timer includes repository open and measured InMemory GET/PUT including body consumption; calls and combined payload bytes exclude bootstrap and verification. Serial depth is longest nonoverlapping interval chain, not a causal DAG or remote latency.",
                "qualification": "Installed-Git comparison using Spin defaults; startup includes compilation/cache costs without attribution."})
        startup = time.monotonic_ns()
        with (temp / "runtime.log").open("w") as log:
            process = subprocess.Popen(["spin", "up", "--from", str(m.ROOT / "memory.toml"), "--listen", f"127.0.0.1:{port}"],
                env=os.environ.copy(), stdout=log, stderr=subprocess.STDOUT)
            try:
                for _ in range(600):
                    try:
                        urllib.request.urlopen(url + "/.well-known/spin/health", timeout=1).close()
                        break
                    except urllib.error.URLError:
                        if process.poll() is not None:
                            raise RuntimeError((temp / "runtime.log").read_text())
                        time.sleep(.1)
                else:
                    raise RuntimeError("Spin startup timeout")
                record({"kind": "startup", "elapsed_ns": time.monotonic_ns()-startup})
                first_request = True
                for fmt in ("sha1", "sha256"):
                    fixtures = {}
                    for label, case in (("4kib", "4kib-push"), ("4kib", "4kib-fetch"), ("8mib", "8mib-push"), ("8mib", "8mib-fetch"), ("history", "384-fetch"), ("history-thin", "incremental-fetch"), ("history-thin", "thin-push")):
                        if label not in fixtures:
                            directory = temp / f"{fmt}-{label}"
                            directory.mkdir()
                            fixtures[label] = m.fixture(directory, fmt, label)
                        fixture = fixtures[label]
                        pairs = []
                        target_pairs = 10
                        sample = 0
                        while sample <= target_pairs:
                            with tempfile.TemporaryDirectory(dir=temp, prefix="pair-") as pair_dir:
                                directory = pathlib.Path(pair_dir)
                                results = {}
                                order = ("git", "spin") if sample % 2 == 0 else ("spin", "git")
                                for engine in order:
                                    results[engine] = oracle(directory, fixture, fmt, case) if engine == "git" else candidate(url, directory, fixture, fmt, case)
                                    record({"kind": "sample", "hash": fmt, "case": case, "sample": sample, "warmup": sample == 0,
                                            "fixture_sha256": hashlib.sha256(fixture[-1]).hexdigest(),
                                            "engine": engine, "first_runtime_request": engine == "spin" and first_request, **results[engine]})
                                    if engine == "spin":
                                        first_request = False
                                if case.endswith("fetch") and case in ("8mib-fetch", "incremental-fetch"):
                                    assert results["spin"]["pack_bytes"] <= results["git"]["pack_bytes"] * 1.10
                                if sample:
                                    pairs.append(results)
                                if sample == 10:
                                    ratios = [percentile([p["spin"]["elapsed_ns"] for p in pairs], q) / percentile([p["git"]["elapsed_ns"] for p in pairs], q) for q in (.5, .95)]
                                    if max(ratios) > 1.25:
                                        target_pairs = 30
                                        record({"kind": "escalation", "hash": fmt, "case": case, "ratios": ratios, "pairs": 30})
                            sample += 1
                        summary = {engine: {name: percentile([p[engine]["elapsed_ns"] for p in pairs], q) for name, q in (("p50_ns", .5), ("p95_ns", .95))} for engine in ("git", "spin")}
                        ratios = {name: summary["spin"][name] / summary["git"][name] for name in ("p50_ns", "p95_ns")}
                        record({"kind": "summary", "hash": fmt, "case": case, "pairs": len(pairs), "timings": summary, "ratios": ratios, "owner_review_required": target_pairs == 30 or max(ratios.values()) > 1.25})
            finally:
                process.terminate()
                process.wait(timeout=10)
                output.with_suffix(".runtime.log").write_text((temp / "runtime.log").read_text())


if __name__ == "__main__":
    run()
