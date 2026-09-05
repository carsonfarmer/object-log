"""Actual WASIp2/InMemory lifecycle in one invocation; no persistence claim."""
import json
import os
import pathlib
import struct
import subprocess
import tempfile
import time
import urllib.error
import urllib.request

ROOT = pathlib.Path(__file__).resolve().parent
ENV = {**os.environ, "GIT_CONFIG_NOSYSTEM": "1", "GIT_CONFIG_GLOBAL": "/dev/null",
       "GIT_AUTHOR_NAME": "Object Log", "GIT_AUTHOR_EMAIL": "object-log@example.invalid",
       "GIT_COMMITTER_NAME": "Object Log", "GIT_COMMITTER_EMAIL": "object-log@example.invalid",
       "GIT_AUTHOR_DATE": "2000-01-01T00:00:00Z", "GIT_COMMITTER_DATE": "2000-01-01T00:00:00Z"}
URL = "http://127.0.0.1:19176"


def git(path, *args, data=b""):
    result = subprocess.run(["git", *args], cwd=path, env=ENV, input=data, capture_output=True, check=False)
    if result.returncode:
        raise RuntimeError(f"git {args}: {result.stderr.decode(errors='replace')}")
    return result.stdout


def packet(line):
    return f"{len(line)+4:04x}".encode() + line


def upload(fmt, target, have=None):
    output = packet(b"command=fetch\n") + packet(f"object-format={fmt}\n".encode()) + b"0001"
    output += packet(f"want {target}\n".encode())
    if have:
        output += packet(f"have {have}\n".encode())
    return output + packet(b"done\n") + b"0000"


def receive(fmt, target, pack, expected=None):
    zero = "0" * len(target)
    command = f"{expected or zero} {target} refs/heads/main\0report-status object-format={fmt}\n".encode()
    return packet(command) + b"0000" + pack


def unpack(reply):
    cursor = 0
    raw = bytearray()
    pack_section = False
    while cursor < len(reply):
        length = int(reply[cursor:cursor+4], 16)
        cursor += 4
        if length <= 2:
            continue
        assert length >= 4 and cursor+length-4 <= len(reply)
        line = reply[cursor:cursor+length-4]
        cursor += length-4
        if line == b"packfile\n":
            pack_section = True
        elif pack_section:
            assert line[0] == 1
            raw.extend(line[1:])
    assert cursor == len(reply) and raw[:4] == b"PACK"
    assert len(raw) <= 9437184 and len(reply) <= 9437926
    return bytes(raw)


def frames(body):
    output = []
    cursor = 0
    while cursor < len(body):
        length, = struct.unpack_from(">I", body, cursor)
        cursor += 4
        assert cursor+length <= len(body)
        output.append(body[cursor:cursor+length])
        cursor += length
    assert cursor == len(body) and len(output) == 7
    return output


def init(path, fmt):
    path.mkdir()
    git(path, "init", "-q", "--bare", f"--object-format={fmt}")
    git(path, "config", "pack.threads", "1")


def check_pack(directory, source, fmt, target, pack, seed=b"", have=None):
    standalone = directory / "standalone"
    init(standalone, fmt)
    git(standalone, "index-pack", "--stdin", data=pack)  # rejects every external delta base
    index, = (standalone / "objects/pack").glob("*.idx")
    actual = {line.split()[0] for line in git(standalone, "verify-pack", "-v", str(index)).decode().splitlines()
              if len(line.split()[0]) == len(target) and all(c in "0123456789abcdef" for c in line.split()[0])}
    revisions = target + "\n" + (f"^{have}\n" if have else "")
    expected = {line.split()[0] for line in git(source, "rev-list", "--objects", "--stdin", data=revisions.encode()).decode().splitlines()}
    assert actual == expected, (len(actual), len(expected))
    receiver = directory / "receiver"
    init(receiver, fmt)
    if seed:
        git(receiver, "index-pack", "--stdin", "--strict", data=seed)
    args = ["index-pack", "--stdin", "--strict"]
    if not seed:
        args.append("--check-self-contained-and-connected")
    git(receiver, *args, data=pack)
    git(receiver, "update-ref", "refs/heads/main", target)
    git(receiver, "fsck", "--strict", "--no-progress")
    return len(actual)


def fixture(directory, fmt, label):
    source = directory / "source"
    source.mkdir()
    git(source, "init", "-q", "-b", "main", f"--object-format={fmt}")
    git(source, "config", "pack.threads", "1")
    size = 8*1024*1024 if label == "8mib" else 4096
    # Same byte generator on every host, with no seeded-library-version dependency.
    value = 17
    content = bytearray(size)
    mask = (1 << 64) - 1
    for i in range(size):
        value ^= (value << 13) & mask
        value ^= value >> 7
        value ^= (value << 17) & mask
        content[i] = value & 255
    count = 384 if label == "history-thin" else 1
    tip = None
    for i in range(count):
        content[:8] = struct.pack("<Q", i)
        (source / "file").write_bytes(content)
        git(source, "add", "file")
        tree = git(source, "write-tree").decode().strip()
        parent = ["-p", tip] if tip else []
        tip = git(source, "commit-tree", tree, *parent, data=f"commit {i}\n".encode()).decode().strip()
    base = tip
    seed = git(source, "pack-objects", "--stdout", "--revs", data=(base+"\n").encode())
    thin = b""
    if label == "history-thin":
        content[16:24] = struct.pack("<Q", 999)
        (source / "file").write_bytes(content)
        git(source, "add", "file")
        tree = git(source, "write-tree").decode().strip()
        tip = git(source, "commit-tree", tree, "-p", base, data=b"incremental\n").decode().strip()
        thin = git(source, "pack-objects", "--stdout", "--revs", "--thin", data=f"{tip}\n^{base}\n".encode())
        empty = directory / "thin-check"
        init(empty, fmt)
        rejected = subprocess.run(["git", "index-pack", "--stdin"], cwd=empty, env=ENV, input=thin, capture_output=True)
        assert rejected.returncode and b"unresolved delta" in rejected.stderr
    git(source, "update-ref", "refs/heads/main", tip)
    assert int(git(source, "rev-list", "--count", tip)) == count + bool(thin)
    pieces = [receive(fmt, base, seed), upload(fmt, base),
              receive(fmt, tip, thin, base) if thin else b"", upload(fmt, tip),
              upload(fmt, tip, base) if thin else b"", receive(fmt, tip, git(source, "pack-objects", "--stdout"))]
    body = b"OLM1" + bytes([1 if fmt == "sha1" else 2]) + b"".join(struct.pack(">I", len(p))+p for p in pieces)
    assert len(body) <= 10*1024*1024
    return source, base, tip, seed, thin, body


def run():
    assert git(ROOT, "--version").startswith(b"git version 2.54.")
    with tempfile.TemporaryDirectory(prefix="object-log-wasip2-memory-") as temp:
        temp = pathlib.Path(temp)
        with (temp / "runtime.log").open("w") as log:
            process = subprocess.Popen(["spin", "up", "--from", str(ROOT / "memory.toml"),
                                        "--listen", "127.0.0.1:19176", "--max-instance-memory", "134217728"],
                                       env={**os.environ, "SPIN_MAX_INSTANCE_COUNT": "1", "SPIN_WASMTIME_INSTANCE_COUNT": "1", "SPIN_WASMTIME_POOLING": "1"},
                                       stdout=log, stderr=subprocess.STDOUT)
            try:
                for _ in range(100):
                    try:
                        urllib.request.urlopen(URL+"/.well-known/spin/health", timeout=5).close()
                        break
                    except urllib.error.URLError:
                        if process.poll() is not None:
                            raise RuntimeError((temp / "runtime.log").read_text())
                        time.sleep(.1)
                else:
                    raise RuntimeError("Spin startup timeout")
                for fmt in ["sha1", "sha256"]:
                    for label in ["4kib", "8mib", "history-thin"]:
                        directory = temp / f"{fmt}-{label}"
                        directory.mkdir()
                        source, base, tip, seed, thin, body = fixture(directory, fmt, label)
                        start = time.monotonic()
                        try:
                            with urllib.request.urlopen(urllib.request.Request(URL+"/", data=body), timeout=120) as response:
                                replies = frames(response.read(20*1024*1024+1))
                        except urllib.error.HTTPError as error:
                            raise RuntimeError(f"{fmt}/{label}: {error.read()!r}\n{(temp / 'runtime.log').read_text()}") from error
                        elapsed = time.monotonic()-start
                        assert b"unpack ok" in replies[0] and b"ok refs/heads/main" in replies[0]
                        assert b"ng refs/heads/main" in replies[4]
                        assert replies[6].startswith(b"checkpoint and collection passed:")
                        packs = [("initial", base, replies[1], b"", None), ("recovered", tip, replies[5], b"", None)]
                        if thin:
                            assert b"unpack ok" in replies[2] and b"ok refs/heads/main" in replies[2]
                            packs.append(("incremental", tip, replies[3], seed, base))
                        else:
                            assert replies[2] == replies[3] == b""
                        counts = {}
                        for name, target, reply, seed_pack, have in packs:
                            checked = directory / name
                            checked.mkdir()
                            counts[name] = check_pack(checked, source, fmt, target, unpack(reply), seed_pack, have)
                        print(json.dumps({"hash":fmt, "fixture":label, "request_bytes":len(body), "response_frame_bytes":[len(r) for r in replies],
                                          "elapsed_seconds":elapsed, "objects":counts, "gc":replies[6].decode().strip(),
                                          "instance_limit_bytes":134217728}), flush=True)
            finally:
                process.terminate()
                process.wait(timeout=10)


if __name__ == "__main__":
    run()
