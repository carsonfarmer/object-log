"""Precompile this component into an explicit Wasmtime cache without touching S3.

Run on the deployment OS/architecture with its exact Spin binary, outside the
128 MiB serving cgroup. The generated config can then be passed to run.sh.
"""
import argparse
import json
import pathlib
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request

ROOT = pathlib.Path(__file__).resolve().parent


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=pathlib.Path, required=True)
    args = parser.parse_args()
    directory = args.directory.resolve()
    directory.mkdir(parents=True, exist_ok=True)
    config = directory / "wasmtime-cache.toml"
    contents = "[cache]\ndirectory = " + json.dumps(str(directory / "artifacts")) + "\n"
    if config.exists() and config.read_text() != contents:
        raise RuntimeError("existing cache configuration differs; choose another directory")
    config.write_text(contents)
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    with tempfile.TemporaryDirectory(prefix="object-log-spin-prewarm-") as state:
        log_path = pathlib.Path(state) / "runtime.log"
        with log_path.open("w") as log:
            process = subprocess.Popen([
                str(ROOT / "run.sh"), "--listen", f"127.0.0.1:{port}",
                "--cache", str(config), "--state-dir", state, "--log-dir", state,
                "--variable", "endpoint=http://127.0.0.1:1", "--variable", "bucket=unused",
                "--variable", "access_key=unused", "--variable", "secret_key=unused",
            ], stdout=log, stderr=subprocess.STDOUT)
            try:
                for _ in range(600):
                    try:
                        request = urllib.request.Request(f"http://127.0.0.1:{port}/repo/info/refs?service=git-upload-pack", headers={"Git-Protocol": "version=2"})
                        with urllib.request.urlopen(request, timeout=3) as response:
                            assert response.read().startswith(b"000eversion 2\n")
                        break
                    except urllib.error.URLError:
                        if process.poll() is not None:
                            raise RuntimeError(log_path.read_text())
                        time.sleep(.1)
                else:
                    raise RuntimeError("Spin cache setup did not complete within its startup deadline")
            finally:
                process.terminate()
                process.wait(timeout=10)
    print(f"Executable cache prepared. Serve the same component and Spin version with --cache {config}")


if __name__ == "__main__":
    main()
