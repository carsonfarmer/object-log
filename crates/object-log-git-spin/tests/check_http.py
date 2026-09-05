"""Exercise the actual component's HTTP boundary without storage access."""
import pathlib
import subprocess
import time
import urllib.error
import urllib.request

ROOT = pathlib.Path(__file__).resolve().parent.parent
URL = "http://127.0.0.1:19174"


def request(path, headers=None, data=None):
    try:
        with urllib.request.urlopen(urllib.request.Request(URL + path, data=data, headers=headers or {}), timeout=5) as response:
            return response.status, response.read(), response.headers
    except urllib.error.HTTPError as error:
        return error.code, error.read(), error.headers


for object_format in ["sha1", "sha256"]:
    with (ROOT / "tests/http-runtime.log").open("w") as log:
        process = subprocess.Popen([
            str(ROOT / "run.sh"), "--listen", "127.0.0.1:19174",
            "--variable", "endpoint=http://127.0.0.1:19173",
            "--variable", "bucket=unused", "--variable", "access_key=fixture-access",
            "--variable", "secret_key=fixture-secret",
            "--variable", "object_format=" + object_format,
        ], stdout=log, stderr=subprocess.STDOUT)
        try:
            for attempt in range(100):
                try:
                    assert request("/.well-known/spin/health")[0] == 200
                    break
                except urllib.error.URLError:
                    if process.poll() is not None:
                        raise RuntimeError((ROOT / "tests/http-runtime.log").read_text())
                    time.sleep(.1)
            else:
                raise RuntimeError("Spin failed to start")
            status, body, headers = request("/repo/info/refs?service=git-upload-pack", {"Git-Protocol": "version=2"})
            assert status == 200 and body.startswith(b"000eversion 2\n") and body.endswith(b"0000"), (status, body)
            assert ("object-format=" + object_format).encode() in body
            assert headers["Content-Type"] == "application/x-git-upload-pack-advertisement"
            assert request("/repo/info/refs?service=git-upload-pack")[0] == 400
            assert request("/repo/git-upload-pack", {"Git-Protocol": "version=2", "Content-Type": "text/plain"}, b"0000")[0] == 400
            assert request("/repo/unknown")[0] == 404
            print(object_format + ": actual component discovery and HTTP rejection passed without a provider")
        finally:
            process.terminate()
            process.wait(timeout=10)
