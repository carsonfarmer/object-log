"""Exercise the actual component's HTTP boundary without storage access."""
import json
import pathlib
import itertools
import subprocess
import tempfile
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


policies = [(None, None), (None, "true"), (None, "invalid"), ("true", "true"), ("true", "invalid"), ("invalid", None)]
for object_format, (read_only, allow_rewrite) in itertools.product(["sha1", "sha256"], policies):
    with tempfile.TemporaryDirectory() as directory, (ROOT / "tests/http-runtime.log").open("w") as log:
        variables = {
            "endpoint": "http://127.0.0.1:19173",
            "bucket": "unused",
            "access_key": "fixture-access",
            "secret_key": "fixture-secret",
            "object_format": object_format,
            "auth_mode": "disabled",
        }
        if read_only is not None:
            variables["read_only"] = read_only
        if allow_rewrite is not None:
            variables["allow_non_fast_forward"] = allow_rewrite
        config = pathlib.Path(directory) / "repository.toml"
        config.write_text("".join(f"{key} = {json.dumps(value)}\n" for key, value in variables.items()))
        command = [
            "spin", "up", "--from", str(ROOT / "spin.toml"), "--listen", "127.0.0.1:19174",
            "--variable", "@" + str(config),
        ]
        process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT)
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
            if read_only is not None or allow_rewrite == "invalid":
                expected = 403 if read_only == "true" and allow_rewrite != "invalid" else 500
                expected_body = b"access forbidden\n" if expected == 403 else b"Git request failed\n"
                result = request("/repo/info/refs?service=git-receive-pack")
                assert result[:2] == (expected, expected_body), result
                for headers in [{}, {"Content-Type": "application/x-git-receive-pack-request"}]:
                    for command_body in [b"0000", b"invalid push"]:
                        status, body, _ = request("/repo/git-receive-pack", headers, command_body)
                        assert (status, body) == (expected, expected_body), (status, body)
                if read_only == "invalid" or allow_rewrite == "invalid":
                    result = request("/repo/info/refs?service=git-upload-pack", {"Git-Protocol": "version=2"})
                    assert result[:2] == (500, expected_body), result
                    print(object_format + ": invalid boolean policy fails closed")
                    continue
            status, body, headers = request("/repo/info/refs?service=git-upload-pack", {"Git-Protocol": "version=2"})
            assert status == 200 and body.startswith(b"000eversion 2\n") and body.endswith(b"0000"), (status, body)
            assert ("object-format=" + object_format).encode() in body
            assert headers["Content-Type"] == "application/x-git-upload-pack-advertisement"
            for encoding in ["GZIP", "Identity"]:
                result = request("/repo/info/refs?service=git-upload-pack", {"Git-Protocol": "version=2", "Content-Encoding": encoding})
                assert result[0] == 200, result
            assert request("/repo/info/refs?service=git-upload-pack")[0] == 400
            assert request("/repo/git-upload-pack", {"Git-Protocol": "version=2", "Content-Type": "text/plain"}, b"0000")[0] == 400
            assert request("/repo/unknown")[0] == 404
            print(f"{object_format} read_only={read_only or 'default'} allow_non_fast_forward={allow_rewrite or 'default'}: actual component discovery and HTTP rejection passed without a provider")
        finally:
            process.terminate()
            process.wait(timeout=10)
