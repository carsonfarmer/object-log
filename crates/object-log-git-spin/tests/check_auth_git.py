"""Opt-in both-hash Git credential-helper lifecycle against local Spin/MinIO."""
import base64
import json
import os
import pathlib
import secrets
import subprocess
import tempfile
import time

from check_auth import host


def private(path, text):
    path.touch(mode=0o600, exist_ok=True)
    path.write_text(text)


def main():
    env = os.environ.copy()
    for key in list(env):
        if key.startswith("GIT_TRACE") or key.startswith("GIT_CONFIG") or key == "GIT_CURL_VERBOSE":
            del env[key]
    env.update(GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL=os.devnull, GIT_TERMINAL_PROMPT="0", GIT_AUTHOR_NAME="Auth fixture", GIT_AUTHOR_EMAIL="auth@example.invalid", GIT_COMMITTER_NAME="Auth fixture", GIT_COMMITTER_EMAIL="auth@example.invalid")
    for object_format in ["sha1", "sha256"]:
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            reader, writer, rotated_reader, rotated_writer = [secrets.token_hex(32) for _ in range(4)]
            token_file = root / "token"
            helper = root / "helper.py"
            helper.write_text('''import os, pathlib, stat, sys
fields = dict(line.rstrip("\\n").split("=", 1) for line in sys.stdin if "=" in line)
if sys.argv[1] == "get":
    assert fields.get("protocol") == "http"
    assert fields.get("host") == os.environ["AUTH_HOST"]
    assert fields.get("path") == "repo"
    token = pathlib.Path(os.environ["AUTH_TOKEN_FILE"])
    assert stat.S_IMODE(token.stat().st_mode) == 0o600
    with open(os.environ["AUTH_HELPER_CALLS"], "a") as calls:
        calls.write("get\\n")
    print("username=git")
    print("password=" + token.read_text().strip())
    print()
''')
            env.update(AUTH_TOKEN_FILE=str(token_file), AUTH_HELPER_CALLS=str(root / "helper-calls"))

            def git(*args, cwd=None, denied=None):
                result = subprocess.run(["git", "-c", "protocol.version=2", "-c", "credential.helper=", "-c", f"credential.helper=!python3 {helper}", "-c", "credential.useHttpPath=true", *args], cwd=cwd, env=env, capture_output=True)
                for secret in [reader, writer, rotated_reader, rotated_writer]:
                    assert secret.encode() not in result.stdout + result.stderr, "credential leaked in Git output"
                    assert base64.b64encode(("git:" + secret).encode()) not in result.stdout + result.stderr, "encoded credential leaked in Git output"
                if denied:
                    expected = b"Authentication failed" if denied == 401 else b"403"
                    assert result.returncode != 0 and expected in result.stderr, "unexpected Git denial"
                elif result.returncode:
                    raise RuntimeError("Git fixture command failed: " + args[0] + ": " + result.stderr.decode(errors="replace"))
                return result.stdout.strip()

            variables = {key: env["OBJECT_LOG_MINIO_" + suffix] for key, suffix in [("endpoint", "ENDPOINT"), ("bucket", "BUCKET"), ("access_key", "ACCESS_KEY"), ("secret_key", "SECRET_KEY")]}
            variables.update(prefix="git-auth-" + secrets.token_hex(12), object_format=object_format, auth_read_token=reader, auth_write_token=writer)
            aws_env = env | dict(AWS_ACCESS_KEY_ID=variables["access_key"], AWS_SECRET_ACCESS_KEY=variables["secret_key"], AWS_DEFAULT_REGION="us-east-1", AWS_PAGER="")

            def aws(*args):
                result = subprocess.run(["aws", "--endpoint-url", variables["endpoint"], "s3api", *args], env=aws_env, capture_output=True)
                assert result.returncode == 0, "head snapshot failed"
                return json.loads(result.stdout)

            def head():
                objects = aws("list-objects-v2", "--bucket", variables["bucket"], "--prefix", variables["prefix"])
                keys = [obj["Key"] for obj in objects["Contents"] if obj["Key"].endswith("/index.cbor")]
                assert len(keys) == 1
                destination = root / "head.cbor"
                metadata = aws("get-object", "--bucket", variables["bucket"], "--key", keys[0], str(destination))
                return destination.read_bytes(), metadata["ETag"]

            source = root / "source"
            git("init", "--quiet", "-b", "main", "--object-format=" + object_format, str(source))
            (source / "file").write_text("first\n")
            git("add", "file", cwd=source)
            git("commit", "--quiet", "-m", "first", cwd=source)
            first = git("rev-parse", "HEAD", cwd=source)
            with host(variables, directory) as port:
                url = f"http://127.0.0.1:{port}/repo"
                env["AUTH_HOST"] = f"127.0.0.1:{port}"
                private(token_file, writer)
                git("push", "--quiet", url, "main", cwd=source)
                private(token_file, reader)
                clone = root / "clone"
                git("clone", "--quiet", url, str(clone))
                assert git("rev-parse", "HEAD", cwd=clone) == first
                before = head()
                git("push", "--quiet", url, "HEAD:refs/heads/denied", cwd=source, denied=403)
                assert head() == before, "reader rejection changed exact head"
                private(token_file, writer)
                (source / "file").write_text("second\n")
                git("commit", "--quiet", "-am", "second", cwd=source)
                git("push", "--quiet", url, "main", cwd=source)
                second = git("rev-parse", "HEAD", cwd=source)
                private(token_file, reader)
                git("fetch", "--quiet", cwd=clone)
                assert git("rev-parse", "origin/main", cwd=clone) == second
                git("fsck", "--strict", cwd=clone)
            # Stop the old host completely; the next host has no local repository.
            variables.update(auth_read_token=rotated_reader, auth_write_token=rotated_writer)
            with host(variables, directory) as port:
                url = f"http://127.0.0.1:{port}/repo"
                env["AUTH_HOST"] = f"127.0.0.1:{port}"
                before = head()
                for old in [reader, writer]:
                    private(token_file, old)
                    git("ls-remote", url, denied=401)
                    git("push", "--quiet", url, "main", cwd=source, denied=401)
                    assert head() == before, "revoked credential changed exact head"
                private(token_file, rotated_reader)
                cold = root / "cold"
                git("clone", "--quiet", url, str(cold))
                assert git("rev-parse", "HEAD", cwd=cold) == second
                git("fsck", "--strict", cwd=cold)
                private(token_file, rotated_writer)
                git("commit", "--quiet", "--allow-empty", "-m", "rotated", cwd=source)
                git("push", "--quiet", url, "main", cwd=source)
            variables["read_only"] = "true"
            with host(variables, directory) as port:
                env["AUTH_HOST"] = f"127.0.0.1:{port}"
                before = head()
                git("push", "--quiet", f"http://127.0.0.1:{port}/repo", "main", cwd=source, denied=403)
                assert head() == before, "read-only rejection changed exact head"
            assert len((root / "helper-calls").read_text().splitlines()) >= 10
            print(f"{object_format}: credential-helper push/clone/fetch, cold recovery, rotation, 403 policy, exact head preservation passed", flush=True)


if __name__ == "__main__":
    main()
