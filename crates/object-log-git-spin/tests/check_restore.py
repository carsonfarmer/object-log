"""Opt-in offline backup/restore drill; requires an existing loopback MinIO.

Build the release component and operator first. Uses the same OBJECT_LOG_MINIO_*
variables as check_partial.py. Only fresh random fixture prefixes are removed.
"""
import json
import os
import pathlib
import secrets
import sys
import tempfile

from check_auth import host
from check_partial import ENV, ROOT, external_minio, git, run


def cleanup(aws, urls, primary_error):
    failures = []
    for url in urls:
        try:
            aws("s3", "rm", url, "--recursive", "--only-show-errors")
        except Exception as error:
            failures.append(error)
            print(f"Fixture cleanup failed for {url}: {error}", file=sys.stderr)
    if failures and primary_error is None:
        raise RuntimeError("fixture cleanup failed") from failures[0]


def main():
    external = external_minio()
    if external is None:
        raise ValueError("set OBJECT_LOG_MINIO_* for an existing loopback MinIO")
    endpoint, bucket, access_key, secret_key = external
    ENV.update(AWS_ACCESS_KEY_ID=access_key, AWS_SECRET_ACCESS_KEY=secret_key,
               AWS_PAGER="", GIT_TERMINAL_PROMPT="0")
    for key in list(ENV):
        if key.startswith(("GIT_TRACE", "GIT_CONFIG")) or key == "GIT_CURL_VERBOSE":
            del ENV[key]
    ENV.update(GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL=os.devnull)
    operator = ROOT.parents[1] / "target/release/object-log-git-maintain"

    def aws(*args):
        return run(["aws", "--endpoint-url", endpoint, *args])

    for name in ("sha1", "sha256"):
        prefix = "restore-drill-" + secrets.token_hex(12)
        restored_prefix = prefix + "-restored"
        source_url = f"s3://{bucket}/{prefix}/"
        restored_url = f"s3://{bucket}/{restored_prefix}/"
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            config = root / "operator.toml"
            variables = dict(endpoint=endpoint, bucket=bucket, access_key=access_key,
                             secret_key=secret_key, prefix=prefix, object_format=name,
                             auth_mode="disabled")

            def maintain(*args):
                config.touch(mode=0o600, exist_ok=True)
                config.write_text("".join(f"{k} = {json.dumps(v)}\n" for k, v in variables.items()))
                return json.loads(run([str(operator), "--config", str(config), *args]))

            def files(path):
                return {str(p.relative_to(path)): p.read_bytes()
                        for p in path.rglob("*") if p.is_file()}

            try:
                source = root / "source"
                git(root, "init", "--quiet", "-b", "main", "--object-format=" + name, str(source))
                (source / "file").write_text("backup contents\n")
                git(source, "add", ".")
                git(source, "commit", "--quiet", "-m", "backup")
                git(source, "tag", "-a", "v1", "-m", "backup tag")
                with host(variables, directory) as port:
                    url = f"http://127.0.0.1:{port}/repo"
                    git(source, "push", "--quiet", url, "main", "--tags")
                # Every serving process is stopped before maintenance or copying.
                maintain("set-default-branch", "--expected", "refs/heads/main",
                         "--target", "refs/heads/trunk", "--recovery-file", str(root / "default.token"))
                maintain("checkpoint", "--retain-packs")
                before = maintain("status")
                backup = root / "backup"
                aws("s3", "sync", source_url, str(backup), "--only-show-errors")
                snapshot = files(backup)
                assert any(k.endswith("/index.cbor") for k in snapshot)
                # Restore to a never-served prefix, preserving every relative key.
                aws("s3", "sync", str(backup), restored_url, "--only-show-errors")
                verification = root / "verification"
                aws("s3", "sync", restored_url, str(verification), "--only-show-errors")
                assert files(verification) == snapshot, "restore changed durable bytes"
                # Prove recovery does not fall back to the original namespace.
                aws("s3", "rm", source_url, "--recursive", "--only-show-errors")
                variables["prefix"] = restored_prefix
                assert maintain("status") == before, "restored head metadata differs"
                with host(variables, directory) as port:
                    url = f"http://127.0.0.1:{port}/repo"
                    git(root, "ls-remote", "--symref", url)
                    clone = root / "cold"
                    git(root, "clone", "--quiet", "--no-checkout", url, str(clone))
                    assert git(clone, "symbolic-ref", "HEAD") == "refs/heads/trunk"
                    assert git(clone, "rev-parse", "refs/remotes/origin/main") == git(source, "rev-parse", "main")
                    assert git(clone, "rev-parse", "refs/tags/v1") == git(source, "rev-parse", "v1")
                    assert git(clone, "show", "origin/main:file") == "backup contents"
                    git(clone, "fsck", "--strict", "--no-reflogs")
                    git(source, "commit", "--quiet", "--allow-empty", "-m", "after restore")
                    git(source, "push", "--quiet", url, "main")
                    git(clone, "fetch", "--quiet", "origin")
                    assert git(clone, "rev-parse", "origin/main") == git(source, "rev-parse", "main")
                print(f"{name}: offline snapshot, byte-exact restore, cold refs/tag/unborn HEAD, fsck and new push passed", flush=True)
            finally:
                cleanup(aws, (source_url, restored_url), sys.exc_info()[1])


if __name__ == "__main__":
    main()
