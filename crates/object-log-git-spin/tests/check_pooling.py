"""Opt-in local MinIO pooling diagnostic. Saves failures; does not retry requests.

Run with --wasm pointing to the release Git component. Each POST runs a fresh
component instance and exercises capability validation and an existing log's
conditional head create. This isolates transport/bootstrap from Git pack work.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import socket
import subprocess
import time
import urllib.error
import urllib.request
import uuid

MINIO = "minio/minio@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e"
ROOT = Path(__file__).resolve().parents[1]


def run(*args, **kwargs):
    return subprocess.check_output(args, text=True, **kwargs).strip()


def packet(data):
    return f"{len(data)+4:04x}".encode() + data


def build_sdk_probe(output):
    """Only the published SDK and anyhow; no custom transport or object log."""
    project = output / "sdk-probe"
    (project / "src").mkdir(parents=True, exist_ok=True)
    (project / "Cargo.toml").write_text('''[package]
name = "pooling-sdk-probe"
version = "0.1.0"
edition = "2024"
[workspace]
[lib]
crate-type = ["cdylib"]
[dependencies]
spin-sdk = { version = "=5.2.0", default-features = false }
anyhow = "1"
''')
    (project / "src/lib.rs").write_text('''use spin_sdk::http::{Method, Request, RequestBuilder, Response};
#[spin_sdk::http_component]
async fn handle(_: Request) -> anyhow::Result<Response> {
    let endpoint = spin_sdk::variables::get("endpoint")?;
    let prefix = spin_sdk::variables::get("prefix")?;
    let url = format!("{endpoint}/object-log-test/{prefix}");
    for _ in 0..5 {
        let put = RequestBuilder::new(Method::Put, &url)
            .header("if-none-match", "*")
            .header("content-length", "15")
            .body("transport-probe")
            .build();
        let response: Response = spin_sdk::http::send(put).await?;
        anyhow::ensure!([200, 412].contains(response.status()), "PUT: {}", response.status());
        let response: Response = spin_sdk::http::send(Request::new(Method::Get, &url)).await?;
        anyhow::ensure!(*response.status() == 200 && response.body() == b"transport-probe", "GET failed");
    }
    Ok(Response::new(200, "0000"))
}
''')
    lock = ROOT.parents[1] / "docs/evidence/spin-pooling-2026-09-05/sdk-Cargo.lock"
    (project / "Cargo.lock").write_bytes(lock.read_bytes())
    subprocess.run(["cargo", "build", "--locked", "--release", "--target", "wasm32-wasip2", "--manifest-path", str(project / "Cargo.toml")], check=True)
    return project / "target/wasm32-wasip2/release/pooling_sdk_probe.wasm"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wasm", type=Path)
    parser.add_argument("--sdk-only", action="store_true", help="Build a minimal SDK-only PUT/GET component with no object-log code")
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--attempts", type=int, default=1000)
    args = parser.parse_args()
    if args.attempts < 1:
        parser.error("--attempts must be positive")
    args.output.mkdir(parents=True, exist_ok=True)
    output = args.output.resolve()
    if args.sdk_only:
        args.wasm = build_sdk_probe(output)
    if args.wasm is None:
        parser.error("--wasm or --sdk-only is required")
    manifest = (ROOT / "spin.toml").read_text().replace(
        '../../target/wasm32-wasip2/release/object_log_git_spin.wasm', str(args.wasm.resolve()))
    (output / "spin.toml").write_text(manifest)
    name = f"object-log-pooling-{uuid.uuid4()}"
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        listen = f"127.0.0.1:{listener.getsockname()[1]}"
    report = {"spin": run("spin", "--version"), "minio_image": MINIO,
              "wasm_sha256": hashlib.sha256(args.wasm.read_bytes()).hexdigest(), "sdk_only": args.sdk_only, "runs": []}
    run("docker", "run", "--detach", "--rm", "--name", name, "--publish", "127.0.0.1::9000",
        "--env", "MINIO_ROOT_USER=objectlog", "--env", "MINIO_ROOT_PASSWORD=objectlog-local-test-secret", MINIO, "server", "/data")
    try:
        endpoint = "http://" + run("docker", "port", name, "9000/tcp")
        for _ in range(100):
            try:
                urllib.request.urlopen(endpoint + "/minio/health/ready", timeout=1).read()
                break
            except (urllib.error.URLError, OSError):
                time.sleep(.1)
        env = {**os.environ, "AWS_ACCESS_KEY_ID": "objectlog", "AWS_SECRET_ACCESS_KEY": "objectlog-local-test-secret", "AWS_DEFAULT_REGION": "us-east-1"}
        run("aws", "--endpoint-url", endpoint, "s3api", "create-bucket", "--bucket", "object-log-test", env=env)
        if args.sdk_only:
            policy = {"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Principal": "*", "Action": ["s3:GetObject", "s3:PutObject"], "Resource": ["arn:aws:s3:::object-log-test/*"]}]}
            run("aws", "--endpoint-url", endpoint, "s3api", "put-bucket-policy", "--bucket", "object-log-test", "--policy", json.dumps(policy), env=env)
        for pooled in [True, False]:
            (output / "runtime.toml").write_text(f"[outbound_http]\nconnection_pooling = {str(pooled).lower()}\n")
            log_path = output / f"pooled-{str(pooled).lower()}.log"
            with log_path.open("w") as log:
                command = ["spin", "up", "--from", str(output / "spin.toml"), "--runtime-config-file", str(output / "runtime.toml"),
                           "--listen", listen, "--max-instance-memory", "134217728"]
                for key, value in {"endpoint": endpoint, "bucket": "object-log-test", "access_key": "objectlog", "secret_key": "objectlog-local-test-secret", "prefix": f"pooling-{pooled}"}.items():
                    command += ["--variable", f"{key}={value}"]
                process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT,
                    env={**os.environ, "SPIN_MAX_INSTANCE_COUNT": "1", "SPIN_WASMTIME_INSTANCE_COUNT": "1", "SPIN_WASMTIME_POOLING": "1", "RUST_LOG": "spin_factor_outbound_http=debug"})
                try:
                    for _ in range(100):
                        try:
                            urllib.request.urlopen(f"http://{listen}/.well-known/spin/health", timeout=1).read()
                            break
                        except (urllib.error.URLError, OSError):
                            if process.poll() is not None:
                                raise RuntimeError(log_path.read_text())
                            time.sleep(.1)
                    request = packet(b"command=ls-refs\n") + packet(b"object-format=sha1\n") + b"00010000"
                    failures = []
                    for iteration in range(args.attempts):
                        try:
                            req = urllib.request.Request(f"http://{listen}/repo/git-upload-pack", data=request,
                                headers={"Content-Type": "application/x-git-upload-pack-request", "Git-Protocol": "version=2"})
                            with urllib.request.urlopen(req, timeout=45) as response:
                                body = response.read()
                            assert body == b"0000", body
                        except Exception as error:
                            failure = {"iteration": iteration, "error": str(error)}
                            if isinstance(error, urllib.error.HTTPError):
                                failure["body"] = error.read().decode(errors="replace")
                            failures.append(failure)
                            if isinstance(error, TimeoutError) or len(failures) >= 10:
                                break
                    report["runs"].append({"pooling": pooled, "attempts": iteration + 1, "failures": failures})
                    (output / "result.json").write_text(json.dumps(report, indent=2) + "\n")
                    print(json.dumps({"pooling": pooled, "attempts": iteration + 1, "failures": len(failures)}), flush=True)
                finally:
                    process.terminate()
                    process.wait(timeout=10)
        (output / "minio.log").write_text(run("docker", "logs", name))
    finally:
        run("docker", "rm", "--force", name)


if __name__ == "__main__":
    main()
