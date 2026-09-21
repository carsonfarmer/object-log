#!/usr/bin/python3
"""One logical pass per repository, then resume only physical collection."""

import base64
import contextlib
import json
from pathlib import Path
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def request_json(request, deadline):
    # A Git maintenance request may use the service's default five-minute budget.
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("maintenance budget exhausted")
    with urllib.request.build_opener(NoRedirect).open(request, timeout=min(330, remaining)) as response:
        data = response.read(16385)
        if len(data) > 16384:
            raise ValueError("oversized maintenance response")
        return json.loads(data)


class Client:
    def __init__(self, config, secret):
        self.config = config
        self.basic = base64.b64encode(f'{config["client_id"]}:{secret}'.encode()).decode()
        self.token = ""
        self.expires = 0

    def post(self, repository, operation, deadline):
        if time.monotonic() >= self.expires:
            result = request_json(urllib.request.Request(
                self.config["token_url"],
                data=b"grant_type=client_credentials&scope=git%2Faccess+git%2Fmaintenance",
                headers={"Authorization": f"Basic {self.basic}",
                         "Content-Type": "application/x-www-form-urlencoded"},
            ), deadline)
            self.token = result["access_token"]
            self.expires = time.monotonic() + int(result["expires_in"]) - 60
        # Canonical repository paths can contain URL-safe punctuation. Encoding
        # those characters would create aliases rejected by the service router.
        name = urllib.parse.quote(repository, safe="/:@!$&'()*+,;=-._~")
        url = f'{self.config["service_url"]}/{name}/{operation}'
        try:
            return request_json(urllib.request.Request(
                url, data=b"", headers={"Authorization": f"Bearer {self.token}"},
            ), deadline)["state"]
        except urllib.error.HTTPError as error:
            # Configured repositories are materialized by their first writer.
            if error.code == 404:
                return "not materialized"
            raise


@contextlib.contextmanager
def time_budget(seconds):
    # urllib's timeout bounds socket inactivity, not the complete response.
    # This worker runs on the main thread on Linux; an alarm bounds the whole pass.
    def expired(signum, frame):
        raise TimeoutError("maintenance budget exhausted")

    previous = signal.signal(signal.SIGALRM, expired)
    signal.setitimer(signal.ITIMER_REAL, seconds)
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)


def maintain(repositories, post, budget_seconds=300, pause_seconds=0,
             marker=Path("/run/object-log-maintenance/pause")):
    failed = False
    run_deadline = time.monotonic() + budget_seconds * len(repositories)
    for repository in repositories:
        try:
            seconds = min(pause_seconds or budget_seconds, run_deadline - time.monotonic())
            if seconds <= 0:
                raise TimeoutError("run budget exhausted")
            deadline = time.monotonic() + seconds
            if pause_seconds:
                marker.touch()
            with time_budget(seconds):
                operation = "maintenance"
                while True:
                    if time.monotonic() >= deadline:
                        raise TimeoutError("repository budget exhausted")
                    state = post(repository, operation, deadline)
                    if state == "more":
                        operation = "collect"
                        continue
                    if state in {"retained", "conflict", "pending"}:
                        if state == "retained":
                            operation = "collect"
                        if pause_seconds:
                            time.sleep(min(1, max(0, deadline - time.monotonic())))
                            continue
                    elif state not in {"complete", "not materialized"}:
                        raise ValueError("unknown maintenance state")
                    break
            print(f"{repository}: {state}", flush=True)
        except urllib.error.HTTPError as error:
            print(f"{repository}: HTTP {error.code}", file=sys.stderr, flush=True)
            failed = True
        except (OSError, ValueError, KeyError) as error:
            # Avoid logging token responses or credential-bearing request objects.
            print(f"{repository}: {type(error).__name__}", file=sys.stderr, flush=True)
            failed = True
        finally:
            if pause_seconds:
                marker.unlink(missing_ok=True)
    return 1 if failed else 0


def main():
    with open(sys.argv[1], encoding="utf-8") as source:
        config = json.load(source)
    secret = subprocess.check_output([
        "aws", "--region", config["region"], "ssm", "get-parameter",
        "--name", config["secret_parameter"], "--with-decryption",
        "--query", "Parameter.Value", "--output", "text",
    ], text=True, timeout=60).strip()
    return maintain(config["repositories"], Client(config, secret).post,
                    config["budget_seconds"], config["pause_seconds"])


if __name__ == "__main__":
    sys.exit(main())
