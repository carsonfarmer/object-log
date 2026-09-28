import contextlib
import http.client
import io
import json
from pathlib import Path
import tempfile
import time
import unittest
import urllib.error
from unittest.mock import patch

import maintenance


class MaintenanceTest(unittest.TestCase):
    def test_logical_once_then_physical_and_next_repository(self):
        calls = []
        states = iter(["more", "more", "complete", "complete"])

        def post(repo, operation, deadline):
            calls.append((repo, operation))
            return next(states)

        self.assertEqual(maintenance.maintain(["a.git", "team/b.git"], post), 0)
        self.assertEqual(calls, [
            ("a.git", "maintenance"), ("a.git", "collect"),
            ("a.git", "collect"), ("team/b.git", "maintenance"),
        ])

    def test_deferrals_do_not_loop_or_clear_readers(self):
        for state in ["conflict", "pending", "retained", "not materialized"]:
            with self.subTest(state=state):
                with patch("maintenance.Client.post", return_value=state) as post:
                    self.assertEqual(maintenance.maintain(["a.git", "b.git"], post), 0)
                    self.assertEqual(post.call_args_list, [
                        unittest.mock.call("a.git", "maintenance", unittest.mock.ANY),
                        unittest.mock.call("b.git", "maintenance", unittest.mock.ANY),
                    ])

    def test_failure_does_not_starve_other_repository_or_print_credentials(self):
        for failure in [ValueError("secret"),
                        urllib.error.HTTPError("https://host", 503, "secret", {}, None),
                        http.client.IncompleteRead(b"secret")]:
            with self.subTest(failure=type(failure)):
                with patch("maintenance.Client.post", side_effect=[failure, "complete"]) as post:
                    stderr = io.StringIO()
                    with contextlib.redirect_stderr(stderr):
                        self.assertEqual(maintenance.maintain(["a.git", "b.git"], post), 1)
                    self.assertEqual(post.call_count, 2)
                    self.assertNotIn("secret", stderr.getvalue())

    def test_token_reuse_renewal_and_exact_log_id(self):
        client = maintenance.Client({"client_id": "client", "token_url": "https://login/oauth2/token",
                                     "service_url": "https://git.example"}, "secret")
        responses = [
            {"access_token": "one", "expires_in": 100}, {"state": "more"},
            {"state": "complete"}, {"access_token": "two", "expires_in": 100},
            {"state": "complete"},
        ]
        with patch("maintenance.request_json", side_effect=responses) as request:
            with patch("maintenance.time.monotonic", side_effect=[0, 0, 10, 41, 41]):
                self.assertEqual(client.post("auto-123", "maintenance", 1000), "more")
                self.assertEqual(client.post("auto-123", "collect", 1000), "complete")
                self.assertEqual(client.post("b.git", "maintenance", 1000), "complete")
            sent = [call.args[0] for call in request.call_args_list]
            self.assertEqual(sent[0].get_header("Authorization"), "Basic Y2xpZW50OnNlY3JldA==")
            self.assertIn(b"git%2Fmaintenance", sent[0].data)
            self.assertEqual(sent[1].full_url, "https://git.example/_maintenance?log_id=auto-123&operation=maintenance")
            self.assertEqual(sent[2].get_header("Authorization"), "Bearer one")
            self.assertEqual(sent[4].get_header("Authorization"), "Bearer two")

    def test_only_service_404_means_unmaterialized(self):
        client = maintenance.Client({"client_id": "client", "token_url": "https://login/oauth2/token",
                                     "service_url": "https://git.example"}, "secret")
        missing = urllib.error.HTTPError("https://host", 404, "missing", {}, None)
        with patch("maintenance.request_json", side_effect=missing):
            with self.assertRaises(urllib.error.HTTPError):
                client.post("a.git", "maintenance", 1000)
        with patch("maintenance.request_json", side_effect=[{"access_token": "one", "expires_in": 100}, missing]):
            self.assertEqual(client.post("a.git", "maintenance", 1000), "not materialized")

    def test_response_bound_and_body_close(self):
        body = io.BytesIO(b"x" * 16385)
        with patch("urllib.request.OpenerDirector.open", return_value=body):
            with self.assertRaises(ValueError):
                maintenance.request_json(urllib.request.Request("https://git.example"), time.monotonic() + 30)
        self.assertTrue(body.closed)

    def test_redirect_is_not_followed(self):
        self.assertIsNone(maintenance.NoRedirect().redirect_request(None, None, 302, "", {}, "https://other"))

    def test_discovery_resumes_after_attempted_logs_and_wraps(self):
        config = {"wal_prefix": "git/", "wal_bucket": "bucket", "region": "us-west-2"}
        prefix = "git/v1/logs/"
        with tempfile.TemporaryDirectory() as directory:
            cursor = Path(directory) / "cursor"
            page = {"CommonPrefixes": [{"Prefix": prefix + value + "/"} for value in
                                       ["auto-first", "auto-second"]], "IsTruncated": True}
            with patch("maintenance.subprocess.check_output", return_value=json.dumps(page)) as listing:
                logs = maintenance.discover_logs(config, time.monotonic() + 10, cursor)
                self.assertEqual(next(logs), "auto-first")
                self.assertFalse(cursor.exists())  # Not advanced before this log is attempted.
                self.assertEqual(next(logs), "auto-second")
                self.assertEqual(cursor.read_text(), prefix + "auto-first/")
                logs.close()  # A run deadline can leave the second log unattempted.
                self.assertIn("32", listing.call_args.args[0])
            last_page = {"CommonPrefixes": [{"Prefix": prefix + "auto-second/"}]}
            with patch("maintenance.subprocess.check_output", return_value=json.dumps(last_page)) as listing:
                self.assertEqual(list(maintenance.discover_logs(config, time.monotonic() + 10, cursor)),
                                 ["auto-second"])
                self.assertEqual(listing.call_args.args[0][-2:], ["--start-after", prefix + "auto-first/"])
            self.assertFalse(cursor.exists())  # Revisit earlier logs on the next sweep.

    def test_discovery_skips_invalid_ids_without_stalling(self):
        config = {"wal_prefix": "", "wal_bucket": "bucket", "region": "us-west-2"}
        with tempfile.TemporaryDirectory() as directory:
            cursor = Path(directory) / "cursor"
            cursor.write_text("wrong-prefix/")
            page = {"CommonPrefixes": [{"Prefix": "v1/logs/../"},
                                       {"Prefix": "v1/logs/valid/"}], "IsTruncated": True}
            with patch("maintenance.subprocess.check_output", return_value=json.dumps(page)) as listing:
                self.assertEqual(list(maintenance.discover_logs(config, time.monotonic() + 10, cursor)),
                                 ["valid"])
                self.assertNotIn("--start-after", listing.call_args.args[0])
            self.assertEqual(cursor.read_text(), "v1/logs/valid/")

    def test_run_deadline_preserves_cursor_for_unattempted_log(self):
        config = {"wal_prefix": "git", "wal_bucket": "bucket", "region": "us-west-2"}
        page = {"CommonPrefixes": [{"Prefix": "git/v1/logs/first/"},
                                   {"Prefix": "git/v1/logs/second/"}]}
        with tempfile.TemporaryDirectory() as directory:
            cursor = Path(directory) / "cursor"
            calls = []
            deadline = time.monotonic() + 0.05

            def post(repo, operation, request_deadline):
                calls.append(repo)
                self.assertLessEqual(request_deadline, deadline)
                time.sleep(0.07)
                return "complete"

            with patch("maintenance.subprocess.check_output", return_value=json.dumps(page)):
                with contextlib.redirect_stderr(io.StringIO()):
                    self.assertEqual(maintenance.maintain(
                        maintenance.discover_logs(config, deadline, cursor), post,
                        run_deadline=deadline), 1)
            self.assertEqual(calls, ["first"])
            self.assertEqual(cursor.read_text(), "git/v1/logs/first/")

    def test_pause_resumes_collection_without_repeating_prune(self):
        with tempfile.TemporaryDirectory() as directory:
            marker = Path(directory) / "pause"
            states = iter(["retained", "conflict", "pending", "more", "complete"])
            operations = []

            def post(repo, operation, deadline):
                self.assertTrue(marker.exists())
                operations.append(operation)
                return next(states)

            with patch("maintenance.time.sleep"):
                self.assertEqual(maintenance.maintain(["a.git"], post, 5, 1, marker), 0)
            self.assertEqual(operations, ["maintenance", "collect", "collect", "collect", "collect"])
            self.assertFalse(marker.exists())

    def test_absolute_pause_deadline_interrupts_stall_and_removes_marker(self):
        with tempfile.TemporaryDirectory() as directory:
            marker = Path(directory) / "pause"
            calls = []

            def post(repo, operation, deadline):
                calls.append(repo)
                self.assertTrue(marker.exists())
                if repo == "a.git":
                    time.sleep(5)
                return "complete"

            start = time.monotonic()
            with contextlib.redirect_stderr(io.StringIO()):
                self.assertEqual(maintenance.maintain(["a.git", "b.git"], post, 5, 0.02, marker), 1)
            self.assertLess(time.monotonic() - start, 2)
            self.assertEqual(calls, ["a.git", "b.git"])
            self.assertFalse(marker.exists())


if __name__ == "__main__":
    unittest.main()
