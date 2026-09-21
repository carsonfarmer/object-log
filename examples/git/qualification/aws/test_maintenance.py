import contextlib
import io
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
        for failure in [ValueError("secret"), urllib.error.HTTPError("https://host", 503, "secret", {}, None)]:
            with self.subTest(failure=type(failure)):
                with patch("maintenance.Client.post", side_effect=[failure, "complete"]) as post:
                    stderr = io.StringIO()
                    with contextlib.redirect_stderr(stderr):
                        self.assertEqual(maintenance.maintain(["a.git", "b.git"], post), 1)
                    self.assertEqual(post.call_count, 2)
                    self.assertNotIn("secret", stderr.getvalue())

    def test_token_reuse_renewal_and_exact_repository_path(self):
        client = maintenance.Client({"client_id": "client", "token_url": "https://login/oauth2/token",
                                     "service_url": "https://git.example"}, "secret")
        responses = [
            {"access_token": "one", "expires_in": 100}, {"state": "more"},
            {"state": "complete"}, {"access_token": "two", "expires_in": 100},
            {"state": "complete"},
        ]
        with patch("maintenance.request_json", side_effect=responses) as request:
            with patch("maintenance.time.monotonic", side_effect=[0, 0, 10, 41, 41]):
                self.assertEqual(client.post("R&D/lib+client@v2.git", "maintenance", 1000), "more")
                self.assertEqual(client.post("R&D/lib+client@v2.git", "collect", 1000), "complete")
                self.assertEqual(client.post("b.git", "maintenance", 1000), "complete")
            sent = [call.args[0] for call in request.call_args_list]
            self.assertEqual(sent[0].get_header("Authorization"), "Basic Y2xpZW50OnNlY3JldA==")
            self.assertIn(b"git%2Fmaintenance", sent[0].data)
            self.assertEqual(sent[1].full_url, "https://git.example/R&D/lib+client@v2.git/maintenance")
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
