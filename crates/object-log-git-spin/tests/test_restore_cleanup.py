"""Provider-free failure checks: python3 -m unittest discover -s crates/object-log-git-spin/tests -p test_restore_cleanup.py"""
import contextlib
import io
import unittest
from unittest.mock import patch

import check_restore as restore


class CleanupTests(unittest.TestCase):
    def test_both_prefixes_attempted_without_masking_primary_failure(self):
        for second in ("", RuntimeError("second cleanup failure")):
            with self.subTest(second=second):
                primary = RuntimeError("original test failure")
                with patch.object(restore, "external_minio", return_value=("http://127.0.0.1:9000", "fixture", "key", "secret")), \
                     patch.object(restore, "git", side_effect=primary), \
                     patch.object(restore, "run", side_effect=[RuntimeError("first cleanup failure"), second]) as run, \
                     patch.dict(restore.ENV), contextlib.redirect_stderr(io.StringIO()) as stderr:
                    with self.assertRaises(RuntimeError) as raised:
                        restore.main()
                self.assertIs(raised.exception, primary)
                self.assertEqual(run.call_count, 2)
                first, last = [call.args[0] for call in run.call_args_list]
                self.assertEqual(first[:5], ["aws", "--endpoint-url", "http://127.0.0.1:9000", "s3", "rm"])
                self.assertTrue(first[5].startswith("s3://fixture/restore-drill-"))
                self.assertEqual(last[5], first[5][:-1] + "-restored/")
                self.assertIn("first cleanup failure", stderr.getvalue())
                if isinstance(second, Exception):
                    self.assertIn("second cleanup failure", stderr.getvalue())

    def test_cleanup_failure_fails_an_otherwise_successful_drill(self):
        failure = RuntimeError("cleanup unavailable")
        with patch.object(restore, "run", side_effect=[failure, ""]) as run, \
             contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(RuntimeError) as raised:
                restore.cleanup(lambda *args: restore.run(list(args)), ("source", "restored"), None)
        self.assertIs(raised.exception.__cause__, failure)
        self.assertEqual(run.call_count, 2)


if __name__ == "__main__":
    unittest.main()
