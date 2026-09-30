import contextlib
import io
import json
import time
import unittest
from unittest.mock import patch

import run


class FakeResponse(io.BytesIO):
    headers = {"Content-Type": "text/event-stream"}


class FakeOpener:
    def __init__(self, events):
        self.events = events
        self.requests = []

    def open(self, request, timeout):
        self.requests.append(request)
        return FakeResponse(self.events)


class RunnerTests(unittest.TestCase):
    def test_preview_never_reads_token_or_opens_network(self):
        original_get = run.os.environ.get

        def guard_token(key, default=None):
            if key == "LETHE_API_TOKEN":
                raise AssertionError("token read")
            return original_get(key, default)

        with patch.object(run.os.environ, "get", side_effect=guard_token):
            with patch.object(run, "build_opener", side_effect=AssertionError("network")):
                with contextlib.redirect_stdout(io.StringIO()) as output:
                    self.assertEqual(run.main([]), 0)
        self.assertIn("Preview only", output.getvalue())

    def test_sse_handles_comments_and_multiline_json(self):
        response = FakeResponse(b': keepalive\n\nevent: text\ndata: {"content":\ndata: "observed"}\n\n')
        events = list(run.sse_events(response, time.monotonic() + 1))
        self.assertEqual(events, [("text", {"content": "observed"})])

    def test_result_omits_secret_events_and_tool_previews(self):
        opener = FakeOpener(
            b'event: secure_input.request\ndata: {"server_pub":"private-canary"}\n\n'
            b'event: tool.start\ndata: {"name":"alien_browser_act","args_preview":"private-canary"}\n\n'
            b'event: tool.end\ndata: {"output_preview":"private-canary"}\n\n'
            b'event: text\ndata: {"content":"Verified heading"}\n\n'
            b'event: done\ndata: {}\n\n'
        )
        case = next(case for case in run.read_cases() if case["id"] == "public-heading")
        result = run.run_case(opener, "http://127.0.0.1:8080", "secret-token", case, None, 1, 123, 0)
        self.assertEqual(result["status"], "turn_completed")
        self.assertEqual(result["judgment"], "pending_manual_review")
        self.assertEqual(result["tool_counts"], {"alien_browser_act": 1})
        self.assertNotIn("private-canary", json.dumps(result))
        self.assertNotIn("secret-token", json.dumps(result))
        self.assertEqual(len(opener.requests), 1)

    def test_stream_end_is_incomplete_and_not_retried(self):
        opener = FakeOpener(b'event: text\ndata: {"content":"Working"}\n\n')
        case = next(case for case in run.read_cases() if case["id"] == "public-heading")
        result = run.run_case(opener, "http://localhost:8080", "test-token", case, None, 1, 123, 0)
        self.assertEqual(result["status"], "incomplete")
        self.assertEqual(len(opener.requests), 1)

    def test_api_rejects_insecure_remote_or_credential_urls(self):
        for url in ("http://example.com", "https://user:pass@example.com", "https://example.com?token=x"):
            with self.assertRaises(ValueError):
                run.validate_url(url, api=True)
        self.assertEqual(run.validate_url("http://127.0.0.1:8080/", api=True), "http://127.0.0.1:8080")

    def test_redirect_is_never_followed(self):
        self.assertIsNone(run.NoRedirects().redirect_request(None, None, 302, "", {}, "https://other.example"))


if __name__ == "__main__":
    unittest.main()
