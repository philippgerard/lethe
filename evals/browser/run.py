#!/usr/bin/env python3
"""Opt-in browser cases through Lethe's authenticated /chat SSE API."""

import argparse
from collections import Counter
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener
import uuid


CASES_PATH = Path(__file__).with_name("cases.json")
MAX_EVENT_BYTES = 1024 * 1024
MAX_TEXT_CHARACTERS = 256 * 1024


class NoRedirects(HTTPRedirectHandler):
    """Never forward the API bearer token to a redirected endpoint."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def read_cases():
    payload = json.loads(CASES_PATH.read_text(encoding="utf-8"))
    if payload.get("schema_version") != 1:
        raise ValueError("Unsupported browser case schema")
    cases = payload["cases"]
    ids = [case["id"] for case in cases]
    if len(set(ids)) != len(ids):
        raise ValueError("Browser case IDs must be unique")
    return cases


def validate_url(value, api=False):
    parsed = urlsplit(value)
    if (
        parsed.scheme not in ("http", "https")
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("URLs must use http(s), without credentials, query, or fragment")
    if api and parsed.scheme == "http" and parsed.hostname not in (
        "localhost", "127.0.0.1", "::1"
    ):
        raise ValueError("A non-loopback API URL must use HTTPS")
    return value.rstrip("/")


def sse_events(response, deadline):
    event = "message"
    data = []
    size = 0
    while True:
        if time.monotonic() >= deadline:
            raise TimeoutError("Browser evaluation exceeded its time budget")
        raw = response.readline(MAX_EVENT_BYTES + 1)
        if not raw:
            return
        size += len(raw)
        if size > MAX_EVENT_BYTES:
            raise ValueError("SSE event exceeded the size limit")
        line = raw.decode("utf-8").rstrip("\r\n")
        if not line:
            if data:
                yield event, json.loads("\n".join(data))
            event, data, size = "message", [], 0
        elif line.startswith("event:"):
            event = line[6:].lstrip(" ")
        elif line.startswith("data:"):
            data.append(line[5:].lstrip(" "))


def run_case(opener, api_url, token, case, fixture_url, timeout, chat_id, user_id):
    prompt = case["prompt"]
    if "fixture_url" in case["required_inputs"]:
        prompt = prompt.replace("{fixture_url}", fixture_url)
    prompt += (
        "\nComplete this browser assignment before your final result. If a worker "
        "is still running or a human is required, report the incomplete outcome "
        "and blocker; an acknowledgement is not a successful benchmark result."
    )
    request = Request(
        api_url + "/chat",
        data=json.dumps({
            "message": prompt,
            "user_id": user_id,
            "chat_id": chat_id,
            "metadata": {"browser_benchmark_case": case["id"]},
        }).encode("utf-8"),
        headers={
            "Authorization": "Bearer " + token,
            "Content-Type": "application/json",
            "Accept": "text/event-stream",
        },
        method="POST",
    )
    started = time.monotonic()
    counts = Counter()
    tool_counts = Counter()
    messages = []
    deltas = []
    text_size = 0
    usage = []
    completed = False
    error = None
    try:
        with opener.open(request, timeout=timeout) as response:
            if "text/event-stream" not in response.headers.get("Content-Type", ""):
                raise ValueError("Expected an SSE response from /chat")
            for event, payload in sse_events(response, started + timeout):
                if not isinstance(payload, dict):
                    raise ValueError("Expected an object in the SSE event")
                counts[event] += 1
                if event == "tool.start":
                    tool_counts[str(payload.get("name", "unknown"))] += 1
                elif event in ("text", "assistant.delta"):
                    content = payload.get("content", "")
                    if isinstance(content, str):
                        text_size += len(content)
                        if text_size > MAX_TEXT_CHARACTERS:
                            raise ValueError("Assistant text exceeded the size limit")
                        (messages if event == "text" else deltas).append(content)
                elif event == "usage":
                    # Lethe currently publishes context usage, not provider cost.
                    tokens = payload.get("prompt_tokens")
                    if isinstance(tokens, int):
                        usage.append({"prompt_tokens": tokens})
                elif event == "done":
                    completed = True
                    break
    except HTTPError as failure:
        error = "HTTP " + str(failure.code) + "; no automatic retry was attempted"
    except (URLError, TimeoutError, OSError, ValueError) as failure:
        error = type(failure).__name__ + "; no automatic retry was attempted"
    return {
        "case_id": case["id"],
        "chat_id": chat_id,
        "status": "turn_completed" if completed else "incomplete",
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "assistant_text": "\n\n".join(messages) if messages else "".join(deltas),
        "event_counts": dict(counts),
        "tool_counts": dict(tool_counts),
        "usage": usage,
        "error": error,
        "judgment": "pending_manual_review",
        "success_criteria": case["success_criteria"],
        "expected_evidence": case["expected_evidence"],
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", action="store_true", help="Explicitly send selected cases to Lethe")
    parser.add_argument("--case", action="append", default=[], help="Case ID to select; repeat for more")
    parser.add_argument("--api-url", help="Dedicated Lethe test instance; HTTPS except loopback")
    parser.add_argument("--fixture-url", help="Fixture URL reachable from the Lethe browser")
    parser.add_argument("--repetitions", type=int, default=1)
    parser.add_argument("--timeout", type=int, default=300, help="Per-request seconds; no automatic retry")
    parser.add_argument("--user-id", type=int, default=0, help="Synthetic test user on the dedicated instance")
    parser.add_argument("--output", type=Path, help="New result file; existing files are never overwritten")
    args = parser.parse_args(argv)
    cases = read_cases()
    by_id = {case["id"]: case for case in cases}
    unknown = sorted(set(args.case) - set(by_id))
    if unknown:
        parser.error("Unknown cases: " + ", ".join(unknown))
    selected = [by_id[case_id] for case_id in dict.fromkeys(args.case)] if args.case else cases
    if not args.run:
        for case in selected:
            print(f"{case['id']}: {case['name']} ({case['effect_scope']})")
        print("Preview only. No API request was sent. Add --run and explicit --case selections to execute.")
        return 0
    if not args.case or not args.api_url:
        parser.error("--run requires explicit --case selections and --api-url")
    if args.repetitions < 1 or args.timeout < 1:
        parser.error("Repetitions and timeout must be positive")
    token = os.environ.get("LETHE_API_TOKEN", "")
    if not token or "\n" in token or "\r" in token:
        parser.error("Set a valid LETHE_API_TOKEN in the environment; no credential files are read")
    if any("fixture_url" in case["required_inputs"] for case in selected) and not args.fixture_url:
        parser.error("Selected fixture cases require --fixture-url")
    try:
        api_url = validate_url(args.api_url, api=True)
        fixture_url = validate_url(args.fixture_url) if args.fixture_url else None
    except ValueError as failure:
        parser.error(str(failure))
    output = args.output or Path("target/browser-evals") / (str(uuid.uuid4()) + ".json")
    output.parent.mkdir(parents=True, exist_ok=True)
    report = {
        "schema_version": 1,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "case_ids": [case["id"] for case in selected],
        "results": [],
    }
    # Reserve the result path before sending anything, without truncating old evidence.
    with output.open("x", encoding="utf-8") as artifact:
        opener = build_opener(NoRedirects())
        for repetition in range(1, args.repetitions + 1):
            for case in selected:
                chat_id = uuid.uuid4().int % (2**53 - 1) + 1
                result = run_case(opener, api_url, token, case, fixture_url, args.timeout, chat_id, args.user_id)
                result["repetition"] = repetition
                report["results"].append(result)
                artifact.seek(0)
                json.dump(report, artifact, indent=2, ensure_ascii=False)
                artifact.truncate()
                artifact.flush()
                print(f"{case['id']} trial {repetition}: {result['status']}, {result['elapsed_seconds']}s, manual review pending")
                if result["status"] != "turn_completed":
                    print("Stopped after an incomplete stream. Inspect the test instance before any rerun.", file=sys.stderr)
                    print("Results: " + str(output.resolve()))
                    return 1
    print("Results: " + str(output.resolve()))
    return 0


if __name__ == "__main__":
    sys.exit(main())
