from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Awaitable, Callable

from lethe.notification_signals import UserNotificationSignal
from lethe.prompts import load_prompt_template
from lethe.utils import normalize_user_visible_message, strip_model_tags


NOTIFICATION_REVIEW_PROMPT = load_prompt_template(
    "notification_review",
    fallback=(
        "You review user-facing notification candidates.\n\n"
        "Decide whether the following internal signal should become a user-visible message right now.\n"
        "Return JSON only with this exact shape:\n"
        "{{\"send\": true|false, \"text\": \"...\"}}\n\n"
        "Return send=false when the signal is primarily:\n"
        "- an internal status update\n"
        "- a background thought or reflection\n"
        "- commentary about waiting for the user's reply\n"
        "- meta commentary about DMN, brainstem, cortex, escalation, or internal reports\n"
        "- a note that nothing needs action or nothing changed\n\n"
        "If send=true:\n"
        "- write a direct message to the user\n"
        "- keep it to 1-2 sentences\n"
        "- do not mention DMN, brainstem, cortex, internal reports, or escalation\n"
        "- do not narrate your internal state\n"
        "- use plain first-person assistant language if needed\n\n"
        "SIGNAL:\n"
        "{signal}\n\n"
        "RECENT CONTEXT:\n"
        "{context}\n"
    ),
)


@dataclass(frozen=True)
class SpeechAct:
    send: bool
    text: str = ""


async def review_user_notification(
    signal: UserNotificationSignal,
    recent_context: str,
    complete_fn: Callable[[str], Awaitable[str]],
) -> SpeechAct:
    signal_block = json.dumps(
        {
            "source": signal.source.value,
            "origin": signal.origin.value,
            "category": signal.category.value,
            "urgency": signal.urgency.value,
            "kind": signal.kind,
            "content": signal.content,
        },
        ensure_ascii=True,
        indent=2,
    )
    prompt = NOTIFICATION_REVIEW_PROMPT.format(
        signal=signal_block,
        context=recent_context.strip() or "No recent user context.",
    )
    raw = await complete_fn(prompt)
    return _parse_speech_act(raw)


def _parse_speech_act(raw: str) -> SpeechAct:
    cleaned = strip_model_tags(raw or "").strip()
    if not cleaned:
        return SpeechAct(send=False, text="")

    parsed = _extract_json_object(cleaned)
    if not isinstance(parsed, dict):
        return SpeechAct(send=False, text="")

    send = bool(parsed.get("send"))
    text = normalize_user_visible_message(str(parsed.get("text", ""))) if send else ""
    if not send or not text:
        return SpeechAct(send=False, text="")
    return SpeechAct(send=True, text=text)


def _extract_json_object(text: str):
    try:
        return json.loads(text)
    except Exception:
        pass

    match = re.search(r"\{.*\}", text, flags=re.DOTALL)
    if not match:
        return None
    try:
        return json.loads(match.group(0))
    except Exception:
        return None
