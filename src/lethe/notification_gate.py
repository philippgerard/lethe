from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum

from lethe.notification_scoring import NotificationAssessment
from lethe.notification_signals import (
    NotificationCategory,
    NotificationOrigin,
    NotificationUrgency,
    UserNotificationSignal,
)


class GateAction(str, Enum):
    DROP = "drop"
    REVIEW = "review"


@dataclass(frozen=True)
class GateDecision:
    action: GateAction
    reason: str


class NotificationGate:
    """Deterministic go/no-go gate before expensive LLM review."""

    def __init__(self, dedupe_window_seconds: int = 900):
        self.dedupe_window_seconds = dedupe_window_seconds
        self._recent_signatures: dict[str, float] = {}

    def decide(
        self,
        signal: UserNotificationSignal,
        assessment: NotificationAssessment,
    ) -> GateDecision:
        now = datetime.now(timezone.utc).timestamp()
        self._prune(now)
        signature = self._signature(signal)
        if signature in self._recent_signatures:
            return GateDecision(GateAction.DROP, "duplicate_signal")

        if signal.origin == NotificationOrigin.STARTUP and signal.category == NotificationCategory.STATUS:
            self._remember(signature, now)
            return GateDecision(GateAction.DROP, "startup_status_hushed")

        if (
            signal.category in (NotificationCategory.STATUS, NotificationCategory.INSIGHT)
            and signal.urgency == NotificationUrgency.LOW
        ):
            self._remember(signature, now)
            return GateDecision(GateAction.DROP, "low_priority_status")

        if assessment.interruptibility < 0.58 and assessment.user_relevance < 0.60:
            self._remember(signature, now)
            return GateDecision(GateAction.DROP, "insufficient_interruptibility")

        self._remember(signature, now)
        return GateDecision(GateAction.REVIEW, "needs_review")

    def _signature(self, signal: UserNotificationSignal) -> str:
        preview = " ".join(signal.content.lower().split())[:160]
        return "|".join(
            [
                signal.source.value,
                signal.origin.value,
                signal.category.value,
                signal.urgency.value,
                signal.kind.lower(),
                preview,
            ]
        )

    def _prune(self, now: float) -> None:
        expired = [
            signature
            for signature, seen_at in self._recent_signatures.items()
            if (now - seen_at) > self.dedupe_window_seconds
        ]
        for signature in expired:
            del self._recent_signatures[signature]

    def _remember(self, signature: str, now: float) -> None:
        self._recent_signatures[signature] = now
