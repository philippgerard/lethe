from __future__ import annotations

from dataclasses import dataclass

from lethe.notification_signals import (
    NotificationCategory,
    NotificationSource,
    NotificationUrgency,
    UserNotificationSignal,
)


@dataclass(frozen=True)
class NotificationAssessment:
    novelty: float
    urgency: float
    user_relevance: float
    interruptibility: float
    confidence: float


class NotificationScoring:
    """Deterministic first-pass scoring for user-notification candidates."""

    _URGENCY_SCORES = {
        NotificationUrgency.LOW: 0.25,
        NotificationUrgency.NORMAL: 0.55,
        NotificationUrgency.HIGH: 0.82,
        NotificationUrgency.CRITICAL: 0.98,
    }

    _RELEVANCE_SCORES = {
        NotificationCategory.STATUS: 0.35,
        NotificationCategory.INSIGHT: 0.30,
        NotificationCategory.UPDATE: 0.62,
        NotificationCategory.WARNING: 0.86,
        NotificationCategory.REMINDER: 0.88,
        NotificationCategory.ERROR: 0.95,
    }

    def assess(self, signal: UserNotificationSignal) -> NotificationAssessment:
        urgency = self._URGENCY_SCORES[signal.urgency]
        user_relevance = self._RELEVANCE_SCORES[signal.category]
        novelty = 0.50
        if signal.source == NotificationSource.DMN:
            novelty += 0.05
        if signal.category in (
            NotificationCategory.WARNING,
            NotificationCategory.REMINDER,
            NotificationCategory.ERROR,
        ):
            novelty += 0.15
        confidence = 0.90 if signal.metadata.get("signal_category") else 0.72
        interruptibility = min(1.0, (urgency * 0.6) + (user_relevance * 0.4))
        return NotificationAssessment(
            novelty=min(1.0, novelty),
            urgency=urgency,
            user_relevance=user_relevance,
            interruptibility=interruptibility,
            confidence=confidence,
        )
