from lethe.notification_gate import GateAction, NotificationGate
from lethe.notification_scoring import NotificationScoring
from lethe.notification_signals import (
    NotificationCategory,
    NotificationOrigin,
    NotificationSource,
    NotificationUrgency,
    UserNotificationSignal,
)


def _signal(
    *,
    source: NotificationSource,
    origin: NotificationOrigin,
    category: NotificationCategory,
    urgency: NotificationUrgency,
    content: str,
    kind: str,
) -> UserNotificationSignal:
    return UserNotificationSignal(
        event_id="sig-1",
        source=source,
        source_name=source.value,
        source_actor_id="actor-1",
        origin=origin,
        category=category,
        urgency=urgency,
        content=content,
        kind=kind,
    )


def test_notification_gate_hushes_startup_status():
    gate = NotificationGate()
    scoring = NotificationScoring()
    signal = _signal(
        source=NotificationSource.BRAINSTEM,
        origin=NotificationOrigin.STARTUP,
        category=NotificationCategory.STATUS,
        urgency=NotificationUrgency.LOW,
        content="Lethe restarted after a clean shutdown.",
        kind="brainstem_restart",
    )

    decision = gate.decide(signal, scoring.assess(signal))

    assert decision.action == GateAction.DROP
    assert decision.reason == "startup_status_hushed"


def test_notification_gate_reviews_urgent_deadline_signal():
    gate = NotificationGate()
    scoring = NotificationScoring()
    signal = _signal(
        source=NotificationSource.DMN,
        origin=NotificationOrigin.REFLECTION,
        category=NotificationCategory.REMINDER,
        urgency=NotificationUrgency.HIGH,
        content="Urgent deadline tomorrow.",
        kind="deadline",
    )

    decision = gate.decide(signal, scoring.assess(signal))

    assert decision.action == GateAction.REVIEW
    assert decision.reason == "needs_review"
