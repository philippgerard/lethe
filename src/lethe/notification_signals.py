from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Mapping


class NotificationSource(str, Enum):
    BRAINSTEM = "brainstem"
    DMN = "dmn"
    SUBAGENT = "subagent"
    UNKNOWN = "unknown"


class NotificationOrigin(str, Enum):
    STARTUP = "startup"
    HEARTBEAT = "heartbeat"
    BACKGROUND = "background"
    REFLECTION = "reflection"
    TASK = "task"


class NotificationCategory(str, Enum):
    STATUS = "status"
    WARNING = "warning"
    REMINDER = "reminder"
    UPDATE = "update"
    INSIGHT = "insight"
    ERROR = "error"


class NotificationUrgency(str, Enum):
    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass(frozen=True)
class UserNotificationSignal:
    """Typed internal signal that may become user-visible speech."""

    event_id: str
    source: NotificationSource
    source_name: str
    source_actor_id: str
    origin: NotificationOrigin
    category: NotificationCategory
    urgency: NotificationUrgency
    content: str
    kind: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)
    observed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


def notification_source_from_name(name: str) -> NotificationSource:
    lowered = (name or "").strip().lower()
    if lowered == NotificationSource.BRAINSTEM.value:
        return NotificationSource.BRAINSTEM
    if lowered == NotificationSource.DMN.value:
        return NotificationSource.DMN
    if lowered:
        return NotificationSource.SUBAGENT
    return NotificationSource.UNKNOWN


def notification_origin_from_metadata(
    metadata: Mapping[str, Any],
    source: NotificationSource,
    kind: str,
) -> NotificationOrigin:
    explicit = str(metadata.get("signal_origin", "")).strip().lower()
    if explicit:
        for option in NotificationOrigin:
            if option.value == explicit:
                return option

    kind_lower = (kind or "").strip().lower()
    if "restart" in kind_lower:
        return NotificationOrigin.STARTUP
    if source == NotificationSource.DMN:
        return NotificationOrigin.REFLECTION
    if "heartbeat" in kind_lower:
        return NotificationOrigin.HEARTBEAT
    return NotificationOrigin.BACKGROUND


def notification_category_from_metadata(
    metadata: Mapping[str, Any],
    kind: str,
) -> NotificationCategory:
    explicit = str(metadata.get("signal_category", "")).strip().lower()
    if explicit:
        for option in NotificationCategory:
            if option.value == explicit:
                return option

    kind_lower = (kind or "").strip().lower()
    if "warning" in kind_lower or "alert" in kind_lower:
        return NotificationCategory.WARNING
    if "deadline" in kind_lower or "reminder" in kind_lower:
        return NotificationCategory.REMINDER
    if "update" in kind_lower:
        return NotificationCategory.UPDATE
    if "error" in kind_lower or "fatal" in kind_lower:
        return NotificationCategory.ERROR
    if "insight" in kind_lower or "idea" in kind_lower:
        return NotificationCategory.INSIGHT
    return NotificationCategory.STATUS


def notification_urgency_from_metadata(
    metadata: Mapping[str, Any],
    category: NotificationCategory,
    kind: str,
) -> NotificationUrgency:
    explicit = str(metadata.get("signal_urgency", "")).strip().lower()
    if explicit:
        for option in NotificationUrgency:
            if option.value == explicit:
                return option

    kind_lower = (kind or "").strip().lower()
    if category == NotificationCategory.ERROR:
        return NotificationUrgency.CRITICAL
    if category in (NotificationCategory.WARNING, NotificationCategory.REMINDER):
        return NotificationUrgency.HIGH
    if category == NotificationCategory.UPDATE:
        return NotificationUrgency.NORMAL
    if "restart" in kind_lower:
        return NotificationUrgency.LOW
    if category == NotificationCategory.INSIGHT:
        return NotificationUrgency.LOW
    return NotificationUrgency.NORMAL
