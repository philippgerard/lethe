from __future__ import annotations

from lethe.actor import ActorEvent, ActorRegistry
from lethe.notification_signals import (
    NotificationSource,
    UserNotificationSignal,
    notification_category_from_metadata,
    notification_origin_from_metadata,
    notification_source_from_name,
    notification_urgency_from_metadata,
)


class NotificationRouter:
    """Turns actor user_notify events into typed notification signals."""

    BACKGROUND_SOURCES = {
        NotificationSource.BRAINSTEM,
        NotificationSource.DMN,
    }

    def __init__(self, registry: ActorRegistry, principal_id: str):
        self.registry = registry
        self.principal_id = principal_id

    def from_actor_event(self, event: ActorEvent) -> UserNotificationSignal | None:
        if event.event_type != "user_notify":
            return None
        if event.payload.get("recipient") != self.principal_id:
            return None

        sender = self.registry.get(event.actor_id)
        sender_name = sender.config.name if sender else ""
        source = notification_source_from_name(sender_name)
        if source not in self.BACKGROUND_SOURCES:
            return None

        metadata = dict(event.payload.get("metadata") or {})
        kind = str(metadata.get("kind", event.payload.get("kind", ""))).strip()
        content = str(event.payload.get("message", "")).strip()
        if not content:
            return None

        category = notification_category_from_metadata(metadata, kind)
        urgency = notification_urgency_from_metadata(metadata, category, kind)
        origin = notification_origin_from_metadata(metadata, source, kind)
        return UserNotificationSignal(
            event_id=event.id,
            source=source,
            source_name=sender_name or source.value,
            source_actor_id=event.actor_id,
            origin=origin,
            category=category,
            urgency=urgency,
            content=content,
            kind=kind,
            metadata=metadata,
            observed_at=event.created_at,
        )
