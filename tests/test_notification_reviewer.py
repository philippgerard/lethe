from unittest.mock import AsyncMock

import pytest

from lethe.notification_reviewer import review_user_notification
from lethe.notification_signals import (
    NotificationCategory,
    NotificationOrigin,
    NotificationSource,
    NotificationUrgency,
    UserNotificationSignal,
)


def _signal(content: str) -> UserNotificationSignal:
    return UserNotificationSignal(
        event_id="sig-1",
        source=NotificationSource.DMN,
        source_name="dmn",
        source_actor_id="actor-1",
        origin=NotificationOrigin.REFLECTION,
        category=NotificationCategory.WARNING,
        urgency=NotificationUrgency.HIGH,
        content=content,
        kind="warning",
    )


@pytest.mark.asyncio
async def test_notification_reviewer_suppresses_internal_status():
    complete = AsyncMock(return_value='{"send": false, "text": ""}')

    result = await review_user_notification(
        _signal("quiet afternoon. nothing to action on the dmn report."),
        recent_context="No recent user context.",
        complete_fn=complete,
    )

    assert result.send is False
    assert result.text == ""
    complete.assert_awaited_once()


@pytest.mark.asyncio
async def test_notification_reviewer_keeps_real_user_message():
    complete = AsyncMock(
        return_value='{"send": true, "text": "You asked me to remind you tomorrow; I still have that queued."}'
    )

    result = await review_user_notification(
        _signal("You asked me to remind the user tomorrow."),
        recent_context="Recent exchanges:\n- user: remind me tomorrow",
        complete_fn=complete,
    )

    assert result.send is True
    assert result.text == "You asked me to remind you tomorrow; I still have that queued."


@pytest.mark.asyncio
async def test_notification_reviewer_fails_closed_on_invalid_json():
    complete = AsyncMock(return_value="send it, probably")

    result = await review_user_notification(
        _signal("deadline tomorrow"),
        recent_context="No recent user context.",
        complete_fn=complete,
    )

    assert result.send is False
    assert result.text == ""
