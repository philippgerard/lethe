"""Shared runtime helpers for Telegram and API entry points."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field

from lethe.config import Settings

logger = logging.getLogger(__name__)


@dataclass
class ProactiveRateLimiter:
    """Hard limiter for assistant-initiated user-visible messages."""

    max_per_day: int
    cooldown_seconds: int
    sends: list[float] = field(default_factory=list)

    @classmethod
    def from_settings(cls, settings: Settings) -> "ProactiveRateLimiter":
        return cls(
            max_per_day=settings.proactive_max_per_day,
            cooldown_seconds=settings.proactive_cooldown_minutes * 60,
        )

    def allowed(self) -> bool:
        now = time.time()
        while self.sends and (now - self.sends[0]) > 86400:
            self.sends.pop(0)

        if self.max_per_day > 0 and len(self.sends) >= self.max_per_day:
            logger.info("Proactive message blocked: daily limit (%d/%d)", len(self.sends), self.max_per_day)
            return False

        if self.sends and (now - self.sends[-1]) < self.cooldown_seconds:
            remaining = int(self.cooldown_seconds - (now - self.sends[-1]))
            logger.info("Proactive message blocked: cooldown (%d seconds remaining)", remaining)
            return False

        return True

    def record(self) -> None:
        self.sends.append(time.time())


async def format_active_reminders(settings: Settings, limit: int = 10) -> str:
    """Return pending reminders as a compact prompt-ready list."""
    from lethe.todos import TodoManager

    todo_manager = TodoManager(settings.db_path)
    todos = await todo_manager.list(status="pending")
    if not todos:
        return ""

    lines = []
    for todo in todos[:limit]:
        priority = todo.get("priority", "normal")
        due = todo.get("due_at", "")
        due_str = f" (due: {due})" if due else ""
        lines.append(f"- [{priority}] {todo['title']}{due_str}")
    return "\n".join(lines)


async def run_background_heartbeat(agent, actor_system, message: str, *, full_context: bool = False) -> str:
    """Route heartbeat work through actors when enabled, otherwise through the agent."""
    if actor_system:
        await actor_system.brainstem_heartbeat(message)
        result = await actor_system.background_round()
        return result or "ok"
    if full_context:
        return await agent.chat(message, use_hippocampus=False)
    return await agent.heartbeat(message)
