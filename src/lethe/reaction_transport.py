from __future__ import annotations

import logging
from typing import Any, Optional


logger = logging.getLogger(__name__)

_REACTION_CACHE_ATTR = "_lethe_available_reactions_cache"


def _is_invalid_reaction_error(exc: Exception) -> bool:
    return "REACTION_INVALID" in str(exc).upper()


def _get_reaction_cache(bot: Any) -> dict[int, Optional[frozenset[str]]]:
    cache = getattr(bot, _REACTION_CACHE_ATTR, None)
    if cache is None:
        cache = {}
        setattr(bot, _REACTION_CACHE_ATTR, cache)
    return cache


async def _get_allowed_reaction_emojis(
    bot: Any,
    chat_id: int,
    *,
    refresh: bool = False,
) -> Optional[frozenset[str]]:
    if not hasattr(bot, "get_chat"):
        return None

    cache = _get_reaction_cache(bot)
    if not refresh and chat_id in cache:
        return cache[chat_id]

    try:
        chat = await bot.get_chat(chat_id)
    except Exception as exc:
        logger.debug("Failed to fetch Telegram chat info for reactions in chat=%s: %s", chat_id, exc)
        return cache.get(chat_id)

    available_reactions = getattr(chat, "available_reactions", None)
    if available_reactions is None:
        cache[chat_id] = None
        return None

    allowed = frozenset(
        reaction.emoji
        for reaction in available_reactions
        if getattr(reaction, "emoji", None)
    )
    cache[chat_id] = allowed
    return allowed


async def _set_message_reaction(bot: Any, chat_id: int, message_id: int, emoji: str) -> None:
    from aiogram.types import ReactionTypeEmoji

    await bot.set_message_reaction(
        chat_id=chat_id,
        message_id=message_id,
        reaction=[ReactionTypeEmoji(emoji=emoji)],
    )


async def send_message_reaction(bot: Any, chat_id: int, message_id: int, emoji: str) -> bool:
    requested = (emoji or "").strip()
    if not requested:
        logger.warning("Skipping empty Telegram reaction for chat=%s message=%s", chat_id, message_id)
        return False

    allowed = await _get_allowed_reaction_emojis(bot, chat_id)
    if allowed is not None and requested not in allowed:
        logger.info(
            "Skipping Telegram reaction %r for chat=%s message=%s; chat allows %s",
            requested,
            chat_id,
            message_id,
            sorted(allowed) if allowed else "no standard emoji reactions",
        )
        return False

    try:
        await _set_message_reaction(bot, chat_id, message_id, requested)
        return True
    except Exception as exc:
        if _is_invalid_reaction_error(exc):
            refreshed_allowed = await _get_allowed_reaction_emojis(bot, chat_id, refresh=True)
            if refreshed_allowed is not None and requested not in refreshed_allowed:
                logger.info(
                    "Telegram rejected reaction %r for chat=%s message=%s after refresh; chat allows %s",
                    requested,
                    chat_id,
                    message_id,
                    sorted(refreshed_allowed) if refreshed_allowed else "no standard emoji reactions",
                )
            else:
                logger.warning(
                    "Telegram rejected reaction %r for chat=%s message=%s: %s",
                    requested,
                    chat_id,
                    message_id,
                    exc,
                )
            return False

        logger.warning(
            "Failed to set Telegram reaction %r for chat=%s message=%s: %s",
            requested,
            chat_id,
            message_id,
            exc,
        )
        return False
