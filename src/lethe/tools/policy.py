"""Central tool policy metadata.

The actual tool functions still live in their domain modules.  This module keeps
cross-cutting runtime policy in one place so persistence, recall, actor routing,
and tool-budget decisions do not drift apart.
"""

from __future__ import annotations


SEARCH_RESULT_SKIP_TOOL_NAMES = frozenset(
    {
        "conversation_search",
        "archival_search",
    }
)

MEMORY_TOOL_NAMES = frozenset(
    {
        "memory_read",
        "memory_update",
        "memory_append",
        "archival_search",
        "archival_insert",
        "conversation_search",
    }
)

TELEGRAM_TOOL_NAMES = frozenset(
    {
        "telegram_send_message",
        "telegram_send_file",
        "telegram_react",
    }
)

ACTOR_TOOL_NAMES = frozenset(
    {
        "send_message",
        "user_notify",
        "terminate",
        "ping_actor",
        "wait_for_response",
        "discover_actors",
        "discover_recently_finished",
        "spawn_actor",
        "spawn_chain",
        "kill_actor",
        "update_task_state",
        "get_task_state",
        "restart_self",
    }
)

TODO_TOOL_NAMES = frozenset(
    {
        "todo_list",
        "todo_add",
        "todo_done",
        "todo_remove",
    }
)

READ_ONLY_TOOL_NAMES = frozenset(
    {
        "grep_search",
        "list_directory",
        "read_file",
    }
)

FREE_TOOL_NAMES = (
    TELEGRAM_TOOL_NAMES
    | ACTOR_TOOL_NAMES
    | MEMORY_TOOL_NAMES
)

TRIVIAL_OUTCOME_TOOL_NAMES = (
    SEARCH_RESULT_SKIP_TOOL_NAMES
    | MEMORY_TOOL_NAMES
    | TELEGRAM_TOOL_NAMES
    | ACTOR_TOOL_NAMES
    | TODO_TOOL_NAMES
    | READ_ONLY_TOOL_NAMES
)

EXTERNAL_INTERACTION_TOOL_NAMES = frozenset(
    {
        "bash",
        "web_search",
        "fetch_webpage",
        "browser_open",
        "browser_click",
        "browser_fill",
    }
)

# Cortex keeps a small initial tool surface. Stripped tools are still made
# requestable through request_tool().
CORTEX_TOOL_NAMES = frozenset(
    {
        "bash",
        "read_file",
        "write_file",
        "edit_file",
        "note_search",
        "note_create",
        "note_list",
        "telegram_react",
        "telegram_send_file",
        "conversation_search",
        "spawn_actor",
        "send_message",
        "discover_actors",
        "kill_actor",
        "request_tool",
    }
)

SUBAGENT_DEFAULT_TOOL_NAMES = frozenset(
    {
        "bash",
        "read_file",
        "write_file",
        "edit_file",
        "list_directory",
        "grep_search",
        "view_image",
    }
)

SUBAGENT_EXCLUDED_TOOL_NAMES = TELEGRAM_TOOL_NAMES

