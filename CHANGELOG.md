# Changelog

## 0.20.0 - Rust v1 release

- First Rust release on `main`. Merges the entire v1 branch (single-binary runtime, SQLite-vec memory, lethe-migrate, multi-target release pipeline).
- Aligned agent loop with the Python `main` reference implementation:
  - Dropped the duplicated `<recent_tool_context>` system-prompt block; tool calls live only in the conversation stream.
  - User messages are always timestamped (current + historical).
  - Removed the hard 20-message history cap; token-budget compaction is the only trimmer. DB read raised to 500 rows per turn.
- Tool-loop hardening:
  - `MAX_TOOL_ITERATIONS` 8 → 50; on cap, push a wrap-up nudge and run a no-tools final call.
  - Empty-response nudge: retry once before forcing wrap-up.
  - `FREE_TOOL_NAMES` (memory, telegram, actor lifecycle) excluded from the billable counter.
  - Per-turn tool log (ready for future auto-archival).
  - Circuit breakers: `MAX_TOOL_ERRORS=8`, `MAX_REPEATED_TOOL_CALLS=4`, `MAX_NO_PROGRESS_TURNS=4`.
  - Recover Gemma/llama-style `<tool_call:name{args}>` text embeddings when the native tool_calls field is empty.
- Telegram transport:
  - Send with `parse_mode=Markdown`; fall back to plain text on parse-entity errors.
  - Restored `---` bubble splitter (Python convention): pure-dash divider lines split, fenced code and markdown table separators preserved.
  - Actor-update flow uses an `ok` sentinel contract — prompt asks for `ok` when nothing to surface; code checks exact match and skips Telegram.

## 0.18.0 - Rust v1

- Rewrote Lethe as a Rust single-binary runtime.
- Added Telegram polling and authenticated HTTP/SSE API modes.
- Added local markdown memory, old-schema LanceDB notes/archival/message recall, SQLite todos, hippocampus recall, curator, heartbeat, notification gating, and resident Kameo actor/subagent runtime.
- Added LanceDB-backed semantic search for notes, archival memory, and message history using the legacy Snowflake Arctic embedding model id.
- Added `genai` LLM routing with OpenRouter model-id normalization and `LLM_API_BASE` support for OpenAI-compatible local servers.
- Added filesystem, shell, PTY terminal, browser, image, web, memory, notes, todos, actor, and transport tools.
- Added binary release packaging and binary-first install/update scripts with source-build fallback.
- Added `lethe backup` / `lethe restore` to pack and unpack the workspace, agent state (memory + history), and `.env` as a single tar.gz, prompting before overwriting an existing workspace or `.env`.
- Added `migrator/` subproject (`lethe-migrate` binary) that moves legacy LanceDB data (`archival_memory`, `message_history`, `notes`) into the new SQLite-vec storage. Standalone Cargo project — keeps the Arrow/LanceDB stack out of the main `lethe` build.
- Release workflow now builds `lethe` and `lethe-migrate` for four targets (linux x86_64/aarch64, macOS x86_64/aarch64) on native GitHub Actions runners.
- `install.sh` now fetches both `lethe` and `lethe-migrate` from the release assets and hands off to `lethe init` for the provider/model/key wizard (no more duplicated bash prompts). `uninstall.sh` explicitly removes both binaries and tidies an emptied `$LETHE_HOME/bin/`.
- Removed the former package/test stack and the web console while keeping Anthropic subscription/OAuth support in the Rust runtime.
