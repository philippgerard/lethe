# Lethe

[![Fork release](https://img.shields.io/github/v/release/philippgerard/lethe?style=flat-square&color=blue)](https://github.com/philippgerard/lethe/releases/latest)
[![Release workflow](https://github.com/philippgerard/lethe/actions/workflows/release.yml/badge.svg)](https://github.com/philippgerard/lethe/actions/workflows/release.yml)
![License: MIT](https://img.shields.io/badge/license-MIT-green?style=flat-square)
[![Rust](https://img.shields.io/badge/rust-stable-orange?style=flat-square&logo=rust)](https://www.rust-lang.org/)

**An independently operated, production-oriented fork of
[`alien-id/lethe`](https://github.com/alien-id/lethe).**

This repository began as a downstream patch set. It now has its own release
line, operating assumptions, integrations, and roadmap. Upstream remains the
origin of the core architecture and is still integrated selectively, but this
repository is no longer a mirror and does not promise drop-in behavioral or
configuration compatibility with upstream.

Lethe is a long-running personal AI runtime built around a cortex,
hippocampus, brainstem, default-mode network, and cooperating actors. It keeps
memory across sessions, delegates work to durable subagents, performs bounded
background work, and reaches the user through Telegram or an authenticated
HTTP/SSE API. Identity and persona are configuration, not a hard-coded
character. The runtime ships as one Rust binary and deliberately has no
bundled web console.

## Why this fork exists

The primary target is a private, always-on assistant that remains useful after
restarts and can safely connect to the rest of Philipp's infrastructure. The
current product surface emphasizes:

- **Durable autonomous work.** Actor state, goals, checkpoints, todos,
  proactive messages, and conversation history survive restarts instead of
  living only in one model context.
- **Controlled integration.** A scoped remote MCP client and an optional
  hosted-plugin bridge extend the tool catalog without compiling every
  external system into Lethe.
- **Reliable delivery.** Telegram, HTTP/SSE, and the scheduler-oriented
  `POST /wake` path share one agent and one Brainstem, with safeguards against
  duplicate or silently discarded replies.
- **Voice as first-class input.** Telegram voice notes and the transcription
  command share OpenAI, OpenRouter, multilingual Mistral Voxtral, and local
  Whisper backends.
- **Tiered model use.** Main, auxiliary, tool-chain, and deep-thinking model
  slots let inexpensive work stay inexpensive while difficult turns and
  subagents can escalate deliberately.
- **Secrets outside model context.** Alien agent-id, its encrypted vault, and
  the vault-sealed browser inject credentials without returning their values
  through ordinary tool results.
- **Operational hardening.** Process-group cleanup, provider-limit handling,
  history repair, bounded tool loops, release checksums, and fork-invariant
  tests are treated as product behavior rather than incidental patches.

The production profile runs as Docker Compose on Dokploy behind Tailscale.
State remains on the persistent `lethe-home` volume and uses SQLite; deployments
are built from checksum-pinned fork releases. The separate `lethe-stack`
repository owns that deployment shape. The Podman/Apple Container workflow in
this repository remains useful for local installations, but it is not the
production control plane.

### Storage: SQLite first

Standalone Lethe uses SQLite and SQLite-vec. That is the default, the normal
self-hosted choice, and the backend used in production. It requires no database
server.

The Cargo feature `postgres-memory` is a different integration boundary: it
exposes a tenant-scoped PostgreSQL implementation for applications embedding
Lethe as a library. Enabling the feature does not migrate a standalone install,
and there is no runtime switch that turns the CLI deployment into PostgreSQL.

### Relationship to upstream

- Fork releases and issues live in `philippgerard/lethe`; tags use a `-pgN`
  suffix when they carry downstream changes.
- `alien-id/lethe` is tracked as `upstream`. Changes are reviewed and merged
  deliberately rather than followed automatically.
- Local safety, delivery, storage, model-routing, MCP, transcription, and
  production invariants take precedence when an upstream change conflicts
  with them.
- The Git history and MIT attribution are retained. “Independent” describes
  maintenance and product direction, not a claim that the project started
  here.

## Quickstart

Building from source is the least ambiguous way to install this fork:

> Linux linker note: `.cargo/config.toml` uses `mold`. Install it first
> (`sudo dnf install mold` or `sudo apt-get install mold`) or adjust the local
> linker configuration.

```bash
git clone https://github.com/philippgerard/lethe.git
cd lethe
cargo build --release --locked --bin lethe
mkdir -p ~/.local/bin
install -m 755 target/release/lethe ~/.local/bin/lethe
LETHE_REPO_OWNER=philippgerard ~/.local/bin/lethe init  # add --yolo for a native setup
```

The remaining command examples assume `~/.local/bin` is on `PATH`; otherwise
invoke the binary as `~/.local/bin/lethe`.

The inherited installer can also install fork release assets, but its repository
defaults are still upstream-compatible and therefore must be explicit:

```bash
curl -fsSL https://raw.githubusercontent.com/philippgerard/lethe/main/install.sh |
  env LETHE_REPO_OWNER=philippgerard \
      LETHE_REPO_URL=https://github.com/philippgerard/lethe.git \
      bash
```

Do not use `https://lethe.gg/install` when you intend to install this fork; that
endpoint belongs to upstream. The same `LETHE_REPO_OWNER=philippgerard`
override is required by `update.sh` and by macOS or other cross-architecture
`lethe container up` runs so downloaded binaries stay on the fork release line.

`lethe init` writes `~/.lethe/config/.env`, seeds the workspace and core memory
blocks, and runs a smoke test against the LLM and embedding pipeline. It runs
non-interactively when stdin is not a terminal: pass
`--provider`/`--model`/`--aux-model` and supply credentials through the
provider's environment variable. If you prefer manual configuration, copy
`.env.example`. The first semantic-memory use downloads the embedding runtime
and model.

Then inspect the runtime:

```bash
lethe                 # status: version + current config (no live probes)
lethe check           # live health check (LLM + embeddings)
lethe chat -m "hello" # one-off message straight to the model
```

To sign in or re-authenticate one provider without rerunning the full wizard,
use `lethe login`:

```bash
lethe login openai       # asks: ChatGPT Plus/Pro subscription (default) or API key
lethe login anthropic    # asks: Claude Pro/Max subscription (default) or API key
lethe login openrouter   # API key only
lethe login opencode-go  # API key only
```

Each command writes credentials to `~/.lethe/credentials/` (subscription) or sets the API key in `~/.lethe/config/.env`, flips `LLM_PROVIDER`, and prompts for `LLM_MODEL` / `LLM_MODEL_AUX` (defaults from the curated catalog — accept with Enter, or type any other model id).

Sanity-check an existing setup any time with `lethe check` — it pings the model and exercises the embedding pipeline rather than just printing config.

## Architecture

```
       Telegram / HTTP+SSE / TUI / POST /wake
                         |
                         v
              Transport supervisor
          one Agent, registry, and Brainstem
                         |
                         v
               Cortex + tool loop
       prompt, memory, delegation, model tiers
                         |
       +-----------------+------------------+
       |                 |                  |
       v                 v                  v
 Associative memory   Actor runtime     Brainstem + DMN
 lexical/vector       durable workers   heartbeat, gating,
 recall               and checkpoints   proactive delivery
       |                 |                  |
       v                 v                  |
 Memory storage     SQLite actor            |
 traits             snapshots               |
 SQLite by default;     |                  |
 Postgres for hosts     +---------+--------+
       |                          |
       +--------------------------+
                         |
                         v
                    Tool registry
 built-ins | Alien vault/browser | remote MCP | hosted plugins
```

Core runtime pieces:

| Area | Rust modules | Responsibility |
|------|--------------|----------------|
| Agent/cortex | `src/agent.rs`, `src/agent/` | Prompt assembly, LLM calls, bounded tool loops, summarization, and actor turn execution. |
| LLM routing | `src/llm/` | `genai` client, OAuth (ChatGPT Plus/Pro, Claude Pro/Max) and API-key auth, per-model prompt-cache dialects, OpenRouter prompt-cache forwarding via vendored genai patch, model metadata. |
| Memory | `src/memory/`, `src/todos.rs` | Storage contracts, Markdown blocks, archival memory, messages, notes, todos, SQLite-vec, and the optional PostgreSQL host backend. |
| Recall | `src/memory/recall.rs`, `src/memory/search.rs` | Hybrid lexical/vector recall over notes, archival memories, and conversation history. |
| Actors | `src/actor.rs`, `src/actor/` | Resident Kameo actors, supervisor-owned state, mailbox/event routing, autonomous subagent wakeups, persistent DMN, SQLite-backed actor snapshots that survive restarts. |
| Background work | `src/scheduler/`, `src/actor/notification.rs` | Heartbeats, DMN wakeups, memory curation, candidate gating, and proactive output limits. |
| Transports | `src/interfaces/`, `src/cli/telegram_loop.rs`, `src/conversation/` | Telegram polling, HTTP/SSE, scheduled wakes, transcription, debounce, and cancellation. |
| Tools | `src/tools/` | Filesystem, shell/PTY, image, web/research, memory, todos, actors, Telegram/client egress, Alien browser, remote MCP, and hosted plugins. |

## Build

```bash
git clone https://github.com/philippgerard/lethe.git
cd lethe
cp .env.example .env
cargo build --release --locked --bin lethe
target/release/lethe --version
```

Release archives and their SHA-256 files are published for Linux x86_64, Linux
ARM64, and Apple-silicon macOS. Before extracting a downloaded binary, the
installer verifies its GitHub artifact attestation with `gh`. A missing
verifier, missing or invalid attestation, or unavailable release asset fails
the binary path closed and falls back to a local Cargo build. See the
fork-aware installer invocation in [Quickstart](#quickstart); `lethe.gg` tracks
upstream, not this release line. Force source builds with
`LETHE_INSTALL_FROM_SOURCE=1`.

Run tests:

```bash
cargo fmt --check
cargo test --locked --no-default-features
cargo test --locked --manifest-path vendor/genai/Cargo.toml --lib
cargo build --release --locked --bin lethe
```

Browser automation uses the vault-sealed Alien Browser exclusively; the former
standalone `agent-browser`/`browser_*` integration has been removed. See
[Alien agent-id](#alien-agent-id).

## Running

For a normal local install, `lethe init` creates an isolated container managed
as a background service. Native `--yolo` mode is also supported. Production
uses the external Dokploy Compose stack described above rather than these CLI
container commands.

On macOS and other cross-architecture container builds, export
`LETHE_REPO_OWNER=philippgerard` before `lethe container up`; otherwise the
inherited download default resolves to the upstream release repository.

**Deploy & manage**

```bash
export LETHE_REPO_OWNER=philippgerard  # keep cross-arch downloads on the fork
lethe run                      # run in the foreground here (Ctrl-C to stop); --yolo for native
lethe service install --now    # install + start the background service (systemd user unit / launchd agent)
lethe service status           # platform, unit path, live status
lethe container up             # build image (if needed), create the container, install + start the service
lethe container status         # engine, container state, shared mounts
lethe container logs -f        # follow the container logs
lethe container shell          # root shell inside the running container
lethe container up --rebuild   # rebuild the image and recreate the container
lethe container down           # stop the container
lethe uninstall                # remove the service/container (add --purge to also delete ~/.lethe)
```

Share extra host directories with the container via `lethe container up --mount host[:container]` (repeatable; persisted).

When a container build needs a published binary for a different host architecture, Lethe verifies that archive with `gh attestation verify` before copying it into the image. Install the GitHub CLI or use `lethe container up --from-source` from a checkout.

**Reach Lethe**

```bash
lethe transport list                       # API + Telegram channels and their status
lethe transport api --port 1373 --token    # configure the local HTTP API (powers the TUI); --token mints a fresh one
lethe transport telegram --enable          # configure + enable the Telegram bot
```

Under the hood a single `lethe api` process hosts the HTTP/SSE transport **and** the Telegram poller (when `TELEGRAM_BOT_TOKEN` is set) in the same address space, sharing one Agent, one actor registry, and one Brainstem (the sole source of heartbeats / proactive emissions — transports just subscribe and forward). API mode binds to `LETHE_API_HOST` (`127.0.0.1` by default) on `LETHE_API_PORT` (`1373`); use a reverse proxy for remote access.

**Configure on the fly**

```bash
lethe status                   # version + current config, secrets censored (this is also the bare `lethe`)
lethe identity set --name "…"  # change identity/persona; `lethe identity edit` opens $EDITOR
lethe model                    # show current model + catalog; `lethe model <id>` or `--pick` to change
lethe login anthropic          # (re-)auth a single provider
lethe completions fish         # print a shell completion script
```

Add `--config <PATH>` to any command to point at a different `.env`. Low-level/debug subcommands (`memory`, `fs`, `sh`, `todo`, `agent`, …) are hidden but still work — `lethe help <command>`.

### Terminal UI

```bash
lethe tui                                       # local API
lethe tui --url http://host:1373 --token $LETHE_API_TOKEN
```

Inline tool cards, an actors/todos sidebar, streaming assistant text
(Anthropic + OpenAI OAuth providers), `@`-prefix workspace path
autocomplete, and slash commands (`/help`, `/clear`, `/cancel`,
`/todos`, `/actors`, `/model`, `/quit`).

## External tool bridges

This fork supports two deliberately separate extension paths:

- **Remote MCP** connects the standalone runtime directly to one Streamable
  HTTP MCP hub. `MCP_SERVER_URL` and a narrowly scoped `MCP_SERVER_TOKEN`
  enable `mcp_list_tools`, `mcp_describe_tool`, and `mcp_call`; with either
  value missing, the whole family stays hidden. Tool visibility remains the
  server's responsibility, based on the token's grants.
- **Hosted plugins** let a trusted embedding host provide a user-scoped tool
  catalog, prompt context, and execution bridge. Lethe loads tools on demand
  through `request_tool` and does not need one credential per plugin. A hosted
  Agenda can replace local todos completely, including during gateway failure,
  to avoid a split-brain task list.

MCP is a direct protocol integration owned by the standalone process. Hosted
plugins are an application-host contract. They can coexist, but they are not
aliases and do not share configuration.

## LLM Providers

Lethe routes chat through `genai`. The runtime supports both API-key and subscription-OAuth auth, plus OpenAI-compatible local servers:

| Provider | Auth | Example `LLM_MODEL` |
|----------|------|---------------------|
| Anthropic (API key) | `ANTHROPIC_API_KEY` | `claude-opus-4-7` |
| Anthropic (Claude Pro/Max) | `lethe login anthropic` → token file | `claude-opus-4-7` |
| OpenAI (API key) | `OPENAI_API_KEY` | `gpt-5.5` |
| OpenAI (ChatGPT Plus/Pro) | `lethe login openai` → token file | `gpt-5.5` |
| OpenRouter | `OPENROUTER_API_KEY` | `openrouter/moonshotai/kimi-k2.6` |
| OpenCode Go | `OPENCODE_GO_API_KEY` | `opencode-go/kimi-k2.6` |
| Local OpenAI-compatible | `LLM_API_BASE` + `OPENAI_API_KEY=local` | `openai/gemma-4-31B-it-Q8_0.gguf` |

[OpenCode Go](https://opencode.ai/zen/go) is a budget-friendly gateway ($5–$10/month) to a curated set of open models. Unlike the other providers it speaks **different wire protocols per model** — some models expect the OpenAI API, others the Anthropic Messages API — so each catalog entry declares its protocol and the router selects the matching adapter automatically. No subscription/OAuth path; API key only.

`LLM_PROVIDER` is optional but useful when a model id does not carry a provider prefix — for example `LLM_PROVIDER=openrouter` with `LLM_MODEL=moonshotai/kimi-k2.6`. Subscription auth also requires `LLM_PROVIDER=openai` or `LLM_PROVIDER=anthropic` so the router picks the OAuth path instead of looking for an API key (the `lethe login` commands set this for you).

Lethe uses up to four model slots. `LLM_MODEL` is the main model; `LLM_MODEL_AUX` (defaults to the main model) handles lightweight/background calls (summarizer, curator, heartbeat). Two optional tiers let a turn change models mid-flight: `LLM_MODEL_TOOL` is a stronger reasoner a turn switches to the moment a tool is used, and `LLM_MODEL_DEEP` is a powerful "deep thinking" model the agent **escalates to on demand** for hard tasks — by calling the `think_deeply` tool (self-recognition), automatically when a turn is visibly struggling, or for a subagent spawned on the `deep` tier. Both reset to `LLM_MODEL` on the next turn; deep escalation outranks the tool switch. The deep tier can also be changed at runtime via `POST /model` (`model_deep`) or Telegram `/deep <model-id>`; the tool tier is environment/config only.

### Subscription OAuth

`lethe login openai` runs a device-code flow against `auth.openai.com`; tokens land in `~/.lethe/credentials/openai_oauth_tokens.json`. Calls then go to the Codex Responses API at `chatgpt.com/backend-api/codex/responses` using your ChatGPT Plus/Pro session — no `OPENAI_API_KEY` needed. Override the token file with `LETHE_OPENAI_OAUTH_TOKENS` or supply a raw token via `OPENAI_AUTH_TOKEN`.

`lethe login anthropic` runs a PKCE browser flow against `claude.ai/oauth/authorize`; tokens land in `~/.lethe/credentials/anthropic_oauth_tokens.json`. Override with `LETHE_ANTHROPIC_OAUTH_TOKENS` or `ANTHROPIC_AUTH_TOKEN`.

### Prompt caching

Lethe stamps cache breakpoints on the system prompt — a 1h-TTL prefix (identity, persona, instructions) plus a 5min-TTL tail (clock, memory state, recall) — but only for models that need an explicit breakpoint. Which marker a model gets is decided by its dialect in [`src/llm/dialect.rs`](src/llm/dialect.rs):

- **Anthropic direct** and **Anthropic OAuth** — emitted on system blocks, 1h + 5min.
- **OpenCode Go models using the Anthropic protocol** — emitted on system blocks, 1h + 5min.
- **OpenRouter → Anthropic** — emitted on system content parts. OpenRouter relays `cache_control` to the upstream vendor (converting between the Anthropic and OpenAI marker formats), and Anthropic is the only vendor accepting the extended `ttl: "1h"`.
- **OpenRouter → Gemini / Qwen** — explicit breakpoints too, but 5min only: the 1h TTL is Anthropic-only.
- **Everything else** — no explicit marker. OpenAI, Grok, Moonshot/Kimi, Groq, DeepSeek and Z.AI/GLM cache automatically, so a breakpoint buys nothing.

See [OpenRouter's prompt-caching docs](https://openrouter.ai/docs/features/prompt-caching) for the per-vendor rules. Upstream `genai` only supports request-level `cache_control` (OpenAI's native `prompt_cache_retention`), which does not cover the OpenRouter route — forwarding it per-message is the one patch our vendored fork carries. See [`vendor/genai/LETHE_FORK.md`](vendor/genai/LETHE_FORK.md).

## Configuration

Configuration is read from process environment, a local `.env`, and
`$LETHE_HOME/config/.env`. The table lists the common operational settings;
[`.env.example`](.env.example) is an annotated starting point.

| Variable | Description | Default |
|----------|-------------|---------|
| `LETHE_MODE` | `cli`, `telegram`, or `api` | `cli` |
| `LETHE_HOME` | Runtime root | `~/.lethe` |
| `LETHE_AGENT_NAME` | Assistant name (see `lethe identity`) | `lethe` |
| `LETHE_CONFIG_FILE` | Config `.env` path (also `--config`) | `$LETHE_HOME/config/.env` |
| `WORKSPACE_DIR` | Workspace directory | `$LETHE_HOME/workspace` |
| `MEMORY_DIR` | Memory data directory | `$LETHE_HOME/data/memory` |
| `DB_PATH` | Legacy todo database imported into unified memory on first run | `$LETHE_HOME/data/lethe.db` |
| `LOGS_DIR` | Runtime log directory | `$LETHE_HOME/logs` |
| `TELEGRAM_BOT_TOKEN` | Bot token from BotFather | required for Telegram |
| `TELEGRAM_ALLOWED_USER_IDS` | Comma-separated allowlist | all users |
| `TELEGRAM_TRANSCRIPTION_ENABLED` | Transcribe Telegram audio/voice | `true` |
| `LETHE_API_TOKEN` | Bearer or `x-lethe-token` auth for API mode | required for API |
| `LETHE_API_HOST` | API bind address | `127.0.0.1` |
| `LETHE_API_PORT` | API port | `1373` |
| `LLM_PROVIDER` | Optional provider hint | auto |
| `LLM_MODEL` | Main model | required for chat |
| `LLM_MODEL_AUX` | Auxiliary model — cheap background calls (summarizer, curator, heartbeat) | main model |
| `LLM_MODEL_TOOL` | Optional stronger reasoner; a turn switches to it the moment a tool is used (rest of the chain), then resets next turn | unset (no switch) |
| `LLM_MODEL_DEEP` | Optional powerful "deep thinking" model the agent escalates to for hard tasks — via the `think_deeply` tool, an auto-escalate backstop when a turn struggles, or a `deep`-tier subagent; resets next turn. Outranks `LLM_MODEL_TOOL` | unset (no escalation) |
| `LLM_API_BASE` | Custom OpenAI-compatible base URL | unset |
| `LLM_CONTEXT_LIMIT` | Context size hint | `100000` |
| `LLM_MAX_OUTPUT` | Per-request output-token limit | `8000` |
| `OPENROUTER_API_KEY` | OpenRouter key | unset |
| `ANTHROPIC_API_KEY` | Anthropic key | unset |
| `ANTHROPIC_AUTH_TOKEN` | Optional Anthropic OAuth access token (raw) | unset |
| `LETHE_ANTHROPIC_OAUTH_TOKENS` | Optional Anthropic OAuth token file | `$CREDENTIALS_DIR/anthropic_oauth_tokens.json` |
| `OPENAI_API_KEY` | OpenAI/local-compatible key | unset |
| `OPENAI_AUTH_TOKEN` | Optional OpenAI OAuth access token (raw) | unset |
| `LETHE_OPENAI_OAUTH_TOKENS` | Optional OpenAI OAuth token file | `$CREDENTIALS_DIR/openai_oauth_tokens.json` |
| `OPENCODE_GO_API_KEY` | OpenCode Go key | unset |
| `MISTRAL_API_KEY` | Mistral key — Voxtral transcription only (not the chat LLM) | unset |
| `EXA_API_KEY` | Exa search/fetch tools | unset |
| `MCP_SERVER_URL` | Remote MCP Streamable HTTP endpoint | unset |
| `MCP_SERVER_TOKEN` | Bearer token scoped by the MCP server | unset |
| `MCP_SERVER_LABEL` | Optional name shown by the MCP discovery tools | unset |
| `LETHE_HOSTED_API_BASE` | Trusted hosted-plugin gateway base URL | unset |
| `LETHE_HOSTED_API_TOKEN` | User-scoped hosted-plugin credential | unset |
| `LETHE_HOSTED_DISABLE_LOCAL_TODOS` | Replace local todos with hosted Agenda without fallback | `false` |
| `LETHE_HOSTED_CATALOG_TTL` | Hosted tool-catalog cache lifetime in seconds | `30` |
| `LETHE_SEMANTIC_SEARCH_ENABLED` | Enable vector recall (fallback is keyword search) | `true` |
| `LETHE_EMBEDDING_PROVIDER` | `fastembed` or `hash` | `fastembed` |
| `LETHE_EMBEDDING_MODEL` | FastEmbed model id | `Snowflake/snowflake-arctic-embed-m-v2.0` |
| `ACTORS_ENABLED` | Enable actor/subagent system | `true` |
| `HIPPOCAMPUS_ENABLED` | Enable associative recall | `true` |
| `CURATOR_ENABLED` | Enable memory curator | `true` |
| `HEARTBEAT_ENABLED` | Enable proactive heartbeat loop | `true` |
| `HEARTBEAT_INTERVAL` | Heartbeat interval seconds | `3600` |
| `DEBOUNCE_SECONDS` | Merge a burst of Telegram messages into one turn | `5.0` |
| `PROACTIVE_MAX_PER_DAY` | Proactive message daily limit | `4` |
| `PROACTIVE_COOLDOWN_MINUTES` | Minimum spacing for proactive messages | `60` |
| `TRANSCRIPTION_PROVIDER` | `auto`, `openrouter`, `openai`, `mistral`, or `local` | `auto` |
| `TRANSCRIPTION_MODEL` | STT model override | provider default |
| `TRANSCRIPTION_LANGUAGE` | Optional language hint | auto |
| `TRANSCRIPTION_LOCAL_COMMAND` | Local Whisper command | `whisper` |

## Memory

The standalone runtime always opens the local memory store. Building with
`postgres-memory` only makes the PostgreSQL types available to an embedding
host; it does not change this path or interpret `DATABASE_URL` in the CLI.

Lethe stores runtime state under the workspace and data directories:

- `workspace/memory/identity.md` -- persona and identity, user-editable.
- `workspace/memory/human.md` -- facts about the user.
- `workspace/memory/project.md` -- current project/context.
- `workspace/notes/` -- tagged markdown notes.
- `$MEMORY_DIR/lethe-memory.db` -- SQLite-vec database with `memory` (archival + notes, with `note-<uuid>` and `mem-<uuid>` ids), `message_history`, their `*_vec` virtual siblings for embedding search, plus `todos` (with `parent_id` subtasks) and `actors` (snapshots of subagent state).
- SQLite database at `$DB_PATH` -- legacy todos location, migrated into `lethe-memory.db` on first run.

Unfinished work is first-class state, not conversation residue:

- In-progress and overdue todos are injected into every system prompt as `<active_tasks>` — the agent sees its own open work without having to remember to ask.
- The heartbeat receives an open-work digest (unfinished subagents — including blocked ones — and in-progress/overdue todos) and never skips a tick while that digest is non-empty.
- Subagent state is snapshotted to the `actors` table on every change. After a restart (deploy, crash, self-upgrade) unfinished subagents are restored with their goals, task state, turn budget, and last checkpoint, and resume automatically.
- When a turn hits its tool budget, the agent is forced to emit a resumable GOAL / DONE / REMAINING / NEXT checkpoint instead of a truncated answer; subagents see their own previous checkpoint each turn, and a subagent that runs out of turns hands its checkpoint to its parent for a successor.

Core memory block defaults and prompt templates are embedded into the binary, so `lethe check` and first startup work without copying prompt files into the workspace.

Upgrading from a pre-0.19 install? See [`MIGRATION.md`](MIGRATION.md) for the one-shot `lethe-migrate` workflow that moves legacy LanceDB data into the new layout.

## Backup & Restore

Pack the workspace, agent state (memory + history), and `.env` into a single tar.gz archive:

```bash
lethe backup                              # ./lethe-backup-YYYYMMDD-HHMMSS.tar.gz
lethe backup --output ~/backups/lethe.tgz
```

The archive is written with `0600` permissions because it contains the `.env` secrets — keep it private.

Restore an archive into the current `$LETHE_HOME`:

```bash
lethe restore lethe-backup-20260525-160522.tar.gz
lethe restore archive.tgz --yes          # skip prompts (for scripts / non-TTY)
```

Restore prompts before overwriting an existing **workspace** and again before overwriting an existing **`.env`** — declining either keeps the local copy intact. Memory and history are restored unconditionally (that is the point of restoring).

## Logging

Lethe writes structured runtime logs to `$LOGS_DIR/lethe.log` and mirrors them to stderr. The default level is `info`; override it with `RUST_LOG`, for example:

The foreground helper reads optional overrides from the host-only
`~/.config/lethe/foreground.env`, never from the container-writable
`~/.lethe/config/.env`. On first use it creates an empty `foreground.env` with
`0600` permissions; add only literal `KEY=VALUE` entries when overrides are
needed.

```bash
RUST_LOG=debug scripts/lethe-telegram-foreground
tail -f ~/.lethe/logs/lethe.log
```

Telegram turns, LLM responses, tool calls, tool results, heartbeat failures, and background actor update relay failures are logged for post-mortem debugging.

Full LLM request/response dumps are opt-in because they contain prompts, memory, tool schemas, tool results, and attachments:

```bash
LLM_DEBUG=true scripts/lethe-telegram-foreground
ls ~/.lethe/logs/llm/
```

Override the dump directory with `LLM_DEBUG_DIR`.

## API

`GET /health` is intentionally unauthenticated for readiness probes. Every
other HTTP route and the browser WebSocket require
`Authorization: Bearer <LETHE_API_TOKEN>` or `x-lethe-token`.

| Route | Method | Purpose |
|-------|--------|---------|
| `/health` | `GET` | Process readiness check; does not probe LLMs or external tools. |
| `/chat` | `POST` | Send a user message and receive SSE response events. |
| `/wake` | `POST` | Run one scheduler-triggered turn with real Telegram egress; a normal final response is delivered automatically when no Telegram tool message was sent (`message`, optional `chat_id`). |
| `/events` | `GET` | Subscribe to brainstem + actor SSE events. |
| `/browser/stream` | `GET`/WebSocket | Relay the live vault-sealed browser viewport and input stream. |
| `/cancel` | `POST` | Cancel active work for a chat. |
| `/configure` | `POST` | Store user metadata in memory. |
| `/model` | `GET`/`POST` | Inspect or update the main/aux/deep model ids (`model`, `model_aux`, `model_deep`). |
| `/file?path=...` | `GET` | Serve a workspace file. |
| `/actors` | `GET` | Snapshot of active and recently terminated actors. |
| `/todos` | `GET` | List todos (filters: `status`, `priority`, `include_completed`, `limit`). |
| `/session/history` | `GET` | Last N persisted messages (`limit`). |
| `/secure-input` | `POST` | Deliver a browser-sealed credential envelope to a pending agent-id prompt (hosted mode). |
| `/secure-input/cancel` | `POST` | Dismiss a pending secure-input request. |
| `/secure-input/pending` | `GET` | Live secure-input requests (with sealing envelope) for tab re-hydration. |

`/wake` reports turn completion and Telegram delivery separately. A confirmed
Telegram tool side effect can coexist with `success: false` when the turn later
checkpoints; callers must inspect `delivered` and `delivery_status` and must not
blindly retry an HTTP 200 response.

Combined event vocabulary for `/chat` and `/events` follows. Not every event is
emitted on both streams: `/chat` owns the live turn, while `/events` carries the
durable/background mirror used by reloaded or secondary clients.
Raw provider reasoning is deliberately not exposed by the built-in HTTP API;
clients should use the turn and typing events for progress indication.

| Event | Payload | Meaning |
|-------|---------|---------|
| `turn.start` | `{chat_id}` | A new agent turn has begun. |
| `turn.active` | `{active}` | Durable activity state for reloaded or secondary clients. |
| `assistant.delta` | `{content}` | Streamed assistant token chunk (Anthropic + OpenAI OAuth). |
| `text` | `{content, parse_mode, message_id}` | Complete (sub-)message; submessage boundaries follow the `---` rule from `interfaces/telegram/formatting.rs`. |
| `message` | `{role, content}` | Durable mirror of a completed assistant reply. |
| `tool.start` | `{call_id, name, args_preview}` | Tool execution started. |
| `tool.end` | `{call_id, name, success, output_preview, duration_ms}` | Tool execution finished. |
| `actor.spawned` / `actor.state` / `actor.task` / `actor.message` | `{actor_id, group, payload}` | Actor lifecycle events fanned out from `ActorEventBus`; payloads include typed background/source metadata where applicable. |
| `actor.user_notify` | `{actor_id, group, payload}` | A worker or the DMN addressed the user directly. |
| `usage` | `{prompt_tokens}` | Updated context window usage. |
| `typing_start` / `typing_stop` | `{}` | Compatibility hints for chat clients. |
| `secure_input.request` | `{request_id, title, description, fields, server_pub, alg, expires_at, …}` | The agent needs a human-typed secret; render a sealed credential card (hosted). |
| `secure_input.resolved` | `{request_id, outcome}` | A secure-input request was `submitted` / `expired` / `cancelled`. |
| `agent_id.bound` | `{owner_sub, jkt}` | Background owner-binding completed. |
| `done` | `{}` | Turn complete; safe to close the stream. |

## Alien agent-id

Each Lethe instance can carry its own **Alien agent identity** (Ed25519, L0
self-asserted out of the box; optionally bound to a human owner via the Alien app
for L1/L2 assurance) and an **encrypted credential vault**, and drive a
**vault-sealed browser** (headless on a server or in a container, headed when a
display is available). These are provided by the
[`agent-id`](https://github.com/alien-id/agent-id) CLIs (`agent-id-core`,
`agent-id-vault`, `agent-id-browser`); Lethe shells out to them.

Enable identity + vault by installing the two published CLIs so they're on `PATH`
(`install.sh` does this automatically when npm is present; `LETHE_SKIP_AGENT_ID=1`
opts out):

```bash
npm i -g @alien-id/agent-id-core @alien-id/agent-id-vault   # identity + vault
```

`lethe init` provisions an L0 identity + vault automatically when the CLIs are
present; the daemon re-provisions on start, and `install.sh` runs the same
provisioning (`lethe agent-id provision`, idempotent) even when the init wizard
is skipped. State is isolated per instance under `AGENT_ID_STATE_DIR` (default
`<LETHE_HOME>/agent-id`). `lethe check` reports CLI presence, identity state,
and browser CLI health.

### Browser tools (optional)

The vault-sealed browser adds the `alien_browser_*` tools (`_open` starts a
session, `_act` runs page verbs, `_inspect_form` / `_fill_form` operate on a
whole form, and `_fill_secret` / `_fill_otp` inject vaulted credentials the
model never sees).
It is Lethe's only browser: when the CLI is absent — or present but unable to
start; Lethe probes it once per run — the `alien_browser_*` tools are hidden and
`lethe check` says why. A normal host install drives Google Chrome through
Patchright's `channel:"chrome"`; the ARM64 production image instead pins
Patchright Chromium and supplies it at the channel-compatible path because
branded Chrome is unavailable there. Host installs therefore need Chrome:

```bash
npm i -g @alien-id/agent-id-browser   # pulls matching core/vault + patchright
# …and install Google Chrome (google-chrome-stable) on the host.
```

`install.sh` does this automatically when npm and Chrome are both present.
Install the browser **from npm** so core/vault/browser come from one release
set. Do not pack the plugin from a repo checkout against npm-installed
dependencies: mid-release-cycle the checkout's imports can be ahead of the
published `@alien-id/agent-id-core`, and the CLI dies in its module loader
before parsing a single argument. To run bleeding-edge browser code from a
checkout, let the workspace supply its siblings instead:

```bash
# From a checkout of github.com/alien-id/agent-id:
bun install                          # links workspace core/vault next to the plugin
# then point Lethe at the checkout's CLI:
AGENT_ID_BROWSER_BIN=<checkout>/plugins/agent-id-browser/bin/cli.mjs
```

Chrome refuses to run as **root** with its sandbox on. In a container that runs as
root (the container is the isolation boundary), set
`AGENT_ID_BROWSER_NO_SANDBOX=1`; leave it unset on a normal desktop so the sandbox
stays on. Headed login (`alien_browser_login`) needs a display and is therefore
unavailable on a headless server — there, use the headless flow below.

**Headless flow:** `alien_browser_open` automatically creates the shared `main`
profile as an anonymous L0 cookie jar, so public pages work immediately. For a
site that needs an account, add a `login` credential *with a `login_url`* (via
`vault_add`), then use `alien_browser_auto_login` to sign in and reseal that
profile. Inspect forms with `alien_browser_inspect_form` and fill ordinary fields,
selections, checks, and workspace file uploads in one verified
`alien_browser_fill_form` call. (A site with an aggressive anti-automation wall
may still need a one-time headed sign-in on a machine with a display.)

| env var | default | meaning |
|---|---|---|
| `AGENT_ID_ENABLED` | `true` | Master switch for the integration. |
| `AGENT_ID_STATE_DIR` | `<LETHE_HOME>/agent-id` | Per-instance identity + vault state. |
| `AGENT_ID_CORE_BIN` / `AGENT_ID_VAULT_BIN` / `AGENT_ID_BROWSER_BIN` | discovered on `PATH` | Override CLI locations. |
| `AGENT_ID_BROWSER_NO_SANDBOX` | unset | Set `1` to keep Chrome's `--no-sandbox` (required when running as root, e.g. in a container). |
| `ALIEN_PROVIDER_ADDRESS` | — | Alien SSO provider for `agent_id_bind`. |
| `LETHE_SECURE_PROMPT` | `off` | `hosted` runs the secure-input socket server (set by lethe-hosted). |

Examples (requested on demand): `agent_id_status`, `agent_id_bind`, `agent_id_sign`,
`vault_list`, `vault_add`, `vault_remove`, `vault_set_totp`, and the browser tools
`alien_browser_login` / `_auto_login` / `_open` / `_close` / `_act` /
`_request_viewport` / `_inspect_form` / `_fill_form` / `_fill_secret` /
`_fill_otp`.

### Security model

Secrets are kept out of the model's context by construction of the tool surface —
there is **no `vault_show` and no generic `vault_exec`** exposed to the agent. The
vault tools return metadata only; secret *values* are typed by the human (over the
hosted secure-input channel or the local loopback browser form) and are used inside
the vault-sealed browser's own session process (`fill_secret`/`fill_otp`), never
handed back to the model.

The **hosted secure-input channel** lets a headless Lethe (which cannot open a
browser) collect a human secret: a credential-collecting CLI POSTs a field spec to
a unix socket Lethe owns; Lethe surfaces it as a `secure_input.request` event
carrying a per-request ephemeral P-256 public key; the browser end-to-end-seals the
typed values (ECDH-P256 → HKDF-SHA256 → AES-256-GCM, with the request id and server
key bound as AAD) and the control plane relays **ciphertext only** — it never sees
plaintext and persists nothing. Lethe binds each socket connection to the PID of a
CLI child it launched itself (`SO_PEERCRED`), so a prompt-injected agent cannot
forge a card via the socket to harvest a freshly typed secret.

**Trust boundary — same uid.** An agent with shell access at the same uid as
`AGENT_ID_STATE_DIR` holds the vault's agent-key and can read the vault directly;
the boundary these tools enforce is against *accidental* transcript/context and
control-plane exposure, not against an actively adversarial agent that acts to
obtain a secret. An actively malicious control plane (it ships the frontend JS and
proxies the SSE stream) is likewise out of scope.

## Local llama.cpp Example

Start an OpenAI-compatible server:

```bash
./build/bin/llama-server \
  --model /path/to/gemma-4-31B-it-Q8_0.gguf \
  --host 0.0.0.0 --port 8090 \
  --ctx-size 98304 \
  --jinja
```

Configure Lethe:

```bash
LLM_PROVIDER=openai
LLM_MODEL=openai/gemma-4-31B-it-Q8_0.gguf
LLM_API_BASE=http://localhost:8090/v1
OPENAI_API_KEY=local
LLM_CONTEXT_LIMIT=96000
```

## Development

```bash
cargo fmt --check
cargo test --locked --no-default-features
cargo test --locked --manifest-path vendor/genai/Cargo.toml --lib
cargo build --release --locked --bin lethe
```

Build a local release archive:

```bash
cargo build --release --locked --bin lethe
scripts/package-release
ls dist/
```

Tagged pushes (`v*`) build GitHub release assets on a three-runner matrix —
`linux-x86_64`, `linux-aarch64`, and `macos-aarch64` — each producing one
`lethe-<target>.tar.gz` plus its `.sha256` checksum (`install.sh` and
`update.sh` consume these assets from the latest release). The separate
`release-migrator.yml` workflow produces the optional legacy
`lethe-migrate-<target>.tar.gz` assets and checksums. Both workflows attest
archives before upload and verify that provenance again before publication.
Linux GNU binaries are built on Ubuntu 24.04 for a glibc 2.39 floor; macOS
binaries link only against system frameworks.

Useful smoke checks:

```bash
target/release/lethe check
target/release/lethe telegram split "hello from lethe"
```

### Upstream integration policy

`origin` is the fork and `upstream` is `alien-id/lethe`. Upstream releases are
integrated as explicit merges so ancestry remains inspectable. Before a merge,
tests characterize the downstream behavior that must survive it; after the
merge, the root and vendored test suites plus every release target run again.

An upstream implementation may replace a local patch when it preserves the
same contract. Otherwise the fork keeps its behavior and documents the
divergence. The current inventory belongs in [CHANGELOG.md](CHANGELOG.md), while
the deliberately minimal `genai` patch surface is tracked separately in
[`vendor/genai/LETHE_FORK.md`](vendor/genai/LETHE_FORK.md).

## License

MIT, as declared in `Cargo.toml`. This fork retains the upstream Git history
and attribution. Vendored dependencies retain their own license files and
notices.
