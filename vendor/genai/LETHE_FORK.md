# Lethe fork of genai 0.6.5

A vendored fork of [genai](https://github.com/jeremychone/rust-genai) v0.6.5,
applied via `[patch.crates-io]` in the workspace `Cargo.toml`.

## Why fork

### Prompt caching

The first reason is **per-message `cache_control` on the OpenAI adapter.**

Upstream reads `cache_control` at the *request* level and maps it to OpenAI's
native `prompt_cache_retention` field. It never emits a per-message marker on
the OpenAI path. That does not cover the route Lethe needs:

OpenRouter speaks the OpenAI protocol, but it *relays* per-message
`cache_control` on to whichever vendor actually serves the model (converting
between the Anthropic and OpenAI marker formats). It is how prompt caching
reaches Anthropic, Gemini and Qwen through OpenRouter — and Anthropic is the
only vendor accepting the extended `ttl: "1h"`. Without the marker, an
always-on assistant re-bills its entire system prompt on every turn, which is
operationally catastrophic on input-token cost.

See <https://openrouter.ai/docs/features/prompt-caching>.

### Provider response resource bounds

Provider-controlled response bodies and streaming frames must not be able to
grow process memory without a fixed upper bound. This fork therefore rejects,
rather than truncates, oversized non-streaming bodies, streaming HTTP error
bodies, raw chunks, framed events, event fan-out, captured stream data, and
OpenAI-compatible tool-call indexes.

The limits are intentionally internal and require no new configuration;
breaches surface through explicit error variants:

- non-streaming success bodies: 16 MiB;
- HTTP error bodies: 64 KiB;
- raw streaming chunks: 16 MiB;
- individual framed events: 1 MiB;
- raw and captured stream events: 65,536 each;
- captured stream data: 16 MiB;
- captured tool calls and OpenAI-compatible tool-call indexes: 128.

## Patch surface (vs upstream 0.6.5)

Prompt-caching changes:

- `src/adapter/adapters/openai/adapter_shared.rs`: in
  `into_openai_request_parts()`, capture `cache_control` off each message and,
  for system messages carrying one, emit the content as a parts array with a
  `cache_control` field instead of a plain string. Messages without a marker
  are byte-identical to upstream. Direct OpenAI silently drops unknown fields,
  so this is safe on both paths. Covered by the `lethe_fork_*` tests in that
  file's `tests` module.
- `src/adapter/adapters/anthropic/adapter_shared.rs`: `cache_control_to_json()`
  visibility widened from private to `pub(in crate::adapter::adapters)` so the
  OpenAI path can reuse it. No behaviour change. The wire format we emit *is*
  Anthropic's, so both paths must agree on this mapping by construction rather
  than through a copy that can drift.
- `src/adapter/adapters/openai_resp/adapter_impl.rs`: one added test,
  `lethe_fork_agent_turn_on_a_gpt5_reasoning_model`. No production code touched.
  It pins the request shape a Lethe agent turn produces for gpt-5 — the URL,
  `max_output_tokens`, tools surviving, and no `temperature` — since that route
  is the whole reason those models are sent to the Responses API.
- `Cargo.toml`: the published crate's `[[example]]`/`[[test]]` target
  declarations are removed — the `examples/` and `tests/` directories are not
  vendored, and the phantom targets break `cargo test` inside the fork.

Resource-bound changes:

- `src/error.rs` and `src/webc/error.rs`: add explicit resource-limit and
  invalid-index errors.
- `src/webc/web_client.rs`: collect non-streaming success and error bodies
  incrementally under separate byte limits.
- `src/webc/web_stream.rs`: bound streaming error bodies, raw chunks,
  delimiter/SSE partial frames, parsed events, and queued event fan-out; expose
  those internal limits crate-wide so Bedrock's custom parser reuses them.
- `src/adapter/adapters/support.rs`: centralize byte/event accounting for
  provider stream capture, cap captured tool-call counts, and keep growable
  capture fields private so adapters cannot append around those controls.
- `src/adapter/adapters/anthropic/streamer.rs`: route text, reasoning, and tool
  input capture through the shared budget.
- `src/adapter/adapters/cohere/streamer.rs`: route captured text through the
  shared budget; Cohere framing is bounded in `web_stream.rs`.
- `src/adapter/adapters/gemini/streamer.rs`: route text, reasoning, tool calls,
  and thought signatures through the shared budget; Gemini pretty-JSON framing
  is bounded in `web_stream.rs`.
- `src/adapter/adapters/openai/streamer.rs`: route captured content and tool
  fragments through the shared budget, require dense tool-call indexes, and
  reject indexes at or above the fixed cap instead of resizing a vector.
- `src/adapter/adapters/openai_resp/streamer.rs`: apply the same shared budget
  to Responses text, reasoning, encrypted signatures, and incremental tool
  arguments, and reject tool output indexes at or above the fixed cap.
- `src/adapter/adapters/ollama/streamer.rs`: route text, reasoning, and tool
  capture through the shared byte/event and tool-count limits.
- `src/adapter/adapters/bedrock/shared.rs`: collect streaming HTTP error
  bodies under the shared 64 KiB limit while preserving bounded error details.
- `src/adapter/adapters/bedrock/streamer.rs`: bound AWS event-stream chunks,
  frame buffering, frame sizes, and event counts; reject impossible header
  lengths before slicing; and route text, reasoning, and tool capture through
  the shared byte/event and tool-count limits.

Files not listed above remain byte-identical to upstream 0.6.5. To audit the
divergence:

```sh
cargo package --list  # or diff src/ against a pristine 0.6.5 extract
```

## What upstream absorbed (do not re-add)

The 0.5.3 fork carried four more patches. All are now unnecessary — this is
recorded so nobody re-applies them:

- **1h cache TTL.** `CacheControl::Ephemeral1h` is upstream and emits
  `{"type": "ephemeral", "ttl": "1h"}` on the Anthropic path, identical to the
  old `CacheControl::Persistent`. Lethe maps `CacheHint::Persistent` to it in
  `cache_hint_to_genai()`.
- **`max_completion_tokens`.** Upstream picks the right key for gpt-5 / o-series
  on Chat Completions.
- **`temperature`/`top_p` dropped for reasoning models.** Moved *out* of the
  adapter and into Lethe's `LlmRouterConfig::chat_options()`. Upstream still
  emits `temperature` unconditionally, so the rule is still needed — it just
  belongs to the caller. An adapter that silently swaps a caller's `0.7` for the
  default is lying about what it sent; only the caller knows the request is
  headed for a model that cannot honour it.
- **`reasoning_effort: "none"` when tools are present.** This was an interim
  unblock for gpt-5.x on Chat Completions, which rejects function tools combined
  with any non-`"none"` reasoning effort. It bought tools at the cost of
  reasoning. It is obsolete: upstream 0.6.5 ships a real Responses streamer
  (`openai_resp/streamer.rs`), and Lethe now routes direct-OpenAI gpt-5
  reasoning models to `AdapterKind::OpenAIResp` (see `adapter_for()` in
  `src/llm/client.rs`). `/v1/responses` supports tools *and* reasoning together.

## Tracking upstream

The remaining patches are genuine upstream gaps, not workarounds. The
per-message `cache_control` passthrough on the OpenAI adapter is worth filing
as a feature request for OpenRouter-style relays. Equivalent provider-response
resource bounds would also be useful upstream. If upstream takes either
change, drop the corresponding patch; if it takes both, drop this fork and
depend on the released crate.

Note that upstream's `AdapterKind::from_model` already routes `gpt-5*` to
`OpenAIResp` on its own; Lethe's `adapter_for()` only matters because Lethe
pins an explicit adapter in its `ServiceTargetResolver`, which bypasses that
inference.

See <https://github.com/jeremychone/rust-genai> for issues.
