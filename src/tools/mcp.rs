//! Remote MCP tools: a minimal Model Context Protocol client (Streamable
//! HTTP, JSON-RPC 2.0) that lets Lethe discover and call tools on ONE remote
//! MCP server. Configured via `MCP_SERVER_URL` (the POST endpoint, e.g.
//! `https://mcp.example.com/mcp`) + `MCP_SERVER_TOKEN` (a bearer token;
//! scoped service tokens recommended), plus an optional `MCP_SERVER_LABEL`
//! (a display name for the server, surfaced in mcp_list_tools output). When
//! unconfigured, the mcp_* tools are hidden from the model entirely
//! (`ToolCategory::Mcp`).
//!
//! The remote `tools/list` is filtered by the token's grants on the server
//! side, so what the model can discover and call is exactly what the token
//! allows. The surface here is a passthrough triple (list / describe / call)
//! rather than one native tool per remote tool: the local catalog is static
//! (`&'static [ToolDef]`), while the remote catalog is only known at runtime.
//!
//! Protocol notes: speaks Streamable HTTP — every request is a POST whose
//! response is either `application/json` or a `text/event-stream` carrying
//! the JSON-RPC response as SSE `data:` events; both are handled. A session
//! (`Mcp-Session-Id` response header) is established via `initialize` +
//! `notifications/initialized` and shared for the process. A session 404
//! permits one retry for catalog discovery; tool calls are never replayed.
//! The tool list is cached for five minutes.

use std::env;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use futures_util::StreamExt;
use serde_json::{Value, json};

use crate::tools::registry::ToolRegistry;
use crate::tools::registry::args::string_arg;
use crate::tools::spec::{ToolCategory, ToolDef, ToolExecutor, p_object, p_str, p_str_req};

const PROTOCOL_VERSION: &str = "2025-06-18";
const TOOLS_TTL: Duration = Duration::from_secs(300);
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(20);
const CALL_TIMEOUT: Duration = Duration::from_secs(120);
const RESPONSE_MAX_BYTES: usize = 2 * 1024 * 1024;
const RESULT_MAX_CHARS: usize = 30_000;
const NOT_CONFIGURED: &str =
    "Error: remote MCP server not configured (MCP_SERVER_URL/MCP_SERVER_TOKEN unset).";

fn mcp_config() -> Option<(String, String)> {
    let url = env::var("MCP_SERVER_URL")
        .ok()
        .map(|v| v.trim().trim_end_matches('/').to_string())
        .filter(|v| !v.is_empty())?;
    let token = env::var("MCP_SERVER_TOKEN")
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())?;
    Some((url, token))
}

/// Whether a remote MCP server is configured (cached: the env is fixed for
/// the process lifetime — containers are recreated to change it).
pub fn is_configured() -> bool {
    static CONFIGURED: OnceLock<bool> = OnceLock::new();
    *CONFIGURED.get_or_init(|| mcp_config().is_some())
}

fn error_json(message: &str) -> String {
    json!({ "error": message }).to_string()
}

// ── transport ────────────────────────────────────────────────────────────────

#[derive(Clone, Default)]
struct McpSession {
    id: Option<String>,
    protocol_version: Option<String>,
}

static SESSION: tokio::sync::Mutex<Option<Arc<McpSession>>> = tokio::sync::Mutex::const_new(None);
static TOOLS_CACHE: Mutex<Option<(Instant, Vec<Value>)>> = Mutex::new(None);
static REQUEST_ID: AtomicU64 = AtomicU64::new(1);

fn next_id() -> u64 {
    REQUEST_ID.fetch_add(1, Ordering::Relaxed)
}

fn http() -> &'static reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    // No global timeout: per-request timeouts differ (handshake vs tool call).
    CLIENT.get_or_init(|| reqwest::Client::builder().build().expect("reqwest client"))
}

/// POST one JSON-RPC message. Returns (http status, content-type,
/// mcp-session-id response header if any, body text).
async fn post_rpc(
    url: &str,
    token: &str,
    session: &McpSession,
    body: &Value,
    timeout: Duration,
) -> Result<(reqwest::StatusCode, String, Option<String>, String), String> {
    let mut request = http()
        .post(url)
        .bearer_auth(token)
        .header("accept", "application/json, text/event-stream")
        .json(body)
        .timeout(timeout);
    if let Some(id) = &session.id {
        request = request.header("mcp-session-id", id);
    }
    if let Some(version) = &session.protocol_version {
        request = request.header("mcp-protocol-version", version);
    }

    let response = request
        .send()
        .await
        .map_err(|e| format!("Error: MCP request failed: {e}"))?;
    let status = response.status();
    let content_type = response
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    let session_id = response
        .headers()
        .get("mcp-session-id")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let text = read_bounded_response(response).await?;
    Ok((status, content_type, session_id, text))
}

async fn read_bounded_response(response: reqwest::Response) -> Result<String, String> {
    let content_length = response.content_length();
    if content_length.is_some_and(|length| length > RESPONSE_MAX_BYTES as u64) {
        return Err(format!(
            "Error: MCP response exceeded the {RESPONSE_MAX_BYTES}-byte limit"
        ));
    }

    let initial_capacity = content_length
        .map(|length| length as usize)
        .unwrap_or_default();
    let mut body = Vec::with_capacity(initial_capacity);
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| format!("Error: MCP response read failed: {error}"))?;
        if chunk.len() > RESPONSE_MAX_BYTES.saturating_sub(body.len()) {
            return Err(format!(
                "Error: MCP response exceeded the {RESPONSE_MAX_BYTES}-byte limit"
            ));
        }
        body.extend_from_slice(&chunk);
    }

    String::from_utf8(body).map_err(|_| "Error: MCP response was not valid UTF-8".to_string())
}

/// Split an SSE body into its `data:` event payloads (multi-line data joined).
fn sse_events(body: &str) -> Vec<String> {
    let mut events = Vec::new();
    let mut data = String::new();
    for line in body.lines() {
        if let Some(rest) = line.strip_prefix("data:") {
            if !data.is_empty() {
                data.push('\n');
            }
            data.push_str(rest.strip_prefix(' ').unwrap_or(rest));
        } else if line.trim().is_empty() && !data.is_empty() {
            events.push(std::mem::take(&mut data));
        }
    }
    if !data.is_empty() {
        events.push(data);
    }
    events
}

/// Pull the JSON-RPC response matching `id` out of a Streamable HTTP body —
/// plain JSON or SSE-framed — and unwrap the result/error envelope.
fn extract_rpc_result(content_type: &str, body: &str, id: u64) -> Result<Value, String> {
    let message = if content_type.contains("text/event-stream") {
        sse_events(body)
            .iter()
            .filter_map(|payload| serde_json::from_str::<Value>(payload).ok())
            .find(|msg| msg.get("id").and_then(Value::as_u64) == Some(id))
            .ok_or_else(|| {
                "Error: MCP response stream carried no reply to this request".to_string()
            })?
    } else {
        serde_json::from_str::<Value>(body).map_err(|_| {
            let head: String = body.chars().take(200).collect();
            format!("Error: MCP server sent a non-JSON response: {head}")
        })?
    };

    if let Some(error) = message.get("error") {
        let code = error["code"].as_i64().unwrap_or(0);
        let text = error["message"].as_str().unwrap_or("unknown error");
        return Err(format!("Error: MCP remote error {code}: {text}"));
    }
    Ok(message.get("result").cloned().unwrap_or(Value::Null))
}

async fn discard_expired_session(expired: &Arc<McpSession>) {
    let mut cached = SESSION.lock().await;
    // A delayed 404 can arrive after another caller has already reconnected.
    if cached
        .as_ref()
        .is_some_and(|current| Arc::ptr_eq(current, expired))
    {
        *cached = None;
    }
}

/// Handshake: `initialize` (capturing the session id + negotiated protocol
/// version) then `notifications/initialized`. Cached for the process.
async fn ensure_session(url: &str, token: &str) -> Result<Arc<McpSession>, String> {
    // Only the handshake holds this lock across network I/O. Established
    // sessions are shared before dispatching any tool request.
    let mut cached = SESSION.lock().await;
    if let Some(session) = cached.as_ref() {
        return Ok(Arc::clone(session));
    }

    let id = next_id();
    let body = json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "initialize",
        "params": {
            "protocolVersion": PROTOCOL_VERSION,
            "capabilities": {},
            "clientInfo": {"name": "lethe", "version": env!("CARGO_PKG_VERSION")}
        }
    });

    let (status, content_type, session_id, text) =
        post_rpc(url, token, &McpSession::default(), &body, HANDSHAKE_TIMEOUT).await?;
    if !status.is_success() {
        let head: String = text.chars().take(200).collect();
        return Err(format!(
            "Error: MCP initialize failed: HTTP {status}: {head}"
        ));
    }
    let result = extract_rpc_result(&content_type, &text, id)?;

    let session = Arc::new(McpSession {
        id: session_id,
        protocol_version: Some(
            result["protocolVersion"]
                .as_str()
                .unwrap_or(PROTOCOL_VERSION)
                .to_string(),
        ),
    });

    // Best-effort: servers that track lifecycle expect it; stateless ones
    // answer 202 and move on. A failure here must not block the actual call.
    let initialized = json!({"jsonrpc": "2.0", "method": "notifications/initialized"});
    let _ = post_rpc(url, token, &session, &initialized, HANDSHAKE_TIMEOUT).await;

    *cached = Some(Arc::clone(&session));
    Ok(session)
}

#[derive(Debug)]
enum RpcCallError {
    /// Streamable HTTP defines HTTP 404 for an expired or otherwise invalid
    /// `Mcp-Session-Id`. Keep this classification structural: a JSON-RPC
    /// application error is not a transport-session signal merely because its
    /// message happens to contain the word "session".
    SessionExpired(String),
    Other(String),
}

impl RpcCallError {
    fn into_message(self) -> String {
        match self {
            Self::SessionExpired(message) | Self::Other(message) => message,
        }
    }
}

/// Only side-effect-free protocol reads may be replayed automatically after a
/// session handshake is renewed. A `tools/call` can have completed remotely
/// even when its response says the session is gone, so replaying it could
/// duplicate an external effect.
fn retry_after_session_expiry_is_safe(method: &str) -> bool {
    matches!(method, "tools/list")
}

async fn do_rpc(
    url: &str,
    token: &str,
    session: &McpSession,
    method: &str,
    params: Value,
    timeout: Duration,
) -> Result<Value, RpcCallError> {
    let id = next_id();
    let body = json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params});
    let (status, content_type, _sid, text) = post_rpc(url, token, session, &body, timeout)
        .await
        .map_err(RpcCallError::Other)?;
    if !status.is_success() {
        let head: String = text.chars().take(200).collect();
        let message = format!("Error: MCP {method} failed: HTTP {status}: {head}");
        if status == reqwest::StatusCode::NOT_FOUND && session.id.is_some() {
            return Err(RpcCallError::SessionExpired(message));
        }
        return Err(RpcCallError::Other(message));
    }
    extract_rpc_result(&content_type, &text, id).map_err(RpcCallError::Other)
}

/// One JSON-RPC request against the configured server, with the handshake
/// performed on demand. A lost/expired session is retried once only for
/// side-effect-free protocol reads; effect-bearing calls fail closed.
async fn rpc_call(method: &str, params: Value, timeout: Duration) -> Result<Value, String> {
    let Some((url, token)) = mcp_config() else {
        return Err(NOT_CONFIGURED.to_string());
    };

    rpc_call_with_config(&url, &token, method, params, timeout).await
}

async fn rpc_call_with_config(
    url: &str,
    token: &str,
    method: &str,
    params: Value,
    timeout: Duration,
) -> Result<Value, String> {
    let session = ensure_session(url, token).await?;
    match do_rpc(url, token, &session, method, params.clone(), timeout).await {
        Ok(result) => Ok(result),
        Err(RpcCallError::Other(message)) => Err(message),
        Err(RpcCallError::SessionExpired(message)) => {
            discard_expired_session(&session).await;
            if !retry_after_session_expiry_is_safe(method) {
                return Err(format!(
                    "{message}; did not retry {method} automatically because the remote operation may already have executed"
                ));
            }
            let session = ensure_session(url, token).await?;
            match do_rpc(url, token, &session, method, params, timeout).await {
                Ok(result) => Ok(result),
                Err(RpcCallError::SessionExpired(message)) => {
                    discard_expired_session(&session).await;
                    Err(message)
                }
                Err(error) => Err(error.into_message()),
            }
        }
    }
}

/// The remote tool catalog (paginated `tools/list`), cached for five minutes.
/// The config check runs BEFORE the cache so an unconfigured process can
/// never serve a stale catalog.
async fn remote_tools() -> Result<Vec<Value>, String> {
    if mcp_config().is_none() {
        return Err(NOT_CONFIGURED.to_string());
    }
    if let Some((at, tools)) = TOOLS_CACHE.lock().expect("mcp tools lock").clone()
        && at.elapsed() < TOOLS_TTL
    {
        return Ok(tools);
    }

    let mut tools: Vec<Value> = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let params = match &cursor {
            Some(c) => json!({"cursor": c}),
            None => json!({}),
        };
        let result = rpc_call("tools/list", params, HANDSHAKE_TIMEOUT).await?;
        if let Some(page) = result["tools"].as_array() {
            tools.extend(page.iter().cloned());
        }
        match result["nextCursor"].as_str() {
            Some(c) if !c.is_empty() && tools.len() < 2000 => cursor = Some(c.to_string()),
            _ => break,
        }
    }

    *TOOLS_CACHE.lock().expect("mcp tools lock") = Some((Instant::now(), tools.clone()));
    Ok(tools)
}

// ── result shaping ───────────────────────────────────────────────────────────

/// First sentence of a description (the compact list stays scannable even
/// when the remote descriptions are paragraphs).
fn first_sentence(text: &str) -> String {
    let text = text.trim();
    let cut = text.find(". ").map(|i| i + 1).unwrap_or(text.len());
    let sentence = &text[..cut];
    if sentence.chars().count() > 200 {
        let head: String = sentence.chars().take(200).collect();
        format!("{head}…")
    } else {
        sentence.to_string()
    }
}

fn truncated(text: String) -> String {
    if text.chars().count() > RESULT_MAX_CHARS {
        let head: String = text.chars().take(RESULT_MAX_CHARS).collect();
        format!("{head}\n… [truncated: result exceeded {RESULT_MAX_CHARS} chars]")
    } else {
        text
    }
}

// ── tool implementations ─────────────────────────────────────────────────────

fn server_label() -> Option<String> {
    env::var("MCP_SERVER_LABEL")
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

/// Keep broad listings compact. For a targeted search, include everything
/// needed to call the best matches directly, within the existing result budget.
fn discovery_result(tools: &[Value], filter: &str, label: Option<String>) -> Value {
    const SCHEMA_MATCH_LIMIT: usize = 3;
    let filter = filter.trim().to_lowercase();
    let mut matches = Vec::new();
    for tool in tools {
        let name = tool["name"].as_str().unwrap_or("").to_lowercase();
        let rank = if filter.is_empty() || name == filter {
            0
        } else if name.starts_with(&filter) {
            1
        } else if name.contains(&filter) {
            2
        } else if tool["description"]
            .as_str()
            .unwrap_or("")
            .to_lowercase()
            .contains(&filter)
        {
            3
        } else {
            continue;
        };
        matches.push((rank, tool));
    }
    matches.sort_by_key(|(rank, _)| *rank);

    let mut result = json!({
        "count": matches.len(),
        "tools": [],
        "note": "Use mcp_call directly with a known schema. For a summary-only match, use a narrower filter or mcp_describe_tool.",
    });
    if let Some(label) = label {
        result["server"] = Value::String(label.chars().take(200).collect());
    }
    let mut remaining = RESULT_MAX_CHARS.saturating_sub(result.to_string().len());
    let mut entries = Vec::new();
    for (index, (_, tool)) in matches.into_iter().enumerate() {
        let description = tool["description"].as_str().unwrap_or("");
        let mut entry = json!({"name": tool["name"], "summary": first_sentence(description)});
        let summary_bytes = entry.to_string().len();
        let separator = usize::from(!entries.is_empty());
        let mut bytes = summary_bytes + separator;
        if bytes > remaining {
            break;
        }
        if !filter.is_empty() && index < SCHEMA_MATCH_LIMIT && tool["inputSchema"].is_object() {
            let detail = json!({
                "name": tool["name"],
                "summary": entry["summary"],
                "description": description,
                "inputSchema": tool["inputSchema"],
            });
            let detail_bytes = detail.to_string().len() + separator;
            if detail_bytes <= remaining {
                entry = detail;
                bytes = detail_bytes;
            }
        }
        remaining -= bytes;
        entries.push(entry);
    }
    result["tools"] = Value::Array(entries);
    result
}

async fn list_tools_impl(args: &Value) -> String {
    match remote_tools().await {
        Ok(tools) => {
            discovery_result(&tools, &string_arg(args, "filter"), server_label()).to_string()
        }
        Err(error) => error,
    }
}

async fn describe_tool_impl(args: &Value) -> String {
    let name = string_arg(args, "tool");
    if name.trim().is_empty() {
        return error_json("'tool' is required (a name from mcp_list_tools).");
    }
    match remote_tools().await {
        Ok(tools) => match tools
            .iter()
            .find(|t| t["name"].as_str() == Some(name.trim()))
        {
            Some(tool) => truncated(tool.to_string()),
            None => format!(
                "Error: no remote tool named '{}' — mcp_list_tools shows what is available.",
                name.trim()
            ),
        },
        Err(error) => error,
    }
}

async fn call_impl(args: &Value) -> String {
    let name = string_arg(args, "tool");
    if name.trim().is_empty() {
        return error_json("'tool' is required (a name from mcp_list_tools).");
    }
    let arguments = args.get("arguments").cloned().unwrap_or_else(|| json!({}));

    let result = match rpc_call(
        "tools/call",
        json!({"name": name.trim(), "arguments": arguments}),
        CALL_TIMEOUT,
    )
    .await
    {
        Ok(result) => result,
        Err(error) => return error,
    };

    let is_error = result["isError"].as_bool().unwrap_or(false);
    let mut parts: Vec<String> = result["content"]
        .as_array()
        .map(|content| {
            content
                .iter()
                .filter_map(|item| item["text"].as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default();
    if let Some(structured) = result.get("structuredContent") {
        parts.push(structured.to_string());
    }

    let text = if parts.is_empty() {
        result.to_string()
    } else {
        parts.join("\n")
    };

    if is_error {
        format!(
            "Error: remote tool '{}' failed: {}",
            name.trim(),
            truncated(text)
        )
    } else {
        truncated(text)
    }
}

// ── executors + defs ─────────────────────────────────────────────────────────

fn exec_mcp_list_tools<'a>(
    _registry: &'a ToolRegistry<'a>,
    args: &'a Value,
) -> Pin<Box<dyn Future<Output = String> + Send + 'a>> {
    Box::pin(list_tools_impl(args))
}

fn exec_mcp_describe_tool<'a>(
    _registry: &'a ToolRegistry<'a>,
    args: &'a Value,
) -> Pin<Box<dyn Future<Output = String> + Send + 'a>> {
    Box::pin(describe_tool_impl(args))
}

fn exec_mcp_call<'a>(
    _registry: &'a ToolRegistry<'a>,
    args: &'a Value,
) -> Pin<Box<dyn Future<Output = String> + Send + 'a>> {
    Box::pin(call_impl(args))
}

pub const TOOL_DEFS: &[ToolDef] = &[
    ToolDef {
        name: "mcp_list_tools",
        description: "Search the remote tools granted to this agent. Use a specific filter to get input schemas and full descriptions for the top matches, then execute with mcp_call. Omit the filter for a compact catalog.",
        params: &[p_str(
            "filter",
            "Case-insensitive substring to match against tool names and descriptions.",
        )],
        category: ToolCategory::Mcp,
        execute: ToolExecutor::Async(exec_mcp_list_tools),
    },
    ToolDef {
        name: "mcp_describe_tool",
        description: "Full definition of ONE tool on the configured remote MCP server: complete description plus its JSON input schema (parameter names, types, required fields). Use this when you need a definition omitted from a search result.",
        params: &[p_str_req(
            "tool",
            "Tool name exactly as listed by mcp_list_tools.",
        )],
        category: ToolCategory::Mcp,
        execute: ToolExecutor::Async(exec_mcp_describe_tool),
    },
    ToolDef {
        name: "mcp_call",
        description: "Call a known remote tool directly with arguments matching its schema. A committed write receipt is sufficient confirmation. Effects happen on the server under this agent's token. If the server stages an effect for out-of-band approval instead of executing it (e.g. the result reports a pending status with a request id), report that status honestly and follow the server's own instructions for checking the outcome — never claim the action completed.",
        params: &[
            p_str_req("tool", "Tool name exactly as listed by mcp_list_tools."),
            p_object(
                "arguments",
                "Arguments object for the tool, matching its input schema. Omit for tools without parameters.",
            ),
        ],
        category: ToolCategory::Mcp,
        execute: ToolExecutor::Async(exec_mcp_call),
    },
];

#[cfg(test)]
mod tests {
    use super::*;
    use axum::Router;
    use axum::body::Body;
    use axum::response::IntoResponse;
    use axum::routing::post;
    use std::sync::atomic::AtomicUsize;

    static ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    fn reset_state() {
        *SESSION.try_lock().unwrap() = None;
        *TOOLS_CACHE.lock().unwrap() = None;
    }

    fn clear_env() {
        unsafe {
            env::remove_var("MCP_SERVER_URL");
            env::remove_var("MCP_SERVER_TOKEN");
            env::remove_var("MCP_SERVER_LABEL");
        }
    }

    #[test]
    fn sse_bodies_and_json_bodies_both_unwrap() {
        let sse = "event: message\ndata: {\"jsonrpc\":\"2.0\",\"id\":7,\"result\":{\"ok\":true}}\n\ndata: {\"jsonrpc\":\"2.0\",\"id\":8,\"result\":{}}\n\n";
        let result = extract_rpc_result("text/event-stream", sse, 7).unwrap();
        assert_eq!(result["ok"], Value::Bool(true));

        let json_body = "{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"tools\":[]}}";
        let result = extract_rpc_result("application/json", json_body, 1).unwrap();
        assert!(result["tools"].as_array().unwrap().is_empty());

        let error_body =
            "{\"jsonrpc\":\"2.0\",\"id\":1,\"error\":{\"code\":-32601,\"message\":\"nope\"}}";
        let error = extract_rpc_result("application/json", error_body, 1).unwrap_err();
        assert!(error.contains("-32601"));
        assert!(error.contains("nope"));
    }

    #[test]
    fn first_sentence_trims_to_the_first_period() {
        assert_eq!(
            first_sentence("Keep-in-touch nudges. Second sentence."),
            "Keep-in-touch nudges."
        );
        assert_eq!(first_sentence("No trailing period"), "No trailing period");
    }

    #[test]
    fn discovery_prioritizes_named_tools_and_preserves_whole_schemas() {
        let names = ["other", "find_logs", "logs_read", "logs", "logs_write"];
        let mut tools: Vec<_> = names
            .iter()
            .map(|name| {
                json!({
                    "name": name, "description": "Reads logs.",
                    "inputSchema": {"type": "object", "required": ["id"]},
                })
            })
            .collect();
        let result = discovery_result(&tools, " LOGS ", None);
        let entries = result["tools"].as_array().unwrap();
        assert_eq!(result["count"], names.len());
        assert_eq!(
            entries
                .iter()
                .map(|row| row["name"].as_str().unwrap())
                .collect::<Vec<_>>(),
            ["logs", "logs_read", "logs_write", "find_logs", "other"]
        );
        for entry in &entries[..3] {
            assert_eq!(entry["inputSchema"]["required"], json!(["id"]));
        }
        assert!(
            entries[3..]
                .iter()
                .all(|row| row.get("inputSchema").is_none())
        );
        tools[3]["inputSchema"]["description"] = json!("📝".repeat(RESULT_MAX_CHARS));
        let result = discovery_result(&tools, "logs", None);
        assert!(result.to_string().len() <= RESULT_MAX_CHARS);
        assert_eq!(result["count"], names.len());
        assert!(result["tools"][0].get("inputSchema").is_none());
        assert!(result["tools"][1]["inputSchema"].is_object());
        assert!(
            discovery_result(&tools, "", None)["tools"]
                .as_array()
                .unwrap()
                .iter()
                .all(|row| row.get("inputSchema").is_none())
        );
    }

    #[test]
    fn mcp_tool_contract_stays_static_and_deployment_agnostic() {
        assert_eq!(
            TOOL_DEFS.iter().map(|def| def.name).collect::<Vec<_>>(),
            ["mcp_list_tools", "mcp_describe_tool", "mcp_call"]
        );
        assert!(
            TOOL_DEFS
                .iter()
                .all(|def| def.category == ToolCategory::Mcp)
        );
        for def in TOOL_DEFS {
            assert!(!def.description.contains("Umbrella"));
            assert!(!def.description.contains("Philipp"));
        }
    }

    #[tokio::test]
    async fn unconfigured_returns_structured_errors() {
        let _guard = ENV_LOCK.lock().await;
        clear_env();
        reset_state();

        assert!(list_tools_impl(&json!({})).await.contains("not configured"));
        assert!(
            describe_tool_impl(&json!({"tool": "x"}))
                .await
                .contains("not configured")
        );
        assert!(
            call_impl(&json!({"tool": "x"}))
                .await
                .contains("not configured")
        );

        // argument validation fires before any dial
        assert!(call_impl(&json!({})).await.contains("'tool' is required"));
        assert!(
            describe_tool_impl(&json!({}))
                .await
                .contains("'tool' is required")
        );
    }

    // A fake Streamable HTTP MCP server: initialize hands out a session id and
    // every later request must carry it (exercising the header plumbing).
    async fn fake_mcp(headers: axum::http::HeaderMap, body: String) -> axum::response::Response {
        let msg: Value = serde_json::from_str(&body).unwrap_or_else(|_| json!({}));
        let method = msg["method"].as_str().unwrap_or("");
        let id = msg["id"].clone();

        if headers.get("authorization").and_then(|v| v.to_str().ok()) != Some("Bearer test-token") {
            return axum::http::StatusCode::UNAUTHORIZED.into_response();
        }

        if method != "initialize" {
            let sid = headers.get("mcp-session-id").and_then(|v| v.to_str().ok());
            if sid != Some("sess-1") {
                return (axum::http::StatusCode::NOT_FOUND, "no session").into_response();
            }
            let version = headers
                .get("mcp-protocol-version")
                .and_then(|v| v.to_str().ok());
            if version != Some(PROTOCOL_VERSION) {
                return axum::http::StatusCode::BAD_REQUEST.into_response();
            }
        }

        match method {
            "initialize" => (
                [("mcp-session-id", "sess-1")],
                axum::Json(json!({"jsonrpc": "2.0", "id": id, "result": {
                    "protocolVersion": PROTOCOL_VERSION,
                    "capabilities": {"tools": {}},
                    "serverInfo": {"name": "fake", "version": "0"}
                }})),
            )
                .into_response(),
            "notifications/initialized" => axum::http::StatusCode::ACCEPTED.into_response(),
            "tools/list" => axum::Json(json!({"jsonrpc": "2.0", "id": id, "result": {"tools": [
                {"name": "crm_get_overdue", "description": "Keep-in-touch nudges. Second sentence with detail.", "inputSchema": {"type": "object", "properties": {"limit": {"type": "integer"}}}},
                {"name": "time_get_current_time", "description": "Current time.", "inputSchema": {"type": "object"}}
            ]}}))
            .into_response(),
            "tools/call" => {
                let name = msg["params"]["name"].as_str().unwrap_or("");
                if name == "boom" {
                    axum::Json(json!({"jsonrpc": "2.0", "id": id, "result": {
                        "isError": true,
                        "content": [{"type": "text", "text": "kaboom"}]
                    }}))
                    .into_response()
                } else {
                    let echo = msg["params"]["arguments"].to_string();
                    axum::Json(json!({"jsonrpc": "2.0", "id": id, "result": {
                        "content": [{"type": "text", "text": format!("called {name} with {echo}")}]
                    }}))
                    .into_response()
                }
            }
            _ => axum::Json(
                json!({"jsonrpc": "2.0", "id": id, "error": {"code": -32601, "message": "unknown method"}}),
            )
            .into_response(),
        }
    }

    #[derive(Clone, Copy)]
    enum RetryProbeMode {
        ApplicationSessionError,
        ExpiredToolCall,
        ExpiredToolListOnce,
        DelayedExpiredToolCall,
    }

    struct RetryProbe {
        mode: RetryProbeMode,
        initialize_requests: AtomicUsize,
        method_requests: AtomicUsize,
        received_call: tokio::sync::Notify,
        release_call: tokio::sync::Notify,
    }

    async fn retry_probe(
        axum::extract::State(probe): axum::extract::State<Arc<RetryProbe>>,
        headers: axum::http::HeaderMap,
        body: String,
    ) -> axum::response::Response {
        let message: Value = serde_json::from_str(&body).unwrap_or_else(|_| json!({}));
        let method = message["method"].as_str().unwrap_or("");
        let id = message["id"].clone();

        match method {
            "initialize" => {
                let attempt = probe.initialize_requests.fetch_add(1, Ordering::SeqCst) + 1;
                let mut response = axum::Json(json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "result": {
                        "protocolVersion": PROTOCOL_VERSION,
                        "capabilities": {"tools": {}},
                        "serverInfo": {"name": "retry-probe", "version": "0"}
                    }
                }))
                .into_response();
                response.headers_mut().insert(
                    "mcp-session-id",
                    format!("probe-session-{attempt}").parse().unwrap(),
                );
                response
            }
            "notifications/initialized" => axum::http::StatusCode::ACCEPTED.into_response(),
            "tools/call" => {
                probe.method_requests.fetch_add(1, Ordering::SeqCst);
                match probe.mode {
                    RetryProbeMode::ApplicationSessionError => axum::Json(json!({
                        "jsonrpc": "2.0",
                        "id": id,
                        "error": {
                            "code": -32042,
                            "message": "application session conflict"
                        }
                    }))
                    .into_response(),
                    RetryProbeMode::DelayedExpiredToolCall => {
                        probe.received_call.notify_one();
                        probe.release_call.notified().await;
                        axum::http::StatusCode::NOT_FOUND.into_response()
                    }
                    RetryProbeMode::ExpiredToolCall => {
                        (axum::http::StatusCode::NOT_FOUND, "expired Mcp-Session-Id")
                            .into_response()
                    }
                    RetryProbeMode::ExpiredToolListOnce => {
                        axum::http::StatusCode::INTERNAL_SERVER_ERROR.into_response()
                    }
                }
            }
            "tools/list" => {
                let attempt = probe.method_requests.fetch_add(1, Ordering::SeqCst) + 1;
                match probe.mode {
                    RetryProbeMode::ExpiredToolListOnce if attempt == 1 => {
                        (axum::http::StatusCode::NOT_FOUND, "expired Mcp-Session-Id")
                            .into_response()
                    }
                    RetryProbeMode::DelayedExpiredToolCall
                        if headers.get("mcp-session-id").and_then(|h| h.to_str().ok())
                            == Some("probe-session-1") =>
                    {
                        axum::http::StatusCode::NOT_FOUND.into_response()
                    }
                    RetryProbeMode::ExpiredToolListOnce
                    | RetryProbeMode::DelayedExpiredToolCall => axum::Json(json!({
                        "jsonrpc": "2.0",
                        "id": id,
                        "result": {"tools": []}
                    }))
                    .into_response(),
                    _ => axum::http::StatusCode::INTERNAL_SERVER_ERROR.into_response(),
                }
            }
            _ => axum::http::StatusCode::BAD_REQUEST.into_response(),
        }
    }

    async fn start_retry_probe(mode: RetryProbeMode) -> (String, Arc<RetryProbe>) {
        let probe = Arc::new(RetryProbe {
            mode,
            initialize_requests: AtomicUsize::new(0),
            method_requests: AtomicUsize::new(0),
            received_call: tokio::sync::Notify::new(),
            release_call: tokio::sync::Notify::new(),
        });
        let app = Router::new()
            .route("/mcp", post(retry_probe))
            .with_state(probe.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (format!("http://{addr}/mcp"), probe)
    }

    #[tokio::test]
    async fn concurrent_cold_callers_share_one_handshake() {
        let _guard = ENV_LOCK.lock().await;
        reset_state();
        let (url, probe) = start_retry_probe(RetryProbeMode::ApplicationSessionError).await;
        let (a, b) = tokio::join!(
            ensure_session(&url, "test-token"),
            ensure_session(&url, "test-token"),
        );
        assert_eq!(a.unwrap().id, b.unwrap().id);
        assert_eq!(probe.initialize_requests.load(Ordering::SeqCst), 1);
        reset_state();
    }

    #[tokio::test]
    async fn delayed_tool_call_404_preserves_the_replacement_session() {
        let _guard = ENV_LOCK.lock().await;
        reset_state();
        let (url, probe) = start_retry_probe(RetryProbeMode::DelayedExpiredToolCall).await;
        let pending_write = rpc_call_with_config(
            &url,
            "test-token",
            "tools/call",
            json!({"name": "write"}),
            HANDSHAKE_TIMEOUT,
        );
        let renew = async {
            probe.received_call.notified().await;
            let result = rpc_call_with_config(
                &url,
                "test-token",
                "tools/list",
                json!({}),
                HANDSHAKE_TIMEOUT,
            )
            .await;
            probe.release_call.notify_one();
            result
        };
        let (write, list) = tokio::time::timeout(HANDSHAKE_TIMEOUT, async {
            tokio::join!(pending_write, renew)
        })
        .await
        .expect("the delayed response must not block session renewal");

        assert_eq!(list.unwrap()["tools"], json!([]));
        assert!(write.unwrap_err().contains("did not retry tools/call"));
        let active = ensure_session(&url, "test-token").await.unwrap();
        assert_eq!(active.id.as_deref(), Some("probe-session-2"));
        assert_eq!(probe.initialize_requests.load(Ordering::SeqCst), 2);
        assert_eq!(probe.method_requests.load(Ordering::SeqCst), 3);
        reset_state();
    }

    #[tokio::test]
    async fn application_error_mentioning_session_is_not_retried_or_reclassified() {
        let _guard = ENV_LOCK.lock().await;
        reset_state();
        let (url, probe) = start_retry_probe(RetryProbeMode::ApplicationSessionError).await;

        let error = rpc_call_with_config(
            &url,
            "test-token",
            "tools/call",
            json!({"name": "calendar_create", "arguments": {}}),
            HANDSHAKE_TIMEOUT,
        )
        .await
        .unwrap_err();

        assert!(error.contains("application session conflict"));
        assert!(!error.contains("did not retry"));
        assert_eq!(probe.initialize_requests.load(Ordering::SeqCst), 1);
        assert_eq!(probe.method_requests.load(Ordering::SeqCst), 1);
        assert!(
            SESSION.try_lock().unwrap().is_some(),
            "application errors keep the session"
        );
        reset_state();
    }

    #[tokio::test]
    async fn expired_session_never_automatically_replays_a_tool_call() {
        let _guard = ENV_LOCK.lock().await;
        reset_state();
        let (url, probe) = start_retry_probe(RetryProbeMode::ExpiredToolCall).await;

        let error = rpc_call_with_config(
            &url,
            "test-token",
            "tools/call",
            json!({"name": "calendar_create", "arguments": {"title": "Meeting"}}),
            HANDSHAKE_TIMEOUT,
        )
        .await
        .unwrap_err();

        assert!(error.contains("did not retry tools/call automatically"));
        assert!(error.contains("may already have executed"));
        assert_eq!(probe.initialize_requests.load(Ordering::SeqCst), 1);
        assert_eq!(probe.method_requests.load(Ordering::SeqCst), 1);
        assert!(
            SESSION.try_lock().unwrap().is_none(),
            "expired session cache is cleared"
        );
    }

    #[tokio::test]
    async fn expired_session_retries_a_side_effect_free_tool_list_once() {
        let _guard = ENV_LOCK.lock().await;
        reset_state();
        let (url, probe) = start_retry_probe(RetryProbeMode::ExpiredToolListOnce).await;

        let result = rpc_call_with_config(
            &url,
            "test-token",
            "tools/list",
            json!({}),
            HANDSHAKE_TIMEOUT,
        )
        .await
        .unwrap();

        assert_eq!(result["tools"], json!([]));
        assert_eq!(probe.initialize_requests.load(Ordering::SeqCst), 2);
        assert_eq!(probe.method_requests.load(Ordering::SeqCst), 2);
        assert!(SESSION.try_lock().unwrap().is_some());
        reset_state();
    }

    fn fixed_body(size: usize) -> axum::response::Response {
        axum::response::Response::builder()
            .header("content-type", "application/json")
            .body(Body::from(vec![b' '; size]))
            .unwrap()
    }

    async fn exact_limit_body() -> axum::response::Response {
        fixed_body(RESPONSE_MAX_BYTES)
    }

    async fn oversized_fixed_body() -> axum::response::Response {
        fixed_body(RESPONSE_MAX_BYTES + 1)
    }

    async fn oversized_chunked_sse_body() -> axum::response::Response {
        let chunks = [
            Ok::<_, std::convert::Infallible>(vec![b'a'; RESPONSE_MAX_BYTES / 2]),
            Ok(vec![b'b'; RESPONSE_MAX_BYTES / 2]),
            Ok(vec![b'c']),
        ];
        axum::response::Response::builder()
            .header("content-type", "text/event-stream")
            .body(Body::from_stream(futures_util::stream::iter(chunks)))
            .unwrap()
    }

    #[tokio::test]
    async fn response_body_limit_rejects_fixed_and_chunked_overflow() {
        let app = Router::new()
            .route("/exact", post(exact_limit_body))
            .route("/fixed", post(oversized_fixed_body))
            .route("/chunked", post(oversized_chunked_sse_body));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let request = json!({"jsonrpc": "2.0", "id": 1, "method": "test"});
        let (_, _, _, exact) = post_rpc(
            &format!("http://{addr}/exact"),
            "test-token",
            &McpSession::default(),
            &request,
            HANDSHAKE_TIMEOUT,
        )
        .await
        .unwrap();
        assert_eq!(exact.len(), RESPONSE_MAX_BYTES);

        for path in ["fixed", "chunked"] {
            let error = post_rpc(
                &format!("http://{addr}/{path}"),
                "test-token",
                &McpSession::default(),
                &request,
                HANDSHAKE_TIMEOUT,
            )
            .await
            .unwrap_err();
            assert!(error.contains("response exceeded"), "{error}");
            assert!(error.contains(&RESPONSE_MAX_BYTES.to_string()), "{error}");
        }
    }

    #[tokio::test]
    async fn round_trips_against_an_in_process_server() {
        let _guard = ENV_LOCK.lock().await;

        let methods = Arc::new(Mutex::new(Vec::<String>::new()));
        let observed = Arc::clone(&methods);
        let app = Router::new().route(
            "/mcp",
            post(move |headers, body: String| {
                let msg: Value = serde_json::from_str(&body).unwrap();
                observed
                    .lock()
                    .unwrap()
                    .push(msg["method"].as_str().unwrap().to_owned());
                fake_mcp(headers, body)
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        unsafe {
            env::set_var("MCP_SERVER_URL", format!("http://{addr}/mcp"));
            env::set_var("MCP_SERVER_TOKEN", "test-token");
            env::set_var("MCP_SERVER_LABEL", "testhub");
        }
        reset_state();

        // A known write goes straight through without discovering or reading it back.
        let receipt = call_impl(&json!({"tool":"append", "arguments":{"text":"example"}})).await;
        assert!(receipt.contains("called append"));
        assert_eq!(
            *methods.lock().unwrap(),
            ["initialize", "notifications/initialized", "tools/call"]
        );

        // list: both tools, compact one-sentence summaries, the optional label
        let listed = list_tools_impl(&json!({})).await;
        assert!(listed.contains("crm_get_overdue"));
        assert!(listed.contains("time_get_current_time"));
        assert!(listed.contains("Keep-in-touch nudges."));
        assert!(!listed.contains("Second sentence"));
        assert!(listed.contains("testhub"));

        // A filtered search supplies the complete input schema and usage guidance.
        let filtered = list_tools_impl(&json!({"filter": "crm"})).await;
        assert!(filtered.contains("crm_get_overdue"));
        assert!(!filtered.contains("time_get_current_time"));
        let discovery: Value = serde_json::from_str(&filtered).unwrap();
        let matched = &discovery["tools"][0];
        assert_eq!(
            matched["inputSchema"]["properties"]["limit"]["type"],
            "integer"
        );
        assert_eq!(
            matched["description"],
            "Keep-in-touch nudges. Second sentence with detail."
        );

        // describe: the full schema comes through
        let described = describe_tool_impl(&json!({"tool": "crm_get_overdue"})).await;
        assert!(described.contains("inputSchema"));
        assert!(described.contains("limit"));

        let missing = describe_tool_impl(&json!({"tool": "nope"})).await;
        assert!(missing.starts_with("Error:"));

        // call: arguments echo back through the session-guarded path
        let called = call_impl(
            &json!({"tool": "time_get_current_time", "arguments": {"timezone": "Europe/Berlin"}}),
        )
        .await;
        assert!(called.contains("called time_get_current_time"));
        assert!(called.contains("Europe/Berlin"));

        // a remote isError result comes back as a structured error string
        let failed = call_impl(&json!({"tool": "boom"})).await;
        assert!(failed.starts_with("Error:"));
        assert!(failed.contains("kaboom"));
        assert_eq!(
            *methods.lock().unwrap(),
            [
                "initialize",
                "notifications/initialized",
                "tools/call",
                "tools/list",
                "tools/call",
                "tools/call"
            ]
        );

        clear_env();
        reset_state();
    }
}
