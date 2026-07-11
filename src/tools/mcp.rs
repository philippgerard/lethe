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
//! `notifications/initialized` and cached for the process; a 404 or
//! session-shaped error drops the cache and retries once. The tool list is
//! cached for five minutes.

use std::env;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use serde_json::{Value, json};

use crate::tools::registry::ToolRegistry;
use crate::tools::registry::args::string_arg;
use crate::tools::spec::{ToolCategory, ToolDef, ToolExecutor, p_object, p_str, p_str_req};

const PROTOCOL_VERSION: &str = "2025-06-18";
const TOOLS_TTL: Duration = Duration::from_secs(300);
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(20);
const CALL_TIMEOUT: Duration = Duration::from_secs(120);
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

static SESSION: Mutex<Option<McpSession>> = Mutex::new(None);
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
    let text = response.text().await.unwrap_or_default();
    Ok((status, content_type, session_id, text))
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

fn cached_session() -> Option<McpSession> {
    SESSION.lock().expect("mcp session lock").clone()
}

fn store_session(session: Option<McpSession>) {
    *SESSION.lock().expect("mcp session lock") = session;
}

/// Handshake: `initialize` (capturing the session id + negotiated protocol
/// version) then `notifications/initialized`. Cached for the process.
async fn ensure_session(url: &str, token: &str) -> Result<McpSession, String> {
    if let Some(session) = cached_session() {
        return Ok(session);
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

    let session = McpSession {
        id: session_id,
        protocol_version: Some(
            result["protocolVersion"]
                .as_str()
                .unwrap_or(PROTOCOL_VERSION)
                .to_string(),
        ),
    };

    // Fire-and-forget: servers that track lifecycle expect it; stateless ones
    // answer 202 and move on. A failure here must not block the actual call.
    let initialized = json!({"jsonrpc": "2.0", "method": "notifications/initialized"});
    let _ = post_rpc(url, token, &session, &initialized, HANDSHAKE_TIMEOUT).await;

    store_session(Some(session.clone()));
    Ok(session)
}

fn session_expired(error: &str) -> bool {
    error.contains("HTTP 404") || error.to_lowercase().contains("session")
}

async fn do_rpc(
    url: &str,
    token: &str,
    session: &McpSession,
    method: &str,
    params: Value,
    timeout: Duration,
) -> Result<Value, String> {
    let id = next_id();
    let body = json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params});
    let (status, content_type, _sid, text) = post_rpc(url, token, session, &body, timeout).await?;
    if !status.is_success() {
        let head: String = text.chars().take(200).collect();
        return Err(format!("Error: MCP {method} failed: HTTP {status}: {head}"));
    }
    extract_rpc_result(&content_type, &text, id)
}

/// One JSON-RPC request against the configured server, with the handshake
/// performed on demand and a single retry on a lost/expired session.
async fn rpc_call(method: &str, params: Value, timeout: Duration) -> Result<Value, String> {
    let Some((url, token)) = mcp_config() else {
        return Err(NOT_CONFIGURED.to_string());
    };

    let session = ensure_session(&url, &token).await?;
    match do_rpc(&url, &token, &session, method, params.clone(), timeout).await {
        Err(error) if session_expired(&error) => {
            store_session(None);
            let session = ensure_session(&url, &token).await?;
            do_rpc(&url, &token, &session, method, params, timeout).await
        }
        other => other,
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

async fn list_tools_impl(args: &Value) -> String {
    let filter = string_arg(args, "filter").trim().to_lowercase();
    match remote_tools().await {
        Ok(tools) => {
            let entries: Vec<Value> = tools
                .iter()
                .filter(|tool| {
                    if filter.is_empty() {
                        return true;
                    }
                    let name = tool["name"].as_str().unwrap_or("").to_lowercase();
                    let description = tool["description"].as_str().unwrap_or("").to_lowercase();
                    name.contains(&filter) || description.contains(&filter)
                })
                .map(|tool| {
                    json!({
                        "name": tool["name"],
                        "summary": first_sentence(tool["description"].as_str().unwrap_or("")),
                    })
                })
                .collect();

            let mut result = json!({
                "count": entries.len(),
                "tools": entries,
                "note": "mcp_describe_tool gives a tool's parameters; mcp_call executes it.",
            });
            if let Some(label) = server_label() {
                result["server"] = Value::String(label);
            }
            truncated(result.to_string())
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
        description: "List the tools available on the configured remote MCP server. Returns compact {name, summary} entries — the visible set is whatever the server grants this agent's token, so listing is also how you discover what the server offers. Start here, then mcp_describe_tool for a tool's parameters and mcp_call to execute it. The catalog is cached for a few minutes.",
        params: &[p_str(
            "filter",
            "Case-insensitive substring to match against tool names and descriptions.",
        )],
        category: ToolCategory::Mcp,
        execute: ToolExecutor::Async(exec_mcp_list_tools),
    },
    ToolDef {
        name: "mcp_describe_tool",
        description: "Full definition of ONE tool on the configured remote MCP server: complete description plus its JSON input schema (parameter names, types, required fields). Read this before the first mcp_call of an unfamiliar tool — the summaries from mcp_list_tools omit parameters.",
        params: &[p_str_req(
            "tool",
            "Tool name exactly as listed by mcp_list_tools.",
        )],
        category: ToolCategory::Mcp,
        execute: ToolExecutor::Async(exec_mcp_describe_tool),
    },
    ToolDef {
        name: "mcp_call",
        description: "Execute a tool on the configured remote MCP server with JSON arguments matching its schema (mcp_describe_tool). Returns the tool's text result. Effects happen on the server under this agent's token. If the server stages an effect for out-of-band approval instead of executing it (e.g. the result reports a pending status with a request id), report that status honestly and follow the server's own instructions for checking the outcome — never claim the action completed.",
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
    use axum::response::IntoResponse;
    use axum::routing::post;

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    fn reset_state() {
        store_session(None);
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

    #[tokio::test]
    async fn unconfigured_returns_structured_errors() {
        let _guard = env_lock().lock().unwrap();
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

        if method != "initialize" {
            let sid = headers.get("mcp-session-id").and_then(|v| v.to_str().ok());
            if sid != Some("sess-1") {
                return (axum::http::StatusCode::NOT_FOUND, "no session").into_response();
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

    #[tokio::test]
    async fn round_trips_against_an_in_process_server() {
        let _guard = env_lock().lock().unwrap();

        let app = Router::new().route("/mcp", post(fake_mcp));
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

        // list: both tools, compact one-sentence summaries, the optional label
        let listed = list_tools_impl(&json!({})).await;
        assert!(listed.contains("crm_get_overdue"));
        assert!(listed.contains("time_get_current_time"));
        assert!(listed.contains("Keep-in-touch nudges."));
        assert!(!listed.contains("Second sentence"));
        assert!(listed.contains("testhub"));

        // filter narrows
        let filtered = list_tools_impl(&json!({"filter": "crm"})).await;
        assert!(filtered.contains("crm_get_overdue"));
        assert!(!filtered.contains("time_get_current_time"));

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

        clear_env();
        reset_state();
    }
}
