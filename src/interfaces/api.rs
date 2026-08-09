use std::collections::HashMap;
use std::convert::Infallible;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, bail};
use async_stream::stream;
use axum::extract::{Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::{Mutex, broadcast, mpsc};
use uuid::Uuid;

use crate::actor::ActorEvent;
use crate::agent::{Agent, TURN_CHECKPOINT_NOTICE, TurnRequest, TurnResult};
use crate::config::Settings;
use crate::conversation::{ConversationManager, ProcessCallback, ProcessContext};
use crate::interfaces::telegram::{
    PendingReaction, SharedTelegramTurnGuard, TelegramClient, TelegramToolContext,
    TelegramTurnGuard, TelegramTypingObserver, llm_limit_reply, split_telegram_messages,
};
use crate::llm::models::{available_providers, normalize_model_id, provider_for_model};
use crate::memory::StoredMessage;
use crate::memory::message_metadata::{
    MessageKind, MessageMetadata, MessageVisibility, RawMessageKind,
    metadata_value as message_metadata_value, raw_metadata_value as raw_message_metadata_value,
};
use crate::scheduler::brainstem::{BrainstemEmission, BrainstemHandle};
use crate::todos::{TodoFilter, TodoPriority, TodoStatus};
use crate::tools::registry::{
    BoxToolFuture, ClientToolContext, SharedTurnObserver, ToolRuntime, TurnObserver,
};

const SESSION_QUEUE_DEPTH: usize = 32;
const EVENT_QUEUE_DEPTH: usize = 64;
const WAKE_TURN_TIMEOUT: Duration = Duration::from_secs(240);
const WAKE_DELIVERY_TIMEOUT: Duration = Duration::from_secs(30);
const WAKE_TURN_FAILURE_MESSAGE: &str =
    "The scheduled task could not be completed. Please try again.";
const WAKE_TURN_TIMEOUT_MESSAGE: &str =
    "The scheduled task timed out before completion. Please try again.";

#[derive(Clone)]
pub struct ApiState {
    settings: Settings,
    agent: Arc<Agent>,
    conversations: ConversationManager,
    sessions: Arc<Mutex<ApiSessions>>,
    /// Auxiliary application-originated proactive events. Brainstem emissions
    /// subscribe directly per `/events` connection below.
    proactive_tx: broadcast::Sender<ApiEvent>,
    /// Shared Brainstem handle. Each live `/events` request subscribes
    /// directly so an API server with no connected SSE client does not count
    /// as a deliverable proactive-message transport.
    brainstem: Option<BrainstemHandle>,
    /// Server-wide stream that fans out actor.* lifecycle events to any
    /// /events subscriber (TUI clients). Populated when an actor runtime
    /// is installed (see `install_actor_broadcaster`).
    stream_tx: broadcast::Sender<ApiEvent>,
    /// Hosted secure-input channel (agent-id credential prompts). Present only
    /// when `LETHE_SECURE_PROMPT=hosted`; drives the `/secure-input*` routes and
    /// is handed to the agent-id tools per turn.
    secure_prompt: Option<crate::agent_id::secure_prompt::SecurePromptHub>,
}

#[derive(Debug, Default)]
struct ApiSessions {
    by_id: HashMap<String, ApiSession>,
    by_chat: HashMap<i64, String>,
}

#[derive(Debug)]
struct ApiSession {
    chat_id: i64,
    sender: mpsc::Sender<ApiEvent>,
}

struct ApiStreamGuard {
    state: ApiState,
    chat_id: i64,
    session_id: String,
    finished: bool,
}

impl ApiStreamGuard {
    fn new(state: ApiState, chat_id: i64, session_id: String) -> Self {
        Self {
            state,
            chat_id,
            session_id,
            finished: false,
        }
    }

    async fn finish(&mut self) {
        self.finished = true;
        self.state.unregister_session(&self.session_id).await;
    }
}

impl Drop for ApiStreamGuard {
    fn drop(&mut self) {
        if self.finished {
            return;
        }

        let state = self.state.clone();
        let chat_id = self.chat_id;
        let session_id = self.session_id.clone();
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                if state.session_matches_chat(chat_id, &session_id).await {
                    state.conversations.cancel(chat_id).await;
                }
                state.unregister_session(&session_id).await;
            });
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ApiEvent {
    pub event: String,
    pub data: Value,
}

impl ApiEvent {
    pub fn new(event: impl Into<String>, data: Value) -> Self {
        Self {
            event: event.into(),
            data,
        }
    }

    fn into_sse(self) -> Event {
        Event::default()
            .event(self.event)
            .data(self.data.to_string())
    }
}

impl ApiState {
    pub fn new(settings: Settings, agent: Agent) -> Self {
        Self::with_shared_agent(settings, Arc::new(agent))
    }

    /// Construct around a pre-built `Arc<Agent>` so multiple transports
    /// (HTTP API + Telegram poller) can share one agent, one memory
    /// store, and one actor registry in the same process.
    pub fn with_shared_agent(settings: Settings, agent: Arc<Agent>) -> Self {
        let (proactive_tx, _) = broadcast::channel(EVENT_QUEUE_DEPTH);
        let (stream_tx, _) = broadcast::channel(EVENT_QUEUE_DEPTH);
        // Hosted secure-input: the hub emits `secure_input.*` onto the same
        // `/events` broadcast the frontend already consumes.
        let secure_prompt = if crate::agent_id::is_enabled()
            && crate::agent_id::secure_prompt_hosted()
        {
            let socket_path = crate::agent_id::secure_prompt_socket_path(&settings);
            let tx = stream_tx.clone();
            let emit: crate::agent_id::secure_prompt::Emit = Arc::new(move |event: &str, data| {
                let _ = tx.send(ApiEvent::new(event.to_string(), data));
            });
            Some(crate::agent_id::secure_prompt::SecurePromptHub::new(
                socket_path,
                emit,
            ))
        } else {
            None
        };
        Self {
            conversations: ConversationManager::new(Duration::from_secs_f64(
                settings.background.debounce_seconds,
            )),
            settings,
            agent,
            sessions: Arc::new(Mutex::new(ApiSessions::default())),
            proactive_tx,
            brainstem: None,
            stream_tx,
            secure_prompt,
        }
    }

    fn with_brainstem(mut self, brainstem: BrainstemHandle) -> Self {
        self.brainstem = Some(brainstem);
        self
    }

    /// Subscribe the API to the agent's actor event bus and translate each
    /// internal `ActorEvent` into a public `actor.*` SSE event on the
    /// stream broadcast. Called once at server start.
    pub async fn install_actor_broadcaster(&self) -> Result<()> {
        let Some(runtime) = self.agent.actor_registry() else {
            return Ok(());
        };
        let mut rx = runtime
            .install_event_broadcaster(256)
            .await
            .map_err(|error| anyhow::anyhow!("install actor broadcaster: {error}"))?;
        let stream_tx = self.stream_tx.clone();
        tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(event) => {
                        if let Some(api_event) = actor_event_to_api(&event) {
                            let _ = stream_tx.send(api_event);
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        });
        Ok(())
    }

    pub fn from_settings(settings: Settings) -> Result<Self> {
        let agent = Agent::from_settings(settings.clone())?;
        Ok(Self::new(settings, agent))
    }

    pub async fn send_proactive(&self, content: &str) -> bool {
        let content = content.trim();
        if content.is_empty() {
            return false;
        }
        self.proactive_tx
            .send(ApiEvent::new(
                "text",
                json!({
                    "content": content,
                    "parse_mode": "Markdown",
                    "message_id": 0,
                    "proactive": true,
                }),
            ))
            .is_ok()
    }

    async fn register_session(&self, chat_id: i64, sender: mpsc::Sender<ApiEvent>) -> String {
        let session_id = Uuid::new_v4().simple().to_string();
        let previous = {
            let mut sessions = self.sessions.lock().await;
            let previous_id = sessions.by_chat.insert(chat_id, session_id.clone());
            let previous = previous_id.and_then(|id| sessions.by_id.remove(&id));
            sessions
                .by_id
                .insert(session_id.clone(), ApiSession { chat_id, sender });
            previous
        };

        if let Some(previous) = previous {
            close_sender(previous.sender).await;
        }
        session_id
    }

    async fn unregister_session(&self, session_id: &str) {
        let mut sessions = self.sessions.lock().await;
        if let Some(session) = sessions.by_id.remove(session_id)
            && sessions.by_chat.get(&session.chat_id) == Some(&session_id.to_string())
        {
            sessions.by_chat.remove(&session.chat_id);
        }
    }

    async fn close_chat_session(&self, chat_id: i64) -> bool {
        let session = {
            let mut sessions = self.sessions.lock().await;
            let Some(session_id) = sessions.by_chat.remove(&chat_id) else {
                return false;
            };
            sessions.by_id.remove(&session_id)
        };
        if let Some(session) = session {
            close_sender(session.sender).await;
            true
        } else {
            false
        }
    }

    async fn session_matches_chat(&self, chat_id: i64, session_id: &str) -> bool {
        let sessions = self.sessions.lock().await;
        sessions
            .by_chat
            .get(&chat_id)
            .is_some_and(|id| id == session_id)
            && sessions.by_id.contains_key(session_id)
    }

    async fn send_to_session(&self, session_id: &str, event: &str, data: Value) -> bool {
        let sender = {
            let sessions = self.sessions.lock().await;
            sessions
                .by_id
                .get(session_id)
                .map(|session| session.sender.clone())
        };
        let Some(sender) = sender else {
            return false;
        };
        sender.send(ApiEvent::new(event, data)).await.is_ok()
    }

    /// Mirror an event onto the durable `/events` broadcast (per-user). Turn
    /// events go to the per-request `/chat` SSE, which dies on reload; the
    /// broadcast survives reloads, so a reloaded/second tab can re-attach to a
    /// running turn (thinking indicator, tool pills, final reply). The owning
    /// tab ignores these while its own `/chat` stream is live (see the UI's
    /// `streamingRef` gate) so nothing double-renders. Sync + best-effort.
    fn broadcast_events(&self, event: &str, data: Value) {
        let _ = self.stream_tx.send(ApiEvent::new(event, data));
    }

    /// Deliver to the chat's CURRENT session, whichever request that is by
    /// now. Turn output must use this, not the session that originated the
    /// turn: a message sent mid-turn replaces the SSE, and events pinned to
    /// the originating session vanish into the closed stream.
    async fn send_to_chat(&self, chat_id: i64, event: &str, data: Value) -> bool {
        let sender = {
            let sessions = self.sessions.lock().await;
            sessions
                .by_chat
                .get(&chat_id)
                .and_then(|id| sessions.by_id.get(id))
                .map(|session| session.sender.clone())
        };
        let Some(sender) = sender else {
            return false;
        };
        sender.send(ApiEvent::new(event, data)).await.is_ok()
    }

    async fn client_tool_context(
        &self,
        session_id: &str,
        chat_id: i64,
        last_message_id: Option<i64>,
    ) -> Option<ClientToolContext> {
        let sender = {
            let sessions = self.sessions.lock().await;
            sessions
                .by_id
                .get(session_id)
                .map(|session| session.sender.clone())
        }?;
        Some(ClientToolContext::new(
            chat_id,
            last_message_id,
            move |event| {
                sender
                    .try_send(ApiEvent::new(event.event, event.data))
                    .is_ok()
            },
        ))
    }
}

async fn close_sender(sender: mpsc::Sender<ApiEvent>) {
    let _ = sender.send(ApiEvent::new("typing_stop", json!({}))).await;
    let _ = sender.send(ApiEvent::new("done", json!({}))).await;
}

/// Bridges the agent's tool-loop hooks into per-session SSE events. The
/// session sender is the same `mpsc::Sender` used for `text`/`typing_*`,
/// so tool cards appear inline with the assistant transcript on the TUI.
struct ApiTurnObserver {
    /// Live session registry, NOT a frozen sender: the user can replace their
    /// SSE mid-turn (every new /chat message does), and a captured sender
    /// would keep streaming the rest of the turn into the dead old stream —
    /// the UI then looks frozen while the agent works (observed live: every
    /// message sent during a long turn rendered nothing).
    sessions: Arc<Mutex<ApiSessions>>,
    chat_id: i64,
    broadcast: broadcast::Sender<ApiEvent>,
}

impl ApiTurnObserver {
    fn new(
        sessions: Arc<Mutex<ApiSessions>>,
        chat_id: i64,
        broadcast: broadcast::Sender<ApiEvent>,
    ) -> Self {
        Self {
            sessions,
            chat_id,
            broadcast,
        }
    }

    /// Deliver to the chat's CURRENT session. Observer methods are sync, so
    /// use try_lock + try_send; on contention or a full queue the event is
    /// dropped, same as the previous try_send semantics.
    fn deliver(&self, event: ApiEvent) {
        let Ok(sessions) = self.sessions.try_lock() else {
            return;
        };
        let Some(sender) = sessions
            .by_chat
            .get(&self.chat_id)
            .and_then(|id| sessions.by_id.get(id))
            .map(|session| session.sender.clone())
        else {
            return;
        };
        drop(sessions);
        let _ = sender.try_send(event);
    }

    /// Also mirror tool lifecycle onto the durable `/events` broadcast so a
    /// reloaded/second tab shows tool activity for a turn it doesn't own.
    /// Assistant deltas stay session-local, while provider reasoning is dropped
    /// entirely; the final `text` mirror carries the full reply.
    fn mirror(&self, event: ApiEvent) {
        let _ = self.broadcast.send(event);
    }
}

impl TurnObserver for ApiTurnObserver {
    fn wrap_tool_call<'a>(&'a self, _name: &'a str, inner: BoxToolFuture<'a>) -> BoxToolFuture<'a> {
        inner
    }

    fn on_tool_start(&self, name: &str, call_id: &str, args_preview: &str) {
        let ev = ApiEvent::new(
            "tool.start",
            json!({
                "name": name,
                "call_id": call_id,
                "args_preview": args_preview,
            }),
        );
        self.deliver(ev.clone());
        self.mirror(ev);
    }

    fn on_tool_end(
        &self,
        name: &str,
        call_id: &str,
        success: bool,
        output_preview: &str,
        duration_ms: u128,
    ) {
        let ev = ApiEvent::new(
            "tool.end",
            json!({
                "name": name,
                "call_id": call_id,
                "success": success,
                "output_preview": output_preview,
                "duration_ms": duration_ms as u64,
            }),
        );
        self.deliver(ev.clone());
        self.mirror(ev);
    }

    fn on_assistant_delta(&self, content: &str) {
        if content.is_empty() {
            return;
        }
        self.deliver(ApiEvent::new(
            "assistant.delta",
            json!({"content": content}),
        ));
    }
}

/// Translate an internal `ActorEvent` into the TUI-facing actor.* surface.
/// Returns `None` for events that the TUI doesn't render so the SSE stream
/// stays low-traffic.
fn actor_event_to_api(event: &ActorEvent) -> Option<ApiEvent> {
    let mut public_payload = event.payload.clone();
    if event.event_type == "task_state_changed" {
        // `update_task_state.note` is actor-private checkpoint/blocker state.
        // Keep state transitions observable without turning the model-controlled
        // note into public SSE content.
        public_payload.remove("note");
        public_payload.remove("task_state_note");
    }
    let payload = serde_json::Value::Object(public_payload);
    match event.event_type.as_str() {
        "actor_spawned" => Some(ApiEvent::new(
            "actor.spawned",
            json!({
                "actor_id": event.actor_id,
                "group": event.group,
                "payload": payload,
            }),
        )),
        "actor_terminated" | "actor_cycle_finished" => Some(ApiEvent::new(
            "actor.state",
            json!({
                "actor_id": event.actor_id,
                "group": event.group,
                "kind": event.event_type,
                "payload": payload,
            }),
        )),
        "task_state_changed" => Some(ApiEvent::new(
            "actor.task",
            json!({
                "actor_id": event.actor_id,
                "group": event.group,
                "payload": payload,
            }),
        )),
        "actor_message" => Some(ApiEvent::new(
            "actor.message",
            json!({
                "actor_id": event.actor_id,
                "group": event.group,
                "payload": payload,
            }),
        )),
        // A worker (or the DMN) addressed the user directly. The payload
        // carries the full message (not just a preview) plus the message
        // intent under `kind`, so clients can render it as a user-facing
        // question/notice from that subagent.
        "user_notify" => Some(ApiEvent::new(
            "actor.user_notify",
            json!({
                "actor_id": event.actor_id,
                "group": event.group,
                "payload": payload,
            }),
        )),
        _ => None,
    }
}

pub fn router(state: ApiState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/chat", post(chat))
        .route("/wake", post(wake))
        .route("/cancel", post(cancel))
        .route("/configure", post(configure))
        .route("/model", get(model_get).post(model_post))
        .route("/events", get(events))
        .route("/browser/stream", get(browser_stream))
        .route("/file", get(serve_file))
        .route("/actors", get(list_actors))
        .route("/todos", get(list_todos))
        .route("/session/history", get(session_history))
        .route("/secure-input", post(secure_input_submit))
        .route("/secure-input/cancel", post(secure_input_cancel))
        .route("/secure-input/pending", get(secure_input_pending))
        .with_state(state)
}

pub async fn serve(settings: Settings, port: u16) -> Result<()> {
    // Standalone API mode: spin up our own Brainstem since there's no
    // shared one. The combined api+telegram path in `cli::handlers`
    // passes its own handle so both transports share one Brainstem.
    let brainstem = BrainstemHandle::new();
    serve_with_agent(settings, port, None, brainstem).await
}

/// Run the API server with optional shared agent + shared Brainstem.
/// When `agent` is `None`, builds one from settings. Each connected
/// `/events` client subscribes to the Brainstem for its own SSE lifetime.
pub async fn serve_with_agent(
    settings: Settings,
    port: u16,
    agent: Option<Arc<Agent>>,
    brainstem: BrainstemHandle,
) -> Result<()> {
    if settings.api.token.trim().is_empty() {
        bail!("LETHE_API_TOKEN must be set in API mode");
    }

    let state = match agent {
        Some(agent) => ApiState::with_shared_agent(settings.clone(), agent),
        None => ApiState::from_settings(settings.clone())?,
    }
    .with_brainstem(brainstem);
    if let Err(error) = state.install_actor_broadcaster().await {
        tracing::warn!(error = %error, "actor broadcaster not installed");
    }

    // Provision the agent's Alien identity + vault (idempotent, degrades to a
    // warning if the CLIs are absent). Cache the state dir synchronously (cheap)
    // so tools resolve it immediately, but run the provisioning itself — which
    // shells out to the agent-id CLIs, each with a 60s budget — off the hot path,
    // so a slow or hung shim can't delay the TCP listener (and thus /health) past
    // the container's health-check grace.
    crate::agent_id::set_state_dir(&settings);
    {
        let settings = settings.clone();
        tokio::spawn(async move {
            crate::agent_id::ensure_provisioned(&settings).await;
        });
    }

    // Hosted secure-input: bind the unix socket and start its accept loop so
    // agent-id CLI children can raise end-to-end-sealed credential cards.
    if let Some(hub) = state.secure_prompt.clone() {
        match hub.bind() {
            Ok(listener) => {
                tokio::spawn(crate::agent_id::secure_prompt::serve(hub, listener));
                tracing::info!("agent-id secure-prompt socket listening");
            }
            Err(error) => {
                tracing::warn!(error = %error, "agent-id secure-prompt socket bind failed");
            }
        }
    }
    let app = router(state.clone());
    let bind = format!("{}:{port}", settings.api.host);
    let listener = tokio::net::TcpListener::bind(&bind).await?;
    println!("Lethe Rust API listening on http://{bind}");

    let result = axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await;
    Ok(result?)
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

#[derive(Debug, Deserialize)]
struct SecureInputBody {
    #[serde(default)]
    request_id: String,
    #[serde(default)]
    client_pub: String,
    #[serde(default)]
    salt: String,
    #[serde(default)]
    iv: String,
    #[serde(default)]
    ciphertext: String,
}

/// Deliver a browser-sealed credential envelope to the pending request. The
/// control plane relays this ciphertext-only; we unseal in-process and hand the
/// values to the waiting CLI child. 2xx = delivered, 404 = gone, 400 = bad
/// ciphertext (frontend retries).
async fn secure_input_submit(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(body): Json<SecureInputBody>,
) -> Response {
    if let Some(response) = require_auth(&state, &headers) {
        return response;
    }
    let Some(hub) = state.secure_prompt.as_ref() else {
        return json_error(StatusCode::SERVICE_UNAVAILABLE, "secure-input not enabled");
    };
    let sealed = crate::agent_id::crypto::SealedInput {
        client_pub: body.client_pub,
        salt: body.salt,
        iv: body.iv,
        ciphertext: body.ciphertext,
    };
    match hub.submit(&body.request_id, &sealed) {
        crate::agent_id::secure_prompt::SubmitOutcome::Accepted => {
            Json(json!({ "ok": true })).into_response()
        }
        crate::agent_id::secure_prompt::SubmitOutcome::NotFound => {
            json_error(StatusCode::NOT_FOUND, "no such pending request")
        }
        crate::agent_id::secure_prompt::SubmitOutcome::BadCiphertext(err) => {
            json_error(StatusCode::BAD_REQUEST, &format!("could not unseal: {err}"))
        }
    }
}

#[derive(Debug, Deserialize)]
struct SecureInputCancelBody {
    #[serde(default)]
    request_id: String,
}

async fn secure_input_cancel(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(body): Json<SecureInputCancelBody>,
) -> Response {
    if let Some(response) = require_auth(&state, &headers) {
        return response;
    }
    let Some(hub) = state.secure_prompt.as_ref() else {
        return json_error(StatusCode::SERVICE_UNAVAILABLE, "secure-input not enabled");
    };
    let cancelled = hub.cancel(&body.request_id);
    Json(json!({ "ok": true, "cancelled": cancelled })).into_response()
}

async fn secure_input_pending(State(state): State<ApiState>, headers: HeaderMap) -> Response {
    if let Some(response) = require_auth(&state, &headers) {
        return response;
    }
    let Some(hub) = state.secure_prompt.as_ref() else {
        return Json(Value::Array(Vec::new())).into_response();
    };
    Json(Value::Array(hub.list_pending())).into_response()
}

async fn health() -> Json<Value> {
    Json(json!({"status": "ready"}))
}

#[derive(Debug, Deserialize)]
struct ChatRequest {
    message: String,
    #[serde(default)]
    user_id: i64,
    #[serde(default)]
    chat_id: Option<i64>,
    #[serde(default)]
    metadata: serde_json::Map<String, Value>,
}

async fn chat(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(mut body): Json<ChatRequest>,
) -> Response {
    if let Some(response) = require_auth(&state, &headers) {
        return response;
    }
    if body.message.trim().is_empty() {
        return json_error(StatusCode::BAD_REQUEST, "message is required");
    }

    let chat_id = body.chat_id.unwrap_or(body.user_id);
    let (sender, mut receiver) = mpsc::channel::<ApiEvent>(SESSION_QUEUE_DEPTH);
    let mid_turn = state.conversations.is_processing(chat_id).await;
    let session_id = state.register_session(chat_id, sender).await;
    body.metadata
        .insert("_api_session_id".to_string(), json!(session_id.clone()));

    let callback = process_chat_callback(state.clone());
    state
        .conversations
        .add_message(
            chat_id,
            body.user_id,
            body.message,
            Some(body.metadata),
            Some(callback),
        )
        .await;
    if mid_turn {
        // The message interrupts a running turn: it was queued, and the
        // in-flight turn's remaining output now lands on THIS session (see
        // send_to_chat). Ack immediately so the new stream isn't byte-less
        // until the next observer event — a silent stream reads as "no
        // response" (its only traffic would be the 15s SSE keepalive).
        let _ = state
            .send_to_session(&session_id, "typing_start", json!({}))
            .await;
    }

    let stream_state = state.clone();
    let stream_session_id = session_id.clone();
    let stream_chat_id = chat_id;
    let event_stream = stream! {
        let mut guard = ApiStreamGuard::new(stream_state, stream_chat_id, stream_session_id);
        while let Some(event) = receiver.recv().await {
            let done = event.event == "done";
            yield Ok::<Event, Infallible>(event.into_sse());
            if done {
                break;
            }
        }
        guard.finish().await;
    };

    Sse::new(event_stream)
        .keep_alive(KeepAlive::default())
        .into_response()
}

fn process_chat_callback(state: ApiState) -> ProcessCallback {
    Arc::new(move |context: ProcessContext| {
        let state = state.clone();
        Box::pin(async move {
            process_chat_context(state, context).await;
            Ok(())
        })
    })
}

async fn process_chat_context(state: ApiState, context: ProcessContext) {
    let session_id = context
        .metadata
        .get("_api_session_id")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    if session_id.is_empty() {
        return;
    }

    let _ = state
        .send_to_chat(context.chat_id, "typing_start", json!({}))
        .await;
    let _ = state
        .send_to_chat(
            context.chat_id,
            "turn.start",
            json!({"chat_id": context.chat_id}),
        )
        .await;
    // Durable mirror: tell reloaded/second tabs a turn is running so they can
    // show the thinking indicator (the /chat SSE that owns the live stream
    // dies on reload; /events survives).
    state.broadcast_events("turn.active", json!({"active": true}));
    let observer: Option<SharedTurnObserver> = Some(Arc::new(ApiTurnObserver::new(
        state.sessions.clone(),
        context.chat_id,
        state.stream_tx.clone(),
    )) as SharedTurnObserver);
    let tool_runtime = ToolRuntime {
        client: state
            .client_tool_context(
                &session_id,
                context.chat_id,
                metadata_i64(&context.metadata, "message_id"),
            )
            .await,
        observer,
        secure_prompt: state.secure_prompt.clone(),
        ..ToolRuntime::default()
    };
    let request_metadata = MessageMetadata::from_map(&context.metadata);
    let mut req = TurnRequest::new(&context.message).with_runtime(tool_runtime);
    if !context.metadata.is_empty() {
        req = req.with_metadata(Value::Object(context.metadata.clone()));
    }
    let response = state.agent.chat_once_result(req).await;

    match response {
        // Deliver the reply even when the user typed mid-turn (interrupt token
        // set): the work is done and already persisted to history — dropping it
        // here just desyncs the live view from reality (observed: a 7-minute
        // research turn whose answer only ever existed after a page reload).
        Ok(TurnResult::Complete(message))
            if request_metadata.kind != Some(MessageKind::TelegramReaction)
                && !message.trim().is_empty() =>
        {
            let _ = state
                .send_to_chat(
                    context.chat_id,
                    "text",
                    json!({
                        "content": &message,
                        "parse_mode": "Markdown",
                        "message_id": 0,
                    }),
                )
                .await;
            // Durable mirror of the final reply so a reloaded/second tab
            // renders it live (reuses the UI's existing `message` handler).
            state.broadcast_events("message", json!({"role": "assistant", "content": message}));
        }
        Ok(TurnResult::Checkpointed)
            if !request_metadata.is_internal()
                && request_metadata.kind != Some(MessageKind::TelegramReaction) =>
        {
            let message = TURN_CHECKPOINT_NOTICE.to_string();
            let _ = state
                .send_to_chat(
                    context.chat_id,
                    "text",
                    json!({
                        "content": &message,
                        "parse_mode": "Markdown",
                        "message_id": 0,
                    }),
                )
                .await;
            state.broadcast_events("message", json!({"role": "assistant", "content": message}));
        }
        Ok(TurnResult::Checkpointed) => {}
        Ok(TurnResult::Complete(_)) => {}
        Err(error) if !context.interrupt.is_interrupted() => {
            let _ = state
                .send_to_chat(
                    context.chat_id,
                    "text",
                    json!({
                        "content": format!("Error: {error}"),
                        "parse_mode": null,
                        "message_id": 0,
                    }),
                )
                .await;
        }
        Err(_) => {}
    }

    if let Some(tokens) = state.agent.last_prompt_tokens() {
        let _ = state
            .send_to_chat(context.chat_id, "usage", json!({"prompt_tokens": tokens}))
            .await;
    }
    let _ = state
        .send_to_chat(context.chat_id, "typing_stop", json!({}))
        .await;
    // `done` ends the client stream. When messages are already queued behind
    // this turn (typed mid-turn), the SAME stream must survive to carry the
    // next turn's output — closing it here would orphan that turn exactly the
    // way the stale-session bug did. The final turn of the run sends the done.
    if state.conversations.pending_count(context.chat_id).await == 0 {
        let _ = state.send_to_chat(context.chat_id, "done", json!({})).await;
        // Clear the reloaded-tab thinking indicator only when the whole run is
        // done (queued follow-up turns keep it active).
        state.broadcast_events("turn.active", json!({"active": false}));
    }
    state.unregister_session(&session_id).await;
}

#[derive(Debug, Deserialize)]
struct ChatIdRequest {
    #[serde(default)]
    chat_id: i64,
}

async fn cancel(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(body): Json<ChatIdRequest>,
) -> Response {
    if let Some(response) = require_auth(&state, &headers) {
        return response;
    }
    let cancelled = if body.chat_id == 0 {
        false
    } else {
        let conversation = state.conversations.cancel(body.chat_id).await;
        let session = state.close_chat_session(body.chat_id).await;
        conversation || session
    };
    Json(json!({"status": "cancelled", "cancelled": cancelled})).into_response()
}

#[derive(Debug, Deserialize)]
struct ConfigureRequest {
    #[serde(default)]
    user_id: i64,
    #[serde(default)]
    username: String,
    #[serde(default)]
    first_name: String,
}

async fn configure(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(body): Json<ConfigureRequest>,
) -> Response {
    if let Some(response) = require_auth(&state, &headers) {
        return response;
    }

    let mut human = format!("Name: {}\n", body.first_name.trim());
    if !body.username.trim().is_empty() {
        human.push_str(&format!("Telegram: @{}\n", body.username.trim()));
    }
    human.push_str(&format!("User ID: {}\n", body.user_id));

    match state
        .agent
        .memory()
        .blocks
        .update("human", Some(&human), None)
    {
        Ok(true) => Json(json!({"status": "configured"})).into_response(),
        Ok(false) => match state.agent.memory().blocks.create(
            "human",
            &human,
            "Information about the human user.",
            crate::memory::DEFAULT_BLOCK_LIMIT,
            false,
            false,
        ) {
            Ok(_) => Json(json!({"status": "configured"})).into_response(),
            Err(error) => json_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string()),
        },
        Err(error) => json_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string()),
    }
}

async fn model_get(State(state): State<ApiState>, headers: HeaderMap) -> Response {
    if let Some(response) = require_auth(&state, &headers) {
        return response;
    }
    let config = match state.agent.router_config() {
        Ok(config) => config,
        Err(error) => return json_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string()),
    };
    Json(json!({
        "model": config.model,
        "model_aux": config.aux_model,
        "model_deep": config.deep_model,
        "provider": model_provider(&config.model, &state.settings.llm.llm_provider),
        "current_auth": "API",
        "available_providers": available_provider_ids(),
        "provider_info": available_providers(),
    }))
    .into_response()
}

#[derive(Debug, Deserialize)]
struct ModelUpdateRequest {
    model: Option<String>,
    model_aux: Option<String>,
    /// Powerful deep-thinking model. An explicit empty string clears it.
    model_deep: Option<String>,
}

async fn model_post(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(body): Json<ModelUpdateRequest>,
) -> Response {
    if let Some(response) = require_auth(&state, &headers) {
        return response;
    }
    // Normalize bare OpenRouter ids (`vendor/model` -> `openrouter/vendor/model`)
    // against the configured provider, matching the persisted `lethe model` path.
    let provider = state.settings.llm.llm_provider.trim();
    let model = body
        .model
        .as_deref()
        .map(|id| normalize_model_id(provider, id));
    let model_aux = body
        .model_aux
        .as_deref()
        .map(|id| normalize_model_id(provider, id));
    // Preserve an explicit empty string (clear the deep slot) — only normalize
    // a non-empty id against the configured provider.
    let model_deep = body.model_deep.as_deref().map(|id| {
        if id.trim().is_empty() {
            String::new()
        } else {
            normalize_model_id(provider, id)
        }
    });
    let changed = match state.agent.reconfigure_models(
        model.as_deref(),
        model_aux.as_deref(),
        model_deep.as_deref(),
    ) {
        Ok(changed) => changed,
        Err(error) => return json_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string()),
    };
    let config = match state.agent.router_config() {
        Ok(config) => config,
        Err(error) => return json_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string()),
    };
    Json(json!({
        "status": "updated",
        "model": config.model,
        "model_aux": config.aux_model,
        "model_deep": config.deep_model,
        "provider": model_provider(&config.model, &state.settings.llm.llm_provider),
        "changed": changed,
    }))
    .into_response()
}

async fn events(State(state): State<ApiState>, headers: HeaderMap) -> Response {
    if let Some(response) = require_auth(&state, &headers) {
        return response;
    }
    // Subscription lifetime matches the authenticated SSE connection. Merely
    // running the API server must not make the Brainstem believe a proactive
    // message has somewhere user-visible to go.
    let mut brainstem_rx = state.brainstem.as_ref().map(BrainstemHandle::subscribe);
    let mut proactive_rx = state.proactive_tx.subscribe();
    let mut stream_rx = state.stream_tx.subscribe();
    // Conversation messages from other transports (e.g. Telegram) so an open web
    // client can append them to its transcript live.
    let mut conversation_rx = state.agent.subscribe_conversation_events();
    let event_stream = stream! {
        loop {
            tokio::select! {
                emission = recv_brainstem(&mut brainstem_rx) => match emission {
                    Ok(BrainstemEmission { message, .. }) => {
                        yield Ok::<Event, Infallible>(ApiEvent::new(
                            "text",
                            json!({
                                "content": message,
                                "parse_mode": "Markdown",
                                "message_id": 0,
                                "proactive": true,
                            }),
                        ).into_sse())
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => break,
                },
                event = proactive_rx.recv() => match event {
                    Ok(event) => yield Ok::<Event, Infallible>(event.into_sse()),
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => break,
                },
                event = stream_rx.recv() => match event {
                    Ok(event) => yield Ok::<Event, Infallible>(event.into_sse()),
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => break,
                },
                event = conversation_rx.recv() => match event {
                    Ok(event) => yield Ok::<Event, Infallible>(ApiEvent::new(event.event, event.data).into_sse()),
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => break,
                },
            }
        }
    };
    Sse::new(event_stream)
        .keep_alive(KeepAlive::default())
        .into_response()
}

async fn recv_brainstem(
    receiver: &mut Option<broadcast::Receiver<BrainstemEmission>>,
) -> Result<BrainstemEmission, broadcast::error::RecvError> {
    match receiver {
        Some(receiver) => receiver.recv().await,
        None => std::future::pending().await,
    }
}

async fn list_actors(State(state): State<ApiState>, headers: HeaderMap) -> Response {
    if let Some(response) = require_auth(&state, &headers) {
        return response;
    }
    let Some(runtime) = state.agent.actor_registry() else {
        return Json(json!({"actors": []})).into_response();
    };
    match runtime.list_actors().await {
        Ok(actors) => Json(json!({"actors": actors})).into_response(),
        Err(error) => json_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string()),
    }
}

#[derive(Debug, Deserialize)]
struct TodoListQuery {
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    priority: Option<String>,
    #[serde(default)]
    include_completed: bool,
    #[serde(default)]
    limit: Option<usize>,
}

async fn list_todos(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(query): Query<TodoListQuery>,
) -> Response {
    if let Some(response) = require_auth(&state, &headers) {
        return response;
    }
    let filter = TodoFilter {
        status: query.status.as_deref().and_then(TodoStatus::parse),
        priority: query.priority.as_deref().and_then(TodoPriority::parse),
        include_completed: query.include_completed,
        limit: query.limit.unwrap_or(50),
    };
    match state.agent.memory().todos.list(filter) {
        Ok(todos) => Json(json!({"todos": todos})).into_response(),
        Err(error) => json_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string()),
    }
}

#[derive(Debug, Deserialize)]
struct HistoryQuery {
    #[serde(default)]
    limit: Option<usize>,
}

async fn session_history(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(query): Query<HistoryQuery>,
) -> Response {
    if let Some(response) = require_auth(&state, &headers) {
        return response;
    }
    let limit = query.limit.unwrap_or(50).clamp(1, 500);
    let messages = match state.agent.memory().messages.get_recent(limit) {
        Ok(messages) => messages,
        Err(error) => {
            return json_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string());
        }
    };
    let serialized = user_visible_session_messages(messages)
        .into_iter()
        .map(serialize_message)
        .collect::<Vec<_>>();
    Json(json!({"messages": serialized})).into_response()
}

fn user_visible_session_messages(messages: Vec<StoredMessage>) -> Vec<StoredMessage> {
    let mut inside_internal_turn = false;
    messages
        .into_iter()
        .filter(|message| {
            let metadata = MessageMetadata::from_value(Some(&message.metadata));
            let internal = metadata.is_internal();
            if message.role.is_user() {
                inside_internal_turn = internal;
                return !internal;
            }
            if !internal
                && message.role.is_assistant()
                && metadata.kind == Some(MessageKind::Proactive)
            {
                inside_internal_turn = false;
                return true;
            }
            !inside_internal_turn && !internal
        })
        .collect()
}

fn serialize_message(message: StoredMessage) -> Value {
    json!({
        "id": message.id,
        "role": message.role.as_str(),
        "content": message.content,
        "created_at": message.created_at,
        "metadata": message.metadata,
    })
}

#[derive(Debug, Deserialize)]
struct BrowserStreamQuery {
    source: Option<String>,
}

// Live browser viewport relay (see interfaces::browser_stream). WS because
// frames + input events flow both ways; the control plane relays it onward to
// the owner's web client.
async fn browser_stream(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(query): Query<BrowserStreamQuery>,
    ws: axum::extract::ws::WebSocketUpgrade,
) -> Response {
    if let Some(response) = require_auth(&state, &headers) {
        return response;
    }
    ws.on_upgrade(move |socket| crate::interfaces::browser_stream::relay(socket, query.source))
}

#[derive(Debug, Deserialize)]
struct FileQuery {
    path: String,
}

async fn serve_file(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(query): Query<FileQuery>,
) -> Response {
    if let Some(response) = require_auth(&state, &headers) {
        return response;
    }
    let Some(path) = resolve_workspace_path(&state.settings.paths.workspace_dir, &query.path)
    else {
        return json_error(StatusCode::FORBIDDEN, "path outside workspace");
    };
    if !path.is_file() {
        return json_error(StatusCode::NOT_FOUND, &format!("not found: {}", query.path));
    }
    match std::fs::read(path) {
        Ok(bytes) => {
            let mut response = bytes.into_response();
            response.headers_mut().insert(
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/octet-stream"),
            );
            response
        }
        Err(error) => json_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string()),
    }
}

fn require_auth(state: &ApiState, headers: &HeaderMap) -> Option<Response> {
    let expected = state.settings.api.token.trim();
    let presented = presented_api_token(headers);
    if expected.is_empty() {
        return Some(json_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "server misconfigured",
        ));
    }
    if presented != expected {
        return Some(json_error(StatusCode::UNAUTHORIZED, "unauthorized"));
    }
    None
}

fn presented_api_token(headers: &HeaderMap) -> String {
    let bearer = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .trim();
    if bearer.to_ascii_lowercase().starts_with("bearer ") {
        return bearer[7..].trim().to_string();
    }
    headers
        .get("x-lethe-token")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .trim()
        .to_string()
}

#[derive(Debug, Deserialize)]
struct WakeRequest {
    /// Prompt that drives the turn (e.g. "produce Philipp's morning brief and
    /// telegram_send_message it").
    message: String,
    /// Telegram chat to deliver to. Defaults to the single configured allowed
    /// user (in a private chat the chat id equals the user id).
    #[serde(default)]
    chat_id: Option<i64>,
}

fn resolve_wake_chat_id(settings: &Settings, requested: Option<i64>) -> Option<i64> {
    requested
        .or_else(|| settings.telegram.allowed_user_ids.first().copied())
        .filter(|chat_id| *chat_id != 0)
}

fn wake_tool_runtime(
    token: String,
    chat_id: i64,
    secure_prompt: Option<crate::agent_id::secure_prompt::SecurePromptHub>,
) -> (ToolRuntime, SharedTelegramTurnGuard) {
    let guard = Arc::new(std::sync::Mutex::new(TelegramTurnGuard::new()));
    let runtime = ToolRuntime {
        telegram: Some(TelegramToolContext {
            token: token.clone(),
            chat_id,
            user_id: Some(chat_id),
            last_message_id: None,
            guard: Some(guard.clone()),
            dry_run: false,
            sent_messages: None,
        }),
        observer: Some(Arc::new(TelegramTypingObserver::new(token, chat_id))),
        secure_prompt,
        ..ToolRuntime::default()
    };
    (runtime, guard)
}

#[derive(Debug, Default, Eq, PartialEq)]
struct WakeGuardDelivery {
    tool_messages_sent: usize,
    visible_texts: Vec<String>,
    pending_reactions: Vec<PendingReaction>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct WakeReactionDelivery {
    requested: usize,
    delivered: usize,
    errors: Vec<String>,
}

impl WakeReactionDelivery {
    fn failed(&self) -> usize {
        self.requested.saturating_sub(self.delivered)
    }

    fn has_failures(&self) -> bool {
        self.failed() > 0
    }

    fn error_summary(&self) -> String {
        if self.errors.is_empty() {
            format!("{} Telegram reaction(s) were not confirmed", self.failed())
        } else {
            self.errors.join("; ")
        }
    }
}

#[derive(Debug)]
struct WakeDeliveryProgress {
    confirmed_messages: std::sync::atomic::AtomicUsize,
    confirmed_reactions: std::sync::atomic::AtomicUsize,
    requested_reactions: usize,
}

impl WakeDeliveryProgress {
    fn new(confirmed_messages: usize, requested_reactions: usize) -> Self {
        Self {
            confirmed_messages: std::sync::atomic::AtomicUsize::new(confirmed_messages),
            confirmed_reactions: std::sync::atomic::AtomicUsize::new(0),
            requested_reactions,
        }
    }

    fn confirmed_messages(&self) -> usize {
        self.confirmed_messages
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    fn confirmed_reactions(&self) -> usize {
        self.confirmed_reactions
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    fn record_message(&self) {
        self.confirmed_messages
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }

    fn record_reaction(&self) {
        self.confirmed_reactions
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }

    fn timeout_reactions(&self, timeout_after: Duration) -> WakeReactionDelivery {
        let delivered = self.confirmed_reactions();
        let failed = self.requested_reactions.saturating_sub(delivered);
        WakeReactionDelivery {
            requested: self.requested_reactions,
            delivered,
            errors: (failed > 0)
                .then(|| {
                    format!(
                        "{failed} Telegram reaction(s) were not confirmed before the {}s delivery timeout",
                        timeout_after.as_secs()
                    )
                })
                .into_iter()
                .collect(),
        }
    }
}

fn take_wake_guard_delivery(
    delivery_guard: &SharedTelegramTurnGuard,
) -> std::result::Result<WakeGuardDelivery, String> {
    delivery_guard
        .lock()
        .map(|mut guard| WakeGuardDelivery {
            tool_messages_sent: guard.visible_messages_sent(),
            visible_texts: guard.drain_visible_texts(),
            pending_reactions: guard.drain_pending_reactions(),
        })
        .map_err(|error| error.to_string())
}

async fn deliver_wake_reactions<F, Fut>(
    pending_reactions: Vec<PendingReaction>,
    mut send_reaction: F,
) -> WakeReactionDelivery
where
    F: FnMut(PendingReaction) -> Fut,
    Fut: Future<Output = std::result::Result<(), String>>,
{
    let mut delivery = WakeReactionDelivery {
        requested: pending_reactions.len(),
        ..WakeReactionDelivery::default()
    };
    for pending in pending_reactions {
        match send_reaction(pending).await {
            Ok(()) => delivery.delivered += 1,
            Err(error) => delivery.errors.push(error),
        }
    }
    delivery
}

fn add_wake_reaction_fields(body: &mut Value, reactions: &WakeReactionDelivery) {
    body["requested_reactions"] = json!(reactions.requested);
    body["delivered_reactions"] = json!(reactions.delivered);
    body["failed_reactions"] = json!(reactions.failed());
    if !reactions.errors.is_empty() {
        body["reaction_errors"] = json!(reactions.errors);
    }
}

fn wake_delivery_response(
    chat_id: i64,
    reply_chars: usize,
    delivery_status: &str,
    delivered_messages: usize,
    message_id: Option<i64>,
    reactions: &WakeReactionDelivery,
) -> Response {
    let mut body = json!({
        "success": true,
        "turn_completed": true,
        "chat_id": chat_id,
        "reply_chars": reply_chars,
        "delivered": true,
        "delivery_status": delivery_status,
        "delivered_messages": delivered_messages,
    });
    add_wake_reaction_fields(&mut body, reactions);
    if let Some(message_id) = message_id {
        body["message_id"] = json!(message_id);
    }
    Json(body).into_response()
}

fn wake_delivery_error(
    status: StatusCode,
    chat_id: i64,
    reply_chars: usize,
    delivery_status: &str,
    turn_completed: bool,
    delivered: bool,
    delivered_messages: usize,
    message_id: Option<i64>,
    message: &str,
    reactions: &WakeReactionDelivery,
) -> Response {
    let mut body = json!({
        "success": false,
        "turn_completed": turn_completed,
        "chat_id": chat_id,
        "reply_chars": reply_chars,
        "delivered": delivered,
        "delivery_status": delivery_status,
        "delivered_messages": delivered_messages,
        "error": message,
    });
    add_wake_reaction_fields(&mut body, reactions);
    if let Some(message_id) = message_id {
        body["message_id"] = json!(message_id);
    }
    (status, Json(body)).into_response()
}

fn reaction_failure_status(base: &str, reactions: &WakeReactionDelivery) -> String {
    if reactions.delivered == 0 {
        format!("{base}_reactions_failed")
    } else {
        format!("{base}_reactions_partially_delivered")
    }
}

async fn finish_wake_delivery<RF, RFut, FF, FFut>(
    chat_id: i64,
    result: TurnResult,
    tool_messages_sent: usize,
    pending_reactions: Vec<PendingReaction>,
    send_reaction: RF,
    mut send_fallback: FF,
) -> Response
where
    RF: FnMut(PendingReaction) -> RFut,
    RFut: Future<Output = std::result::Result<(), String>>,
    FF: FnMut(String) -> FFut,
    FFut: Future<Output = std::result::Result<i64, String>>,
{
    let reply_chars = result
        .complete_text()
        .map(|reply| reply.chars().count())
        .unwrap_or(0);
    let reactions = deliver_wake_reactions(pending_reactions, send_reaction).await;
    let reply = match result {
        TurnResult::Complete(reply) => reply,
        TurnResult::Checkpointed => {
            let delivered = tool_messages_sent > 0 || reactions.delivered > 0;
            let delivery_status = if reactions.has_failures() {
                reaction_failure_status("checkpoint_suppressed", &reactions)
            } else {
                "checkpoint_suppressed".to_string()
            };
            return wake_delivery_error(
                StatusCode::OK,
                chat_id,
                0,
                &delivery_status,
                false,
                delivered,
                tool_messages_sent,
                None,
                "turn ended with an internal checkpoint; fallback delivery was suppressed",
                &reactions,
            );
        }
    };
    if tool_messages_sent > 0 {
        if reactions.has_failures() {
            return wake_delivery_error(
                StatusCode::OK,
                chat_id,
                reply_chars,
                &reaction_failure_status("tool_delivered", &reactions),
                true,
                true,
                tool_messages_sent,
                None,
                &reactions.error_summary(),
                &reactions,
            );
        }
        return wake_delivery_response(
            chat_id,
            reply_chars,
            "tool_delivered",
            tool_messages_sent,
            None,
            &reactions,
        );
    }

    let chunks = split_telegram_messages(reply.trim());
    if chunks.is_empty() {
        if reactions.requested > 0 {
            if !reactions.has_failures() && reactions.delivered > 0 {
                return wake_delivery_response(
                    chat_id,
                    reply_chars,
                    "reaction_delivered",
                    0,
                    None,
                    &reactions,
                );
            }
            return wake_delivery_error(
                if reactions.delivered > 0 {
                    StatusCode::OK
                } else {
                    StatusCode::BAD_GATEWAY
                },
                chat_id,
                reply_chars,
                &reaction_failure_status("reaction_only", &reactions),
                true,
                reactions.delivered > 0,
                0,
                None,
                &reactions.error_summary(),
                &reactions,
            );
        }
        return wake_delivery_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            chat_id,
            reply_chars,
            "empty_reply",
            true,
            false,
            0,
            None,
            "turn completed without a Telegram tool delivery or a fallback reply",
            &reactions,
        );
    }

    let mut delivered_messages = 0;
    let mut last_message_id = None;
    for chunk in chunks {
        match send_fallback(chunk).await {
            Ok(message_id) => {
                delivered_messages += 1;
                last_message_id = Some(message_id);
            }
            Err(error) => {
                let delivery_status = if delivered_messages == 0 {
                    "fallback_failed"
                } else {
                    "fallback_partially_delivered"
                };
                return wake_delivery_error(
                    if delivered_messages > 0 {
                        // Telegram creates a new message on retry. Once any
                        // chunk is confirmed, surface the partial failure in
                        // JSON but keep the HTTP response non-retryable.
                        StatusCode::OK
                    } else {
                        StatusCode::BAD_GATEWAY
                    },
                    chat_id,
                    reply_chars,
                    delivery_status,
                    true,
                    delivered_messages > 0 || reactions.delivered > 0,
                    delivered_messages,
                    last_message_id,
                    &format!("turn completed but fallback Telegram delivery failed: {error}"),
                    &reactions,
                );
            }
        }
    }

    if reactions.has_failures() {
        return wake_delivery_error(
            StatusCode::OK,
            chat_id,
            reply_chars,
            &reaction_failure_status("fallback_delivered", &reactions),
            true,
            true,
            delivered_messages,
            last_message_id,
            &reactions.error_summary(),
            &reactions,
        );
    }
    wake_delivery_response(
        chat_id,
        reply_chars,
        "fallback_delivered",
        delivered_messages,
        last_message_id,
        &reactions,
    )
}

async fn finish_wake_failure<RF, RFut, NF, NFut>(
    chat_id: i64,
    tool_messages_sent: usize,
    pending_reactions: Vec<PendingReaction>,
    _status: StatusCode,
    failure_kind: &str,
    user_message: &str,
    error: String,
    send_reaction: RF,
    send_notification: NF,
) -> Response
where
    RF: FnMut(PendingReaction) -> RFut,
    RFut: Future<Output = std::result::Result<(), String>>,
    NF: FnOnce(String) -> NFut,
    NFut: Future<Output = std::result::Result<i64, String>>,
{
    let reactions = deliver_wake_reactions(pending_reactions, send_reaction).await;
    if tool_messages_sent > 0 {
        let delivery_status = if reactions.has_failures() {
            reaction_failure_status(&format!("{failure_kind}_after_tool_delivery"), &reactions)
        } else {
            format!("{failure_kind}_after_tool_delivery")
        };
        return wake_delivery_error(
            // A visible tool delivery is already confirmed. Returning a 5xx
            // here would make the scheduler retry and can duplicate that
            // message even though the remainder of the turn did not finish.
            StatusCode::OK,
            chat_id,
            0,
            &delivery_status,
            false,
            true,
            tool_messages_sent,
            None,
            &error,
            &reactions,
        );
    }

    match send_notification(user_message.to_string()).await {
        Ok(message_id) => wake_delivery_error(
            // The failure/timeout/limit notice itself is now confirmed. A
            // retry would send the same notice again, so preserve the turn
            // failure in JSON while acknowledging delivery at HTTP level.
            StatusCode::OK,
            chat_id,
            0,
            &format!("{failure_kind}_notice_delivered"),
            false,
            true,
            1,
            Some(message_id),
            &error,
            &reactions,
        ),
        Err(send_error) => wake_delivery_error(
            StatusCode::BAD_GATEWAY,
            chat_id,
            0,
            &format!("{failure_kind}_notice_failed"),
            false,
            reactions.delivered > 0,
            0,
            None,
            &format!("{error}; Telegram failure notice could not be sent: {send_error}"),
            &reactions,
        ),
    }
}

async fn run_wake_turn_with_timeout<T>(
    timeout_after: Duration,
    future: impl Future<Output = T>,
) -> Result<T, tokio::time::error::Elapsed> {
    tokio::time::timeout(timeout_after, future).await
}

async fn run_wake_delivery_with_timeout(
    timeout_after: Duration,
    chat_id: i64,
    reply_chars: usize,
    turn_completed: bool,
    delivery_kind: &str,
    progress: Arc<WakeDeliveryProgress>,
    future: impl Future<Output = Response>,
) -> Response {
    match tokio::time::timeout(timeout_after, future).await {
        Ok(response) => response,
        Err(_) => {
            let delivered_messages = progress.confirmed_messages();
            let reactions = progress.timeout_reactions(timeout_after);
            let delivered = delivered_messages > 0 || reactions.delivered > 0;
            let delivery_status = if !delivered {
                format!("{delivery_kind}_delivery_timeout")
            } else {
                format!("{delivery_kind}_delivery_timeout_after_partial_delivery")
            };
            wake_delivery_error(
                if delivered_messages > 0 {
                    // A confirmed message is not safe to retry: Telegram would
                    // create another message rather than update the first one.
                    StatusCode::OK
                } else {
                    StatusCode::GATEWAY_TIMEOUT
                },
                chat_id,
                reply_chars,
                &delivery_status,
                turn_completed,
                delivered,
                delivered_messages,
                None,
                &format!(
                    "{delivery_kind} Telegram delivery timed out after {} seconds",
                    timeout_after.as_secs()
                ),
                &reactions,
            )
        }
    }
}

async fn send_wake_telegram_message(
    client: TelegramClient,
    chat_id: i64,
    text: String,
    progress: Arc<WakeDeliveryProgress>,
    visible_texts: Arc<std::sync::Mutex<Vec<String>>>,
) -> std::result::Result<i64, String> {
    match client.send_message(chat_id, &text).await {
        Ok(message_id) => {
            progress.record_message();
            if let Ok(mut visible_texts) = visible_texts.lock() {
                visible_texts.push(text);
            }
            Ok(message_id)
        }
        Err(error) => Err(error.to_string()),
    }
}

fn persist_wake_visible_history(agent: &Agent, visible_texts: Vec<String>) {
    for text in visible_texts {
        if text.trim().is_empty() {
            continue;
        }
        if let Err(error) = agent.memory().messages.add(
            crate::memory::MessageRole::Assistant,
            &text,
            Some(message_metadata_value(
                MessageVisibility::UserVisible,
                MessageKind::Proactive,
                "wake",
            )),
        ) {
            // Delivery is already confirmed. A local transcript failure must
            // never turn the webhook into a retry that duplicates Telegram.
            tracing::warn!(error = %error, "failed to persist confirmed wake delivery");
        }
    }
}

async fn send_wake_telegram_reaction(
    client: TelegramClient,
    pending: PendingReaction,
    progress: Arc<WakeDeliveryProgress>,
) -> std::result::Result<(), String> {
    match client
        .set_message_reaction(pending.chat_id, pending.message_id, &pending.emoji)
        .await
    {
        Ok(true) => {
            progress.record_reaction();
            Ok(())
        }
        Ok(false) => Err(format!(
            "Telegram did not apply reaction '{}' to message {} in chat {}",
            pending.emoji, pending.message_id, pending.chat_id
        )),
        Err(error) => Err(format!(
            "Telegram reaction '{}' for message {} in chat {} failed: {error}",
            pending.emoji, pending.message_id, pending.chat_id
        )),
    }
}

/// Proactive wake: run ONE agent turn bound to the real Telegram egress, so an
/// external scheduler (cron-mcp) can trigger a brief that actually reaches the
/// user.
///
/// `/chat` exists for an interactive API client (a web UI/TUI): its egress is a
/// `ClientToolContext` that streams tool output back over the HTTP/SSE response.
/// When the caller is a fire-and-forget webhook, that stream is discarded, so a
/// `telegram_send_message` during the turn went nowhere (it returned a stub
/// success with chat_id 0 — the silent drop). `/wake` instead installs a
/// `TelegramToolContext`, which `message_egress()` prefers over the client
/// egress, so the agent's `telegram_send_message` is delivered to Telegram. A
/// turn guard suppresses duplicate fallback delivery; when the model sends no
/// Telegram message itself, the handler delivers its final non-empty reply.
async fn wake(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(body): Json<WakeRequest>,
) -> Response {
    if let Some(response) = require_auth(&state, &headers) {
        return response;
    }
    if body.message.trim().is_empty() {
        return json_error(StatusCode::BAD_REQUEST, "message is required");
    }
    let token = state.settings.telegram.bot_token.clone();
    if token.trim().is_empty() {
        return json_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "Telegram bot token is not configured",
        );
    }
    let Some(chat_id) = resolve_wake_chat_id(&state.settings, body.chat_id) else {
        return json_error(
            StatusCode::BAD_REQUEST,
            "no chat_id given and no allowed user configured to deliver to",
        );
    };

    // Bind the turn to the real Telegram egress. message_egress() prefers
    // runtime.telegram over the SSE client egress, so telegram_send_message
    // reaches Telegram instead of a discarded response stream.
    let telegram_client = match TelegramClient::new(token.clone(), vec![chat_id]) {
        Ok(client) => client,
        Err(error) => {
            return json_error(
                StatusCode::SERVICE_UNAVAILABLE,
                &format!("Telegram client setup failed: {error}"),
            );
        }
    };
    let (runtime, delivery_guard) = wake_tool_runtime(token, chat_id, state.secure_prompt.clone());
    let req = TurnRequest::new(&body.message)
        .with_runtime(runtime)
        .with_metadata(raw_message_metadata_value(
            MessageVisibility::Internal,
            RawMessageKind::Wake,
            "wake",
        ));

    let turn_result =
        run_wake_turn_with_timeout(WAKE_TURN_TIMEOUT, state.agent.chat_once_result(req)).await;
    let WakeGuardDelivery {
        tool_messages_sent,
        visible_texts: tool_visible_texts,
        pending_reactions,
    } = match take_wake_guard_delivery(&delivery_guard) {
        Ok(delivery) => delivery,
        Err(error) => {
            return wake_delivery_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                chat_id,
                0,
                "delivery_state_failed",
                false,
                false,
                0,
                None,
                &format!("Telegram delivery state unavailable: {error}"),
                &WakeReactionDelivery::default(),
            );
        }
    };

    let fallback_visible_texts = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let response = match turn_result {
        Ok(Ok(result)) => {
            let reply_chars = result
                .complete_text()
                .map(|reply| reply.chars().count())
                .unwrap_or(0);
            let turn_completed = !result.is_checkpointed();
            let delivery_kind = if turn_completed {
                "fallback"
            } else {
                "checkpoint"
            };
            let progress = Arc::new(WakeDeliveryProgress::new(
                tool_messages_sent,
                pending_reactions.len(),
            ));
            let reaction_progress = progress.clone();
            let reaction_client = telegram_client.clone();
            let send_progress = progress.clone();
            let fallback_client = telegram_client.clone();
            let visible_texts = fallback_visible_texts.clone();
            let delivery = finish_wake_delivery(
                chat_id,
                result,
                tool_messages_sent,
                pending_reactions,
                move |pending| {
                    let client = reaction_client.clone();
                    let progress = reaction_progress.clone();
                    send_wake_telegram_reaction(client, pending, progress)
                },
                move |text| {
                    let client = fallback_client.clone();
                    let progress = send_progress.clone();
                    let visible_texts = visible_texts.clone();
                    send_wake_telegram_message(client, chat_id, text, progress, visible_texts)
                },
            );
            run_wake_delivery_with_timeout(
                WAKE_DELIVERY_TIMEOUT,
                chat_id,
                reply_chars,
                turn_completed,
                delivery_kind,
                progress,
                delivery,
            )
            .await
        }
        Ok(Err(error)) => {
            let error = anyhow::Error::new(error);
            let (status, failure_kind, user_message, error_message) = match llm_limit_reply(&error)
            {
                Some(message) => (
                    StatusCode::TOO_MANY_REQUESTS,
                    "limit",
                    message,
                    message.to_string(),
                ),
                None => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "turn_failed",
                    WAKE_TURN_FAILURE_MESSAGE,
                    format!("turn failed: {error}"),
                ),
            };
            let progress = Arc::new(WakeDeliveryProgress::new(
                tool_messages_sent,
                pending_reactions.len(),
            ));
            let reaction_progress = progress.clone();
            let reaction_client = telegram_client.clone();
            let send_progress = progress.clone();
            let notification_client = telegram_client.clone();
            let visible_texts = fallback_visible_texts.clone();
            let delivery = finish_wake_failure(
                chat_id,
                tool_messages_sent,
                pending_reactions,
                status,
                failure_kind,
                user_message,
                error_message,
                move |pending| {
                    let client = reaction_client.clone();
                    let progress = reaction_progress.clone();
                    send_wake_telegram_reaction(client, pending, progress)
                },
                move |text| {
                    send_wake_telegram_message(
                        notification_client,
                        chat_id,
                        text,
                        send_progress,
                        visible_texts,
                    )
                },
            );
            let delivery_kind = format!("{failure_kind}_notice");
            run_wake_delivery_with_timeout(
                WAKE_DELIVERY_TIMEOUT,
                chat_id,
                0,
                false,
                &delivery_kind,
                progress,
                delivery,
            )
            .await
        }
        Err(_) => {
            let progress = Arc::new(WakeDeliveryProgress::new(
                tool_messages_sent,
                pending_reactions.len(),
            ));
            let reaction_progress = progress.clone();
            let reaction_client = telegram_client.clone();
            let send_progress = progress.clone();
            let notification_client = telegram_client.clone();
            let visible_texts = fallback_visible_texts.clone();
            let delivery = finish_wake_failure(
                chat_id,
                tool_messages_sent,
                pending_reactions,
                StatusCode::GATEWAY_TIMEOUT,
                "turn_timeout",
                WAKE_TURN_TIMEOUT_MESSAGE,
                format!(
                    "turn timed out after {} seconds",
                    WAKE_TURN_TIMEOUT.as_secs()
                ),
                move |pending| {
                    let client = reaction_client.clone();
                    let progress = reaction_progress.clone();
                    send_wake_telegram_reaction(client, pending, progress)
                },
                move |text| {
                    send_wake_telegram_message(
                        notification_client,
                        chat_id,
                        text,
                        send_progress,
                        visible_texts,
                    )
                },
            );
            run_wake_delivery_with_timeout(
                WAKE_DELIVERY_TIMEOUT,
                chat_id,
                0,
                false,
                "turn_timeout_notice",
                progress,
                delivery,
            )
            .await
        }
    };

    let mut visible_texts = tool_visible_texts;
    match fallback_visible_texts.lock() {
        Ok(fallback_texts) => visible_texts.extend(fallback_texts.iter().cloned()),
        Err(error) => tracing::warn!(error = %error, "wake delivery transcript lock poisoned"),
    }
    persist_wake_visible_history(state.agent.as_ref(), visible_texts);
    response
}

fn json_error(status: StatusCode, message: &str) -> Response {
    (status, Json(json!({"error": message}))).into_response()
}

fn metadata_i64(metadata: &serde_json::Map<String, Value>, key: &str) -> Option<i64> {
    metadata.get(key).and_then(Value::as_i64)
}

fn available_provider_ids() -> Vec<String> {
    available_providers()
        .into_iter()
        .map(|provider| provider.provider)
        .collect()
}

fn model_provider<'a>(model: &'a str, configured_provider: &'a str) -> &'a str {
    provider_for_model(model)
        .or_else(|| (!configured_provider.trim().is_empty()).then_some(configured_provider))
        .unwrap_or("")
}

fn resolve_workspace_path(workspace_root: &Path, raw_path: &str) -> Option<PathBuf> {
    if raw_path.trim().is_empty() {
        return None;
    }
    let root = workspace_root.canonicalize().ok()?;
    let requested = Path::new(raw_path);
    let candidate = if requested.is_absolute() {
        requested.to_path_buf()
    } else {
        root.join(requested)
    };
    let resolved = candidate.canonicalize().ok()?;
    resolved.starts_with(&root).then_some(resolved)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex as StdMutex};
    use std::time::Duration;

    use axum::http::HeaderValue;
    use tempfile::tempdir;
    use tokio::sync::Notify;
    use tokio::time::{sleep, timeout};

    use super::*;

    fn test_settings(root: &std::path::Path) -> Settings {
        let mut settings = crate::config::test_settings(root);
        settings.api.token = "secret".to_string();
        settings.llm.llm_model = "openai/gpt-5".to_string();
        settings.llm.llm_model_aux = "openai/gpt-5-mini".to_string();
        settings
    }

    fn authenticated_headers() -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Bearer secret"),
        );
        headers
    }

    async fn assert_json_error(response: Response, status: StatusCode, message: &str) {
        assert_eq!(response.status(), status);
        let body = response_json(response).await;
        assert_eq!(body["error"], message);
    }

    async fn response_json(response: Response) -> Value {
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        serde_json::from_slice(&body).unwrap()
    }

    fn pending_reaction(message_id: i64, emoji: &str) -> PendingReaction {
        PendingReaction {
            chat_id: 42,
            message_id,
            emoji: emoji.to_string(),
        }
    }

    #[test]
    fn actor_events_carry_group_and_surface_user_notify() {
        let mut event = ActorEvent::new("task_state_changed", "worker-1");
        event.group = "default".to_string();
        event.payload = serde_json::json!({
            "from": "pending",
            "to": "in_progress",
            "note": "SECRET_ACTOR_TASK_STATE_CANARY",
            "task_state_note": "SECOND_SECRET_ACTOR_TASK_STATE_CANARY",
        })
        .as_object()
        .cloned()
        .unwrap();
        let api_event = actor_event_to_api(&event).unwrap();
        assert_eq!(api_event.event, "actor.task");
        assert_eq!(api_event.data["group"], "default");
        assert_eq!(api_event.data["payload"]["from"], "pending");
        assert_eq!(api_event.data["payload"]["to"], "in_progress");
        assert!(api_event.data["payload"].get("note").is_none());
        assert!(api_event.data["payload"].get("task_state_note").is_none());

        let mut notify = ActorEvent::new("user_notify", "worker-1");
        notify.group = "default".to_string();
        notify.payload = serde_json::json!({
            "message": "Need your input on the draft.",
            "kind": "user_notify",
        })
        .as_object()
        .cloned()
        .unwrap();
        let api_event = actor_event_to_api(&notify).unwrap();
        assert_eq!(api_event.event, "actor.user_notify");
        assert_eq!(api_event.data["actor_id"], "worker-1");
        assert_eq!(
            api_event.data["payload"]["message"],
            "Need your input on the draft."
        );
    }

    #[test]
    fn presented_token_prefers_bearer_then_custom_header() {
        let mut headers = HeaderMap::new();
        headers.insert("x-lethe-token", HeaderValue::from_static("fallback"));
        assert_eq!(presented_api_token(&headers), "fallback");

        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Bearer secret"),
        );
        assert_eq!(presented_api_token(&headers), "secret");
    }

    #[test]
    fn workspace_file_resolution_rejects_traversal() {
        let tmp = tempdir().unwrap();
        let workspace = tmp.path().join("workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        std::fs::write(workspace.join("ok.txt"), "ok").unwrap();
        std::fs::write(tmp.path().join("outside.txt"), "no").unwrap();

        assert_eq!(
            resolve_workspace_path(&workspace, "ok.txt")
                .unwrap()
                .file_name()
                .unwrap(),
            "ok.txt"
        );
        assert!(resolve_workspace_path(&workspace, "../outside.txt").is_none());
    }

    #[test]
    fn api_event_preserves_event_name_and_data() {
        let event = ApiEvent::new("text", json!({"content": "hello"}));
        assert_eq!(event.event, "text");
        assert_eq!(event.data["content"], "hello");
    }

    #[test]
    fn session_history_filters_internal_checkpoints_and_turns_server_side() {
        const CANARY: &str = "SECRET_CHECKPOINT_CANARY";
        let tmp = tempdir().unwrap();
        let settings = test_settings(tmp.path());
        let memory = crate::memory::MemoryStore::from_settings(&settings).unwrap();

        memory
            .messages
            .add(crate::memory::MessageRole::User, "visible question", None)
            .unwrap();
        memory
            .messages
            .add(
                crate::memory::MessageRole::Assistant,
                CANARY,
                Some(raw_message_metadata_value(
                    MessageVisibility::Internal,
                    RawMessageKind::Checkpoint,
                    "tool_loop",
                )),
            )
            .unwrap();
        memory
            .messages
            .add(
                crate::memory::MessageRole::User,
                "internal wake prompt",
                Some(raw_message_metadata_value(
                    MessageVisibility::Internal,
                    RawMessageKind::Wake,
                    "wake",
                )),
            )
            .unwrap();
        memory
            .messages
            .add(
                crate::memory::MessageRole::Assistant,
                "legacy untagged internal answer",
                None,
            )
            .unwrap();
        memory
            .messages
            .add(
                crate::memory::MessageRole::User,
                "next visible question",
                None,
            )
            .unwrap();

        let visible = user_visible_session_messages(memory.messages.get_recent(20).unwrap());
        let contents = visible
            .iter()
            .map(|message| message.content.as_str())
            .collect::<Vec<_>>();
        assert_eq!(contents, ["visible question", "next visible question"]);
        assert!(contents.iter().all(|content| !content.contains(CANARY)));
    }

    #[test]
    fn confirmed_wake_text_is_persisted_as_user_visible_proactive_history() {
        let tmp = tempdir().unwrap();
        let settings = test_settings(tmp.path());
        let agent = Agent::from_settings(settings).unwrap();

        persist_wake_visible_history(
            &agent,
            vec!["first confirmed bubble".to_string(), "   ".to_string()],
        );

        let stored = agent.memory().messages.get_recent(10).unwrap();
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].content, "first confirmed bubble");
        let metadata = MessageMetadata::from_value(Some(&stored[0].metadata));
        assert!(!metadata.is_internal());
        assert_eq!(metadata.kind, Some(MessageKind::Proactive));
    }

    #[test]
    fn wake_target_prefers_explicit_chat_then_configured_user() {
        let tmp = tempdir().unwrap();
        let mut settings = test_settings(tmp.path());
        settings.telegram.allowed_user_ids = vec![42];

        assert_eq!(resolve_wake_chat_id(&settings, Some(99)), Some(99));
        assert_eq!(resolve_wake_chat_id(&settings, None), Some(42));
        assert_eq!(resolve_wake_chat_id(&settings, Some(0)), None);

        settings.telegram.allowed_user_ids.clear();
        assert_eq!(resolve_wake_chat_id(&settings, None), None);
    }

    #[test]
    fn wake_runtime_binds_telegram_observer_and_secure_prompt() {
        let tmp = tempdir().unwrap();
        let hub = crate::agent_id::secure_prompt::SecurePromptHub::new(
            tmp.path().join("secure-prompt.sock"),
            Arc::new(|_, _| {}),
        );

        let (runtime, delivery_guard) =
            wake_tool_runtime("telegram-token".to_string(), 42, Some(hub.clone()));
        let telegram = runtime.telegram.as_ref().expect("telegram egress");

        assert_eq!(telegram.token, "telegram-token");
        assert_eq!(telegram.chat_id, 42);
        assert_eq!(telegram.user_id, Some(42));
        assert_eq!(telegram.last_message_id, None);
        let runtime_guard = telegram.guard.as_ref().expect("wake delivery guard");
        assert!(Arc::ptr_eq(runtime_guard, &delivery_guard));
        assert_eq!(runtime_guard.lock().unwrap().visible_messages_sent(), 0);
        runtime_guard.lock().unwrap().record_visible_message();
        assert_eq!(delivery_guard.lock().unwrap().visible_messages_sent(), 1);
        assert!(!telegram.dry_run);
        assert!(telegram.sent_messages.is_none());
        assert!(
            runtime.observer.is_some(),
            "deep-model notice/typing observer"
        );
        assert_eq!(
            runtime
                .secure_prompt
                .as_ref()
                .expect("secure-prompt hub")
                .socket_path(),
            hub.socket_path()
        );
    }

    #[test]
    fn wake_guard_delivery_atomically_captures_and_drains_pending_reactions() {
        let guard = Arc::new(StdMutex::new(TelegramTurnGuard::new()));
        {
            let mut guard = guard.lock().unwrap();
            guard.record_visible_text("confirmed tool message");
            guard.queue_pending_reaction(42, 70, "👍");
            guard.queue_pending_reaction(42, 71, "🔥");
        }

        let delivery = take_wake_guard_delivery(&guard).unwrap();

        assert_eq!(delivery.tool_messages_sent, 1);
        assert_eq!(delivery.visible_texts, ["confirmed tool message"]);
        assert_eq!(
            delivery.pending_reactions,
            vec![pending_reaction(70, "👍"), pending_reaction(71, "🔥")]
        );
        let guard = guard.lock().unwrap();
        assert_eq!(guard.visible_messages_sent(), 1);
        assert!(!guard.has_pending_reactions());
    }

    #[tokio::test]
    async fn wake_delivery_does_not_duplicate_a_tool_delivered_message() {
        let fallback_calls = Arc::new(AtomicUsize::new(0));
        let calls = fallback_calls.clone();

        let response = finish_wake_delivery(
            42,
            TurnResult::Complete("redundant final".to_string()),
            2,
            Vec::new(),
            |_| async { Ok(()) },
            move |_| {
                calls.fetch_add(1, Ordering::SeqCst);
                async { Ok(99) }
            },
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        let body = response_json(response).await;
        assert_eq!(body["success"], true);
        assert_eq!(body["turn_completed"], true);
        assert_eq!(body["chat_id"], 42);
        assert_eq!(body["reply_chars"], 15);
        assert_eq!(body["delivered"], true);
        assert_eq!(body["delivery_status"], "tool_delivered");
        assert_eq!(body["delivered_messages"], 2);
        assert_eq!(fallback_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn wake_delivery_keeps_confirmed_tool_message_duplicate_safe_when_reaction_fails() {
        let fallback_calls = Arc::new(AtomicUsize::new(0));
        let calls = fallback_calls.clone();

        let response = finish_wake_delivery(
            42,
            TurnResult::Complete("redundant final".to_string()),
            1,
            vec![pending_reaction(70, "👍"), pending_reaction(71, "🔥")],
            |pending| async move {
                if pending.message_id == 70 {
                    Ok(())
                } else {
                    Err("reaction rejected".to_string())
                }
            },
            move |_| {
                calls.fetch_add(1, Ordering::SeqCst);
                async { Ok(99) }
            },
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        let body = response_json(response).await;
        assert_eq!(body["success"], false);
        assert_eq!(body["turn_completed"], true);
        assert_eq!(body["delivered"], true);
        assert_eq!(body["delivered_messages"], 1);
        assert_eq!(body["requested_reactions"], 2);
        assert_eq!(body["delivered_reactions"], 1);
        assert_eq!(body["failed_reactions"], 1);
        assert_eq!(
            body["delivery_status"],
            "tool_delivered_reactions_partially_delivered"
        );
        assert_eq!(body["reaction_errors"][0], "reaction rejected");
        assert_eq!(fallback_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn wake_delivery_sends_the_final_reply_as_a_fallback() {
        let sent = Arc::new(StdMutex::new(Vec::<String>::new()));
        let recorded = sent.clone();

        let response = finish_wake_delivery(
            42,
            TurnResult::Complete("  final answer  ".to_string()),
            0,
            Vec::new(),
            |_| async { Ok(()) },
            move |text| {
                recorded.lock().unwrap().push(text);
                async { Ok(73) }
            },
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        let body = response_json(response).await;
        assert_eq!(body["success"], true);
        assert_eq!(body["turn_completed"], true);
        assert_eq!(body["chat_id"], 42);
        assert_eq!(body["reply_chars"], 16);
        assert_eq!(body["delivered"], true);
        assert_eq!(body["delivery_status"], "fallback_delivered");
        assert_eq!(body["delivered_messages"], 1);
        assert_eq!(body["message_id"], 73);
        assert_eq!(sent.lock().unwrap().as_slice(), ["final answer"]);
    }

    #[tokio::test]
    async fn wake_delivery_never_uses_a_checkpoint_as_fallback() {
        let fallback_calls = Arc::new(AtomicUsize::new(0));
        let calls = fallback_calls.clone();

        let response = finish_wake_delivery(
            42,
            TurnResult::Checkpointed,
            0,
            Vec::new(),
            |_| async { Ok(()) },
            move |_| {
                calls.fetch_add(1, Ordering::SeqCst);
                async { Ok(73) }
            },
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        let body = response_json(response).await;
        assert_eq!(body["success"], false);
        assert_eq!(body["turn_completed"], false);
        assert_eq!(body["reply_chars"], 0);
        assert_eq!(body["delivered"], false);
        assert_eq!(body["delivery_status"], "checkpoint_suppressed");
        assert_eq!(body["delivered_messages"], 0);
        assert_eq!(fallback_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn wake_checkpoint_keeps_confirmed_tool_delivery_non_retryable() {
        let fallback_calls = Arc::new(AtomicUsize::new(0));
        let calls = fallback_calls.clone();

        let response = finish_wake_delivery(
            42,
            TurnResult::Checkpointed,
            2,
            vec![pending_reaction(70, "👍")],
            |_| async { Ok(()) },
            move |_| {
                calls.fetch_add(1, Ordering::SeqCst);
                async { Ok(73) }
            },
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        let body = response_json(response).await;
        assert_eq!(body["success"], false);
        assert_eq!(body["turn_completed"], false);
        assert_eq!(body["delivered"], true);
        assert_eq!(body["delivery_status"], "checkpoint_suppressed");
        assert_eq!(body["delivered_messages"], 2);
        assert_eq!(body["delivered_reactions"], 1);
        assert_eq!(fallback_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn wake_delivery_reports_reaction_failure_even_when_fallback_text_arrives() {
        let fallback_calls = Arc::new(AtomicUsize::new(0));
        let calls = fallback_calls.clone();

        let response = finish_wake_delivery(
            42,
            TurnResult::Complete("final answer".to_string()),
            0,
            vec![pending_reaction(70, "👍")],
            |_| async { Err("reaction rejected".to_string()) },
            move |_| {
                calls.fetch_add(1, Ordering::SeqCst);
                async { Ok(73) }
            },
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        let body = response_json(response).await;
        assert_eq!(body["success"], false);
        assert_eq!(body["turn_completed"], true);
        assert_eq!(body["delivered"], true);
        assert_eq!(
            body["delivery_status"],
            "fallback_delivered_reactions_failed"
        );
        assert_eq!(body["delivered_messages"], 1);
        assert_eq!(body["requested_reactions"], 1);
        assert_eq!(body["delivered_reactions"], 0);
        assert_eq!(body["failed_reactions"], 1);
        assert_eq!(body["reaction_errors"][0], "reaction rejected");
        assert_eq!(fallback_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn wake_delivery_accepts_confirmed_reaction_only_with_empty_final_reply() {
        let fallback_calls = Arc::new(AtomicUsize::new(0));
        let calls = fallback_calls.clone();

        let response = finish_wake_delivery(
            42,
            TurnResult::Complete("   ".to_string()),
            0,
            vec![pending_reaction(70, "👍")],
            |_| async { Ok(()) },
            move |_| {
                calls.fetch_add(1, Ordering::SeqCst);
                async { Ok(73) }
            },
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        let body = response_json(response).await;
        assert_eq!(body["success"], true);
        assert_eq!(body["turn_completed"], true);
        assert_eq!(body["delivered"], true);
        assert_eq!(body["delivery_status"], "reaction_delivered");
        assert_eq!(body["delivered_messages"], 0);
        assert_eq!(body["requested_reactions"], 1);
        assert_eq!(body["delivered_reactions"], 1);
        assert_eq!(body["failed_reactions"], 0);
        assert_eq!(fallback_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn wake_delivery_splits_a_long_fallback_into_confirmed_chunks() {
        let sent = Arc::new(StdMutex::new(Vec::<String>::new()));
        let recorded = sent.clone();

        let response = finish_wake_delivery(
            42,
            TurnResult::Complete("x".repeat(5000)),
            0,
            Vec::new(),
            |_| async { Ok(()) },
            move |text| {
                let message_id = {
                    let mut recorded = recorded.lock().unwrap();
                    recorded.push(text);
                    90 + recorded.len() as i64
                };
                async move { Ok(message_id) }
            },
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        let body = response_json(response).await;
        assert_eq!(body["success"], true);
        assert_eq!(body["reply_chars"], 5000);
        assert_eq!(body["delivery_status"], "fallback_delivered");
        assert_eq!(body["delivered_messages"], 2);
        assert_eq!(body["message_id"], 92);
        let sent = sent.lock().unwrap();
        assert_eq!(sent.len(), 2);
        assert!(sent.iter().all(|chunk| chunk.len() <= 4096));
        assert_eq!(sent.concat(), "x".repeat(5000));
    }

    #[tokio::test]
    async fn wake_delivery_rejects_an_empty_undelivered_reply() {
        let fallback_calls = Arc::new(AtomicUsize::new(0));
        let calls = fallback_calls.clone();

        let response = finish_wake_delivery(
            42,
            TurnResult::Complete("   ".to_string()),
            0,
            Vec::new(),
            |_| async { Ok(()) },
            move |_| {
                calls.fetch_add(1, Ordering::SeqCst);
                async { Ok(99) }
            },
        )
        .await;

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let body = response_json(response).await;
        assert_eq!(body["success"], false);
        assert_eq!(body["turn_completed"], true);
        assert_eq!(body["delivered"], false);
        assert_eq!(body["delivery_status"], "empty_reply");
        assert_eq!(body["delivered_messages"], 0);
        assert_eq!(fallback_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn wake_delivery_reports_fallback_send_failure() {
        let response = finish_wake_delivery(
            42,
            TurnResult::Complete("final answer".to_string()),
            0,
            Vec::new(),
            |_| async { Ok(()) },
            |_| async { Err("telegram offline".to_string()) },
        )
        .await;

        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
        let body = response_json(response).await;
        assert_eq!(body["success"], false);
        assert_eq!(body["turn_completed"], true);
        assert_eq!(body["delivered"], false);
        assert_eq!(body["delivery_status"], "fallback_failed");
        assert_eq!(body["delivered_messages"], 0);
        assert!(body["error"].as_str().unwrap().contains("telegram offline"));
    }

    #[tokio::test]
    async fn wake_delivery_partial_fallback_is_non_retry_after_confirmed_chunk() {
        let calls = Arc::new(AtomicUsize::new(0));
        let recorded = calls.clone();

        let response = finish_wake_delivery(
            42,
            TurnResult::Complete("x".repeat(5000)),
            0,
            Vec::new(),
            |_| async { Ok(()) },
            move |_| {
                let call = recorded.fetch_add(1, Ordering::SeqCst);
                async move {
                    if call == 0 {
                        Ok(91)
                    } else {
                        Err("telegram offline".to_string())
                    }
                }
            },
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        let body = response_json(response).await;
        assert_eq!(body["success"], false);
        assert_eq!(body["turn_completed"], true);
        assert_eq!(body["delivered"], true);
        assert_eq!(body["delivery_status"], "fallback_partially_delivered");
        assert_eq!(body["delivered_messages"], 1);
        assert_eq!(body["message_id"], 91);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn wake_failure_notice_is_non_retry_after_confirmed_delivery() {
        let sent = Arc::new(StdMutex::new(Vec::<String>::new()));
        let recorded = sent.clone();

        let response = finish_wake_failure(
            42,
            0,
            Vec::new(),
            StatusCode::INTERNAL_SERVER_ERROR,
            "turn_failed",
            WAKE_TURN_FAILURE_MESSAGE,
            "turn failed: provider disconnected".to_string(),
            |_| async { Ok(()) },
            move |text| {
                recorded.lock().unwrap().push(text);
                async { Ok(81) }
            },
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        let body = response_json(response).await;
        assert_eq!(body["success"], false);
        assert_eq!(body["turn_completed"], false);
        assert_eq!(body["delivered"], true);
        assert_eq!(body["delivery_status"], "turn_failed_notice_delivered");
        assert_eq!(body["delivered_messages"], 1);
        assert_eq!(body["message_id"], 81);
        assert_eq!(sent.lock().unwrap().as_slice(), [WAKE_TURN_FAILURE_MESSAGE]);
    }

    #[tokio::test]
    async fn wake_timeout_and_limit_notices_are_non_retry_after_confirmed_delivery() {
        for (turn_status, failure_kind) in [
            (StatusCode::GATEWAY_TIMEOUT, "turn_timeout"),
            (StatusCode::TOO_MANY_REQUESTS, "limit"),
        ] {
            let response = finish_wake_failure(
                42,
                0,
                Vec::new(),
                turn_status,
                failure_kind,
                WAKE_TURN_FAILURE_MESSAGE,
                format!("{failure_kind} reached"),
                |_| async { Ok(()) },
                |_| async { Ok(82) },
            )
            .await;

            assert_eq!(response.status(), StatusCode::OK);
            let body = response_json(response).await;
            assert_eq!(body["success"], false);
            assert_eq!(body["turn_completed"], false);
            assert_eq!(body["delivered"], true);
            assert_eq!(
                body["delivery_status"],
                format!("{failure_kind}_notice_delivered")
            );
            assert_eq!(body["delivered_messages"], 1);
            assert_eq!(body["message_id"], 82);
        }
    }

    #[tokio::test]
    async fn wake_failure_does_not_duplicate_after_guard_confirmed_tool_delivery() {
        let notification_calls = Arc::new(AtomicUsize::new(0));
        let calls = notification_calls.clone();
        let reaction_calls = Arc::new(AtomicUsize::new(0));
        let reactions = reaction_calls.clone();
        let delivery_guard = Arc::new(StdMutex::new(TelegramTurnGuard::new()));
        {
            let mut guard = delivery_guard.lock().unwrap();
            guard.record_visible_message();
            guard.queue_pending_reaction(42, 70, "👍");
        }
        let guard_delivery = take_wake_guard_delivery(&delivery_guard).unwrap();

        let response = finish_wake_failure(
            42,
            guard_delivery.tool_messages_sent,
            guard_delivery.pending_reactions,
            StatusCode::GATEWAY_TIMEOUT,
            "turn_timeout",
            WAKE_TURN_TIMEOUT_MESSAGE,
            "turn timed out after 240 seconds".to_string(),
            move |_| {
                reactions.fetch_add(1, Ordering::SeqCst);
                async { Err("reaction rejected".to_string()) }
            },
            move |_| {
                calls.fetch_add(1, Ordering::SeqCst);
                async { Ok(99) }
            },
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        let body = response_json(response).await;
        assert_eq!(body["success"], false);
        assert_eq!(body["turn_completed"], false);
        assert_eq!(body["delivered"], true);
        assert_eq!(
            body["delivery_status"],
            "turn_timeout_after_tool_delivery_reactions_failed"
        );
        assert_eq!(body["delivered_messages"], 1);
        assert_eq!(body["requested_reactions"], 1);
        assert_eq!(body["delivered_reactions"], 0);
        assert_eq!(body["failed_reactions"], 1);
        assert_eq!(reaction_calls.load(Ordering::SeqCst), 1);
        assert_eq!(notification_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn wake_failure_reports_notification_send_failure() {
        let response = finish_wake_failure(
            42,
            0,
            Vec::new(),
            StatusCode::GATEWAY_TIMEOUT,
            "turn_timeout",
            WAKE_TURN_TIMEOUT_MESSAGE,
            "turn timed out after 240 seconds".to_string(),
            |_| async { Ok(()) },
            |_| async { Err("telegram offline".to_string()) },
        )
        .await;

        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
        let body = response_json(response).await;
        assert_eq!(body["success"], false);
        assert_eq!(body["turn_completed"], false);
        assert_eq!(body["delivered"], false);
        assert_eq!(body["delivery_status"], "turn_timeout_notice_failed");
        assert_eq!(body["delivered_messages"], 0);
        assert!(body["error"].as_str().unwrap().contains("telegram offline"));
    }

    #[tokio::test]
    async fn wake_turn_timeout_is_bounded() {
        let result =
            run_wake_turn_with_timeout(Duration::from_millis(1), std::future::pending::<()>())
                .await;

        assert!(result.is_err());
    }

    #[test]
    fn wake_turn_and_delivery_budgets_finish_before_the_scheduler_deadline() {
        assert!(
            WAKE_TURN_TIMEOUT + WAKE_DELIVERY_TIMEOUT < Duration::from_secs(300),
            "the endpoint needs time to serialize its response before the 300s caller deadline"
        );
    }

    #[tokio::test]
    async fn wake_delivery_budget_reports_an_unconfirmed_timeout() {
        let progress = Arc::new(WakeDeliveryProgress::new(0, 0));
        let response = run_wake_delivery_with_timeout(
            Duration::from_millis(1),
            42,
            12,
            true,
            "fallback",
            progress,
            std::future::pending::<Response>(),
        )
        .await;

        assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);
        let body = response_json(response).await;
        assert_eq!(body["success"], false);
        assert_eq!(body["turn_completed"], true);
        assert_eq!(body["delivered"], false);
        assert_eq!(body["delivery_status"], "fallback_delivery_timeout");
        assert_eq!(body["delivered_messages"], 0);
    }

    #[tokio::test]
    async fn wake_delivery_budget_preserves_confirmed_partial_progress() {
        let progress = Arc::new(WakeDeliveryProgress::new(1, 0));
        let response = run_wake_delivery_with_timeout(
            Duration::from_millis(1),
            42,
            5000,
            true,
            "fallback",
            progress,
            std::future::pending::<Response>(),
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        let body = response_json(response).await;
        assert_eq!(body["success"], false);
        assert_eq!(body["turn_completed"], true);
        assert_eq!(body["delivered"], true);
        assert_eq!(
            body["delivery_status"],
            "fallback_delivery_timeout_after_partial_delivery"
        );
        assert_eq!(body["delivered_messages"], 1);
    }

    #[tokio::test]
    async fn wake_reactions_and_fallback_share_one_delivery_timeout_budget() {
        let progress = Arc::new(WakeDeliveryProgress::new(0, 1));
        let reaction_progress = progress.clone();
        let fallback_calls = Arc::new(AtomicUsize::new(0));
        let calls = fallback_calls.clone();
        let delivery = finish_wake_delivery(
            42,
            TurnResult::Complete("fallback answer".to_string()),
            0,
            vec![pending_reaction(70, "👍")],
            move |_| {
                reaction_progress.record_reaction();
                async { Ok(()) }
            },
            move |_| {
                calls.fetch_add(1, Ordering::SeqCst);
                std::future::pending::<std::result::Result<i64, String>>()
            },
        );

        let response = run_wake_delivery_with_timeout(
            Duration::from_millis(1),
            42,
            15,
            true,
            "fallback",
            progress,
            delivery,
        )
        .await;

        assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);
        let body = response_json(response).await;
        assert_eq!(body["success"], false);
        assert_eq!(body["turn_completed"], true);
        assert_eq!(body["delivered"], true);
        assert_eq!(
            body["delivery_status"],
            "fallback_delivery_timeout_after_partial_delivery"
        );
        assert_eq!(body["delivered_messages"], 0);
        assert_eq!(body["requested_reactions"], 1);
        assert_eq!(body["delivered_reactions"], 1);
        assert_eq!(body["failed_reactions"], 0);
        assert_eq!(fallback_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn wake_rejects_invalid_requests_before_starting_a_turn() {
        let tmp = tempdir().unwrap();
        let state = ApiState::from_settings(test_settings(tmp.path())).unwrap();

        assert_json_error(
            wake(
                State(state.clone()),
                HeaderMap::new(),
                Json(WakeRequest {
                    message: "morning brief".to_string(),
                    chat_id: Some(42),
                }),
            )
            .await,
            StatusCode::UNAUTHORIZED,
            "unauthorized",
        )
        .await;

        assert_json_error(
            wake(
                State(state.clone()),
                authenticated_headers(),
                Json(WakeRequest {
                    message: "   ".to_string(),
                    chat_id: Some(42),
                }),
            )
            .await,
            StatusCode::BAD_REQUEST,
            "message is required",
        )
        .await;

        assert_json_error(
            wake(
                State(state),
                authenticated_headers(),
                Json(WakeRequest {
                    message: "morning brief".to_string(),
                    chat_id: Some(42),
                }),
            )
            .await,
            StatusCode::SERVICE_UNAVAILABLE,
            "Telegram bot token is not configured",
        )
        .await;

        let mut settings = test_settings(tmp.path());
        settings.telegram.bot_token = "telegram-token".to_string();
        settings.telegram.allowed_user_ids.clear();
        let state = ApiState::from_settings(settings).unwrap();
        assert_json_error(
            wake(
                State(state),
                authenticated_headers(),
                Json(WakeRequest {
                    message: "morning brief".to_string(),
                    chat_id: None,
                }),
            )
            .await,
            StatusCode::BAD_REQUEST,
            "no chat_id given and no allowed user configured to deliver to",
        )
        .await;
    }

    #[tokio::test]
    async fn api_subscribes_to_brainstem_only_for_an_authenticated_event_stream() {
        let tmp = tempdir().unwrap();
        let brainstem = BrainstemHandle::new();
        let state = ApiState::from_settings(test_settings(tmp.path()))
            .unwrap()
            .with_brainstem(brainstem.clone());
        assert_eq!(brainstem.subscriber_count(), 0);

        let unauthorized = events(State(state.clone()), HeaderMap::new()).await;
        assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(brainstem.subscriber_count(), 0);

        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Bearer secret"),
        );
        let response = events(State(state), headers).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(brainstem.subscriber_count(), 1);

        drop(response);
        assert_eq!(brainstem.subscriber_count(), 0);
    }

    #[tokio::test]
    async fn client_tool_context_routes_transport_tools_to_session_events() {
        let tmp = tempdir().unwrap();
        let state = ApiState::from_settings(test_settings(tmp.path())).unwrap();
        let (sender, mut receiver) = mpsc::channel::<ApiEvent>(SESSION_QUEUE_DEPTH);
        let session_id = state.register_session(99, sender).await;
        let context = state
            .client_tool_context(&session_id, 99, Some(42))
            .await
            .unwrap();

        let message_payload = context.send_message(
            "progress",
            "html",
            Some(r#"{"inline_keyboard":[[{"text":"Start","callback_data":"start"}]]}"#),
        );
        let message: Value = serde_json::from_str(&message_payload).unwrap();
        assert_eq!(message["success"], true);
        let event = receiver.recv().await.unwrap();
        assert_eq!(event.event, "text");
        assert_eq!(event.data["content"], "progress");
        assert_eq!(event.data["parse_mode"], "HTML");
        assert_eq!(event.data["message_id"], 1);
        assert_eq!(
            event.data["reply_markup"]["inline_keyboard"][0][0]["text"],
            "Start"
        );

        let reaction_payload = context.react("✅", 0);
        let reaction: Value = serde_json::from_str(&reaction_payload).unwrap();
        assert_eq!(reaction["success"], true);
        let event = receiver.recv().await.unwrap();
        assert_eq!(event.event, "reaction");
        assert_eq!(event.data["emoji"], "✅");
        assert_eq!(event.data["message_id"], 42);
    }

    #[tokio::test]
    async fn api_turn_observer_drops_reasoning_but_keeps_assistant_deltas() {
        let chat_id = 99;
        let session_id = "observer-test".to_string();
        let (sender, mut receiver) = mpsc::channel::<ApiEvent>(SESSION_QUEUE_DEPTH);
        let mut sessions = ApiSessions::default();
        sessions.by_chat.insert(chat_id, session_id.clone());
        sessions
            .by_id
            .insert(session_id, ApiSession { chat_id, sender });
        let (broadcast, mut broadcast_receiver) = broadcast::channel(EVENT_QUEUE_DEPTH);
        let observer = ApiTurnObserver::new(Arc::new(Mutex::new(sessions)), chat_id, broadcast);

        observer.on_reasoning_delta("SECRET_REASONING_CANARY");

        assert!(matches!(
            receiver.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        assert!(matches!(
            broadcast_receiver.try_recv(),
            Err(broadcast::error::TryRecvError::Empty)
        ));

        observer.on_assistant_delta("natural answer");

        let event = receiver.recv().await.unwrap();
        assert_eq!(event.event, "assistant.delta");
        assert_eq!(event.data, json!({"content": "natural answer"}));
        assert!(matches!(
            broadcast_receiver.try_recv(),
            Err(broadcast::error::TryRecvError::Empty)
        ));
    }

    #[tokio::test]
    async fn api_stream_guard_cancels_current_session_on_drop() {
        let tmp = tempdir().unwrap();
        let state = ApiState::from_settings(test_settings(tmp.path())).unwrap();
        let (sender, _receiver) = mpsc::channel::<ApiEvent>(SESSION_QUEUE_DEPTH);
        let session_id = state.register_session(99, sender).await;
        let started = Arc::new(Notify::new());
        let callback: ProcessCallback = {
            let started = started.clone();
            Arc::new(move |_context: ProcessContext| {
                let started = started.clone();
                Box::pin(async move {
                    started.notify_waiters();
                    sleep(Duration::from_secs(60)).await;
                    Ok(())
                })
            })
        };

        state
            .conversations
            .add_message(99, 7, "hello", None, Some(callback))
            .await;
        timeout(Duration::from_secs(1), started.notified())
            .await
            .unwrap();
        assert!(state.conversations.is_processing(99).await);

        drop(ApiStreamGuard::new(state.clone(), 99, session_id.clone()));

        timeout(Duration::from_secs(1), async {
            loop {
                if !state.conversations.is_processing(99).await
                    && !state.session_matches_chat(99, &session_id).await
                {
                    break;
                }
                sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
    }
}
