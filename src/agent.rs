use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use anyhow::anyhow;
use chrono::Local;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use thiserror::Error;
use tokio::sync::broadcast;

mod summarizer;
mod tool_loop;
use tool_loop::{
    TurnExecutionContext, TurnOutput, actor_turn_executor, complete_turn_with_tools_config_shared,
};
#[cfg(test)]
use tool_loop::{actor_turn_instruction, extract_image_views, image_view_message};

use crate::actor::background::{
    BackgroundResult, collect_user_notifications_from_events, queue_dmn_heartbeat,
};
use crate::actor::notification::NotificationGate;
use crate::actor::{ActorConfig, ActorRegistry, ActorRuntime};
use crate::config::Settings;
use crate::llm::prompts::PromptStore;
use crate::llm::response_format::normalize_message_envelope;
use crate::llm::{
    HistoricalToolCall, HistoricalToolResponse, LlmAttachment, LlmMessage, LlmRole, LlmRouter,
    LlmRouterConfig, PromptBuilder, dialect_for_model,
};
use crate::memory::message_metadata::{
    HISTORY_CONTENT_KEY, MessageKind, MessageMetadata, MessageVisibility, RawMessageKind,
    annotate_map, metadata_value, raw_message_kind, raw_metadata_value,
    user_visible_history_content,
};
use crate::memory::messages::{MessageHistoryError, MessageRole, StoredMessage};
use crate::memory::recall::{Hippocampus, HippocampusConfig, HippocampusError};
use crate::memory::{MemoryStore, MemoryStoreError};
use crate::scheduler::curator::{CuratorError, CuratorRunStats, MemoryCurator};
use crate::tools::hosted_plugins::HostedPluginClient;
use crate::tools::registry::{
    ActorToolContext, SharedActorRegistry, ToolPolicy, ToolRuntime, requestable_tools_directory_for,
};
use crate::tools::shell::ShellTools;

#[derive(Debug, Error)]
pub enum AgentError {
    #[error(transparent)]
    MemoryStore(#[from] MemoryStoreError),
    #[error(transparent)]
    Messages(#[from] MessageHistoryError),
    #[error(transparent)]
    Hippocampus(#[from] HippocampusError),
    #[error(transparent)]
    Llm(#[from] anyhow::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Curator(#[from] CuratorError),
}

pub type AgentResult<T> = Result<T, AgentError>;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AgentTurn {
    pub messages: Vec<LlmMessage>,
    pub recall: Option<String>,
    pub synthetic: bool,
    /// History messages that compaction dropped from this turn. Carried so
    /// the post-turn summarizer can incorporate them into the rolling
    /// conversation_summary block.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub dropped_for_summary: Vec<LlmMessage>,
}

struct PreparedAgentTurn {
    turn: AgentTurn,
    resumable_checkpoint_id: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AgentOptions {
    pub use_hippocampus: bool,
    /// History compaction budget for this turn. Derived per-turn from the
    /// configured context limit + recent prompt token usage (see
    /// [`CompactionBudget::from_settings`]); tests and entry points without
    /// settings should leave [`CompactionBudget::legacy_default`].
    #[serde(skip)]
    pub compaction_budget: CompactionBudget,
}

impl Default for AgentOptions {
    fn default() -> Self {
        Self {
            use_hippocampus: true,
            compaction_budget: CompactionBudget::legacy_default(),
        }
    }
}

/// A single agent turn input. Build via [`TurnRequest::new`] and the
/// `with_*` setters; pass to [`Agent::chat_once`], [`Agent::chat_once_result`],
/// or [`Agent::prepare_turn`].
#[derive(Clone, Debug, Default)]
pub struct TurnRequest {
    pub message: String,
    pub attachments: Vec<LlmAttachment>,
    pub metadata: Option<Value>,
    pub runtime: crate::tools::registry::ToolRuntime,
    pub options: AgentOptions,
}

impl TurnRequest {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            ..Self::default()
        }
    }

    pub fn with_attachments(mut self, attachments: Vec<LlmAttachment>) -> Self {
        self.attachments = attachments;
        self
    }

    pub fn with_metadata(mut self, metadata: Value) -> Self {
        self.metadata = Some(metadata);
        self
    }

    pub fn with_runtime(mut self, runtime: crate::tools::registry::ToolRuntime) -> Self {
        self.runtime = runtime;
        self
    }

    pub fn with_options(mut self, options: AgentOptions) -> Self {
        self.options = options;
        self
    }
}

/// Public disposition of a completed agent call.
///
/// A cut-short tool loop deliberately does not expose its checkpoint text:
/// callers can branch on the typed status, but cannot accidentally forward
/// internal GOAL/DONE/REMAINING/NEXT state to a user-facing transport.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TurnResult {
    Complete(String),
    Checkpointed,
}

impl TurnResult {
    pub fn complete_text(&self) -> Option<&str> {
        match self {
            Self::Complete(text) => Some(text),
            Self::Checkpointed => None,
        }
    }

    pub fn into_complete_text(self) -> Option<String> {
        match self {
            Self::Complete(text) => Some(text),
            Self::Checkpointed => None,
        }
    }

    pub fn is_checkpointed(&self) -> bool {
        matches!(self, Self::Checkpointed)
    }
}

/// Fixed user-safe status for an interrupted ordinary conversation turn.
/// Never interpolate the raw checkpoint or its stop reason into this text.
pub const TURN_CHECKPOINT_NOTICE: &str = "I couldn't finish that reliably in this turn. I've saved the progress and can continue without starting over.";
const RESUMABLE_CHECKPOINT_PREFIX: &str = "<internal_resumable_checkpoint>\n";
const RESUMABLE_CHECKPOINT_SUFFIX: &str = "\n</internal_resumable_checkpoint>";
const CHECKPOINT_ID_KEY: &str = "checkpoint_id";

/// A conversation message produced outside the requesting client's own stream —
/// e.g. a Telegram turn — broadcast so other live surfaces (an open web tab) can
/// append it. `event` is the SSE event name, `data` its JSON payload.
#[derive(Clone, Debug)]
pub struct ConversationEvent {
    pub event: String,
    pub data: Value,
}

/// Depth of the conversation-event broadcast buffer. Slow subscribers that lag
/// past this just miss intermediate events (they re-sync on reload).
const CONVERSATION_EVENT_DEPTH: usize = 256;

pub struct Agent {
    settings: Settings,
    memory: Arc<MemoryStore>,
    prompts: PromptStore,
    router: Arc<RwLock<LlmRouter>>,
    shell: ShellTools,
    /// Immutable capability ceiling selected when the agent is constructed.
    /// Per-turn runtimes may narrow this policy, but must never widen it.
    tool_policy: ToolPolicy,
    actor_registry: Option<SharedActorRegistry>,
    principal_actor_id: Option<String>,
    notification_gate: Mutex<NotificationGate>,
    processed_notification_events: Mutex<HashSet<String>>,
    /// `prompt_tokens` from the most recent LLM response. Drives the
    /// per-turn compaction budget so we shrink history when we're actually
    /// pressing the model's context limit, not based on a crude char guess.
    /// Zero means "no measurement yet".
    last_prompt_tokens: Arc<AtomicU64>,
    /// Broadcast of conversation messages from non-requesting transports (e.g.
    /// Telegram), so an open web client can append them live via `/events`.
    conversation_tx: broadcast::Sender<ConversationEvent>,
    /// In-flight conversation-summary update from the previous turn. The next
    /// turn briefly waits on this before assembling its prompt so a fast
    /// follow-up message can't read the summary block before the dropped
    /// batch has been merged into it (the old detached spawn raced exactly
    /// that way and lost context silently).
    pending_summary: tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
    hosted_plugins: Option<Arc<HostedPluginClient>>,
    /// Host-installed factory producing a per-turn observer for subagent
    /// turns (see [`Agent::install_subagent_observer`]). Captured by the
    /// actor turn executor at build time; `None` leaves subagent turns
    /// unobserved.
    subagent_observer: crate::tools::registry::SubagentObserverSlot,
}

/// How long the next turn waits for the previous turn's summary update before
/// proceeding anyway. Bounded so a slow aux model degrades to the old racy
/// behavior instead of blocking the user.
const SUMMARY_SYNC_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// Await a previously spawned background task, bounded by `timeout`. Clears
/// the slot when the task finished; leaves it in place (still running,
/// detached) on timeout. Free function so the sync-point semantics are unit
/// testable without an Agent.
async fn await_pending_task(
    slot: &tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
    timeout: std::time::Duration,
) {
    let mut guard = slot.lock().await;
    let Some(handle) = guard.as_mut() else {
        return;
    };
    match tokio::time::timeout(timeout, &mut *handle).await {
        Ok(_) => {
            *guard = None;
        }
        Err(_) => {
            tracing::warn!(
                "previous turn's summary update still running at next turn start — proceeding without it"
            );
        }
    }
}

impl Agent {
    pub fn from_settings(settings: Settings) -> AgentResult<Self> {
        let memory = Arc::new(MemoryStore::from_settings(&settings)?);
        Self::from_settings_with_memory(settings, memory)
    }

    /// Build the real Lethe agent around an injected memory backend. This is
    /// the hosted/multiplexing seam: prompt assembly, Hippocampus recall, tool
    /// semantics, history compaction, and curation remain exactly the same as
    /// the standalone binary.
    pub fn from_settings_with_memory(
        settings: Settings,
        memory: Arc<MemoryStore>,
    ) -> AgentResult<Self> {
        Self::from_settings_with_memory_policy(settings, memory, ToolPolicy::Full)
    }

    /// [`Agent::from_settings_with_memory`] with an explicit capability
    /// policy. A hosted multiplexer passes [`ToolPolicy::HostedSafe`] so the
    /// policy applies not only to the turns it constructs itself but also to
    /// everything the agent runs internally on the host's behalf: subagent
    /// turns spawned by the actor runtime and the `<available_on_request>`
    /// directories rendered into actor prompts.
    pub fn from_settings_with_memory_policy(
        settings: Settings,
        memory: Arc<MemoryStore>,
        tool_policy: ToolPolicy,
    ) -> AgentResult<Self> {
        let prompts = PromptStore::new(&settings.paths.workspace_dir, &settings.paths.config_dir);
        let router = Arc::new(RwLock::new(LlmRouter::new(LlmRouterConfig::from_settings(
            &settings,
        ))));
        let shell = ShellTools::new(&settings.paths.workspace_dir);
        let last_prompt_tokens = Arc::new(AtomicU64::new(0));
        let hosted_plugins = HostedPluginClient::from_config(&settings.hosted_plugins);
        let subagent_observer: crate::tools::registry::SubagentObserverSlot =
            Arc::new(std::sync::RwLock::new(None));
        let (actor_registry, principal_actor_id) = if settings.background.actors_enabled {
            let mut registry = ActorRegistry::new();
            registry.set_prompts(prompts.clone());
            registry.set_tool_policy(tool_policy);
            // Durable actor state: snapshots every mutation into the unified
            // memory DB and rehydrates unfinished subagents after a restart —
            // a deploy or self-restart interrupts work instead of erasing it.
            match crate::actor::ActorStore::open(settings.paths.memory_dir.join("lethe-memory.db"))
            {
                Ok(store) => registry.set_store(store),
                Err(error) => {
                    tracing::warn!(error = %error, "actor store unavailable — actor state will not survive restarts");
                }
            }
            let principal_id = registry.spawn(
                ActorConfig::new("cortex", "Serve the user.").in_group("main"),
                None,
                true,
            );
            registry.restore_unfinished(&principal_id);
            let runtime = ActorRuntime::new(registry);
            runtime
                .install_typed_turn_executor(actor_turn_executor(
                    settings.clone(),
                    memory.clone(),
                    router.clone(),
                    shell.clone(),
                    last_prompt_tokens.clone(),
                    hosted_plugins.clone(),
                    tool_policy,
                    subagent_observer.clone(),
                ))
                .map_err(|error| AgentError::Llm(anyhow!("actor runtime failed: {error}")))?;
            (Some(runtime), Some(principal_id))
        } else {
            (None, None)
        };
        Ok(Self {
            settings,
            memory,
            prompts,
            router,
            shell,
            tool_policy,
            actor_registry,
            principal_actor_id,
            notification_gate: Mutex::new(NotificationGate::new(15 * 60)),
            processed_notification_events: Mutex::new(HashSet::new()),
            last_prompt_tokens,
            conversation_tx: broadcast::channel(CONVERSATION_EVENT_DEPTH).0,
            pending_summary: tokio::sync::Mutex::new(None),
            hosted_plugins,
            subagent_observer,
        })
    }

    pub fn memory(&self) -> &MemoryStore {
        self.memory.as_ref()
    }

    /// Subscribe to conversation messages emitted by non-requesting transports
    /// (e.g. Telegram). The HTTP `/events` stream relays these to web clients.
    pub fn subscribe_conversation_events(&self) -> broadcast::Receiver<ConversationEvent> {
        self.conversation_tx.subscribe()
    }

    /// Broadcast a conversation message. No-op (returns) when nobody is listening.
    pub fn emit_conversation_event(&self, event: impl Into<String>, data: Value) {
        let _ = self.conversation_tx.send(ConversationEvent {
            event: event.into(),
            data,
        });
    }

    /// Most recent prompt-token count seen by the tool loop. Drives the
    /// TUI footer's context usage indicator. `None` before the first turn.
    pub fn last_prompt_tokens(&self) -> Option<u64> {
        match self.last_prompt_tokens.load(Ordering::Relaxed) {
            0 => None,
            value => Some(value),
        }
    }

    pub fn router_config(&self) -> AgentResult<LlmRouterConfig> {
        let router = self
            .router
            .read()
            .map_err(|error| AgentError::Llm(anyhow!("router lock poisoned: {error}")))?;
        Ok(router.config().clone())
    }

    pub fn reconfigure_models(
        &self,
        model: Option<&str>,
        aux_model: Option<&str>,
        deep_model: Option<&str>,
    ) -> AgentResult<serde_json::Value> {
        let mut router = self
            .router
            .write()
            .map_err(|error| AgentError::Llm(anyhow!("router lock poisoned: {error}")))?;
        let mut config = router.config().clone();
        let mut changed = serde_json::Map::new();

        if let Some(model) = model.map(str::trim).filter(|value| !value.is_empty())
            && model != config.model
        {
            changed.insert(
                "model".to_string(),
                json!({"old": config.model.clone(), "new": model}),
            );
            config.model = model.to_string();
        }
        if let Some(aux_model) = aux_model.map(str::trim).filter(|value| !value.is_empty())
            && aux_model != config.aux_model
        {
            changed.insert(
                "model_aux".to_string(),
                json!({"old": config.aux_model.clone(), "new": aux_model}),
            );
            config.aux_model = aux_model.to_string();
        }
        // Deep model accepts an explicit empty string as "clear it", so unlike
        // model/aux we don't filter empties out — only skip a no-op change.
        if let Some(deep_model) = deep_model.map(str::trim)
            && deep_model != config.deep_model
        {
            changed.insert(
                "model_deep".to_string(),
                json!({"old": config.deep_model.clone(), "new": deep_model}),
            );
            config.deep_model = deep_model.to_string();
        }

        if !changed.is_empty() {
            *router = LlmRouter::new(config);
        }
        Ok(serde_json::Value::Object(changed))
    }

    /// Assemble the LLM messages for a single turn without calling the model.
    pub async fn prepare_turn(&self, req: &TurnRequest) -> AgentResult<AgentTurn> {
        Ok(self.prepare_turn_with_checkpoint(req).await?.turn)
    }

    async fn prepare_turn_with_checkpoint(
        &self,
        req: &TurnRequest,
    ) -> AgentResult<PreparedAgentTurn> {
        // Sync point: the prompt reads the conversation_summary block, so let
        // the previous turn's in-flight summary update land first (bounded).
        await_pending_task(&self.pending_summary, SUMMARY_SYNC_TIMEOUT).await;
        let request_metadata = MessageMetadata::from_value(req.metadata.as_ref());
        let reaction_turn = request_metadata.kind == Some(MessageKind::TelegramReaction);
        let hosted_context = if reaction_turn {
            String::new()
        } else if let Some(client) = self.hosted_plugins.as_ref() {
            if let Err(error) = client.refresh_catalog().await {
                tracing::warn!(error = %error, "hosted plugin catalog refresh failed during prompt assembly");
            }
            match client.context_blocks().await {
                Ok(context) => context,
                Err(error) => {
                    // Plugin context is volatile convenience, never a reason to
                    // fail the user's chat turn.
                    tracing::warn!(error = %error, "hosted plugin context unavailable");
                    String::new()
                }
            }
        } else {
            String::new()
        };
        let mut request_options = req.options.clone();
        let last_prompt_tokens = match self.last_prompt_tokens.load(Ordering::Relaxed) {
            0 => None,
            value => Some(value),
        };
        request_options.compaction_budget =
            CompactionBudget::from_settings(&self.settings, last_prompt_tokens);
        let req = TurnRequest {
            options: request_options,
            ..req.clone()
        };
        let req = &req;
        let mut prepared = prepare_turn_with_checkpoint(
            &self.settings,
            self.memory.as_ref(),
            &self.prompts,
            &req.message,
            req.attachments.clone(),
            req.metadata.as_ref(),
            &req.options,
        )?;
        let turn = &mut prepared.turn;
        let actor_context = if reaction_turn {
            None
        } else {
            self.actor_context_for_prompt_async().await?
        };
        // Actor context and the requestable directory are per-turn volatile —
        // they belong on the volatile system message so they don't invalidate
        // the stable cache prefix.
        if let Some(context) = actor_context
            && let Some(system) = volatile_system_message_mut(&mut turn.messages)
        {
            system.content.push_str("\n\n<actor_context>\n");
            system.content.push_str(&context);
            system.content.push_str("\n</actor_context>");
        }
        if !hosted_context.trim().is_empty()
            && let Some(system) = volatile_system_message_mut(&mut turn.messages)
        {
            system.content.push_str("\n\n");
            system.content.push_str(hosted_context.trim());
        }
        let directory = if reaction_turn {
            String::new()
        } else {
            self.requestable_tools_directory_async(req).await?
        };
        if !directory.is_empty()
            && let Some(system) = volatile_system_message_mut(&mut turn.messages)
        {
            system.content.push_str("\n\n");
            system.content.push_str(&directory);
        }
        Ok(prepared)
    }

    async fn requestable_tools_directory_async(&self, req: &TurnRequest) -> AgentResult<String> {
        if let (Some(registry), Some(actor_id)) = (&self.actor_registry, &self.principal_actor_id) {
            let mut directory = registry
                .build_requestable_directory(actor_id, req.runtime.agent_id_state_dir.is_some())
                .await
                .map_err(|error| {
                    AgentError::Llm(anyhow!("requestable directory failed: {error}"))
                })?;
            if let Some(client) = self.hosted_plugins.as_ref() {
                directory = client.filter_requestable_directory(&directory);
                let hosted = client.requestable_directory();
                if !hosted.is_empty() {
                    if !directory.is_empty() {
                        directory.push_str("\n\n");
                    }
                    directory.push_str(
                        "<available_on_request source=\"hosted_plugins\">\nTools below are NOT loaded. Call request_tool(name=...) to enable one for this turn.\n",
                    );
                    directory.push_str(&hosted);
                    directory.push_str("\n</available_on_request>");
                }
            }
            return Ok(directory);
        }
        let runtime = self.with_actor_runtime(req.runtime.clone());
        let body = requestable_tools_directory_for(&runtime);
        if body.is_empty() {
            return Ok(String::new());
        }
        Ok(format!(
            "<available_on_request>\nTools below are NOT loaded. Call request_tool(name=...) to enable one for this turn.\n{body}\n</available_on_request>"
        ))
    }

    /// Backward-compatible text-only turn API. Cut-short checkpoints fail
    /// closed as either the fixed safe notice for an ordinary undelivered user
    /// turn or an empty response for internal/reaction/already-delivered turns.
    /// Transports that need the typed status must use [`Agent::chat_once_result`].
    pub async fn chat_once(&self, req: TurnRequest) -> AgentResult<String> {
        let metadata = MessageMetadata::from_value(req.metadata.as_ref());
        let telegram_guard = req
            .runtime
            .telegram
            .as_ref()
            .and_then(|telegram| telegram.guard.clone());
        match self.chat_once_result(req).await? {
            TurnResult::Complete(_) if metadata.kind == Some(MessageKind::TelegramReaction) => {
                Ok(String::new())
            }
            TurnResult::Complete(response) => Ok(response),
            TurnResult::Checkpointed
                if !metadata.is_internal()
                    && metadata.kind != Some(MessageKind::TelegramReaction)
                    && final_response_should_be_persisted(telegram_guard.as_ref())? =>
            {
                Ok(TURN_CHECKPOINT_NOTICE.to_string())
            }
            TurnResult::Checkpointed => Ok(String::new()),
        }
    }

    /// Run one full turn: prepare messages, call the model with tool support,
    /// persist user/assistant history, and return a typed completion status.
    pub async fn chat_once_result(&self, req: TurnRequest) -> AgentResult<TurnResult> {
        let request_metadata = MessageMetadata::from_value(req.metadata.as_ref());
        // Reactions to Lethe's own Telegram messages are durable social input,
        // not a new task. Persist only their sanitized public event and do not
        // invoke the model/tools at all: a silent reaction cannot stream,
        // persist, or deliver unrelated resumable work by accident.
        if request_metadata.kind == Some(MessageKind::TelegramReaction) {
            if !request_metadata.is_internal() {
                let defensive_history =
                    user_visible_history_content(&req.message, req.metadata.as_ref());
                self.memory.messages.add(
                    MessageRole::User,
                    &defensive_history,
                    metadata_for_persistence(req.metadata.clone()),
                )?;
            }
            return Ok(TurnResult::Complete(String::new()));
        }
        let PreparedAgentTurn {
            turn,
            resumable_checkpoint_id,
        } = self.prepare_turn_with_checkpoint(&req).await?;
        let TurnRequest {
            message,
            metadata,
            runtime,
            ..
        } = req;
        if !turn.synthetic {
            let defensive_history = user_visible_history_content(&message, metadata.as_ref());
            self.memory.messages.add(
                MessageRole::User,
                &defensive_history,
                metadata_for_persistence(metadata),
            )?;
        }
        let runtime = self.with_actor_runtime(runtime);
        let telegram_guard = runtime
            .telegram
            .as_ref()
            .and_then(|telegram| telegram.guard.clone());
        let dropped_for_summary = turn.dropped_for_summary.clone();
        let ordinary_turn =
            !turn.synthetic && request_metadata.kind != Some(MessageKind::TelegramReaction);
        let output = self
            .complete_turn_with_tools(turn.messages, runtime, !turn.synthetic)
            .await?;

        let result = if let Some(stop_reason) = output.stop_reason.as_deref() {
            tracing::warn!(reason = %stop_reason, "turn cut short — preserving an internal resumable checkpoint");
            if !turn.synthetic && request_metadata.kind != Some(MessageKind::TelegramReaction) {
                // The raw resumable state is the durability boundary. Store it
                // before reading any transport guard so a poisoned guard can
                // suppress a notice but can never lose the checkpoint itself.
                persist_resumable_checkpoint(self.memory.as_ref(), &output.text, stop_reason)?;
                let should_show_notice =
                    final_response_should_be_persisted(telegram_guard.as_ref())?;
                if should_show_notice {
                    persist_checkpoint_notice(self.memory.as_ref())?;
                }
            }
            TurnResult::Checkpointed
        } else {
            let response = output.text;
            if ordinary_turn {
                persist_completed_ordinary_turn(
                    self.memory.as_ref(),
                    &response,
                    telegram_guard.as_ref(),
                    resumable_checkpoint_id.as_deref(),
                )?;
            }
            TurnResult::Complete(response)
        };
        // Post-turn memory maintenance — rolling the dropped batch into the
        // persistent conversation_summary, plus the cadence-gated curator pass —
        // is background work that makes aux-model LLM calls. It must NEVER block
        // the user-facing reply / `done` (otherwise the client sits on a typing
        // indicator after the answer is already complete). Errors are logged,
        // never propagated. The summary task's handle is kept so the NEXT turn
        // can wait for it before reading the summary block (see prepare_turn);
        // the curator stays fully detached — nothing downstream reads its output
        // synchronously.
        let needs_summary = ordinary_turn && !dropped_for_summary.is_empty();
        let curator_enabled = self.settings.background.curator_enabled;
        if needs_summary {
            let memory = self.memory.clone();
            let router = self.router.clone();
            let prompts = self.prompts.clone();
            let handle = tokio::spawn(async move {
                if let Err(error) = summarizer::update_conversation_summary(
                    memory.as_ref(),
                    &prompts,
                    router,
                    &dropped_for_summary,
                )
                .await
                {
                    tracing::warn!(error = %error, "conversation summary update failed");
                }
            });
            *self.pending_summary.lock().await = Some(handle);
        }
        if ordinary_turn && curator_enabled {
            let memory = self.memory.clone();
            let router = self.router.clone();
            let memory_dir = self.settings.paths.memory_dir.clone();
            tokio::spawn(async move {
                let router_snapshot = match router.read() {
                    Ok(guard) => guard.clone(),
                    Err(error) => {
                        tracing::warn!(error = %error, "router lock poisoned in curator pass");
                        return;
                    }
                };
                let curator = MemoryCurator::new(memory_dir.join("curator_state.json"));
                if let Err(error) = curator
                    .run_pass(memory.as_ref(), &router_snapshot, false)
                    .await
                {
                    tracing::warn!(error = %error, "curator pass failed");
                }
            });
        }
        Ok(result)
    }

    pub fn actor_registry(&self) -> Option<SharedActorRegistry> {
        self.actor_registry.clone()
    }

    pub fn principal_actor_id(&self) -> Option<&str> {
        self.principal_actor_id.as_deref()
    }

    /// Install a factory that observes subagent turns. The actor turn
    /// executor calls it with the acting actor's id at the start of each
    /// subagent turn and threads the returned observer through the tool
    /// loop, so hosts can surface per-subagent tool/reasoning activity
    /// (attributed by actor id) on their own event streams. Applies to all
    /// turns started after installation; without it subagent turns run
    /// unobserved.
    pub fn install_subagent_observer(
        &self,
        factory: crate::tools::registry::SubagentObserverFactory,
    ) {
        if let Ok(mut slot) = self.subagent_observer.write() {
            *slot = Some(factory);
        }
    }

    pub async fn run_curator_pass(&self, force: bool) -> AgentResult<CuratorRunStats> {
        let curator = MemoryCurator::new(self.settings.paths.memory_dir.join("curator_state.json"));
        let router = self
            .router
            .read()
            .map_err(|error| AgentError::Llm(anyhow!("router lock poisoned: {error}")))?
            .clone();
        Ok(curator
            .run_pass(self.memory.as_ref(), &router, force)
            .await?)
    }

    /// Everything the system is on the hook for right now: unfinished
    /// subagents (including Blocked ones that will never autocontinue) and
    /// in-progress/overdue todos. Feeds the heartbeat so background ticks are
    /// need-driven — a tick with open work is never silently skipped.
    /// Best-effort: failures degrade to an empty digest, never an error.
    pub async fn open_work_digest(&self) -> String {
        let mut lines = Vec::new();
        if let Some(registry) = &self.actor_registry {
            match registry.open_work_lines().await {
                Ok(actor_lines) => lines.extend(actor_lines),
                Err(error) => {
                    tracing::warn!(error = %error, "actor open-work query failed")
                }
            }
        }
        if self.settings.hosted_plugins.replace_local_todos {
            if let Some(client) = self.hosted_plugins.as_ref() {
                match client.context_blocks().await {
                    Ok(context) if !context.trim().is_empty() => lines.push(context),
                    Ok(_) => {}
                    Err(error) => {
                        tracing::warn!(error = %error, "hosted open-work context failed")
                    }
                }
            }
        } else {
            match self.memory.todos.open_work_digest(20) {
                Ok(digest) if !digest.trim().is_empty() => lines.push(digest),
                Ok(_) => {}
                Err(error) => tracing::warn!(error = %error, "todo open-work digest failed"),
            }
        }
        lines.join("\n")
    }

    pub async fn process_background_heartbeat(
        &self,
        heartbeat_message: &str,
        reminders: &str,
    ) -> AgentResult<BackgroundResult> {
        let mut result = self
            .queue_background_heartbeat(heartbeat_message, reminders)
            .await?;
        if let Some(registry) = self.actor_registry.clone() {
            let candidates = {
                let events = registry
                    .user_notification_events(50)
                    .await
                    .map_err(|error| {
                        AgentError::Llm(anyhow!("notification query failed: {error}"))
                    })?;
                let mut gate = self.notification_gate.lock().map_err(|error| {
                    AgentError::Llm(anyhow!("notification gate lock poisoned: {error}"))
                })?;
                let mut processed = self.processed_notification_events.lock().map_err(|error| {
                    AgentError::Llm(anyhow!("notification event lock poisoned: {error}"))
                })?;
                collect_user_notifications_from_events(events, &mut gate, &mut processed)
            };
            // Run the aux-LLM content gate. This catches leaks the heuristic
            // gate misses — internal reflection, meta commentary about DMN/
            // cortex, redundant pings — and rewrites borderline content. On
            // failure the gate drops the candidate (fail-closed).
            result.notifications = if candidates.is_empty() {
                Vec::new()
            } else {
                let recent_context = self.recent_user_context_for_review(5);
                let router = self
                    .router
                    .read()
                    .map_err(|error| AgentError::Llm(anyhow!("router lock poisoned: {error}")))?
                    .clone();
                crate::actor::background::review_notifications_with_llm(
                    candidates,
                    &recent_context,
                    &self.prompts,
                    &router,
                )
                .await
            };
        }
        Ok(result)
    }

    /// Compact recent user-side context for the notification review gate so
    /// it can spot redundant pings (e.g. "I already asked them this 10 min
    /// ago and they haven't replied"). Cheap — just last N user messages.
    fn recent_user_context_for_review(&self, count: usize) -> String {
        let Ok(recent) = self.memory.messages.get_recent(count.saturating_mul(4)) else {
            return String::new();
        };
        recent_user_context_from_messages(&recent, count)
    }

    pub async fn process_background_heartbeat_quiet(
        &self,
        heartbeat_message: &str,
        reminders: &str,
    ) -> AgentResult<BackgroundResult> {
        self.queue_background_heartbeat(heartbeat_message, reminders)
            .await
    }

    async fn queue_background_heartbeat(
        &self,
        heartbeat_message: &str,
        reminders: &str,
    ) -> AgentResult<BackgroundResult> {
        let mut result = BackgroundResult::default();
        if let (Some(registry), Some(principal_id)) =
            (self.actor_registry.clone(), self.principal_actor_id.clone())
        {
            let dmn_actor_id =
                queue_dmn_heartbeat(&registry, &principal_id, heartbeat_message, reminders)
                    .await
                    .map_err(|error| AgentError::Llm(anyhow!("dmn queue failed: {error}")))?;
            result.dmn_actor_id = Some(dmn_actor_id);
        }

        if self.settings.background.curator_enabled {
            result.curator = Some(self.run_curator_pass(false).await?);
        }
        Ok(result)
    }

    fn with_actor_runtime(&self, mut runtime: ToolRuntime) -> ToolRuntime {
        // The constructor policy is the outer trust boundary. A hosted caller
        // forgetting to repeat `HostedSafe` on an individual TurnRequest must
        // fail closed, while a Full agent still honors an explicitly narrower
        // per-turn policy.
        if self.tool_policy == ToolPolicy::HostedSafe {
            runtime.policy = ToolPolicy::HostedSafe;
        }
        if runtime.actor.is_none()
            && let (Some(registry), Some(actor_id)) =
                (self.actor_registry.clone(), self.principal_actor_id.clone())
        {
            runtime.actor = Some(ActorToolContext {
                runtime: registry,
                actor_id,
                is_subagent: false,
            });
        }
        if runtime.hosted_plugins.is_none() {
            runtime.hosted_plugins = self.hosted_plugins.clone();
        }
        runtime
    }

    async fn actor_context_for_prompt_async(&self) -> AgentResult<Option<String>> {
        let (Some(registry), Some(actor_id)) = (&self.actor_registry, &self.principal_actor_id)
        else {
            return Ok(None);
        };
        let context = registry
            .build_system_prompt(actor_id)
            .await
            .map_err(|error| AgentError::Llm(anyhow!("actor context failed: {error}")))?;
        Ok(Some(context))
    }

    async fn complete_turn_with_tools(
        &self,
        messages: Vec<LlmMessage>,
        runtime: ToolRuntime,
        record_tool_messages: bool,
    ) -> AgentResult<TurnOutput> {
        self.complete_turn_with_tools_config(messages, runtime, false, record_tool_messages)
            .await
    }

    async fn complete_turn_with_tools_config(
        &self,
        messages: Vec<LlmMessage>,
        runtime: ToolRuntime,
        use_aux: bool,
        record_tool_messages: bool,
    ) -> AgentResult<TurnOutput> {
        complete_turn_with_tools_config_shared(
            TurnExecutionContext {
                settings: self.settings.clone(),
                memory: self.memory.clone(),
                router: self.router.clone(),
                shell: self.shell.clone(),
                last_prompt_tokens: self.last_prompt_tokens.clone(),
                hosted_plugins: self.hosted_plugins.clone(),
            },
            messages,
            runtime,
            use_aux,
            record_tool_messages,
        )
        .await
    }
}

fn recent_user_context_from_messages(recent: &[StoredMessage], count: usize) -> String {
    let mut lines = Vec::new();
    let visible = history_records_for_turn(recent.to_vec(), false);
    for message in visible.iter().rev() {
        if !message.role.is_user() && !message.role.is_assistant() {
            continue;
        }
        if MessageMetadata::from_value(Some(&message.metadata)).has_tool_calls() {
            continue;
        }
        let role = if message.role.is_user() {
            "user"
        } else {
            "assistant"
        };
        let content = message.content.trim();
        if content.is_empty() {
            continue;
        }
        let snippet = if content.chars().count() > 400 {
            content.chars().take(400).collect::<String>() + "…"
        } else {
            content.to_string()
        };
        lines.push(format!("[{role}] {snippet}"));
        if lines.len() >= count {
            break;
        }
    }
    lines.reverse();
    lines.join("\n")
}

pub fn prepare_turn(
    settings: &Settings,
    memory: &MemoryStore,
    prompts: &PromptStore,
    message: &str,
    attachments: Vec<LlmAttachment>,
    metadata: Option<&Value>,
    options: &AgentOptions,
) -> AgentResult<AgentTurn> {
    Ok(prepare_turn_with_checkpoint(
        settings,
        memory,
        prompts,
        message,
        attachments,
        metadata,
        options,
    )?
    .turn)
}

fn prepare_turn_with_checkpoint(
    settings: &Settings,
    memory: &MemoryStore,
    prompts: &PromptStore,
    message: &str,
    attachments: Vec<LlmAttachment>,
    metadata: Option<&Value>,
    options: &AgentOptions,
) -> AgentResult<PreparedAgentTurn> {
    let request_metadata = MessageMetadata::from_value(metadata);
    let synthetic = request_metadata.is_internal();
    let reaction_turn = request_metadata.kind == Some(MessageKind::TelegramReaction);
    // A reaction to Lethe's own message is social feedback, not permission to
    // resume an unfinished tool task. Internal scheduler/heartbeat turns also
    // must not inherit hidden checkpoints, while otherwise retaining their
    // established context behavior.
    let include_checkpoints = !synthetic && !reaction_turn;
    let raw_recent = memory.messages.get_recent(HISTORY_FETCH_LIMIT)?;
    let recent = if reaction_turn {
        Vec::new()
    } else {
        history_records_for_turn(raw_recent, include_checkpoints)
    };
    let resumable_checkpoint_id = recent
        .iter()
        .rev()
        .find(|message| is_resumable_checkpoint_record(message))
        .map(|message| message.id.clone());
    let recall = if options.use_hippocampus && !synthetic && !reaction_turn {
        Hippocampus::new(HippocampusConfig {
            enabled: settings.background.hippocampus_enabled,
            ..Default::default()
        })
        .recall(memory, message, &recent)?
    } else {
        None
    };

    let parts = build_system_prompt(
        memory,
        prompts,
        recall.as_deref(),
        !reaction_turn && !settings.hosted_plugins.replace_local_todos,
        !reaction_turn,
    )?;
    let dialect = dialect_for_model(&settings.llm.llm_model);
    let mut messages = parts.into_messages();
    apply_cache_markers(&mut messages, dialect.as_ref());
    let mut history_messages = history_to_llm_messages(recent);
    // Pull typed resumable state out of the ordinary compaction pool, then
    // place it immediately before the current request. Both the hard prompt
    // clamp and the tool-loop clamp recognize only this provenance-derived
    // wrapper and will fail with an explicit overflow rather than silently
    // deleting the state we told the user was saved.
    let mut checkpoint_messages = Vec::new();
    history_messages.retain(|message| {
        if is_resumable_checkpoint_message(message) {
            checkpoint_messages.push(message.clone());
            false
        } else {
            true
        }
    });
    let dropped_for_summary = compact_history(&mut history_messages, options.compaction_budget);
    messages.extend(history_messages);
    messages.extend(checkpoint_messages);
    let stamped = stamp_user_message(message);
    let user_message = if attachments.is_empty() {
        LlmMessage::user(stamped)
    } else {
        LlmMessage::user_with_attachments(stamped, attachments)
    };
    messages.push(user_message);

    // Hard backstop behind the soft compaction budget: guarantee the assembled
    // prompt fits the model's context window minus the output reservation, so a
    // bad overhead estimate can't push us past the provider's limit (and onto a
    // slow fallback). The tools array isn't in `messages`, so this leaves the
    // remaining headroom for it implicitly.
    let context_tokens = settings.llm.context_limit_for(&settings.llm.llm_model);
    let max_total_chars = context_tokens
        .saturating_sub(settings.llm.llm_max_output as u64)
        .saturating_mul(CHARS_PER_TOKEN as u64) as usize;
    clamp_messages_to_budget(&mut messages, max_total_chars);
    // Final guard, independent of the budget paths above: the first non-system
    // message must never be an orphaned `tool_result`. A turn interrupted
    // mid-tool-call (e.g. the process was killed before the result was paired)
    // can persist a half-pair; without this one such record wedges every later
    // turn with an Anthropic 400 ("tool_result without tool_use").
    drop_leading_orphan_tool_results(&mut messages);

    Ok(PreparedAgentTurn {
        turn: AgentTurn {
            messages,
            recall,
            synthetic,
            dropped_for_summary,
        },
        resumable_checkpoint_id,
    })
}

/// System prompt split into a long-stable head (identity, persona,
/// instructions, stable memory blocks) and a per-turn-volatile tail (volatile
/// blocks, clock, recall, tool history). Letting them be separate system
/// messages lets Anthropic's prompt cache land a breakpoint between them.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SystemParts {
    pub stable: String,
    pub volatile: String,
}

impl SystemParts {
    pub fn into_messages(self) -> Vec<LlmMessage> {
        let mut out = Vec::new();
        if !self.stable.trim().is_empty() {
            out.push(LlmMessage::system(self.stable));
        }
        if !self.volatile.trim().is_empty() {
            out.push(LlmMessage::system(self.volatile));
        }
        out
    }

    /// Flatten back to a single string. Used by tests that still assert on the
    /// monolithic prompt shape.
    pub fn render_joined(&self) -> String {
        match (
            self.stable.trim().is_empty(),
            self.volatile.trim().is_empty(),
        ) {
            (true, true) => String::new(),
            (false, true) => self.stable.clone(),
            (true, false) => self.volatile.clone(),
            (false, false) => format!("{}\n\n{}", self.stable, self.volatile),
        }
    }
}

fn build_system_prompt(
    memory: &MemoryStore,
    prompts: &PromptStore,
    recall: Option<&str>,
    include_local_active_tasks: bool,
    include_continuation_context: bool,
) -> AgentResult<SystemParts> {
    let identity = memory
        .blocks
        .get("identity")
        .map_err(MemoryStoreError::Blocks)?
        .map(|block| block.value)
        .unwrap_or_default();
    let instructions = prompts.load("agent_instructions", "You are Lethe.").text;
    let (memory_stable, memory_volatile) = memory.get_context_split()?;
    let summary = if include_continuation_context {
        memory.conversation_summary()?
    } else {
        String::new()
    };
    let clock_block = format_clock_block();

    let mut stable_builder = PromptBuilder::new();
    stable_builder
        .block("identity_block", identity)
        .raw(instructions)
        .raw(memory_stable);

    let mut volatile_builder = PromptBuilder::new();
    if !summary.trim().is_empty() {
        volatile_builder.block("conversation_summary", summary);
    }
    // Standing work commitments — in-progress and overdue todos — are injected
    // every turn so unfinished work survives context compaction and session
    // restarts without the model having to remember to call todo_list. Text
    // comes from the overridable `active_tasks` template.
    if include_continuation_context && include_local_active_tasks {
        match memory.todos.open_work_digest(ACTIVE_TASKS_PROMPT_LIMIT) {
            Ok(digest) if !digest.trim().is_empty() => {
                let mut variables = std::collections::HashMap::new();
                variables.insert("digest".to_string(), digest);
                let body = prompts
                    .render("active_tasks", &variables, "Your open work:\n{digest}")
                    .text;
                volatile_builder.block("active_tasks", body);
            }
            Ok(_) => {}
            Err(error) => tracing::warn!(error = %error, "active-tasks digest failed"),
        }
    }
    if include_continuation_context {
        volatile_builder.raw(memory_volatile);
    }
    volatile_builder.raw(clock_block);

    if let Some(recall) = recall.filter(|value| !value.trim().is_empty()) {
        let timestamp = Local::now().format("%a %Y-%m-%d %H:%M:%S %Z").to_string();
        let body = format!(
            "<recall_block source=\"hippocampus\">\n{}\n</recall_block>",
            recall.trim()
        );
        volatile_builder.block_with(
            "runtime_context",
            [("source", "hippocampus"), ("timestamp", timestamp.as_str())],
            body,
        );
    }

    Ok(SystemParts {
        stable: stable_builder.render(),
        volatile: volatile_builder.render(),
    })
}

/// How many recent messages to pull from storage per turn. Python `main`
/// doesn't enforce a count cap at all — it relies on token-budget
/// compaction. We still cap the DB read so we don't load thousands of
/// rows on long-lived sessions; `compact_history` then trims to the
/// active context budget.
const HISTORY_FETCH_LIMIT: usize = 500;

/// Cap on the `<active_tasks>` lines injected into every system prompt. Keeps
/// the standing work block cheap even with a crowded todo list.
const ACTIVE_TASKS_PROMPT_LIMIT: usize = 10;

fn history_records_for_turn(
    recent: Vec<StoredMessage>,
    include_checkpoints: bool,
) -> Vec<StoredMessage> {
    let mut history: Vec<StoredMessage> = Vec::new();
    let mut inside_internal_turn = false;
    let mut active_checkpoint_id: Option<String> = None;
    for message in recent {
        let metadata = MessageMetadata::from_value(Some(&message.metadata));
        let internal = metadata.is_internal();
        if message.role.is_user() {
            inside_internal_turn = internal;
            if inside_internal_turn {
                continue;
            }
        } else if inside_internal_turn {
            continue;
        }

        if raw_message_kind(Some(&message.metadata)) == Some(RawMessageKind::CheckpointResolved) {
            let resolved_id = message
                .metadata
                .get(CHECKPOINT_ID_KEY)
                .and_then(Value::as_str);
            if active_checkpoint_id.as_deref() == resolved_id {
                history.retain(|stored| {
                    raw_message_kind(Some(&stored.metadata)) != Some(RawMessageKind::Checkpoint)
                });
                active_checkpoint_id = None;
            }
            continue;
        }

        let resumable_checkpoint = include_checkpoints && is_resumable_checkpoint_record(&message);
        if internal && !resumable_checkpoint {
            continue;
        }

        if is_turn_history_record(&message, include_checkpoints) {
            if resumable_checkpoint {
                history.retain(|stored| {
                    raw_message_kind(Some(&stored.metadata)) != Some(RawMessageKind::Checkpoint)
                });
                active_checkpoint_id = Some(message.id.clone());
            }
            history.push(message);
        }
    }

    drop_history_before_first_user(&mut history);
    history
}

fn is_resumable_checkpoint_record(message: &StoredMessage) -> bool {
    message.role.is_assistant()
        && raw_message_kind(Some(&message.metadata)) == Some(RawMessageKind::Checkpoint)
}

fn is_turn_history_record(message: &StoredMessage, include_checkpoints: bool) -> bool {
    let metadata = MessageMetadata::from_value(Some(&message.metadata));
    // The companion notice exists for transcript parity only. Keeping it out
    // of model history makes the checkpoint + current user message the atomic
    // minimum retained pair under tight compaction budgets.
    if raw_message_kind(Some(&message.metadata)) == Some(RawMessageKind::CheckpointNotice) {
        return false;
    }
    let resumable_checkpoint = include_checkpoints && is_resumable_checkpoint_record(message);
    if metadata.is_internal() && !resumable_checkpoint {
        return false;
    }
    match message.role {
        MessageRole::User | MessageRole::Assistant | MessageRole::Tool => {
            // Tool results legitimately carry a tool_call_id in metadata
            // instead of inline text; assistant messages with tool_calls may
            // also have empty content. Both stay; the pairing pass filters
            // orphans later.
            !message.content.trim().is_empty()
                || MessageMetadata::from_value(Some(&message.metadata)).has_tool_calls()
                || message.metadata.get("tool_call_id").is_some()
        }
        _ => false,
    }
}

fn drop_history_before_first_user(history: &mut Vec<StoredMessage>) {
    let Some(first_user) = history.iter().position(|message| message.role.is_user()) else {
        // A proactive message can legitimately begin the conversation. Keep
        // only those outbound rows; the conversion step renders them as one
        // self-describing user-role context record so Anthropic's user-first
        // protocol does not discard them.
        history.retain(is_user_visible_proactive_assistant);
        return;
    };
    if first_user > 0 {
        let proactive_prefix = history
            .drain(0..first_user)
            .filter(is_user_visible_proactive_assistant)
            .collect::<Vec<_>>();
        if !proactive_prefix.is_empty() {
            history.splice(0..0, proactive_prefix);
        }
    }
}

fn is_user_visible_proactive_assistant(message: &StoredMessage) -> bool {
    if !message.role.is_assistant() {
        return false;
    }
    let metadata = MessageMetadata::from_value(Some(&message.metadata));
    !metadata.is_internal() && metadata.kind == Some(MessageKind::Proactive)
}

/// Convert a slice of stored messages into the LLM message stream, preserving
/// assistant_tool_calls ↔ tool_response pairing so the wire format stays
/// valid (Anthropic enforces this; OpenAI is more lenient but still expects
/// matching ids). Orphans on either side are dropped.
fn history_to_llm_messages(history: Vec<StoredMessage>) -> Vec<LlmMessage> {
    let mut out = Vec::new();
    let leading_proactive = history
        .iter()
        .take_while(|message| is_user_visible_proactive_assistant(message))
        .count();
    if leading_proactive > 0 {
        let messages = history[..leading_proactive]
            .iter()
            .map(|message| {
                format!(
                    "<proactive_assistant_message>\n{}\n</proactive_assistant_message>",
                    message.content
                )
            })
            .collect::<Vec<_>>()
            .join("\n");
        // Anthropic requires the message list to start with `user`. Encode the
        // outbound text inside one self-describing context record: compaction
        // can drop that record as a unit but cannot orphan a leading assistant.
        out.push(LlmMessage::user(format!(
            "[Conversation context: before the user's first inbound turn, you initiated the conversation with the following message(s).]\n{messages}"
        )));
    }
    let mut iter = history.into_iter().skip(leading_proactive).peekable();

    while let Some(message) = iter.next() {
        match message.role {
            MessageRole::User if !message.content.trim().is_empty() => {
                out.push(LlmMessage::user(history_content_with_timestamp(&message)));
            }
            MessageRole::Assistant => {
                let metadata = MessageMetadata::from_value(Some(&message.metadata));
                let calls = extract_historical_tool_calls(&message.metadata);
                let intended_tool_calls = metadata.has_tool_calls();
                if calls.is_empty() {
                    // The model was reported to have made tool calls but the
                    // payload is missing call_ids — we can't reconstruct a
                    // valid pair, so drop the chatter entirely instead of
                    // surfacing it as plain narration.
                    if intended_tool_calls {
                        continue;
                    }
                    if !message.content.trim().is_empty() {
                        let content = if raw_message_kind(Some(&message.metadata))
                            == Some(RawMessageKind::Checkpoint)
                        {
                            format!(
                                "{RESUMABLE_CHECKPOINT_PREFIX}{}{RESUMABLE_CHECKPOINT_SUFFIX}",
                                message.content
                            )
                        } else {
                            message.content
                        };
                        out.push(LlmMessage::assistant(content));
                    }
                    continue;
                }

                // Collect the tool results that should follow this assistant
                // message. Anthropic requires every tool_use_id to have a
                // matching tool_result in the very next user message; we
                // greedily consume Tool-role messages while they match a
                // pending call_id.
                let expected: std::collections::HashSet<String> =
                    calls.iter().map(|call| call.call_id.clone()).collect();
                let mut responses: Vec<HistoricalToolResponse> = Vec::new();
                let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
                while let Some(next) = iter.peek() {
                    if !matches!(next.role, MessageRole::Tool) {
                        break;
                    }
                    let Some(call_id) = next.metadata.get("tool_call_id").and_then(Value::as_str)
                    else {
                        // Tool message without a tool_call_id — orphan, skip.
                        iter.next();
                        continue;
                    };
                    if !expected.contains(call_id) {
                        // Belongs to a different call group; stop consuming.
                        break;
                    }
                    let call_id = call_id.to_string();
                    let tool_msg = iter.next().expect("peeked tool message");
                    if seen.insert(call_id.clone()) {
                        responses.push(HistoricalToolResponse {
                            call_id,
                            content: tool_msg.content,
                            source_message_id: Some(tool_msg.id),
                        });
                    }
                }

                // Drop the whole pair if any tool_use_id is missing its
                // response — Anthropic 400s on a mismatched id list.
                if seen.len() != expected.len() {
                    continue;
                }
                out.push(LlmMessage::assistant_with_tool_calls(
                    message.content,
                    calls,
                ));
                out.push(LlmMessage::tool_results(responses));
            }
            // Orphaned tool result (no preceding assistant tool_call). Skip.
            MessageRole::Tool => continue,
            _ => continue,
        }
    }

    out
}

fn is_resumable_checkpoint_message(message: &LlmMessage) -> bool {
    message.role == LlmRole::Assistant
        && message.content.starts_with(RESUMABLE_CHECKPOINT_PREFIX)
        && message.content.ends_with(RESUMABLE_CHECKPOINT_SUFFIX)
}

fn extract_historical_tool_calls(metadata: &Value) -> Vec<HistoricalToolCall> {
    metadata
        .get("tool_calls")
        .and_then(Value::as_array)
        .map(|calls| {
            calls
                .iter()
                .filter_map(|call| {
                    let call_id = call.get("call_id").or_else(|| call.get("id"))?.as_str()?;
                    let fn_name = call
                        .get("fn_name")
                        .or_else(|| call.get("function").and_then(|f| f.get("name")))?
                        .as_str()?;
                    let fn_arguments = call
                        .get("fn_arguments")
                        .cloned()
                        .or_else(|| {
                            call.get("function")
                                .and_then(|f| f.get("arguments"))
                                .and_then(|args| args.as_str())
                                .and_then(|raw| serde_json::from_str::<Value>(raw).ok())
                        })
                        .unwrap_or(Value::Object(serde_json::Map::new()));
                    let thought_signatures = call
                        .get("thought_signatures")
                        .and_then(Value::as_array)
                        .map(|values| {
                            values
                                .iter()
                                .filter_map(|value| value.as_str().map(str::to_string))
                                .collect::<Vec<_>>()
                        });
                    Some(HistoricalToolCall {
                        call_id: call_id.to_string(),
                        fn_name: fn_name.to_string(),
                        fn_arguments,
                        thought_signatures,
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

fn assistant_history_content(response: &str) -> String {
    normalize_message_envelope(response).unwrap_or_else(|| response.to_string())
}

fn metadata_for_persistence(metadata: Option<Value>) -> Option<Value> {
    let mut metadata = metadata.unwrap_or_else(|| json!({}));
    if let Value::Object(map) = &mut metadata {
        map.remove(HISTORY_CONTENT_KEY);
        let parsed = MessageMetadata::from_map(map);
        if !parsed.is_internal() && parsed.kind.is_none() {
            annotate_map(
                map,
                MessageVisibility::UserVisible,
                MessageKind::Chat,
                "chat",
            );
        }
    }
    Some(metadata)
}

fn persist_completed_ordinary_turn(
    memory: &MemoryStore,
    response: &str,
    telegram_guard: Option<&crate::interfaces::telegram::SharedTelegramTurnGuard>,
    resumable_checkpoint_id: Option<&str>,
) -> AgentResult<()> {
    // Once the model completed an ordinary resumed turn, the old checkpoint
    // must stop influencing future prompts even if a later transport-guard
    // read or visible-row write fails. The persisted user follow-up is enough
    // for a retry to regenerate the final answer without reviving old state.
    if let Some(checkpoint_id) = resumable_checkpoint_id {
        persist_checkpoint_resolution(memory, checkpoint_id)?;
    }
    if final_response_should_be_persisted(telegram_guard)? {
        let history_content = assistant_history_content(response);
        // Don't persist whitespace-only final replies — unlike the loop's
        // per-iteration rows (whose tool_calls metadata pairs the tool results
        // that follow), an empty row here is pure noise.
        if !history_content.trim().is_empty() {
            memory.messages.add(
                MessageRole::Assistant,
                &history_content,
                Some(metadata_value(
                    MessageVisibility::UserVisible,
                    MessageKind::Chat,
                    "chat",
                )),
            )?;
        }
    }
    Ok(())
}

fn persist_resumable_checkpoint(
    memory: &MemoryStore,
    checkpoint: &str,
    stop_reason: &str,
) -> AgentResult<()> {
    let mut checkpoint_metadata = raw_metadata_value(
        MessageVisibility::Internal,
        RawMessageKind::Checkpoint,
        "tool_loop",
    );
    if let Value::Object(map) = &mut checkpoint_metadata {
        map.insert("stop_reason".to_string(), json!(stop_reason));
    }
    memory.messages.add(
        MessageRole::Assistant,
        &assistant_history_content(checkpoint),
        Some(checkpoint_metadata),
    )?;
    Ok(())
}

fn persist_checkpoint_notice(memory: &MemoryStore) -> AgentResult<()> {
    memory.messages.add(
        MessageRole::Assistant,
        TURN_CHECKPOINT_NOTICE,
        Some(raw_metadata_value(
            MessageVisibility::UserVisible,
            RawMessageKind::CheckpointNotice,
            "tool_loop",
        )),
    )?;
    Ok(())
}

fn persist_checkpoint_resolution(memory: &MemoryStore, checkpoint_id: &str) -> AgentResult<()> {
    let mut metadata = raw_metadata_value(
        MessageVisibility::Internal,
        RawMessageKind::CheckpointResolved,
        "tool_loop",
    );
    if let Value::Object(map) = &mut metadata {
        map.insert(CHECKPOINT_ID_KEY.to_string(), json!(checkpoint_id));
    }
    memory
        .messages
        .add(MessageRole::Assistant, "", Some(metadata))?;
    Ok(())
}

fn final_response_should_be_persisted(
    telegram_guard: Option<&crate::interfaces::telegram::SharedTelegramTurnGuard>,
) -> AgentResult<bool> {
    let Some(guard) = telegram_guard else {
        return Ok(true);
    };
    let guard = guard
        .lock()
        .map_err(|error| AgentError::Llm(anyhow!("telegram turn guard poisoned: {error}")))?;
    Ok(guard.visible_messages_sent() == 0)
}

/// Return the last system message (the volatile half of the split prompt) so
/// per-turn additions (actor_context, directory) don't bust the stable cache.
fn volatile_system_message_mut(messages: &mut [LlmMessage]) -> Option<&mut LlmMessage> {
    messages
        .iter_mut()
        .rev()
        .find(|message| message.role == LlmRole::System)
}

/// Coarse time-of-day label for behavioural nudges. Keeps the buckets small
/// so the model has a clear signal without having to do clock math.
fn time_of_day_label(hour: u32) -> &'static str {
    match hour {
        5..=6 => "early_morning",
        7..=11 => "morning",
        12..=16 => "afternoon",
        17..=20 => "evening",
        21..=23 => "night",
        _ => "late_night",
    }
}

/// `<runtime_context source="clock">` block surfacing the current time. Lives
/// at top level of the volatile system prompt so a model scanning for "when
/// is now?" finds it at a stable location instead of buried in memory state.
fn format_clock_block() -> String {
    let now = chrono::Utc::now();
    let local = now.with_timezone(&Local);
    let weekday = local.format("%A").to_string();
    let hour = local.format("%H").to_string().parse::<u32>().unwrap_or(0);
    format!(
        "<runtime_context source=\"clock\">\n- now={}\n- weekday={}\n- time_of_day={}\n</runtime_context>",
        local.format("%a %Y-%m-%d %H:%M:%S %Z"),
        weekday,
        time_of_day_label(hour),
    )
}

/// Floor for the per-tool-result inline cap when we can't derive a budget
/// from the model's context window (e.g. tests, fallback paths).
const MIN_TOOL_RESULT_INLINE_CHARS: usize = 4_000;
/// Cap on a single tool result that stays inline, as a fraction of total
/// history budget. Larger results get replaced with a compact reference
/// pointing at the persistent message id (`conversation_get(message_id=...)`).
const TOOL_RESULT_INLINE_BUDGET_DIVISOR: usize = 15;
/// Number of trailing tool-call groups whose results we never archive. The
/// most recent two turns of tool work are the freshest reasoning context.
const RECENT_TOOL_CALL_GROUPS_TO_PRESERVE: usize = 2;

/// Rough conversion factor — Anthropic/OpenAI English+JSON averages ~4
/// chars per token. Use as a budget heuristic, not for exact accounting.
const CHARS_PER_TOKEN: usize = 4;
/// Fallback fixed-overhead estimate when we have no measured prompt size
/// yet. Covers system prompt + tool schemas + memory blocks for a typical
/// cortex turn. Refined per-turn via [`CompactionBudget::from_settings`].
const ESTIMATED_FIXED_OVERHEAD_TOKENS: u64 = 6_000;
/// Start compacting when history chars exceed this fraction of the budget.
const COMPACTION_TRIGGER_PCT: usize = 85;
/// Compact down to this fraction of the budget so we have slack to grow
/// across the next few turns before retriggering.
const COMPACTION_KEEP_PCT: usize = 70;
/// User messages weigh this much less when deciding the keep window — a
/// user message of N chars contributes N/3 to the running cutoff total so
/// user turns survive deeper into the kept history than assistant turns.
const USER_MESSAGE_WEIGHT_DIVISOR: usize = 3;

/// Per-turn budget for the history portion of the prompt. Computed from the
/// configured context limit, the output reservation, and (when known) the
/// prompt-token count from the prior LLM response.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CompactionBudget {
    pub max_history_chars: usize,
}

impl Default for CompactionBudget {
    fn default() -> Self {
        Self::legacy_default()
    }
}

impl CompactionBudget {
    /// Derive a budget from settings + the prompt-token count reported by
    /// the most recent LLM response (if any). Without prior measurement we
    /// fall back to a conservative fixed-overhead estimate. The output
    /// reservation is read from `settings.llm.llm_max_output` so a deployment
    /// that raises max_output (e.g. for a thinking-capable model) also
    /// raises the reserve automatically. Context window is auto-detected
    /// from the current model id via the per-model catalog.
    pub fn from_settings(settings: &Settings, last_prompt_tokens: Option<u64>) -> Self {
        let context_tokens = settings.llm.context_limit_for(&settings.llm.llm_model);
        let output_reserve_tokens = settings.llm.llm_max_output as u64;
        let overhead_tokens = match last_prompt_tokens {
            // Use the prior prompt size as an overhead floor: it includes
            // the system + tools + memory + history we just sent. Halving it
            // gives a soft estimate of the non-history portion that's likely
            // stable, leaving the rest for new history this turn.
            Some(prior) if prior > 0 => (prior / 2).max(ESTIMATED_FIXED_OVERHEAD_TOKENS),
            _ => ESTIMATED_FIXED_OVERHEAD_TOKENS,
        };
        let available = context_tokens
            .saturating_sub(output_reserve_tokens)
            .saturating_sub(overhead_tokens);
        Self {
            max_history_chars: (available as usize).saturating_mul(CHARS_PER_TOKEN),
        }
    }

    /// Legacy fixed budget for tests and entry points without a configured
    /// context limit.
    pub fn legacy_default() -> Self {
        Self {
            max_history_chars: 120_000,
        }
    }

    fn trigger_chars(self) -> usize {
        self.max_history_chars * COMPACTION_TRIGGER_PCT / 100
    }

    fn keep_chars(self) -> usize {
        self.max_history_chars * COMPACTION_KEEP_PCT / 100
    }

    /// Per-tool-result inline cap scaled to this budget. Big-context models
    /// keep more inline before archiving; small-context models archive
    /// aggressively to avoid one chunky tool result eating the whole budget.
    fn max_tool_result_inline_chars(self) -> usize {
        (self.max_history_chars / TOOL_RESULT_INLINE_BUDGET_DIVISOR.max(1))
            .max(MIN_TOOL_RESULT_INLINE_CHARS)
    }
}

/// Multi-pass compaction modelled after the Python main branch.
///
/// Pass 1: archive any tool result older than the most recent
/// [`RECENT_TOOL_CALL_GROUPS_TO_PRESERVE`] groups whose payload exceeds
/// the budget's per-tool inline cap. We replace the content with a one-line
/// reference that points back at the persistent message id, so the agent can
/// `conversation_get(message_id="…")` to retrieve the full text if needed.
///
/// Pass 2: if we're still over the trigger threshold, drop oldest messages
/// down to the keep target. Counts user messages at reduced weight
/// ([`USER_MESSAGE_WEIGHT_DIVISOR`]) so user turns survive deeper into the
/// kept window than equally-sized assistant turns. Tool_call/tool_result
/// pairs are dropped atomically so the wire format stays valid.
///
/// The dropped batch is returned to the caller so it can be summarized into
/// the rolling `conversation_summary` block (Pass 3, async, in chat_once).
fn compact_history(messages: &mut Vec<LlmMessage>, budget: CompactionBudget) -> Vec<LlmMessage> {
    archive_old_tool_results(messages, budget.max_tool_result_inline_chars());
    archive_old_images(messages);
    if total_chars(messages) <= budget.trigger_chars() {
        return Vec::new();
    }
    drop_oldest_to_target(messages, budget.keep_chars())
}

fn message_chars(message: &LlmMessage) -> usize {
    message.content.chars().count()
        + message
            .attachments
            .iter()
            .map(|att| att.base64_content.chars().count())
            .sum::<usize>()
        + message
            .tool_responses
            .iter()
            .map(|response| response.content.chars().count())
            .sum::<usize>()
}

fn total_chars(messages: &[LlmMessage]) -> usize {
    messages.iter().map(message_chars).sum()
}

/// Per Python's logic: walk newest-first, accumulating `effective` weighted
/// chars, find the index at which we'd exceed `target_chars`. User messages
/// Replace base64-encoded image payloads in old user messages with
/// lightweight `[\u200bimage: N chars archived]` placeholders, freeing
/// the compaction budget from multi-megabyte inline images that would
/// otherwise blow past the model's context window. Operates in-place
/// on `LlmMessage.content` since image data is stored as JSON text
/// (OpenAI-style content parts with `image_url`/`data:` URLs).
///
/// Recent images (last 2 user turns) are preserved so the model can
/// still see what was just sent. Older images get replaced with a stub
/// that preserves the message structure but drops the binary payload.
fn archive_old_images(messages: &mut [LlmMessage]) {
    // Find the index cutoff: keep images in the last 2 user messages.
    let recent_user_cutoff = {
        let mut user_count = 0usize;
        let mut cutoff = messages.len(); // default: archive everything
        for (idx, msg) in messages.iter().enumerate().rev() {
            if msg.role == LlmRole::User {
                user_count += 1;
                if user_count >= 2 {
                    cutoff = idx;
                    break;
                }
            }
        }
        cutoff
    };

    for (idx, message) in messages.iter_mut().enumerate() {
        if idx >= recent_user_cutoff {
            continue;
        }
        if !message.content.contains("data:image") && !message.content.contains("base64,") {
            continue;
        }
        message.content = replace_base64_images_with_stubs(&message.content);
    }
}

/// Parse a message's content (which may be a JSON array of content parts)
/// and replace `image_url` parts containing `data:` base64 URLs with
/// lightweight placeholders. If parsing fails, fall back to a regex-based
/// strip that preserves surrounding text.
fn replace_base64_images_with_stubs(content: &str) -> String {
    // Try parsing as JSON array of content parts (OpenAI multi-modal format).
    if let Ok(parts) = serde_json::from_str::<Vec<serde_json::Value>>(content) {
        let mut changed = false;
        let mut result = Vec::with_capacity(parts.len());
        for part in &parts {
            if let Some(obj) = part.as_object() {
                if obj.get("type").and_then(|v| v.as_str()) == Some("image_url") {
                    if let Some(url) = obj
                        .get("image_url")
                        .and_then(|iu| iu.get("url"))
                        .and_then(|v| v.as_str())
                        && url.starts_with("data:image")
                    {
                        let media_type = url
                            .strip_prefix("data:")
                            .and_then(|s| s.split(';').next())
                            .unwrap_or("image");
                        let char_count = url.chars().count();
                        let stub = format!("[image: {media_type}, {char_count} chars archived]");
                        result.push(serde_json::json!({
                            "type": "text",
                            "text": stub
                        }));
                        changed = true;
                        continue;
                    }
                }
            }
            result.push(part.clone());
        }
        if changed {
            return serde_json::to_string(&result).unwrap_or_else(|_| content.to_string());
        }
    }

    // Fallback: regex-strip any `data:image/...;base64,...` blobs.
    // This handles cases where content is not a clean JSON array.
    let re = regex::Regex::new(r"data:image/[^;]+;base64,[A-Za-z0-9+/=]+")
        .unwrap_or_else(|_| unreachable!());
    let stripped = re.replace_all(content, "[image archived]");
    if stripped != content {
        return stripped.into_owned();
    }

    content.to_string()
}

/// count at 1/[`USER_MESSAGE_WEIGHT_DIVISOR`] of their raw size so they're
/// retained more aggressively.
fn weighted_keep_cutoff(messages: &[LlmMessage], target_chars: usize) -> usize {
    let mut weighted = 0usize;
    for (idx, message) in messages.iter().enumerate().rev() {
        let raw = message_chars(message);
        let effective = if message.role == LlmRole::User {
            raw / USER_MESSAGE_WEIGHT_DIVISOR.max(1)
        } else {
            raw
        };
        if weighted.saturating_add(effective) > target_chars {
            return idx + 1;
        }
        weighted += effective;
    }
    0
}

/// Adjust the proposed cutoff so we never split a tool_call/tool_result
/// pair across the kept/dropped boundary. Back up until the boundary
/// neither cuts off a trailing assistant_tool_calls nor starts with a
/// leading tool_results.
fn pair_safe_cutoff(messages: &[LlmMessage], proposed: usize) -> usize {
    let mut cutoff = proposed;
    while cutoff > 0 && cutoff < messages.len() {
        let prev_has_calls = !messages[cutoff - 1].tool_calls.is_empty();
        let at_has_responses = !messages[cutoff].tool_responses.is_empty();
        if prev_has_calls || at_has_responses {
            cutoff -= 1;
        } else {
            break;
        }
    }
    cutoff
}

fn archive_old_tool_results(messages: &mut [LlmMessage], inline_cap: usize) {
    // Walk back from the end to find the indices of the last N tool_result
    // messages; those are the "fresh" groups we leave untouched.
    let mut fresh_indices: std::collections::HashSet<usize> = std::collections::HashSet::new();
    let mut seen_groups = 0;
    for (idx, message) in messages.iter().enumerate().rev() {
        if !message.tool_responses.is_empty() {
            fresh_indices.insert(idx);
            seen_groups += 1;
            if seen_groups >= RECENT_TOOL_CALL_GROUPS_TO_PRESERVE {
                break;
            }
        }
    }

    for (idx, message) in messages.iter_mut().enumerate() {
        if message.tool_responses.is_empty() || fresh_indices.contains(&idx) {
            continue;
        }
        for response in &mut message.tool_responses {
            if response.content.chars().count() <= inline_cap {
                continue;
            }
            let original_chars = response.content.chars().count();
            let reference = match &response.source_message_id {
                Some(id) => format!(
                    "[{} chars archived — conversation_get(message_id=\"{}\") for full text]",
                    original_chars, id
                ),
                None => format!(
                    "[{} chars archived — original message id unavailable]",
                    original_chars
                ),
            };
            response.content = reference;
        }
    }
}

/// Hard ceiling on the fully-assembled prompt, applied AFTER `compact_history`.
///
/// The soft compaction budget estimates the non-history overhead (system, tools,
/// memory, recall) from half the prior prompt size. When that estimate is wrong
/// — e.g. a runaway tool chain, or a prior measurement that doesn't reflect the
/// current turn — history can survive well past the model's context window
/// (observed: a 136k-token prompt against an 80k budget, which then overflows
/// the provider's context limit and forces every call onto a slow fallback).
///
/// This is the guaranteed backstop: drop the oldest history messages — never the
/// leading system head, never the final (current user) message, and never
/// leaving a `tool_results` message without its preceding `assistant`+tool_calls
/// — until the total character count fits `max_total_chars`.
fn clamp_messages_to_budget(messages: &mut Vec<LlmMessage>, max_total_chars: usize) {
    if max_total_chars == 0 {
        return;
    }
    let history_start = messages
        .iter()
        .position(|message| message.role != LlmRole::System)
        .unwrap_or(messages.len());
    while total_chars(messages) > max_total_chars {
        let current_user_index = messages.len().saturating_sub(1);
        let Some(remove_index) = (history_start..current_user_index)
            .find(|index| !is_resumable_checkpoint_message(&messages[*index]))
        else {
            break;
        };
        messages.remove(remove_index);
        // Removing an `assistant_with_tool_calls` would orphan the `tool_results`
        // that follow it; drop those too so the wire format stays valid.
        while remove_index < messages.len().saturating_sub(1)
            && !messages[remove_index].tool_responses.is_empty()
        {
            messages.remove(remove_index);
        }
    }
}

/// Final assembly guard: drop any leading orphaned `tool_result` messages so the
/// first non-system message is never a `tool_result`. The truncation paths
/// (`compact_history`, `clamp_messages_to_budget`) try to keep tool_use↔tool_result
/// pairs intact, but a turn interrupted mid-tool-call can persist a half-pair in
/// stored history; Anthropic then 400s ("tool_result without tool_use") on every
/// subsequent turn until the orphan ages out. This makes that unrepresentable.
fn drop_leading_orphan_tool_results(messages: &mut Vec<LlmMessage>) {
    let start = messages
        .iter()
        .position(|message| message.role != LlmRole::System)
        .unwrap_or(messages.len());
    while start < messages.len() && !messages[start].tool_responses.is_empty() {
        messages.remove(start);
    }
}

fn drop_oldest_to_target(messages: &mut Vec<LlmMessage>, target_chars: usize) -> Vec<LlmMessage> {
    // Always keep at least the last two messages so the LLM has SOME
    // immediate context to react to.
    const MIN_RETAINED: usize = 2;
    let proposed = weighted_keep_cutoff(messages, target_chars);
    // Apply the MIN_RETAINED clamp BEFORE the pair-safety walk. Taking `.min()`
    // *after* `pair_safe_cutoff` could lower the cutoff onto a non-pair-safe
    // index — splitting an assistant `tool_call` from its `tool_result` and
    // leaving the kept history starting on an orphaned `tool_result` (Anthropic
    // 400). Running `pair_safe_cutoff` last guarantees the final cutoff never
    // splits a pair; since it only ever lowers the cutoff, at least MIN_RETAINED
    // messages are still retained.
    let bounded = proposed.min(messages.len().saturating_sub(MIN_RETAINED));
    let cutoff = pair_safe_cutoff(messages, bounded);

    let mut dropped = Vec::with_capacity(cutoff);
    for _ in 0..cutoff {
        dropped.push(messages.remove(0));
    }
    dropped
}

/// Attach the dialect's cache hints to the system messages: the first system
/// message is the stable head, the last is the volatile tail. With two
/// messages this lands two cache breakpoints; with one (e.g. when only the
/// stable half exists) it lands one.
fn apply_cache_markers(messages: &mut [LlmMessage], dialect: &dyn crate::llm::PromptDialect) {
    let system_indices: Vec<usize> = messages
        .iter()
        .enumerate()
        .filter(|(_, message)| message.role == LlmRole::System)
        .map(|(index, _)| index)
        .collect();
    if let Some(&first) = system_indices.first()
        && let Some(hint) = dialect.cache_marker_for_stable()
    {
        messages[first].cache_control = Some(hint);
    }
    if let Some(&last) = system_indices.last()
        && system_indices.len() > 1
        && let Some(hint) = dialect.cache_marker_for_volatile()
    {
        messages[last].cache_control = Some(hint);
    }
}

fn history_content_with_timestamp(message: &StoredMessage) -> String {
    let Ok(created) = chrono::DateTime::parse_from_rfc3339(&message.created_at) else {
        return message.content.clone();
    };
    let stamp = created
        .with_timezone(&Local)
        .format("%a %Y-%m-%d %H:%M:%S %Z")
        .to_string();
    format!("[{stamp}]\n{}", message.content)
}

fn stamp_user_message(message: &str) -> String {
    let stamp = Local::now().format("%a %Y-%m-%d %H:%M:%S %Z").to_string();
    format!("[{stamp}]\n{message}")
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;
    use crate::actor::{ActorCompletionDelivery, ActorRunSpec, ModelTier};
    use crate::config::Settings;
    use crate::llm::LlmRole;
    use crate::memory::message_metadata::{MessageKind, MessageVisibility, metadata_value};

    fn settings(root: &std::path::Path) -> Settings {
        crate::config::test_settings(root)
    }

    /// Concatenate every System-role message into one string. The prompt is
    /// now split into (stable, volatile) parts, so tests checking individual
    /// fragments shouldn't care which half they landed in.
    fn system_content(messages: &[LlmMessage]) -> String {
        messages
            .iter()
            .filter(|message| message.role == LlmRole::System)
            .map(|message| message.content.as_str())
            .collect::<Vec<_>>()
            .join("\n\n")
    }

    #[test]
    fn prepare_turn_includes_memory_context_history_and_recall() {
        let tmp = tempdir().unwrap();
        let settings = settings(tmp.path());
        let memory = MemoryStore::from_settings(&settings).unwrap();
        let prompts = PromptStore::new(&settings.paths.workspace_dir, &settings.paths.config_dir);

        memory
            .notes
            .create(
                "Graph Email",
                "Use MSAL for graph email.",
                &["skill".to_string()],
                None,
            )
            .unwrap();
        memory
            .messages
            .add(MessageRole::User, "previous graph question", None)
            .unwrap();

        let turn = prepare_turn(
            &settings,
            &memory,
            &prompts,
            "How do I use graph email?",
            Vec::new(),
            None,
            &AgentOptions::default(),
        )
        .unwrap();

        assert_eq!(turn.messages[0].role, LlmRole::System);
        let system = system_content(&turn.messages);
        assert!(system.contains("<identity_block>"));
        assert!(system.contains("<memory_metadata>"));
        assert!(system.contains("<runtime_context source=\"hippocampus\""));
        assert!(system.contains("Graph Email"));
        assert!(
            turn.messages
                .iter()
                .any(|message| message.content.ends_with("previous graph question"))
        );
        assert!(
            turn.messages
                .last()
                .unwrap()
                .content
                .ends_with("How do I use graph email?")
        );
    }

    #[tokio::test]
    async fn pending_summary_sync_point_waits_then_clears() {
        use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};

        // A fast summary task: the next turn must observe its side effects.
        let done = Arc::new(AtomicBool::new(false));
        let done_clone = done.clone();
        let slot = tokio::sync::Mutex::new(Some(tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            done_clone.store(true, AtomicOrdering::SeqCst);
        })));
        await_pending_task(&slot, std::time::Duration::from_secs(5)).await;
        assert!(
            done.load(AtomicOrdering::SeqCst),
            "next turn must start only after the summary task finished"
        );
        assert!(slot.lock().await.is_none(), "finished handle is cleared");

        // A pathologically slow task: the sync point gives up after the
        // timeout instead of blocking the user's turn forever.
        let slow_slot = tokio::sync::Mutex::new(Some(tokio::spawn(async {
            tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
        })));
        let started = std::time::Instant::now();
        await_pending_task(&slow_slot, std::time::Duration::from_millis(50)).await;
        assert!(started.elapsed() < std::time::Duration::from_secs(5));
        let guard = slow_slot.lock().await;
        assert!(guard.is_some(), "unfinished handle stays for a later check");
        guard.as_ref().unwrap().abort();
    }

    #[test]
    fn prepare_turn_injects_open_work_into_system_prompt() {
        let tmp = tempdir().unwrap();
        let settings = settings(tmp.path());
        let memory = MemoryStore::from_settings(&settings).unwrap();
        let prompts = PromptStore::new(&settings.paths.workspace_dir, &settings.paths.config_dir);

        // No open work — no <active_tasks> block at all.
        let turn = prepare_turn(
            &settings,
            &memory,
            &prompts,
            "hello",
            Vec::new(),
            None,
            &AgentOptions::default(),
        )
        .unwrap();
        assert!(!system_content(&turn.messages).contains("<active_tasks>"));

        // An in-progress todo shows up in every subsequent prompt, unprompted.
        let id = memory
            .todos
            .create(crate::todos::NewTodo {
                title: "finish the migration plan".to_string(),
                ..Default::default()
            })
            .unwrap();
        memory
            .todos
            .update(
                id,
                crate::todos::TodoUpdate {
                    status: Some(crate::todos::TodoStatus::InProgress),
                    ..Default::default()
                },
            )
            .unwrap();

        let turn = prepare_turn(
            &settings,
            &memory,
            &prompts,
            "hello again",
            Vec::new(),
            None,
            &AgentOptions::default(),
        )
        .unwrap();
        let system = system_content(&turn.messages);
        assert!(system.contains("<active_tasks>"));
        assert!(system.contains("finish the migration plan"));
        assert!(system.contains("[in_progress]"));
    }

    #[test]
    fn hosted_agenda_suppresses_local_active_tasks_prompt() {
        let tmp = tempdir().unwrap();
        let mut settings = settings(tmp.path());
        settings.hosted_plugins.replace_local_todos = true;
        let memory = MemoryStore::from_settings(&settings).unwrap();
        let prompts = PromptStore::new(&settings.paths.workspace_dir, &settings.paths.config_dir);
        let id = memory
            .todos
            .create(crate::todos::NewTodo {
                title: "local split-brain task".to_string(),
                ..Default::default()
            })
            .unwrap();
        memory
            .todos
            .update(
                id,
                crate::todos::TodoUpdate {
                    status: Some(crate::todos::TodoStatus::InProgress),
                    ..Default::default()
                },
            )
            .unwrap();

        let turn = prepare_turn(
            &settings,
            &memory,
            &prompts,
            "hello",
            Vec::new(),
            None,
            &AgentOptions::default(),
        )
        .unwrap();
        let system = system_content(&turn.messages);
        assert!(!system.contains("<active_tasks>"));
        assert!(!system.contains("local split-brain task"));
    }

    #[test]
    fn prepare_turn_excludes_tool_loop_chatter_from_history() {
        let tmp = tempdir().unwrap();
        let settings = settings(tmp.path());
        let memory = MemoryStore::from_settings(&settings).unwrap();
        let prompts = PromptStore::new(&settings.paths.workspace_dir, &settings.paths.config_dir);

        memory
            .messages
            .add(
                MessageRole::Assistant,
                "I will inspect that now.",
                Some(json!({"tool_calls": [{"name": "bash"}]})),
            )
            .unwrap();
        memory
            .messages
            .add(
                MessageRole::Tool,
                "secret tool output",
                Some(json!({"name": "bash"})),
            )
            .unwrap();
        memory
            .messages
            .add(MessageRole::User, "previous visible user request", None)
            .unwrap();
        memory
            .messages
            .add(MessageRole::Assistant, "previous visible answer", None)
            .unwrap();

        let turn = prepare_turn(
            &settings,
            &memory,
            &prompts,
            "follow-up",
            Vec::new(),
            None,
            &AgentOptions {
                use_hippocampus: false,
                ..Default::default()
            },
        )
        .unwrap();

        let contents = turn
            .messages
            .iter()
            .map(|message| message.content.as_str())
            .collect::<Vec<_>>();
        assert!(
            !contents
                .iter()
                .any(|c| c.contains("I will inspect that now."))
        );
        assert!(!contents.iter().any(|c| c.contains("secret tool output")));
        assert!(
            contents
                .iter()
                .any(|c| c.ends_with("previous visible user request"))
        );
        assert!(contents.contains(&"previous visible answer"));
        let system = system_content(&turn.messages);
        assert!(!system.contains("recent_tool_history"));
    }

    #[test]
    fn prepare_turn_preserves_paired_tool_calls_and_responses() {
        let tmp = tempdir().unwrap();
        let settings = settings(tmp.path());
        let memory = MemoryStore::from_settings(&settings).unwrap();
        let prompts = PromptStore::new(&settings.paths.workspace_dir, &settings.paths.config_dir);

        memory
            .messages
            .add(MessageRole::User, "what's in foo.txt?", None)
            .unwrap();
        memory
            .messages
            .add(
                MessageRole::Assistant,
                "reading it",
                Some(json!({
                    "tool_calls": [{
                        "call_id": "call-abc",
                        "fn_name": "read_file",
                        "fn_arguments": {"file_path": "foo.txt"},
                    }]
                })),
            )
            .unwrap();
        memory
            .messages
            .add(
                MessageRole::Tool,
                "file contents: hello",
                Some(json!({"tool_call_id": "call-abc", "name": "read_file"})),
            )
            .unwrap();
        memory
            .messages
            .add(MessageRole::Assistant, "it says hello", None)
            .unwrap();

        let turn = prepare_turn(
            &settings,
            &memory,
            &prompts,
            "ok thanks",
            Vec::new(),
            None,
            &AgentOptions {
                use_hippocampus: false,
                ..Default::default()
            },
        )
        .unwrap();

        // 2 system (stable + volatile) + user + assistant_with_tool_calls
        // + tool_results + assistant + new user = 7.
        assert_eq!(turn.messages.len(), 7);
        let call_msg = turn
            .messages
            .iter()
            .find(|m| !m.tool_calls.is_empty())
            .expect("assistant with tool_calls");
        assert_eq!(call_msg.tool_calls.len(), 1);
        assert_eq!(call_msg.tool_calls[0].call_id, "call-abc");
        assert_eq!(call_msg.tool_calls[0].fn_name, "read_file");
        let result_msg = turn
            .messages
            .iter()
            .find(|m| !m.tool_responses.is_empty())
            .expect("tool results");
        assert_eq!(result_msg.tool_responses.len(), 1);
        assert_eq!(result_msg.tool_responses[0].call_id, "call-abc");
        assert_eq!(result_msg.tool_responses[0].content, "file contents: hello");
    }

    #[test]
    fn compact_history_archives_old_tool_results_above_inline_cap() {
        let inline_cap = CompactionBudget::legacy_default().max_tool_result_inline_chars();
        let big = "x".repeat(inline_cap + 100);
        let small = "y".repeat(200);
        let mut messages = vec![
            LlmMessage::user("first"),
            LlmMessage::assistant_with_tool_calls(
                "running A",
                vec![HistoricalToolCall {
                    call_id: "c-old".to_string(),
                    fn_name: "bash".to_string(),
                    fn_arguments: json!({}),
                    thought_signatures: None,
                }],
            ),
            LlmMessage::tool_results(vec![HistoricalToolResponse {
                call_id: "c-old".to_string(),
                content: big.clone(),
                source_message_id: Some("msg-old".to_string()),
            }]),
            // 2 recent groups follow — they must NOT be archived.
            LlmMessage::assistant_with_tool_calls(
                "running B",
                vec![HistoricalToolCall {
                    call_id: "c-mid".to_string(),
                    fn_name: "bash".to_string(),
                    fn_arguments: json!({}),
                    thought_signatures: None,
                }],
            ),
            LlmMessage::tool_results(vec![HistoricalToolResponse {
                call_id: "c-mid".to_string(),
                content: big.clone(),
                source_message_id: Some("msg-mid".to_string()),
            }]),
            LlmMessage::assistant_with_tool_calls(
                "running C",
                vec![HistoricalToolCall {
                    call_id: "c-new".to_string(),
                    fn_name: "bash".to_string(),
                    fn_arguments: json!({}),
                    thought_signatures: None,
                }],
            ),
            LlmMessage::tool_results(vec![HistoricalToolResponse {
                call_id: "c-new".to_string(),
                content: small.clone(),
                source_message_id: Some("msg-new".to_string()),
            }]),
        ];

        compact_history(&mut messages, CompactionBudget::legacy_default());

        // Old tool result was archived with a reference back to msg-old.
        let old_response = &messages[2].tool_responses[0];
        assert!(old_response.content.contains("archived"));
        assert!(old_response.content.contains("msg-old"));
        // Recent results were preserved untouched.
        assert_eq!(messages[4].tool_responses[0].content.len(), big.len());
        assert_eq!(messages[6].tool_responses[0].content, small);
    }

    #[test]
    fn compact_history_drops_oldest_messages_when_over_budget() {
        let budget = CompactionBudget {
            max_history_chars: 4_000,
        };
        let chunk = "z".repeat(budget.max_history_chars / 4);
        let mut messages = vec![
            LlmMessage::user(chunk.clone()),
            LlmMessage::assistant(chunk.clone()),
            LlmMessage::user(chunk.clone()),
            LlmMessage::assistant(chunk.clone()),
            LlmMessage::user("recent-marker".to_string()),
            LlmMessage::assistant(chunk),
        ];
        let initial_chars = total_chars(&messages);
        assert!(initial_chars > budget.trigger_chars());

        let dropped = compact_history(&mut messages, budget);

        assert!(!dropped.is_empty(), "compaction should drop some history");
        // Pass-2 compacts down to ~keep_chars; we should be at or below it.
        assert!(total_chars(&messages) <= budget.max_history_chars);
        // Newest messages survived; the "recent-marker" anchor is still there.
        assert!(messages.iter().any(|m| m.content == "recent-marker"));
    }

    #[test]
    fn weighted_cutoff_keeps_more_when_history_is_all_user_messages() {
        // Build two histories of equal raw size — one all user, one all
        // assistant — and run compaction with the same budget. The user
        // history should keep more messages because each is counted at
        // 1/USER_MESSAGE_WEIGHT_DIVISOR of its raw chars when computing the
        // keep cutoff.
        let chunk = "x".repeat(2_000);
        let mut all_user: Vec<LlmMessage> =
            (0..10).map(|_| LlmMessage::user(chunk.clone())).collect();
        let mut all_assistant: Vec<LlmMessage> = (0..10)
            .map(|_| LlmMessage::assistant(chunk.clone()))
            .collect();
        let budget = CompactionBudget {
            max_history_chars: 8_000,
        };

        compact_history(&mut all_user, budget);
        compact_history(&mut all_assistant, budget);

        assert!(
            all_user.len() > all_assistant.len(),
            "user-heavy history should keep more messages than assistant-heavy \
             under the same budget (user={}, assistant={})",
            all_user.len(),
            all_assistant.len(),
        );
    }

    #[test]
    fn budget_from_settings_uses_prior_prompt_tokens_when_available() {
        let tmp = tempdir().unwrap();
        let settings = settings(tmp.path());
        let no_prior = CompactionBudget::from_settings(&settings, None);
        let with_prior = CompactionBudget::from_settings(&settings, Some(50_000));
        // A real prior measurement makes the budget smaller (we know overhead
        // is at least prior/2), which means less room for new history.
        assert!(with_prior.max_history_chars < no_prior.max_history_chars);
    }

    #[test]
    fn clamp_messages_bounds_prompt_and_keeps_system_and_last() {
        let mut messages = vec![
            LlmMessage::system("S".repeat(20)),
            LlmMessage::user("a".repeat(500)),
            LlmMessage::assistant("b".repeat(500)),
            LlmMessage::user("c".repeat(500)),
            LlmMessage::user("CURRENT".to_string()),
        ];
        clamp_messages_to_budget(&mut messages, 100);
        // Fits the hard budget…
        assert!(
            total_chars(&messages) <= 100,
            "total={}",
            total_chars(&messages)
        );
        // …while always preserving the system head and the current user turn.
        assert_eq!(messages.first().unwrap().role, LlmRole::System);
        assert_eq!(messages.last().unwrap().content, "CURRENT");
    }

    #[test]
    fn clamp_messages_drops_orphaned_tool_results() {
        // [system, assistant+tool_calls, tool_results, user]. Dropping the
        // assistant must also drop its now-orphaned tool_results so the kept
        // history never starts with a dangling tool result.
        let mut messages = vec![
            LlmMessage::system("S"),
            LlmMessage::assistant_with_tool_calls(
                "x".repeat(400),
                vec![HistoricalToolCall {
                    call_id: "c1".into(),
                    fn_name: "read_file".into(),
                    fn_arguments: json!({}),
                    thought_signatures: None,
                }],
            ),
            LlmMessage::tool_results(vec![HistoricalToolResponse {
                call_id: "c1".into(),
                content: "y".repeat(400),
                source_message_id: Some("m1".into()),
            }]),
            LlmMessage::user("CURRENT".to_string()),
        ];
        clamp_messages_to_budget(&mut messages, 50);
        // The tool_call/tool_result pair is gone as a unit; no leading orphan.
        assert!(messages.iter().all(|m| m.tool_responses.is_empty()));
        assert!(messages.iter().all(|m| m.tool_calls.is_empty()));
        assert_eq!(messages.first().unwrap().role, LlmRole::System);
        assert_eq!(messages.last().unwrap().content, "CURRENT");
    }

    #[test]
    fn hard_clamp_never_discards_a_resumable_checkpoint() {
        let checkpoint = format!(
            "{RESUMABLE_CHECKPOINT_PREFIX}SECRET_HARD_CLAMP_CANARY{RESUMABLE_CHECKPOINT_SUFFIX}"
        );
        let mut messages = vec![
            LlmMessage::system("S".repeat(80)),
            LlmMessage::user("old history".repeat(40)),
            LlmMessage::assistant(checkpoint.clone()),
            LlmMessage::user("CURRENT"),
        ];

        clamp_messages_to_budget(&mut messages, 20);

        assert!(messages.iter().any(|message| message.content == checkpoint));
        assert_eq!(messages.last().unwrap().content, "CURRENT");
        assert!(
            messages
                .iter()
                .all(|message| !message.content.contains("old history"))
        );
    }

    #[test]
    fn drop_oldest_to_target_never_leaves_leading_orphan_via_min_retained() {
        // The MIN_RETAINED clamp must not be able to lower the cutoff onto a
        // tool_results boundary. Here a naive `pair_safe_cutoff(..).min(len-2)`
        // would keep [tool_results(c2), CURRENT] — a leading orphan; applying
        // pair_safe_cutoff AFTER the clamp must keep the whole c2 pair instead.
        let call = |id: &str| HistoricalToolCall {
            call_id: id.into(),
            fn_name: "read_file".into(),
            fn_arguments: json!({}),
            thought_signatures: None,
        };
        let resp = |id: &str| HistoricalToolResponse {
            call_id: id.into(),
            content: "r".repeat(50),
            source_message_id: Some("m".into()),
        };
        let mut messages = vec![
            LlmMessage::assistant_with_tool_calls("a".repeat(50), vec![call("c1")]),
            LlmMessage::tool_results(vec![resp("c1")]),
            LlmMessage::assistant_with_tool_calls("a".repeat(50), vec![call("c2")]),
            LlmMessage::tool_results(vec![resp("c2")]),
            LlmMessage::user("CURRENT".to_string()),
        ];
        // Tiny target forces dropping down toward MIN_RETAINED.
        let _ = drop_oldest_to_target(&mut messages, 1);
        assert!(
            messages
                .first()
                .map(|m| m.tool_responses.is_empty())
                .unwrap_or(true),
            "kept history must not start with an orphaned tool_result (role={:?})",
            messages.first().map(|m| m.role.clone())
        );
        assert_eq!(messages.last().unwrap().content, "CURRENT");
    }

    #[test]
    fn drop_leading_orphan_tool_results_strips_leading_orphan() {
        // A crash-persisted half-pair can put a tool_result first; the guard
        // must remove it so the wire never starts with a dangling tool_result.
        let mut messages = vec![
            LlmMessage::system("S"),
            LlmMessage::tool_results(vec![HistoricalToolResponse {
                call_id: "orphan".into(),
                content: "stale".into(),
                source_message_id: Some("m1".into()),
            }]),
            LlmMessage::user("CURRENT".to_string()),
        ];
        drop_leading_orphan_tool_results(&mut messages);
        assert_eq!(messages.first().unwrap().role, LlmRole::System);
        assert!(messages.iter().all(|m| m.tool_responses.is_empty()));
        assert_eq!(messages.last().unwrap().content, "CURRENT");
    }

    #[test]
    fn prepare_turn_drops_orphan_tool_call_pairs() {
        let tmp = tempdir().unwrap();
        let settings = settings(tmp.path());
        let memory = MemoryStore::from_settings(&settings).unwrap();
        let prompts = PromptStore::new(&settings.paths.workspace_dir, &settings.paths.config_dir);

        memory
            .messages
            .add(MessageRole::User, "do two things", None)
            .unwrap();
        // Assistant emitted two tool calls but only one matching result is
        // persisted — the whole pair must be dropped to avoid Anthropic 400s.
        memory
            .messages
            .add(
                MessageRole::Assistant,
                "running both",
                Some(json!({
                    "tool_calls": [
                        {"call_id": "c1", "fn_name": "read_file", "fn_arguments": {}},
                        {"call_id": "c2", "fn_name": "read_file", "fn_arguments": {}},
                    ]
                })),
            )
            .unwrap();
        memory
            .messages
            .add(
                MessageRole::Tool,
                "first result",
                Some(json!({"tool_call_id": "c1"})),
            )
            .unwrap();
        // No tool_result for c2 — orphan.

        let turn = prepare_turn(
            &settings,
            &memory,
            &prompts,
            "follow-up",
            Vec::new(),
            None,
            &AgentOptions {
                use_hippocampus: false,
                ..Default::default()
            },
        )
        .unwrap();

        assert!(
            !turn
                .messages
                .iter()
                .any(|m| !m.tool_calls.is_empty() || !m.tool_responses.is_empty()),
            "orphan pair must be dropped entirely"
        );
        assert!(
            turn.messages
                .iter()
                .any(|m| m.content.ends_with("do two things"))
        );
    }

    #[test]
    fn prepare_turn_excludes_internal_metadata_turns_from_history() {
        let tmp = tempdir().unwrap();
        let settings = settings(tmp.path());
        let memory = MemoryStore::from_settings(&settings).unwrap();
        let prompts = PromptStore::new(&settings.paths.workspace_dir, &settings.paths.config_dir);
        let internal = metadata_value(
            MessageVisibility::Internal,
            MessageKind::Heartbeat,
            "heartbeat",
        );

        memory
            .messages
            .add(
                MessageRole::User,
                "internal heartbeat prompt",
                Some(internal.clone()),
            )
            .unwrap();
        memory
            .messages
            .add(MessageRole::Assistant, "internal heartbeat answer", None)
            .unwrap();
        memory
            .messages
            .add(MessageRole::User, "visible question", None)
            .unwrap();

        let turn = prepare_turn(
            &settings,
            &memory,
            &prompts,
            "follow-up",
            Vec::new(),
            None,
            &AgentOptions {
                use_hippocampus: false,
                ..Default::default()
            },
        )
        .unwrap();

        let contents = turn
            .messages
            .iter()
            .map(|message| message.content.as_str())
            .collect::<Vec<_>>();
        assert!(
            !contents
                .iter()
                .any(|c| c.contains("internal heartbeat prompt"))
        );
        assert!(
            !contents
                .iter()
                .any(|c| c.contains("internal heartbeat answer"))
        );
        assert!(contents.iter().any(|c| c.ends_with("visible question")));
    }

    #[test]
    fn hidden_checkpoint_resumes_an_ordinary_followup_but_not_a_reaction() {
        const CANARY: &str = "SECRET_CHECKPOINT_CANARY";
        const SUMMARY_CANARY: &str = "SECRET_SUMMARY_CANARY";
        const HISTORY_CANARY: &str = "SECRET_VISIBLE_HISTORY_CANARY";
        const VOLATILE_CANARY: &str = "SECRET_VOLATILE_MEMORY_CANARY";
        const TODO_CANARY: &str = "SECRET_ACTIVE_TASK_CANARY";
        let tmp = tempdir().unwrap();
        let settings = settings(tmp.path());
        let memory = MemoryStore::from_settings(&settings).unwrap();
        let prompts = PromptStore::new(&settings.paths.workspace_dir, &settings.paths.config_dir);

        memory.set_conversation_summary(SUMMARY_CANARY).unwrap();
        memory
            .blocks
            .create(
                "reaction_volatile",
                VOLATILE_CANARY,
                "reaction isolation test",
                1_000,
                false,
                false,
            )
            .unwrap();
        let todo_id = memory
            .todos
            .create(crate::todos::NewTodo {
                title: TODO_CANARY.to_string(),
                ..Default::default()
            })
            .unwrap();
        memory
            .todos
            .update(
                todo_id,
                crate::todos::TodoUpdate {
                    status: Some(crate::todos::TodoStatus::InProgress),
                    ..Default::default()
                },
            )
            .unwrap();

        memory
            .messages
            .add(
                MessageRole::User,
                &format!("research this {HISTORY_CANARY}"),
                None,
            )
            .unwrap();
        persist_resumable_checkpoint(
            &memory,
            &format!("GOAL — {CANARY}\nDONE — partial"),
            "test_breaker",
        )
        .unwrap();
        persist_checkpoint_notice(&memory).unwrap();

        let stored = memory.messages.get_recent(10).unwrap();
        assert_eq!(stored.len(), 3);
        let checkpoint_metadata = MessageMetadata::from_value(Some(&stored[1].metadata));
        assert!(checkpoint_metadata.is_internal());
        assert_eq!(
            raw_message_kind(Some(&stored[1].metadata)),
            Some(RawMessageKind::Checkpoint)
        );
        assert_eq!(stored[2].content, TURN_CHECKPOINT_NOTICE);
        let notice_metadata = MessageMetadata::from_value(Some(&stored[2].metadata));
        assert!(!notice_metadata.is_internal());
        assert_eq!(
            raw_message_kind(Some(&stored[2].metadata)),
            Some(RawMessageKind::CheckpointNotice)
        );

        let followup = prepare_turn(
            &settings,
            &memory,
            &prompts,
            "continue",
            Vec::new(),
            None,
            &AgentOptions {
                use_hippocampus: false,
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(
            followup
                .messages
                .iter()
                .filter(|message| message.content.contains(CANARY))
                .count(),
            1,
            "ordinary follow-up must receive the resumable checkpoint exactly once"
        );
        let followup_system = system_content(&followup.messages);
        assert!(followup_system.contains(SUMMARY_CANARY));
        assert!(followup_system.contains(VOLATILE_CANARY));
        assert!(followup_system.contains(TODO_CANARY));
        assert!(
            followup
                .messages
                .iter()
                .all(|message| !message.content.contains(TURN_CHECKPOINT_NOTICE)),
            "the UI-only notice must not displace the checkpoint during compaction"
        );

        let reaction_metadata = metadata_value(
            MessageVisibility::UserVisible,
            MessageKind::TelegramReaction,
            "telegram",
        );
        let reaction = prepare_turn(
            &settings,
            &memory,
            &prompts,
            "The user reacted with 👍. Silence is the default.",
            Vec::new(),
            Some(&reaction_metadata),
            &AgentOptions::default(),
        )
        .unwrap();
        let reaction_text = reaction
            .messages
            .iter()
            .map(|message| message.content.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        for private_canary in [
            CANARY,
            SUMMARY_CANARY,
            HISTORY_CANARY,
            VOLATILE_CANARY,
            TODO_CANARY,
        ] {
            assert!(
                !reaction_text.contains(private_canary),
                "reaction prompt exposed {private_canary}"
            );
        }
        assert!(reaction.recall.is_none());
        assert!(!reaction_text.contains("<runtime_context source=\"hippocampus\""));
        assert!(reaction_text.contains("The user reacted with 👍"));

        let wake_metadata =
            raw_metadata_value(MessageVisibility::Internal, RawMessageKind::Wake, "wake");
        let wake = prepare_turn(
            &settings,
            &memory,
            &prompts,
            "run the scheduled task",
            Vec::new(),
            Some(&wake_metadata),
            &AgentOptions {
                use_hippocampus: false,
                ..Default::default()
            },
        )
        .unwrap();
        assert!(
            wake.messages
                .iter()
                .all(|message| !message.content.contains(CANARY)),
            "an internal wake must not inherit a prior hidden checkpoint"
        );
        let wake_text = wake
            .messages
            .iter()
            .map(|message| message.content.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        for preserved_context in [SUMMARY_CANARY, HISTORY_CANARY, VOLATILE_CANARY, TODO_CANARY] {
            assert!(
                wake_text.contains(preserved_context),
                "wake lost established context {preserved_context}"
            );
        }
        let still_resumable = prepare_turn(
            &settings,
            &memory,
            &prompts,
            "ordinary turn after synthetic activity",
            Vec::new(),
            None,
            &AgentOptions {
                use_hippocampus: false,
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(
            still_resumable
                .messages
                .iter()
                .filter(|message| message.content.contains(CANARY))
                .count(),
            1,
            "reaction and synthetic turns must not consume an ordinary checkpoint"
        );
    }

    #[test]
    fn completed_resume_consumes_checkpoint_durably_across_restart() {
        const CANARY: &str = "SECRET_CONSUMED_CHECKPOINT_CANARY";
        let tmp = tempdir().unwrap();
        let settings = settings(tmp.path());
        let memory = MemoryStore::from_settings(&settings).unwrap();
        let prompts = PromptStore::new(&settings.paths.workspace_dir, &settings.paths.config_dir);
        memory
            .messages
            .add(MessageRole::User, "start resumable work", None)
            .unwrap();
        persist_resumable_checkpoint(&memory, CANARY, "test_breaker").unwrap();
        persist_checkpoint_notice(&memory).unwrap();

        let prepared = prepare_turn_with_checkpoint(
            &settings,
            &memory,
            &prompts,
            "continue",
            Vec::new(),
            None,
            &AgentOptions {
                use_hippocampus: false,
                ..Default::default()
            },
        )
        .unwrap();
        let checkpoint_id = prepared
            .resumable_checkpoint_id
            .expect("ordinary follow-up should bind the active checkpoint");
        assert!(
            prepared
                .turn
                .messages
                .iter()
                .any(|message| message.content.contains(CANARY))
        );
        memory
            .messages
            .add(MessageRole::User, "continue", None)
            .unwrap();
        persist_completed_ordinary_turn(&memory, "finished result", None, Some(&checkpoint_id))
            .unwrap();

        let stored = memory.messages.get_recent(20).unwrap();
        let resolution = stored
            .iter()
            .find(|message| {
                raw_message_kind(Some(&message.metadata))
                    == Some(RawMessageKind::CheckpointResolved)
            })
            .expect("durable resolution marker");
        assert_eq!(
            resolution
                .metadata
                .get(CHECKPOINT_ID_KEY)
                .and_then(Value::as_str),
            Some(checkpoint_id.as_str())
        );
        assert!(MessageMetadata::from_value(Some(&resolution.metadata)).is_internal());

        let next = prepare_turn(
            &settings,
            &memory,
            &prompts,
            "unrelated next request",
            Vec::new(),
            None,
            &AgentOptions {
                use_hippocampus: false,
                ..Default::default()
            },
        )
        .unwrap();
        assert!(
            next.messages
                .iter()
                .all(|message| !message.content.contains(CANARY))
        );

        drop(memory);
        let reopened = MemoryStore::from_settings(&settings).unwrap();
        let after_restart = prepare_turn(
            &settings,
            &reopened,
            &prompts,
            "request after restart",
            Vec::new(),
            None,
            &AgentOptions {
                use_hippocampus: false,
                ..Default::default()
            },
        )
        .unwrap();
        assert!(
            after_restart
                .messages
                .iter()
                .all(|message| !message.content.contains(CANARY))
        );
    }

    #[test]
    fn tool_delivery_or_poisoned_guard_cannot_leave_a_completed_checkpoint_live() {
        for poison_guard in [false, true] {
            let tmp = tempdir().unwrap();
            let settings = settings(tmp.path());
            let memory = MemoryStore::from_settings(&settings).unwrap();
            let prompts =
                PromptStore::new(&settings.paths.workspace_dir, &settings.paths.config_dir);
            let canary = format!("SECRET_GUARD_CHECKPOINT_{poison_guard}");
            memory
                .messages
                .add(MessageRole::User, "start", None)
                .unwrap();
            persist_resumable_checkpoint(&memory, &canary, "test_breaker").unwrap();
            let prepared = prepare_turn_with_checkpoint(
                &settings,
                &memory,
                &prompts,
                "continue",
                Vec::new(),
                None,
                &AgentOptions {
                    use_hippocampus: false,
                    ..Default::default()
                },
            )
            .unwrap();
            let checkpoint_id = prepared.resumable_checkpoint_id.unwrap();
            memory
                .messages
                .add(MessageRole::User, "continue", None)
                .unwrap();

            let guard = std::sync::Arc::new(std::sync::Mutex::new(
                crate::interfaces::telegram::TelegramTurnGuard::new(),
            ));
            if poison_guard {
                let poison = guard.clone();
                let _ = std::panic::catch_unwind(move || {
                    let _guard = poison.lock().unwrap();
                    panic!("poison guard for lifecycle test");
                });
            } else {
                guard
                    .lock()
                    .unwrap()
                    .record_visible_text("already delivered by tool");
            }
            let result = persist_completed_ordinary_turn(
                &memory,
                "SECRET_REDUNDANT_FINAL",
                Some(&guard),
                Some(&checkpoint_id),
            );
            assert_eq!(result.is_err(), poison_guard);

            let stored = memory.messages.get_recent(20).unwrap();
            assert!(
                stored
                    .iter()
                    .all(|message| message.content != "SECRET_REDUNDANT_FINAL")
            );
            let next = prepare_turn(
                &settings,
                &memory,
                &prompts,
                "next",
                Vec::new(),
                None,
                &AgentOptions {
                    use_hippocampus: false,
                    ..Default::default()
                },
            )
            .unwrap();
            assert!(
                next.messages
                    .iter()
                    .all(|message| !message.content.contains(&canary))
            );
        }
    }

    #[tokio::test]
    async fn reaction_turn_without_history_override_persists_only_the_safe_event() {
        const PRIVATE_PROMPT: &str = "SECRET reaction handling prompt and copied task details";
        let tmp = tempdir().unwrap();
        let settings = settings(tmp.path());
        let agent = Agent::from_settings(settings).unwrap();
        let metadata = json!({
            "lethe_visibility": "user_visible",
            "lethe_message_kind": "telegram_reaction",
            "lethe_source": "telegram",
            "reaction_new": ["👍"],
            "message_id": 77,
        });

        let result = agent
            .chat_once_result(TurnRequest::new(PRIVATE_PROMPT).with_metadata(metadata))
            .await
            .unwrap();

        assert_eq!(result, TurnResult::Complete(String::new()));
        let stored = agent.memory().messages.get_recent(10).unwrap();
        assert_eq!(stored.len(), 1);
        assert_eq!(
            stored[0].content,
            "[Telegram reaction added: 👍 on message 77]"
        );
        assert!(!stored[0].content.contains("SECRET"));
    }

    #[test]
    fn hidden_checkpoints_are_never_folded_into_the_conversation_summary_batch() {
        const CANARY: &str = "SECRET_CHECKPOINT_SUMMARY_CANARY";
        let tmp = tempdir().unwrap();
        let settings = settings(tmp.path());
        let memory = MemoryStore::from_settings(&settings).unwrap();
        let prompts = PromptStore::new(&settings.paths.workspace_dir, &settings.paths.config_dir);
        memory
            .messages
            .add(MessageRole::User, "start", None)
            .unwrap();
        persist_resumable_checkpoint(&memory, CANARY, "test_breaker").unwrap();
        for index in 0..6 {
            memory
                .messages
                .add(
                    MessageRole::User,
                    &format!("newer user {index} {}", "u".repeat(120)),
                    None,
                )
                .unwrap();
            memory
                .messages
                .add(
                    MessageRole::Assistant,
                    &format!("newer assistant {index} {}", "a".repeat(120)),
                    None,
                )
                .unwrap();
        }

        let turn = prepare_turn(
            &settings,
            &memory,
            &prompts,
            "current",
            Vec::new(),
            None,
            &AgentOptions {
                use_hippocampus: false,
                compaction_budget: CompactionBudget {
                    max_history_chars: 300,
                },
            },
        )
        .unwrap();

        assert!(!turn.dropped_for_summary.is_empty());
        assert!(
            turn.dropped_for_summary
                .iter()
                .all(|message| !message.content.contains(CANARY))
        );
    }

    #[test]
    fn notification_review_context_excludes_internal_checkpoints() {
        const CANARY: &str = "SECRET_REVIEW_CONTEXT_CANARY";
        const LEGACY_CANARY: &str = "SECRET_LEGACY_INTERNAL_TURN_CANARY";
        let tmp = tempdir().unwrap();
        let settings = settings(tmp.path());
        let memory = MemoryStore::from_settings(&settings).unwrap();
        memory
            .messages
            .add(MessageRole::User, "visible question", None)
            .unwrap();
        persist_resumable_checkpoint(&memory, CANARY, "test_breaker").unwrap();
        memory
            .messages
            .add(
                MessageRole::User,
                "internal wake",
                Some(raw_metadata_value(
                    MessageVisibility::Internal,
                    RawMessageKind::Wake,
                    "wake",
                )),
            )
            .unwrap();
        memory
            .messages
            .add(MessageRole::Assistant, LEGACY_CANARY, None)
            .unwrap();
        memory
            .messages
            .add(MessageRole::User, "visible follow-up", None)
            .unwrap();
        memory
            .messages
            .add(MessageRole::Assistant, "visible answer", None)
            .unwrap();

        let recent = memory.messages.get_recent(10).unwrap();
        let context = recent_user_context_from_messages(&recent, 5);

        assert!(context.contains("visible question"));
        assert!(context.contains("visible follow-up"));
        assert!(context.contains("visible answer"));
        assert!(!context.contains(CANARY));
        assert!(!context.contains(LEGACY_CANARY));
    }

    #[test]
    fn ephemeral_history_override_is_removed_before_metadata_persistence() {
        let metadata = metadata_for_persistence(Some(json!({
            "lethe_message_kind": "chat",
            (HISTORY_CONTENT_KEY): "sanitized history",
        })))
        .unwrap();

        assert!(metadata.get(HISTORY_CONTENT_KEY).is_none());
        assert_eq!(metadata["lethe_message_kind"], "chat");
    }

    #[test]
    fn ordinary_history_rows_receive_explicit_visible_chat_metadata() {
        let user_metadata = metadata_for_persistence(None).unwrap();
        let parsed = MessageMetadata::from_value(Some(&user_metadata));
        assert!(!parsed.is_internal());
        assert_eq!(parsed.kind, Some(MessageKind::Chat));

        let tmp = tempdir().unwrap();
        let settings = settings(tmp.path());
        let memory = MemoryStore::from_settings(&settings).unwrap();
        persist_completed_ordinary_turn(&memory, "natural reply", None, None).unwrap();

        let stored = memory.messages.get_recent(5).unwrap();
        assert_eq!(stored.len(), 1);
        let parsed = MessageMetadata::from_value(Some(&stored[0].metadata));
        assert!(!parsed.is_internal());
        assert_eq!(parsed.kind, Some(MessageKind::Chat));
    }

    #[test]
    fn checkpoint_persists_without_a_duplicate_safe_notice() {
        const CANARY: &str = "SECRET_CHECKPOINT_CANARY";
        let tmp = tempdir().unwrap();
        let settings = settings(tmp.path());
        let memory = MemoryStore::from_settings(&settings).unwrap();
        memory
            .messages
            .add(MessageRole::User, "research this", None)
            .unwrap();

        // This is the path used after a Telegram tool already delivered a
        // visible message. The checkpoint still has to survive for resume.
        persist_resumable_checkpoint(&memory, CANARY, "test_breaker").unwrap();

        let stored = memory.messages.get_recent(10).unwrap();
        assert_eq!(stored.len(), 2);
        assert_eq!(stored[1].content, CANARY);
        let metadata = MessageMetadata::from_value(Some(&stored[1].metadata));
        assert!(metadata.is_internal());
        assert_eq!(
            raw_message_kind(Some(&stored[1].metadata)),
            Some(RawMessageKind::Checkpoint)
        );
    }

    #[test]
    fn prepare_turn_preserves_a_proactive_message_before_the_first_user_reply() {
        let tmp = tempdir().unwrap();
        let settings = settings(tmp.path());
        let memory = MemoryStore::from_settings(&settings).unwrap();
        let prompts = PromptStore::new(&settings.paths.workspace_dir, &settings.paths.config_dir);
        let proactive = "How was your meeting with Katie?";

        memory
            .messages
            .add(
                MessageRole::Assistant,
                proactive,
                Some(metadata_value(
                    MessageVisibility::UserVisible,
                    MessageKind::Proactive,
                    "brainstem",
                )),
            )
            .unwrap();

        let turn = prepare_turn(
            &settings,
            &memory,
            &prompts,
            "Your meeting?",
            Vec::new(),
            None,
            &AgentOptions {
                use_hippocampus: false,
                ..Default::default()
            },
        )
        .unwrap();
        let conversational = turn
            .messages
            .iter()
            .filter(|message| message.role != LlmRole::System)
            .collect::<Vec<_>>();

        assert_eq!(conversational.len(), 2);
        assert_eq!(conversational[0].role, LlmRole::User);
        assert!(conversational[0].content.contains("you initiated"));
        assert!(conversational[0].content.contains(proactive));
        assert_eq!(conversational[1].role, LlmRole::User);
        assert!(conversational[1].content.ends_with("Your meeting?"));

        // If the first inbound turn was persisted but failed before producing
        // an assistant row, its next retry/follow-up must retain the proactive
        // message that user was replying to.
        memory
            .messages
            .add(MessageRole::User, "Your meeting?", None)
            .unwrap();
        let retry = prepare_turn(
            &settings,
            &memory,
            &prompts,
            "I mean the question you just asked.",
            Vec::new(),
            None,
            &AgentOptions {
                use_hippocampus: false,
                ..Default::default()
            },
        )
        .unwrap();
        assert!(
            retry
                .messages
                .iter()
                .any(|message| message.content.contains(proactive)),
            "a failed first reply must not sever the proactive context"
        );
    }

    #[test]
    fn assistant_history_content_normalizes_message_envelope() {
        let normalized = assistant_history_content(
            r#"{"messages":["doing pretty well","I have thoughts when you have a sec"]}"#,
        );

        assert_eq!(
            normalized,
            "doing pretty well\n\nI have thoughts when you have a sec"
        );
    }

    #[test]
    fn telegram_tool_delivery_suppresses_the_undelivered_final_history_row() {
        let guard = Arc::new(Mutex::new(
            crate::interfaces::telegram::TelegramTurnGuard::new(),
        ));

        assert!(final_response_should_be_persisted(Some(&guard)).unwrap());
        guard.lock().unwrap().record_visible_message();
        assert!(!final_response_should_be_persisted(Some(&guard)).unwrap());
        assert!(final_response_should_be_persisted(None).unwrap());
    }

    #[test]
    fn internal_metadata_turn_skips_recall() {
        let tmp = tempdir().unwrap();
        let settings = settings(tmp.path());
        let memory = MemoryStore::from_settings(&settings).unwrap();
        let prompts = PromptStore::new(&settings.paths.workspace_dir, &settings.paths.config_dir);
        let metadata = metadata_value(
            MessageVisibility::Internal,
            MessageKind::Heartbeat,
            "heartbeat",
        );

        let turn = prepare_turn(
            &settings,
            &memory,
            &prompts,
            "heartbeat prompt",
            Vec::new(),
            Some(&metadata),
            &AgentOptions::default(),
        )
        .unwrap();

        assert!(turn.synthetic);
        assert!(turn.recall.is_none());
        // The recall block has both an outer <runtime_context source="hippocampus"...>
        // attribute and an inner <recall_block source="hippocampus"> tag. The
        // outer attribute name can now appear in static instruction prose, so
        // we look for the inner tag which only appears when recall actually
        // renders.
        assert!(
            !turn.messages[0]
                .content
                .contains("<recall_block source=\"hippocampus\">")
        );
    }

    #[tokio::test]
    async fn agent_prepare_turn_includes_principal_actor_context_when_enabled() {
        let tmp = tempdir().unwrap();
        let settings = settings(tmp.path());
        let agent = Agent::from_settings(settings).unwrap();

        let turn = agent
            .prepare_turn(&TurnRequest::new("Please research this in parallel"))
            .await
            .unwrap();

        assert!(agent.actor_registry().is_some());
        assert!(agent.principal_actor_id().is_some());
        let system = system_content(&turn.messages);
        assert!(system.contains("<actor_context>"));
        assert!(system.contains("runtime role: cortex"));
        assert!(system.contains("<available_on_request>"));
    }

    #[tokio::test]
    async fn agent_prepare_turn_isolates_reactions_from_actor_and_tool_context() {
        let tmp = tempdir().unwrap();
        let settings = settings(tmp.path());
        let agent = Agent::from_settings(settings).unwrap();
        let metadata = metadata_value(
            MessageVisibility::UserVisible,
            MessageKind::TelegramReaction,
            "telegram",
        );

        let turn = agent
            .prepare_turn(&TurnRequest::new("The user reacted with 👍").with_metadata(metadata))
            .await
            .unwrap();

        let system = system_content(&turn.messages);
        assert!(!system.contains("<actor_context>"));
        assert!(!system.contains("<available_on_request>"));
    }

    #[tokio::test]
    async fn agent_prepare_turn_omits_actor_context_when_disabled() {
        let tmp = tempdir().unwrap();
        let mut settings = settings(tmp.path());
        settings.background.actors_enabled = false;
        let agent = Agent::from_settings(settings).unwrap();

        let turn = agent
            .prepare_turn(&TurnRequest::new("Handle this directly"))
            .await
            .unwrap();

        assert!(agent.actor_registry().is_none());
        assert!(!turn.messages[0].content.contains("<actor_context>"));
    }

    #[test]
    fn hosted_safe_agent_constrains_default_turn_runtime() {
        let tmp = tempdir().unwrap();
        let mut settings = settings(tmp.path());
        settings.background.actors_enabled = false;
        let memory = Arc::new(MemoryStore::from_settings(&settings).unwrap());
        let agent =
            Agent::from_settings_with_memory_policy(settings, memory, ToolPolicy::HostedSafe)
                .unwrap();

        let request = TurnRequest::new("stay inside the hosted boundary");
        assert_eq!(request.runtime.policy, ToolPolicy::Full);

        let runtime = agent.with_actor_runtime(request.runtime);
        assert_eq!(runtime.policy, ToolPolicy::HostedSafe);

        let registry = crate::tools::registry::ToolRegistry::with_runtime(
            agent.memory(),
            agent.settings.paths.workspace_dir.clone(),
            agent.settings.paths.cache_dir.clone(),
            &agent.shell,
            runtime,
        );
        let names = registry
            .tools()
            .into_iter()
            .map(|tool| tool.name.to_string())
            .collect::<HashSet<_>>();
        assert!(names.contains("read_file"));
        assert!(!names.contains("bash"));
    }

    #[test]
    fn agent_reconfigures_router_models() {
        let tmp = tempdir().unwrap();
        let agent = Agent::from_settings(settings(tmp.path())).unwrap();

        let changed = agent
            .reconfigure_models(Some("main-next"), Some("aux-next"), Some("deep-next"))
            .unwrap();
        let config = agent.router_config().unwrap();

        assert_eq!(config.model, "main-next");
        assert_eq!(config.aux_model, "aux-next");
        assert_eq!(config.deep_model, "deep-next");
        assert_eq!(changed["model"]["old"], "test-model");
        assert_eq!(changed["model_aux"]["new"], "aux-next");
        assert_eq!(changed["model_deep"]["new"], "deep-next");

        // An explicit empty string clears the deep slot; a `None` leaves it.
        let cleared = agent.reconfigure_models(None, None, Some("")).unwrap();
        assert_eq!(cleared["model_deep"]["new"], "");
        assert_eq!(agent.router_config().unwrap().deep_model, "");
    }

    #[test]
    fn image_view_payload_is_stripped_and_injected_as_binary_message() {
        let payload = json!({
            "status": "ok",
            "message": "Viewing image",
            "_image_view": {
                "path": "/tmp/image.png",
                "mime_type": "image/png",
                "data": "aGVsbG8=",
                "name": "image.png"
            }
        })
        .to_string();

        let (tool_response, images) = extract_image_views(payload);
        assert!(!tool_response.contains("_image_view"));
        assert_eq!(images.len(), 1);

        let message = image_view_message(images[0].clone());
        assert_eq!(message.content.parts().len(), 2);
        assert!(message.content.parts()[1].is_image());
    }

    #[test]
    fn actor_turn_instruction_tracks_first_turn_and_inbox() {
        let spec = ActorRunSpec {
            actor_id: "a1".to_string(),
            name: "worker".to_string(),
            system_prompt: "system".to_string(),
            turn_number: 1,
            max_turns: 3,
            model: ModelTier::Aux,
            has_pending_messages: true,
            requested_tools: vec![],
            completion_delivery: ActorCompletionDelivery::ParentMessage,
        };

        let first = actor_turn_instruction(&spec);
        assert!(first.contains("Begin your actor task"));
        assert!(first.contains("pending inbox"));
        assert!(first.contains("turn 1/3"));

        let later_spec = ActorRunSpec {
            turn_number: 2,
            has_pending_messages: false,
            ..spec
        };
        let later = actor_turn_instruction(&later_spec);
        assert!(later.contains("Continue your actor task"));
        assert!(later.contains("send_message"));
        assert!(later.contains("turn 2/3"));

        let poll_only = actor_turn_instruction(&ActorRunSpec {
            completion_delivery: ActorCompletionDelivery::PollOnly,
            ..later_spec
        });
        assert!(poll_only.contains("caller is polling"));
        assert!(!poll_only.contains("send_message"));
        assert!(poll_only.contains("Do not send messages to your parent"));
    }

    fn system_message(content: &str) -> LlmMessage {
        LlmMessage {
            role: LlmRole::System,
            content: content.to_string(),
            attachments: vec![],
            tool_calls: vec![],
            tool_responses: vec![],
            cache_control: None,
        }
    }

    /// The OpenRouter → Claude cache path, end to end: dialect → markers →
    /// genai request. This crosses the seam that actually broke: the dialect
    /// silently returned "no marker" while the adapter stood ready to emit one,
    /// so every turn re-billed the full prompt. Testing either layer alone
    /// misses that, which is why this test spans both.
    #[test]
    fn openrouter_claude_carries_cache_control_into_the_genai_request() {
        let mut messages = vec![
            system_message("identity + persona + instructions"),
            system_message("clock + memory state + recall"),
            LlmMessage {
                role: LlmRole::User,
                content: "hi".to_string(),
                attachments: vec![],
                tool_calls: vec![],
                tool_responses: vec![],
                cache_control: None,
            },
        ];

        let dialect = dialect_for_model("openrouter/anthropic/claude-opus-4.7");
        apply_cache_markers(&mut messages, dialect.as_ref());

        let request = crate::llm::build_chat_request(messages);
        let marker = |index: usize| {
            request.messages[index]
                .options
                .as_ref()
                .and_then(|options| options.cache_control.clone())
        };

        assert_eq!(
            marker(0),
            Some(genai::chat::CacheControl::Ephemeral1h),
            "the stable prefix must carry the 1h marker OpenRouter forwards to Anthropic"
        );
        assert_eq!(
            marker(1),
            Some(genai::chat::CacheControl::Ephemeral),
            "the volatile tail must carry the 5m marker"
        );
        assert_eq!(marker(2), None, "user messages carry no cache marker");
    }

    /// A vendor with automatic prefix caching must stay unmarked all the way
    /// through, not just at the dialect.
    #[test]
    fn openrouter_kimi_carries_no_cache_control_into_the_genai_request() {
        let mut messages = vec![
            system_message("identity + persona"),
            system_message("clock + recall"),
        ];

        let dialect = dialect_for_model("openrouter/moonshotai/kimi-k2.6");
        apply_cache_markers(&mut messages, dialect.as_ref());

        let request = crate::llm::build_chat_request(messages);
        for (index, message) in request.messages.iter().enumerate() {
            assert!(
                message
                    .options
                    .as_ref()
                    .and_then(|o| o.cache_control.as_ref())
                    .is_none(),
                "message {index} must stay unmarked — Moonshot caches automatically"
            );
        }
    }

    #[test]
    fn archive_old_images_strips_old_image_payloads() {
        let image_content = format!(
            r#"[{{\"type\":\"text\",\"text\":\"check this out\"}},{{\"type\":\"image_url\",\"image_url\":{{\"url\":\"data:image/jpeg;base64,{base64}\"}}}}]"#,
            base64 = "A".repeat(100_000)
        );
        let recent_content = format!(
            r#"[{{\"type\":\"text\",\"text\":\"new photo\"}},{{\"type\":\"image_url\",\"image_url\":{{\"url\":\"data:image/png;base64,{base64}\"}}}}]"#,
            base64 = "B".repeat(100_000)
        );
        let mut messages = vec![
            LlmMessage {
                role: LlmRole::System,
                content: "You are helpful.".to_string(),
                attachments: vec![],
                tool_calls: vec![],
                tool_responses: vec![],
                cache_control: None,
            },
            LlmMessage {
                role: LlmRole::User,
                content: image_content.clone(),
                attachments: vec![],
                tool_calls: vec![],
                tool_responses: vec![],
                cache_control: None,
            },
            LlmMessage {
                role: LlmRole::Assistant,
                content: "I see it!".to_string(),
                attachments: vec![],
                tool_calls: vec![],
                tool_responses: vec![],
                cache_control: None,
            },
            LlmMessage {
                role: LlmRole::User,
                content: "thanks".to_string(),
                attachments: vec![],
                tool_calls: vec![],
                tool_responses: vec![],
                cache_control: None,
            },
            LlmMessage {
                role: LlmRole::User,
                content: recent_content.clone(),
                attachments: vec![],
                tool_calls: vec![],
                tool_responses: vec![],
                cache_control: None,
            },
        ];

        let before_chars: usize = messages.iter().map(|m| m.content.chars().count()).sum();
        assert!(before_chars > 200_000, "should have large base64 payloads");

        archive_old_images(&mut messages);

        let after_chars: usize = messages.iter().map(|m| m.content.chars().count()).sum();
        // First user message should have its image stripped
        assert!(
            !messages[1].content.contains("base64,"),
            "old image should be archived: {}",
            &messages[1].content[..100.min(messages[1].content.len())]
        );
        // Recent user message should keep its image
        assert!(
            messages[4].content.contains("base64,"),
            "recent image should be preserved: {}",
            &messages[4].content[..100.min(messages[4].content.len())]
        );
        // Total chars should drop significantly (at least the old image)
        assert!(
            after_chars < before_chars,
            "archived images should reduce char count: {after_chars} vs {before_chars}"
        );
        // The old image should be gone
        assert!(
            messages[1].content.chars().count() < 1000,
            "old image msg should be small after archival"
        );
    }

    #[test]
    fn replace_base64_images_handles_non_json_content() {
        let content = "Look at this! data:image/png;base64,iVBORw0KG... and that's it.";
        let result = replace_base64_images_with_stubs(content);
        assert!(!result.contains("base64,"));
        assert!(result.contains("[image archived]"));

        // Plain text without images should be unchanged
        let plain = "Hello, no images here.";
        assert_eq!(replace_base64_images_with_stubs(plain), plain);
    }
}
