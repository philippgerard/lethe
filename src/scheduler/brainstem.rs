//! Brainstem: the single source of periodic beats, urges, and proactive
//! emissions. Owns the heartbeat loop, the rate limiter, and the DMN
//! background pass. Transports (Telegram, HTTP/SSE API) are dumb
//! subscribers — they listen for `BrainstemEmission`s and forward each to
//! their own clients.
//!
//! This is deliberately the *only* place periodic agent activity lives.
//! Putting heartbeats inside transport loops leads to double-firing when
//! more than one transport runs in the same process, divergent
//! rate-limiter state, and a muddled mental model where transports do
//! brain-level work. Lethe's architecture (cortex / hippocampus /
//! brainstem / DMN) names this responsibility explicitly — `NotificationSource::Brainstem`
//! already exists in `actor/notification.rs` for these signals.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tokio::sync::broadcast;

use serde::{Deserialize, Serialize};

use crate::agent::{Agent, AgentOptions, TurnRequest};
use crate::config::Settings;
use crate::llm::prompts::PromptStore;
use crate::memory::message_metadata::{
    MessageKind, MessageVisibility, metadata_value as message_metadata_value,
};
use crate::memory::{MemoryStore, MessageRole};
use crate::scheduler::heartbeat::{Heartbeat, HeartbeatAction, HeartbeatConfig, HeartbeatState};
use crate::scheduler::proactive::{
    ActiveReminder, ProactiveOutbox, ProactiveRateLimiter, format_active_reminders,
};
use crate::todos::TodoFilter;
use crate::tools::registry::ToolRuntime;

const EMISSION_QUEUE_DEPTH: usize = 64;

/// A user-visible emission from the brainstem. Today this is just
/// proactive messages from the heartbeat; future kinds (urges,
/// reflections, status pulses) reuse the same channel so subscribers
/// don't have to grow.
#[derive(Clone, Debug)]
pub struct BrainstemEmission {
    pub kind: BrainstemEmissionKind,
    pub message: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BrainstemEmissionKind {
    Proactive,
}

/// Hand-out side of the brainstem: subscribers grab a receiver, the run
/// task feeds the broadcast. Cloneable — the run task and any number of
/// subscribers can share it cheaply.
#[derive(Clone, Debug)]
pub struct BrainstemHandle {
    sender: broadcast::Sender<BrainstemEmission>,
}

impl BrainstemHandle {
    pub fn new() -> Self {
        let (sender, _) = broadcast::channel(EMISSION_QUEUE_DEPTH);
        Self { sender }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<BrainstemEmission> {
        self.sender.subscribe()
    }

    #[cfg(test)]
    pub(crate) fn subscriber_count(&self) -> usize {
        self.sender.receiver_count()
    }
}

impl Default for BrainstemHandle {
    fn default() -> Self {
        Self::new()
    }
}

/// Main brainstem loop. Wakes on the configured heartbeat interval,
/// trains the agent on the heartbeat prompt, and broadcasts any
/// `Send`-action outcome that the rate limiter permits. Returns when
/// the broadcast loses all subscribers and the channel closes, or on
/// agent error.
pub async fn run(
    agent: Arc<Agent>,
    settings: Settings,
    options: AgentOptions,
    handle: BrainstemHandle,
) -> Result<()> {
    let mut heartbeat = Heartbeat::new(HeartbeatConfig::from_settings(&settings));
    if !heartbeat.config().enabled {
        // Heartbeat disabled in settings — Brainstem still exists for
        // future urge kinds, but the loop is dormant.
        std::future::pending::<()>().await;
        return Ok(());
    }
    let mut limiter = ProactiveRateLimiter::from_settings(&settings);
    let mut outbox = ProactiveOutbox::default();
    let mut interval = tokio::time::interval(Duration::from_secs(
        heartbeat.config().interval_seconds.max(1),
    ));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        interval.tick().await;
        if let Err(error) = tick(
            &agent,
            &settings,
            &options,
            &mut heartbeat,
            &mut limiter,
            &mut outbox,
            &handle,
        )
        .await
        {
            tracing::warn!(error = ?error, "brainstem heartbeat tick failed");
        }
    }
}

/// One-shot manual trigger. Runs a single brainstem tick on demand
/// (e.g. the Telegram `/heartbeat` command) and returns the proactive
/// message it produced, if any. Uses a fresh local handle so the caller
/// gets the result back synchronously without needing to be the main
/// brainstem subscriber.
pub async fn trigger_once(
    agent: &Agent,
    settings: &Settings,
    options: &AgentOptions,
) -> Result<Option<String>> {
    let mut heartbeat = Heartbeat::new(HeartbeatConfig::from_settings(settings));
    let mut limiter = ProactiveRateLimiter::from_settings(settings);
    let mut outbox = ProactiveOutbox::default();
    let handle = BrainstemHandle::new();
    let mut rx = handle.subscribe();
    tick(
        agent,
        settings,
        options,
        &mut heartbeat,
        &mut limiter,
        &mut outbox,
        &handle,
    )
    .await?;
    match rx.try_recv() {
        Ok(BrainstemEmission { message, .. }) => Ok(Some(message)),
        Err(_) => Ok(None),
    }
}

/// True when this tick can skip the LLM round-trip entirely. A tick is only
/// idle when there is nothing to act on: no due reminders AND no unfinished
/// work (subagents mid-task, blocked actors, in-progress/overdue todos).
/// Before open-work awareness, a Blocked subagent could sit invisible for
/// days because the gate only looked at reminders.
fn is_idle_tick(
    first_tick: bool,
    use_full_context: bool,
    reminders: &str,
    open_work: &str,
) -> bool {
    !first_tick && !use_full_context && reminders.trim().is_empty() && open_work.trim().is_empty()
}

/// The persistable state a host-driven beat loop threads through
/// [`beat`]. The resident [`run`] loop keeps it on its stack; a multi-tenant
/// host serializes it between beats (config fields inside the limiter are
/// overwritten from settings on restore via [`BeatState::restored`]).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BeatState {
    pub heartbeat: HeartbeatState,
    pub limiter: ProactiveRateLimiter,
    pub outbox: ProactiveOutbox,
}

impl BeatState {
    pub fn new(settings: &Settings) -> Self {
        Self {
            heartbeat: HeartbeatState::default(),
            limiter: ProactiveRateLimiter::from_settings(settings),
            outbox: ProactiveOutbox::default(),
        }
    }

    /// Rebuild from persisted state, re-deriving every config-like field from
    /// current settings so tuning changes apply without state migrations.
    pub fn restored(mut self, settings: &Settings) -> Self {
        let fresh = ProactiveRateLimiter::from_settings(settings);
        self.limiter.max_per_day = fresh.max_per_day;
        self.limiter.cooldown_seconds = fresh.cooldown_seconds;
        self
    }
}

/// What one beat decided. `messages` are user-facing texts the caller must
/// deliver (rate-limit accounting and conversation history are already
/// handled inside [`beat`]); `idle` means the LLM round-trip was skipped;
/// `open_work` reports whether unfinished subagents/todos existed at beat
/// time, which hosts use to keep beating for quiet-but-busy agents.
#[derive(Clone, Debug, Default)]
pub struct BeatOutcome {
    pub messages: Vec<String>,
    pub idle: bool,
    pub open_work: bool,
}

/// Run one brainstem beat with caller-owned state: the cortex heartbeat turn,
/// the DMN/curator background pass, the notification review gate, and the
/// proactive rate limiter/outbox. This is the hosting seam — the resident
/// [`run`] loop and one-shot [`trigger_once`] are thin wrappers over it.
///
/// `runtime` is attached to the cortex heartbeat turn; a hosted multiplexer
/// passes its per-tenant `ToolRuntime` (policy, observer, secure prompt,
/// agent-id state) so the heartbeat turn runs under the exact same boundary
/// as user turns. Standalone callers pass `ToolRuntime::default()`.
///
/// Every message returned in [`BeatOutcome::messages`] has already been
/// recorded in conversation history as a user-visible assistant message —
/// the agent remembers what it proactively told the user — and counted
/// against the rate limiter. Callers only deliver.
pub async fn beat(
    agent: &Agent,
    settings: &Settings,
    options: &AgentOptions,
    runtime: ToolRuntime,
    state: &mut BeatState,
) -> Result<BeatOutcome> {
    beat_with_delivery(agent, settings, options, runtime, state, None).await
}

/// Internal form used by the resident loop. A host-driven beat has an
/// implicit delivery target (its caller), while the resident loop must only
/// commit a proactive message when at least one live transport accepted the
/// broadcast. Keeping the target explicit preserves that publication boundary
/// without giving up the serializable host-driven state machine.
async fn beat_with_delivery(
    agent: &Agent,
    settings: &Settings,
    options: &AgentOptions,
    runtime: ToolRuntime,
    state: &mut BeatState,
    broadcast: Option<&BrainstemHandle>,
) -> Result<BeatOutcome> {
    let mut heartbeat = Heartbeat::with_state(
        HeartbeatConfig::from_settings(settings),
        state.heartbeat.clone(),
    );
    let mut outcome = BeatOutcome::default();

    // A previously rate-limited proactive message gets first claim on this
    // beat's send budget — flushed even when the beat itself goes idle.
    // Peek first: the resident loop keeps the original item queued until a
    // live subscriber accepts it, whereas a host-driven caller is itself the
    // delivery boundary.
    if let Some(deferred) = state.outbox.peek_ready(&mut state.limiter)
        && deliver(
            agent,
            &mut state.limiter,
            &mut outcome.messages,
            &deferred,
            broadcast,
        )?
    {
        state.outbox.clear();
    }

    let prompts = PromptStore::new(&settings.paths.workspace_dir, &settings.paths.config_dir);
    let reminders = active_reminders(agent.memory(), settings)?;
    let open_work = agent.open_work_digest().await;
    outcome.open_work = !open_work.trim().is_empty();
    let prompt = heartbeat.trigger(&prompts, &reminders, &open_work);

    if is_idle_tick(
        prompt.first_tick,
        prompt.use_full_context,
        &reminders,
        &open_work,
    ) {
        heartbeat.finish_response(r#"{"action":"idle","message":""}"#, None);
        state.heartbeat = heartbeat.state();
        outcome.idle = true;
        return Ok(outcome);
    }

    let result = agent
        .chat_once_result(
            TurnRequest::new(&prompt.message)
                .with_metadata(message_metadata_value(
                    MessageVisibility::Internal,
                    MessageKind::Heartbeat,
                    "brainstem",
                ))
                .with_runtime(runtime)
                .with_options(options.clone()),
        )
        .await?;
    let response = result.into_complete_text().unwrap_or_default();
    let heartbeat_outcome = heartbeat.finish_response(&response, None);
    state.heartbeat = heartbeat.state();
    // Queue the DMN reflection + curator pass, then drain gated subagent/DMN
    // `user_notify` signals. The two-stage gate (heuristic + aux-LLM review)
    // has already filtered them; survivors are user-facing by definition.
    let background = agent
        .process_background_heartbeat(&prompt.message, &reminders)
        .await?;
    for notification in background.user_messages() {
        let delivered = deliver(
            agent,
            &mut state.limiter,
            &mut outcome.messages,
            &notification,
            broadcast,
        );
        match delivered {
            Ok(true) => {}
            Ok(false) => state.outbox.defer(notification),
            Err(error) => {
                state.outbox.defer(notification);
                return Err(error);
            }
        }
    }

    if heartbeat_outcome.action == HeartbeatAction::Send {
        let trimmed = heartbeat_outcome.message.trim();
        if !trimmed.is_empty() {
            if state.limiter.allowed() {
                match deliver(
                    agent,
                    &mut state.limiter,
                    &mut outcome.messages,
                    trimmed,
                    broadcast,
                ) {
                    Ok(true) => {}
                    Ok(false) => state.outbox.defer(trimmed),
                    Err(error) => {
                        state.outbox.defer(trimmed);
                        return Err(error);
                    }
                }
            } else {
                // Rate-limited, not silenced: hold the message for a later
                // beat instead of discarding the heartbeat's judgement.
                tracing::info!("proactive send rate-limited — deferring to outbox");
                state.outbox.defer(trimmed);
            }
        }
    }
    Ok(outcome)
}

/// Count a delivery against the rate limiter and record it in conversation
/// history so the agent's next prompt includes what it told the user.
fn deliver(
    agent: &Agent,
    limiter: &mut ProactiveRateLimiter,
    messages: &mut Vec<String>,
    message: &str,
    broadcast: Option<&BrainstemHandle>,
) -> Result<bool> {
    if let Some(handle) = broadcast {
        let published = emit_proactive(agent.memory(), handle, limiter, message)?;
        if published {
            messages.push(message.to_string());
        }
        return Ok(published);
    }

    agent.memory().messages.add(
        MessageRole::Assistant,
        message,
        Some(message_metadata_value(
            MessageVisibility::UserVisible,
            MessageKind::Proactive,
            "brainstem",
        )),
    )?;
    limiter.record();
    messages.push(message.to_string());
    Ok(true)
}

#[allow(clippy::too_many_arguments)]
async fn tick(
    agent: &Agent,
    settings: &Settings,
    options: &AgentOptions,
    heartbeat: &mut Heartbeat,
    limiter: &mut ProactiveRateLimiter,
    outbox: &mut ProactiveOutbox,
    handle: &BrainstemHandle,
) -> Result<()> {
    let mut state = BeatState {
        heartbeat: heartbeat.state(),
        limiter: limiter.clone(),
        outbox: outbox.clone(),
    };
    let result = beat_with_delivery(
        agent,
        settings,
        options,
        ToolRuntime::default(),
        &mut state,
        Some(handle),
    )
    .await;
    *heartbeat = Heartbeat::with_state(HeartbeatConfig::from_settings(settings), state.heartbeat);
    *limiter = state.limiter;
    *outbox = state.outbox;
    result.map(|_| ())
}

/// Persist a user-visible proactive message before publishing it to any
/// transport. A fast Telegram reply can otherwise start its turn after delivery
/// but before the assistant message exists in history, leaving the model without
/// the question the user is answering.
///
/// The Brainstem owns this write rather than its subscribers: API and Telegram
/// can both receive the same emission in a combined runtime, but the shared
/// conversation stream must contain it exactly once.
///
/// The durable boundary is successful publication to at least one Brainstem
/// subscriber. Downstream transport acknowledgement is intentionally separate:
/// one subscriber can fail after another has already delivered the same event.
fn emit_proactive(
    memory: &MemoryStore,
    handle: &BrainstemHandle,
    limiter: &mut ProactiveRateLimiter,
    message: &str,
) -> Result<bool> {
    // Do not create a conversation entry that no live transport could receive.
    // `send` can still lose its final receiver in the small race below; that
    // case is rolled back.
    if handle.sender.receiver_count() == 0 {
        return Ok(false);
    }

    let message_id = memory.messages.add(
        MessageRole::Assistant,
        message,
        Some(message_metadata_value(
            MessageVisibility::UserVisible,
            MessageKind::Proactive,
            "brainstem",
        )),
    )?;
    let emission = BrainstemEmission {
        kind: BrainstemEmissionKind::Proactive,
        message: message.to_string(),
    };

    if handle.sender.send(emission).is_ok() {
        limiter.record();
        return Ok(true);
    }

    // The last receiver disappeared between the count and the send. Keep
    // history aligned with what was actually published and let the caller
    // re-defer rather than leaving a ghost assistant message.
    if let Err(error) = memory.messages.delete(&message_id) {
        tracing::warn!(
            message_id,
            error = %error,
            "failed to roll back undelivered proactive history entry"
        );
    }
    Ok(false)
}

fn active_reminders(memory: &crate::memory::MemoryStore, settings: &Settings) -> Result<String> {
    if settings.hosted_plugins.replace_local_todos {
        return Ok(String::new());
    }
    // Read through the agent's injected memory store — building a store from
    // settings here would silently point hosted deployments (whose todos live
    // behind an injected backend) at an empty local database.
    let todos = memory.todos.list(TodoFilter {
        include_completed: false,
        limit: 20,
        ..Default::default()
    })?;
    let reminders = todos
        .into_iter()
        .map(|todo| ActiveReminder {
            title: todo.title,
            priority: todo.priority.as_str().to_string(),
            due: todo.due_date,
        })
        .collect::<Vec<_>>();
    Ok(format_active_reminders(&reminders, 10))
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use crate::agent::{AgentOptions, prepare_turn};
    use crate::config;
    use crate::memory::message_metadata::MessageMetadata;

    use super::*;

    #[test]
    fn hosted_todo_replacement_suppresses_local_reminders() {
        let mut settings =
            crate::config::test_settings(std::path::Path::new("/tmp/lethe-hosted-reminder-test"));
        settings.hosted_plugins.replace_local_todos = true;
        let memory = crate::memory::MemoryStore::from_settings(&settings).unwrap();
        assert_eq!(active_reminders(&memory, &settings).unwrap(), "");
    }

    #[test]
    fn beat_state_roundtrips_and_rederives_limiter_config() {
        let mut settings =
            crate::config::test_settings(std::path::Path::new("/tmp/lethe-beat-state-test"));
        settings.background.proactive_max_per_day = 4;
        settings.background.proactive_cooldown_minutes = 60;
        let mut state = BeatState::new(&settings);
        state.limiter.record();
        state.outbox.defer("held insight");
        state.heartbeat.heartbeat_count = 7;

        let json = serde_json::to_value(&state).unwrap();
        let restored: BeatState = serde_json::from_value(json).unwrap();
        // A config change between beats applies on restore without touching
        // the dynamic parts (send history, deferred message, beat counter).
        settings.background.proactive_max_per_day = 9;
        let restored = restored.restored(&settings);

        assert_eq!(restored.limiter.max_per_day, 9);
        assert_eq!(restored.limiter.send_count(), 1);
        assert!(!restored.outbox.is_empty());
        assert_eq!(restored.heartbeat.heartbeat_count, 7);
    }

    #[test]
    fn host_driven_delivery_preserves_proactive_history_marker() {
        let tmp = tempdir().unwrap();
        let settings = config::test_settings(tmp.path());
        let agent = Agent::from_settings(settings).unwrap();
        let mut limiter = ProactiveRateLimiter::new(4, 0);
        let mut messages = Vec::new();

        assert!(
            deliver(
                &agent,
                &mut limiter,
                &mut messages,
                "A hosted proactive update",
                None,
            )
            .unwrap()
        );

        assert_eq!(messages, vec!["A hosted proactive update"]);
        assert_eq!(limiter.send_count(), 1);
        let stored = agent.memory().messages.get_recent(1).unwrap();
        let metadata = MessageMetadata::from_value(Some(&stored[0].metadata));
        assert!(!metadata.is_internal());
        assert_eq!(metadata.kind, Some(MessageKind::Proactive));
    }

    #[test]
    fn idle_gate_yields_to_open_work() {
        // The historical behavior: nothing due, not first tick → skip.
        assert!(is_idle_tick(false, false, "", ""));
        assert!(is_idle_tick(false, false, "  \n", "  "));

        // Any unfinished work defeats the gate, even with no reminders.
        // This is the fix for Blocked subagents sitting invisible for days.
        assert!(!is_idle_tick(
            false,
            false,
            "",
            "- subagent 'researcher' (task=blocked) — BLOCKED, needs attention"
        ));
        assert!(!is_idle_tick(false, false, "", "- todo #3 [in_progress]"));

        // Reminders, first tick, and the deep review still defeat it too.
        assert!(!is_idle_tick(false, false, "- [high] Submit report", ""));
        assert!(!is_idle_tick(true, false, "", ""));
        assert!(!is_idle_tick(false, true, "", ""));
    }

    #[test]
    fn proactive_emission_is_in_the_next_turn_context_before_delivery() {
        let tmp = tempdir().unwrap();
        let settings = config::test_settings(tmp.path());
        let memory = MemoryStore::from_settings(&settings).unwrap();
        let prompts = PromptStore::new(&settings.paths.workspace_dir, &settings.paths.config_dir);
        memory
            .messages
            .add(MessageRole::User, "How is Katie?", None)
            .unwrap();
        memory
            .messages
            .add(MessageRole::Assistant, "We meet on Monday.", None)
            .unwrap();

        let handle = BrainstemHandle::new();
        let mut telegram_receiver = handle.subscribe();
        let mut api_receiver = handle.subscribe();
        let mut limiter = ProactiveRateLimiter::new(4, 0);
        let proactive = "How was your meeting with Katie?";

        assert!(emit_proactive(&memory, &handle, &mut limiter, proactive).unwrap());

        let stored = memory.messages.get_recent(10).unwrap();
        let persisted = stored.last().expect("persisted proactive message");
        assert_eq!(persisted.role, MessageRole::Assistant);
        assert_eq!(persisted.content, proactive);
        let metadata = MessageMetadata::from_value(Some(&persisted.metadata));
        assert!(!metadata.is_internal());
        assert_eq!(metadata.kind, Some(MessageKind::Proactive));

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
        assert!(
            turn.messages
                .iter()
                .any(|message| message.content == proactive),
            "the next user turn must see the proactive assistant message"
        );

        let telegram_delivery = telegram_receiver.try_recv().expect("telegram emission");
        let api_delivery = api_receiver.try_recv().expect("api emission");
        assert_eq!(telegram_delivery.message, proactive);
        assert_eq!(api_delivery.message, proactive);
        assert_eq!(
            memory
                .messages
                .get_recent(10)
                .unwrap()
                .iter()
                .filter(|message| message.content == proactive)
                .count(),
            1,
            "multiple transport subscribers must not duplicate history"
        );
        assert_eq!(limiter.send_count(), 1);
    }

    #[test]
    fn proactive_emission_without_a_subscriber_does_not_create_ghost_history() {
        let tmp = tempdir().unwrap();
        let settings = config::test_settings(tmp.path());
        let memory = MemoryStore::from_settings(&settings).unwrap();
        let handle = BrainstemHandle::new();
        let mut limiter = ProactiveRateLimiter::new(4, 0);
        let mut outbox = ProactiveOutbox::default();

        let message = "No transport can receive this";
        assert!(!emit_proactive(&memory, &handle, &mut limiter, message).unwrap());
        outbox.defer(message);
        assert!(memory.messages.get_recent(10).unwrap().is_empty());
        assert_eq!(limiter.send_count(), 0);
        assert!(
            !outbox.is_empty(),
            "an unpublished message must remain queued for a later tick"
        );

        let mut receiver = handle.subscribe();
        let queued = outbox
            .peek_ready(&mut limiter)
            .expect("queued message becomes ready");
        assert!(emit_proactive(&memory, &handle, &mut limiter, &queued).unwrap());
        outbox.clear();
        assert!(outbox.is_empty());
        assert_eq!(
            receiver.try_recv().expect("deferred emission").message,
            "No transport can receive this"
        );
        assert_eq!(memory.messages.get_recent(10).unwrap().len(), 1);
        assert_eq!(limiter.send_count(), 1);
    }
}
