//! Single-user Linq transport with a durable inbox/outbox and scoped decisions.
//! Webhook acknowledgement means persisted, not completed or delivered. An
//! interrupted model turn is never replayed automatically; network delivery
//! retries reuse the same provider idempotency key.

use std::fs::OpenOptions;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Result, anyhow, bail};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::{Notify, watch};
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::agent::{Agent, TURN_CHECKPOINT_NOTICE, TurnRequest, TurnResult};
use crate::config::ImessageConfig;
use crate::interfaces::actions::{ActionStore, ApprovalRequest, ApprovalScope, ApprovalStatus};
use crate::interfaces::linq::{DeliveryUpdate, LinqClient, LinqError, PollEnvelope, WebhookEvent};
use crate::scheduler::brainstem::BrainstemHandle;
use crate::tools::actions::ActionToolContext;
use crate::tools::registry::{ClientToolContext, ToolRuntime};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Inbound {
    event_id: String,
    chat: String,
    sender: String,
    message_id: String,
    text: String,
    vote: Option<String>,
}

impl Inbound {
    fn scope(&self) -> ApprovalScope {
        ApprovalScope {
            owner: "linq".to_string(),
            chat: self.chat.clone(),
            actor: self.sender.clone(),
        }
    }
}

#[derive(Clone)]
struct DeliveryStore {
    path: PathBuf,
}

impl DeliveryStore {
    fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut options = OpenOptions::new();
        options.create(true).append(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        options.open(path)?;
        let store = Self { path: path.into() };
        store.connection()?.execute_batch(
            "CREATE TABLE IF NOT EXISTS linq_inbox (
                event_id TEXT PRIMARY KEY, payload TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'queued'
            );
            CREATE TABLE IF NOT EXISTS linq_outbox (
                id TEXT PRIMARY KEY, chat TEXT NOT NULL, sender TEXT NOT NULL,
                reply_to TEXT, payload TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'queued', attempts INTEGER NOT NULL DEFAULT 0,
                retry_at INTEGER NOT NULL DEFAULT 0, provider_id TEXT, delivery_status TEXT,
                created_at INTEGER NOT NULL DEFAULT (unixepoch())
            );
            CREATE TABLE IF NOT EXISTS linq_choices (
                chat TEXT NOT NULL, sender TEXT NOT NULL, message_id TEXT NOT NULL,
                option_id TEXT NOT NULL, value TEXT NOT NULL, expires_at INTEGER NOT NULL,
                consumed INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY(chat, sender, message_id, option_id)
            );
            CREATE TABLE IF NOT EXISTS linq_sent_messages (
                outbox_id TEXT NOT NULL, chat TEXT NOT NULL, message_id TEXT NOT NULL,
                PRIMARY KEY(chat, message_id)
            );
            CREATE INDEX IF NOT EXISTS linq_sent_messages_outbox
                ON linq_sent_messages(outbox_id);
            CREATE TABLE IF NOT EXISTS linq_delivery_receipts (
                chat TEXT NOT NULL, message_id TEXT NOT NULL, status TEXT NOT NULL,
                event_id TEXT NOT NULL,
                PRIMARY KEY(chat, message_id)
            );
            CREATE TABLE IF NOT EXISTS linq_waiting_votes (
                event_id TEXT PRIMARY KEY, created_at INTEGER NOT NULL DEFAULT (unixepoch())
            );",
        )?;
        Ok(store)
    }

    fn connection(&self) -> Result<Connection> {
        let connection = Connection::open(&self.path)?;
        connection.busy_timeout(Duration::from_secs(5))?;
        Ok(connection)
    }

    fn enqueue(&self, inbound: &Inbound) -> Result<bool> {
        Ok(self.connection()?.execute(
            "INSERT OR IGNORE INTO linq_inbox(event_id,payload) VALUES (?1,?2)",
            params![inbound.event_id, serde_json::to_string(inbound)?],
        )? == 1)
    }

    fn next_inbound(&self) -> Result<Option<Inbound>> {
        let mut connection = self.connection()?;
        let transaction =
            connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        transaction.execute(
            "UPDATE linq_inbox SET status='ignored' WHERE status='waiting_vote' AND event_id IN
             (SELECT event_id FROM linq_waiting_votes WHERE created_at<?1)",
            [chrono::Utc::now().timestamp() - 3600],
        )?;
        transaction.execute(
            "DELETE FROM linq_waiting_votes WHERE event_id IN
             (SELECT event_id FROM linq_inbox WHERE status!='waiting_vote')",
            [],
        )?;
        let raw: Option<(String, String)> = transaction
            .query_row(
                "SELECT event_id,payload FROM linq_inbox WHERE status='queued' ORDER BY rowid LIMIT 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        let Some((id, payload)) = raw else {
            transaction.commit()?;
            return Ok(None);
        };
        transaction.execute(
            "UPDATE linq_inbox SET status='processing' WHERE event_id=?1",
            [&id],
        )?;
        transaction.commit()?;
        Ok(Some(serde_json::from_str(&payload)?))
    }

    fn finish(&self, id: &str, status: &str) -> Result<()> {
        self.connection()?.execute(
            "UPDATE linq_inbox SET status=?2 WHERE event_id=?1",
            params![id, status],
        )?;
        Ok(())
    }

    fn queue_output(&self, inbound: &Inbound, payload: Value) -> Result<String> {
        let id = Uuid::new_v4().to_string();
        self.connection()?.execute(
            "INSERT INTO linq_outbox(id,chat,sender,reply_to,payload) VALUES (?1,?2,?3,?4,?5)",
            params![
                id,
                inbound.chat,
                inbound.sender,
                (!inbound.message_id.is_empty()).then_some(&inbound.message_id),
                payload.to_string()
            ],
        )?;
        Ok(id)
    }

    fn record_sent(&self, outbox_id: &str, chat: &str, message_id: &str) -> Result<()> {
        let mut connection = self.connection()?;
        let transaction =
            connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        transaction.execute(
            "INSERT OR IGNORE INTO linq_sent_messages(outbox_id,chat,message_id)
             SELECT id,chat,?3 FROM linq_outbox WHERE id=?1 AND chat=?2",
            params![outbox_id, chat, message_id],
        )?;
        let owner: Option<String> = transaction
            .query_row(
                "SELECT outbox_id FROM linq_sent_messages WHERE chat=?1 AND message_id=?2",
                params![chat, message_id],
                |row| row.get(0),
            )
            .optional()?;
        if owner.as_deref() != Some(outbox_id) {
            bail!("Linq message does not belong to the queued output");
        }
        Self::refresh_delivery(&transaction, outbox_id)?;
        transaction.commit()?;
        Ok(())
    }

    fn record_receipt(&self, receipt: &DeliveryUpdate) -> Result<()> {
        if !matches!(
            receipt.status.as_str(),
            "sent" | "delivered" | "read" | "failed"
        ) {
            bail!("unsupported Linq delivery status");
        }
        let mut connection = self.connection()?;
        let transaction =
            connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        // Keep early receipts before the HTTP send result provides a mapping.
        // A failure beats sent, but cannot erase observed delivery or reading.
        transaction.execute(
            "INSERT INTO linq_delivery_receipts(chat,message_id,status,event_id)
             VALUES (?1,?2,?3,?4)
             ON CONFLICT(chat,message_id) DO UPDATE SET
                status=CASE
                    WHEN linq_delivery_receipts.status='read' OR excluded.status='read' THEN 'read'
                    WHEN linq_delivery_receipts.status='delivered' OR excluded.status='delivered' THEN 'delivered'
                    WHEN linq_delivery_receipts.status='failed' OR excluded.status='failed' THEN 'failed'
                    ELSE 'sent' END,
                event_id=excluded.event_id",
            params![receipt.chat_id, receipt.message_id, receipt.status, receipt.event_id],
        )?;
        let outbox_id: Option<String> = transaction
            .query_row(
                "SELECT outbox_id FROM linq_sent_messages WHERE chat=?1 AND message_id=?2",
                params![receipt.chat_id, receipt.message_id],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(outbox_id) = outbox_id {
            Self::refresh_delivery(&transaction, &outbox_id)?;
        }
        transaction.commit()?;
        Ok(())
    }

    fn refresh_delivery(connection: &Connection, outbox_id: &str) -> Result<()> {
        let (total, pending, failed, delivered, read): (i64, i64, i64, i64, i64) = connection
            .query_row(
                "SELECT COUNT(*),
                COALESCE(SUM(r.status IS NULL),0),
                COALESCE(SUM(r.status='failed'),0),
                COALESCE(SUM(r.status IN ('delivered','read')),0),
                COALESCE(SUM(r.status='read'),0)
             FROM linq_sent_messages s LEFT JOIN linq_delivery_receipts r
                ON r.chat=s.chat AND r.message_id=s.message_id
             WHERE s.outbox_id=?1",
                [outbox_id],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )?;
        let status = if total == 0 {
            None
        } else if failed > 0 {
            Some("failed")
        } else if pending > 0 {
            Some("pending")
        } else if read == total {
            Some("read")
        } else if delivered == total {
            Some("delivered")
        } else {
            Some("sent")
        };
        connection.execute(
            "UPDATE linq_outbox SET delivery_status=?2 WHERE id=?1",
            params![outbox_id, status],
        )?;
        Ok(())
    }

    fn recover(&self) -> Result<()> {
        let mut connection = self.connection()?;
        let transaction =
            connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let interrupted = {
            let mut statement =
                transaction.prepare("SELECT payload FROM linq_inbox WHERE status='processing'")?;
            statement
                .query_map([], |row| row.get::<_, String>(0))?
                .collect::<Result<Vec<_>, _>>()?
        };
        for raw in interrupted {
            let inbound: Inbound = serde_json::from_str(&raw)?;
            let payload = json!({
                "kind": "text",
                "content": "Lethe restarted during your task. I have not repeated it because an action may already have happened. Ask me to check the result; use /approvals to inspect pending decisions."
            });
            transaction.execute(
                "INSERT OR IGNORE INTO linq_outbox(id,chat,sender,reply_to,payload)
                 VALUES (?1,?2,?3,?4,?5)",
                params![
                    format!("recovery:{}", inbound.event_id),
                    inbound.chat,
                    inbound.sender,
                    inbound.message_id,
                    payload.to_string()
                ],
            )?;
            transaction.execute(
                "UPDATE linq_inbox SET status='interrupted' WHERE event_id=?1",
                [&inbound.event_id],
            )?;
        }
        transaction.commit()?;
        Ok(())
    }

    #[cfg(test)]
    fn take_choice(&self, inbound: &Inbound, option_id: &str, now: i64) -> Result<Option<String>> {
        Ok(match self.take_choice_or_defer(inbound, option_id, now)? {
            ChoiceOutcome::Ready(value) => Some(value),
            ChoiceOutcome::Unavailable | ChoiceOutcome::Deferred => None,
        })
    }

    fn take_choice_or_defer(
        &self,
        inbound: &Inbound,
        option_id: &str,
        now: i64,
    ) -> Result<ChoiceOutcome> {
        let mut connection = self.connection()?;
        let transaction =
            connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let value: Option<String> = transaction
            .query_row(
                "SELECT value FROM linq_choices WHERE chat=?1 AND sender=?2 AND message_id=?3
                 AND option_id=?4 AND consumed=0 AND expires_at>?5",
                params![
                    inbound.chat,
                    inbound.sender,
                    inbound.message_id,
                    option_id,
                    now
                ],
                |row| row.get(0),
            )
            .optional()?;
        let outcome = if let Some(value) = value {
            transaction.execute(
                "UPDATE linq_choices SET consumed=1 WHERE chat=?1 AND sender=?2 AND message_id=?3",
                params![inbound.chat, inbound.sender, inbound.message_id],
            )?;
            ChoiceOutcome::Ready(value)
        } else {
            let known: bool = transaction.query_row(
                "SELECT EXISTS(SELECT 1 FROM linq_choices WHERE chat=?1 AND sender=?2 AND message_id=?3)",
                params![inbound.chat, inbound.sender, inbound.message_id],
                |row| row.get(0),
            )?;
            if !known && transaction.execute(
                "UPDATE linq_inbox SET status='waiting_vote' WHERE event_id=?1 AND status='processing'",
                [&inbound.event_id],
            )? == 1 {
                transaction.execute(
                    "INSERT OR IGNORE INTO linq_waiting_votes(event_id) VALUES (?1)",
                    [&inbound.event_id],
                )?;
                ChoiceOutcome::Deferred
            } else {
                ChoiceOutcome::Unavailable
            }
        };
        transaction.commit()?;
        Ok(outcome)
    }

    fn record_poll_choices(
        &self,
        chat: &str,
        sender: &str,
        envelope: &PollEnvelope,
        options: &[String],
        values: &[String],
        expires_at: i64,
    ) -> Result<()> {
        if envelope.chat_id != chat || options.len() != values.len() {
            bail!("poll options do not match their saved decisions");
        }
        let mut connection = self.connection()?;
        let transaction =
            connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        for option in &envelope.poll.options {
            let Some(index) = options.iter().position(|text| text == &option.text) else {
                bail!("poll option does not match a saved decision");
            };
            transaction.execute(
                "INSERT OR IGNORE INTO linq_choices(chat,sender,message_id,option_id,value,expires_at)
                 VALUES (?1,?2,?3,?4,?5,?6)",
                params![chat, sender, envelope.message_id, option.option_id, values[index], expires_at],
            )?;
        }
        transaction.execute(
            "DELETE FROM linq_waiting_votes WHERE event_id IN
             (SELECT event_id FROM linq_inbox WHERE status='waiting_vote'
              AND json_extract(payload,'$.chat')=?1 AND json_extract(payload,'$.sender')=?2
              AND json_extract(payload,'$.message_id')=?3)",
            params![chat, sender, envelope.message_id],
        )?;
        transaction.execute(
            "UPDATE linq_inbox SET status='queued' WHERE status='waiting_vote'
             AND json_extract(payload,'$.chat')=?1 AND json_extract(payload,'$.sender')=?2
             AND json_extract(payload,'$.message_id')=?3",
            params![chat, sender, envelope.message_id],
        )?;
        transaction.commit()?;
        Ok(())
    }
}

#[derive(Debug, PartialEq)]
enum ChoiceOutcome {
    Ready(String),
    Unavailable,
    Deferred,
}

#[derive(Debug, PartialEq)]
enum ProcessOutcome {
    Complete,
    Deferred,
}

pub struct ImessageTransport {
    config: ImessageConfig,
    client: LinqClient,
    store: DeliveryStore,
    actions: ActionStore,
    inbox_notify: Notify,
    outbox_notify: Notify,
    active: Mutex<Option<ActiveTurn>>,
}

struct ActiveTurn {
    event_id: String,
    scope: ApprovalScope,
    cancel: watch::Sender<bool>,
}

pub struct ImessageTasks(Vec<JoinHandle<()>>);

impl Drop for ImessageTasks {
    fn drop(&mut self) {
        for task in &self.0 {
            task.abort();
        }
    }
}

impl ImessageTransport {
    pub fn new(config: ImessageConfig, state_dir: &Path) -> Result<Self> {
        config.validate().map_err(|error| anyhow!(error))?;
        let store = DeliveryStore::open(&state_dir.join("linq.sqlite"))?;
        store.connection()?.execute_batch(
            "CREATE TABLE IF NOT EXISTS linq_notification_target (
                singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                chat TEXT NOT NULL, sender TEXT NOT NULL
            );",
        )?;
        Ok(Self {
            client: LinqClient::new(config.api_token.clone())?,
            config,
            store,
            actions: ActionStore::open(state_dir.join("actions.sqlite"))?,
            inbox_notify: Notify::new(),
            outbox_notify: Notify::new(),
            active: Mutex::new(None),
        })
    }

    pub fn signing_secret(&self) -> &str {
        &self.config.webhook_secret
    }

    pub fn accept(&self, event: WebhookEvent) -> Result<bool> {
        let inbound = match event {
            WebhookEvent::Message(message) => {
                if message.is_group || !self.config.allowed_senders.contains(&message.sender) {
                    return Ok(false);
                }
                if message.text.len() > 32 * 1024 {
                    bail!("iMessage input exceeds 32 KiB");
                }
                Inbound {
                    event_id: message.event_id,
                    chat: message.chat_id,
                    sender: message.sender,
                    message_id: message.message_id,
                    text: if message.text.is_empty() && message.attachment_count > 0 {
                        "/unsupported_attachment".to_string()
                    } else {
                        message.text
                    },
                    vote: None,
                }
            }
            WebhookEvent::PollVote(vote) => {
                if vote.is_group
                    || !vote.added
                    || !self.config.allowed_senders.contains(&vote.sender)
                {
                    return Ok(false);
                }
                Inbound {
                    event_id: vote.event_id,
                    chat: vote.chat_id,
                    sender: vote.sender,
                    message_id: vote.message_id,
                    text: String::new(),
                    vote: Some(vote.option_id),
                }
            }
            WebhookEvent::Ignored { .. } => return Ok(false),
            WebhookEvent::Delivery(update) => {
                self.store.record_receipt(&update)?;
                return Ok(false);
            }
        };
        // Hold registration's lock before publishing control commands to the
        // inbox, so the worker cannot claim a command while its handler runs.
        let control =
            if notification_command(&inbound.text).is_some() || inbound.text.trim() == "/cancel" {
                Some(
                    self.active
                        .lock()
                        .map_err(|_| anyhow!("iMessage turn lock unavailable"))?,
                )
            } else {
                None
            };
        let accepted = self.store.enqueue(&inbound)?;
        if accepted {
            if let Some(enabled) = notification_command(&inbound.text) {
                self.set_notifications(&inbound, enabled)?;
                self.store.finish(&inbound.event_id, "done")?;
                return Ok(true);
            }
            if inbound.text.trim() == "/cancel" {
                // The same lock surrounds inbox claim + turn registration. A
                // cancellation cannot slip between those two operations.
                self.store.connection()?.execute(
                    "UPDATE linq_inbox SET status='cancelled' WHERE status IN ('queued','waiting_vote')
                     AND json_extract(payload,'$.chat')=?1
                     AND json_extract(payload,'$.sender')=?2",
                    params![inbound.chat, inbound.sender],
                )?;
                let running = control
                    .as_deref()
                    .and_then(Option::as_ref)
                    .filter(|turn| turn.scope == inbound.scope());
                if let Some(turn) = running {
                    let _ = turn.cancel.send(true);
                }
                self.queue(&inbound, json!({"kind": "text", "content": "Stopped the current iMessage turn and cleared queued messages in this conversation. An external action may already have happened; check its result before retrying. Independently running workers continue."}));
                return Ok(true);
            }
            self.inbox_notify.notify_one();
        }
        Ok(accepted)
    }

    pub fn status(&self) -> Result<Value> {
        let connection = self.store.connection()?;
        let count = |table: &str, status: &str| -> Result<i64> {
            Ok(connection.query_row(
                &format!("SELECT COUNT(*) FROM {table} WHERE status=?1"),
                [status],
                |row| row.get(0),
            )?)
        };
        let delivery_count = |states: &str| -> Result<i64> {
            Ok(connection.query_row(
                &format!(
                    "SELECT COUNT(*) FROM linq_outbox WHERE status='accepted'
                    AND EXISTS (SELECT 1 FROM linq_sent_messages s WHERE s.outbox_id=linq_outbox.id)
                    AND ({states})"
                ),
                [],
                |row| row.get(0),
            )?)
        };
        Ok(json!({
            "enabled": true,
            "native_polls": self.config.native_polls,
            "allowed_sender_count": self.config.allowed_senders.len(),
            "queued_inputs": count("linq_inbox", "queued")?,
            "waiting_votes": count("linq_inbox", "waiting_vote")?,
            "skipped_inputs": count("linq_inbox", "skipped")?,
            "interrupted_inputs": count("linq_inbox", "interrupted")?,
            "queued_outputs": count("linq_outbox", "queued")?,
            "failed_outputs": count("linq_outbox", "failed")?,
            "provider_accepted_outputs": delivery_count("1=1")?,
            "awaiting_delivery_outputs": delivery_count("delivery_status IS NULL OR delivery_status IN ('pending','sent')")?,
            "failed_delivery_outputs": delivery_count("delivery_status='failed'")?,
            "device_delivered_outputs": delivery_count("delivery_status IN ('delivered','read')")?,
            "device_read_outputs": delivery_count("delivery_status='read'")?,
        }))
    }

    pub fn start(
        self: &Arc<Self>,
        agent: Arc<Agent>,
        secure_prompt: Option<crate::agent_id::secure_prompt::SecurePromptHub>,
        brainstem: Option<BrainstemHandle>,
    ) -> Result<ImessageTasks> {
        self.store.recover()?;
        let input = self.clone();
        let output = self.clone();
        let mut tasks = vec![
            tokio::spawn(async move { input.run_inbox(agent, secure_prompt).await }),
            tokio::spawn(async move { output.run_outbox().await }),
        ];
        if let Some(brainstem) = brainstem {
            let transport = self.clone();
            tasks.push(tokio::spawn(async move {
                transport.run_notifications(brainstem).await
            }));
        }
        Ok(ImessageTasks(tasks))
    }

    fn next_authorized_input(&self) -> Result<Option<Inbound>> {
        while let Some(inbound) = self.store.next_inbound()? {
            if self.config.allowed_senders.contains(&inbound.sender) {
                return Ok(Some(inbound));
            }
            self.store.finish(&inbound.event_id, "skipped")?;
        }
        Ok(None)
    }

    fn queue(&self, inbound: &Inbound, output: Value) -> bool {
        match self.store.queue_output(inbound, output) {
            Ok(_) => {
                self.outbox_notify.notify_one();
                true
            }
            Err(error) => {
                tracing::error!(error = %error, "iMessage output could not be persisted");
                false
            }
        }
    }

    async fn run_inbox(
        self: Arc<Self>,
        agent: Arc<Agent>,
        secure_prompt: Option<crate::agent_id::secure_prompt::SecurePromptHub>,
    ) {
        loop {
            let next = (|| -> Result<_> {
                let mut active = self
                    .active
                    .lock()
                    .map_err(|_| anyhow!("iMessage turn lock unavailable"))?;
                let Some(inbound) = self.next_authorized_input()? else {
                    return Ok(None);
                };
                let (cancel, receiver) = watch::channel(false);
                *active = Some(ActiveTurn {
                    event_id: inbound.event_id.clone(),
                    scope: inbound.scope(),
                    cancel,
                });
                Ok(Some((inbound, receiver)))
            })();
            match next {
                Ok(Some((inbound, mut cancelled))) => {
                    let result = tokio::select! {
                        biased;
                        _ = cancelled.changed() => None,
                        result = self.process(&agent, &inbound, secure_prompt.clone()) => Some(result),
                    };
                    if let Ok(mut active) = self.active.lock() {
                        if active
                            .as_ref()
                            .is_some_and(|turn| turn.event_id == inbound.event_id)
                        {
                            *active = None;
                        }
                    }
                    if result.is_none() {
                        let _ = self.store.finish(&inbound.event_id, "cancelled");
                        continue;
                    }
                    let result = result.expect("checked result");
                    if matches!(result, Ok(ProcessOutcome::Deferred)) {
                        continue;
                    }
                    if let Err(error) = &result {
                        tracing::warn!(error = %error, "iMessage turn failed");
                        self.queue(&inbound, json!({
                            "kind": "text", "content": "I couldn't complete that task. Ask me to check its state before repeating an action."
                        }));
                    }
                    let _ = self.store.finish(
                        &inbound.event_id,
                        if result.is_ok() { "done" } else { "failed" },
                    );
                }
                Ok(None) => {
                    tokio::select! {
                        _ = self.inbox_notify.notified() => {},
                        _ = tokio::time::sleep(Duration::from_secs(2)) => {},
                    }
                }
                Err(error) => {
                    tracing::error!(error = %error, "iMessage inbox unavailable");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }
        }
    }

    async fn process(
        self: &Arc<Self>,
        agent: &Agent,
        inbound: &Inbound,
        secure_prompt: Option<crate::agent_id::secure_prompt::SecurePromptHub>,
    ) -> Result<ProcessOutcome> {
        if !self.config.allowed_senders.contains(&inbound.sender) {
            return Ok(ProcessOutcome::Complete);
        }
        let scope = inbound.scope();
        let now = chrono::Utc::now().timestamp();
        let mut message = inbound.text.clone();
        if let Some(enabled) = notification_command(&message) {
            self.set_notifications(inbound, enabled)?;
            return Ok(ProcessOutcome::Complete);
        }
        if message == "/unsupported_attachment" {
            self.queue(inbound, json!({"kind": "text", "content": "This iMessage integration currently accepts text. Please describe the attachment in a message."}));
            return Ok(ProcessOutcome::Complete);
        }
        if message == "/approvals" {
            let pending = self.actions.list_active(&scope, now)?;
            if pending.is_empty() {
                self.queue(inbound, json!({"kind": "text", "content": "There are no pending approvals in this conversation."}));
            }
            for request in pending {
                self.queue(inbound, json!({"kind": "approval", "request": request}));
            }
            return Ok(ProcessOutcome::Complete);
        }
        let resume = message.strip_prefix("/resume ").and_then(|id| {
            self.actions
                .get(&scope, id, now)
                .ok()
                .filter(|request| request.status == ApprovalStatus::Approved)
        });
        if let Some(request) = &resume {
            message = approved_continuation(request);
        } else if message.starts_with("/resume ") {
            self.queue(inbound, json!({"kind": "text", "content": "That action is not approved, has expired, or was already consumed. Use /approvals to inspect active requests."}));
            return Ok(ProcessOutcome::Complete);
        }
        let choice = if let Some(option) = &inbound.vote {
            match self.store.take_choice_or_defer(inbound, option, now)? {
                ChoiceOutcome::Ready(value) => Some(value),
                ChoiceOutcome::Unavailable => None,
                ChoiceOutcome::Deferred => return Ok(ProcessOutcome::Deferred),
            }
        } else if let Some((approve, id)) = approval_command(&message) {
            match self.actions.get(&scope, id, now) {
                Ok(request) => Some(if approve {
                    request.approve_choice
                } else {
                    request.reject_choice
                }),
                Err(_) => {
                    self.queue(inbound, json!({"kind": "text", "content": "That approval is unavailable in this conversation. Use /approvals to see pending requests."}));
                    return Ok(ProcessOutcome::Complete);
                }
            }
        } else {
            None
        };
        if let Some(choice) = choice {
            if let Some(answer) = choice.strip_prefix("answer:") {
                message = format!("I chose: {answer}");
            } else {
                match self.actions.resolve_choice(&scope, &choice, now) {
                    Ok(request) if request.status == ApprovalStatus::Approved => {
                        message = approved_continuation(&request);
                    }
                    Ok(_) => {
                        self.queue(inbound, json!({"kind": "text", "content": "Rejected. I have not executed the proposed action."}));
                        return Ok(ProcessOutcome::Complete);
                    }
                    Err(_) => {
                        self.queue(inbound, json!({"kind": "text", "content": "That decision has expired or was already used. Use /approvals to inspect pending requests."}));
                        return Ok(ProcessOutcome::Complete);
                    }
                }
            }
        } else if inbound.vote.is_some() {
            return Ok(ProcessOutcome::Complete);
        }
        if message.trim().is_empty() {
            return Ok(ProcessOutcome::Complete);
        }
        let transport = self.clone();
        let target = inbound.clone();
        let client = ClientToolContext::new(0, None, move |event| {
            if event.event != "text"
                || event
                    .data
                    .get("reply_markup")
                    .is_some_and(|value| !value.is_null())
            {
                return false;
            }
            transport.queue(&target, json!({"kind": "text", "content": event.data["content"], "reply_markup": event.data["reply_markup"]}))
        });
        let transport = self.clone();
        let target = inbound.clone();
        let actions = ActionToolContext::new(self.actions.clone(), scope, move |event| {
            transport.queue(&target, event)
        });
        let runtime = ToolRuntime {
            client: Some(client),
            actions: Some(actions),
            secure_prompt,
            ..ToolRuntime::default()
        };
        let request = TurnRequest::new(message)
            .with_runtime(runtime)
            .with_metadata(json!({
                "lethe_source": "imessage", "linq_chat_id": inbound.chat,
                "linq_message_id": inbound.message_id,
            }));
        let result = tokio::time::timeout(
            Duration::from_secs(15 * 60),
            agent.chat_once_result(request),
        )
        .await??;
        let text = match result {
            TurnResult::Complete(text) => text,
            TurnResult::Checkpointed => TURN_CHECKPOINT_NOTICE.to_string(),
        };
        if !text.trim().is_empty() && !self.queue(inbound, json!({"kind": "text", "content": text}))
        {
            bail!("final iMessage output could not be persisted");
        }
        Ok(ProcessOutcome::Complete)
    }

    fn set_notifications(&self, inbound: &Inbound, enabled: bool) -> Result<()> {
        let connection = self.store.connection()?;
        let content = if enabled {
            connection.execute(
                "INSERT INTO linq_notification_target VALUES (1,?1,?2)
                 ON CONFLICT(singleton) DO UPDATE SET chat=excluded.chat,sender=excluded.sender",
                params![inbound.chat, inbound.sender],
            )?;
            "Reminders and reviewed background updates will go to this conversation. Use /notifications off to stop them."
        } else {
            connection.execute(
                "DELETE FROM linq_notification_target WHERE chat=?1 AND sender=?2",
                params![inbound.chat, inbound.sender],
            )?;
            "Notifications are off for this conversation."
        };
        if !self.queue(inbound, json!({"kind": "text", "content": content})) {
            bail!("notification acknowledgement could not be persisted");
        }
        Ok(())
    }

    fn notification_target(&self) -> Result<Option<Inbound>> {
        let target: Option<(String, String)> = self
            .store
            .connection()?
            .query_row(
                "SELECT chat,sender FROM linq_notification_target WHERE singleton=1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        Ok(target
            .filter(|(_, sender)| self.config.allowed_senders.contains(sender))
            .map(|(chat, sender)| Inbound {
                event_id: String::new(),
                chat,
                sender,
                message_id: String::new(),
                text: String::new(),
                vote: None,
            }))
    }

    async fn run_notifications(self: Arc<Self>, brainstem: BrainstemHandle) {
        let mut receiver = None;
        loop {
            // An unbound/disabled line must not count as a deliverable
            // Brainstem subscriber. Recheck persisted consent on each pass.
            let target = self.notification_target().ok().flatten();
            if target.is_none() {
                receiver = None;
                tokio::time::sleep(Duration::from_secs(2)).await;
                continue;
            }
            let receiver = receiver.get_or_insert_with(|| brainstem.subscribe());
            tokio::select! {
                emission = receiver.recv() => match emission {
                    Ok(emission) => {
                        if let Ok(Some(current_target)) = self.notification_target() {
                            self.queue(&current_target, json!({"kind": "notification", "content": emission.message}));
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        tracing::warn!("iMessage background notification receiver lagged");
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
                },
                _ = tokio::time::sleep(Duration::from_secs(2)) => {},
            }
        }
    }

    async fn run_outbox(self: Arc<Self>) {
        loop {
            let result = self.deliver_next().await;
            match result {
                Ok(true) => {}
                Ok(false) => {
                    tokio::select! {
                        _ = self.outbox_notify.notified() => {},
                        _ = tokio::time::sleep(Duration::from_secs(2)) => {},
                    }
                }
                Err(error) => {
                    tracing::warn!(error = %error, "iMessage outbox unavailable");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }
        }
    }

    fn output_is_authorized(&self, chat: &str, sender: &str, output: &Value) -> Result<bool> {
        if !self
            .config
            .allowed_senders
            .iter()
            .any(|allowed| allowed == sender)
        {
            return Ok(false);
        }
        if output["kind"].as_str() == Some("notification") {
            return Ok(self
                .notification_target()?
                .is_some_and(|target| target.chat == chat && target.sender == sender));
        }
        Ok(true)
    }

    async fn deliver_next(&self) -> Result<bool> {
        self.store.connection()?.execute(
            "UPDATE linq_outbox SET status='failed' WHERE status='queued' AND created_at<?1",
            [chrono::Utc::now().timestamp() - 3600],
        )?;
        let row: Option<(String, String, String, Option<String>, String, i64)> = self
            .store
            .connection()?
            .query_row(
                "SELECT id,chat,sender,reply_to,payload,attempts FROM linq_outbox
             WHERE status='queued' AND retry_at<=?1 ORDER BY rowid LIMIT 1",
                [chrono::Utc::now().timestamp()],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                    ))
                },
            )
            .optional()?;
        let Some((id, chat, sender, reply_to, payload, attempts)) = row else {
            return Ok(false);
        };
        let output: Value = serde_json::from_str(&payload)?;
        if !self.output_is_authorized(&chat, &sender, &output)? {
            self.store
                .connection()?
                .execute("UPDATE linq_outbox SET status='skipped' WHERE id=?1", [&id])?;
            return Ok(true);
        }
        let result = self
            .deliver(&id, &chat, &sender, reply_to.as_deref(), &output)
            .await;
        let connection = self.store.connection()?;
        match result {
            Ok(provider_id) => {
                connection.execute(
                    "UPDATE linq_outbox SET status=?3,provider_id=?2 WHERE id=?1",
                    params![
                        id,
                        provider_id,
                        if provider_id.is_empty() {
                            "skipped"
                        } else {
                            "accepted"
                        }
                    ],
                )?;
            }
            Err(error) => {
                let attempts = attempts + 1;
                let status = if attempts >= 8 { "failed" } else { "queued" };
                connection.execute(
                    "UPDATE linq_outbox SET status=?2,attempts=?3,retry_at=?4 WHERE id=?1",
                    params![
                        id,
                        status,
                        attempts,
                        chrono::Utc::now().timestamp() + (1_i64 << attempts.min(8))
                    ],
                )?;
                tracing::warn!(attempts, "iMessage send was not accepted: {error}");
            }
        }
        Ok(true)
    }

    async fn deliver(
        &self,
        id: &str,
        chat: &str,
        sender: &str,
        reply_to: Option<&str>,
        output: &Value,
    ) -> Result<String> {
        let (text, options, values, expires_at) = match output["kind"].as_str() {
            Some("approval") => {
                let request: ApprovalRequest = serde_json::from_value(output["request"].clone())?;
                if request.expires_at <= chrono::Utc::now().timestamp() {
                    return Ok(String::new());
                }
                if request.status == ApprovalStatus::Approved {
                    let text = format!(
                        "Approved, awaiting continuation: {}\nReply /resume {} to continue after checking the terms. This approval expires automatically.",
                        request.summary, request.request_id
                    );
                    let sent = self
                        .client
                        .send_text(chat, &text, reply_to, &format!("lethe:{id}:text:0"))
                        .await?;
                    self.store.record_sent(id, chat, &sent.message_id)?;
                    return Ok(sent.message_id);
                }
                let text = format!(
                    "Approval required: {}\nReply /approve {} or /reject {}. This request expires automatically.",
                    request.summary, request.request_id, request.request_id
                );
                (
                    text,
                    vec!["Approve".to_string(), "Reject".to_string()],
                    vec![request.approve_choice, request.reject_choice],
                    request.expires_at,
                )
            }
            Some("choices") => {
                let options: Vec<String> = serde_json::from_value(output["options"].clone())?;
                let text = format!(
                    "{}\n{}\nReply with your choice.",
                    output["question"].as_str().unwrap_or("Choose:"),
                    options.join(" / ")
                );
                let values = options
                    .iter()
                    .map(|option| format!("answer:{option}"))
                    .collect();
                let created_at: i64 = self.store.connection()?.query_row(
                    "SELECT created_at FROM linq_outbox WHERE id=?1",
                    [id],
                    |row| row.get(0),
                )?;
                (text, options, values, created_at + 900)
            }
            _ => (
                output["content"].as_str().unwrap_or("").to_string(),
                Vec::new(),
                Vec::new(),
                0,
            ),
        };
        let mut provider_id = String::new();
        for (index, chunk) in text_chunks(&text, 3000).iter().enumerate() {
            let sent = self
                .client
                .send_text(chat, chunk, reply_to, &format!("lethe:{id}:text:{index}"))
                .await?;
            self.store.record_sent(id, chat, &sent.message_id)?;
            provider_id = sent.message_id;
        }
        if self.config.native_polls && !options.is_empty() {
            match self
                .client
                .create_poll(chat, &options, &format!("lethe:{id}:poll"))
                .await
            {
                Ok(envelope) => {
                    self.store.record_sent(id, chat, &envelope.message_id)?;
                    self.store.record_poll_choices(
                        chat, sender, &envelope, &options, &values, expires_at,
                    )?;
                    self.inbox_notify.notify_one();
                }
                Err(error) if retryable_poll_error(&error) => return Err(error.into()),
                Err(error) => tracing::warn!(
                    "iMessage poll unavailable; text controls remain usable: {error}"
                ),
            }
        }
        Ok(provider_id)
    }
}

fn approved_continuation(request: &ApprovalRequest) -> String {
    format!(
        "I approved request {}: {}. Recheck that its material terms still match, then continue with execute_approved_action using this request_id. Do not replace its saved arguments.",
        request.request_id, request.summary
    )
}

fn retryable_poll_error(error: &LinqError) -> bool {
    match error {
        LinqError::Http(_) | LinqError::InvalidResponse => true,
        LinqError::ApiStatus(status) => *status == 408 || *status == 429 || *status >= 500,
        _ => false,
    }
}

fn notification_command(text: &str) -> Option<bool> {
    match text.trim() {
        "/notifications on" => Some(true),
        "/notifications off" => Some(false),
        _ => None,
    }
}

fn approval_command(text: &str) -> Option<(bool, &str)> {
    let (command, id) = text.trim().split_once(' ')?;
    if Uuid::parse_str(id).is_err() {
        return None;
    }
    match command {
        "/approve" => Some((true, id)),
        "/reject" => Some((false, id)),
        _ => None,
    }
}

fn text_chunks(text: &str, limit: usize) -> Vec<String> {
    text.chars()
        .collect::<Vec<_>>()
        .chunks(limit)
        .map(|chunk| chunk.iter().collect())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn transport(directory: &Path) -> ImessageTransport {
        ImessageTransport::new(
            ImessageConfig {
                enabled: true,
                api_token: "test-token".into(),
                webhook_secret: "test-secret".into(),
                allowed_senders: vec!["+49123456789".into()],
                native_polls: false,
            },
            directory,
        )
        .unwrap()
    }

    fn command_event(id: &str, sender: &str, command: &str) -> WebhookEvent {
        WebhookEvent::Message(crate::interfaces::linq::IncomingMessage {
            event_id: id.into(),
            chat_id: "chat-1".into(),
            message_id: "message-1".into(),
            sender: sender.into(),
            text: command.into(),
            service: "iMessage".into(),
            is_group: false,
            reply_to: None,
            attachment_count: 0,
        })
    }

    fn native_vote(id: &str) -> WebhookEvent {
        WebhookEvent::PollVote(crate::interfaces::linq::IncomingPollVote {
            event_id: id.into(),
            chat_id: "chat-1".into(),
            message_id: "poll-1".into(),
            sender: "+49123456789".into(),
            option_id: "approve-1".into(),
            added: true,
            service: "iMessage".into(),
            is_group: false,
        })
    }

    fn map_poll(store: &DeliveryStore, expires_at: i64) {
        let envelope = PollEnvelope {
            chat_id: "chat-1".into(),
            message_id: "poll-1".into(),
            poll: crate::interfaces::linq::Poll {
                options: vec![
                    crate::interfaces::linq::PollOption {
                        option_id: "approve-1".into(),
                        text: "Approve".into(),
                    },
                    crate::interfaces::linq::PollOption {
                        option_id: "reject-1".into(),
                        text: "Reject".into(),
                    },
                ],
            },
        };
        store
            .record_poll_choices(
                "chat-1",
                "+49123456789",
                &envelope,
                &["Approve".into(), "Reject".into()],
                &["opaque-approve".into(), "opaque-reject".into()],
                expires_at,
            )
            .unwrap();
    }

    #[test]
    fn early_native_votes_wait_durably_for_poll_mapping_and_consume_once() {
        let directory = tempfile::tempdir().unwrap();
        let original = transport(directory.path());
        original.accept(native_vote("early-vote")).unwrap();
        let vote = original.next_authorized_input().unwrap().unwrap();
        assert_eq!(
            original
                .store
                .take_choice_or_defer(&vote, "approve-1", 10)
                .unwrap(),
            ChoiceOutcome::Deferred
        );
        assert_eq!(original.status().unwrap()["waiting_votes"], 1);
        drop(original);
        let restored = transport(directory.path());
        assert!(restored.next_authorized_input().unwrap().is_none());
        map_poll(&restored.store, 100);
        let vote = restored.next_authorized_input().unwrap().unwrap();
        assert_eq!(vote.event_id, "early-vote");
        assert_eq!(
            restored
                .store
                .take_choice_or_defer(&vote, "approve-1", 10)
                .unwrap(),
            ChoiceOutcome::Ready("opaque-approve".into())
        );
        restored.store.finish(&vote.event_id, "done").unwrap();
        restored.accept(native_vote("duplicate-choice")).unwrap();
        let vote = restored.next_authorized_input().unwrap().unwrap();
        assert_eq!(
            restored
                .store
                .take_choice_or_defer(&vote, "approve-1", 10)
                .unwrap(),
            ChoiceOutcome::Unavailable
        );
        assert_eq!(restored.status().unwrap()["waiting_votes"], 0);
    }

    #[test]
    fn cancellation_prevents_waiting_votes_from_resuming_after_mapping() {
        let directory = tempfile::tempdir().unwrap();
        let transport = transport(directory.path());
        transport.accept(native_vote("early-vote")).unwrap();
        let vote = transport.next_authorized_input().unwrap().unwrap();
        assert_eq!(
            transport
                .store
                .take_choice_or_defer(&vote, "approve-1", 10)
                .unwrap(),
            ChoiceOutcome::Deferred
        );
        transport
            .accept(command_event("cancel", "+49123456789", "/cancel"))
            .unwrap();
        map_poll(&transport.store, 100);
        assert!(transport.next_authorized_input().unwrap().is_none());
        assert_eq!(transport.status().unwrap()["waiting_votes"], 0);
    }

    #[test]
    fn unmapped_votes_expire_and_known_expired_votes_do_not_wait_again() {
        let directory = tempfile::tempdir().unwrap();
        let transport = transport(directory.path());
        transport.accept(native_vote("old-vote")).unwrap();
        let vote = transport.next_authorized_input().unwrap().unwrap();
        assert_eq!(
            transport
                .store
                .take_choice_or_defer(&vote, "approve-1", 10)
                .unwrap(),
            ChoiceOutcome::Deferred
        );
        transport
            .store
            .connection()
            .unwrap()
            .execute(
                "UPDATE linq_waiting_votes SET created_at=?1",
                [chrono::Utc::now().timestamp() - 3601],
            )
            .unwrap();
        assert!(transport.next_authorized_input().unwrap().is_none());
        assert_eq!(transport.status().unwrap()["waiting_votes"], 0);
        map_poll(&transport.store, 10);
        transport.accept(native_vote("expired-choice")).unwrap();
        let vote = transport.next_authorized_input().unwrap().unwrap();
        assert_eq!(
            transport
                .store
                .take_choice_or_defer(&vote, "approve-1", 10)
                .unwrap(),
            ChoiceOutcome::Unavailable
        );
    }

    #[tokio::test]
    async fn revoked_queued_notifications_and_removed_recipients_skip_delivery() {
        let directory = tempfile::tempdir().unwrap();
        let mut transport = transport(directory.path());
        transport
            .accept(command_event("on", "+49123456789", "/notifications on"))
            .unwrap();
        transport
            .store
            .connection()
            .unwrap()
            .execute("UPDATE linq_outbox SET status='skipped'", [])
            .unwrap();
        let target = transport.notification_target().unwrap().unwrap();
        let id = transport
            .store
            .queue_output(
                &target,
                json!({"kind": "notification", "content": "reviewed"}),
            )
            .unwrap();
        transport
            .accept(command_event("off", "+49123456789", "/notifications off"))
            .unwrap();
        assert!(transport.deliver_next().await.unwrap());
        let status: String = transport
            .store
            .connection()
            .unwrap()
            .query_row("SELECT status FROM linq_outbox WHERE id=?1", [&id], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(status, "skipped");
        transport
            .store
            .connection()
            .unwrap()
            .execute("UPDATE linq_outbox SET status='skipped'", [])
            .unwrap();
        let id = transport
            .store
            .queue_output(&inbound(), json!({"kind": "text", "content": "old reply"}))
            .unwrap();
        transport.config.allowed_senders.clear();
        assert!(transport.deliver_next().await.unwrap());
        let status: String = transport
            .store
            .connection()
            .unwrap()
            .query_row("SELECT status FROM linq_outbox WHERE id=?1", [&id], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(status, "skipped");
        assert_eq!(transport.status().unwrap()["provider_accepted_outputs"], 0);
    }

    #[test]
    fn restart_skips_queued_commands_from_revoked_senders() {
        let directory = tempfile::tempdir().unwrap();
        let original = transport(directory.path());
        let mut command = inbound();
        command.text = "/notifications on".into();
        original.store.enqueue(&command).unwrap();
        let approval = Inbound {
            event_id: "approval-after-revocation".into(),
            text: "/approve 550e8400-e29b-41d4-a716-446655440000".into(),
            ..inbound()
        };
        original.store.enqueue(&approval).unwrap();
        drop(original);
        let mut restored = transport(directory.path());
        restored.config.allowed_senders.clear();
        assert!(restored.next_authorized_input().unwrap().is_none());
        assert!(restored.notification_target().unwrap().is_none());
        assert_eq!(restored.status().unwrap()["skipped_inputs"], 2);
        assert_eq!(restored.status().unwrap()["queued_outputs"], 0);
    }

    #[test]
    fn poll_retries_are_limited_to_transient_or_ambiguous_failures() {
        for status in [408, 429, 500, 503] {
            assert!(retryable_poll_error(&LinqError::ApiStatus(status)));
        }
        assert!(retryable_poll_error(&LinqError::InvalidResponse));
        for status in [307, 400, 401, 403, 404, 422] {
            assert!(!retryable_poll_error(&LinqError::ApiStatus(status)));
        }
        assert!(!retryable_poll_error(&LinqError::InvalidRequest("options")));
    }

    #[test]
    fn cancellation_is_immediate_scoped_and_deduplicated() {
        let directory = tempfile::tempdir().unwrap();
        let transport = transport(directory.path());
        let mut pending = inbound();
        transport.store.enqueue(&pending).unwrap();
        pending.event_id = "other-scope".into();
        pending.sender = "+49888888888".into();
        transport.store.enqueue(&pending).unwrap();
        let (cancel, receiver) = watch::channel(false);
        *transport.active.lock().unwrap() = Some(ActiveTurn {
            event_id: "running".into(),
            scope: inbound().scope(),
            cancel,
        });
        assert!(
            !transport
                .accept(command_event("wrong-sender", "+49888888888", "/cancel"))
                .unwrap()
        );
        assert!(!*receiver.borrow());
        assert!(
            transport
                .accept(command_event("cancel", "+49123456789", "/cancel"))
                .unwrap()
        );
        assert!(*receiver.borrow());
        assert!(
            !transport
                .accept(command_event("cancel", "+49123456789", "/cancel"))
                .unwrap()
        );
        let next = transport.store.next_inbound().unwrap().unwrap();
        assert_eq!(next.event_id, "other-scope");
        assert!(transport.store.next_inbound().unwrap().is_none());
        let notices: i64 = transport
            .store
            .connection()
            .unwrap()
            .query_row("SELECT COUNT(*) FROM linq_outbox", [], |row| row.get(0))
            .unwrap();
        assert_eq!(notices, 1);
    }

    #[test]
    fn notification_consent_survives_restart_but_not_allowlist_removal() {
        let directory = tempfile::tempdir().unwrap();
        let original = transport(directory.path());
        assert!(original.notification_target().unwrap().is_none());
        assert!(
            original
                .accept(command_event("on", "+49123456789", "/notifications on"))
                .unwrap()
        );
        assert!(original.store.next_inbound().unwrap().is_none());
        drop(original);
        let mut restored = transport(directory.path());
        let target = restored.notification_target().unwrap().unwrap();
        assert_eq!(target.scope(), inbound().scope());
        assert!(target.message_id.is_empty());
        assert!(
            !restored
                .accept(command_event(
                    "wrong-off",
                    "+49888888888",
                    "/notifications off"
                ))
                .unwrap()
        );
        assert!(restored.notification_target().unwrap().is_some());
        assert!(
            restored
                .accept(command_event("off", "+49123456789", "/notifications off"))
                .unwrap()
        );
        assert!(restored.notification_target().unwrap().is_none());
        assert!(
            restored
                .accept(command_event(
                    "back-on",
                    "+49123456789",
                    "/notifications on"
                ))
                .unwrap()
        );
        restored.config.allowed_senders.clear();
        assert!(restored.notification_target().unwrap().is_none());
    }

    #[tokio::test]
    async fn notification_loop_only_subscribes_after_consent_and_releases_on_stop() {
        let directory = tempfile::tempdir().unwrap();
        let transport = Arc::new(transport(directory.path()));
        let brainstem = BrainstemHandle::new();
        let task_transport = transport.clone();
        let task_brainstem = brainstem.clone();
        let task =
            tokio::spawn(async move { task_transport.run_notifications(task_brainstem).await });
        tokio::task::yield_now().await;
        assert_eq!(brainstem.subscriber_count(), 0);
        transport
            .store
            .connection()
            .unwrap()
            .execute(
                "INSERT INTO linq_notification_target VALUES (1,'chat-1','+49123456789')",
                [],
            )
            .unwrap();
        tokio::time::timeout(Duration::from_secs(3), async {
            while brainstem.subscriber_count() == 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(brainstem.subscriber_count(), 1);
        transport
            .store
            .connection()
            .unwrap()
            .execute("DELETE FROM linq_notification_target", [])
            .unwrap();
        tokio::time::timeout(Duration::from_secs(3), async {
            while brainstem.subscriber_count() > 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        task.abort();
        let _ = task.await;
        assert_eq!(brainstem.subscriber_count(), 0);
    }

    fn inbound() -> Inbound {
        Inbound {
            event_id: "event-1".to_string(),
            chat: "chat-1".to_string(),
            sender: "+49123456789".to_string(),
            message_id: "message-1".to_string(),
            text: "hello".to_string(),
            vote: None,
        }
    }

    #[test]
    fn inbox_deduplicates_and_does_not_replay_interrupted_turns() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("delivery.sqlite");
        let store = DeliveryStore::open(&path).unwrap();
        assert!(store.enqueue(&inbound()).unwrap());
        assert!(!store.enqueue(&inbound()).unwrap());
        assert!(store.next_inbound().unwrap().is_some());
        drop(store);
        let store = DeliveryStore::open(&path).unwrap();
        store.recover().unwrap();
        assert!(store.next_inbound().unwrap().is_none());
        let recovery_messages: i64 = store
            .connection()
            .unwrap()
            .query_row("SELECT COUNT(*) FROM linq_outbox", [], |row| row.get(0))
            .unwrap();
        assert_eq!(recovery_messages, 1);
    }

    #[test]
    fn choice_votes_are_bound_to_sender_and_consumed_once() {
        let directory = tempfile::tempdir().unwrap();
        let store = DeliveryStore::open(&directory.path().join("delivery.sqlite")).unwrap();
        store.connection().unwrap().execute(
            "INSERT INTO linq_choices VALUES ('chat-1','+49123456789','message-1','option-1','choice',100,0)", [],
        ).unwrap();
        let mut wrong_sender = inbound();
        wrong_sender.sender = "+49888888888".to_string();
        assert_eq!(
            store.take_choice(&wrong_sender, "option-1", 10).unwrap(),
            None
        );
        assert_eq!(
            store.take_choice(&inbound(), "option-1", 10).unwrap(),
            Some("choice".to_string())
        );
        assert_eq!(store.take_choice(&inbound(), "option-1", 10).unwrap(), None);
    }

    #[test]
    fn bare_yes_and_expired_votes_do_not_authorize_actions() {
        assert!(approval_command("yes").is_none());
        assert!(approval_command("/approve anything").is_none());
        let directory = tempfile::tempdir().unwrap();
        let store = DeliveryStore::open(&directory.path().join("delivery.sqlite")).unwrap();
        store.connection().unwrap().execute(
            "INSERT INTO linq_choices VALUES ('chat-1','+49123456789','message-1','option-1','choice',10,0)", [],
        ).unwrap();
        assert_eq!(store.take_choice(&inbound(), "option-1", 10).unwrap(), None);
    }

    #[test]
    fn unicode_messages_split_without_losing_content() {
        let text = "🎈hello".repeat(700);
        let chunks = text_chunks(&text, 3000);
        assert_eq!(chunks.concat(), text);
        assert!(chunks.iter().all(|chunk| chunk.chars().count() <= 3000));
    }

    fn receipt(message_id: &str, status: &str) -> DeliveryUpdate {
        DeliveryUpdate {
            event_id: format!("receipt:{message_id}:{status}"),
            chat_id: "chat-1".to_string(),
            message_id: message_id.to_string(),
            status: status.to_string(),
        }
    }

    fn output_delivery(store: &DeliveryStore, id: &str) -> Option<String> {
        store
            .connection()
            .unwrap()
            .query_row(
                "SELECT delivery_status FROM linq_outbox WHERE id=?1",
                [id],
                |row| row.get(0),
            )
            .unwrap()
    }

    #[test]
    fn retains_early_receipts_until_the_send_mapping_arrives() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("delivery.sqlite");
        let store = DeliveryStore::open(&path).unwrap();
        let id = store
            .queue_output(&inbound(), json!({"kind": "text", "content": "hello"}))
            .unwrap();
        store
            .record_receipt(&receipt("outbound-1", "read"))
            .unwrap();
        assert_eq!(output_delivery(&store, &id), None);
        drop(store);
        let store = DeliveryStore::open(&path).unwrap();
        store.record_sent(&id, "chat-1", "outbound-1").unwrap();
        store.record_sent(&id, "chat-1", "outbound-1").unwrap();
        assert_eq!(output_delivery(&store, &id).as_deref(), Some("read"));
        let wrong_chat = DeliveryUpdate {
            chat_id: "another-chat".into(),
            ..receipt("outbound-1", "failed")
        };
        store.record_receipt(&wrong_chat).unwrap();
        assert_eq!(output_delivery(&store, &id).as_deref(), Some("read"));
        let other = store
            .queue_output(&inbound(), json!({"kind": "text", "content": "other"}))
            .unwrap();
        assert!(store.record_sent(&other, "chat-1", "outbound-1").is_err());
    }

    #[test]
    fn delivery_progress_survives_duplicate_and_out_of_order_receipts() {
        let directory = tempfile::tempdir().unwrap();
        let store = DeliveryStore::open(&directory.path().join("delivery.sqlite")).unwrap();
        let id = store
            .queue_output(&inbound(), json!({"kind": "text", "content": "hello"}))
            .unwrap();
        store.record_sent(&id, "chat-1", "outbound-1").unwrap();
        for status in ["sent", "failed", "sent"] {
            store
                .record_receipt(&receipt("outbound-1", status))
                .unwrap();
        }
        assert_eq!(output_delivery(&store, &id).as_deref(), Some("failed"));
        for status in ["delivered", "failed", "sent"] {
            store
                .record_receipt(&receipt("outbound-1", status))
                .unwrap();
        }
        assert_eq!(output_delivery(&store, &id).as_deref(), Some("delivered"));
        for status in ["read", "delivered", "read", "sent", "failed"] {
            store
                .record_receipt(&receipt("outbound-1", status))
                .unwrap();
        }
        assert_eq!(output_delivery(&store, &id).as_deref(), Some("read"));
    }

    #[test]
    fn device_delivery_requires_every_text_chunk_and_poll_receipt() {
        let directory = tempfile::tempdir().unwrap();
        let transport = ImessageTransport::new(
            ImessageConfig {
                enabled: true,
                api_token: "test-token".into(),
                webhook_secret: "test-secret".into(),
                allowed_senders: vec![inbound().sender],
                native_polls: false,
            },
            directory.path(),
        )
        .unwrap();
        let store = &transport.store;
        let id = store
            .queue_output(&inbound(), json!({"kind": "text", "content": "hello"}))
            .unwrap();
        for message_id in ["chunk-1", "chunk-2", "poll-1"] {
            store.record_sent(&id, "chat-1", message_id).unwrap();
        }
        transport
            .accept(WebhookEvent::Delivery(receipt("chunk-1", "read")))
            .unwrap();
        transport
            .accept(WebhookEvent::Delivery(receipt("chunk-2", "delivered")))
            .unwrap();
        assert_eq!(output_delivery(store, &id).as_deref(), Some("pending"));
        assert_eq!(transport.status().unwrap()["device_delivered_outputs"], 0);
        store
            .connection()
            .unwrap()
            .execute(
                "UPDATE linq_outbox SET status='accepted' WHERE id=?1",
                [&id],
            )
            .unwrap();
        let status = transport.status().unwrap();
        assert_eq!(status["provider_accepted_outputs"], 1);
        assert_eq!(status["awaiting_delivery_outputs"], 1);
        assert_eq!(status["device_delivered_outputs"], 0);
        transport
            .accept(WebhookEvent::Delivery(receipt("poll-1", "delivered")))
            .unwrap();
        let status = transport.status().unwrap();
        assert_eq!(status["device_delivered_outputs"], 1);
        assert_eq!(status["device_read_outputs"], 0);
        transport
            .accept(WebhookEvent::Delivery(receipt("chunk-2", "read")))
            .unwrap();
        transport
            .accept(WebhookEvent::Delivery(receipt("poll-1", "read")))
            .unwrap();
        assert_eq!(transport.status().unwrap()["device_read_outputs"], 1);
        let unsent = store
            .queue_output(&inbound(), json!({"kind": "text", "content": "expired"}))
            .unwrap();
        store
            .connection()
            .unwrap()
            .execute(
                "UPDATE linq_outbox SET status='accepted' WHERE id=?1",
                [&unsent],
            )
            .unwrap();
        assert_eq!(transport.status().unwrap()["provider_accepted_outputs"], 1);
    }

    #[test]
    fn synthetic_notification_outputs_have_no_reply_anchor() {
        let directory = tempfile::tempdir().unwrap();
        let store = DeliveryStore::open(&directory.path().join("delivery.sqlite")).unwrap();
        let target = Inbound {
            message_id: String::new(),
            ..inbound()
        };
        let id = store
            .queue_output(&target, json!({"kind": "text", "content": "notification"}))
            .unwrap();
        let anchor: Option<String> = store
            .connection()
            .unwrap()
            .query_row(
                "SELECT reply_to FROM linq_outbox WHERE id=?1",
                [&id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(anchor, None);
    }
}
