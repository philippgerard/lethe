use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

use chrono::{DateTime, Utc};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use thiserror::Error;
use uuid::Uuid;

use super::codec::{ensure_parent, f32_slice_as_bytes, open_conn, parent_dir, semantic_score};
use super::message_metadata::{
    MESSAGE_KIND_KEY, MessageKind, MessageMetadata, SOURCE_KEY, VISIBILITY_KEY,
};
use super::search::{indent_block, query_terms, search_result_text};
use super::semantic::{EmbeddingEngine, LEGACY_EMBEDDING_DIMENSIONS};

const TABLE_NAME: &str = "message_history";
const VEC_TABLE_NAME: &str = "message_history_vec";
const MIGRATION_TABLE_NAME: &str = "message_history_migrations";
const SEARCH_RESULT_MAX_LINES: usize = 50;
const LEGACY_WAKE_QUARANTINE_VERSION: i64 = 1;
const LEGACY_WAKE_QUARANTINE_NAME: &str = "legacy_wake_forced_wrap_quarantine";
const LEGACY_WAKE_QUARANTINE_SOURCE: &str = "legacy_wake_quarantine_v1";
const LEGACY_WAKE_ORIGINAL_METADATA_KEY: &str = "lethe_quarantine_original_metadata";
const LEGACY_WAKE_QUARANTINE_VERSION_KEY: &str = "lethe_quarantine_version";
const LEGACY_WAKE_QUARANTINE_REASON_KEY: &str = "lethe_quarantine_reason";
const LEGACY_WAKE_QUARANTINE_REASON: &str = "legacy_untyped_wake_forced_wrap";
const LEGACY_INTERNAL_DESCENDANT_SOURCE: &str = "legacy_internal_turn_quarantine_v1";
const LEGACY_INTERNAL_DESCENDANT_REASON: &str = "legacy_untyped_internal_turn_descendant";

#[derive(Debug, Error)]
pub enum MessageHistoryError {
    #[error("message role is required")]
    EmptyRole,
    #[error("message metadata must be a JSON object")]
    InvalidMetadata,
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Sqlite(#[from] rusqlite::Error),
    #[error(transparent)]
    Embedding(#[from] anyhow::Error),
    #[error("storage backend error: {0}")]
    Backend(String),
}

pub type MessageHistoryResult<T> = Result<T, MessageHistoryError>;

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", untagged)]
pub enum MessageRole {
    User,
    Assistant,
    Tool,
    System,
    /// Anything the persisted store contains that doesn't map to the known
    /// roles above. Round-tripped verbatim through the database.
    Other(String),
}

impl MessageRole {
    pub fn parse(value: &str) -> Self {
        match value.trim() {
            "user" => Self::User,
            "assistant" => Self::Assistant,
            "tool" => Self::Tool,
            "system" => Self::System,
            other => Self::Other(other.to_string()),
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            Self::User => "user",
            Self::Assistant => "assistant",
            Self::Tool => "tool",
            Self::System => "system",
            Self::Other(value) => value.as_str(),
        }
    }

    pub fn is_user(&self) -> bool {
        matches!(self, Self::User)
    }

    pub fn is_assistant(&self) -> bool {
        matches!(self, Self::Assistant)
    }

    pub fn is_tool(&self) -> bool {
        matches!(self, Self::Tool)
    }
}

impl std::fmt::Display for MessageRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StoredMessage {
    pub id: String,
    pub role: MessageRole,
    pub content: String,
    pub metadata: Value,
    pub created_at: String,
    pub score: f64,
}

#[derive(Clone, Debug)]
pub struct MessageHistory {
    data_path: PathBuf,
    embedder: EmbeddingEngine,
}

#[derive(Debug)]
struct LegacyMessageRow {
    id: String,
    role: MessageRole,
    content: String,
    metadata_raw: String,
    metadata: Option<Value>,
}

impl MessageHistory {
    pub fn open(data_path: impl Into<PathBuf>) -> MessageHistoryResult<Self> {
        let data_path = data_path.into();
        let embedder = EmbeddingEngine::from_env(parent_dir(&data_path));
        Self::open_with_embedder(data_path, embedder)
    }

    pub fn open_with_embedder(
        data_path: impl Into<PathBuf>,
        embedder: EmbeddingEngine,
    ) -> MessageHistoryResult<Self> {
        let data_path = data_path.into();
        let history = Self {
            embedder,
            data_path,
        };
        history.ensure_schema()?;
        Ok(history)
    }

    #[cfg(test)]
    fn open_with_hash_embedder(
        data_path: impl Into<PathBuf>,
        dimensions: usize,
    ) -> MessageHistoryResult<Self> {
        Self::open_with_embedder(data_path, EmbeddingEngine::with_hash_dimensions(dimensions))
    }

    pub fn add(
        &self,
        role: MessageRole,
        content: &str,
        metadata: Option<Value>,
    ) -> MessageHistoryResult<String> {
        if role.as_str().is_empty() {
            return Err(MessageHistoryError::EmptyRole);
        }
        let metadata = metadata.unwrap_or_else(|| json!({}));
        if !metadata.is_object() {
            return Err(MessageHistoryError::InvalidMetadata);
        }

        let id = format!("msg-{}", Uuid::new_v4());
        let now = Utc::now().to_rfc3339();
        let vector = self.embedder.embed_document(content)?;
        let metadata_str = serde_json::to_string(&metadata)?;
        let role_str = role.as_str();

        let mut conn = self.open_conn()?;
        let tx = conn.transaction()?;
        tx.execute(
            "INSERT INTO message_history (id, role, content, metadata, created_at) \
             VALUES (?, ?, ?, ?, ?)",
            params![id, role_str, content, metadata_str, now],
        )?;
        tx.execute(
            "INSERT INTO message_history_vec (id, embedding) VALUES (?, ?)",
            params![id, f32_slice_as_bytes(&vector)],
        )?;
        tx.commit()?;
        Ok(id)
    }

    pub fn get(&self, message_id: &str) -> MessageHistoryResult<Option<StoredMessage>> {
        let conn = self.open_conn()?;
        let message = conn
            .query_row(
                "SELECT id, role, content, metadata, created_at FROM message_history WHERE id = ?",
                params![message_id],
                row_to_message,
            )
            .optional()?;
        Ok(message)
    }

    pub fn get_recent(&self, limit: usize) -> MessageHistoryResult<Vec<StoredMessage>> {
        let conn = self.open_conn()?;
        let limit = if limit == 0 { 20 } else { limit };
        let mut stmt = conn.prepare(
            "SELECT id, role, content, metadata, created_at FROM message_history \
             ORDER BY created_at DESC, id DESC LIMIT ?",
        )?;
        let rows = stmt.query_map(params![limit as i64], row_to_message)?;
        let mut messages = Vec::new();
        for row in rows {
            messages.push(row?);
        }
        messages.reverse();
        Ok(messages)
    }

    pub fn search(
        &self,
        query: &str,
        limit: usize,
        role: Option<&MessageRole>,
    ) -> MessageHistoryResult<Vec<StoredMessage>> {
        let query = query.trim();
        let limit = if limit == 0 { 20 } else { limit };
        let terms = query_terms(query);
        let mut merged = HashMap::new();

        for mut message in self.all()? {
            if role.is_some_and(|role| &message.role != role) {
                continue;
            }
            message.score = score_message(query, &terms, &message);
            if terms.is_empty() || message.score > 0.0 {
                merged.insert(message.id.clone(), message);
            }
        }

        if !query.is_empty() {
            match self.vector_search(query, limit * 4) {
                Ok(messages) => {
                    for message in messages {
                        if role.is_some_and(|role| &message.role != role) {
                            continue;
                        }
                        merged
                            .entry(message.id.clone())
                            .and_modify(|existing: &mut StoredMessage| {
                                existing.score += message.score
                            })
                            .or_insert(message);
                    }
                }
                Err(error) => {
                    tracing::warn!("message vector search failed; using lexical results: {error}");
                }
            }
        }

        let mut messages = merged.into_values().collect::<Vec<_>>();
        messages.sort_by(compare_messages);
        messages.truncate(limit);
        Ok(messages)
    }

    pub fn search_by_role(
        &self,
        query: &str,
        role: &MessageRole,
        limit: usize,
    ) -> MessageHistoryResult<Vec<StoredMessage>> {
        self.search(query, limit, Some(role))
    }

    pub fn get_by_role(
        &self,
        role: &MessageRole,
        limit: usize,
    ) -> MessageHistoryResult<Vec<StoredMessage>> {
        let conn = self.open_conn()?;
        let limit = if limit == 0 { 50 } else { limit };
        let mut stmt = conn.prepare(
            "SELECT id, role, content, metadata, created_at FROM message_history \
             WHERE role = ? ORDER BY created_at, id LIMIT ?",
        )?;
        let rows = stmt.query_map(params![role.as_str(), limit as i64], row_to_message)?;
        let mut messages = Vec::new();
        for row in rows {
            messages.push(row?);
        }
        Ok(messages)
    }

    pub fn all_messages(&self) -> MessageHistoryResult<Vec<StoredMessage>> {
        self.all()
    }

    pub fn delete(&self, message_id: &str) -> MessageHistoryResult<bool> {
        let mut conn = self.open_conn()?;
        let tx = conn.transaction()?;
        let removed = tx.execute(
            "DELETE FROM message_history WHERE id = ?",
            params![message_id],
        )?;
        tx.execute(
            "DELETE FROM message_history_vec WHERE id = ?",
            params![message_id],
        )?;
        tx.commit()?;
        Ok(removed > 0)
    }

    pub fn cleanup_search_results(
        &self,
        tool_names: Option<&[String]>,
    ) -> MessageHistoryResult<usize> {
        let names: HashSet<String> = tool_names
            .map(clean_names)
            .filter(|names| !names.is_empty())
            .unwrap_or_else(|| {
                ["conversation_search", "archival_search"]
                    .into_iter()
                    .map(str::to_string)
                    .collect()
            });

        let messages = self.all()?;
        let mut tool_call_names = HashMap::new();
        for message in &messages {
            if !message.role.is_assistant() {
                continue;
            }
            let Some(calls) = message.metadata.get("tool_calls").and_then(Value::as_array) else {
                continue;
            };
            for call in calls {
                let Some(call_id) = call.get("id").and_then(Value::as_str) else {
                    continue;
                };
                let Some(name) = call
                    .get("function")
                    .and_then(|function| function.get("name"))
                    .and_then(Value::as_str)
                else {
                    continue;
                };
                tool_call_names.insert(call_id.to_string(), name.to_string());
            }
        }

        let mut deleted = 0;
        for message in messages {
            if !message.role.is_tool() {
                continue;
            }
            let Some(call_id) = message.metadata.get("tool_call_id").and_then(Value::as_str) else {
                continue;
            };
            let Some(tool_name) = tool_call_names.get(call_id) else {
                continue;
            };
            if names.contains(tool_name) && self.delete(&message.id)? {
                deleted += 1;
            }
        }
        Ok(deleted)
    }

    pub fn count(&self) -> MessageHistoryResult<usize> {
        let conn = self.open_conn()?;
        let count: i64 =
            conn.query_row("SELECT COUNT(*) FROM message_history", [], |row| row.get(0))?;
        Ok(count as usize)
    }

    pub fn clear(&self) -> MessageHistoryResult<usize> {
        let count = self.count()?;
        let mut conn = self.open_conn()?;
        let tx = conn.transaction()?;
        tx.execute("DELETE FROM message_history", [])?;
        tx.execute("DELETE FROM message_history_vec", [])?;
        tx.commit()?;
        Ok(count)
    }

    pub fn get_context_window(
        &self,
        max_messages: usize,
        max_chars: usize,
    ) -> MessageHistoryResult<Vec<StoredMessage>> {
        let messages = self.get_recent(max_messages)?;
        let mut total_chars = 0;
        let mut result = Vec::new();
        for message in messages.into_iter().rev() {
            let message_chars = message.content.chars().count();
            if total_chars + message_chars > max_chars {
                break;
            }
            total_chars += message_chars;
            result.insert(0, message);
        }
        Ok(result)
    }

    /// Render a single stored message in full, including the entire content
    /// without the line-cap that search results apply. Used by
    /// `conversation_get` so the agent can drill into a hit whose body was
    /// trimmed by recall or search formatting.
    pub fn format_detail(message: &StoredMessage) -> String {
        let mut lines = vec![format!("id: {}", message.id)];
        lines.push(format!("role: {}", message.role));
        lines.push(format!("created_at: {}", message.created_at));
        if message.metadata.is_object()
            && message
                .metadata
                .as_object()
                .is_some_and(|map| !map.is_empty())
        {
            lines.push(format!(
                "metadata: {}",
                serde_json::to_string(&message.metadata).unwrap_or_default()
            ));
        }
        lines.push(String::new());
        lines.push(message.content.clone());
        lines.join("\n")
    }

    pub fn format_messages(messages: &[StoredMessage]) -> String {
        if messages.is_empty() {
            return "No messages found.".to_string();
        }
        let mut lines = vec![format!("Found {} message(s):", messages.len())];
        for message in messages {
            let score = if message.score > 0.0 {
                format!(" score={:.2}", message.score)
            } else {
                String::new()
            };
            lines.push(format!(
                "\n- [{}] {} {}{}\n{}",
                message.created_at,
                message.role,
                message.id,
                score,
                indent_block(
                    &search_result_text(&message.content, SEARCH_RESULT_MAX_LINES),
                    "  "
                )
            ));
        }
        lines.join("\n")
    }

    fn open_conn(&self) -> MessageHistoryResult<Connection> {
        Ok(open_conn(&self.data_path)?)
    }

    fn ensure_schema(&self) -> MessageHistoryResult<()> {
        ensure_parent(&self.data_path)?;
        let mut conn = self.open_conn()?;
        conn.execute_batch(&format!(
            "CREATE TABLE IF NOT EXISTS {table} (
                id          TEXT PRIMARY KEY,
                role        TEXT NOT NULL,
                content     TEXT NOT NULL,
                metadata    TEXT NOT NULL DEFAULT '{{}}',
                created_at  TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS {table}_created_at_idx ON {table} (created_at);
            CREATE INDEX IF NOT EXISTS {table}_role_idx ON {table} (role);
            CREATE VIRTUAL TABLE IF NOT EXISTS {vec_table} USING vec0(
                id TEXT PRIMARY KEY,
                embedding float[{dim}]
            );
            CREATE TABLE IF NOT EXISTS {migration_table} (
                version             INTEGER PRIMARY KEY,
                name                TEXT NOT NULL,
                applied_at          TEXT NOT NULL,
                quarantined_turns   INTEGER NOT NULL,
                quarantined_rows    INTEGER NOT NULL,
                cleanup_completed_at TEXT,
                conversation_summary_cleared INTEGER NOT NULL DEFAULT 0,
                archival_entries_deleted INTEGER NOT NULL DEFAULT 0
            );",
            table = TABLE_NAME,
            vec_table = VEC_TABLE_NAME,
            migration_table = MIGRATION_TABLE_NAME,
            dim = LEGACY_EMBEDDING_DIMENSIONS,
        ))?;
        ensure_legacy_quarantine_cleanup_columns(&conn)?;
        quarantine_legacy_wake_forced_wraps(&mut conn)?;
        Ok(())
    }

    pub(crate) fn pending_legacy_wake_cleanup_message_ids(
        &self,
    ) -> MessageHistoryResult<Vec<String>> {
        let conn = self.open_conn()?;
        let cleanup_pending = conn
            .query_row(
                &format!(
                    "SELECT 1 FROM {MIGRATION_TABLE_NAME} \
                     WHERE version = ? AND quarantined_rows > 0 \
                     AND cleanup_completed_at IS NULL"
                ),
                params![LEGACY_WAKE_QUARANTINE_VERSION],
                |_| Ok(()),
            )
            .optional()?
            .is_some();
        if !cleanup_pending {
            return Ok(Vec::new());
        }

        Ok(self
            .all()?
            .into_iter()
            .filter(|message| {
                message.metadata.get(VISIBILITY_KEY) == Some(&json!("internal"))
                    && message
                        .metadata
                        .get(LEGACY_WAKE_ORIGINAL_METADATA_KEY)
                        .is_some()
                    && message.metadata.get(LEGACY_WAKE_QUARANTINE_VERSION_KEY)
                        == Some(&json!(LEGACY_WAKE_QUARANTINE_VERSION))
            })
            .map(|message| message.id)
            .collect())
    }

    pub(crate) fn complete_legacy_wake_cleanup(
        &self,
        conversation_summary_cleared: bool,
        archival_entries_deleted: usize,
    ) -> MessageHistoryResult<()> {
        let conn = self.open_conn()?;
        conn.execute(
            &format!(
                "UPDATE {MIGRATION_TABLE_NAME} \
                 SET cleanup_completed_at = ?, \
                     conversation_summary_cleared = \
                         conversation_summary_cleared + ?, \
                     archival_entries_deleted = archival_entries_deleted + ? \
                 WHERE version = ? AND cleanup_completed_at IS NULL"
            ),
            params![
                Utc::now().to_rfc3339(),
                i64::from(conversation_summary_cleared),
                archival_entries_deleted as i64,
                LEGACY_WAKE_QUARANTINE_VERSION,
            ],
        )?;
        Ok(())
    }

    fn all(&self) -> MessageHistoryResult<Vec<StoredMessage>> {
        let conn = self.open_conn()?;
        let mut stmt = conn.prepare(
            "SELECT id, role, content, metadata, created_at FROM message_history ORDER BY id",
        )?;
        let rows = stmt.query_map([], row_to_message)?;
        let mut messages = Vec::new();
        for row in rows {
            messages.push(row?);
        }
        Ok(messages)
    }

    fn vector_search(&self, query: &str, limit: usize) -> MessageHistoryResult<Vec<StoredMessage>> {
        let query_vector = self.embedder.embed_query(query)?;
        let limit = limit.max(1);
        let conn = self.open_conn()?;
        let mut stmt = conn.prepare(
            "SELECT m.id, m.role, m.content, m.metadata, m.created_at, v.distance \
             FROM message_history_vec v \
             JOIN message_history m ON m.id = v.id \
             WHERE v.embedding MATCH ? AND k = ? \
             ORDER BY v.distance",
        )?;
        let rows = stmt.query_map(
            params![f32_slice_as_bytes(&query_vector), limit as i64],
            |row| {
                let mut message = row_to_message(row)?;
                let distance: f64 = row.get(5)?;
                message.score = semantic_score(distance);
                Ok(message)
            },
        )?;
        let mut messages = Vec::new();
        for row in rows {
            messages.push(row?);
        }
        Ok(messages)
    }
}

fn ensure_legacy_quarantine_cleanup_columns(conn: &Connection) -> MessageHistoryResult<()> {
    let columns = {
        let mut stmt = conn.prepare(&format!("PRAGMA table_info({MIGRATION_TABLE_NAME})"))?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(1))?;
        rows.collect::<rusqlite::Result<HashSet<_>>>()?
    };
    for (name, definition) in [
        ("cleanup_completed_at", "TEXT"),
        ("conversation_summary_cleared", "INTEGER NOT NULL DEFAULT 0"),
        ("archival_entries_deleted", "INTEGER NOT NULL DEFAULT 0"),
    ] {
        if !columns.contains(name) {
            conn.execute(
                &format!("ALTER TABLE {MIGRATION_TABLE_NAME} ADD COLUMN {name} {definition}"),
                [],
            )?;
        }
    }
    Ok(())
}

fn quarantine_legacy_wake_forced_wraps(conn: &mut Connection) -> MessageHistoryResult<()> {
    let tx = conn.transaction()?;
    let prior_state = tx
        .query_row(
            &format!(
                "SELECT quarantined_turns, quarantined_rows, \
                        conversation_summary_cleared, archival_entries_deleted \
                 FROM {MIGRATION_TABLE_NAME} WHERE version = ?"
            ),
            params![LEGACY_WAKE_QUARANTINE_VERSION],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                ))
            },
        )
        .optional()?;

    let rows = {
        let mut stmt = tx.prepare(&format!(
            "SELECT id, role, content, metadata FROM {TABLE_NAME} ORDER BY created_at, rowid"
        ))?;
        let rows = stmt.query_map([], |row| {
            let metadata_raw: String = row.get(3)?;
            let metadata = serde_json::from_str::<Value>(&metadata_raw)
                .ok()
                .filter(Value::is_object);
            Ok(LegacyMessageRow {
                id: row.get(0)?,
                role: MessageRole::parse(&row.get::<_, String>(1)?),
                content: row.get(2)?,
                metadata_raw,
                metadata,
            })
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()?
    };

    let mut candidate_ranges = Vec::new();
    let mut turn_start = None;
    for (index, row) in rows.iter().enumerate() {
        if !row.role.is_user() {
            continue;
        }
        if let Some(start) = turn_start.replace(index)
            && should_quarantine_legacy_turn(&rows[start..index])
        {
            candidate_ranges.push(start..index);
        }
    }
    if let Some(start) = turn_start
        && should_quarantine_legacy_turn(&rows[start..])
    {
        candidate_ranges.push(start..rows.len());
    }

    let mut quarantined_rows = 0usize;
    for range in &candidate_ranges {
        for row in &rows[range.clone()] {
            let original_metadata = row
                .metadata
                .as_ref()
                .expect("validated legacy metadata object")
                .clone();
            let mut metadata = original_metadata
                .as_object()
                .expect("validated legacy metadata object")
                .clone();
            metadata.insert(VISIBILITY_KEY.to_string(), json!("internal"));
            // Use the private wake provenance rather than `checkpoint`: these
            // historical rows must be hidden, never resumed as active work.
            metadata.insert(MESSAGE_KIND_KEY.to_string(), json!("wake"));
            metadata.insert(SOURCE_KEY.to_string(), json!(LEGACY_WAKE_QUARANTINE_SOURCE));
            metadata.insert(
                LEGACY_WAKE_ORIGINAL_METADATA_KEY.to_string(),
                original_metadata,
            );
            metadata.insert(
                LEGACY_WAKE_QUARANTINE_VERSION_KEY.to_string(),
                json!(LEGACY_WAKE_QUARANTINE_VERSION),
            );
            metadata.insert(
                LEGACY_WAKE_QUARANTINE_REASON_KEY.to_string(),
                json!(LEGACY_WAKE_QUARANTINE_REASON),
            );
            tx.execute(
                &format!("UPDATE {TABLE_NAME} SET metadata = ? WHERE id = ?"),
                params![serde_json::to_string(&Value::Object(metadata))?, row.id],
            )?;
            quarantined_rows += 1;
        }
    }

    let mut internal_descendant_turns = 0usize;
    let mut inside_internal_turn = false;
    let mut turn_modified = false;
    for row in &rows {
        if row.role.is_user() {
            if turn_modified {
                internal_descendant_turns += 1;
            }
            let parent_metadata = MessageMetadata::from_value(row.metadata.as_ref());
            inside_internal_turn = parent_metadata.is_internal();
            turn_modified = false;
            continue;
        }
        if !inside_internal_turn {
            continue;
        }
        let row_metadata = MessageMetadata::from_value(row.metadata.as_ref());
        if !row_metadata.is_internal()
            && row.role.is_assistant()
            && row_metadata.kind == Some(MessageKind::Proactive)
        {
            inside_internal_turn = false;
            continue;
        }
        let already_typed = row
            .metadata
            .as_ref()
            .and_then(Value::as_object)
            .is_some_and(|metadata| {
                metadata.contains_key(VISIBILITY_KEY) || metadata.contains_key(MESSAGE_KIND_KEY)
            });
        if already_typed {
            continue;
        }

        let original_metadata = row
            .metadata
            .clone()
            .unwrap_or_else(|| Value::String(row.metadata_raw.clone()));
        let mut metadata = row
            .metadata
            .as_ref()
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default();
        metadata.insert(VISIBILITY_KEY.to_string(), json!("internal"));
        // Quarantined descendants are historical internal protocol, never a
        // resumable checkpoint even if the legacy parent carried bad metadata.
        metadata.insert(MESSAGE_KIND_KEY.to_string(), json!("wake"));
        metadata.insert(
            SOURCE_KEY.to_string(),
            json!(LEGACY_INTERNAL_DESCENDANT_SOURCE),
        );
        metadata.insert(
            LEGACY_WAKE_ORIGINAL_METADATA_KEY.to_string(),
            original_metadata,
        );
        metadata.insert(
            LEGACY_WAKE_QUARANTINE_VERSION_KEY.to_string(),
            json!(LEGACY_WAKE_QUARANTINE_VERSION),
        );
        metadata.insert(
            LEGACY_WAKE_QUARANTINE_REASON_KEY.to_string(),
            json!(LEGACY_INTERNAL_DESCENDANT_REASON),
        );
        tx.execute(
            &format!("UPDATE {TABLE_NAME} SET metadata = ? WHERE id = ?"),
            params![serde_json::to_string(&Value::Object(metadata))?, row.id],
        )?;
        quarantined_rows += 1;
        turn_modified = true;
    }
    if turn_modified {
        internal_descendant_turns += 1;
    }
    let quarantined_turns = candidate_ranges.len() + internal_descendant_turns;

    if prior_state.is_none() || quarantined_turns > 0 {
        let (prior_turns, prior_rows, prior_summary_clears, prior_archival_deletes) =
            prior_state.unwrap_or_default();
        tx.execute(
            &format!(
                "INSERT OR REPLACE INTO {MIGRATION_TABLE_NAME} \
                 (version, name, applied_at, quarantined_turns, quarantined_rows, \
                  cleanup_completed_at, conversation_summary_cleared, \
                  archival_entries_deleted) \
                 VALUES (?, ?, ?, ?, ?, NULL, ?, ?)"
            ),
            params![
                LEGACY_WAKE_QUARANTINE_VERSION,
                LEGACY_WAKE_QUARANTINE_NAME,
                Utc::now().to_rfc3339(),
                prior_turns + quarantined_turns as i64,
                prior_rows + quarantined_rows as i64,
                prior_summary_clears,
                prior_archival_deletes,
            ],
        )?;
    }
    tx.commit()?;

    tracing::info!(
        migration = LEGACY_WAKE_QUARANTINE_NAME,
        version = LEGACY_WAKE_QUARANTINE_VERSION,
        quarantined_turns,
        quarantined_rows,
        "message-history legacy quarantine applied"
    );
    Ok(())
}

fn should_quarantine_legacy_turn(turn: &[LegacyMessageRow]) -> bool {
    let (Some(first), Some(last)) = (turn.first(), turn.last()) else {
        return false;
    };
    if !first.role.is_user() || !last.role.is_assistant() {
        return false;
    }
    if !metadata_is_empty_object(first.metadata.as_ref())
        || !metadata_is_empty_object(last.metadata.as_ref())
    {
        return false;
    }
    if turn.iter().any(|row| {
        row.metadata
            .as_ref()
            .and_then(Value::as_object)
            .is_none_or(|metadata| {
                metadata.contains_key(VISIBILITY_KEY) || metadata.contains_key(MESSAGE_KIND_KEY)
            })
    }) {
        return false;
    }
    matches_legacy_forced_wrap_contract(&last.content)
}

fn metadata_is_empty_object(metadata: Option<&Value>) -> bool {
    metadata
        .and_then(Value::as_object)
        .is_some_and(serde_json::Map::is_empty)
}

fn matches_legacy_forced_wrap_contract(content: &str) -> bool {
    if content.contains("```") || content.contains("~~~") {
        return false;
    }

    let mut sections = vec![Vec::new()];
    let mut dividers = 0usize;
    for line in content.lines() {
        if line == "---" {
            dividers += 1;
            sections.push(Vec::new());
        } else {
            sections
                .last_mut()
                .expect("at least one forced-wrap section")
                .push(line);
        }
    }
    if dividers != 3 || sections.len() != 4 {
        return false;
    }

    ["GOAL —", "DONE —", "REMAINING —", "NEXT —"]
        .into_iter()
        .zip(sections)
        .all(|(prefix, section)| {
            section.first().is_some_and(|line| {
                *line == prefix
                    || line.strip_prefix(prefix).is_some_and(|suffix| {
                        suffix.chars().next().is_some_and(char::is_whitespace)
                    })
            })
        })
}

fn row_to_message(row: &rusqlite::Row<'_>) -> rusqlite::Result<StoredMessage> {
    let id: String = row.get(0)?;
    let role: String = row.get(1)?;
    let content: String = row.get(2)?;
    let metadata_raw: String = row.get(3)?;
    let created_at: String = row.get(4)?;
    let metadata = serde_json::from_str(&metadata_raw).unwrap_or_else(|_| json!({}));
    let metadata = if metadata.is_object() {
        metadata
    } else {
        json!({})
    };
    Ok(StoredMessage {
        id,
        role: MessageRole::parse(&role),
        content,
        metadata,
        created_at,
        score: 0.0,
    })
}

pub(crate) fn compare_messages(left: &StoredMessage, right: &StoredMessage) -> Ordering {
    right
        .score
        .partial_cmp(&left.score)
        .unwrap_or(Ordering::Equal)
        .then_with(|| parse_time(&right.created_at).cmp(&parse_time(&left.created_at)))
        .then_with(|| left.id.cmp(&right.id))
}

fn parse_time(value: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|time| time.with_timezone(&Utc))
}

pub(crate) fn score_message(query: &str, terms: &[String], message: &StoredMessage) -> f64 {
    if terms.is_empty() {
        return 1.0;
    }
    let query_lower = query.to_ascii_lowercase();
    let content_lower = message.content.to_ascii_lowercase();
    let metadata_lower = message.metadata.to_string().to_ascii_lowercase();
    let mut score = 0.0;

    if !query_lower.is_empty() && content_lower.contains(&query_lower) {
        score += 5.0;
    }
    for term in terms {
        score += content_lower.matches(term).count() as f64;
        if metadata_lower.contains(term) {
            score += 1.0;
        }
    }
    score
}

fn clean_names(names: &[String]) -> HashSet<String> {
    names
        .iter()
        .map(|name| name.trim().to_string())
        .filter(|name| !name.is_empty())
        .collect()
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use tempfile::tempdir;

    use super::*;

    const EXACT_LEGACY_FORCED_WRAP: &str = "GOAL — preserve the current task\n\
continue from the durable state\n\
---\n\
DONE — completed the safe step\n\
---\n\
REMAINING — finish the pending work\n\
---\n\
NEXT — resume with the next tool call";

    fn history() -> (tempfile::TempDir, MessageHistory) {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("messages.db");
        let history =
            MessageHistory::open_with_hash_embedder(path, LEGACY_EMBEDDING_DIMENSIONS).unwrap();
        (tmp, history)
    }

    fn seed_legacy_history(rows: &[(MessageRole, &str, Value)]) -> (tempfile::TempDir, PathBuf) {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("messages.db");
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch(
            "CREATE TABLE message_history (
                id          TEXT PRIMARY KEY,
                role        TEXT NOT NULL,
                content     TEXT NOT NULL,
                metadata    TEXT NOT NULL DEFAULT '{}',
                created_at  TEXT NOT NULL
            );",
        )
        .unwrap();
        for (index, (role, content, metadata)) in rows.iter().enumerate() {
            conn.execute(
                "INSERT INTO message_history (id, role, content, metadata, created_at) \
                 VALUES (?, ?, ?, ?, ?)",
                params![
                    format!("legacy-{index:03}"),
                    role.as_str(),
                    *content,
                    serde_json::to_string(metadata).unwrap(),
                    format!("2026-08-08T22:00:{index:02}Z"),
                ],
            )
            .unwrap();
        }
        drop(conn);
        (tmp, path)
    }

    fn open_legacy_history(path: &Path) -> MessageHistory {
        MessageHistory::open_with_hash_embedder(path, LEGACY_EMBEDDING_DIMENSIONS).unwrap()
    }

    fn exact_legacy_turn() -> Vec<(MessageRole, &'static str, Value)> {
        vec![
            (MessageRole::User, "/wake legacy task", json!({})),
            (
                MessageRole::Assistant,
                "",
                json!({
                    "tool_calls": [{
                        "id": "legacy-call",
                        "function": {"name": "memory_read"}
                    }]
                }),
            ),
            (
                MessageRole::Tool,
                "legacy tool result",
                json!({"tool_call_id": "legacy-call", "name": "memory_read"}),
            ),
            (MessageRole::Assistant, EXACT_LEGACY_FORCED_WRAP, json!({})),
        ]
    }

    fn assert_legacy_turn_quarantined(
        messages: &[StoredMessage],
        original_rows: &[(MessageRole, &str, Value)],
    ) {
        assert_eq!(messages.len(), original_rows.len());
        for (message, (_, _, original_metadata)) in messages.iter().zip(original_rows) {
            assert_eq!(
                message.metadata.get(VISIBILITY_KEY),
                Some(&json!("internal"))
            );
            assert_eq!(message.metadata.get(MESSAGE_KIND_KEY), Some(&json!("wake")));
            assert_ne!(
                message.metadata.get(MESSAGE_KIND_KEY),
                Some(&json!("checkpoint")),
                "legacy quarantine must never create a resumable checkpoint"
            );
            assert_eq!(
                message.metadata.get(SOURCE_KEY),
                Some(&json!(LEGACY_WAKE_QUARANTINE_SOURCE))
            );
            assert_eq!(
                message.metadata.get(LEGACY_WAKE_ORIGINAL_METADATA_KEY),
                Some(original_metadata)
            );
            assert_eq!(
                message.metadata.get(LEGACY_WAKE_QUARANTINE_VERSION_KEY),
                Some(&json!(LEGACY_WAKE_QUARANTINE_VERSION))
            );
            assert_eq!(
                message.metadata.get(LEGACY_WAKE_QUARANTINE_REASON_KEY),
                Some(&json!(LEGACY_WAKE_QUARANTINE_REASON))
            );
        }
    }

    #[test]
    fn add_get_recent_count_and_clear_messages() {
        let (_tmp, history) = history();
        let first = history.add(MessageRole::User, "hello", None).unwrap();
        let second = history
            .add(MessageRole::Assistant, "hi there", None)
            .unwrap();

        assert_eq!(history.count().unwrap(), 2);
        assert_eq!(history.get(&first).unwrap().unwrap().content, "hello");

        let recent = history.get_recent(2).unwrap();
        assert_eq!(
            recent.iter().map(|message| &message.id).collect::<Vec<_>>(),
            vec![&first, &second]
        );
        assert!(MessageHistory::format_messages(&recent).contains("hi there"));

        assert_eq!(history.clear().unwrap(), 2);
        assert_eq!(history.count().unwrap(), 0);
    }

    #[test]
    fn search_and_role_filters_rank_messages() {
        let (_tmp, history) = history();
        history
            .add(MessageRole::User, "Graph API email access", None)
            .unwrap();
        history
            .add(MessageRole::Assistant, "Use cargo fmt", None)
            .unwrap();
        history
            .add(MessageRole::User, "Graph tokens are in a file", None)
            .unwrap();

        let results = history.search("graph email", 10, None).unwrap();
        assert_eq!(results[0].content, "Graph API email access");

        let assistant = history
            .search_by_role("cargo", &MessageRole::Assistant, 10)
            .unwrap();
        assert_eq!(assistant.len(), 1);
        assert_eq!(assistant[0].role, MessageRole::Assistant);

        let users = history.get_by_role(&MessageRole::User, 10).unwrap();
        assert_eq!(users.len(), 2);
    }

    #[test]
    fn format_messages_preserves_search_result_lines() {
        let content = (0..60)
            .map(|index| format!("line {index}"))
            .collect::<Vec<_>>()
            .join("\n");
        let formatted = MessageHistory::format_messages(&[StoredMessage {
            id: "msg-test".to_string(),
            role: MessageRole::Assistant,
            content,
            metadata: json!({}),
            created_at: "2026-01-01T00:00:00Z".to_string(),
            score: 0.0,
        }]);

        assert!(formatted.contains("  line 0\n  line 1"));
        assert!(formatted.contains("  line 49"));
        assert!(!formatted.contains("line 50"));
        assert!(formatted.contains("[... 10 more lines]"));
    }

    #[test]
    fn context_window_keeps_recent_messages_within_char_budget() {
        let (_tmp, history) = history();
        history.add(MessageRole::User, "one", None).unwrap();
        history
            .add(MessageRole::Assistant, "two two", None)
            .unwrap();
        history
            .add(MessageRole::User, "three three three", None)
            .unwrap();

        let window = history.get_context_window(3, 10).unwrap();
        assert!(window.is_empty() || window.last().unwrap().content.len() <= 10);
    }

    #[test]
    fn legacy_forced_wrap_turn_is_quarantined_across_reopen_and_queries() {
        let rows = exact_legacy_turn();
        let (_tmp, path) = seed_legacy_history(&rows);
        let history = open_legacy_history(&path);

        let recent = history.get_recent(20).unwrap();
        assert_legacy_turn_quarantined(&recent, &rows);

        let final_message = history.get("legacy-003").unwrap().unwrap();
        assert_eq!(final_message.content, EXACT_LEGACY_FORCED_WRAP);
        assert_legacy_turn_quarantined(&[final_message], &rows[3..]);

        let results = history.search("pending work", 20, None).unwrap();
        let final_result = results
            .iter()
            .find(|message| message.id == "legacy-003")
            .expect("legacy final should remain auditable through search");
        assert_legacy_turn_quarantined(&[final_result.clone()], &rows[3..]);

        drop(history);
        let reopened = open_legacy_history(&path);
        assert_legacy_turn_quarantined(&reopened.get_recent(20).unwrap(), &rows);
    }

    #[test]
    fn legacy_quarantine_ignores_near_misses_and_visible_goal_replies() {
        let two_dividers =
            "GOAL — first\n---\nDONE — second\n---\nREMAINING — third\nNEXT — fourth";
        let four_dividers = "GOAL — first\n---\nDONE — second\n---\nREMAINING — third\n---\nNEXT — fourth\n---\nextra";
        let padded_divider =
            "GOAL — first\n --- \nDONE — second\n---\nREMAINING — third\n---\nNEXT — fourth";
        let wrong_order =
            "GOAL — first\n---\nREMAINING — third\n---\nDONE — second\n---\nNEXT — fourth";
        let fenced = "GOAL — first\n```text\nprivate\n```\n---\nDONE — second\n---\nREMAINING — third\n---\nNEXT — fourth";
        let typed_visible = json!({
            VISIBILITY_KEY: "user_visible",
            MESSAGE_KIND_KEY: "chat"
        });
        let rows = vec![
            (MessageRole::User, "ordinary question", json!({})),
            (
                MessageRole::Assistant,
                "GOAL — this is an ordinary natural planning reply",
                json!({}),
            ),
            (MessageRole::User, "two-divider case", json!({})),
            (MessageRole::Assistant, two_dividers, json!({})),
            (MessageRole::User, "four-divider case", json!({})),
            (MessageRole::Assistant, four_dividers, json!({})),
            (MessageRole::User, "padded-divider case", json!({})),
            (MessageRole::Assistant, padded_divider, json!({})),
            (MessageRole::User, "wrong-order case", json!({})),
            (MessageRole::Assistant, wrong_order, json!({})),
            (MessageRole::User, "fenced case", json!({})),
            (MessageRole::Assistant, fenced, json!({})),
            (MessageRole::User, "typed exact case", typed_visible.clone()),
            (
                MessageRole::Assistant,
                EXACT_LEGACY_FORCED_WRAP,
                typed_visible,
            ),
            (MessageRole::User, "untyped metadata case", json!({})),
            (
                MessageRole::Assistant,
                EXACT_LEGACY_FORCED_WRAP,
                json!({"style": "natural"}),
            ),
            (MessageRole::User, "typed intermediate case", json!({})),
            (
                MessageRole::Tool,
                "typed internal row",
                json!({VISIBILITY_KEY: "internal", MESSAGE_KIND_KEY: "wake"}),
            ),
            (MessageRole::Assistant, EXACT_LEGACY_FORCED_WRAP, json!({})),
        ];
        let expected_metadata = rows
            .iter()
            .map(|(_, _, metadata)| metadata.clone())
            .collect::<Vec<_>>();
        let (_tmp, path) = seed_legacy_history(&rows);
        let history = open_legacy_history(&path);

        let stored = history.get_recent(50).unwrap();
        assert_eq!(stored.len(), rows.len());
        assert_eq!(
            stored
                .iter()
                .map(|message| message.metadata.clone())
                .collect::<Vec<_>>(),
            expected_metadata
        );
        assert!(stored.iter().all(|message| {
            message
                .metadata
                .get(LEGACY_WAKE_ORIGINAL_METADATA_KEY)
                .is_none()
        }));
    }

    #[test]
    fn legacy_quarantine_is_versioned_idempotent_and_auditable() {
        let rows = exact_legacy_turn();
        let (_tmp, path) = seed_legacy_history(&rows);
        let history = open_legacy_history(&path);
        let metadata_after_first_open = history
            .get_recent(20)
            .unwrap()
            .into_iter()
            .map(|message| message.metadata)
            .collect::<Vec<_>>();
        drop(history);

        let reopened = open_legacy_history(&path);
        let metadata_after_reopen = reopened
            .get_recent(20)
            .unwrap()
            .into_iter()
            .map(|message| message.metadata)
            .collect::<Vec<_>>();
        assert_eq!(metadata_after_reopen, metadata_after_first_open);

        let conn = Connection::open(&path).unwrap();
        let ledger = conn
            .query_row(
                &format!(
                    "SELECT name, quarantined_turns, quarantined_rows \
                     FROM {MIGRATION_TABLE_NAME} WHERE version = ?"
                ),
                params![LEGACY_WAKE_QUARANTINE_VERSION],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(
            ledger,
            (
                LEGACY_WAKE_QUARANTINE_NAME.to_string(),
                1,
                rows.len() as i64
            )
        );
        let version_rows: i64 = conn
            .query_row(
                &format!("SELECT COUNT(*) FROM {MIGRATION_TABLE_NAME} WHERE version = ?"),
                params![LEGACY_WAKE_QUARANTINE_VERSION],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(version_rows, 1);
        assert_legacy_turn_quarantined(&reopened.get_recent(20).unwrap(), &rows);
    }

    #[test]
    fn legacy_internal_turn_descendant_is_typed_before_limited_tail_reads() {
        let rows = vec![
            (
                MessageRole::User,
                "typed internal wake prompt",
                json!({
                    VISIBILITY_KEY: "internal",
                    MESSAGE_KIND_KEY: "checkpoint",
                    SOURCE_KEY: "wake",
                }),
            ),
            (
                MessageRole::Assistant,
                "LEGACY_UNTYPED_INTERNAL_CHILD_CANARY",
                json!({}),
            ),
        ];
        let (_tmp, path) = seed_legacy_history(&rows);
        let history = open_legacy_history(&path);

        let tail = history.get_recent(1).unwrap();
        assert_eq!(tail.len(), 1);
        assert_eq!(tail[0].content, "LEGACY_UNTYPED_INTERNAL_CHILD_CANARY");
        assert_eq!(tail[0].metadata[VISIBILITY_KEY], "internal");
        assert_eq!(tail[0].metadata[MESSAGE_KIND_KEY], "wake");
        assert_eq!(
            tail[0].metadata[SOURCE_KEY],
            LEGACY_INTERNAL_DESCENDANT_SOURCE
        );
        assert_eq!(
            tail[0].metadata[LEGACY_WAKE_ORIGINAL_METADATA_KEY],
            json!({})
        );
        assert_eq!(
            tail[0].metadata[LEGACY_WAKE_QUARANTINE_REASON_KEY],
            LEGACY_INTERNAL_DESCENDANT_REASON
        );

        drop(history);
        let reopened = open_legacy_history(&path);
        assert_eq!(
            reopened.get_recent(1).unwrap()[0].metadata,
            tail[0].metadata
        );
    }

    #[test]
    fn visible_proactive_message_ends_legacy_internal_descendant_backfill() {
        let rows = vec![
            (
                MessageRole::User,
                "typed internal wake prompt",
                json!({
                    VISIBILITY_KEY: "internal",
                    MESSAGE_KIND_KEY: "wake",
                    SOURCE_KEY: "wake",
                }),
            ),
            (
                MessageRole::Assistant,
                "confirmed proactive delivery",
                json!({
                    VISIBILITY_KEY: "user_visible",
                    MESSAGE_KIND_KEY: "proactive",
                    SOURCE_KEY: "wake",
                }),
            ),
            (
                MessageRole::Assistant,
                "legacy visible continuation",
                json!({}),
            ),
        ];
        let expected_metadata = rows[1..]
            .iter()
            .map(|(_, _, metadata)| metadata.clone())
            .collect::<Vec<_>>();
        let (_tmp, path) = seed_legacy_history(&rows);
        let history = open_legacy_history(&path);

        assert_eq!(
            history
                .get_recent(2)
                .unwrap()
                .into_iter()
                .map(|message| message.metadata)
                .collect::<Vec<_>>(),
            expected_metadata
        );
    }
}
