//! Durable, transport-scoped approval for one immutable tool invocation.
//!
//! Human-facing snapshots contain a summary and opaque choice identifiers, never
//! the tool arguments. Consumption commits before returning the saved arguments:
//! if dispatch crashes afterwards, the action cannot be replayed automatically.

use std::fs::{self, OpenOptions};
use std::path::PathBuf;
use std::time::Duration;

use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use uuid::Uuid;

const MAX_SCOPE_BYTES: usize = 512;
const MAX_SUMMARY_BYTES: usize = 2048;
const MAX_ARGS_BYTES: usize = 16 * 1024;
const MAX_APPROVAL_LIFETIME: i64 = 24 * 60 * 60;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ApprovalScope {
    pub owner: String,
    pub chat: String,
    pub actor: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ActionPayload {
    pub tool: String,
    pub args: Value,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalStatus {
    Pending,
    Approved,
    Rejected,
    Consumed,
    Expired,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ApprovalRequest {
    pub request_id: String,
    pub scope: ApprovalScope,
    pub summary: String,
    pub expires_at: i64,
    pub status: ApprovalStatus,
    pub approve_choice: String,
    pub reject_choice: String,
}

#[derive(Debug, thiserror::Error)]
pub enum ApprovalError {
    #[error("invalid approval request: {0}")]
    InvalidRequest(&'static str),
    #[error("approval request was not found")]
    NotFound,
    #[error("approval request belongs to a different owner, chat, or actor")]
    WrongScope,
    #[error("approval request has expired")]
    Expired,
    #[error("approval choice has already been resolved")]
    AlreadyResolved,
    #[error("action is not approved or has already been consumed")]
    NotApproved,
    #[error("approval's saved action or binding has changed")]
    PayloadChanged,
    #[error("approval storage: {0}")]
    Storage(#[from] rusqlite::Error),
    #[error("approval storage file: {0}")]
    File(#[from] std::io::Error),
}

pub type ApprovalResult<T> = Result<T, ApprovalError>;

#[derive(Clone, Debug)]
pub struct ActionStore {
    path: PathBuf,
}

struct SavedRequest {
    public: ApprovalRequest,
    payload: ActionPayload,
    fingerprint: String,
}

impl SavedRequest {
    fn verify(&self) -> ApprovalResult<()> {
        if self.fingerprint != fingerprint(&self.public, &self.payload) {
            return Err(ApprovalError::PayloadChanged);
        }
        Ok(())
    }

    fn snapshot(mut self, now: i64) -> ApprovalRequest {
        if now >= self.public.expires_at
            && matches!(
                self.public.status,
                ApprovalStatus::Pending | ApprovalStatus::Approved
            )
        {
            self.public.status = ApprovalStatus::Expired;
        }
        self.public
    }
}

impl ActionStore {
    pub fn open(path: impl Into<PathBuf>) -> ApprovalResult<Self> {
        let path = path.into();
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            fs::create_dir_all(parent)?;
        }
        let mut options = OpenOptions::new();
        options.create(true).append(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        options.open(&path)?;
        let store = Self { path };
        store.connection()?.execute_batch(
            "CREATE TABLE IF NOT EXISTS action_approvals (
                request_id TEXT PRIMARY KEY,
                owner TEXT NOT NULL,
                chat TEXT NOT NULL,
                actor TEXT NOT NULL,
                summary TEXT NOT NULL,
                tool TEXT NOT NULL,
                args TEXT NOT NULL,
                expires_at INTEGER NOT NULL,
                created_at INTEGER NOT NULL,
                status TEXT NOT NULL CHECK(status IN ('pending','approved','rejected','consumed')),
                approve_choice TEXT NOT NULL UNIQUE,
                reject_choice TEXT NOT NULL UNIQUE,
                fingerprint TEXT NOT NULL,
                resolved_at INTEGER,
                consumed_at INTEGER
            );
            CREATE INDEX IF NOT EXISTS action_approvals_scope
                ON action_approvals(owner, chat, actor, status, expires_at);",
        )?;
        Ok(store)
    }

    pub fn request_action(
        &self,
        scope: ApprovalScope,
        payload: ActionPayload,
        summary: impl Into<String>,
        expires_at: i64,
        now: i64,
    ) -> ApprovalResult<ApprovalRequest> {
        let summary = summary.into();
        validate_request(&scope, &payload, &summary, expires_at, now)?;
        let public = ApprovalRequest {
            request_id: Uuid::new_v4().to_string(),
            scope,
            summary,
            expires_at,
            status: ApprovalStatus::Pending,
            approve_choice: Uuid::new_v4().to_string(),
            reject_choice: Uuid::new_v4().to_string(),
        };
        let args = serde_json::to_string(&payload.args)
            .map_err(|_| ApprovalError::InvalidRequest("arguments are not JSON"))?;
        self.connection()?.execute(
            "INSERT INTO action_approvals (
                request_id, owner, chat, actor, summary, tool, args,
                expires_at, created_at, status, approve_choice, reject_choice, fingerprint
             ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,'pending',?10,?11,?12)",
            params![
                public.request_id,
                public.scope.owner,
                public.scope.chat,
                public.scope.actor,
                public.summary,
                payload.tool,
                args,
                expires_at,
                now,
                public.approve_choice,
                public.reject_choice,
                fingerprint(&public, &payload),
            ],
        )?;
        Ok(public)
    }

    /// Resolve only the stored opaque choice, under its exact originating scope.
    /// A plain "yes", a request id, and a choice used once are never approvals.
    pub fn resolve_choice(
        &self,
        scope: &ApprovalScope,
        choice: &str,
        now: i64,
    ) -> ApprovalResult<ApprovalRequest> {
        let mut conn = self.connection()?;
        let transaction = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let mut saved = load_request(
            &transaction,
            "approve_choice = ?1 OR reject_choice = ?1",
            choice,
        )?;
        check_binding(&saved, scope, now)?;
        if saved.public.status != ApprovalStatus::Pending {
            return Err(ApprovalError::AlreadyResolved);
        }
        let status = if choice == saved.public.approve_choice {
            ApprovalStatus::Approved
        } else {
            ApprovalStatus::Rejected
        };
        transaction.execute(
            "UPDATE action_approvals SET status = ?1, resolved_at = ?2
             WHERE request_id = ?3 AND status = 'pending'",
            params![status_name(status), now, saved.public.request_id],
        )?;
        transaction.commit()?;
        saved.public.status = status;
        Ok(saved.public)
    }

    /// Commit consumption before dispatch. Callers execute only this saved
    /// payload; accepting replacement arguments would invalidate the approval.
    pub fn take_approved_action(
        &self,
        scope: &ApprovalScope,
        request_id: &str,
        now: i64,
    ) -> ApprovalResult<ActionPayload> {
        let mut conn = self.connection()?;
        let transaction = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let saved = load_request(&transaction, "request_id = ?1", request_id)?;
        check_binding(&saved, scope, now)?;
        if saved.public.status != ApprovalStatus::Approved {
            return Err(ApprovalError::NotApproved);
        }
        transaction.execute(
            "UPDATE action_approvals SET status = 'consumed', consumed_at = ?1
             WHERE request_id = ?2 AND status = 'approved'",
            params![now, saved.public.request_id],
        )?;
        transaction.commit()?;
        Ok(saved.payload)
    }

    pub fn get(
        &self,
        scope: &ApprovalScope,
        request_id: &str,
        now: i64,
    ) -> ApprovalResult<ApprovalRequest> {
        let saved = load_request(&self.connection()?, "request_id = ?1", request_id)?;
        saved.verify()?;
        if &saved.public.scope != scope {
            return Err(ApprovalError::WrongScope);
        }
        Ok(saved.snapshot(now))
    }

    pub fn list_pending(
        &self,
        scope: &ApprovalScope,
        now: i64,
    ) -> ApprovalResult<Vec<ApprovalRequest>> {
        self.list_requests(scope, now, false)
    }

    /// Include approved, unconsumed actions so interrupted approval turns can
    /// be inspected and explicitly resumed without silently replaying dispatch.
    pub fn list_active(
        &self,
        scope: &ApprovalScope,
        now: i64,
    ) -> ApprovalResult<Vec<ApprovalRequest>> {
        self.list_requests(scope, now, true)
    }

    fn list_requests(
        &self,
        scope: &ApprovalScope,
        now: i64,
        include_approved: bool,
    ) -> ApprovalResult<Vec<ApprovalRequest>> {
        let mut conn = self.connection()?;
        let transaction = conn.transaction()?;
        let mut statement = transaction.prepare(
            "SELECT request_id FROM action_approvals
             WHERE owner = ?1 AND chat = ?2 AND actor = ?3
               AND (status = 'pending' OR (?5 = 1 AND status = 'approved'))
               AND expires_at > ?4
             ORDER BY created_at, request_id LIMIT 100",
        )?;
        let ids = statement
            .query_map(
                params![scope.owner, scope.chat, scope.actor, now, include_approved],
                |row| row.get::<_, String>(0),
            )?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        drop(statement);
        let requests = ids
            .into_iter()
            .map(|id| {
                let saved = load_request(&transaction, "request_id = ?1", &id)?;
                saved.verify()?;
                Ok(saved.snapshot(now))
            })
            .collect::<ApprovalResult<Vec<_>>>()?;
        transaction.commit()?;
        Ok(requests)
    }

    fn connection(&self) -> ApprovalResult<Connection> {
        let conn = Connection::open(&self.path)?;
        conn.busy_timeout(Duration::from_secs(5))?;
        Ok(conn)
    }
}

fn check_binding(saved: &SavedRequest, scope: &ApprovalScope, now: i64) -> ApprovalResult<()> {
    saved.verify()?;
    if &saved.public.scope != scope {
        return Err(ApprovalError::WrongScope);
    }
    if now >= saved.public.expires_at {
        return Err(ApprovalError::Expired);
    }
    Ok(())
}

fn validate_request(
    scope: &ApprovalScope,
    payload: &ActionPayload,
    summary: &str,
    expires_at: i64,
    now: i64,
) -> ApprovalResult<()> {
    if [&scope.owner, &scope.chat, &scope.actor]
        .iter()
        .any(|value| value.trim().is_empty() || value.len() > MAX_SCOPE_BYTES)
    {
        return Err(ApprovalError::InvalidRequest("scope is empty or too long"));
    }
    if summary.trim().is_empty() || summary.len() > MAX_SUMMARY_BYTES {
        return Err(ApprovalError::InvalidRequest(
            "summary is empty or too long",
        ));
    }
    if payload.tool.is_empty()
        || payload.tool.len() > 128
        || !payload
            .tool
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"_.:-".contains(&byte))
    {
        return Err(ApprovalError::InvalidRequest("invalid tool name"));
    }
    if !payload.args.is_object()
        || serde_json::to_vec(&payload.args).map_or(true, |args| args.len() > MAX_ARGS_BYTES)
    {
        return Err(ApprovalError::InvalidRequest(
            "arguments must be a bounded JSON object",
        ));
    }
    if expires_at <= now
        || expires_at
            .checked_sub(now)
            .is_none_or(|lifetime| lifetime > MAX_APPROVAL_LIFETIME)
    {
        return Err(ApprovalError::InvalidRequest(
            "expiry must be within 24 hours",
        ));
    }
    Ok(())
}

fn fingerprint(public: &ApprovalRequest, payload: &ActionPayload) -> String {
    let immutable = (
        &public.request_id,
        &public.scope,
        &public.summary,
        public.expires_at,
        &public.approve_choice,
        &public.reject_choice,
        payload,
    );
    let bytes = serde_json::to_vec(&immutable).expect("approval fields are JSON serializable");
    format!("{:x}", Sha256::digest(bytes))
}

fn status_name(status: ApprovalStatus) -> &'static str {
    match status {
        ApprovalStatus::Pending => "pending",
        ApprovalStatus::Approved => "approved",
        ApprovalStatus::Rejected => "rejected",
        ApprovalStatus::Consumed => "consumed",
        ApprovalStatus::Expired => "expired",
    }
}

fn load_request(conn: &Connection, clause: &str, value: &str) -> ApprovalResult<SavedRequest> {
    // `clause` is a private fixed SQL expression, never a caller-provided value.
    let query = format!(
        "SELECT request_id, owner, chat, actor, summary, tool, args, expires_at,
                status, approve_choice, reject_choice, fingerprint
         FROM action_approvals WHERE {clause}"
    );
    let saved = conn
        .query_row(&query, [value], |row| {
            let args: String = row.get(6)?;
            let status: String = row.get(8)?;
            Ok((
                ApprovalRequest {
                    request_id: row.get(0)?,
                    scope: ApprovalScope {
                        owner: row.get(1)?,
                        chat: row.get(2)?,
                        actor: row.get(3)?,
                    },
                    summary: row.get(4)?,
                    expires_at: row.get(7)?,
                    status: match status.as_str() {
                        "pending" => ApprovalStatus::Pending,
                        "approved" => ApprovalStatus::Approved,
                        "rejected" => ApprovalStatus::Rejected,
                        "consumed" => ApprovalStatus::Consumed,
                        _ => return Err(rusqlite::Error::InvalidQuery),
                    },
                    approve_choice: row.get(9)?,
                    reject_choice: row.get(10)?,
                },
                row.get::<_, String>(5)?,
                args,
                row.get::<_, String>(11)?,
            ))
        })
        .optional()?
        .ok_or(ApprovalError::NotFound)?;
    Ok(SavedRequest {
        public: saved.0,
        payload: ActionPayload {
            tool: saved.1,
            args: serde_json::from_str(&saved.2).map_err(|_| ApprovalError::PayloadChanged)?,
        },
        fingerprint: saved.3,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Barrier};

    use serde_json::json;
    use tempfile::TempDir;

    use super::*;

    fn scope() -> ApprovalScope {
        ApprovalScope {
            owner: "linq:+15550000000".into(),
            chat: "chat-1".into(),
            actor: "cortex".into(),
        }
    }

    fn fixture() -> (TempDir, ActionStore, ApprovalRequest) {
        let tmp = tempfile::tempdir().unwrap();
        let store = ActionStore::open(tmp.path().join("actions.db")).unwrap();
        let request = store
            .request_action(
                scope(),
                ActionPayload {
                    tool: "send_email".into(),
                    args: json!({"to": "alice@example.com", "body": "Approved body"}),
                },
                "Send Alice the prepared email",
                200,
                100,
            )
            .unwrap();
        (tmp, store, request)
    }

    #[test]
    fn approval_survives_restart_and_consumption_never_replays() {
        let (tmp, store, request) = fixture();
        assert_eq!(
            store.list_pending(&scope(), 101).unwrap(),
            [request.clone()]
        );
        store
            .resolve_choice(&scope(), &request.approve_choice, 102)
            .unwrap();
        assert!(store.list_pending(&scope(), 103).unwrap().is_empty());
        drop(store);
        let restarted = ActionStore::open(tmp.path().join("actions.db")).unwrap();
        assert_eq!(
            restarted
                .get(&scope(), &request.request_id, 103)
                .unwrap()
                .status,
            ApprovalStatus::Approved
        );
        let active = restarted.list_active(&scope(), 103).unwrap();
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].request_id, request.request_id);
        assert_eq!(active[0].status, ApprovalStatus::Approved);
        let payload = restarted
            .take_approved_action(&scope(), &request.request_id, 103)
            .unwrap();
        assert_eq!(payload.tool, "send_email");
        assert_eq!(payload.args["body"], "Approved body");
        drop(restarted);
        let after_dispatch_crash = ActionStore::open(tmp.path().join("actions.db")).unwrap();
        assert!(matches!(
            after_dispatch_crash.take_approved_action(&scope(), &request.request_id, 104),
            Err(ApprovalError::NotApproved)
        ));
        assert_eq!(
            after_dispatch_crash
                .get(&scope(), &request.request_id, 104)
                .unwrap()
                .status,
            ApprovalStatus::Consumed
        );
        assert!(
            after_dispatch_crash
                .list_active(&scope(), 104)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn choices_require_exact_owner_chat_and_actor() {
        let (_tmp, store, request) = fixture();
        for wrong in [
            ApprovalScope {
                owner: "linq:someone-else".into(),
                ..scope()
            },
            ApprovalScope {
                chat: "chat-2".into(),
                ..scope()
            },
            ApprovalScope {
                actor: "worker".into(),
                ..scope()
            },
        ] {
            assert!(matches!(
                store.resolve_choice(&wrong, &request.approve_choice, 101),
                Err(ApprovalError::WrongScope)
            ));
            assert!(matches!(
                store.get(&wrong, &request.request_id, 101),
                Err(ApprovalError::WrongScope)
            ));
            assert!(store.list_pending(&wrong, 101).unwrap().is_empty());
            assert!(store.list_active(&wrong, 101).unwrap().is_empty());
        }
        store
            .resolve_choice(&scope(), &request.approve_choice, 102)
            .unwrap();
        let wrong = ApprovalScope {
            chat: "chat-2".into(),
            ..scope()
        };
        assert!(matches!(
            store.take_approved_action(&wrong, &request.request_id, 103),
            Err(ApprovalError::WrongScope)
        ));
        assert!(
            store
                .take_approved_action(&scope(), &request.request_id, 103)
                .is_ok()
        );
    }

    #[test]
    fn rejection_and_plain_yes_never_authorize_dispatch() {
        let (_tmp, store, request) = fixture();
        for text in ["yes", "approve", request.request_id.as_str()] {
            assert!(matches!(
                store.resolve_choice(&scope(), text, 101),
                Err(ApprovalError::NotFound)
            ));
        }
        assert!(matches!(
            store.take_approved_action(&scope(), &request.request_id, 101),
            Err(ApprovalError::NotApproved)
        ));
        assert_eq!(
            store
                .resolve_choice(&scope(), &request.reject_choice, 102)
                .unwrap()
                .status,
            ApprovalStatus::Rejected
        );
        assert!(matches!(
            store.resolve_choice(&scope(), &request.approve_choice, 103),
            Err(ApprovalError::AlreadyResolved)
        ));
        assert!(matches!(
            store.take_approved_action(&scope(), &request.request_id, 103),
            Err(ApprovalError::NotApproved)
        ));
        assert!(store.list_active(&scope(), 103).unwrap().is_empty());
    }

    #[test]
    fn expiry_is_enforced_at_resolution_and_consumption() {
        let (_tmp, store, request) = fixture();
        assert!(matches!(
            store.resolve_choice(&scope(), &request.approve_choice, 200),
            Err(ApprovalError::Expired)
        ));
        assert_eq!(
            store
                .get(&scope(), &request.request_id, 200)
                .unwrap()
                .status,
            ApprovalStatus::Expired
        );
        assert!(store.list_pending(&scope(), 200).unwrap().is_empty());
        assert!(store.list_active(&scope(), 200).unwrap().is_empty());
        // A fresh record approved immediately before expiry still cannot run at expiry.
        let (_tmp2, store2, request2) = fixture();
        store2
            .resolve_choice(&scope(), &request2.approve_choice, 199)
            .unwrap();
        assert!(matches!(
            store2.take_approved_action(&scope(), &request2.request_id, 200),
            Err(ApprovalError::Expired)
        ));
        assert!(store2.list_active(&scope(), 200).unwrap().is_empty());
    }

    #[test]
    fn changed_saved_arguments_and_binding_fail_closed() {
        for column_and_value in [
            ("args", r#"{"to":"mallory@example.com","body":"Changed"}"#),
            ("tool", "delete_account"),
            ("summary", "A different action"),
            ("chat", "chat-2"),
            ("expires_at", "999999"),
        ] {
            let (_tmp, store, request) = fixture();
            store
                .resolve_choice(&scope(), &request.approve_choice, 101)
                .unwrap();
            let query = format!(
                "UPDATE action_approvals SET {} = ?1 WHERE request_id = ?2",
                column_and_value.0
            );
            store
                .connection()
                .unwrap()
                .execute(&query, params![column_and_value.1, request.request_id])
                .unwrap();
            assert!(matches!(
                store.take_approved_action(&scope(), &request.request_id, 102),
                Err(ApprovalError::PayloadChanged)
            ));
        }
    }

    #[test]
    fn snapshot_never_contains_saved_tool_arguments() {
        let (_tmp, store, request) = fixture();
        let snapshot =
            serde_json::to_string(&store.get(&scope(), &request.request_id, 101).unwrap()).unwrap();
        assert!(!snapshot.contains("Approved body"));
        assert!(!snapshot.contains("alice@example.com"));
        assert!(!snapshot.contains("send_email"));
    }

    #[test]
    fn invalid_or_unbounded_requests_are_not_saved() {
        let tmp = tempfile::tempdir().unwrap();
        let store = ActionStore::open(tmp.path().join("actions.db")).unwrap();
        for (tool, args, expires_at) in [
            ("", json!({}), 200),
            ("send_email; delete", json!({}), 200),
            ("send_email", json!([]), 200),
            (
                "send_email",
                json!({"body": "x".repeat(MAX_ARGS_BYTES)}),
                200,
            ),
            ("send_email", json!({}), 100),
            ("send_email", json!({}), 100 + MAX_APPROVAL_LIFETIME + 1),
        ] {
            assert!(matches!(
                store.request_action(
                    scope(),
                    ActionPayload {
                        tool: tool.into(),
                        args,
                    },
                    "Send the prepared message",
                    expires_at,
                    100,
                ),
                Err(ApprovalError::InvalidRequest(_))
            ));
        }
        assert!(store.list_pending(&scope(), 101).unwrap().is_empty());
    }

    #[test]
    fn competing_dispatchers_can_take_an_action_only_once() {
        let (_tmp, store, request) = fixture();
        store
            .resolve_choice(&scope(), &request.approve_choice, 101)
            .unwrap();
        let barrier = Arc::new(Barrier::new(2));
        let threads = (0..2)
            .map(|_| {
                let store = store.clone();
                let request_id = request.request_id.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    store.take_approved_action(&scope(), &request_id, 102)
                })
            })
            .collect::<Vec<_>>();
        let results = threads
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(
            results
                .iter()
                .filter(|result| matches!(result, Err(ApprovalError::NotApproved)))
                .count(),
            1
        );
    }
}
