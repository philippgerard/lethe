//! Trusted transport-scoped decisions. The model can propose an exact action,
//! but only the verified ingress can resolve it; execution consumes it once.

use std::collections::HashSet;
use std::sync::Arc;

use serde_json::{Value, json};

use crate::interfaces::actions::{ActionPayload, ActionStore, ApprovalScope};
use crate::tools::registry::ToolRegistry;
use crate::tools::registry::args::{string_arg, usize_arg};
use crate::tools::spec::{
    ParamKind, ParamSpec, ToolCategory, ToolDef, ToolExecutor, p_int, p_str_req,
};

#[derive(Clone)]
pub struct ActionToolContext {
    pub store: ActionStore,
    pub scope: ApprovalScope,
    emit: Arc<dyn Fn(Value) -> bool + Send + Sync>,
}

impl ActionToolContext {
    pub fn new(
        store: ActionStore,
        scope: ApprovalScope,
        emit: impl Fn(Value) -> bool + Send + Sync + 'static,
    ) -> Self {
        Self {
            store,
            scope,
            emit: Arc::new(emit),
        }
    }
}

impl std::fmt::Debug for ActionToolContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ActionToolContext").finish_non_exhaustive()
    }
}

fn request_action(registry: &ToolRegistry<'_>, args: &Value) -> String {
    let Some(context) = registry.runtime.actions.as_ref() else {
        return "Error: no trusted decision transport is attached.".to_string();
    };
    let tool = string_arg(args, "tool");
    if matches!(
        tool.as_str(),
        "request_action_approval" | "execute_approved_action"
    ) || !registry.tool_is_available(&tool)
    {
        return "Error: the target tool is unavailable or cannot be approved.".to_string();
    }
    let Some(payload_args) = args.get("args").filter(|value| value.is_object()) else {
        return "Error: args must be the exact target tool argument object.".to_string();
    };
    let now = chrono::Utc::now().timestamp();
    let ttl = usize_arg(args, "expires_in_seconds", 600).clamp(30, 1800) as i64;
    let result = context.store.request_action(
        context.scope.clone(),
        ActionPayload {
            tool,
            args: payload_args.clone(),
        },
        &string_arg(args, "summary"),
        now + ttl,
        now,
    );
    match result {
        Ok(request) => {
            let event = json!({"kind": "approval", "request": request});
            if !(context.emit)(event) {
                return "Error: approval is stored but its prompt could not be queued; stop and report the delivery failure.".to_string();
            }
            json!({
                "request_id": request.request_id,
                "status": "pending",
                "instruction": "The exact action is parked. Stop; only a verified user decision can authorize execute_approved_action. Do not invoke the target directly."
            })
            .to_string()
        }
        Err(error) => format!("Error: {error}"),
    }
}

fn execute_action<'a>(
    registry: &'a ToolRegistry<'a>,
    args: &'a Value,
) -> crate::tools::registry::BoxToolFuture<'a> {
    Box::pin(async move {
        let Some(context) = registry.runtime.actions.as_ref() else {
            return "Error: no trusted decision transport is attached.".to_string();
        };
        let request_id = string_arg(args, "request_id");
        let payload = match context.store.take_approved_action(
            &context.scope,
            &request_id,
            chrono::Utc::now().timestamp(),
        ) {
            Ok(payload) => payload,
            Err(error) => return format!("Error: {error}"),
        };
        if matches!(
            payload.tool.as_str(),
            "request_action_approval" | "execute_approved_action"
        ) || !registry.tool_is_available(&payload.tool)
        {
            return "Error: approved target is no longer available; the request is consumed and will not be retried.".to_string();
        }
        let output = registry.execute_async(&payload.tool, &payload.args).await;
        json!({"request_id": request_id, "status": "consumed", "output": output}).to_string()
    })
}

fn send_choices(registry: &ToolRegistry<'_>, args: &Value) -> String {
    let Some(context) = registry.runtime.actions.as_ref() else {
        return "Error: no trusted decision transport is attached.".to_string();
    };
    let question = string_arg(args, "question");
    let options = args.get("options").and_then(Value::as_array);
    let Some(options) = options else {
        return "Error: options must contain two to four short strings.".to_string();
    };
    let mut labels = HashSet::new();
    if question.is_empty()
        || question.chars().count() > 1500
        || !(2..=4).contains(&options.len())
        || options.iter().any(|option| {
            option.as_str().is_none_or(|text| {
                text.trim().is_empty()
                    || text.chars().count() > 80
                    || !labels.insert(text.trim().to_lowercase())
            })
        })
    {
        return "Error: supply a question and two to four short, distinct, non-secret options."
            .to_string();
    }
    if !(context.emit)(json!({"kind": "choices", "question": question, "options": options})) {
        return "Error: the choice prompt could not be queued.".to_string();
    }
    json!({"success": true, "instruction": "The choice prompt is queued; stop and await the user's reply. A choice does not approve an external action."}).to_string()
}

pub const TOOL_DEFS: &[ToolDef] = &[
    ToolDef {
        name: "request_action_approval",
        description: "Park one exact consequential tool call and show its material terms to the user. Include merchant, item, quantity, option, total/currency, recipient or destructive effect as applicable in summary. Never include raw credentials. This returns pending, not permission: stop and wait. The verified transport binds the decision to this user, chat, immutable payload and expiry.",
        params: &[
            p_str_req("tool", "Exact available target tool name."),
            ParamSpec {
                name: "args",
                kind: ParamKind::Object,
                description: "Exact target arguments, saved immutably. Never include raw secrets.",
                required: true,
            },
            p_str_req(
                "summary",
                "Complete, human-readable action and material terms.",
            ),
            p_int(
                "expires_in_seconds",
                "Validity in seconds, 30–1800 (default 600).",
            ),
        ],
        category: ToolCategory::Actions,
        execute: ToolExecutor::Sync(request_action),
    },
    ToolDef {
        name: "execute_approved_action",
        description: "Execute the saved exact action once after verified approval. Accepts only request_id, never replacement arguments. Recheck browser state and material terms before execution; if they changed, request fresh approval. Consumed requests cannot be replayed even after an uncertain outcome or restart.",
        params: &[p_str_req(
            "request_id",
            "Approved request ID returned by the transport.",
        )],
        category: ToolCategory::Actions,
        execute: ToolExecutor::Async(execute_action),
    },
    ToolDef {
        name: "chat_send_choices",
        description: "Ask the current user to choose between two to four short options. The transport uses native iMessage polls when enabled and supported, otherwise text. Stop after asking. This is a preference question, not authorization for a consequential action; use request_action_approval for that.",
        params: &[
            p_str_req("question", "The question shown before the choices."),
            ParamSpec {
                name: "options",
                kind: ParamKind::StringArray,
                description: "Two to four short, distinct answer labels.",
                required: true,
            },
        ],
        category: ToolCategory::Actions,
        execute: ToolExecutor::Sync(send_choices),
    },
];

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Mutex;

    use crate::interfaces::actions::{ApprovalRequest, ApprovalStatus};
    use crate::memory::MemoryStore;
    use crate::tools::registry::ToolRuntime;
    use crate::tools::shell::ShellTools;

    use super::*;

    struct Fixture {
        directory: tempfile::TempDir,
        memory: MemoryStore,
        shell: ShellTools,
        store: ActionStore,
        events: Arc<Mutex<Vec<Value>>>,
    }

    impl Fixture {
        fn new() -> Self {
            let directory = tempfile::tempdir().unwrap();
            let workspace = directory.path().join("workspace");
            let memory = MemoryStore::open(
                &workspace,
                directory.path().join("data/lethe.db"),
                workspace.join("notes"),
            )
            .unwrap();
            Self {
                shell: ShellTools::new(&workspace),
                store: ActionStore::open(directory.path().join("actions.sqlite")).unwrap(),
                directory,
                memory,
                events: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn scope(&self) -> ApprovalScope {
            ApprovalScope {
                owner: "linq".into(),
                chat: "chat-1".into(),
                actor: "+49123456789".into(),
            }
        }

        fn context(&self, scope: ApprovalScope) -> ActionToolContext {
            let events = self.events.clone();
            ActionToolContext::new(self.store.clone(), scope, move |event| {
                events.lock().unwrap().push(event);
                true
            })
        }

        fn registry(&self, context: Option<ActionToolContext>) -> ToolRegistry<'_> {
            ToolRegistry::with_runtime(
                &self.memory,
                self.memory.workspace_dir(),
                self.directory.path().join("cache"),
                &self.shell,
                ToolRuntime {
                    actions: context,
                    ..ToolRuntime::default()
                },
            )
        }

        fn propose(&self, registry: &ToolRegistry<'_>) -> ApprovalRequest {
            let output = registry.execute(
                "request_action_approval",
                &json!({
                    "tool": "write_file",
                    "args": {"file_path": "approved.txt", "content": "EXACT_SAVED_CONTENT"},
                    "summary": "Write the reviewed draft to approved.txt",
                }),
            );
            let response: Value = serde_json::from_str(&output).unwrap();
            assert_eq!(response["status"], "pending");
            let event = self.events.lock().unwrap().last().unwrap().clone();
            assert_eq!(event["kind"], "approval");
            let request: ApprovalRequest =
                serde_json::from_value(event["request"].clone()).unwrap();
            assert_eq!(response["request_id"], request.request_id);
            request
        }
    }

    #[tokio::test]
    async fn absent_decision_context_hides_schemas_and_refuses_dispatch() {
        let fixture = Fixture::new();
        let registry = fixture.registry(None);
        let names = registry
            .tools_for_active(&HashSet::new())
            .into_iter()
            .map(|tool| tool.name)
            .collect::<Vec<_>>();
        for tool in [
            "request_action_approval",
            "execute_approved_action",
            "chat_send_choices",
        ] {
            assert!(!names.iter().any(|name| name.as_str() == tool));
            assert!(!registry.tool_is_available(tool));
            assert!(!registry.requestable_tools_directory().contains(tool));
            assert!(
                registry
                    .execute_async(tool, &json!({}))
                    .await
                    .contains("no trusted decision transport")
            );
        }
        assert!(fixture.events.lock().unwrap().is_empty());
        assert!(
            fixture
                .store
                .list_active(&fixture.scope(), chrono::Utc::now().timestamp())
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn unavailable_and_recursive_targets_cannot_create_requests() {
        let fixture = Fixture::new();
        let registry = fixture.registry(Some(fixture.context(fixture.scope())));
        for tool in [
            "missing_tool",
            "telegram_send_message",
            "request_action_approval",
            "execute_approved_action",
        ] {
            let output = registry.execute(
                "request_action_approval",
                &json!({"tool": tool, "args": {}, "summary": "Perform the proposed action"}),
            );
            assert!(output.contains("unavailable or cannot be approved"));
        }
        assert!(fixture.events.lock().unwrap().is_empty());
        assert!(
            fixture
                .store
                .list_active(&fixture.scope(), chrono::Utc::now().timestamp())
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn proposal_persists_a_safe_event_without_executing_the_target() {
        let fixture = Fixture::new();
        let registry = fixture.registry(Some(fixture.context(fixture.scope())));
        let request = fixture.propose(&registry);
        let events = fixture.events.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert!(!events[0].to_string().contains("EXACT_SAVED_CONTENT"));
        assert_eq!(
            fixture
                .store
                .get(
                    &fixture.scope(),
                    &request.request_id,
                    chrono::Utc::now().timestamp()
                )
                .unwrap(),
            request
        );
        assert!(!fixture.memory.workspace_dir().join("approved.txt").exists());
    }

    #[tokio::test]
    async fn wrapper_requires_approval_and_uses_saved_arguments_only_once() {
        let fixture = Fixture::new();
        let registry = fixture.registry(Some(fixture.context(fixture.scope())));
        let request = fixture.propose(&registry);
        let target = fixture.memory.workspace_dir().join("approved.txt");
        let execute_args = json!({
            "request_id": request.request_id,
            "args": {"file_path": "replacement.txt", "content": "CHANGED_ARGUMENTS"},
        });
        let pending = registry
            .execute_async("execute_approved_action", &execute_args)
            .await;
        assert!(pending.contains("not approved"));
        assert!(!target.exists());
        fixture
            .store
            .resolve_choice(
                &fixture.scope(),
                &request.approve_choice,
                chrono::Utc::now().timestamp(),
            )
            .unwrap();
        let result = registry
            .execute_async("execute_approved_action", &execute_args)
            .await;
        let result: Value = serde_json::from_str(&result).unwrap();
        assert_eq!(result["status"], "consumed");
        assert!(
            result["output"]
                .as_str()
                .unwrap()
                .contains("Successfully wrote")
        );
        assert_eq!(
            std::fs::read_to_string(&target).unwrap(),
            "EXACT_SAVED_CONTENT"
        );
        assert!(
            !fixture
                .memory
                .workspace_dir()
                .join("replacement.txt")
                .exists()
        );
        // A repeated filesystem write would replace this sentinel even though
        // the original write itself is idempotent; keep the effect observable.
        std::fs::write(&target, "AFTER_FIRST_DISPATCH").unwrap();
        let replay = registry
            .execute_async("execute_approved_action", &execute_args)
            .await;
        assert!(replay.contains("already been consumed"));
        assert_eq!(
            std::fs::read_to_string(&target).unwrap(),
            "AFTER_FIRST_DISPATCH"
        );
        assert_eq!(
            fixture
                .store
                .get(
                    &fixture.scope(),
                    &request.request_id,
                    chrono::Utc::now().timestamp()
                )
                .unwrap()
                .status,
            ApprovalStatus::Consumed
        );
    }

    #[tokio::test]
    async fn wrapper_cannot_dispatch_another_chat_or_actors_approval() {
        let fixture = Fixture::new();
        let registry = fixture.registry(Some(fixture.context(fixture.scope())));
        let request = fixture.propose(&registry);
        fixture
            .store
            .resolve_choice(
                &fixture.scope(),
                &request.approve_choice,
                chrono::Utc::now().timestamp(),
            )
            .unwrap();
        for scope in [
            ApprovalScope {
                chat: "chat-2".into(),
                ..fixture.scope()
            },
            ApprovalScope {
                actor: "+49888888888".into(),
                ..fixture.scope()
            },
        ] {
            let other = fixture.registry(Some(fixture.context(scope)));
            let result = other
                .execute_async(
                    "execute_approved_action",
                    &json!({"request_id": request.request_id}),
                )
                .await;
            assert!(result.contains("different owner, chat, or actor"));
            assert!(!fixture.memory.workspace_dir().join("approved.txt").exists());
        }
        assert_eq!(
            fixture
                .store
                .get(
                    &fixture.scope(),
                    &request.request_id,
                    chrono::Utc::now().timestamp()
                )
                .unwrap()
                .status,
            ApprovalStatus::Approved
        );
    }

    #[test]
    fn failed_prompt_delivery_leaves_an_inspectable_unapproved_request() {
        let fixture = Fixture::new();
        let context = ActionToolContext::new(fixture.store.clone(), fixture.scope(), |_| false);
        let registry = fixture.registry(Some(context));
        let output = registry.execute(
            "request_action_approval",
            &json!({
                "tool": "write_file", "args": {"file_path": "approved.txt", "content": "Draft"},
                "summary": "Write the reviewed draft to approved.txt",
            }),
        );
        assert!(output.contains("prompt could not be queued"));
        let pending = fixture
            .store
            .list_pending(&fixture.scope(), chrono::Utc::now().timestamp())
            .unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].status, ApprovalStatus::Pending);
        assert!(!fixture.memory.workspace_dir().join("approved.txt").exists());
    }

    #[test]
    fn choice_labels_must_be_distinct_before_the_prompt_is_queued() {
        let fixture = Fixture::new();
        let registry = fixture.registry(Some(fixture.context(fixture.scope())));
        for options in [
            json!(["Friday", "Friday"]),
            json!([" Friday", "Friday "]),
            json!(["Friday", "FRIDAY"]),
        ] {
            let result = registry.execute(
                "chat_send_choices",
                &json!({"question": "Which day?", "options": options}),
            );
            assert!(result.contains("distinct"));
            assert!(fixture.events.lock().unwrap().is_empty());
        }
        let result = registry.execute(
            "chat_send_choices",
            &json!({"question": "Which day?", "options": ["Friday", "Saturday"]}),
        );
        let result: Value = serde_json::from_str(&result).unwrap();
        assert_eq!(result["success"], true);
        let events = fixture.events.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["kind"], "choices");
        assert_eq!(events[0]["options"], json!(["Friday", "Saturday"]));
        assert!(
            fixture
                .store
                .list_active(&fixture.scope(), chrono::Utc::now().timestamp())
                .unwrap()
                .is_empty()
        );
    }
}
