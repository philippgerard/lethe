use std::future::Future;
use std::pin::Pin;
use std::time::{Duration, Instant};

use serde_json::Value;
use uuid::Uuid;

use crate::actor::{
    ActorCompletionDelivery, ActorState, ActorToolCommand, Outcome, SpawnReport, SpawnSubagent,
};
use crate::tools::registry::ActorToolContext;
use crate::tools::registry::ToolRegistry;
use crate::tools::registry::args::{string_arg, string_arg_default, usize_arg};
use crate::tools::spec::{ToolCategory, ToolDef, ToolExecutor, p_enum, p_int, p_str, p_str_req};

const FRAMER_PROMPT: &str = include_str!("../../config/prompts/actor_research_framer.md");
const HYPOTHESIS_PROMPT: &str = include_str!("../../config/prompts/actor_research_hypothesis.md");
const JUDGE_PROMPT: &str = include_str!("../../config/prompts/actor_research_judge.md");

const MIN_HYPOTHESES: usize = 2;
const MAX_HYPOTHESES: usize = 5;
const DEFAULT_HYPOTHESES: usize = 3;
const POLL_INTERVAL: Duration = Duration::from_secs(2);
const FRAMER_MAX_TURNS: usize = 4;
const FRAMER_TIMEOUT: Duration = Duration::from_secs(2 * 60);
const JUDGE_MAX_TURNS: usize = 6;
const JUDGE_TIMEOUT: Duration = Duration::from_secs(3 * 60);
const DEPTH_VALUES: &[&str] = &["shallow", "deep"];
const MISSING_ACTOR_CONTEXT: &str = "Error: research tool only works inside an actor.";

#[derive(Clone, Copy)]
struct ResearchTimings {
    poll_interval: Duration,
    framer_timeout: Duration,
    shallow_hypothesis_timeout: Duration,
    deep_hypothesis_timeout: Duration,
    judge_timeout: Duration,
}

const DEFAULT_TIMINGS: ResearchTimings = ResearchTimings {
    poll_interval: POLL_INTERVAL,
    framer_timeout: FRAMER_TIMEOUT,
    shallow_hypothesis_timeout: Duration::from_secs(3 * 60),
    deep_hypothesis_timeout: Duration::from_secs(8 * 60),
    judge_timeout: JUDGE_TIMEOUT,
};

fn research_actor_request(
    actor_id: &str,
    name: String,
    goals: String,
    tools: &str,
    model: &str,
    max_turns: usize,
) -> SpawnSubagent {
    SpawnSubagent {
        actor_id: actor_id.to_string(),
        name,
        goals,
        group: None,
        tools: tools.to_string(),
        model: model.to_string(),
        max_turns,
        completion_delivery: ActorCompletionDelivery::PollOnly,
    }
}

fn exec_research<'a>(
    registry: &'a ToolRegistry<'a>,
    args: &'a Value,
) -> Pin<Box<dyn Future<Output = String> + Send + 'a>> {
    Box::pin(execute_research(registry, args))
}

async fn execute_research(registry: &ToolRegistry<'_>, args: &Value) -> String {
    execute_research_with_timings(registry, args, DEFAULT_TIMINGS).await
}

async fn execute_research_with_timings(
    registry: &ToolRegistry<'_>,
    args: &Value,
    timings: ResearchTimings,
) -> String {
    let Some(context) = registry.runtime.actor.as_ref() else {
        return MISSING_ACTOR_CONTEXT.to_string();
    };

    let question = string_arg(args, "question").trim().to_string();
    if question.is_empty() {
        return "Error: question is required.".to_string();
    }

    let depth = string_arg_default(args, "depth", "shallow");
    let max_turns_per_hyp = if depth == "deep" { 12 } else { 8 };
    let hyp_timeout = if depth == "deep" {
        timings.deep_hypothesis_timeout
    } else {
        timings.shallow_hypothesis_timeout
    };

    let session_id = short_id();

    let provided = string_arg_default(args, "hypotheses", "");
    let mut hypotheses: Vec<String> = if provided.trim().is_empty() {
        let n = usize_arg(args, "n", DEFAULT_HYPOTHESES);
        match generate_hypotheses(context, &question, n, &session_id, timings).await {
            Ok(v) => v,
            Err(e) => return format!("Research: framing failed. {e}"),
        }
    } else {
        match serde_json::from_str::<Vec<String>>(&provided) {
            Ok(v) => v
                .into_iter()
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect(),
            Err(error) => {
                return format!(
                    "Error: hypotheses must be a JSON array of strings. Parse error: {error}"
                );
            }
        }
    };

    if hypotheses.len() < MIN_HYPOTHESES {
        return format!(
            "Research: need at least {MIN_HYPOTHESES} hypotheses, got {}.",
            hypotheses.len()
        );
    }
    hypotheses.truncate(MAX_HYPOTHESES);

    let mut child_ids: Vec<String> = Vec::with_capacity(hypotheses.len());
    for (index, hypothesis) in hypotheses.iter().enumerate() {
        let goals = HYPOTHESIS_PROMPT
            .replace("{hypothesis}", hypothesis)
            .replace("{question}", &question);
        let name = format!("research-hyp-{}-{session_id}", index + 1);
        let spawn = context
            .runtime
            .spawn_subagent(research_actor_request(
                &context.actor_id,
                name.clone(),
                goals,
                "web_search,fetch_webpage",
                "aux",
                max_turns_per_hyp,
            ))
            .await;
        match spawn {
            Ok(SpawnReport::Spawned { actor_id, .. }) => child_ids.push(actor_id),
            Ok(SpawnReport::Rejected { message }) | Err(message) => {
                kill_running_poll_only_children(context, &child_ids).await;
                return format!(
                    "Research: spawning hypothesis {} failed: {message}",
                    index + 1
                );
            }
        }
    }

    let deadline = Instant::now() + hyp_timeout;
    let mut findings: Vec<Option<String>> = vec![None; child_ids.len()];
    loop {
        if findings.iter().all(Option::is_some) {
            break;
        }
        if Instant::now() >= deadline {
            break;
        }
        for (index, id) in child_ids.iter().enumerate() {
            if findings[index].is_some() {
                continue;
            }
            match context.runtime.actor_info(id).await {
                Some(info) if info.state == ActorState::Terminated => {
                    findings[index] =
                        Some(info.result.unwrap_or_else(|| "[no result]".to_string()));
                }
                Some(_) => {}
                None => {
                    findings[index] =
                        Some(format!("[hypothesis {} actor {id} disappeared]", index + 1));
                }
            }
        }
        if findings.iter().any(Option::is_none) {
            tokio::time::sleep(timings.poll_interval).await;
        }
    }

    if findings.iter().any(Option::is_none) {
        kill_running_poll_only_children(context, &child_ids).await;
    }

    let findings_str: Vec<String> = findings
        .into_iter()
        .enumerate()
        .map(|(index, opt)| {
            opt.unwrap_or_else(|| {
                format!(
                    "[hypothesis {} timed out after {} seconds]",
                    index + 1,
                    hyp_timeout.as_secs()
                )
            })
        })
        .collect();

    let findings_json = serde_json::to_string(&findings_str).unwrap_or_else(|_| "[]".to_string());
    let judge_goals = JUDGE_PROMPT
        .replace("{question}", &question)
        .replace("{findings}", &findings_json);
    let judge_name = format!("research-judge-{session_id}");
    let spawn = context
        .runtime
        .spawn_subagent(research_actor_request(
            &context.actor_id,
            judge_name,
            judge_goals,
            "",
            "main",
            JUDGE_MAX_TURNS,
        ))
        .await;
    let judge_id = match spawn {
        Ok(SpawnReport::Spawned { actor_id, .. }) => actor_id,
        Ok(SpawnReport::Rejected { message }) | Err(message) => {
            return format!(
                "Research: judge spawn failed: {message}\n\nRaw findings:\n{findings_json}"
            );
        }
    };

    match poll_until_terminated(
        context,
        &judge_id,
        timings.judge_timeout,
        timings.poll_interval,
    )
    .await
    {
        Ok((verdict, _)) => verdict,
        Err(error) => {
            kill_running_poll_only_children(context, std::slice::from_ref(&judge_id)).await;
            format!("Research: judge failed: {error}\n\nRaw findings:\n{findings_json}")
        }
    }
}

async fn generate_hypotheses(
    context: &ActorToolContext,
    question: &str,
    n: usize,
    session_id: &str,
    timings: ResearchTimings,
) -> Result<Vec<String>, String> {
    let n = n.clamp(MIN_HYPOTHESES, MAX_HYPOTHESES);
    let goals = format!(
        "{FRAMER_PROMPT}\n\n---\n\nQuestion: {question}\n\nProduce exactly {n} hypotheses."
    );
    let name = format!("research-framer-{session_id}");
    let spawn = context
        .runtime
        .spawn_subagent(research_actor_request(
            &context.actor_id,
            name,
            goals,
            "",
            "aux",
            FRAMER_MAX_TURNS,
        ))
        .await;
    let framer_id = match spawn {
        Ok(SpawnReport::Spawned { actor_id, .. }) => actor_id,
        Ok(SpawnReport::Rejected { message }) | Err(message) => {
            return Err(format!("Framer spawn failed: {message}"));
        }
    };
    let (result, _outcome) = match poll_until_terminated(
        context,
        &framer_id,
        timings.framer_timeout,
        timings.poll_interval,
    )
    .await
    {
        Ok(result) => result,
        Err(error) => {
            kill_running_poll_only_children(context, std::slice::from_ref(&framer_id)).await;
            return Err(error);
        }
    };
    parse_framer_hypotheses(&result)
}

async fn kill_running_poll_only_children(context: &ActorToolContext, actor_ids: &[String]) {
    for actor_id in actor_ids {
        let Some(info) = context.runtime.actor_info(actor_id).await else {
            continue;
        };
        if info.state == ActorState::Terminated {
            continue;
        }
        let result = context
            .runtime
            .execute_actor_tool(ActorToolCommand::KillActor {
                actor_id: context.actor_id.clone(),
                target_id: actor_id.clone(),
            })
            .await;
        if result.starts_with("Error:") || result.starts_with("Cannot kill") {
            tracing::warn!(actor_id, result, "failed to clean up research child");
        }
    }
}

fn parse_framer_hypotheses(text: &str) -> Result<Vec<String>, String> {
    let trimmed = text.trim();
    let json_start = trimmed.find('{');
    let json_end = trimmed.rfind('}');
    let json_str = match (json_start, json_end) {
        (Some(start), Some(end)) if end >= start => &trimmed[start..=end],
        _ => return Err(format!("Framer returned no JSON object. Raw: {trimmed}")),
    };
    let value: Value = serde_json::from_str(json_str)
        .map_err(|error| format!("Framer JSON parse error: {error}. Raw: {json_str}"))?;
    let array = value
        .get("hypotheses")
        .and_then(Value::as_array)
        .ok_or_else(|| format!("Framer JSON missing 'hypotheses' array. Got: {value}"))?;
    let result: Vec<String> = array
        .iter()
        .filter_map(|value| value.as_str().map(|s| s.trim().to_string()))
        .filter(|s| !s.is_empty())
        .collect();
    if result.is_empty() {
        return Err("Framer returned empty hypotheses array.".to_string());
    }
    Ok(result)
}

async fn poll_until_terminated(
    context: &ActorToolContext,
    actor_id: &str,
    timeout: Duration,
    poll_interval: Duration,
) -> Result<(String, Outcome), String> {
    let deadline = Instant::now() + timeout;
    loop {
        if Instant::now() >= deadline {
            return Err(format!("timed out after {} seconds", timeout.as_secs()));
        }
        match context.runtime.actor_info(actor_id).await {
            Some(info) if info.state == ActorState::Terminated => {
                let outcome = info.outcome.unwrap_or(Outcome::Success);
                return Ok((
                    info.result.unwrap_or_else(|| "no result".to_string()),
                    outcome,
                ));
            }
            Some(_) => tokio::time::sleep(poll_interval).await,
            None => return Err(format!("actor {actor_id} disappeared")),
        }
    }
}

fn short_id() -> String {
    Uuid::new_v4().to_string()[..6].to_string()
}

pub const TOOL_DEFS: &[ToolDef] = &[ToolDef {
    name: "research",
    description: "Investigate a question by spawning N parallel hypothesis subagents (web-search-enabled) and a judge that selects or synthesizes. Returns the judge's JSON verdict.",
    params: &[
        p_str_req("question", "The research question to investigate."),
        p_str(
            "hypotheses",
            "JSON array of 2-5 hypothesis strings. If omitted, a framer subagent generates them.",
        ),
        p_int(
            "n",
            "How many hypotheses to generate when 'hypotheses' is omitted (default 3, clamped 2-5).",
        ),
        p_enum(
            "depth",
            "shallow (max 8 turns per hypothesis) or deep (max 12).",
            DEPTH_VALUES,
        ),
    ],
    category: ToolCategory::CortexOnly,
    execute: ToolExecutor::Async(exec_research),
}];

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};

    use serde_json::json;
    use tempfile::{TempDir, tempdir};
    use tokio::time::timeout;

    use super::*;
    use crate::actor::{
        ActorConfig, ActorInfo, ActorRegistry, ActorResult, ActorRunSpec, ActorRuntime,
        ActorToolCommand,
    };
    use crate::memory::MemoryStore;
    use crate::tools::registry::ToolRuntime;
    use crate::tools::shell::ShellTools;

    const TEST_TIMINGS: ResearchTimings = ResearchTimings {
        poll_interval: Duration::from_millis(1),
        framer_timeout: Duration::from_millis(25),
        shallow_hypothesis_timeout: Duration::from_millis(25),
        deep_hypothesis_timeout: Duration::from_millis(25),
        judge_timeout: Duration::from_millis(25),
    };

    struct ResearchHarness {
        _tmp: TempDir,
        workspace: PathBuf,
        cache: PathBuf,
        memory: MemoryStore,
        shell: ShellTools,
        runtime: ActorRuntime,
        principal: String,
        preexisting_children: Vec<String>,
    }

    impl ResearchHarness {
        fn new(preexisting_children: usize) -> Self {
            let tmp = tempdir().unwrap();
            let workspace = tmp.path().join("workspace");
            let memory = MemoryStore::open(
                &workspace,
                tmp.path().join("data/lethe.db"),
                workspace.join("notes"),
            )
            .unwrap();
            let shell = ShellTools::new(&workspace);
            let mut actors = ActorRegistry::new();
            let principal = actors.spawn(
                ActorConfig::new("cortex", "Serve the user").in_group("main"),
                None,
                true,
            );
            let preexisting_children = (0..preexisting_children)
                .map(|index| {
                    actors.spawn(
                        ActorConfig::new(format!("existing-{index}"), "Unrelated existing work")
                            .in_group("main"),
                        Some(&principal),
                        false,
                    )
                })
                .collect();
            let runtime = ActorRuntime::new(actors);

            Self {
                cache: tmp.path().join("cache"),
                _tmp: tmp,
                workspace,
                memory,
                shell,
                runtime,
                principal,
                preexisting_children,
            }
        }

        fn registry(&self) -> ToolRegistry<'_> {
            ToolRegistry::with_runtime(
                &self.memory,
                &self.workspace,
                &self.cache,
                &self.shell,
                ToolRuntime {
                    actor: Some(ActorToolContext {
                        runtime: self.runtime.clone(),
                        actor_id: self.principal.clone(),
                        is_subagent: false,
                    }),
                    ..ToolRuntime::default()
                },
            )
        }

        async fn actors_with_prefix(&self, prefix: &str) -> Vec<ActorInfo> {
            self.runtime
                .list_actors()
                .await
                .unwrap()
                .into_iter()
                .filter(|actor| actor.name.starts_with(prefix))
                .collect()
        }
    }

    impl Drop for ResearchHarness {
        fn drop(&mut self) {
            self.runtime.shutdown();
        }
    }

    async fn terminate_success(
        spec: ActorRunSpec,
        runtime: ActorRuntime,
        result: String,
    ) -> ActorResult<String> {
        let _ = runtime
            .execute_actor_tool(ActorToolCommand::Terminate {
                actor_id: spec.actor_id,
                result: result.clone(),
                outcome: "success".to_string(),
                files_touched: String::new(),
                follow_up: String::new(),
            })
            .await;
        Ok(result)
    }

    #[test]
    fn research_actors_use_poll_only_completion_delivery() {
        let request = research_actor_request(
            "cortex",
            "research-framer-test".to_string(),
            "Frame the question".to_string(),
            "",
            "aux",
            4,
        );

        assert_eq!(
            request.completion_delivery,
            ActorCompletionDelivery::PollOnly
        );
    }

    #[tokio::test]
    async fn partial_hypothesis_spawn_failure_kills_the_already_spawned_child() {
        let harness = ResearchHarness::new(4);
        harness
            .runtime
            .install_turn_executor(Arc::new(|_spec, _runtime| {
                Box::pin(async move {
                    std::future::pending::<()>().await;
                    unreachable!()
                })
            }))
            .unwrap();
        let registry = harness.registry();

        let output = timeout(
            Duration::from_secs(1),
            execute_research_with_timings(
                &registry,
                &json!({
                    "question": "Which hypothesis survives?",
                    "hypotheses": r#"["first","second"]"#,
                }),
                TEST_TIMINGS,
            ),
        )
        .await
        .expect("partial-spawn failure returned");

        assert!(output.contains("spawning hypothesis 2 failed"), "{output}");
        let children = harness.actors_with_prefix("research-hyp-").await;
        assert_eq!(children.len(), 1);
        assert_eq!(children[0].state, ActorState::Terminated);
        assert_eq!(children[0].outcome, Some(Outcome::Killed));
        for actor_id in &harness.preexisting_children {
            assert_ne!(
                harness.runtime.actor_info(actor_id).await.unwrap().state,
                ActorState::Terminated,
                "cleanup must not kill unrelated pre-existing children"
            );
        }
    }

    #[tokio::test]
    async fn framer_timeout_kills_the_blocked_poll_only_child() {
        let harness = ResearchHarness::new(0);
        harness
            .runtime
            .install_turn_executor(Arc::new(|_spec, _runtime| {
                Box::pin(async move {
                    std::future::pending::<()>().await;
                    unreachable!()
                })
            }))
            .unwrap();
        let registry = harness.registry();

        let output = timeout(
            Duration::from_secs(1),
            execute_research_with_timings(
                &registry,
                &json!({"question": "Frame this", "n": 2}),
                TEST_TIMINGS,
            ),
        )
        .await
        .expect("framer timeout returned");

        assert!(output.contains("framing failed"), "{output}");
        assert!(output.contains("timed out"), "{output}");
        let framers = harness.actors_with_prefix("research-framer-").await;
        assert_eq!(framers.len(), 1);
        assert_eq!(framers[0].state, ActorState::Terminated);
        assert_eq!(framers[0].outcome, Some(Outcome::Killed));
    }

    #[tokio::test]
    async fn hypothesis_timeout_kills_all_blocked_children_before_judging() {
        let harness = ResearchHarness::new(0);
        harness
            .runtime
            .install_turn_executor(Arc::new(|spec, runtime| {
                Box::pin(async move {
                    if spec.name.starts_with("research-hyp-") {
                        std::future::pending::<()>().await;
                        unreachable!()
                    }
                    terminate_success(spec, runtime, r#"{"verdict":"timeouts noted"}"#.to_string())
                        .await
                })
            }))
            .unwrap();
        let registry = harness.registry();

        let output = timeout(
            Duration::from_secs(1),
            execute_research_with_timings(
                &registry,
                &json!({
                    "question": "Which hypothesis survives?",
                    "hypotheses": r#"["first","second"]"#,
                }),
                TEST_TIMINGS,
            ),
        )
        .await
        .expect("hypothesis timeout returned");

        assert_eq!(output, r#"{"verdict":"timeouts noted"}"#);
        let hypotheses = harness.actors_with_prefix("research-hyp-").await;
        assert_eq!(hypotheses.len(), 2);
        assert!(hypotheses.iter().all(|actor| {
            actor.state == ActorState::Terminated && actor.outcome == Some(Outcome::Killed)
        }));
    }

    #[tokio::test]
    async fn judge_timeout_kills_the_blocked_poll_only_child() {
        let harness = ResearchHarness::new(0);
        harness
            .runtime
            .install_turn_executor(Arc::new(|spec, runtime| {
                Box::pin(async move {
                    if spec.name.starts_with("research-judge-") {
                        std::future::pending::<()>().await;
                        unreachable!()
                    }
                    let result = format!("finding from {}", spec.name);
                    terminate_success(spec, runtime, result).await
                })
            }))
            .unwrap();
        let registry = harness.registry();

        let output = timeout(
            Duration::from_secs(1),
            execute_research_with_timings(
                &registry,
                &json!({
                    "question": "Which hypothesis survives?",
                    "hypotheses": r#"["first","second"]"#,
                }),
                TEST_TIMINGS,
            ),
        )
        .await
        .expect("judge timeout returned");

        assert!(output.contains("judge failed: timed out"), "{output}");
        let judges = harness.actors_with_prefix("research-judge-").await;
        assert_eq!(judges.len(), 1);
        assert_eq!(judges[0].state, ActorState::Terminated);
        assert_eq!(judges[0].outcome, Some(Outcome::Killed));
    }

    #[tokio::test]
    async fn research_orchestrator_keeps_every_internal_actor_poll_only() {
        let tmp = tempdir().unwrap();
        let workspace = tmp.path().join("workspace");
        let memory = MemoryStore::open(
            &workspace,
            tmp.path().join("data/lethe.db"),
            workspace.join("notes"),
        )
        .unwrap();
        let shell = ShellTools::new(&workspace);

        let mut actors = ActorRegistry::new();
        let principal = actors.spawn(
            ActorConfig::new("cortex", "Serve the user").in_group("main"),
            None,
            true,
        );
        let runtime = ActorRuntime::new(actors);
        let observed = Arc::new(Mutex::new(Vec::<(String, ActorCompletionDelivery)>::new()));
        let observed_by_executor = observed.clone();
        runtime
            .install_turn_executor(Arc::new(move |spec, runtime| {
                let observed = observed_by_executor.clone();
                Box::pin(async move {
                    observed
                        .lock()
                        .unwrap()
                        .push((spec.name.clone(), spec.completion_delivery));
                    let result = if spec.name.starts_with("research-framer-") {
                        r#"{"hypotheses":["first","second"]}"#.to_string()
                    } else if spec.name.starts_with("research-judge-") {
                        r#"{"verdict":"integrated"}"#.to_string()
                    } else {
                        format!("finding from {}", spec.name)
                    };
                    let _ = runtime
                        .execute_actor_tool(ActorToolCommand::Terminate {
                            actor_id: spec.actor_id,
                            result: result.clone(),
                            outcome: "success".to_string(),
                            files_touched: String::new(),
                            follow_up: String::new(),
                        })
                        .await;
                    Ok(result)
                })
            }))
            .unwrap();

        let registry = ToolRegistry::with_runtime(
            &memory,
            &workspace,
            tmp.path().join("cache"),
            &shell,
            ToolRuntime {
                actor: Some(ActorToolContext {
                    runtime: runtime.clone(),
                    actor_id: principal.clone(),
                    is_subagent: false,
                }),
                ..ToolRuntime::default()
            },
        );
        let output = tokio::time::timeout(
            Duration::from_secs(10),
            execute_research(
                &registry,
                &json!({
                    "question": "Which hypothesis survives?",
                    "n": 2,
                    "depth": "shallow"
                }),
            ),
        )
        .await
        .expect("research orchestration completed");

        assert_eq!(output, r#"{"verdict":"integrated"}"#);
        let observed = observed.lock().unwrap();
        assert_eq!(observed.len(), 4, "framer + two hypotheses + judge");
        assert_eq!(
            observed
                .iter()
                .filter(|(name, _)| name.starts_with("research-framer-"))
                .count(),
            1
        );
        assert_eq!(
            observed
                .iter()
                .filter(|(name, _)| name.starts_with("research-hyp-"))
                .count(),
            2
        );
        assert_eq!(
            observed
                .iter()
                .filter(|(name, _)| name.starts_with("research-judge-"))
                .count(),
            1
        );
        assert!(
            observed
                .iter()
                .all(|(_, delivery)| *delivery == ActorCompletionDelivery::PollOnly)
        );
        drop(observed);
        assert!(runtime.pop_inbox(&principal).await.is_none());
        assert!(
            runtime
                .principal_task_update_events(&principal, 10)
                .await
                .unwrap()
                .is_empty()
        );
    }
}
