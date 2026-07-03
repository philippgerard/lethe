//! Async bridge to the agent-id CLIs (`agent-id-core`, `agent-id-vault`,
//! `agent-id-browser`).
//!
//! Every call is `tokio::process` with `kill_on_drop(true)` so a cancelled turn
//! (the tool future is dropped at an `.await`) actually kills the child — the
//! secure-prompt-bearing subcommands can block for many minutes waiting on the
//! human, and must die when the user hits Stop. Secrets are never passed on the
//! argv; the only secret channel is the unix socket (see `secure_prompt`).

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use tokio::process::Command;

use super::secure_prompt::SecurePromptHub;

/// Budget for fast, non-interactive calls (status, list, init, sign).
pub const FAST_TIMEOUT: Duration = Duration::from_secs(60);
/// Budget for subcommands that can block on a human typing a secret. Must exceed
/// the secure-prompt server's 15-minute deadline plus slack so Lethe never kills
/// the child mid-wait (which would drop the secret while the card still shows).
pub const HUMAN_TIMEOUT: Duration = Duration::from_secs(16 * 60);

/// Which agent-id CLI to invoke.
#[derive(Clone, Copy)]
pub enum Bin {
    Core,
    Vault,
    Browser,
}

impl Bin {
    fn resolve(self) -> Option<PathBuf> {
        let (env_key, name) = match self {
            Bin::Core => ("AGENT_ID_CORE_BIN", "agent-id-core"),
            Bin::Vault => ("AGENT_ID_VAULT_BIN", "agent-id-vault"),
            Bin::Browser => ("AGENT_ID_BROWSER_BIN", "agent-id-browser"),
        };
        super::find_bin(env_key, name)
    }
}

/// A parsed CLI result. Non-zero exit or unparseable stdout is surfaced as an
/// error the tool layer turns into a JSON `{error}` for the model.
pub struct CliResult {
    pub json: Value,
    pub code: i32,
}

fn base_command(bin: Bin, state_dir: &Path) -> Result<Command, String> {
    let program = bin
        .resolve()
        .ok_or_else(|| "agent-id CLI not found on PATH".to_string())?;
    let mut cmd = Command::new(program);
    cmd.env("AGENT_ID_STATE_DIR", state_dir)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    Ok(cmd)
}

/// Run a fast, non-interactive subcommand and parse its JSON stdout.
pub async fn run(bin: Bin, state_dir: &Path, args: &[&str]) -> Result<CliResult, String> {
    let mut cmd = base_command(bin, state_dir)?;
    cmd.args(args);
    run_command(cmd, FAST_TIMEOUT).await
}

/// Run a secure-prompt-bearing subcommand. When a hub is present (hosted mode),
/// wire the child to the socket and authorize its PID so its `collectSecret`
/// resolver can raise a frontend card; without a hub (local mode), the child's
/// resolver falls back to the loopback browser form. Always uses the long human
/// budget.
pub async fn run_interactive(
    bin: Bin,
    state_dir: &Path,
    args: &[&str],
    hub: Option<&SecurePromptHub>,
) -> Result<CliResult, String> {
    let mut cmd = base_command(bin, state_dir)?;
    cmd.args(args);
    if let Some(hub) = hub {
        cmd.env("AGENT_ID_SECURE_PROMPT_SOCK", hub.socket_path())
            .env("AGENT_ID_SECURE_PROMPT", "hosted");
    }

    let mut child = cmd.spawn().map_err(|e| format!("spawn failed: {e}"))?;
    // Authorize the child's PID before it can connect (the server also waits
    // briefly on unknown PIDs to absorb the race).
    let pid = child.id();
    if let (Some(hub), Some(pid)) = (hub, pid) {
        hub.authorize(pid);
    }

    let result = wait_parsed(&mut child, HUMAN_TIMEOUT).await;

    if let (Some(hub), Some(pid)) = (hub, pid) {
        hub.deauthorize(pid);
    }
    result
}

async fn run_command(cmd: Command, timeout: Duration) -> Result<CliResult, String> {
    let mut cmd = cmd;
    let mut child = cmd.spawn().map_err(|e| format!("spawn failed: {e}"))?;
    wait_parsed(&mut child, timeout).await
}

async fn wait_parsed(child: &mut tokio::process::Child, timeout: Duration) -> Result<CliResult, String> {
    let stdout = child.stdout.take();
    let stderr = child.stderr.take();

    let collect = async {
        let mut out = Vec::new();
        let mut err = Vec::new();
        if let Some(mut s) = stdout {
            let _ = s.read_to_end(&mut out).await;
        }
        if let Some(mut s) = stderr {
            let _ = s.read_to_end(&mut err).await;
        }
        let status = child.wait().await;
        (out, err, status)
    };

    let (out, err, status) = match tokio::time::timeout(timeout, collect).await {
        Ok(triple) => triple,
        Err(_) => {
            let _ = child.start_kill();
            return Err("agent-id CLI timed out".to_string());
        }
    };

    let code = status.ok().and_then(|s| s.code()).unwrap_or(-1);
    let stdout_str = String::from_utf8_lossy(&out);
    let trimmed = stdout_str.trim();

    // The CLIs print one JSON object on stdout. If parsing fails, surface stderr
    // (trimmed) so the model sees something actionable.
    match serde_json::from_str::<Value>(trimmed) {
        Ok(json) => Ok(CliResult { json, code }),
        Err(_) => {
            let stderr_str = String::from_utf8_lossy(&err);
            let detail = stderr_str.trim();
            let detail = if detail.is_empty() { trimmed } else { detail };
            let detail: String = detail.chars().take(400).collect();
            Err(format!("agent-id CLI produced no JSON (exit {code}): {detail}"))
        }
    }
}

/// Convenience: run a fast subcommand and return the JSON, mapping errors to a
/// `{error}` object the tool layer can hand straight to the model.
pub async fn run_json(bin: Bin, state_dir: &Path, args: &[&str]) -> Value {
    match run(bin, state_dir, args).await {
        Ok(result) => result.json,
        Err(err) => json!({ "error": err }),
    }
}

/// Start a long-lived browser session daemon (`agent-id-browser open`), read its
/// `{ ready: true, ... }` line, then leave it running and drain the rest of its
/// stdout to a log so it never blocks on a full pipe. Returns the ready line.
pub async fn spawn_daemon_ready(
    state_dir: &Path,
    args: &[&str],
    log_path: PathBuf,
) -> Result<Value, String> {
    let mut cmd = base_command(Bin::Browser, state_dir)?;
    // The daemon must OUTLIVE this call: do not kill it when the child handle
    // drops.
    cmd.kill_on_drop(false);
    cmd.args(args);
    let mut child = cmd.spawn().map_err(|e| format!("spawn failed: {e}"))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| "no stdout from browser daemon".to_string())?;

    let mut reader = BufReader::new(stdout).lines();
    let ready = tokio::time::timeout(Duration::from_secs(90), async {
        while let Ok(Some(line)) = reader.next_line().await {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            if let Ok(value) = serde_json::from_str::<Value>(trimmed)
                && value.get("ready").and_then(Value::as_bool) == Some(true)
            {
                return Some(value);
            }
        }
        None
    })
    .await
    .map_err(|_| "browser daemon did not report ready in time".to_string())?;

    // Keep draining stdout to a log so the daemon doesn't SIGPIPE later, and let
    // the process run detached from this call.
    tokio::spawn(async move {
        let mut sink = tokio::fs::File::create(&log_path).await.ok();
        let mut lines = reader;
        while let Ok(Some(line)) = lines.next_line().await {
            if let Some(f) = sink.as_mut() {
                use tokio::io::AsyncWriteExt;
                let _ = f.write_all(line.as_bytes()).await;
                let _ = f.write_all(b"\n").await;
            }
        }
        let _ = child.wait().await;
    });

    ready.ok_or_else(|| "browser daemon closed before ready".to_string())
}
