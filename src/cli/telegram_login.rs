//! Native recovery commands: deliberately independent of the LLM and transcript.
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};

use anyhow::Result;
use lethe::interfaces::telegram::{IncomingTelegramText, TelegramClient};
use lethe::llm::openai_oauth::{OpenAiDeviceLogin, openai_static_token_configured};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

struct PendingLogin {
    task: JoinHandle<()>,
    connected: Arc<AtomicBool>,
}

fn pending_login() -> &'static Mutex<Option<PendingLogin>> {
    static PENDING: OnceLock<Mutex<Option<PendingLogin>>> = OnceLock::new();
    PENDING.get_or_init(|| Mutex::new(None))
}

async fn cancel_login(pending: Option<PendingLogin>) -> &'static str {
    let Some(pending) = pending else {
        return "No OpenAI sign-in is pending.";
    };
    pending.task.abort();
    let cancelled = pending.task.await.is_err_and(|error| error.is_cancelled());
    if pending.connected.load(Ordering::Acquire) {
        "OpenAI sign-in already completed. You are connected."
    } else if cancelled {
        "OpenAI sign-in cancelled."
    } else {
        "No OpenAI sign-in is pending."
    }
}

pub async fn handle_login(
    client: &TelegramClient,
    incoming: &IncomingTelegramText,
    argument: Option<&str>,
) -> Result<()> {
    if !client.is_private_owner(incoming.chat_id, incoming.user_id) {
        client
            .send_message(
                incoming.chat_id,
                "Sign-in is only available to the configured owner in their private chat with me.",
            )
            .await?;
        return Ok(());
    }
    let mut pending = pending_login().lock().await;
    if argument == Some("cancel") {
        let message = cancel_login(pending.take()).await;
        client.send_message(incoming.chat_id, message).await?;
        return Ok(());
    }
    if argument != Some("openai") {
        client
            .send_message(
                incoming.chat_id,
                "Use /login openai to reconnect, or /login cancel to cancel a pending sign-in.",
            )
            .await?;
        return Ok(());
    }
    if openai_static_token_configured() {
        client
            .send_message(
                incoming.chat_id,
                lethe::interfaces::telegram::OPENAI_STATIC_TOKEN_MESSAGE,
            )
            .await?;
        return Ok(());
    }
    if pending
        .as_ref()
        .is_some_and(|login| !login.task.is_finished())
    {
        client.send_message(incoming.chat_id, "OpenAI sign-in is already pending. Complete the browser step or use /login cancel before starting again.").await?;
        return Ok(());
    }
    let client = client.clone();
    let chat_id = incoming.chat_id;
    let connected = Arc::new(AtomicBool::new(false));
    let committed = connected.clone();
    let task = tokio::spawn(async move {
        let result = async {
            let login = OpenAiDeviceLogin::start().await?;
            client.send_sign_in_message(chat_id, &format!(
                "Reconnect OpenAI: open {} and enter code {}.\n\nApprove in your browser within 15 minutes. I will confirm here when connected. Use /login cancel to cancel. Never send passwords or tokens here.",
                login.verification_url(), login.user_code()
            )).await?;
            login.finish().await
        }.await;
        let message = if result.is_ok() {
            // No await between successful commit and this marker. Cancelling
            // the later notification must not claim the saved login was undone.
            committed.store(true, Ordering::Release);
            "OpenAI reconnected. You can send your request again; no restart is needed."
        } else {
            // Provider response bodies may carry secrets. Neither logs nor chat
            // should include the raw error or device credentials.
            tracing::warn!("OpenAI device sign-in did not complete");
            "OpenAI sign-in did not complete. The code may have expired or the connection failed. Check that device-code login is enabled in ChatGPT security settings, then use /login openai to try again."
        };
        if client.send_message(chat_id, message).await.is_err() {
            tracing::warn!("Could not deliver OpenAI sign-in status to Telegram");
        }
    });
    *pending = Some(PendingLogin { task, connected });
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn cancellation_stops_pending_authorization() {
        let pending = PendingLogin {
            task: tokio::spawn(std::future::pending()),
            connected: Arc::new(AtomicBool::new(false)),
        };
        assert_eq!(
            cancel_login(Some(pending)).await,
            "OpenAI sign-in cancelled."
        );
        assert_eq!(cancel_login(None).await, "No OpenAI sign-in is pending.");
    }

    #[tokio::test]
    async fn cancellation_after_commit_does_not_claim_to_undo_sign_in() {
        let connected = Arc::new(AtomicBool::new(false));
        let committed = connected.clone();
        let (ready, waiting) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            committed.store(true, Ordering::Release);
            ready.send(()).unwrap();
            // Simulate the final Telegram notification awaiting delivery.
            std::future::pending::<()>().await;
        });
        waiting.await.unwrap();
        assert_eq!(
            cancel_login(Some(PendingLogin { task, connected })).await,
            "OpenAI sign-in already completed. You are connected."
        );
    }
}
