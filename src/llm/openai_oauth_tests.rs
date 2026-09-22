use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

struct Fixture {
    client: OpenAiOAuthClient,
    refreshes: Arc<AtomicUsize>,
    requests: Arc<AtomicUsize>,
    pending_poll: Arc<tokio::sync::Notify>,
    base: String,
    server: tokio::task::JoinHandle<()>,
    _directory: tempfile::TempDir,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}

async fn fixture(refresh_status: u16, always_reject: bool) -> Fixture {
    use axum::{Json, Router, response::IntoResponse, routing::post};
    let refreshes = Arc::new(AtomicUsize::new(0));
    let requests = Arc::new(AtomicUsize::new(0));
    let refresh_count = refreshes.clone();
    let request_count = requests.clone();
    let pending_poll = Arc::new(tokio::sync::Notify::new());
    let poll_started = pending_poll.clone();
    let app = Router::new()
        .route("/token", post(move || {
            let count = refresh_count.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(10)).await;
                let body = if refresh_status == 200 {
                    json!({"access_token":"renewed", "refresh_token":"rotated", "expires_in":3600})
                } else {
                    json!({"error":"invalid_grant", "secret":"DO_NOT_EXPOSE"})
                };
                (StatusCode::from_u16(refresh_status).unwrap(), Json(body))
            }
        }))
        .route("/responses", post(move |headers: HeaderMap| {
            let count = request_count.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                if always_reject || headers.get("authorization").unwrap() != "Bearer renewed" {
                    return (StatusCode::UNAUTHORIZED, "rejected").into_response();
                }
                let events = concat!(
                    "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"hello\"}\n\n",
                    "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"output\":[{\"type\":\"message\",\"content\":[{\"type\":\"output_text\",\"text\":\"hello\"}]}]}}\n\n"
                );
                ([("content-type", "text/event-stream")], events).into_response()
            }
        }))
        .route("/start", post(|| async {
            Json(json!({"device_auth_id":"device", "user_code":"TEST-1234", "interval":1, "verification_uri":"https://untrusted.example"}))
        }))
        .route("/poll", post(|| async {
            Json(json!({"authorization_code":"code", "code_verifier":"verifier"}))
        }))
        .route("/pending", post(move || {
            let started = poll_started.clone();
            async move {
                started.notify_one();
                StatusCode::FORBIDDEN
            }
        }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let directory = tempfile::tempdir().unwrap();
    let token_file = directory.path().join("tokens.json");
    let tokens = OpenAiOAuthTokens {
        access_token: Some("old".to_string()),
        refresh_token: Some("refresh".to_string()),
        expires_at: Some(unix_now_seconds() + 3600.0),
        account_id: Some("account".to_string()),
        ..Default::default()
    };
    write_openai_oauth_tokens(&token_file, &tokens).unwrap();
    let client = OpenAiOAuthClient {
        http: reqwest::Client::new(),
        token_file: token_file.clone(),
        tokens: shared_token_state(&token_file, tokens),
        request_gate: Arc::new(Semaphore::new(2)),
        rate_limit_until: Arc::new(Mutex::new(None)),
        token_url: format!("{base}/token"),
        responses_url: format!("{base}/responses"),
    };
    Fixture {
        client,
        refreshes,
        requests,
        pending_poll,
        base,
        server,
        _directory: directory,
    }
}

#[tokio::test]
async fn concurrent_clients_refresh_rotating_credentials_once() {
    let f = fixture(200, false).await;
    {
        let mut tokens = f.client.tokens.lock().await;
        tokens.expires_at = Some(0.0);
        write_openai_oauth_tokens(&f.client.token_file, &tokens).unwrap();
    }
    let mut other = f.client.clone();
    other.tokens = shared_token_state(&f.client.token_file, OpenAiOAuthTokens::default());
    let (a, b) = tokio::join!(f.client.ensure_access(), other.ensure_access());
    a.unwrap();
    b.unwrap();
    assert_eq!(f.refreshes.load(Ordering::SeqCst), 1);
    let saved = read_openai_oauth_tokens(&f.client.token_file).unwrap();
    assert_eq!(saved.refresh_token.as_deref(), Some("rotated"));
    assert_eq!(saved.account_id.as_deref(), Some("account"));
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            fs::metadata(&f.client.token_file)
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }
}

#[tokio::test]
async fn rejected_unexpired_access_token_refreshes_and_retries_once() {
    let f = fixture(200, false).await;
    let response = f
        .client
        .exec_chat_request("gpt-5", ChatRequest::default(), &ChatOptions::default())
        .await
        .unwrap();
    assert_eq!(response.first_text(), Some("hello"));
    assert_eq!(f.requests.load(Ordering::SeqCst), 2);
    assert_eq!(f.refreshes.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn streaming_rejection_refreshes_before_emitting_any_text() {
    let f = fixture(200, false).await;
    let output = std::sync::Mutex::new(String::new());
    let on_delta = |text: &str| output.lock().unwrap().push_str(text);
    f.client
        .exec_chat_request_stream(
            "gpt-5",
            ChatRequest::default(),
            &ChatOptions::default(),
            &on_delta,
        )
        .await
        .unwrap();
    assert_eq!(*output.lock().unwrap(), "hello");
    assert_eq!(f.requests.load(Ordering::SeqCst), 2);
    assert_eq!(f.refreshes.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn second_rejection_requires_login_without_a_retry_loop() {
    let f = fixture(200, true).await;
    let error = f
        .client
        .exec_chat_request("gpt-5", ChatRequest::default(), &ChatOptions::default())
        .await
        .unwrap_err();
    assert_eq!(
        crate::interfaces::telegram::llm_failure_reply(&error),
        crate::interfaces::telegram::OPENAI_LOGIN_MESSAGE
    );
    let error = anyhow::Error::new(crate::agent::AgentError::Llm(
        error.context("LLM chat request failed for model gpt-5"),
    ));
    assert_eq!(
        crate::interfaces::telegram::llm_failure_reply(&error),
        crate::interfaces::telegram::OPENAI_LOGIN_MESSAGE
    );
    assert_eq!(f.requests.load(Ordering::SeqCst), 2);
    assert_eq!(f.refreshes.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn revoked_refresh_is_typed_redacted_and_not_retried_until_login() {
    let f = fixture(400, false).await;
    let error = anyhow::Error::from(f.client.refresh_access(Some("old")).await.unwrap_err())
        .context("LLM request failed");
    assert_eq!(
        crate::interfaces::telegram::llm_failure_reply(&error),
        crate::interfaces::telegram::OPENAI_LOGIN_MESSAGE
    );
    assert!(!format!("{error:?}").contains("DO_NOT_EXPOSE"));
    assert!(f.client.auth_status().await.contains("sign-in required"));
    assert!(f.client.ensure_access().await.is_err());
    assert_eq!(f.refreshes.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn raw_token_rejection_requires_configuration_change_without_refresh() {
    let f = fixture(200, false).await;
    f.client.tokens.lock().await.env_access_token = true;
    let error = anyhow::Error::from(f.client.refresh_access(Some("old")).await.unwrap_err());
    assert_eq!(
        crate::interfaces::telegram::llm_failure_reply(&error),
        crate::interfaces::telegram::OPENAI_STATIC_TOKEN_MESSAGE
    );
    assert_eq!(f.refreshes.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn device_login_updates_existing_clients_and_clears_reauth_failure() {
    let f = fixture(200, false).await;
    f.client.tokens.lock().await.reauth_required = true;
    let login = OpenAiDeviceLogin::start_at(
        f.client.token_file.clone(),
        &format!("{}/start", f.base),
        &format!("{}/poll", f.base),
        &f.client.token_url,
    )
    .await
    .unwrap();
    assert_eq!(
        login.verification_url(),
        "https://auth.openai.com/codex/device"
    );
    assert_eq!(login.user_code(), "TEST-1234");
    login.finish().await.unwrap();
    f.client.ensure_access().await.unwrap();
    assert_eq!(
        f.client.tokens.lock().await.access_token.as_deref(),
        Some("renewed")
    );
    assert_eq!(f.refreshes.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn expired_device_login_does_not_change_existing_credentials() {
    let f = fixture(200, false).await;
    let mut login = OpenAiDeviceLogin::start_at(
        f.client.token_file.clone(),
        &format!("{}/start", f.base),
        &format!("{}/pending", f.base),
        &f.client.token_url,
    )
    .await
    .unwrap();
    login.deadline = tokio::time::Instant::now() + Duration::from_millis(20);
    assert!(
        login
            .finish()
            .await
            .unwrap_err()
            .to_string()
            .contains("timed out")
    );
    assert_eq!(
        read_openai_oauth_tokens(&f.client.token_file)
            .unwrap()
            .access_token
            .as_deref(),
        Some("old")
    );
    assert_eq!(f.refreshes.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn temporary_refresh_failures_do_not_invalidate_the_session() {
    for status in [429, 503] {
        let f = fixture(status, false).await;
        for _ in 0..2 {
            let error = f.client.refresh_access(Some("old")).await.unwrap_err();
            assert!(matches!(error, OpenAiOAuthError::Transient { .. }));
            assert!(!format!("{error:?}").contains("DO_NOT_EXPOSE"));
            assert!(!f.client.tokens.lock().await.reauth_required);
        }
        assert_eq!(f.refreshes.load(Ordering::SeqCst), 2);
    }
}

#[tokio::test]
async fn cancelled_device_login_preserves_credentials_and_stops_polling() {
    let f = fixture(200, false).await;
    let login = OpenAiDeviceLogin::start_at(
        f.client.token_file.clone(),
        &format!("{}/start", f.base),
        &format!("{}/pending", f.base),
        &f.client.token_url,
    )
    .await
    .unwrap();
    let task = tokio::spawn(login.finish());
    tokio::time::timeout(Duration::from_secs(2), f.pending_poll.notified())
        .await
        .unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(
        read_openai_oauth_tokens(&f.client.token_file)
            .unwrap()
            .access_token
            .as_deref(),
        Some("old")
    );
    assert_eq!(f.refreshes.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn failed_persistence_retains_rotated_tokens_until_the_file_is_writable() {
    let f = fixture(200, false).await;
    let backup = f._directory.path().join("previous.json");
    fs::rename(&f.client.token_file, &backup).unwrap();
    fs::create_dir(&f.client.token_file).unwrap();
    assert!(f.client.refresh_access(Some("old")).await.is_err());
    {
        let tokens = f.client.tokens.lock().await;
        assert_eq!(tokens.refresh_token.as_deref(), Some("rotated"));
        assert!(tokens.persistence_pending);
    }
    assert!(
        f.client
            .auth_status()
            .await
            .contains("credential save failed")
    );
    // Restore the stale file. The next request must save the pending rotation,
    // not reload that file and reuse its invalidated refresh token.
    fs::remove_dir(&f.client.token_file).unwrap();
    fs::rename(&backup, &f.client.token_file).unwrap();
    f.client.ensure_access().await.unwrap();
    assert_eq!(
        read_openai_oauth_tokens(&f.client.token_file)
            .unwrap()
            .refresh_token
            .as_deref(),
        Some("rotated")
    );
    assert!(!f.client.tokens.lock().await.persistence_pending);
    assert_eq!(f.refreshes.load(Ordering::SeqCst), 1);
}
