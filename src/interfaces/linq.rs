//! Linq's V3 wire protocol. Runtime policy and conversation ownership live in
//! the API interface; a signed provider event alone does not authorize a tool.
//!
//! Protocol references:
//! https://docs.linqapp.com/channel/imessage/guides/webhooks/
//! https://docs.linqapp.com/channel/imessage/guides/webhooks/events/
//! https://docs.linqapp.com/channel/imessage/guides/messaging/polls/

use std::time::Duration;

use axum::http::HeaderMap;
use base64::{Engine, engine::general_purpose::STANDARD};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use sha2::Sha256;
use thiserror::Error;

use super::imessage_formatting::TextDecoration;

pub const LINQ_API_BASE: &str = "https://api.linqapp.com/api/partner/v3";
pub const LINQ_WEBHOOK_VERSION: &str = "2026-02-03";
pub const LINQ_TEXT_LIMIT: usize = 10_000;
pub const LINQ_WEBHOOK_BODY_LIMIT: usize = 1024 * 1024;
pub const LINQ_AUDIO_LIMIT: usize = 25 * 1024 * 1024;
const MAX_RESPONSE_BYTES: usize = 256 * 1024;
const WEBHOOK_TOLERANCE_SECONDS: i64 = 300;

#[derive(Debug, Error)]
pub enum LinqError {
    #[error("Linq API token is required")]
    MissingToken,
    #[error("invalid Linq request: {0}")]
    InvalidRequest(&'static str),
    #[error("Linq API returned HTTP {0}")]
    ApiStatus(u16),
    #[error("invalid Linq API response")]
    InvalidResponse,
    #[error("invalid Linq webhook signature")]
    InvalidSignature,
    #[error("invalid Linq webhook payload")]
    InvalidWebhook,
    #[error("unsupported Linq webhook version")]
    UnsupportedWebhookVersion,
    #[error(transparent)]
    Http(#[from] reqwest::Error),
}

pub type LinqResult<T> = Result<T, LinqError>;

#[derive(Clone)]
pub struct LinqClient {
    token: String,
    http: reqwest::Client,
    #[cfg(test)]
    test_api_base: Option<String>,
    #[cfg(test)]
    test_audio_base: Option<reqwest::Url>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SentMessage {
    pub chat_id: String,
    pub message_id: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PollEnvelope {
    pub chat_id: String,
    pub message_id: String,
    pub poll: Poll,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Poll {
    pub options: Vec<PollOption>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PollOption {
    pub option_id: String,
    pub text: String,
}

impl LinqClient {
    pub fn new(token: String) -> LinqResult<Self> {
        if token.trim().is_empty() {
            return Err(LinqError::MissingToken);
        }
        let http = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(20))
            .redirect(reqwest::redirect::Policy::none())
            .build()?;
        Ok(Self {
            token,
            http,
            #[cfg(test)]
            test_api_base: None,
            #[cfg(test)]
            test_audio_base: None,
        })
    }

    /// Download signed inbound media without sending the Linq API credential.
    pub async fn download_audio(&self, audio: &IncomingAudio) -> LinqResult<Vec<u8>> {
        let url = authorized_audio_url(&audio.url)?;
        if audio
            .size_bytes
            .is_some_and(|size| size > LINQ_AUDIO_LIMIT as u64)
        {
            return Err(LinqError::InvalidRequest(
                "audio exceeds the download limit",
            ));
        }
        #[cfg(test)]
        let url = if let Some(base) = &self.test_audio_base {
            let mut rewritten = base.clone();
            rewritten.set_path(url.path());
            rewritten.set_query(url.query());
            rewritten
        } else {
            url
        };
        let mut response = self
            .http
            .get(url)
            .send()
            .await
            .map_err(|error| LinqError::Http(error.without_url()))?;
        if !response.status().is_success() {
            return Err(LinqError::ApiStatus(response.status().as_u16()));
        }
        if response
            .content_length()
            .is_some_and(|size| size > LINQ_AUDIO_LIMIT as u64)
        {
            return Err(LinqError::InvalidResponse);
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|error| LinqError::Http(error.without_url()))?
        {
            if chunk.len() > LINQ_AUDIO_LIMIT - bytes.len() {
                return Err(LinqError::InvalidResponse);
            }
            bytes.extend_from_slice(&chunk);
        }
        if bytes.is_empty() {
            return Err(LinqError::InvalidResponse);
        }
        Ok(bytes)
    }

    #[cfg(test)]
    pub(super) fn set_test_audio_base(&mut self, base: String) {
        let url = reqwest::Url::parse(&base).expect("valid test audio base");
        assert_eq!(url.scheme(), "http");
        let address: std::net::IpAddr = url
            .host_str()
            .expect("test audio host")
            .trim_matches(['[', ']'])
            .parse()
            .expect("test audio host must be a loopback IP");
        assert!(address.is_loopback());
        assert!(url.username().is_empty() && url.password().is_none());
        self.test_audio_base = Some(url);
    }

    /// The result means accepted for delivery, not a delivery receipt.
    pub async fn send_text(
        &self,
        chat_id: &str,
        text: &str,
        reply_to: Option<&str>,
        idempotency_key: &str,
    ) -> LinqResult<SentMessage> {
        self.send_text_with_decorations(chat_id, text, &[], reply_to, idempotency_key)
            .await
    }

    pub(super) async fn send_text_with_decorations(
        &self,
        chat_id: &str,
        text: &str,
        decorations: &[TextDecoration],
        reply_to: Option<&str>,
        idempotency_key: &str,
    ) -> LinqResult<SentMessage> {
        validate_uuid(chat_id)?;
        validate_idempotency_key(idempotency_key)?;
        if text.trim().is_empty() || text.chars().count() > LINQ_TEXT_LIMIT {
            return Err(LinqError::InvalidRequest(
                "text must contain 1-10000 characters",
            ));
        }
        let mut boundaries = vec![0];
        for character in text.chars() {
            boundaries.push(boundaries.last().copied().unwrap() + character.len_utf16());
        }
        if decorations.iter().any(|decoration| {
            decoration.range[0] >= decoration.range[1]
                || boundaries.binary_search(&decoration.range[0]).is_err()
                || boundaries.binary_search(&decoration.range[1]).is_err()
        }) {
            return Err(LinqError::InvalidRequest("invalid text decoration range"));
        }
        let mut message = json!({
            "parts": [{"type": "text", "value": text}],
            "idempotency_key": idempotency_key,
        });
        if !decorations.is_empty() {
            message["parts"][0]["text_decorations"] = json!(decorations);
        }
        if let Some(message_id) = reply_to {
            validate_uuid(message_id)?;
            message["reply_to"] = json!({"message_id": message_id});
        }
        let response: SendResponse = self
            .post_json(
                &format!("/chats/{chat_id}/messages"),
                &json!({"message": message}),
            )
            .await?;
        if response.chat_id != chat_id || uuid::Uuid::parse_str(&response.message.id).is_err() {
            return Err(LinqError::InvalidResponse);
        }
        Ok(SentMessage {
            chat_id: response.chat_id,
            message_id: response.message.id,
        })
    }

    /// Native iMessage polls need an existing chat and at least two options.
    /// They have no title/question field: send the exact action summary first.
    /// Votes toggle individual options; callers must consume a pending action
    /// once and match the returned provider option IDs, never option text.
    pub async fn create_poll(
        &self,
        chat_id: &str,
        options: &[String],
        idempotency_key: &str,
    ) -> LinqResult<PollEnvelope> {
        validate_uuid(chat_id)?;
        validate_idempotency_key(idempotency_key)?;
        if options.len() < 2
            || options.iter().any(|option| option.trim().is_empty())
            || options
                .iter()
                .collect::<std::collections::HashSet<_>>()
                .len()
                != options.len()
        {
            return Err(LinqError::InvalidRequest(
                "a poll needs at least two nonempty options",
            ));
        }
        let payload_options: Vec<Value> =
            options.iter().map(|text| json!({"text": text})).collect();
        let response: PollEnvelope = self
            .post_json(
                &format!("/chats/{chat_id}/polls"),
                &json!({"poll": {"options": payload_options, "idempotency_key": idempotency_key}}),
            )
            .await?;
        if response.chat_id != chat_id
            || uuid::Uuid::parse_str(&response.message_id).is_err()
            || response.poll.options.len() != options.len()
            || response.poll.options.iter().any(|option| {
                uuid::Uuid::parse_str(&option.option_id).is_err() || !options.contains(&option.text)
            })
            || response
                .poll
                .options
                .iter()
                .map(|option| &option.option_id)
                .collect::<std::collections::HashSet<_>>()
                .len()
                != options.len()
            || response
                .poll
                .options
                .iter()
                .map(|option| &option.text)
                .collect::<std::collections::HashSet<_>>()
                .len()
                != options.len()
        {
            return Err(LinqError::InvalidResponse);
        }
        Ok(response)
    }

    async fn post_json<T: DeserializeOwned>(&self, path: &str, body: &Value) -> LinqResult<T> {
        let mut response = self
            .http
            .post(format!("{}{path}", self.api_base()))
            .bearer_auth(&self.token)
            .json(body)
            .send()
            .await?;
        if !response.status().is_success() {
            // Provider bodies can contain personal message content; do not
            // include them in user-facing errors or log chains.
            return Err(LinqError::ApiStatus(response.status().as_u16()));
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await? {
            if bytes.len().saturating_add(chunk.len()) > MAX_RESPONSE_BYTES {
                return Err(LinqError::InvalidResponse);
            }
            bytes.extend_from_slice(&chunk);
        }
        serde_json::from_slice(&bytes).map_err(|_| LinqError::InvalidResponse)
    }

    fn api_base(&self) -> &str {
        #[cfg(test)]
        if let Some(base) = &self.test_api_base {
            return base;
        }
        LINQ_API_BASE
    }
}

#[derive(Deserialize)]
struct SendResponse {
    chat_id: String,
    message: MessageReference,
}

#[derive(Deserialize)]
struct MessageReference {
    id: String,
}

fn validate_uuid(value: &str) -> LinqResult<()> {
    uuid::Uuid::parse_str(value)
        .map(|_| ())
        .map_err(|_| LinqError::InvalidRequest("message and chat IDs must be UUIDs"))
}

fn validate_idempotency_key(value: &str) -> LinqResult<()> {
    if value.trim().is_empty() || value.chars().count() > 255 {
        return Err(LinqError::InvalidRequest(
            "idempotency key must contain 1-255 characters",
        ));
    }
    Ok(())
}

fn authorized_audio_url(value: &str) -> LinqResult<reqwest::Url> {
    let url = reqwest::Url::parse(value)
        .map_err(|_| LinqError::InvalidRequest("invalid audio download URL"))?;
    if url.scheme() != "https"
        || url.host_str() != Some("cdn.linqapp.com")
        || !url.username().is_empty()
        || url.password().is_some()
        || value.split_once("://").is_some_and(|(_, rest)| {
            rest.split(['/', '?', '#'])
                .next()
                .is_some_and(|authority| authority.contains('@'))
        })
        || url.port().is_some_and(|port| port != 443)
        || url.fragment().is_some()
    {
        return Err(LinqError::InvalidRequest("invalid audio download URL"));
    }
    Ok(url)
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct IncomingAudio {
    pub url: String,
    pub filename: String,
    pub mime_type: Option<String>,
    pub size_bytes: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum WebhookEvent {
    Message(IncomingMessage),
    PollVote(IncomingPollVote),
    Delivery(DeliveryUpdate),
    Ignored { event_id: String },
}

impl WebhookEvent {
    pub fn event_id(&self) -> &str {
        match self {
            Self::Message(message) => &message.event_id,
            Self::PollVote(vote) => &vote.event_id,
            Self::Delivery(delivery) => &delivery.event_id,
            Self::Ignored { event_id } => event_id,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct IncomingMessage {
    pub event_id: String,
    pub chat_id: String,
    pub message_id: String,
    pub sender: String,
    pub text: String,
    pub service: String,
    pub is_group: bool,
    pub reply_to: Option<String>,
    pub attachment_count: usize,
    #[serde(default)]
    pub audio: Vec<IncomingAudio>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct IncomingPollVote {
    pub event_id: String,
    pub chat_id: String,
    pub message_id: String,
    pub sender: String,
    pub option_id: String,
    pub added: bool,
    pub service: String,
    pub is_group: bool,
}

/// Delivery status is telemetry only; it never grants action authorization.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DeliveryUpdate {
    pub event_id: String,
    pub chat_id: String,
    pub message_id: String,
    pub status: String,
}

/// Verify Standard Webhooks before parsing. Callers must additionally authorize
/// the sender/chat and persist event_id deduplication before starting a turn.
/// A bounded symmetric timestamp window rejects stale events and future replays.
pub fn verify_webhook(
    secret: &str,
    headers: &HeaderMap,
    body: &[u8],
    now_unix: i64,
) -> LinqResult<WebhookEvent> {
    if body.len() > LINQ_WEBHOOK_BODY_LIMIT {
        return Err(LinqError::InvalidWebhook);
    }
    let id = signature_header(headers, "webhook-id")?;
    let timestamp = signature_header(headers, "webhook-timestamp")?;
    let signatures = signature_header(headers, "webhook-signature")?;
    if !timestamp.bytes().all(|byte| byte.is_ascii_digit()) || now_unix < 0 {
        return Err(LinqError::InvalidSignature);
    }
    let timestamp_unix: i64 = timestamp.parse().map_err(|_| LinqError::InvalidSignature)?;
    if timestamp_unix.abs_diff(now_unix) > WEBHOOK_TOLERANCE_SECONDS as u64 {
        return Err(LinqError::InvalidSignature);
    }
    let key = STANDARD
        .decode(secret.strip_prefix("whsec_").unwrap_or(secret))
        .map_err(|_| LinqError::InvalidSignature)?;
    if key.is_empty() {
        return Err(LinqError::InvalidSignature);
    }
    let mut mac = Hmac::<Sha256>::new_from_slice(&key).map_err(|_| LinqError::InvalidSignature)?;
    mac.update(id.as_bytes());
    mac.update(b".");
    mac.update(timestamp.as_bytes());
    mac.update(b".");
    mac.update(body);
    let valid = signatures.split_whitespace().any(|signature| {
        signature
            .strip_prefix("v1,")
            .and_then(|encoded| STANDARD.decode(encoded).ok())
            .is_some_and(|signature| mac.clone().verify_slice(&signature).is_ok())
    });
    if !valid {
        return Err(LinqError::InvalidSignature);
    }
    parse_webhook(body)
}

fn signature_header<'a>(headers: &'a HeaderMap, name: &str) -> LinqResult<&'a str> {
    let mut values = headers.get_all(name).iter();
    let value = values.next().ok_or(LinqError::InvalidSignature)?;
    if values.next().is_some() {
        return Err(LinqError::InvalidSignature);
    }
    let value = value.to_str().map_err(|_| LinqError::InvalidSignature)?;
    if value.is_empty() || value.len() > 4096 {
        return Err(LinqError::InvalidSignature);
    }
    Ok(value)
}

#[derive(Deserialize)]
struct WebhookEnvelope {
    api_version: String,
    webhook_version: String,
    event_type: String,
    event_id: String,
    data: Value,
}

#[derive(Deserialize)]
struct InboundData {
    chat: Chat,
    direction: String,
    sender_handle: Sender,
    service: String,
    #[serde(default)]
    reconciled_at: Option<String>,
}

#[derive(Deserialize)]
struct Chat {
    id: String,
    is_group: bool,
}

#[derive(Deserialize)]
struct Sender {
    handle: String,
    is_me: bool,
    #[serde(default)]
    status: Option<String>,
}

#[derive(Deserialize)]
struct MessageData {
    #[serde(flatten)]
    inbound: InboundData,
    id: String,
    parts: Vec<MessagePart>,
    #[serde(default)]
    reply_to: Option<ReplyReference>,
}

#[derive(Deserialize)]
struct MessagePart {
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    value: Option<String>,
    #[serde(default)]
    url: Value,
    #[serde(default)]
    filename: Value,
    #[serde(default)]
    mime_type: Value,
    #[serde(default)]
    size_bytes: Value,
}

impl MessagePart {
    fn into_audio(self) -> LinqResult<Option<IncomingAudio>> {
        let mime = self.mime_type.as_str();
        let media_type = mime.map(|mime| mime.split(';').next().unwrap_or(mime).trim());
        let is_audio = media_type.is_some_and(|mime| {
            mime.get(..6)
                .is_some_and(|prefix| prefix.eq_ignore_ascii_case("audio/"))
                && mime.len() > 6
        });
        let infer_from_extension = self.mime_type.is_null()
            || media_type.is_some_and(|mime| mime.eq_ignore_ascii_case("application/octet-stream"));
        if !is_audio && !infer_from_extension {
            return Ok(None);
        }
        let url = self.url.as_str();
        let filename = self
            .filename
            .as_str()
            .filter(|name| !name.trim().is_empty());
        let filename_is_audio = filename.is_some_and(has_audio_extension);
        let parsed_url = if filename.is_none() || (!is_audio && !filename_is_audio) {
            url.and_then(|url| reqwest::Url::parse(url).ok())
        } else {
            None
        };
        let url_filename = parsed_url
            .as_ref()
            .and_then(|url| url.path_segments()?.next_back());
        if !is_audio && !(filename_is_audio || url_filename.is_some_and(has_audio_extension)) {
            return Ok(None);
        }
        let url = url
            .filter(|url| !url.trim().is_empty())
            .ok_or(LinqError::InvalidWebhook)?;
        let size_bytes = if self.size_bytes.is_null() {
            None
        } else {
            Some(self.size_bytes.as_u64().ok_or(LinqError::InvalidWebhook)?)
        };
        Ok(Some(IncomingAudio {
            url: url.to_owned(),
            filename: filename
                .or(url_filename.filter(|name| !name.is_empty()))
                .unwrap_or("audio")
                .to_owned(),
            mime_type: mime.map(str::to_owned),
            size_bytes,
        }))
    }
}

fn has_audio_extension(filename: &str) -> bool {
    filename.rsplit_once('.').is_some_and(|(_, extension)| {
        [
            "m4a", "mp3", "aac", "caf", "wav", "aiff", "aif", "amr", "ogg", "oga", "opus", "flac",
        ]
        .iter()
        .any(|known| extension.eq_ignore_ascii_case(known))
    })
}

#[derive(Deserialize)]
struct ReplyReference {
    message_id: String,
}

#[derive(Deserialize)]
struct VoteData {
    #[serde(flatten)]
    inbound: InboundData,
    message_id: String,
    option_id: String,
}

#[derive(Deserialize)]
struct DeliveryData {
    chat: Chat,
    id: String,
}

fn parse_webhook(body: &[u8]) -> LinqResult<WebhookEvent> {
    let envelope: WebhookEnvelope =
        serde_json::from_slice(body).map_err(|_| LinqError::InvalidWebhook)?;
    if !valid_identifier(&envelope.event_id) {
        return Err(LinqError::InvalidWebhook);
    }
    let ignored = || WebhookEvent::Ignored {
        event_id: envelope.event_id.clone(),
    };
    if let Some(status) = match envelope.event_type.as_str() {
        "message.sent" => Some("sent"),
        "message.delivered" => Some("delivered"),
        "message.read" => Some("read"),
        "message.failed" => Some("failed"),
        _ => None,
    } {
        if envelope.api_version != "v3" || envelope.webhook_version != LINQ_WEBHOOK_VERSION {
            return Err(LinqError::UnsupportedWebhookVersion);
        }
        let delivery: DeliveryData =
            serde_json::from_value(envelope.data.clone()).map_err(|_| LinqError::InvalidWebhook)?;
        if !valid_identifier(&delivery.id) || !valid_identifier(&delivery.chat.id) {
            return Err(LinqError::InvalidWebhook);
        }
        return Ok(WebhookEvent::Delivery(DeliveryUpdate {
            event_id: envelope.event_id,
            chat_id: delivery.chat.id,
            message_id: delivery.id,
            status: status.to_string(),
        }));
    }
    if !matches!(
        envelope.event_type.as_str(),
        "message.received" | "poll.vote.added" | "poll.vote.removed"
    ) {
        return Ok(ignored());
    }
    if envelope.api_version != "v3" || envelope.webhook_version != LINQ_WEBHOOK_VERSION {
        return Err(LinqError::UnsupportedWebhookVersion);
    }
    if envelope.event_type == "message.received" {
        let message: MessageData =
            serde_json::from_value(envelope.data.clone()).map_err(|_| LinqError::InvalidWebhook)?;
        if !accept_inbound(&message.inbound)? {
            return Ok(ignored());
        }
        if !valid_identifier(&message.id)
            || message
                .reply_to
                .as_ref()
                .is_some_and(|reply| !valid_identifier(&reply.message_id))
        {
            return Err(LinqError::InvalidWebhook);
        }
        let mut text = Vec::new();
        let mut attachment_count = 0;
        let mut audio = Vec::new();
        for part in message.parts {
            match part.kind.as_str() {
                "text" => text.push(part.value.ok_or(LinqError::InvalidWebhook)?),
                "media" => {
                    attachment_count += 1;
                    if let Some(attachment) = part.into_audio()? {
                        audio.push(attachment);
                    }
                }
                _ => (),
            }
        }
        Ok(WebhookEvent::Message(IncomingMessage {
            event_id: envelope.event_id,
            chat_id: message.inbound.chat.id,
            message_id: message.id,
            sender: message.inbound.sender_handle.handle,
            text: text.join("\n"),
            service: message.inbound.service,
            is_group: message.inbound.chat.is_group,
            reply_to: message.reply_to.map(|reply| reply.message_id),
            attachment_count,
            audio,
        }))
    } else {
        let vote: VoteData =
            serde_json::from_value(envelope.data.clone()).map_err(|_| LinqError::InvalidWebhook)?;
        if !accept_inbound(&vote.inbound)? {
            return Ok(ignored());
        }
        if vote.inbound.service != "iMessage"
            || !valid_identifier(&vote.message_id)
            || !valid_identifier(&vote.option_id)
        {
            return Err(LinqError::InvalidWebhook);
        }
        Ok(WebhookEvent::PollVote(IncomingPollVote {
            event_id: envelope.event_id,
            chat_id: vote.inbound.chat.id,
            message_id: vote.message_id,
            sender: vote.inbound.sender_handle.handle,
            option_id: vote.option_id,
            added: envelope.event_type == "poll.vote.added",
            service: vote.inbound.service,
            is_group: vote.inbound.chat.is_group,
        }))
    }
}

fn accept_inbound(data: &InboundData) -> LinqResult<bool> {
    if !valid_identifier(&data.chat.id)
        || !valid_identifier(&data.sender_handle.handle)
        || !matches!(data.service.as_str(), "iMessage" | "RCS" | "SMS")
    {
        return Err(LinqError::InvalidWebhook);
    }
    Ok(data.direction == "inbound"
        && !data.sender_handle.is_me
        && data.reconciled_at.is_none()
        && !matches!(
            data.sender_handle.status.as_deref(),
            Some("left" | "removed")
        ))
}

fn valid_identifier(value: &str) -> bool {
    !value.is_empty() && value.len() <= 255 && !value.chars().any(char::is_whitespace)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        Json, Router,
        extract::OriginalUri,
        routing::{get, post},
    };
    use std::sync::{Arc, Mutex};

    const CHAT: &str = "550e8400-e29b-41d4-a716-446655440000";
    const MESSAGE: &str = "550e8400-e29b-41d4-a716-446655440001";
    const OPTION: &str = "550e8400-e29b-41d4-a716-446655440002";
    const SECRET: &str = "whsec_dGVzdC1zaWduaW5nLWtleQ==";
    const NOW: i64 = 1_800_000_000;

    fn payload(kind: &str) -> Value {
        json!({
            "api_version": "v3",
            "webhook_version": LINQ_WEBHOOK_VERSION,
            "event_type": kind,
            "event_id": "test-event",
            "data": {
                "id": MESSAGE,
                "message_id": MESSAGE,
                "option_id": OPTION,
                "chat": {"id": CHAT, "is_group": false},
                "direction": "inbound",
                "sender_handle": {"handle": "+491234567890", "is_me": false, "status": "active"},
                "service": "iMessage",
                "parts": [{"type": "text", "value": "Hello"}],
            },
        })
    }

    fn signed_headers(body: &[u8], timestamp: i64) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert("webhook-id", "test-event".parse().unwrap());
        headers.insert("webhook-timestamp", timestamp.to_string().parse().unwrap());
        let mut mac = Hmac::<Sha256>::new_from_slice(b"test-signing-key").unwrap();
        mac.update(format!("test-event.{timestamp}.").as_bytes());
        mac.update(body);
        let signature = STANDARD.encode(mac.finalize().into_bytes());
        headers.insert(
            "webhook-signature",
            format!("v1,{signature}").parse().unwrap(),
        );
        headers
    }

    #[test]
    fn verifies_an_independently_generated_standard_webhooks_fixture() {
        // Signature computed with Python's hmac/hashlib, independently of the
        // Rust fixture signer above. The webhook ID need not equal event_id.
        let body = br#"{"api_version":"v3","webhook_version":"2026-02-03","event_type":"ignored","event_id":"fixture","data":{}}"#;
        let mut headers = HeaderMap::new();
        headers.insert("webhook-id", "wh_fixture".parse().unwrap());
        headers.insert("webhook-timestamp", "1800000000".parse().unwrap());
        headers.insert(
            "webhook-signature",
            "v1,hKI4kpED6q/wJO4QUVI2mtN0aHZukBmCIA14NshGd7g="
                .parse()
                .unwrap(),
        );
        assert_eq!(
            verify_webhook(SECRET, &headers, body, NOW).unwrap(),
            WebhookEvent::Ignored {
                event_id: "fixture".to_string(),
            }
        );
    }

    #[test]
    fn verifies_raw_bytes_and_returns_the_provider_sender() {
        let mut value = payload("message.received");
        value["data"]["parts"] = json!([
            {"type": "text", "value": "Hello"},
            {"type": "media", "url": "https://cdn.example/image.png"},
            {"type": "text", "value": "again"},
        ]);
        value["data"]["reply_to"] = json!({"message_id": MESSAGE});
        let body = serde_json::to_vec_pretty(&value).unwrap();
        let headers = signed_headers(&body, NOW);
        let WebhookEvent::Message(message) = verify_webhook(SECRET, &headers, &body, NOW).unwrap()
        else {
            panic!("expected an inbound message");
        };
        assert_eq!(message.sender, "+491234567890");
        assert_eq!(message.text, "Hello\nagain");
        assert_eq!(message.reply_to.as_deref(), Some(MESSAGE));
        assert_eq!(message.attachment_count, 1);
        let changed = serde_json::to_vec(&value).unwrap();
        assert!(matches!(
            verify_webhook(SECRET, &headers, &changed, NOW),
            Err(LinqError::InvalidSignature)
        ));
    }

    #[test]
    fn rejects_stale_future_missing_and_invalid_signatures() {
        let body = serde_json::to_vec(&payload("message.received")).unwrap();
        for timestamp in [NOW - 301, NOW + 301, i64::MIN] {
            assert!(matches!(
                verify_webhook(SECRET, &signed_headers(&body, timestamp), &body, NOW),
                Err(LinqError::InvalidSignature)
            ));
        }
        let mut headers = signed_headers(&body, NOW);
        headers.insert("webhook-signature", "v1,AAAA".parse().unwrap());
        assert!(verify_webhook(SECRET, &headers, &body, NOW).is_err());
        assert!(verify_webhook(SECRET, &HeaderMap::new(), &body, NOW).is_err());
        assert!(verify_webhook("whsec_", &signed_headers(&body, NOW), &body, NOW).is_err());
    }

    #[test]
    fn accepts_rotated_signatures_and_rejects_duplicate_headers() {
        let body = serde_json::to_vec(&payload("message.received")).unwrap();
        let mut headers = signed_headers(&body, NOW);
        let valid = headers["webhook-signature"].to_str().unwrap().to_string();
        headers.insert(
            "webhook-signature",
            format!("v2,ignored v1,AAAA {valid}").parse().unwrap(),
        );
        assert!(verify_webhook(SECRET, &headers, &body, NOW).is_ok());
        headers.append("webhook-id", "different-event".parse().unwrap());
        assert!(verify_webhook(SECRET, &headers, &body, NOW).is_err());
    }

    #[test]
    fn ignores_self_outbound_and_reconciled_events() {
        for change in ["self", "outbound", "reconciled", "removed"] {
            let mut value = payload("message.received");
            match change {
                "self" => value["data"]["sender_handle"]["is_me"] = json!(true),
                "outbound" => value["data"]["direction"] = json!("outbound"),
                "reconciled" => value["data"]["reconciled_at"] = json!("2026-09-29T00:00:00Z"),
                _ => value["data"]["sender_handle"]["status"] = json!("removed"),
            }
            let body = serde_json::to_vec(&value).unwrap();
            assert!(matches!(
                verify_webhook(SECRET, &signed_headers(&body, NOW), &body, NOW).unwrap(),
                WebhookEvent::Ignored { .. }
            ));
        }
    }

    #[test]
    fn rejects_missing_actor_and_unknown_versions() {
        for change in ["actor", "version", "group-kind"] {
            let mut value = payload("message.received");
            match change {
                "actor" => value["data"]
                    .as_object_mut()
                    .unwrap()
                    .remove("sender_handle"),
                "version" => Some(std::mem::replace(
                    &mut value["webhook_version"],
                    json!("2025-01-01"),
                )),
                _ => value["data"]["chat"]
                    .as_object_mut()
                    .unwrap()
                    .remove("is_group"),
            };
            let body = serde_json::to_vec(&value).unwrap();
            assert!(verify_webhook(SECRET, &signed_headers(&body, NOW), &body, NOW).is_err());
        }
    }

    #[test]
    fn native_poll_votes_preserve_actor_option_and_operation() {
        for (kind, added) in [("poll.vote.added", true), ("poll.vote.removed", false)] {
            let body = serde_json::to_vec(&payload(kind)).unwrap();
            let WebhookEvent::PollVote(vote) =
                verify_webhook(SECRET, &signed_headers(&body, NOW), &body, NOW).unwrap()
            else {
                panic!("expected a poll vote");
            };
            assert_eq!(vote.sender, "+491234567890");
            assert_eq!(vote.option_id, OPTION);
            assert_eq!(vote.message_id, MESSAGE);
            assert_eq!(vote.added, added);
        }
    }

    #[test]
    fn preserves_group_context_and_serializes_only_verified_input() {
        let mut value = payload("poll.vote.added");
        value["data"]["chat"]["is_group"] = json!(true);
        let body = serde_json::to_vec(&value).unwrap();
        let event = verify_webhook(SECRET, &signed_headers(&body, NOW), &body, NOW).unwrap();
        let WebhookEvent::PollVote(vote) = &event else {
            panic!("expected a vote");
        };
        assert!(vote.is_group);
        let restored: WebhookEvent =
            serde_json::from_slice(&serde_json::to_vec(&event).unwrap()).unwrap();
        assert_eq!(restored, event);
        assert_eq!(event.event_id(), "test-event");
    }

    #[test]
    fn delivery_receipts_do_not_become_user_input() {
        let body = serde_json::to_vec(&payload("message.delivered")).unwrap();
        let event = verify_webhook(SECRET, &signed_headers(&body, NOW), &body, NOW).unwrap();
        let WebhookEvent::Delivery(delivery) = event else {
            panic!("expected delivery telemetry");
        };
        assert_eq!(delivery.status, "delivered");
        assert_eq!(delivery.message_id, MESSAGE);
    }

    #[tokio::test]
    async fn sends_bearer_authenticated_text_and_native_poll_wire_payloads() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&requests);
        let app = Router::new().route("/{*path}", post(move |OriginalUri(uri): OriginalUri, headers: HeaderMap, Json(body): Json<Value>| {
            let captured = Arc::clone(&captured);
            async move {
                captured.lock().unwrap().push((uri.path().to_string(), headers, body.clone()));
                if body.get("poll").is_some() {
                    Json(json!({
                        "chat_id": CHAT, "message_id": MESSAGE,
                        "poll": {"options": [
                            {"option_id": OPTION, "text": "Approve"},
                            {"option_id": "550e8400-e29b-41d4-a716-446655440003", "text": "Cancel"},
                        ]},
                    }))
                } else {
                    Json(json!({"chat_id": CHAT, "message": {"id": MESSAGE}}))
                }
            }
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut client = LinqClient::new("test-token".into()).unwrap();
        client.test_api_base = Some(format!("http://{}", listener.local_addr().unwrap()));
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let sent = client
            .send_text(CHAT, "Ready", Some(MESSAGE), "text-request")
            .await
            .unwrap();
        assert_eq!(sent.message_id, MESSAGE);
        let poll = client
            .create_poll(CHAT, &["Approve".into(), "Cancel".into()], "poll-request")
            .await
            .unwrap();
        assert_eq!(poll.poll.options[0].option_id, OPTION);
        let decorations: Vec<TextDecoration> = serde_json::from_value(json!([
            {"range": [3, 8], "style": "bold"},
            {"range": [3, 8], "style": "italic"},
        ]))
        .unwrap();
        client
            .send_text_with_decorations(
                CHAT,
                "😀 Ready",
                &decorations,
                Some(MESSAGE),
                "formatted-request",
            )
            .await
            .unwrap();
        server.abort();
        let requests = requests.lock().unwrap();
        assert_eq!(requests[0].0, format!("/chats/{CHAT}/messages"));
        assert_eq!(requests[1].0, format!("/chats/{CHAT}/polls"));
        assert_eq!(requests[0].1["authorization"], "Bearer test-token");
        assert_eq!(
            requests[0].2,
            json!({"message": {
                "parts": [{"type": "text", "value": "Ready"}],
                "reply_to": {"message_id": MESSAGE}, "idempotency_key": "text-request",
            }})
        );
        assert_eq!(
            requests[1].2,
            json!({"poll": {
                "options": [{"text": "Approve"}, {"text": "Cancel"}],
                "idempotency_key": "poll-request",
            }})
        );
        assert_eq!(
            requests[2].2,
            json!({"message": {
                "parts": [{"type": "text", "value": "😀 Ready", "text_decorations": [
                    {"range": [3, 8], "style": "bold"},
                    {"range": [3, 8], "style": "italic"},
                ]}],
                "reply_to": {"message_id": MESSAGE},
                "idempotency_key": "formatted-request",
            }})
        );
    }

    #[tokio::test]
    async fn rejects_decoration_ranges_that_split_unicode_or_exceed_text() {
        let client = LinqClient::new("test-token".into()).unwrap();
        for range in [[1, 2], [3, 9], [3, 3], [8, 3]] {
            let decorations: Vec<TextDecoration> = serde_json::from_value(json!([
                {"range": range, "style": "bold"}
            ]))
            .unwrap();
            assert!(matches!(
                client
                    .send_text_with_decorations(CHAT, "😀 Ready", &decorations, None, "request")
                    .await,
                Err(LinqError::InvalidRequest("invalid text decoration range"))
            ));
        }
    }

    #[tokio::test]
    async fn refuses_redirects_and_redacts_provider_error_bodies() {
        use axum::http::{StatusCode, header};
        let app = Router::new().route(
            "/{*path}",
            post(|| async {
                (
                    StatusCode::TEMPORARY_REDIRECT,
                    [(header::LOCATION, "https://unexpected.example/")],
                    "private-provider-message-and-token",
                )
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut client = LinqClient::new("test-token".into()).unwrap();
        client.test_api_base = Some(format!("http://{}", listener.local_addr().unwrap()));
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let error = client
            .send_text(CHAT, "Ready", None, "request")
            .await
            .unwrap_err();
        server.abort();
        assert!(matches!(error, LinqError::ApiStatus(307)));
        assert_eq!(error.to_string(), "Linq API returned HTTP 307");
    }

    #[tokio::test]
    async fn bounds_provider_response_body_size() {
        let app = Router::new().route(
            "/{*path}",
            post(|| async { "x".repeat(MAX_RESPONSE_BYTES + 1) }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut client = LinqClient::new("test-token".into()).unwrap();
        client.test_api_base = Some(format!("http://{}", listener.local_addr().unwrap()));
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let error = client
            .send_text(CHAT, "Ready", None, "request")
            .await
            .unwrap_err();
        server.abort();
        assert!(matches!(error, LinqError::InvalidResponse));
    }

    fn verified_message(value: &Value) -> IncomingMessage {
        let body = serde_json::to_vec(value).unwrap();
        let WebhookEvent::Message(message) =
            verify_webhook(SECRET, &signed_headers(&body, NOW), &body, NOW).unwrap()
        else {
            panic!("expected an inbound message");
        };
        message
    }

    #[test]
    fn signed_voice_memo_and_audio_preserve_metadata_caption_and_order() {
        // The official SDK's MessageEventV2 uses SchemasMediaPartResponse
        // (type "media") for attachments, with no separate voice discriminator:
        // https://github.com/linq-team/linq-node/blob/dd5f49ce1e6d8122d453666da00e1277c83cabc0/src/resources/webhooks.ts#L715-L745
        let mut value = payload("message.received");
        value["data"]["chat"]["is_group"] = json!(true);
        value["data"]["reply_to"] = json!({"message_id": MESSAGE});
        value["data"]["parts"] = json!([
            {"type": "text", "value": "Caption"},
            {
                "type": "media", "filename": "voice-memo.m4a",
                "mime_type": "audio/x-m4a", "size_bytes": 524288,
                "url": "https://cdn.linqapp.com/voice-memos/abc123.m4a?signature=private"
            },
            {"type": "media", "mime_type": "image/png", "url": "https://cdn.linqapp.com/photo.png"},
            {"type": "text", "value": "More text"},
            {
                "type": "media", "filename": "recording.wav",
                "mime_type": "audio/wav", "size_bytes": 42,
                "url": "https://cdn.linqapp.com/attachments/recording.wav"
            },
            {"type": "unknown", "url": "https://cdn.linqapp.com/ignored.mp3"},
        ]);
        let message = verified_message(&value);
        assert_eq!(message.text, "Caption\nMore text");
        assert_eq!(message.sender, "+491234567890");
        assert!(message.is_group);
        assert_eq!(message.reply_to.as_deref(), Some(MESSAGE));
        assert_eq!(message.attachment_count, 3);
        assert_eq!(message.audio.len(), 2);
        assert_eq!(
            message.audio[0],
            IncomingAudio {
                url: "https://cdn.linqapp.com/voice-memos/abc123.m4a?signature=private".into(),
                filename: "voice-memo.m4a".into(),
                mime_type: Some("audio/x-m4a".into()),
                size_bytes: Some(524288),
            }
        );
        assert_eq!(message.audio[1].filename, "recording.wav");
        let restored: IncomingMessage =
            serde_json::from_slice(&serde_json::to_vec(&message).unwrap()).unwrap();
        assert_eq!(restored, message);
        let mut old_message = serde_json::to_value(&message).unwrap();
        old_message.as_object_mut().unwrap().remove("audio");
        let restored: IncomingMessage = serde_json::from_value(old_message).unwrap();
        assert!(restored.audio.is_empty());
        let body = serde_json::to_vec(&value).unwrap();
        value["data"]["parts"][1]["url"] = json!("https://cdn.linqapp.com/altered.m4a");
        assert!(matches!(
            verify_webhook(
                SECRET,
                &signed_headers(&body, NOW),
                &serde_json::to_vec(&value).unwrap(),
                NOW
            ),
            Err(LinqError::InvalidSignature)
        ));
    }

    #[test]
    fn recognizes_audio_mime_and_only_safe_extension_fallbacks() {
        for (part, expected) in [
            (
                json!({"mime_type": "Audio/MP4; codecs=mp4a", "filename": "recording.mp4"}),
                true,
            ),
            (
                json!({"mime_type": "audio/opus", "filename": "download"}),
                true,
            ),
            (json!({"filename": "recording.M4A"}), true),
            (
                json!({"filename": "recording.AIFF", "mime_type": "application/octet-stream"}),
                true,
            ),
            (
                json!({"url": "https://cdn.linqapp.com/audio.CAF?signature=private"}),
                true,
            ),
            (json!({"filename": "recording.mp4"}), false),
            (
                json!({"mime_type": "video/mp4", "filename": "recording.m4a"}),
                false,
            ),
            (
                json!({"mime_type": "image/png", "filename": "recording.mp3"}),
                false,
            ),
            (json!({"filename": "recording.mp3.exe"}), false),
            (
                json!({"url": "https://cdn.linqapp.com/download?filename=recording.mp3"}),
                false,
            ),
            (
                json!({"filename": 7, "mime_type": 12, "size_bytes": -1, "url": {}}),
                false,
            ),
        ] {
            let mut part = part;
            part["type"] = json!("media");
            if part.get("url").is_none() {
                part["url"] = json!("https://cdn.linqapp.com/download");
            }
            let mut value = payload("message.received");
            value["data"]["parts"] = json!([part]);
            let message = verified_message(&value);
            assert_eq!(!message.audio.is_empty(), expected, "part: {part}");
            assert_eq!(message.attachment_count, 1);
        }
    }

    #[test]
    fn rejects_missing_audio_url_but_preserves_malformed_non_audio_behavior() {
        for url in [Value::Null, json!(""), json!("   "), json!(17)] {
            let mut value = payload("message.received");
            value["data"]["parts"] = json!([{
                "type": "media", "mime_type": "audio/x-m4a",
                "filename": "voice-memo.m4a", "url": url,
            }]);
            let body = serde_json::to_vec(&value).unwrap();
            assert!(matches!(
                verify_webhook(SECRET, &signed_headers(&body, NOW), &body, NOW),
                Err(LinqError::InvalidWebhook)
            ));
        }
        let mut value = payload("message.received");
        value["data"]["parts"] = json!([
            {"type": "media"},
            {"type": "media", "mime_type": "image/png", "url": 42, "size_bytes": "bad"},
            {"type": "unknown", "mime_type": "audio/mpeg"},
        ]);
        let message = verified_message(&value);
        assert_eq!(message.attachment_count, 2);
        assert!(message.audio.is_empty());
    }

    fn downloadable_audio(path: &str) -> IncomingAudio {
        IncomingAudio {
            url: format!("https://cdn.linqapp.com/{path}?signature=private-signed-url"),
            filename: "voice.m4a".into(),
            mime_type: Some("audio/x-m4a".into()),
            size_bytes: None,
        }
    }

    #[tokio::test]
    async fn audio_download_authorizes_original_cdn_url_before_test_rewrite() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut client = LinqClient::new("private-linq-token".into()).unwrap();
        client.set_test_audio_base(format!("http://{}", listener.local_addr().unwrap()));
        assert!(authorized_audio_url("https://cdn.linqapp.com:443/voice.m4a").is_ok());
        for url in [
            "http://cdn.linqapp.com/voice.m4a",
            "https://cdn.linqapp.com.evil.example/voice.m4a",
            "https://evil.example/cdn.linqapp.com/voice.m4a",
            "https://127.0.0.1/voice.m4a",
            "https://cdn.linqapp.com:444/voice.m4a",
            "https://user:password@cdn.linqapp.com/voice.m4a",
            "https://user@cdn.linqapp.com/voice.m4a",
            "https://@cdn.linqapp.com/voice.m4a",
            "https://cdn.linqapp.com/voice.m4a#fragment",
            "not-a-url",
        ] {
            let mut audio = downloadable_audio("voice.m4a");
            audio.url = url.into();
            assert!(matches!(
                client.download_audio(&audio).await,
                Err(LinqError::InvalidRequest(_))
            ));
        }
        let mut audio = downloadable_audio("voice.m4a");
        audio.size_bytes = Some(LINQ_AUDIO_LIMIT as u64 + 1);
        assert!(matches!(
            client.download_audio(&audio).await,
            Err(LinqError::InvalidRequest(_))
        ));
        assert!(
            tokio::time::timeout(Duration::from_millis(20), listener.accept())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn audio_download_is_bounded_without_auth_and_refuses_redirects() {
        use axum::{
            body::Body,
            http::{Response, StatusCode, header},
        };
        let requests = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&requests);
        let app = Router::new().route(
            "/{*path}",
            get(move |OriginalUri(uri): OriginalUri, headers: HeaderMap| {
                let captured = Arc::clone(&captured);
                async move {
                    captured.lock().unwrap().push((uri.clone(), headers));
                    match uri.path() {
                        "/redirect" => Response::builder()
                            .status(StatusCode::TEMPORARY_REDIRECT)
                            .header(header::LOCATION, "/redirect-target")
                            .body(Body::from("private-provider-body"))
                            .unwrap(),
                        "/provider-error" => Response::builder()
                            .status(StatusCode::FORBIDDEN)
                            .body(Body::from("private-provider-body"))
                            .unwrap(),
                        "/advertised-too-large" => Response::builder()
                            .header(header::CONTENT_LENGTH, (LINQ_AUDIO_LIMIT + 1).to_string())
                            .body(Body::from_stream(futures_util::stream::pending::<
                                Result<Vec<u8>, std::io::Error>,
                            >()))
                            .unwrap(),
                        path => {
                            let size: usize = match path {
                                "/exact-limit" => LINQ_AUDIO_LIMIT,
                                "/chunked-too-large" => LINQ_AUDIO_LIMIT + 1,
                                "/empty" => 0,
                                _ => 3,
                            };
                            const CHUNK: usize = 64 * 1024;
                            let chunks = (0..size.div_ceil(CHUNK)).map(move |index| {
                                Ok::<_, std::io::Error>(vec![b'x'; CHUNK.min(size - index * CHUNK)])
                            });
                            Response::new(Body::from_stream(futures_util::stream::iter(chunks)))
                        }
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut client = LinqClient::new("private-linq-token".into()).unwrap();
        client.set_test_audio_base(format!("http://{}", listener.local_addr().unwrap()));
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        assert_eq!(
            client
                .download_audio(&downloadable_audio("voice.m4a"))
                .await
                .unwrap(),
            b"xxx"
        );
        let mut exact = downloadable_audio("exact-limit");
        exact.size_bytes = Some(LINQ_AUDIO_LIMIT as u64);
        assert_eq!(
            client.download_audio(&exact).await.unwrap().len(),
            LINQ_AUDIO_LIMIT
        );
        for path in ["advertised-too-large", "chunked-too-large", "empty"] {
            assert!(matches!(
                client.download_audio(&downloadable_audio(path)).await,
                Err(LinqError::InvalidResponse)
            ));
        }
        for (path, status) in [("redirect", 307), ("provider-error", 403)] {
            let error = client
                .download_audio(&downloadable_audio(path))
                .await
                .unwrap_err();
            assert!(matches!(error, LinqError::ApiStatus(code) if code == status));
            let rendered = format!("{error:?} {error}");
            for private in [
                "private-provider-body",
                "private-signed-url",
                "private-linq-token",
            ] {
                assert!(!rendered.contains(private));
            }
        }
        server.abort();
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 7);
        assert!(requests.iter().all(|(uri, headers)| {
            uri.query() == Some("signature=private-signed-url")
                && !headers.contains_key("authorization")
        }));
        assert!(
            !requests
                .iter()
                .any(|(uri, _)| uri.path() == "/redirect-target")
        );
    }

    #[tokio::test]
    async fn audio_transport_errors_do_not_expose_signed_urls_or_tokens() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut client = LinqClient::new("private-linq-token".into()).unwrap();
        client.set_test_audio_base(format!("http://{}", listener.local_addr().unwrap()));
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 4096];
            socket.read(&mut request).await.unwrap();
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\nConnection: close\r\n\r\nx")
                .await
                .unwrap();
            socket.shutdown().await.unwrap();
        });
        let error = client
            .download_audio(&downloadable_audio("voice.m4a"))
            .await
            .unwrap_err();
        server.await.unwrap();
        assert!(matches!(error, LinqError::Http(_)));
        let rendered = format!("{error:?} {error}");
        for private in [
            "private-signed-url",
            "private-linq-token",
            "signature=",
            "voice.m4a",
        ] {
            assert!(!rendered.contains(private));
        }
    }
}
