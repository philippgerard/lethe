use serde_json::{Map, Value, json};

pub const VISIBILITY_KEY: &str = "lethe_visibility";
pub const MESSAGE_KIND_KEY: &str = "lethe_message_kind";
pub const SOURCE_KEY: &str = "lethe_source";
/// Ephemeral per-request override used when a transport's model prompt differs
/// from the text that belongs in user-visible history. This key is stripped
/// before persistence.
pub const HISTORY_CONTENT_KEY: &str = "_lethe_history_content";

/// Message kinds used only by Lethe's internal persistence and transport
/// boundaries. Keep these out of the public [`MessageKind`] enum so adding a
/// new internal provenance tag cannot break downstream exhaustive matches.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RawMessageKind {
    Wake,
    Checkpoint,
    CheckpointResolved,
    CheckpointNotice,
}

impl RawMessageKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Wake => "wake",
            Self::Checkpoint => "checkpoint",
            Self::CheckpointResolved => "checkpoint_resolved",
            Self::CheckpointNotice => "checkpoint_notice",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value.trim() {
            "wake" | "scheduler_wake" => Some(Self::Wake),
            "checkpoint" | "tool_loop_checkpoint" => Some(Self::Checkpoint),
            "checkpoint_resolved" => Some(Self::CheckpointResolved),
            "checkpoint_notice" => Some(Self::CheckpointNotice),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MessageVisibility {
    UserVisible,
    Internal,
}

impl MessageVisibility {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::UserVisible => "user_visible",
            Self::Internal => "internal",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value.trim() {
            "user_visible" | "visible" | "external" => Some(Self::UserVisible),
            "internal" | "system" | "background" => Some(Self::Internal),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum MessageKind {
    Chat,
    Heartbeat,
    Proactive,
    ActorUpdate,
    TelegramMedia,
    TelegramReaction,
}

impl MessageKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Chat => "chat",
            Self::Heartbeat => "heartbeat",
            Self::Proactive => "proactive",
            Self::ActorUpdate => "actor_update",
            Self::TelegramMedia => "telegram_media",
            Self::TelegramReaction => "telegram_reaction",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value.trim() {
            "chat" | "telegram_text" => Some(Self::Chat),
            "heartbeat" | "background_heartbeat" => Some(Self::Heartbeat),
            "proactive" => Some(Self::Proactive),
            "actor_update" => Some(Self::ActorUpdate),
            "telegram_media" | "telegram_audio" | "telegram_photo" | "telegram_document"
            | "telegram_sticker" => Some(Self::TelegramMedia),
            "telegram_reaction" => Some(Self::TelegramReaction),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MessageMetadata {
    pub visibility: MessageVisibility,
    pub kind: Option<MessageKind>,
    pub has_tool_calls: bool,
}

impl MessageMetadata {
    pub fn from_value(value: Option<&Value>) -> Self {
        let Some(Value::Object(map)) = value else {
            return Self::default();
        };
        Self::from_map(map)
    }

    pub fn from_map(map: &Map<String, Value>) -> Self {
        let kind = metadata_string(map, MESSAGE_KIND_KEY)
            .and_then(|value| MessageKind::parse(&value))
            .or_else(|| metadata_string(map, "source").and_then(|value| MessageKind::parse(&value)))
            .or_else(|| metadata_string(map, "kind").and_then(|value| MessageKind::parse(&value)));

        let visibility = metadata_string(map, VISIBILITY_KEY)
            .and_then(|value| MessageVisibility::parse(&value))
            .unwrap_or_else(|| legacy_visibility(map, kind));

        let has_tool_calls = map
            .get("tool_calls")
            .and_then(Value::as_array)
            .is_some_and(|calls| !calls.is_empty());

        Self {
            visibility,
            kind,
            has_tool_calls,
        }
    }

    pub fn is_internal(self) -> bool {
        self.visibility == MessageVisibility::Internal
    }

    pub fn has_tool_calls(self) -> bool {
        self.has_tool_calls
    }
}

impl Default for MessageMetadata {
    fn default() -> Self {
        Self {
            visibility: MessageVisibility::UserVisible,
            kind: None,
            has_tool_calls: false,
        }
    }
}

pub fn metadata_value(
    visibility: MessageVisibility,
    kind: MessageKind,
    source: &'static str,
) -> Value {
    let mut map = Map::new();
    annotate_map(&mut map, visibility, kind, source);
    Value::Object(map)
}

pub(crate) fn raw_metadata_value(
    visibility: MessageVisibility,
    kind: RawMessageKind,
    source: &'static str,
) -> Value {
    let mut map = Map::new();
    map.insert(VISIBILITY_KEY.to_string(), json!(visibility.as_str()));
    map.insert(MESSAGE_KIND_KEY.to_string(), json!(kind.as_str()));
    map.insert(SOURCE_KEY.to_string(), json!(source));
    Value::Object(map)
}

pub(crate) fn raw_message_kind(value: Option<&Value>) -> Option<RawMessageKind> {
    let Some(Value::Object(map)) = value else {
        return None;
    };
    raw_message_kind_from_map(map)
}

fn raw_message_kind_from_map(map: &Map<String, Value>) -> Option<RawMessageKind> {
    metadata_string(map, MESSAGE_KIND_KEY)
        .and_then(|value| RawMessageKind::parse(&value))
        .or_else(|| metadata_string(map, "source").and_then(|value| RawMessageKind::parse(&value)))
        .or_else(|| metadata_string(map, "kind").and_then(|value| RawMessageKind::parse(&value)))
}

pub fn annotate_map(
    map: &mut Map<String, Value>,
    visibility: MessageVisibility,
    kind: MessageKind,
    source: &'static str,
) {
    map.insert(VISIBILITY_KEY.to_string(), json!(visibility.as_str()));
    map.insert(MESSAGE_KIND_KEY.to_string(), json!(kind.as_str()));
    map.insert(SOURCE_KEY.to_string(), json!(source));
}

pub fn annotate_value(
    value: Value,
    visibility: MessageVisibility,
    kind: MessageKind,
    source: &'static str,
) -> Value {
    match value {
        Value::Object(mut map) => {
            annotate_map(&mut map, visibility, kind, source);
            Value::Object(map)
        }
        value => {
            let mut map = Map::new();
            map.insert("metadata".to_string(), value);
            annotate_map(&mut map, visibility, kind, source);
            Value::Object(map)
        }
    }
}

/// Return the durable, user-facing representation of an inbound message.
///
/// Some transports send a richer private prompt to the model than should ever
/// appear in chat history. Telegram self-message reactions are the canonical
/// example: their prompt contains handling instructions and a copy of the
/// reacted-to assistant message. Keep the sanitization at the persistence
/// boundary so callers that forget to supply a separate history string still
/// fail closed.
pub fn user_visible_history_content(message: &str, metadata: Option<&Value>) -> String {
    if let Some(Value::Object(map)) = metadata
        && let Some(content) = map.get(HISTORY_CONTENT_KEY).and_then(Value::as_str)
    {
        return content.to_string();
    }
    let parsed = MessageMetadata::from_value(metadata);
    if parsed.kind != Some(MessageKind::TelegramReaction) {
        return message.to_string();
    }

    let Some(Value::Object(map)) = metadata else {
        return "[Telegram reaction added]".to_string();
    };
    let emojis = map
        .get("reaction_new")
        .and_then(Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(Value::as_str)
                .collect::<Vec<_>>()
                .join(" ")
        })
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "reaction".to_string());
    let message_id = map
        .get("message_id")
        .and_then(Value::as_i64)
        .map(|value| value.to_string())
        .unwrap_or_else(|| "unknown".to_string());
    format!("[Telegram reaction added: {emojis} on message {message_id}]")
}

fn legacy_visibility(map: &Map<String, Value>, kind: Option<MessageKind>) -> MessageVisibility {
    if matches!(
        kind,
        Some(MessageKind::Heartbeat | MessageKind::ActorUpdate)
    ) || matches!(
        raw_message_kind_from_map(map),
        Some(
            RawMessageKind::Wake | RawMessageKind::Checkpoint | RawMessageKind::CheckpointResolved
        )
    ) {
        return MessageVisibility::Internal;
    }

    if metadata_string(map, "source")
        .as_deref()
        .is_some_and(|source| {
            matches!(
                source,
                "heartbeat" | "background_heartbeat" | "system" | "actor_update"
            )
        })
    {
        return MessageVisibility::Internal;
    }

    MessageVisibility::UserVisible
}

fn metadata_string(map: &Map<String, Value>, key: &str) -> Option<String> {
    map.get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn typed_metadata_drives_visibility() {
        let value = metadata_value(
            MessageVisibility::Internal,
            MessageKind::Heartbeat,
            "heartbeat",
        );

        let metadata = MessageMetadata::from_value(Some(&value));

        assert!(metadata.is_internal());
        assert_eq!(metadata.kind, Some(MessageKind::Heartbeat));
    }

    #[test]
    fn legacy_heartbeat_metadata_is_internal() {
        let value = json!({"source": "heartbeat"});

        assert!(MessageMetadata::from_value(Some(&value)).is_internal());
    }

    #[test]
    fn legacy_actor_update_metadata_is_internal() {
        let value = json!({"source": "actor_update"});

        assert!(MessageMetadata::from_value(Some(&value)).is_internal());
    }

    #[test]
    fn typed_delivered_actor_update_is_user_visible() {
        let value = metadata_value(
            MessageVisibility::UserVisible,
            MessageKind::Proactive,
            "actor_update",
        );
        let metadata = MessageMetadata::from_value(Some(&value));

        assert!(!metadata.is_internal());
        assert_eq!(metadata.kind, Some(MessageKind::Proactive));
    }

    #[test]
    fn proactive_metadata_is_user_visible() {
        let value = metadata_value(
            MessageVisibility::UserVisible,
            MessageKind::Proactive,
            "brainstem",
        );
        let metadata = MessageMetadata::from_value(Some(&value));

        assert!(!metadata.is_internal());
        assert_eq!(metadata.kind, Some(MessageKind::Proactive));
    }

    #[test]
    fn wake_and_checkpoint_metadata_are_typed_internal_rows() {
        for (kind, source) in [
            (RawMessageKind::Wake, "wake"),
            (RawMessageKind::Checkpoint, "tool_loop"),
        ] {
            let value = raw_metadata_value(MessageVisibility::Internal, kind, source);
            let metadata = MessageMetadata::from_value(Some(&value));

            assert!(metadata.is_internal());
            assert_eq!(raw_message_kind(Some(&value)), Some(kind));
        }
    }

    #[test]
    fn checkpoint_kind_fails_closed_without_an_explicit_visibility() {
        let value = json!({"lethe_message_kind": "checkpoint"});
        let metadata = MessageMetadata::from_value(Some(&value));

        assert!(metadata.is_internal());
        assert_eq!(
            raw_message_kind(Some(&value)),
            Some(RawMessageKind::Checkpoint)
        );
    }

    #[test]
    fn reaction_history_content_never_uses_the_private_model_prompt() {
        let metadata = json!({
            "lethe_message_kind": "telegram_reaction",
            "reaction_new": ["👍"],
            "message_id": 77,
        });
        let private_prompt = "SECRET transport instructions and copied assistant text";

        let visible = user_visible_history_content(private_prompt, Some(&metadata));

        assert_eq!(visible, "[Telegram reaction added: 👍 on message 77]");
        assert!(!visible.contains("SECRET"));
        assert_eq!(user_visible_history_content("hello", None), "hello");
    }

    #[test]
    fn explicit_combined_history_content_overrides_last_wins_chat_metadata() {
        let metadata = json!({
            "lethe_message_kind": "chat",
            (HISTORY_CONTENT_KEY): "visible text\n\n[Telegram reaction added]",
        });

        assert_eq!(
            user_visible_history_content("private combined model prompt", Some(&metadata)),
            "visible text\n\n[Telegram reaction added]"
        );
    }
}
