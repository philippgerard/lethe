//! Notification-only egress. Interactive turns keep their original transport.
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use anyhow::Result;
use serde_json::{Value, json};

use super::notifications::NotificationPreferences;
use super::telegram::{SharedTelegramTurnGuard, TelegramToolContext};
use crate::config::Settings;
use crate::tools::registry::MessageEgress;

pub struct NotificationDelivery {
    preferences: Option<NotificationPreferences>,
    telegram: Arc<dyn MessageEgress + Send + Sync>,
    guard: Option<SharedTelegramTurnGuard>,
    telegram_sent: AtomicUsize,
    imessage_queued: AtomicUsize,
    imessage_failed: AtomicUsize,
}

impl NotificationDelivery {
    pub fn new(
        settings: &Settings,
        chat_id: i64,
        guard: Option<SharedTelegramTurnGuard>,
    ) -> Result<Arc<Self>> {
        // An explicitly addressed third party is never copied to the owner's
        // iMessage chat or controlled by the owner's notification settings.
        let preferences = if settings
            .telegram
            .allowed_user_ids
            .first()
            .is_none_or(|owner| *owner == chat_id)
        {
            Some(NotificationPreferences::open(
                &settings.paths.lethe_home.join("data/transports"),
                &settings.imessage,
                !settings.telegram.bot_token.trim().is_empty() && chat_id > 0,
            )?)
        } else {
            None
        };
        Ok(Arc::new(Self {
            preferences,
            telegram: Arc::new(TelegramToolContext {
                token: settings.telegram.bot_token.clone(),
                chat_id,
                user_id: Some(chat_id),
                last_message_id: None,
                guard: None,
                dry_run: false,
                sent_messages: None,
            }),
            guard,
            telegram_sent: AtomicUsize::new(0),
            imessage_queued: AtomicUsize::new(0),
            imessage_failed: AtomicUsize::new(0),
        }))
    }

    fn telegram_enabled(&self) -> Result<bool> {
        self.preferences
            .as_ref()
            .map(|prefs| prefs.route().map(|route| route.telegram))
            .unwrap_or(Ok(true))
    }

    fn queue_imessage(&self, text: &str) -> bool {
        let Some(preferences) = &self.preferences else {
            return false;
        };
        match preferences.queue_imessage(text) {
            Ok(true) => {
                self.imessage_queued.fetch_add(1, Ordering::Relaxed);
                true
            }
            Ok(false) => false,
            Err(error) => {
                self.imessage_failed.fetch_add(1, Ordering::Relaxed);
                tracing::error!(error = %crate::diagnostics::error_summary(&error), "iMessage notification could not be queued");
                false
            }
        }
    }

    fn prepare_delivery(&self, text: &str, parse_mode: &str) -> Result<(bool, bool)> {
        let Some(preferences) = &self.preferences else {
            return Ok((true, false));
        };
        match preferences.queue_delivery(text, parse_mode) {
            Ok(route) => {
                let queued = route.imessage.is_some();
                if queued {
                    self.imessage_queued.fetch_add(1, Ordering::Relaxed);
                }
                Ok((route.telegram, queued))
            }
            Err(error) => {
                self.imessage_failed.fetch_add(1, Ordering::Relaxed);
                tracing::error!(error = %crate::diagnostics::error_summary(&error), "notification queue unavailable");
                Ok((preferences.route()?.telegram, false))
            }
        }
    }

    pub fn imessage_status(&self) -> &'static str {
        if self.imessage_failed.load(Ordering::Relaxed) > 0 {
            "queue_failed"
        } else if self.imessage_queued.load(Ordering::Relaxed) > 0 {
            "queued"
        } else if self.preferences.is_none() {
            "different_recipient"
        } else {
            "no_notification"
        }
    }

    pub fn telegram_messages(&self) -> usize {
        self.telegram_sent.load(Ordering::Relaxed)
    }

    pub fn imessage_messages(&self) -> usize {
        self.imessage_queued.load(Ordering::Relaxed)
    }

    fn record_text(&self, text: &str) {
        if let Some(guard) = &self.guard
            && let Ok(mut guard) = guard.lock()
        {
            guard.record_visible_text(text);
        }
    }

    pub async fn send_text(self: &Arc<Self>, text: String) -> Result<i64, String> {
        let delivery = self.clone();
        tokio::task::spawn_blocking(move || {
            let result: Value = serde_json::from_str(&delivery.send_message(&text, "", None))
                .map_err(|_| "Invalid notification delivery result".to_string())?;
            if result["success"] == true {
                Ok(result["message_id"].as_i64().unwrap_or(0))
            } else {
                Err(result["error"]
                    .as_str()
                    .unwrap_or("Notification delivery failed")
                    .to_string())
            }
        })
        .await
        .map_err(|_| "Notification delivery worker failed".to_string())?
    }
}

impl MessageEgress for NotificationDelivery {
    fn send_message(
        &self,
        text: &str,
        parse_mode: &str,
        reply_markup_json: Option<&str>,
    ) -> String {
        if text.trim().is_empty() {
            return json!({"success": false, "error": "Notification text is required"}).to_string();
        }
        let (telegram_enabled, queued) = match self.prepare_delivery(text, parse_mode) {
            Ok(route) => route,
            Err(_) => {
                return json!({"success": false, "error": "Notification settings are unavailable"})
                    .to_string();
            }
        };
        let mut result = if telegram_enabled {
            serde_json::from_str::<Value>(&self.telegram.send_message(
                text,
                parse_mode,
                reply_markup_json,
            ))
            .unwrap_or_else(|_| json!({"success": false, "error": "Telegram delivery failed"}))
        } else {
            json!({"success": false})
        };
        let sent = result["success"] == true;
        if sent {
            self.telegram_sent.fetch_add(1, Ordering::Relaxed);
        }
        if queued || sent {
            self.record_text(text);
            result["success"] = json!(true);
            result["imessage_status"] = json!(if queued { "queued" } else { "not_subscribed" });
            result["telegram_status"] = json!(if sent {
                "sent"
            } else if telegram_enabled {
                "failed"
            } else {
                "off"
            });
            result.as_object_mut().unwrap().remove("error");
        } else if result["error"].is_null() {
            result["error"] = json!("Notification could not be queued or sent");
        }
        result.to_string()
    }

    fn send_file(&self, source: &str, caption: &str, as_document: bool) -> String {
        if !self.telegram_enabled().unwrap_or(false) {
            return json!({"success": false, "error": "Telegram notifications are off. Attachments cannot be sent through iMessage; send a text summary with telegram_send_message instead."}).to_string();
        }
        let result = self.telegram.send_file(source, caption, as_document);
        if serde_json::from_str::<Value>(&result).is_ok_and(|value| value["success"] == true) {
            self.telegram_sent.fetch_add(1, Ordering::Relaxed);
            let notice = format!("An attachment was sent in Telegram.\n{caption}");
            self.queue_imessage(&notice);
            self.record_text(&notice);
        }
        result
    }

    fn react(&self, emoji: &str, message_id: i64) -> String {
        if self.telegram_enabled().unwrap_or(false) {
            self.telegram.react(emoji, message_id)
        } else {
            json!({"success": false, "error": "Telegram notifications are off; send text instead."})
                .to_string()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::notifications::{Channel, NotificationCommand};
    use super::super::telegram::TelegramTurnGuard;
    use super::*;
    use std::sync::Mutex;

    #[derive(Default)]
    struct FakeTelegram {
        texts: Mutex<Vec<String>>,
        parse_modes: Mutex<Vec<String>>,
        fail: std::sync::atomic::AtomicBool,
    }
    impl MessageEgress for FakeTelegram {
        fn send_message(&self, text: &str, parse_mode: &str, _: Option<&str>) -> String {
            if self.fail.load(Ordering::Relaxed) {
                return json!({"success": false, "error": "offline"}).to_string();
            }
            self.texts.lock().unwrap().push(text.into());
            self.parse_modes.lock().unwrap().push(parse_mode.into());
            json!({"success": true, "message_id": 42}).to_string()
        }
        fn send_file(&self, _: &str, _: &str, _: bool) -> String {
            panic!("disabled channel must not send files")
        }
        fn react(&self, _: &str, _: i64) -> String {
            panic!("disabled channel must not react")
        }
    }

    #[test]
    fn html_notifications_render_for_imessage_and_preserve_telegram_payloads() {
        for telegram_enabled in [true, false] {
            let dir = tempfile::tempdir().unwrap();
            let config = crate::config::ImessageConfig {
                enabled: true,
                api_token: "test".into(),
                webhook_secret: "secret".into(),
                allowed_senders: vec!["+49123456789".into()],
                native_polls: false,
            };
            let prefs = NotificationPreferences::open(dir.path(), &config, true).unwrap();
            prefs
                .command(
                    Channel::Imessage,
                    NotificationCommand::Set(true),
                    Some(("chat-1", "+49123456789")),
                )
                .unwrap();
            if !telegram_enabled {
                prefs
                    .command(Channel::Telegram, NotificationCommand::Set(false), None)
                    .unwrap();
            }
            let telegram = Arc::new(FakeTelegram::default());
            let delivery = NotificationDelivery {
                preferences: Some(prefs),
                telegram: telegram.clone(),
                guard: None,
                telegram_sent: AtomicUsize::new(0),
                imessage_queued: AtomicUsize::new(0),
                imessage_failed: AtomicUsize::new(0),
            };
            let html = "🥐 <b>Frühstück</b>: <b>557 kcal</b> &amp; Kaffee";
            let result: Value =
                serde_json::from_str(&delivery.send_message(html, "HTML", None)).unwrap();
            assert_eq!(result["success"], true);
            assert_eq!(result["imessage_status"], "queued");
            assert_eq!(
                result["telegram_status"],
                if telegram_enabled { "sent" } else { "off" }
            );
            if telegram_enabled {
                assert_eq!(telegram.texts.lock().unwrap().as_slice(), [html]);
                assert_eq!(telegram.parse_modes.lock().unwrap().as_slice(), ["HTML"]);
            } else {
                assert!(telegram.texts.lock().unwrap().is_empty());
            }
            // Read the durable delivery contract after dropping the producer.
            drop(delivery);
            let connection = rusqlite::Connection::open(dir.path().join("linq.sqlite")).unwrap();
            let raw: String = connection
                .query_row("SELECT payload FROM linq_outbox", [], |row| row.get(0))
                .unwrap();
            let output: Value = serde_json::from_str(&raw).unwrap();
            assert_eq!(output["content"], html);
            assert_eq!(
                output["text_parts"],
                json!([{
                    "text": "🥐 Frühstück: 557 kcal & Kaffee",
                    "text_decorations": [
                        {"range": [3, 12], "style": "bold"},
                        {"range": [14, 22], "style": "bold"}
                    ]
                }])
            );
        }
    }

    #[test]
    fn scheduled_egress_obeys_live_preferences_without_duplicates_or_false_delivery() {
        let dir = tempfile::tempdir().unwrap();
        let config = crate::config::ImessageConfig {
            enabled: true,
            api_token: "test".into(),
            webhook_secret: "secret".into(),
            allowed_senders: vec!["+49123456789".into()],
            native_polls: false,
        };
        let prefs = NotificationPreferences::open(dir.path(), &config, true).unwrap();
        let telegram = Arc::new(FakeTelegram::default());
        let guard = Arc::new(Mutex::new(TelegramTurnGuard::new()));
        let delivery = NotificationDelivery {
            preferences: Some(prefs.clone()),
            telegram: telegram.clone(),
            guard: Some(guard.clone()),
            telegram_sent: AtomicUsize::new(0),
            imessage_queued: AtomicUsize::new(0),
            imessage_failed: AtomicUsize::new(0),
        };
        // Quiet completion has no output to either channel.
        assert_eq!(delivery.imessage_status(), "no_notification");
        assert_eq!(guard.lock().unwrap().visible_messages_sent(), 0);
        prefs
            .command(
                Channel::Imessage,
                NotificationCommand::Set(true),
                Some(("chat-1", "+49123456789")),
            )
            .unwrap();
        let both: Value = serde_json::from_str(&delivery.send_message("both", "", None)).unwrap();
        assert_eq!(both["telegram_status"], "sent");
        prefs
            .command(Channel::Telegram, NotificationCommand::Set(false), None)
            .unwrap();
        let imessage: Value =
            serde_json::from_str(&delivery.send_message("iMessage only", "", None)).unwrap();
        assert_eq!(imessage["success"], true);
        assert_eq!(imessage["telegram_status"], "off");
        assert_eq!(imessage["imessage_status"], "queued");
        assert!(imessage["message_id"].is_null());
        assert_eq!(telegram.texts.lock().unwrap().as_slice(), ["both"]);
        assert_eq!(guard.lock().unwrap().visible_messages_sent(), 2);
        assert_eq!(delivery.telegram_messages(), 1);
        assert_eq!(
            serde_json::from_str::<Value>(&delivery.send_file("file", "caption", false)).unwrap()["success"],
            false
        );
        assert_eq!(
            serde_json::from_str::<Value>(&delivery.react("👍", 1)).unwrap()["success"],
            false
        );
        let connection = rusqlite::Connection::open(dir.path().join("linq.sqlite")).unwrap();
        assert_eq!(
            connection
                .query_row("SELECT count(*) FROM linq_outbox", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            2
        );
        prefs
            .command(
                Channel::Imessage,
                NotificationCommand::Set(false),
                Some(("chat-1", "+49123456789")),
            )
            .unwrap();
        delivery.send_message("Telegram only", "", None);
        assert_eq!(
            telegram.texts.lock().unwrap().as_slice(),
            ["both", "Telegram only"]
        );
        assert_eq!(
            connection
                .query_row("SELECT count(*) FROM linq_outbox", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            2
        );
        prefs
            .command(
                Channel::Imessage,
                NotificationCommand::Set(true),
                Some(("chat-1", "+49123456789")),
            )
            .unwrap();
        telegram.fail.store(true, Ordering::Relaxed);
        let partial: Value =
            serde_json::from_str(&delivery.send_message("Telegram unavailable", "", None)).unwrap();
        assert_eq!(partial["success"], true);
        assert_eq!(partial["telegram_status"], "failed");
        assert_eq!(partial["imessage_status"], "queued");
        assert_eq!(delivery.imessage_messages(), 3);
    }

    #[test]
    fn another_recipient_does_not_get_owner_routing() {
        let dir = tempfile::tempdir().unwrap();
        let mut settings = crate::config::test_settings(dir.path());
        settings.telegram.allowed_user_ids = vec![42];
        let delivery = NotificationDelivery::new(&settings, 99, None).unwrap();
        assert!(delivery.preferences.is_none());
        assert!(delivery.telegram_enabled().unwrap());
    }
}
