//! Durable notification preferences shared by both native chat commands.
use std::path::Path;

use anyhow::{Result, bail};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};

use super::imessage::DeliveryStore;
use crate::config::ImessageConfig;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Channel {
    Telegram,
    Imessage,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NotificationCommand {
    Status,
    Set(bool),
    Help,
}

impl NotificationCommand {
    pub fn parse(text: &str) -> Option<Self> {
        let mut words = text.split_whitespace();
        if !words
            .next()?
            .split('@')
            .next()?
            .eq_ignore_ascii_case("/notifications")
        {
            return None;
        }
        let argument = words.next().unwrap_or("status").to_ascii_lowercase();
        Some(if words.next().is_some() {
            Self::Help
        } else {
            match argument.as_str() {
                "on" => Self::Set(true),
                "off" => Self::Set(false),
                "status" => Self::Status,
                _ => Self::Help,
            }
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NotificationRoute {
    pub telegram: bool,
    pub imessage: Option<(String, String)>,
    pub generation: i64,
}

#[derive(Clone)]
pub struct NotificationPreferences {
    store: DeliveryStore,
    telegram_available: bool,
    imessage_available: bool,
    allowed_senders: Vec<String>,
}

impl NotificationPreferences {
    pub fn open(
        directory: &Path,
        imessage: &ImessageConfig,
        telegram_available: bool,
    ) -> Result<Self> {
        let store = DeliveryStore::open(&directory.join("linq.sqlite"))?;
        store.connection()?.execute_batch(
            "CREATE TABLE IF NOT EXISTS linq_notification_target (
                singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                chat TEXT NOT NULL, sender TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS notification_preferences (
                singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                telegram INTEGER NOT NULL, imessage INTEGER NOT NULL,
                generation INTEGER NOT NULL DEFAULT 0,
                CHECK(telegram OR imessage)
             );
             INSERT OR IGNORE INTO notification_preferences(singleton,telegram,imessage)
             VALUES (1,1,EXISTS(SELECT 1 FROM linq_notification_target));",
        )?;
        Ok(Self {
            store,
            telegram_available,
            imessage_available: imessage.enabled && imessage.validate().is_ok(),
            allowed_senders: imessage.allowed_senders.clone(),
        })
    }

    fn linked_target(&self, connection: &Connection) -> Result<Option<(String, String)>> {
        let target: Option<(String, String)> = connection
            .query_row(
                "SELECT chat,sender FROM linq_notification_target WHERE singleton=1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        Ok(target
            .filter(|(_, sender)| self.imessage_available && self.allowed_senders.contains(sender)))
    }

    fn read_route(&self, connection: &Connection) -> Result<NotificationRoute> {
        let (telegram, imessage, generation): (bool, bool, i64) = connection.query_row(
            "SELECT telegram,imessage,generation FROM notification_preferences WHERE singleton=1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;
        let linked = self.linked_target(connection)?;
        let mut route = NotificationRoute {
            telegram: telegram && self.telegram_available,
            imessage: linked.clone().filter(|_| imessage),
            generation,
        };
        // Revoking an allowlist entry or disabling a transport must not leave
        // the only selected destination unusable.
        if !route.telegram && route.imessage.is_none() {
            if self.telegram_available {
                route.telegram = true;
            } else if linked.is_some() {
                route.imessage = linked;
            } else {
                bail!("No usable notification channel is configured");
            }
        }
        if imessage && route.imessage.is_none() {
            route.generation += 1;
        }
        Ok(route)
    }

    pub fn route(&self) -> Result<NotificationRoute> {
        let mut connection = self.store.connection()?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let route = self.read_route(&tx)?;
        self.save_route(&tx, &route)?;
        tx.commit()?;
        Ok(route)
    }

    fn save_route(&self, connection: &Connection, route: &NotificationRoute) -> Result<()> {
        connection.execute(
            "UPDATE notification_preferences SET telegram=?1,imessage=?2,generation=?3 WHERE singleton=1",
            params![route.telegram, route.imessage.is_some(), route.generation],
        )?;
        Ok(())
    }

    /// Caller authenticates the private owner before supplying a linked chat.
    pub fn command(
        &self,
        channel: Channel,
        command: NotificationCommand,
        linked: Option<(&str, &str)>,
    ) -> Result<String> {
        if command == NotificationCommand::Help {
            return Ok("Use /notifications to see both channels, or /notifications on or /notifications off to change this channel.".into());
        }
        let mut connection = self.store.connection()?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let previous_target = self.linked_target(&tx)?;
        if matches!(command, NotificationCommand::Set(_)) && channel == Channel::Imessage {
            let Some((chat, sender)) = linked else {
                bail!("iMessage conversation is required");
            };
            if !self.imessage_available
                || !self.allowed_senders.iter().any(|allowed| allowed == sender)
            {
                bail!("iMessage sender is not authorized");
            }
            tx.execute(
                "INSERT INTO linq_notification_target VALUES(1,?1,?2)
                 ON CONFLICT(singleton) DO UPDATE SET chat=excluded.chat,sender=excluded.sender",
                params![chat, sender],
            )?;
        }
        let mut route = self.read_route(&tx)?;
        let mut explanation = String::new();
        if let NotificationCommand::Set(enabled) = command {
            let target = self.linked_target(&tx)?;
            match channel {
                Channel::Telegram => route.telegram = enabled && self.telegram_available,
                Channel::Imessage => route.imessage = target.clone().filter(|_| enabled),
            }
            explanation = self.ensure_channel(&mut route, channel, target).to_string();
            // Invalidate queued notifications whenever consent changes. A
            // later on command must not resurrect messages queued before off.
            if (channel == Channel::Imessage && !enabled && route.imessage.is_none())
                || previous_target != self.linked_target(&tx)?
            {
                route.generation += 1;
            }
        }
        self.save_route(&tx, &route)?;
        tx.commit()?;
        Ok(format!(
            "{explanation}Notifications: Telegram {}; iMessage {}. Replies still come back in the chat you use.",
            if route.telegram { "on" } else { "off" },
            if route.imessage.is_some() {
                "on"
            } else {
                "off"
            }
        ))
    }

    fn ensure_channel(
        &self,
        route: &mut NotificationRoute,
        channel: Channel,
        target: Option<(String, String)>,
    ) -> &'static str {
        if route.telegram || route.imessage.is_some() {
            return "";
        }
        match channel {
            Channel::Telegram if target.is_some() => {
                route.imessage = target;
                "iMessage was automatically turned on so you still receive notifications. "
            }
            Channel::Imessage if self.telegram_available => {
                route.telegram = true;
                "Telegram was automatically turned on so you still receive notifications. "
            }
            Channel::Telegram => {
                route.telegram = self.telegram_available;
                "Telegram stays on because iMessage is not linked or available. Send /notifications on in our iMessage chat first. "
            }
            Channel::Imessage => {
                route.imessage = target;
                "iMessage stays on because Telegram is not configured. "
            }
        }
    }

    pub(crate) fn queue_imessage(&self, text: &str) -> Result<bool> {
        Ok(self.queue_delivery(text)?.imessage.is_some())
    }

    /// Select both destinations and persist iMessage output in one transaction,
    /// so simultaneous chat commands cannot create a gap between two reads.
    pub(crate) fn queue_delivery(&self, text: &str) -> Result<NotificationRoute> {
        let mut connection = self.store.connection()?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let route = self.read_route(&tx)?;
        self.save_route(&tx, &route)?;
        let Some((chat, sender)) = &route.imessage else {
            tx.commit()?;
            return Ok(route);
        };
        let output = super::imessage::prepare_output(serde_json::json!({
            "kind": "notification", "content": text, "notification_generation": route.generation,
        }))?;
        tx.execute(
            "INSERT INTO linq_outbox(id,chat,sender,payload) VALUES(?1,?2,?3,?4)",
            params![
                uuid::Uuid::new_v4().to_string(),
                chat,
                sender,
                output.to_string()
            ],
        )?;
        tx.commit()?;
        Ok(route)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    pub(crate) fn config() -> ImessageConfig {
        ImessageConfig {
            enabled: true,
            api_token: "test-token".into(),
            webhook_secret: "test-secret".into(),
            allowed_senders: vec!["+49123456789".into()],
            native_polls: false,
        }
    }

    fn set(prefs: &NotificationPreferences, channel: Channel, on: bool) -> String {
        prefs
            .command(
                channel,
                NotificationCommand::Set(on),
                Some(("chat-1", "+49123456789")),
            )
            .unwrap()
    }

    #[test]
    fn commands_parse_status_case_whitespace_and_bot_suffix_without_model() {
        for text in [
            "/notifications",
            " /Notifications STATUS ",
            "/notifications@LetheBot status",
        ] {
            assert_eq!(
                NotificationCommand::parse(text),
                Some(NotificationCommand::Status)
            );
        }
        assert_eq!(
            NotificationCommand::parse("/notifications off"),
            Some(NotificationCommand::Set(false))
        );
        assert_eq!(
            NotificationCommand::parse("/notifications on extra"),
            Some(NotificationCommand::Help)
        );
        assert_eq!(
            NotificationCommand::parse("/notifications maybe"),
            Some(NotificationCommand::Help)
        );
        assert_eq!(
            NotificationCommand::parse("please /notifications off"),
            None
        );
    }

    #[test]
    fn both_channels_can_be_enabled_and_last_off_switches_back_across_restart() {
        let dir = tempfile::tempdir().unwrap();
        let prefs = NotificationPreferences::open(dir.path(), &config(), true).unwrap();
        assert!(set(&prefs, Channel::Telegram, false).contains("Telegram stays on"));
        assert!(prefs.route().unwrap().telegram);
        set(&prefs, Channel::Imessage, true);
        assert!(prefs.route().unwrap().telegram);
        assert!(prefs.route().unwrap().imessage.is_some());
        set(&prefs, Channel::Telegram, false);
        assert!(!prefs.route().unwrap().telegram);
        assert!(
            set(&prefs, Channel::Imessage, false).contains("Telegram was automatically turned on")
        );
        assert!(prefs.route().unwrap().imessage.is_none());
        drop(prefs);
        let prefs = NotificationPreferences::open(dir.path(), &config(), true).unwrap();
        assert!(
            set(&prefs, Channel::Telegram, false).contains("iMessage was automatically turned on")
        );
        assert!(!prefs.route().unwrap().telegram);
        assert_eq!(
            prefs.route().unwrap().imessage,
            Some(("chat-1".into(), "+49123456789".into()))
        );
    }

    #[test]
    fn migrates_existing_consent_and_fails_over_after_allowlist_revocation() {
        let dir = tempfile::tempdir().unwrap();
        let store = DeliveryStore::open(&dir.path().join("linq.sqlite")).unwrap();
        store.connection().unwrap().execute_batch("CREATE TABLE linq_notification_target(singleton INTEGER PRIMARY KEY,chat TEXT,sender TEXT); INSERT INTO linq_notification_target VALUES(1,'chat-1','+49123456789');").unwrap();
        let prefs = NotificationPreferences::open(dir.path(), &config(), true).unwrap();
        assert!(prefs.route().unwrap().imessage.is_some());
        set(&prefs, Channel::Telegram, false);
        let mut removed = config();
        removed.allowed_senders = vec!["+49999999999".into()];
        let revoked = NotificationPreferences::open(dir.path(), &removed, true).unwrap();
        let route = revoked.route().unwrap();
        assert!(route.telegram);
        assert!(route.imessage.is_none());
        assert!(!revoked.queue_imessage("private").unwrap());
        assert!(
            revoked
                .command(
                    Channel::Imessage,
                    NotificationCommand::Set(true),
                    Some(("chat-1", "+49123456789"))
                )
                .is_err()
        );
    }

    #[test]
    fn only_configured_channel_is_retained_and_status_does_not_change_consent() {
        let dir = tempfile::tempdir().unwrap();
        let prefs = NotificationPreferences::open(dir.path(), &config(), false).unwrap();
        set(&prefs, Channel::Imessage, true);
        let before = prefs.route().unwrap();
        assert!(set(&prefs, Channel::Imessage, false).contains("iMessage stays on"));
        assert!(prefs.route().unwrap().imessage.is_some());
        prefs
            .command(Channel::Imessage, NotificationCommand::Status, None)
            .unwrap();
        assert_eq!(prefs.route().unwrap().imessage, before.imessage);
    }

    #[test]
    fn competing_commands_never_persist_both_channels_off() {
        let dir = tempfile::tempdir().unwrap();
        let prefs = NotificationPreferences::open(dir.path(), &config(), true).unwrap();
        set(&prefs, Channel::Imessage, true);
        std::thread::scope(|scope| {
            for channel in [Channel::Telegram, Channel::Imessage] {
                let prefs = prefs.clone();
                scope.spawn(move || {
                    for _ in 0..20 {
                        set(&prefs, channel, false);
                        let route = prefs.route().unwrap();
                        assert!(route.telegram || route.imessage.is_some());
                    }
                });
            }
        });
        let route = prefs.route().unwrap();
        assert!(route.telegram || route.imessage.is_some());
    }
}
