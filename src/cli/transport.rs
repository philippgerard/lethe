//! `lethe transport` — configure how you reach Lethe: the local HTTP API
//! (which powers `lethe tui`) and chat channels like Telegram. Discord and
//! Signal are planned. Settings are written to the config `.env`; `lethe run`
//! (and `lethe api`) start only the *enabled* transports.

use anyhow::{Result, bail};
use lethe::config::Settings;

use crate::TransportCommand;
use crate::cli::util::{
    confirm, mask_secret, prompt_line, prompt_secret, secret_status, upsert_env,
};

pub fn run(settings: &Settings, command: Option<TransportCommand>) -> Result<()> {
    match command.unwrap_or(TransportCommand::List) {
        TransportCommand::List => {
            list(settings);
            Ok(())
        }
        TransportCommand::Telegram { enable, disable } => telegram(settings, enable, disable),
        TransportCommand::Api {
            enable,
            disable,
            port,
            token,
        } => api(settings, enable, disable, port, token),
    }
}

fn on_off(enabled: bool) -> &'static str {
    if enabled { "enabled " } else { "disabled" }
}

fn list(settings: &Settings) {
    println!(
        "Transports  (config: {})\n",
        settings.paths.config_file.display()
    );
    println!(
        "  api       {}  {}:{}  token {:<8}  — local control plane, powers `lethe tui`",
        on_off(settings.api.enabled),
        settings.api.host,
        settings.api.port,
        if settings.api.token.trim().is_empty() {
            "not set"
        } else {
            "set"
        },
    );
    let tg = &settings.telegram;
    println!(
        "  telegram  {}  token {}, {} allowed id(s)",
        on_off(tg.enabled),
        if tg.bot_token.trim().is_empty() {
            "not set"
        } else {
            "set"
        },
        tg.allowed_user_ids.len(),
    );
    println!("  discord   --        (planned)");
    println!("  signal    --        (planned)");
    println!();
    println!("Configure: `lethe transport telegram`   Toggle: `--enable` / `--disable`");
}

fn telegram(settings: &Settings, enable: bool, disable: bool) -> Result<()> {
    if enable && disable {
        bail!("--enable and --disable are mutually exclusive");
    }
    let path = &settings.paths.config_file;
    if disable {
        upsert_env(path, &[("TELEGRAM_ENABLED".into(), "false".into())])?;
        println!("Telegram disabled (token kept). Restart `lethe run` to apply.");
        return Ok(());
    }
    if enable {
        if settings.telegram.bot_token.trim().is_empty() {
            bail!("No bot token set — run `lethe transport telegram` to configure one first.");
        }
        upsert_env(path, &[("TELEGRAM_ENABLED".into(), "true".into())])?;
        println!("Telegram enabled. Restart `lethe run` to apply.");
        return Ok(());
    }

    // No flag → interactively configure + enable.
    println!("Configure the Telegram bot:");
    let existing = settings.telegram.bot_token.trim().to_string();
    let token = if !existing.is_empty() {
        println!("  Current token: {}", mask_secret(&existing));
        if confirm("  Keep it? [Y/n]: ", true)? {
            existing
        } else {
            prompt_secret("  Paste new bot token (input hidden): ")?
                .trim()
                .to_string()
        }
    } else {
        println!("  Get a token from @BotFather (https://t.me/BotFather).");
        prompt_secret("  Paste TELEGRAM_BOT_TOKEN (input hidden): ")?
            .trim()
            .to_string()
    };
    if token.is_empty() {
        bail!("a bot token is required");
    }
    let allowed = prompt_line("  Allowed Telegram user ids (comma-separated, blank for any): ")?
        .trim()
        .to_string();
    let mut updates = vec![
        ("TELEGRAM_BOT_TOKEN".into(), token),
        ("TELEGRAM_ENABLED".into(), "true".into()),
    ];
    if allowed.is_empty() {
        println!("  ! No allowed ids — ANYONE who finds the bot can talk to your assistant.");
    } else {
        updates.push(("TELEGRAM_ALLOWED_USER_IDS".into(), allowed));
    }
    upsert_env(path, &updates)?;
    println!("Telegram configured + enabled. Start it with `lethe run`.");
    Ok(())
}

fn api(
    settings: &Settings,
    enable: bool,
    disable: bool,
    port: Option<u16>,
    token: bool,
) -> Result<()> {
    if enable && disable {
        bail!("--enable and --disable are mutually exclusive");
    }
    let path = &settings.paths.config_file;
    let mut updates: Vec<(String, String)> = Vec::new();
    if disable {
        updates.push(("API_ENABLED".into(), "false".into()));
    }
    if enable {
        updates.push(("API_ENABLED".into(), "true".into()));
    }
    if let Some(p) = port {
        updates.push(("LETHE_API_PORT".into(), p.to_string()));
    }
    if token {
        updates.push((
            "LETHE_API_TOKEN".into(),
            uuid::Uuid::new_v4().simple().to_string(),
        ));
    }

    if updates.is_empty() {
        println!(
            "api: {}  {}:{}  token {}",
            on_off(settings.api.enabled),
            settings.api.host,
            settings.api.port,
            secret_status(&settings.api.token),
        );
        println!("Flags: --enable | --disable | --port <N> | --token (generate a fresh one)");
        return Ok(());
    }
    upsert_env(path, &updates)?;
    if disable {
        println!("! HTTP API disabled — `lethe tui` won't be able to connect.");
    }
    if token {
        println!("Generated a new LETHE_API_TOKEN.");
    }
    println!("Updated. Restart `lethe run` to apply.");
    Ok(())
}
