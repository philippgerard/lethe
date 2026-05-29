//! `lethe service` — install / uninstall / status of the background service.
//!
//! systemd (user instance) on Linux, launchd (LaunchAgent) on macOS. WSL is
//! detected and handled like Linux when systemd is running, otherwise we bail
//! with guidance. The installed service runs `lethe api`, which hosts the
//! HTTP/SSE transport, the Telegram poller, and the Brainstem in one process.
//!
//! Safety: `install` refuses to overwrite an existing unit without `--force`
//! and never restarts a running service implicitly; `uninstall` confirms
//! before stopping the (possibly live) assistant.

use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result, bail};
use lethe::config::Settings;

use crate::ServiceCommand;
use crate::cli::util::confirm;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Manager {
    Systemd,
    Launchd,
    Unsupported,
}

pub struct Platform {
    pub manager: Manager,
    pub is_wsl: bool,
    pub os: &'static str,
}

impl Manager {
    fn label(self) -> &'static str {
        match self {
            Manager::Systemd => "systemd (user)",
            Manager::Launchd => "launchd (LaunchAgent)",
            Manager::Unsupported => "none",
        }
    }
}

pub fn detect() -> Platform {
    let is_wsl = is_wsl();
    if cfg!(target_os = "macos") {
        Platform {
            manager: Manager::Launchd,
            is_wsl: false,
            os: "macOS",
        }
    } else if cfg!(target_os = "linux") {
        let manager = if systemd_running() {
            Manager::Systemd
        } else {
            Manager::Unsupported
        };
        Platform {
            manager,
            is_wsl,
            os: if is_wsl { "WSL" } else { "Linux" },
        }
    } else {
        Platform {
            manager: Manager::Unsupported,
            is_wsl,
            os: "this OS",
        }
    }
}

fn is_wsl() -> bool {
    if std::env::var_os("WSL_DISTRO_NAME").is_some() {
        return true;
    }
    std::fs::read_to_string("/proc/sys/kernel/osrelease")
        .map(|s| {
            let s = s.to_ascii_lowercase();
            s.contains("microsoft") || s.contains("wsl")
        })
        .unwrap_or(false)
}

fn systemd_running() -> bool {
    Path::new("/run/systemd/system").exists()
}

pub fn run(settings: &Settings, command: ServiceCommand) -> Result<()> {
    match command {
        ServiceCommand::Install { force, now } => install(settings, force, now),
        ServiceCommand::Uninstall { yes } => uninstall(yes),
        ServiceCommand::Status => status(settings),
    }
}

// =============================================================================
// Paths + unit rendering
// =============================================================================

pub(crate) fn config_home() -> PathBuf {
    std::env::var_os("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".config")))
        .unwrap_or_else(|| PathBuf::from(".config"))
}

pub(crate) fn systemd_unit_path() -> PathBuf {
    config_home().join("systemd/user/lethe.service")
}

pub(crate) fn launchd_plist_path() -> PathBuf {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_default()
        .join("Library/LaunchAgents/com.lethe.lethe.plist")
}

/// File recording how Lethe was deployed (`native` or `container`), so
/// `service`/`uninstall` know what they're managing. Lives next to `.env`.
pub(crate) fn deployment_marker(settings: &Settings) -> PathBuf {
    settings
        .paths
        .config_file
        .parent()
        .map(|p| p.join("deployment"))
        .unwrap_or_else(|| PathBuf::from("deployment"))
}

pub(crate) fn write_deployment(settings: &Settings, kind: &str) {
    let path = deployment_marker(settings);
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let _ = std::fs::write(&path, format!("{kind}\n"));
}

pub(crate) fn read_deployment(settings: &Settings) -> Option<String> {
    std::fs::read_to_string(deployment_marker(settings))
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

/// Best-effort `loginctl enable-linger` so a rootless user service keeps
/// running without an active login session. Ignored on non-systemd hosts.
pub(crate) fn enable_linger() {
    let _ = Command::new("loginctl").arg("enable-linger").status();
}

fn render_systemd_unit(exe: &Path, workdir: &Path, home: &Path, config: &Path) -> String {
    format!(
        "[Unit]\n\
         Description=Lethe Autonomous AI Agent\n\
         After=network-online.target\n\
         Wants=network-online.target\n\
         \n\
         [Service]\n\
         Type=simple\n\
         WorkingDirectory={workdir}\n\
         ExecStart={exe} api\n\
         Restart=always\n\
         RestartSec=10\n\
         Environment=\"LETHE_HOME={home}\"\n\
         EnvironmentFile=-{config}\n\
         \n\
         [Install]\n\
         WantedBy=default.target\n",
        workdir = workdir.display(),
        exe = exe.display(),
        home = home.display(),
        config = config.display(),
    )
}

fn render_launchd_plist(exe: &Path, home: &Path) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n\
         <plist version=\"1.0\">\n\
         <dict>\n\
         \x20 <key>Label</key>\n\
         \x20 <string>com.lethe.lethe</string>\n\
         \x20 <key>ProgramArguments</key>\n\
         \x20 <array>\n\
         \x20   <string>{exe}</string>\n\
         \x20   <string>api</string>\n\
         \x20 </array>\n\
         \x20 <key>RunAtLoad</key>\n\
         \x20 <true/>\n\
         \x20 <key>KeepAlive</key>\n\
         \x20 <true/>\n\
         \x20 <key>EnvironmentVariables</key>\n\
         \x20 <dict>\n\
         \x20   <key>LETHE_HOME</key>\n\
         \x20   <string>{home}</string>\n\
         \x20 </dict>\n\
         \x20 <key>StandardOutPath</key>\n\
         \x20 <string>{home}/logs/lethe.out.log</string>\n\
         \x20 <key>StandardErrorPath</key>\n\
         \x20 <string>{home}/logs/lethe.err.log</string>\n\
         </dict>\n\
         </plist>\n",
        exe = exe.display(),
        home = home.display(),
    )
}

// =============================================================================
// Install / uninstall / status
// =============================================================================

fn install(settings: &Settings, force: bool, now: bool) -> Result<()> {
    let platform = detect();
    let exe = std::env::current_exe().with_context(|| "resolving current executable path")?;
    let home = &settings.paths.lethe_home;
    let config = &settings.paths.config_file;
    let workdir = exe
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| home.clone());

    match platform.manager {
        Manager::Systemd => {
            let path = systemd_unit_path();
            guard_existing(&path, force)?;
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::write(&path, render_systemd_unit(&exe, &workdir, home, config))
                .with_context(|| format!("writing {}", path.display()))?;
            write_deployment(settings, "native");
            println!("Wrote {}", path.display());
            println!("  ExecStart={} api", exe.display());

            if now || confirm("Enable and start it now? [y/N]: ", false)? {
                enable_linger();
                run_cmd("systemctl", &["--user", "daemon-reload"])?;
                run_cmd("systemctl", &["--user", "enable", "--now", "lethe.service"])?;
                println!("Service enabled and started.");
            } else {
                println!("Next steps:");
                println!("  systemctl --user daemon-reload");
                println!("  systemctl --user enable --now lethe.service");
            }
            Ok(())
        }
        Manager::Launchd => {
            let path = launchd_plist_path();
            guard_existing(&path, force)?;
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::write(&path, render_launchd_plist(&exe, home))
                .with_context(|| format!("writing {}", path.display()))?;
            write_deployment(settings, "native");
            println!("Wrote {}", path.display());
            if now || confirm("Load and start it now? [y/N]: ", false)? {
                run_cmd("launchctl", &["load", "-w", &path.to_string_lossy()])?;
                println!("Service loaded.");
            } else {
                println!("Next step:");
                println!("  launchctl load -w {}", path.display());
            }
            Ok(())
        }
        Manager::Unsupported => bail!(unsupported_message(&platform)),
    }
}

fn uninstall(yes: bool) -> Result<()> {
    let platform = detect();
    match platform.manager {
        Manager::Systemd => {
            let path = systemd_unit_path();
            if !path.exists() {
                println!("No service unit at {}.", path.display());
                return Ok(());
            }
            if !yes
                && !confirm(
                    "Stop, disable and remove the Lethe service? This stops your running assistant. [y/N]: ",
                    false,
                )?
            {
                println!("Cancelled.");
                return Ok(());
            }
            // Best-effort stop/disable; remove the unit regardless.
            let _ = run_cmd(
                "systemctl",
                &["--user", "disable", "--now", "lethe.service"],
            );
            std::fs::remove_file(&path).with_context(|| format!("removing {}", path.display()))?;
            let _ = run_cmd("systemctl", &["--user", "daemon-reload"]);
            println!("Removed {}", path.display());
            Ok(())
        }
        Manager::Launchd => {
            let path = launchd_plist_path();
            if !path.exists() {
                println!("No LaunchAgent at {}.", path.display());
                return Ok(());
            }
            if !yes
                && !confirm(
                    "Unload and remove the Lethe LaunchAgent? This stops your running assistant. [y/N]: ",
                    false,
                )?
            {
                println!("Cancelled.");
                return Ok(());
            }
            let _ = run_cmd("launchctl", &["unload", "-w", &path.to_string_lossy()]);
            std::fs::remove_file(&path).with_context(|| format!("removing {}", path.display()))?;
            println!("Removed {}", path.display());
            Ok(())
        }
        Manager::Unsupported => bail!(unsupported_message(&platform)),
    }
}

fn status(settings: &Settings) -> Result<()> {
    let platform = detect();
    println!(
        "Platform: {}{}",
        platform.os,
        if platform.is_wsl && platform.os != "WSL" {
            " (WSL)"
        } else {
            ""
        }
    );
    println!(
        "Deployment: {}",
        read_deployment(settings)
            .unwrap_or_else(|| "unknown (not installed via lethe)".to_string())
    );
    println!("Service manager: {}", platform.manager.label());
    match platform.manager {
        Manager::Systemd => {
            let path = systemd_unit_path();
            println!(
                "Unit: {} ({})",
                path.display(),
                if path.exists() { "present" } else { "absent" }
            );
            if path.exists() {
                let _ = Command::new("systemctl")
                    .args(["--user", "--no-pager", "status", "lethe.service"])
                    .status();
            }
            Ok(())
        }
        Manager::Launchd => {
            let path = launchd_plist_path();
            println!(
                "LaunchAgent: {} ({})",
                path.display(),
                if path.exists() { "present" } else { "absent" }
            );
            Ok(())
        }
        Manager::Unsupported => {
            println!("{}", unsupported_message(&platform));
            Ok(())
        }
    }
}

/// Offer to install a service during `init` (interactive only). Skips quietly
/// when there's no service manager or one is already installed.
pub fn offer_install(settings: &Settings) -> Result<()> {
    let platform = detect();
    let path = match platform.manager {
        Manager::Systemd => systemd_unit_path(),
        Manager::Launchd => launchd_plist_path(),
        Manager::Unsupported => return Ok(()),
    };
    println!(
        "\nRun Lethe in the background as a service ({} on {})?",
        platform.manager.label(),
        platform.os
    );
    if path.exists() {
        println!("  Already installed at {}. Skipping.", path.display());
        println!("  Manage it with `lethe service status` / `lethe service uninstall`.");
        return Ok(());
    }
    if confirm("  Install Lethe as a service now? [y/N]: ", false)? {
        install(settings, false, false)?;
    } else {
        println!("  Skipped — install later with `lethe service install`.");
    }
    Ok(())
}

// =============================================================================
// Helpers
// =============================================================================

pub(crate) fn guard_existing(path: &Path, force: bool) -> Result<()> {
    if path.exists() && !force {
        bail!(
            "A service unit already exists at {}.\n\
             Refusing to overwrite it (your running assistant may use it).\n\
             Re-run with --force to replace it — that alone won't stop the running service.",
            path.display()
        );
    }
    Ok(())
}

pub(crate) fn run_cmd(program: &str, args: &[&str]) -> Result<()> {
    let status = Command::new(program)
        .args(args)
        .status()
        .with_context(|| format!("running `{program} {}`", args.join(" ")))?;
    if !status.success() {
        bail!("`{program} {}` failed ({status})", args.join(" "));
    }
    Ok(())
}

fn unsupported_message(platform: &Platform) -> String {
    if platform.is_wsl {
        "No systemd on this WSL instance. Enable it (add `[boot]\\nsystemd=true` to \
         /etc/wsl.conf, then `wsl --shutdown`) and retry, or run `lethe api` under your \
         own supervisor."
            .to_string()
    } else {
        format!(
            "No supported service manager detected on {}. Run `lethe api` under your own \
             supervisor (tmux, nohup, or a cron @reboot entry).",
            platform.os
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn systemd_unit_has_execstart_and_install_section() {
        let unit = render_systemd_unit(
            Path::new("/usr/local/bin/lethe"),
            Path::new("/home/x/devel/lethe"),
            Path::new("/home/x/.lethe"),
            Path::new("/home/x/.lethe/config/.env"),
        );
        assert!(unit.contains("ExecStart=/usr/local/bin/lethe api"));
        assert!(unit.contains("WorkingDirectory=/home/x/devel/lethe"));
        assert!(unit.contains("Environment=\"LETHE_HOME=/home/x/.lethe\""));
        assert!(unit.contains("EnvironmentFile=-/home/x/.lethe/config/.env"));
        assert!(unit.contains("[Install]\nWantedBy=default.target"));
    }

    #[test]
    fn launchd_plist_has_program_arguments() {
        let plist = render_launchd_plist(
            Path::new("/usr/local/bin/lethe"),
            Path::new("/Users/x/.lethe"),
        );
        assert!(plist.contains("<string>com.lethe.lethe</string>"));
        assert!(plist.contains("<string>/usr/local/bin/lethe</string>"));
        assert!(plist.contains("<string>api</string>"));
        assert!(plist.contains("<string>/Users/x/.lethe</string>"));
    }

    #[test]
    fn guard_refuses_existing_without_force() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("lethe.service");
        std::fs::write(&path, "x").unwrap();
        assert!(guard_existing(&path, false).is_err());
        assert!(guard_existing(&path, true).is_ok());
        assert!(guard_existing(&tmp.path().join("absent.service"), false).is_ok());
    }
}
