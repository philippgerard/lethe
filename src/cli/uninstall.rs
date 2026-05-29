//! `lethe uninstall` — interactive teardown. Removes the service (native or
//! container), tears down the container when that's the deployment, and only
//! deletes your data when you explicitly ask with `--purge` and confirm.
//!
//! Data safety: the `~/.lethe` workspace + memory are irreplaceable, so a purge
//! is never implied — it requires `--purge` *and* an interactive confirmation,
//! and `--yes` does not bypass that final prompt.

use anyhow::Result;
use lethe::config::Settings;

use crate::cli::util::confirm;
use crate::cli::{container, service};

pub fn run(settings: &Settings, yes: bool, purge: bool) -> Result<()> {
    let deployment = service::read_deployment(settings);
    println!("Lethe uninstall");
    println!(
        "  deployment: {}",
        deployment
            .as_deref()
            .unwrap_or("unknown (not installed via lethe)")
    );
    println!("  data dir:   {}", settings.paths.lethe_home.display());
    println!();

    remove_service(yes)?;

    if deployment.as_deref() == Some("container")
        && (yes || confirm("Remove the Lethe container? [Y/n]: ", true)?)
    {
        let remove_image = yes || confirm("  Also remove the built image? [y/N]: ", false)?;
        container::teardown(settings, remove_image)?;
        println!(
            "Removed the container{}.",
            if remove_image { " and image" } else { "" }
        );
    }

    service::write_deployment(settings, "removed");

    if purge {
        purge_data(settings)?;
    } else {
        println!();
        println!(
            "Kept your data at {} (config, memory, workspace).",
            settings.paths.lethe_home.display()
        );
        println!("Re-run with `lethe uninstall --purge` to delete it too.");
    }
    println!("\nDone.");
    Ok(())
}

fn remove_service(yes: bool) -> Result<()> {
    let platform = service::detect();
    match platform.manager {
        service::Manager::Systemd => {
            let path = service::systemd_unit_path();
            if !path.exists() {
                println!("No systemd service unit installed.");
                return Ok(());
            }
            if !yes && !confirm("Stop and remove the Lethe service? [Y/n]: ", true)? {
                println!("Left the service in place.");
                return Ok(());
            }
            let _ = service::run_cmd(
                "systemctl",
                &["--user", "disable", "--now", "lethe.service"],
            );
            let _ = std::fs::remove_file(&path);
            let _ = service::run_cmd("systemctl", &["--user", "daemon-reload"]);
            println!("Removed {}", path.display());
        }
        service::Manager::Launchd => {
            let path = service::launchd_plist_path();
            if !path.exists() {
                println!("No LaunchAgent installed.");
                return Ok(());
            }
            if !yes && !confirm("Stop and remove the Lethe LaunchAgent? [Y/n]: ", true)? {
                println!("Left the LaunchAgent in place.");
                return Ok(());
            }
            let _ = service::run_cmd("launchctl", &["unload", "-w", &path.to_string_lossy()]);
            let _ = std::fs::remove_file(&path);
            println!("Removed {}", path.display());
        }
        service::Manager::Unsupported => {
            println!("No service manager detected — nothing to remove.");
        }
    }
    Ok(())
}

/// Delete `~/.lethe`. Always asks (even with `--yes`), defaults to *no*, since
/// the data is irreplaceable.
fn purge_data(settings: &Settings) -> Result<()> {
    let home = &settings.paths.lethe_home;
    if !home.exists() {
        println!("No data dir at {} — nothing to purge.", home.display());
        return Ok(());
    }
    println!();
    println!("! PURGE: this permanently deletes {} —", home.display());
    println!("! your config, memory, message history, and workspace. Irreversible.");
    if !confirm("Type y to delete everything [y/N]: ", false)? {
        println!("Purge cancelled — data kept.");
        return Ok(());
    }
    std::fs::remove_dir_all(home)
        .map_err(|e| anyhow::anyhow!("removing {}: {e}", home.display()))?;
    println!("Deleted {}", home.display());
    Ok(())
}
