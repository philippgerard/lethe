//! `lethe identity` — view and change who the assistant is.
//!
//! "Identity" is two things kept in sync: the `LETHE_AGENT_NAME` config value
//! (the short name, used in transports and the status view) and the `identity`
//! memory block (the second-person system persona). This module reads/writes
//! both, and exposes helpers the `init` wizard uses to set identity during
//! first-time setup.

use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use lethe::config::Settings;
use lethe::memory::BlockManager;

use crate::IdentityCommand;
use crate::cli::util::{confirm, prompt_line, read_multiline, upsert_env};

/// A custom identity collected from the user.
pub struct IdentitySetup {
    pub name: String,
    /// Free-form persona text. `None` means "keep whatever block exists"
    /// (used when the user renames without rewriting the persona).
    pub persona: Option<String>,
}

pub fn run(settings: &Settings, command: Option<IdentityCommand>) -> Result<()> {
    match command.unwrap_or(IdentityCommand::Show) {
        IdentityCommand::Show => show(settings),
        IdentityCommand::Set { name } => set(settings, name),
        IdentityCommand::Reset { yes } => reset(settings, yes),
        IdentityCommand::Edit => edit(settings),
    }
}

fn blocks_dir(settings: &Settings) -> PathBuf {
    settings.paths.workspace_dir.join("memory")
}

fn block_manager(settings: &Settings) -> Result<BlockManager> {
    let dir = blocks_dir(settings);
    let manager =
        BlockManager::new(&dir).with_context(|| format!("opening blocks dir {}", dir.display()))?;
    manager
        .init_embedded_defaults()
        .with_context(|| "seeding default memory blocks")?;
    Ok(manager)
}

/// Write `value` to the named memory block (seeding defaults first so the
/// block exists). Shared with `init` for the `human` block.
pub fn write_block(settings: &Settings, label: &str, value: &str) -> Result<()> {
    let manager = block_manager(settings)?;
    manager
        .update(label, Some(value), None)
        .with_context(|| format!("writing {label} block"))?;
    Ok(())
}

fn render_identity(name: &str, persona: &str) -> String {
    format!("You are {name}.\n\n{persona}\n")
}

pub fn show(settings: &Settings) -> Result<()> {
    let manager = block_manager(settings)?;
    println!("Name (LETHE_AGENT_NAME): {}", settings.agent_name);
    let path = blocks_dir(settings).join("identity.md");
    println!("Identity block: {}\n", path.display());
    match manager.get("identity")? {
        Some(block) if !block.value.trim().is_empty() => {
            println!("{}", block.value.trim_end());
        }
        _ => println!("(no identity block yet — using the embedded default)"),
    }
    Ok(())
}

pub fn set(settings: &Settings, name_flag: Option<String>) -> Result<()> {
    let current = &settings.agent_name;
    let name = match name_flag {
        Some(n) => n.trim().to_string(),
        None => {
            let entered = prompt_line(&format!("Assistant name [{current}]: "))?;
            let entered = entered.trim().to_string();
            if entered.is_empty() {
                current.clone()
            } else {
                entered
            }
        }
    };
    if name.is_empty() {
        bail!("a name is required");
    }

    let persona = read_multiline(&[
        "Describe who they are — personality, role, voice.",
        "Multi-line; finish with an empty line. Blank keeps the current persona.",
    ])?;

    if let Some(persona) = persona {
        write_block(settings, "identity", &render_identity(&name, &persona))?;
        println!("Updated the identity block.");
    }

    upsert_env(
        &settings.paths.config_file,
        &[("LETHE_AGENT_NAME".into(), name.clone())],
    )?;
    println!(
        "Set LETHE_AGENT_NAME={name} in {}",
        settings.paths.config_file.display()
    );
    println!("Restart any running `lethe` service for the change to take effect.");
    Ok(())
}

pub fn reset(settings: &Settings, yes: bool) -> Result<()> {
    if !yes
        && !confirm(
            "Restore the default Lethe identity (overwrites your identity block)? [y/N]: ",
            false,
        )?
    {
        println!("Cancelled.");
        return Ok(());
    }
    let dir = blocks_dir(settings);
    let manager =
        BlockManager::new(&dir).with_context(|| format!("opening blocks dir {}", dir.display()))?;
    manager
        .delete("identity")
        .with_context(|| "removing identity block")?;
    manager
        .init_embedded_defaults()
        .with_context(|| "reseeding the default identity")?;
    upsert_env(
        &settings.paths.config_file,
        &[("LETHE_AGENT_NAME".into(), "lethe".into())],
    )?;
    println!("Restored the default identity (LETHE_AGENT_NAME=lethe).");
    Ok(())
}

pub fn edit(settings: &Settings) -> Result<()> {
    let _ = block_manager(settings)?; // ensure the block file exists
    let path = blocks_dir(settings).join("identity.md");
    let editor = std::env::var("VISUAL")
        .or_else(|_| std::env::var("EDITOR"))
        .unwrap_or_else(|_| "vi".to_string());
    println!("Opening {} in {editor}...", path.display());
    let status = std::process::Command::new(&editor)
        .arg(&path)
        .status()
        .with_context(|| format!("launching editor `{editor}`"))?;
    if !status.success() {
        bail!("editor `{editor}` exited with {status}");
    }
    println!("Saved. Restart any running `lethe` service to pick up the change.");
    Ok(())
}

/// Interactive identity prompt used by `lethe init`. Returns `None` when the
/// user keeps the default Lethe persona.
pub fn prompt_identity(default_name: &str) -> Result<Option<IdentitySetup>> {
    println!("Who should your assistant be?");
    println!("  Lethe ships with a default identity (an autonomous AI named '{default_name}').");
    println!("  1) Keep the default identity");
    println!("  2) Define your own (name + short description)");
    let choice = prompt_line("Choose [1-2, default=1]: ")?;
    if choice.trim() != "2" {
        return Ok(None);
    }
    let name = loop {
        let entered = prompt_line("  Assistant name: ")?;
        let entered = entered.trim().to_string();
        if !entered.is_empty() {
            break entered;
        }
        println!("  (a name is required)");
    };
    let persona = read_multiline(&[
        "  Describe who they are — personality, role, voice (optional).",
        "  Multi-line; finish with an empty line.",
    ])?;
    Ok(Some(IdentitySetup { name, persona }))
}

/// Apply a collected identity: rewrite the `identity` block when a persona was
/// supplied. The name is persisted to `.env` separately by the caller (`init`
/// folds it into its single config write).
pub fn apply_identity(settings: &Settings, setup: &IdentitySetup) -> Result<()> {
    if let Some(persona) = &setup.persona {
        write_block(settings, "identity", &render_identity(&setup.name, persona))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_identity_is_second_person() {
        let text = render_identity("Aria", "You help with research.");
        assert!(text.starts_with("You are Aria."));
        assert!(text.contains("You help with research."));
        assert!(text.ends_with('\n'));
    }

    #[test]
    fn write_and_read_block_roundtrips() {
        let tmp = tempfile::tempdir().unwrap();
        let settings = lethe::config::test_settings(tmp.path());
        write_block(&settings, "identity", "You are Test.\n\nA tester.").unwrap();
        let manager = block_manager(&settings).unwrap();
        let block = manager.get("identity").unwrap().unwrap();
        assert!(block.value.contains("You are Test."));
    }
}
