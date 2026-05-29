//! `lethe prompts` — export and inspect the overridable prompt templates.
//!
//! The built-in prompts are compiled into the binary. `export` writes them to
//! `<workspace>/prompts/` (under `~/.lethe` by default) so they can be edited;
//! at runtime `PromptStore` loads that directory ahead of the embedded copy
//! (workspace → config → embedded), so an exported+edited file overrides the
//! built-in on the next run. Existing files are never clobbered without
//! `--force`.

use std::path::PathBuf;

use anyhow::{Context, Result};
use lethe::config::Settings;
use lethe::llm::prompts::{PromptSource, PromptStore, embedded_prompts};

use crate::PromptsCommand;

pub fn run(settings: &Settings, command: PromptsCommand) -> Result<()> {
    match command {
        PromptsCommand::Export { force, dir } => export(settings, force, dir),
        PromptsCommand::List => list(settings),
    }
}

fn override_dir(settings: &Settings) -> PathBuf {
    settings.paths.workspace_dir.join("prompts")
}

fn export(settings: &Settings, force: bool, dir: Option<PathBuf>) -> Result<()> {
    let dir = dir.unwrap_or_else(|| override_dir(settings));
    std::fs::create_dir_all(&dir).with_context(|| format!("creating {}", dir.display()))?;

    let mut written = 0usize;
    let mut skipped = 0usize;
    for (name, text) in embedded_prompts() {
        let path = dir.join(format!("{name}.md"));
        if path.exists() && !force {
            println!("  skip   {} (exists)", path.display());
            skipped += 1;
            continue;
        }
        std::fs::write(&path, text).with_context(|| format!("writing {}", path.display()))?;
        println!("  write  {}", path.display());
        written += 1;
    }

    println!();
    println!("Exported {written} prompt(s) to {}.", dir.display());
    if skipped > 0 {
        println!(
            "Kept {skipped} existing file(s) untouched{}.",
            if force {
                ""
            } else {
                " — re-run with --force to overwrite"
            }
        );
    }
    println!("Edit any file there; it overrides the built-in on the next `lethe` start.");
    Ok(())
}

fn list(settings: &Settings) -> Result<()> {
    let store = PromptStore::new(&settings.paths.workspace_dir, &settings.paths.config_dir);
    println!("Overridable prompts (resolution order: workspace → config → embedded):\n");
    for (name, _) in embedded_prompts() {
        let resolved = store.load(name, "");
        let source = match &resolved.source {
            PromptSource::Workspace(p) => format!("workspace  {}", p.display()),
            PromptSource::Config(p) => format!("config     {}", p.display()),
            PromptSource::Embedded => "embedded   (built-in)".to_string(),
            PromptSource::Fallback => "fallback".to_string(),
        };
        println!("  {name:<24} {source}");
    }
    println!();
    println!("Override dir: {}", override_dir(settings).display());
    println!("Run `lethe prompts export` to write the built-ins there for editing.");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn export_writes_then_skips_existing() {
        let tmp = tempfile::tempdir().unwrap();
        let settings = lethe::config::test_settings(tmp.path());
        let dir = override_dir(&settings);

        export(&settings, false, None).unwrap();
        let sample = dir.join("agent_instructions.md");
        assert!(sample.exists());
        let original = std::fs::read_to_string(&sample).unwrap();
        assert!(!original.trim().is_empty());

        // Tamper, re-export without --force: file must be preserved.
        std::fs::write(&sample, "EDITED").unwrap();
        export(&settings, false, None).unwrap();
        assert_eq!(std::fs::read_to_string(&sample).unwrap(), "EDITED");

        // With --force it is overwritten back to the built-in.
        export(&settings, true, None).unwrap();
        assert_eq!(std::fs::read_to_string(&sample).unwrap(), original);
    }
}
