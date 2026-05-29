//! `lethe model` — show or change the configured LLM model(s), persisted to
//! the config `.env` via the same safe merge as the rest of the CLI. The
//! catalog (config/model_catalog.json) is per-provider; any custom id is
//! accepted too. Changes apply on the next `lethe run` / service restart.

use anyhow::{Result, bail};
use lethe::config::Settings;
use lethe::llm::models::{ModelEntry, model_catalog, normalize_model_id};

use crate::cli::util::{prompt_line, upsert_env};

pub fn run(
    settings: &Settings,
    model: Option<String>,
    aux: Option<String>,
    pick: bool,
) -> Result<()> {
    let provider = settings.llm.llm_provider.trim().to_string();

    if pick {
        return pick_interactive(settings, &provider);
    }
    if model.is_none() && aux.is_none() {
        show(settings, &provider);
        return Ok(());
    }

    let mut updates: Vec<(String, String)> = Vec::new();
    if let Some(m) = model {
        let m = m.trim();
        if m.is_empty() {
            bail!("empty model id");
        }
        updates.push(("LLM_MODEL".into(), normalize_model_id(&provider, m)));
    }
    if let Some(a) = aux {
        let a = a.trim();
        if a.is_empty() {
            bail!("empty aux model id");
        }
        updates.push(("LLM_MODEL_AUX".into(), normalize_model_id(&provider, a)));
    }
    upsert_env(&settings.paths.config_file, &updates)?;
    for (k, v) in &updates {
        println!("Set {k}={v}");
    }
    println!("Restart `lethe run` (or the service) to apply.");
    Ok(())
}

fn show(settings: &Settings, provider: &str) {
    let llm = &settings.llm;
    println!("Current:");
    println!(
        "  provider:  {}",
        if provider.is_empty() {
            "(not set)"
        } else {
            provider
        }
    );
    println!(
        "  main:      {}",
        if llm.llm_model.is_empty() {
            "(not set)"
        } else {
            &llm.llm_model
        }
    );
    println!("  aux:       {}", settings.effective_aux_model());
    println!();

    match model_catalog().get(provider) {
        Some(entry) => {
            if let Some(main) = entry.get("main") {
                println!("Main models ({provider}):");
                print_list(main, &llm.llm_model);
            }
            if let Some(aux) = entry.get("aux") {
                println!("\nAux models ({provider}):");
                print_list(aux, settings.effective_aux_model());
            }
        }
        None if provider.is_empty() => {
            println!("No provider configured — run `lethe login <provider>` first.");
        }
        None => println!("No catalog entries for `{provider}` (you can still set any id)."),
    }
    println!();
    println!("Set:  lethe model <id>   |   lethe model --aux <id>   |   lethe model --pick");
}

fn print_list(entries: &[ModelEntry], current: &str) {
    for (i, e) in entries.iter().enumerate() {
        let mark = if e.model_id() == current {
            "   [current]"
        } else {
            ""
        };
        println!(
            "  {}) {} — {} ({}){mark}",
            i + 1,
            e.name(),
            e.model_id(),
            e.price()
        );
    }
}

fn pick_interactive(settings: &Settings, provider: &str) -> Result<()> {
    let catalog = model_catalog();
    let entry = catalog.get(provider);
    let main_entries = entry
        .and_then(|p| p.get("main"))
        .cloned()
        .unwrap_or_default();
    let aux_entries = entry
        .and_then(|p| p.get("aux"))
        .cloned()
        .unwrap_or_default();
    if main_entries.is_empty() {
        bail!(
            "No catalog for provider `{}`. Set ids directly: `lethe model <id> --aux <id>`.",
            if provider.is_empty() {
                "(none)"
            } else {
                provider
            }
        );
    }

    println!("Main model:");
    let main = normalize_model_id(provider, &pick_one(&main_entries, &settings.llm.llm_model)?);
    println!("\nAuxiliary model (cheap background calls):");
    let aux = if aux_entries.is_empty() {
        main.clone()
    } else {
        normalize_model_id(
            provider,
            &pick_one(&aux_entries, settings.effective_aux_model())?,
        )
    };

    upsert_env(
        &settings.paths.config_file,
        &[
            ("LLM_MODEL".into(), main.clone()),
            ("LLM_MODEL_AUX".into(), aux.clone()),
        ],
    )?;
    println!("\nSet main={main}, aux={aux}. Restart `lethe run` to apply.");
    Ok(())
}

fn pick_one(entries: &[ModelEntry], current: &str) -> Result<String> {
    print_list(entries, current);
    let answer = prompt_line(&format!(
        "  Choose [1-{}, a custom id, or blank to keep]: ",
        entries.len()
    ))?;
    let answer = answer.trim();
    if answer.is_empty() {
        return Ok(if current.is_empty() {
            entries[0].model_id().to_string()
        } else {
            current.to_string()
        });
    }
    if let Ok(n) = answer.parse::<usize>()
        && (1..=entries.len()).contains(&n)
    {
        return Ok(entries[n - 1].model_id().to_string());
    }
    Ok(answer.to_string())
}
