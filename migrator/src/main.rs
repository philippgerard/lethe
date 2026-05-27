//! `lethe-migrate` — one-shot migrator from Lethe's legacy LanceDB
//! storage to the SQLite-vec storage shipped in v0.19.0+. See
//! `../../MIGRATION-SPEC.md` for the contract this implements.
//!
//! Exit codes (per spec §9):
//!   0  success and verification passed
//!   1  usage / argument error
//!   2  source data missing or unreadable
//!   3  destination exists and `--force` not given
//!   4  verification failed (destination left in place for inspection)
//!   5  unexpected error

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use anyhow::{Context, Result, bail};
use clap::Parser;
use thiserror::Error;
use tracing_subscriber::EnvFilter;

mod dest;
mod source;
mod verify;

use dest::{Destination, ensure_note_id};
use source::{
    ARCHIVAL_TABLE, MESSAGES_TABLE, NOTES_TABLE, SourceCounts, count_rows, detect_embedding_dim,
    read_archival, read_messages, read_notes,
};
use verify::{ExpectedCounts, verify};

/// Embedding dim baked into the legacy storage. The spec §7.1 says
/// abort on mismatch; the user can override with `--embedding-dim`.
const EXPECTED_DIM: usize = 768;

#[derive(Debug, Parser)]
#[command(
    name = "lethe-migrate",
    about = "Migrate Lethe memory data from LanceDB to SQLite-vec",
    version
)]
struct Cli {
    /// Path to the legacy `<data>/memory/lancedb/` directory.
    #[arg(long)]
    lancedb_dir: PathBuf,
    /// Path to write the new `lethe-memory.db`.
    #[arg(long)]
    sqlite_path: PathBuf,
    /// Build the destination in a temp path, verify, do not replace.
    #[arg(long)]
    dry_run: bool,
    /// Overwrite an existing destination file.
    #[arg(long)]
    force: bool,
    /// Skip the dim==768 guard and use the given dim verbatim.
    #[arg(long)]
    embedding_dim: Option<usize>,
}

/// Internal error wrapper so we can map specific failure modes to the
/// exit codes documented in the spec.
#[derive(Debug, Error)]
enum MigrateError {
    #[error("source data missing or unreadable: {0}")]
    SourceMissing(String),
    #[error("destination already exists at {0} — pass --force to overwrite")]
    DestinationExists(PathBuf),
    #[error("verification failed: {0} (destination preserved at {1})")]
    VerificationFailed(String, PathBuf),
}

impl MigrateError {
    fn exit_code(&self) -> u8 {
        match self {
            Self::SourceMissing(_) => 2,
            Self::DestinationExists(_) => 3,
            Self::VerificationFailed(_, _) => 4,
        }
    }
}

fn main() -> ExitCode {
    init_logging();

    // Clap handles --help and arg errors itself with exit code 2 by
    // default; remap to spec's "1" for usage errors.
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(err) => {
            err.print().ok();
            let code = match err.kind() {
                clap::error::ErrorKind::DisplayHelp | clap::error::ErrorKind::DisplayVersion => 0,
                _ => 1,
            };
            return ExitCode::from(code);
        }
    };

    let result = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("building tokio runtime")
        .block_on(run(cli));

    match result {
        Ok(()) => ExitCode::from(0),
        Err(err) => {
            let code = err
                .downcast_ref::<MigrateError>()
                .map(MigrateError::exit_code)
                .unwrap_or(5);
            eprintln!("error: {err:#}");
            ExitCode::from(code)
        }
    }
}

fn init_logging() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .with_ansi(false)
        .with_writer(std::io::stderr)
        .try_init();
}

async fn run(cli: Cli) -> Result<()> {
    if !cli.lancedb_dir.exists() {
        return Err(MigrateError::SourceMissing(format!(
            "{}",
            cli.lancedb_dir.display()
        ))
        .into());
    }

    // Refuse to clobber an existing destination unless --force (live)
    // or --dry-run (we won't touch the canonical path anyway).
    if cli.sqlite_path.exists() && !cli.force && !cli.dry_run {
        return Err(MigrateError::DestinationExists(cli.sqlite_path.clone()).into());
    }

    // Detect embedding dim before we open the destination, since the
    // dim is baked into the vec0 schema at create time.
    let detected = detect_embedding_dim(&cli.lancedb_dir).await?;
    let dim = resolve_dim(detected, cli.embedding_dim)?;
    tracing::info!("using embedding dim = {dim}");

    // Read source counts up front so verification has a target number.
    let source_counts = ExpectedCounts {
        archival: count_rows(&cli.lancedb_dir, ARCHIVAL_TABLE).await?,
        messages: count_rows(&cli.lancedb_dir, MESSAGES_TABLE).await?,
        notes: count_rows(&cli.lancedb_dir, NOTES_TABLE).await?,
    };
    log_source_counts(&source_counts);

    // Always write into a .partial sibling first so a failed run never
    // leaves a half-written file at the canonical path.
    let work_path = working_path(&cli.sqlite_path, cli.dry_run);
    if work_path.exists() {
        std::fs::remove_file(&work_path)
            .with_context(|| format!("clearing stale {}", work_path.display()))?;
    }
    tracing::info!("writing destination to {}", work_path.display());

    let archival_rows = read_archival(&cli.lancedb_dir).await?;
    let message_rows = read_messages(&cli.lancedb_dir).await?;
    let note_rows = read_notes(&cli.lancedb_dir).await?;

    enforce_note_path_uniqueness(&note_rows)?;

    let archival_sample = sample_archival(&archival_rows);
    let message_sample = sample_messages(&message_rows);
    let note_sample = sample_notes(&note_rows);

    // Single transaction across the whole migration; dropping the
    // Destination without commit() forces a ROLLBACK.
    let dest = Destination::create(&work_path, dim)?;
    for row in &archival_rows {
        dest.insert_archival(row)?;
    }
    for row in &message_rows {
        dest.insert_message(row)?;
    }
    for row in &note_rows {
        dest.insert_note(row)?;
    }
    let conn = dest.commit().context("committing migration transaction")?;
    tracing::info!(
        "inserted archival={}, messages={}, notes={}",
        archival_rows.len(),
        message_rows.len(),
        note_rows.len()
    );

    if let Err(error) = verify(
        &conn,
        &source_counts,
        &archival_sample,
        &message_sample,
        &note_sample,
    ) {
        // Drop conn so SQLite releases the file before we hand off to
        // the user; the .partial stays in place for inspection.
        drop(conn);
        return Err(MigrateError::VerificationFailed(
            format!("{error:#}"),
            work_path,
        )
        .into());
    }
    drop(conn);
    tracing::info!("verification passed");

    if cli.dry_run {
        tracing::info!(
            "dry-run: leaving migrated file at {} (no replacement)",
            work_path.display()
        );
        return Ok(());
    }

    if cli.sqlite_path.exists() {
        std::fs::remove_file(&cli.sqlite_path).with_context(|| {
            format!("removing prior destination {}", cli.sqlite_path.display())
        })?;
    }
    std::fs::rename(&work_path, &cli.sqlite_path).with_context(|| {
        format!(
            "promoting {} to {}",
            work_path.display(),
            cli.sqlite_path.display()
        )
    })?;
    tracing::info!("migration complete → {}", cli.sqlite_path.display());

    let old_dir = cli
        .lancedb_dir
        .canonicalize()
        .unwrap_or_else(|_| cli.lancedb_dir.clone());
    println!();
    println!("The legacy LanceDB directory is no longer used:");
    println!("    {}", old_dir.display());
    println!("It can now be backed up, moved, or deleted.");
    Ok(())
}

fn resolve_dim(detected: Option<usize>, override_dim: Option<usize>) -> Result<usize> {
    match (detected, override_dim) {
        (_, Some(dim)) => Ok(dim),
        (Some(dim), None) if dim == EXPECTED_DIM => Ok(dim),
        (Some(dim), None) => bail!(
            "detected embedding dim {dim} != expected {EXPECTED_DIM}. \
             Rerun with --embedding-dim {dim} if this is intentional \
             (a non-default embedding model was in use)."
        ),
        (None, None) => Ok(EXPECTED_DIM),
    }
}

fn log_source_counts(counts: &ExpectedCounts) {
    log_one("archival_memory", counts.archival);
    log_one("message_history", counts.messages);
    log_one("notes",           counts.notes);
}

fn log_one(label: &str, counts: SourceCounts) {
    tracing::info!(
        "source {label}: {} user row(s) ({} raw, {} init filtered)",
        counts.user(),
        counts.raw,
        counts.init
    );
}

fn working_path(dest: &Path, dry_run: bool) -> PathBuf {
    let suffix = if dry_run { ".dryrun" } else { ".partial" };
    let mut name = dest.file_name().unwrap_or_default().to_os_string();
    name.push(suffix);
    dest.with_file_name(name)
}

fn enforce_note_path_uniqueness(rows: &[source::NoteRow]) -> Result<()> {
    let mut seen: std::collections::HashSet<&str> = std::collections::HashSet::new();
    for row in rows {
        if !seen.insert(row.file_path.as_str()) {
            bail!(
                "two notes share file_path {} (id {}) — refusing to migrate corrupted source",
                row.file_path,
                row.id
            );
        }
    }
    Ok(())
}

fn sample_archival(rows: &[source::ArchivalRow]) -> Vec<(String, String, Vec<f32>)> {
    rows.iter()
        .map(|r| (r.id.clone(), r.text.clone(), r.embedding.clone()))
        .collect()
}

fn sample_messages(rows: &[source::MessageRow]) -> Vec<(String, String, Vec<f32>)> {
    rows.iter()
        .map(|r| (r.id.clone(), r.content.clone(), r.embedding.clone()))
        .collect()
}

fn sample_notes(rows: &[source::NoteRow]) -> Vec<(String, String, Vec<f32>)> {
    // Note ids gain a `note-` prefix on insert; verify looks the row up
    // by stored id, so the sample must use the prefixed form too.
    rows.iter()
        .map(|r| (ensure_note_id(&r.id), r.text.clone(), r.embedding.clone()))
        .collect()
}
