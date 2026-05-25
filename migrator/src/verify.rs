//! Post-migration sanity checks. Compares row counts and round-trips a
//! handful of sample rows through `vec_to_json` to confirm embeddings
//! survived the LanceDB → sqlite-vec hop.

use anyhow::{Result, bail};
use rusqlite::{Connection, params};
use serde_json::Value;

use crate::source::SourceCounts;

/// Number of rows to spot-check per kind. Picked by deterministic
/// stride over the row ordering so reruns inspect the same sample.
const SAMPLE_SIZE: usize = 10;

pub struct ExpectedCounts {
    pub archival: SourceCounts,
    pub messages: SourceCounts,
    pub notes: SourceCounts,
}

pub fn verify(
    conn: &Connection,
    expected: &ExpectedCounts,
    archival_sample: &[(String, String, Vec<f32>)],
    message_sample: &[(String, String, Vec<f32>)],
    note_sample: &[(String, String, Vec<f32>)],
) -> Result<()> {
    check_count(
        conn,
        "memory archival rows",
        "SELECT COUNT(*) FROM memory WHERE kind = 'archival'",
        expected.archival.user(),
    )?;
    check_count(
        conn,
        "memory note rows",
        "SELECT COUNT(*) FROM memory WHERE kind = 'note'",
        expected.notes.user(),
    )?;
    check_count(
        conn,
        "message_history rows",
        "SELECT COUNT(*) FROM message_history",
        expected.messages.user(),
    )?;

    let memory_total = expected.archival.user() + expected.notes.user();
    check_count(
        conn,
        "memory_vec rows",
        "SELECT COUNT(*) FROM memory_vec",
        memory_total,
    )?;
    check_count(
        conn,
        "message_history_vec rows",
        "SELECT COUNT(*) FROM message_history_vec",
        expected.messages.user(),
    )?;

    spot_check_archival(conn, archival_sample)?;
    spot_check_messages(conn, message_sample)?;
    spot_check_notes(conn, note_sample)?;

    Ok(())
}

fn check_count(conn: &Connection, label: &str, sql: &str, expected: usize) -> Result<()> {
    let actual: usize = conn.query_row(sql, [], |row| row.get::<_, i64>(0))? as usize;
    if actual != expected {
        bail!("{label}: expected {expected}, found {actual}");
    }
    tracing::info!("verify: {label} = {actual} (matches source)");
    Ok(())
}

fn spot_check_archival(
    conn: &Connection,
    samples: &[(String, String, Vec<f32>)],
) -> Result<()> {
    for (id, expected_text, expected_vec) in pick_samples(samples) {
        let row: (String, String) = conn.query_row(
            "SELECT id, text FROM memory WHERE id = ? AND kind = 'archival'",
            params![id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        if row.0 != *id {
            bail!("archival sample id mismatch: expected {id}, got {}", row.0);
        }
        if row.1 != *expected_text {
            bail!("archival sample {id}: text mismatch");
        }
        check_vec_prefix(conn, "memory_vec", id, expected_vec)?;
    }
    Ok(())
}

fn spot_check_messages(
    conn: &Connection,
    samples: &[(String, String, Vec<f32>)],
) -> Result<()> {
    for (id, expected_content, expected_vec) in pick_samples(samples) {
        let row: (String, String) = conn.query_row(
            "SELECT id, content FROM message_history WHERE id = ?",
            params![id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        if row.0 != *id {
            bail!("message sample id mismatch: expected {id}, got {}", row.0);
        }
        if row.1 != *expected_content {
            bail!("message sample {id}: content mismatch");
        }
        check_vec_prefix(conn, "message_history_vec", id, expected_vec)?;
    }
    Ok(())
}

fn spot_check_notes(
    conn: &Connection,
    samples: &[(String, String, Vec<f32>)],
) -> Result<()> {
    for (id, expected_text, expected_vec) in pick_samples(samples) {
        let row: (String, String) = conn.query_row(
            "SELECT id, text FROM memory WHERE id = ? AND kind = 'note'",
            params![id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        if row.0 != *id {
            bail!("note sample id mismatch: expected {id}, got {}", row.0);
        }
        if row.1 != *expected_text {
            bail!("note sample {id}: text mismatch");
        }
        check_vec_prefix(conn, "memory_vec", id, expected_vec)?;
    }
    Ok(())
}

fn check_vec_prefix(
    conn: &Connection,
    vec_table: &str,
    id: &str,
    expected: &[f32],
) -> Result<()> {
    let sql = format!("SELECT vec_to_json(embedding) FROM {vec_table} WHERE id = ?");
    let json_string: String = conn.query_row(&sql, params![id], |row| row.get(0))?;
    let parsed: Value = serde_json::from_str(&json_string)?;
    let array = parsed
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("vec_to_json did not return an array for {id}"))?;
    for (i, expected_value) in expected.iter().take(4).enumerate() {
        let actual = array
            .get(i)
            .and_then(Value::as_f64)
            .ok_or_else(|| anyhow::anyhow!("missing dim {i} in {vec_table} for {id}"))?;
        // Tolerate the tiny float→json→float round-trip drift.
        if (actual as f32 - expected_value).abs() > 1e-5 {
            bail!(
                "{vec_table} sample {id}: dim {i} expected {expected_value}, got {actual}"
            );
        }
    }
    Ok(())
}

/// Deterministic stride sampling so reruns inspect the same rows.
/// Spec just says "10 random rows"; a fixed stride is more debuggable
/// and equally diagnostic for migration QA.
fn pick_samples<T>(rows: &[T]) -> Vec<&T> {
    if rows.is_empty() {
        return Vec::new();
    }
    let n = rows.len().min(SAMPLE_SIZE);
    let stride = (rows.len() / n).max(1);
    (0..n)
        .map(|i| &rows[(i * stride).min(rows.len() - 1)])
        .collect()
}
