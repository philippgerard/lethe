//! Read rows from the legacy LanceDB tables (`archival_memory`,
//! `message_history`, `notes`) into plain owned Rust structs the writer
//! can consume. Each table is read in a single pass; bootstrap `_init_`
//! rows are filtered out here so callers never see them.

use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use arrow_array::{
    Array, FixedSizeListArray, Float32Array, RecordBatch, StringArray,
    cast::AsArray,
};
use futures::TryStreamExt;
use lancedb::query::{ExecutableQuery, QueryBase};

const INIT_ID: &str = "_init_";
pub const ARCHIVAL_TABLE: &str = "archival_memory";
pub const MESSAGES_TABLE: &str = "message_history";
pub const NOTES_TABLE: &str = "notes";
pub const VECTOR_COLUMN: &str = "vector";

/// Raw archival row, post-init-filter.
pub struct ArchivalRow {
    pub id: String,
    pub text: String,
    pub metadata: String,
    pub tags: String,
    pub created_at: String,
    pub embedding: Vec<f32>,
}

pub struct MessageRow {
    pub id: String,
    pub role: String,
    pub content: String,
    pub metadata: String,
    pub created_at: String,
    pub embedding: Vec<f32>,
}

pub struct NoteRow {
    pub id: String,
    pub title: String,
    pub text: String,
    pub tags_csv: String,
    pub file_path: String,
    pub created_at: String,
    pub updated_at: String,
    pub embedding: Vec<f32>,
}

/// Counts of rows in the source LanceDB tables, raw and filtered.
#[derive(Clone, Copy, Default)]
pub struct SourceCounts {
    pub raw: usize,
    pub init: usize,
}

impl SourceCounts {
    pub fn user(self) -> usize {
        self.raw.saturating_sub(self.init)
    }
}

pub async fn count_rows(lancedb_dir: &Path, table: &str) -> Result<SourceCounts> {
    if !table_path(lancedb_dir, table).exists() {
        return Ok(SourceCounts::default());
    }
    let db = lancedb::connect(&lancedb_dir.display().to_string())
        .execute()
        .await
        .with_context(|| format!("opening LanceDB at {}", lancedb_dir.display()))?;
    let table = db
        .open_table(table)
        .execute()
        .await
        .with_context(|| format!("opening table {table}"))?;
    let raw = table.count_rows(None).await? as usize;
    // The `_init_` row may not exist (a fresh install that never wrote it),
    // so swallow "not found" filters as zero.
    let init = table
        .count_rows(Some(format!("id = '{INIT_ID}'")))
        .await
        .unwrap_or(0) as usize;
    Ok(SourceCounts { raw, init })
}

pub async fn read_archival(lancedb_dir: &Path) -> Result<Vec<ArchivalRow>> {
    let Some(batches) = read_table(lancedb_dir, ARCHIVAL_TABLE).await? else {
        return Ok(Vec::new());
    };
    let mut out = Vec::new();
    for batch in batches {
        let ids = string_column(&batch, "id")?;
        let texts = string_column(&batch, "text")?;
        let metadata = string_column(&batch, "metadata")?;
        let tags = string_column(&batch, "tags")?;
        let created = string_column(&batch, "created_at")?;
        let vectors = vector_column(&batch)?;
        for row in 0..batch.num_rows() {
            let id = ids.value(row);
            if id == INIT_ID {
                continue;
            }
            out.push(ArchivalRow {
                id: id.to_string(),
                text: texts.value(row).to_string(),
                metadata: metadata.value(row).to_string(),
                tags: tags.value(row).to_string(),
                created_at: created.value(row).to_string(),
                embedding: extract_vector(&vectors, row)?,
            });
        }
    }
    Ok(out)
}

pub async fn read_messages(lancedb_dir: &Path) -> Result<Vec<MessageRow>> {
    let Some(batches) = read_table(lancedb_dir, MESSAGES_TABLE).await? else {
        return Ok(Vec::new());
    };
    let mut out = Vec::new();
    for batch in batches {
        let ids = string_column(&batch, "id")?;
        let roles = string_column(&batch, "role")?;
        let contents = string_column(&batch, "content")?;
        let metadata = string_column(&batch, "metadata")?;
        let created = string_column(&batch, "created_at")?;
        let vectors = vector_column(&batch)?;
        for row in 0..batch.num_rows() {
            let id = ids.value(row);
            if id == INIT_ID {
                continue;
            }
            out.push(MessageRow {
                id: id.to_string(),
                role: roles.value(row).to_string(),
                content: contents.value(row).to_string(),
                metadata: metadata.value(row).to_string(),
                created_at: created.value(row).to_string(),
                embedding: extract_vector(&vectors, row)?,
            });
        }
    }
    Ok(out)
}

pub async fn read_notes(lancedb_dir: &Path) -> Result<Vec<NoteRow>> {
    let Some(batches) = read_table(lancedb_dir, NOTES_TABLE).await? else {
        return Ok(Vec::new());
    };
    let mut out = Vec::new();
    for batch in batches {
        let ids = string_column(&batch, "id")?;
        let titles = string_column(&batch, "title")?;
        let texts = string_column(&batch, "text")?;
        let tags = string_column(&batch, "tags")?;
        let file_paths = string_column(&batch, "file_path")?;
        let created = string_column(&batch, "created_at")?;
        let updated = string_column(&batch, "updated_at")?;
        let vectors = vector_column(&batch)?;
        for row in 0..batch.num_rows() {
            let id = ids.value(row);
            if id == INIT_ID {
                continue;
            }
            out.push(NoteRow {
                id: id.to_string(),
                title: titles.value(row).to_string(),
                text: texts.value(row).to_string(),
                tags_csv: tags.value(row).to_string(),
                file_path: file_paths.value(row).to_string(),
                created_at: created.value(row).to_string(),
                updated_at: updated.value(row).to_string(),
                embedding: extract_vector(&vectors, row)?,
            });
        }
    }
    Ok(out)
}

async fn read_table(lancedb_dir: &Path, table: &str) -> Result<Option<Vec<RecordBatch>>> {
    if !table_path(lancedb_dir, table).exists() {
        tracing::info!("source table {table} missing — treating as empty");
        return Ok(None);
    }
    let db = lancedb::connect(&lancedb_dir.display().to_string())
        .execute()
        .await
        .with_context(|| format!("opening LanceDB at {}", lancedb_dir.display()))?;
    let table_ref = db
        .open_table(table)
        .execute()
        .await
        .with_context(|| format!("opening table {table}"))?;
    let count = table_ref.count_rows(None).await? as usize;
    let stream = table_ref
        .query()
        .limit(count.max(1))
        .execute()
        .await
        .with_context(|| format!("scanning table {table}"))?;
    let batches: Vec<RecordBatch> = stream
        .try_collect()
        .await
        .with_context(|| format!("collecting batches for {table}"))?;
    Ok(Some(batches))
}

fn table_path(lancedb_dir: &Path, table: &str) -> std::path::PathBuf {
    lancedb_dir.join(format!("{table}.lance"))
}

fn string_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a StringArray> {
    batch
        .column_by_name(name)
        .ok_or_else(|| anyhow!("column {name} missing from batch"))?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| anyhow!("column {name} is not Utf8"))
}

fn vector_column(batch: &RecordBatch) -> Result<Arc<FixedSizeListArray>> {
    let column = batch
        .column_by_name(VECTOR_COLUMN)
        .ok_or_else(|| anyhow!("vector column missing from batch"))?;
    let arr = column
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .ok_or_else(|| anyhow!("vector column is not FixedSizeList"))?;
    Ok(Arc::new(arr.clone()))
}

fn extract_vector(list: &FixedSizeListArray, row: usize) -> Result<Vec<f32>> {
    let cell = list.value(row);
    let floats = cell
        .as_primitive_opt::<arrow_array::types::Float32Type>()
        .ok_or_else(|| anyhow!("vector cell is not Float32"))?;
    // Float32Array of length `dim`; copy to an owned Vec.
    Ok((0..floats.len()).map(|i| floats.value(i)).collect())
}

/// Inspect the first non-init row of any table that has data and return
/// the observed embedding dim. Returns `None` if every table is empty.
pub async fn detect_embedding_dim(lancedb_dir: &Path) -> Result<Option<usize>> {
    for table in [ARCHIVAL_TABLE, MESSAGES_TABLE, NOTES_TABLE] {
        let Some(batches) = read_table(lancedb_dir, table).await? else {
            continue;
        };
        for batch in &batches {
            let ids = string_column(batch, "id")?;
            let vectors = vector_column(batch)?;
            for row in 0..batch.num_rows() {
                if ids.value(row) == INIT_ID {
                    continue;
                }
                let cell = vectors.value(row);
                let floats = cell
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .ok_or_else(|| anyhow!("vector cell is not Float32Array"))?;
                if floats.is_empty() {
                    bail!("empty vector cell in table {table}");
                }
                return Ok(Some(floats.len()));
            }
        }
    }
    Ok(None)
}
