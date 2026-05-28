# Migrating from v0.18 (LanceDB → SQLite-vec)

v0.19 moved memory storage from LanceDB to SQLite-vec. The main `lethe` binary no longer links LanceDB at all. If you ran a pre-0.19 Lethe, use the one-shot `lethe-migrate` tool to copy your `archival_memory`, `message_history`, and `notes` into the new layout.

`install.sh` and binary release tarballs ship `lethe-migrate` alongside `lethe`, so installer users already have it at `~/.lethe/bin/lethe-migrate`. Source builders can build it explicitly:

```bash
cargo build --release --manifest-path migrator/Cargo.toml
```

It's a standalone Cargo project — the Arrow/LanceDB stack stays out of the main `lethe` build. The migrator pulls in protobuf at build time, so the host needs `protoc` and protobuf headers available.

## Workflow

No destructive step until you've verified:

```bash
# 1. Dry-run: writes to lethe-memory.db.dryrun, runs full verification.
lethe-migrate \
  --lancedb-dir  ~/.lethe/data/memory/lancedb \
  --sqlite-path  ~/.lethe/data/memory/lethe-memory.db \
  --dry-run

# 2. Inspect the dry-run file if you want, then run for real.
lethe-migrate \
  --lancedb-dir  ~/.lethe/data/memory/lancedb \
  --sqlite-path  ~/.lethe/data/memory/lethe-memory.db

# 3. Smoke-test the new storage.
lethe check
lethe memory recall -m "<something you remember>"

# 4. The old LanceDB directory is never touched. After step 3 looks good,
#    back it up, move it, or delete it — the migrator prints the full
#    path on success.
```

## Flags

| Flag | Effect |
|------|--------|
| `--dry-run` | Build the destination at `<sqlite-path>.dryrun`, verify it, and leave it there. The canonical path is never touched. |
| `--force` | Overwrite an existing `<sqlite-path>`. Without this and without `--dry-run`, the migrator exits 3 instead of clobbering. |
| `--embedding-dim N` | Skip the 768-dim guard and use `N` verbatim. Required only if Lethe was configured with a non-default embedding model. |

## Exit codes

| Code | Meaning |
|------|---------|
| `0` | Success — counts match and the sample rows round-tripped. |
| `1` | Usage / argument error. |
| `2` | Source `lancedb/` directory missing or unreadable. |
| `3` | Destination already exists and `--force` was not given. |
| `4` | Verification failed — the migrated file is preserved at `<sqlite-path>.partial` for inspection. |
| `5` | Unexpected error. |

## What gets migrated

- `archival_memory.lance` → `memory` table with `kind = 'archival'`
- `notes.lance` → `memory` table with `kind = 'note'` (tag CSV converted to JSON array)
- `message_history.lance` → `message_history` table

Each table also writes a `*_vec` virtual table holding raw little-endian f32 embeddings. The semantic-search cache (`semantic-cache.db`) is **not** migrated — it's purely derived and Lethe rebuilds it on the next run.
