# lethe-migrate

One-shot migrator from Lethe's legacy LanceDB storage (v0.18.0 and earlier) to the SQLite-vec storage shipped in v0.19.0+.

This is a **standalone Cargo project**, intentionally not a workspace member of the parent `lethe` crate — the migrator pulls in the LanceDB and Arrow stacks that Lethe itself has dropped, and we don't want either side to inherit the other's dependency tree. The empty `[workspace]` table at the top of `Cargo.toml` keeps Cargo from walking up.

For the user-facing migration workflow see [`../MIGRATION.md`](../MIGRATION.md). The data contract (old/new schemas, row mapping, edge cases, verification) lives in the migrator source.

## Build

```bash
cd migrator
cargo build --release
```

The LanceDB dependency builds protobuf bindings, so the host needs `protoc` and protobuf headers available (same prerequisite as the pre-0.19 Lethe build).

## Run

```bash
target/release/lethe-migrate \
  --lancedb-dir  ~/.lethe/data/memory/lancedb \
  --sqlite-path  ~/.lethe/data/memory/lethe-memory.db
```

Default Lethe install layout has both of those paths already; substitute the real values if `LETHE_HOME` / `DB_PATH` / `MEMORY_DIR` were overridden.

### Flags

| Flag | Effect |
|------|--------|
| `--dry-run` | Build the destination at `<sqlite-path>.dryrun`, verify it, and leave it there. The canonical `<sqlite-path>` is never touched. |
| `--force` | Overwrite an existing `<sqlite-path>`. Without this and without `--dry-run`, the migrator exits 3 instead of clobbering. |
| `--embedding-dim N` | Skip the dim==768 guard and use `N` verbatim. Required only if Lethe was configured with a non-default embedding model. |

### Exit codes (per spec §9)

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

Each table also writes a `*_vec` virtual table holding the raw little-endian f32 embeddings.

The semantic-search cache (`semantic-cache.db`) is **not** migrated — it's purely derived and Lethe rebuilds it on the next run.

## After migration

Once `lethe-migrate` exits 0, the new `lethe-memory.db` is the source of truth. The old `lancedb/` directory is left untouched; verify with `lethe check` and a sanity recall query before deleting or archiving it manually.
