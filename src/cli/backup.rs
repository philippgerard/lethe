//! `lethe backup` and `lethe restore`: pack the workspace, agent state
//! (context + history), and the `.env` file into a single `.tar.gz`
//! archive, and unpack one back into place.
//!
//! Backup creation still uses the platform tar, while restore extraction is
//! handled in-process so every archive entry can be validated before any
//! restored data is promoted into place.

use std::collections::HashSet;
use std::ffi::CString;
use std::fs::{self, OpenOptions};
use std::io::{self, BufRead, IsTerminal, Write};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt};
use std::path::{Component, Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result, bail};
use chrono::Local;
use flate2::read::GzDecoder;
use serde_json::json;
use tar::{Archive, EntryType};
use uuid::Uuid;

use lethe::config::Settings;

/// `lethe backup`. Returns Ok after writing `output` (or a timestamped
/// default in the current directory) as a 0600 tar.gz containing
/// workspace, data (memory + history), and `.env`.
pub fn backup(output: Option<String>) -> Result<()> {
    let settings = Settings::from_env();
    let output_path = resolve_output_path(output);

    let staging = scratch_dir("lethe-backup-");
    create_private_staging(&staging)?;

    let result = run_backup(&settings, &staging, &output_path);
    let _ = fs::remove_dir_all(&staging);
    result?;

    if let Err(error) = fs::set_permissions(&output_path, fs::Permissions::from_mode(0o600)) {
        eprintln!(
            "warning: could not chmod 0600 {}: {error}",
            output_path.display()
        );
    }

    println!("Wrote backup to {}", output_path.display());
    println!("Note: archive may contain secrets from .env — keep it private.");
    Ok(())
}

/// `lethe restore <archive> [--yes]`. Asks before overwriting an
/// existing workspace and before overwriting an existing `.env`;
/// memory + history are restored unconditionally (that's the point).
pub fn restore(archive: String, yes: bool) -> Result<()> {
    let settings = Settings::from_env();
    let archive_path = PathBuf::from(&archive);
    if !archive_path.exists() {
        bail!("archive not found: {}", archive_path.display());
    }

    let staging = scratch_dir("lethe-restore-");
    create_private_staging(&staging)?;

    let result = run_restore(&settings, &archive_path, &staging, yes);
    let _ = fs::remove_dir_all(&staging);
    result
}

fn run_backup(settings: &Settings, staging: &Path, output: &Path) -> Result<()> {
    let mut components: Vec<&str> = Vec::new();

    if dir_exists(&settings.paths.workspace_dir) {
        copy_dir(&settings.paths.workspace_dir, &staging.join("workspace"))?;
        components.push("workspace");
    }

    let data_dst = staging.join("data");
    let mut wrote_data = false;
    if dir_exists(&settings.paths.memory_dir) {
        fs::create_dir_all(&data_dst)?;
        copy_dir(&settings.paths.memory_dir, &data_dst.join("memory"))?;
        wrote_data = true;
    }
    if settings.paths.db_path.exists() {
        fs::create_dir_all(&data_dst)?;
        copy_regular_file(
            &settings.paths.db_path,
            &data_dst.join("lethe.db"),
            "backup database",
        )?;
        wrote_data = true;
    }
    if wrote_data {
        components.push("data");
    }

    let env_src = settings.paths.lethe_home.join("config").join(".env");
    if env_src.exists() {
        let dst_dir = staging.join("config");
        fs::create_dir_all(&dst_dir)?;
        copy_regular_file(&env_src, &dst_dir.join(".env"), "backup environment")?;
        components.push("env");
    }

    if components.is_empty() {
        bail!(
            "nothing to back up: workspace, data, and .env are all empty/missing under {}",
            settings.paths.lethe_home.display()
        );
    }

    let manifest = json!({
        "version": 1,
        "lethe_version": env!("CARGO_PKG_VERSION"),
        "created_at": Local::now().to_rfc3339(),
        "components": components,
    });
    fs::write(
        staging.join("manifest.json"),
        serde_json::to_string_pretty(&manifest)?,
    )?;

    if let Some(parent) = output.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }
    tar_create(output, staging)
}

fn run_restore(settings: &Settings, archive: &Path, staging: &Path, yes: bool) -> Result<()> {
    tar_extract(archive, staging)?;

    let manifest_path = staging.join("manifest.json");
    let manifest_text = fs::read_to_string(&manifest_path)
        .with_context(|| format!("reading restore manifest {}", manifest_path.display()))?;
    let manifest: serde_json::Value = serde_json::from_str(&manifest_text)
        .with_context(|| format!("parsing restore manifest {}", manifest_path.display()))?;
    if manifest.get("version").and_then(|value| value.as_u64()) != Some(1) {
        bail!("unsupported or missing restore manifest version");
    }
    if let Some(created) = manifest.get("created_at").and_then(|value| value.as_str()) {
        println!("Archive created at {created}");
    }

    let src_ws = staging.join("workspace");
    if src_ws.exists() {
        let dst = settings.paths.workspace_dir.clone();
        let proceed = if dir_has_content(&dst) && !yes {
            confirm(&format!(
                "Workspace {} already exists. Overwrite? [y/N]: ",
                dst.display()
            ))?
        } else {
            true
        };
        if proceed {
            replace_dir_atomically(&src_ws, &dst)?;
            println!("Restored workspace → {}", dst.display());
        } else {
            println!("Skipped workspace.");
        }
    }

    let src_data = staging.join("data");
    if src_data.exists() {
        let src_mem = src_data.join("memory");
        if src_mem.exists() {
            replace_dir_atomically(&src_mem, &settings.paths.memory_dir)?;
            println!("Restored memory → {}", settings.paths.memory_dir.display());
        }
        let src_db = src_data.join("lethe.db");
        if src_db.exists() {
            replace_file_atomically(&src_db, &settings.paths.db_path, 0o600)?;
            println!("Restored db → {}", settings.paths.db_path.display());
        }
    }

    let src_env = staging.join("config").join(".env");
    if src_env.exists() {
        let dst = settings.paths.lethe_home.join("config").join(".env");
        let proceed = if dst.exists() && !yes {
            confirm(&format!(
                ".env already exists at {}. Overwrite? [y/N]: ",
                dst.display()
            ))?
        } else {
            true
        };
        if proceed {
            replace_file_atomically(&src_env, &dst, 0o600)?;
            println!("Restored .env → {}", dst.display());
        } else {
            println!("Skipped .env.");
        }
    }

    Ok(())
}

fn copy_dir(src: &Path, dst: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(src)
        .with_context(|| format!("inspecting backup directory {}", src.display()))?;
    if metadata.file_type().is_symlink() {
        bail!("backup source contains a symlink: {}", src.display());
    }
    if !metadata.is_dir() {
        bail!("backup source is not a directory: {}", src.display());
    }
    if let Some(parent) = dst.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::create_dir(dst).with_context(|| format!("creating backup directory {}", dst.display()))?;

    for entry in
        fs::read_dir(src).with_context(|| format!("reading backup directory {}", src.display()))?
    {
        let entry = entry.with_context(|| format!("reading backup directory {}", src.display()))?;
        let source = entry.path();
        let destination = dst.join(entry.file_name());
        let file_type = entry
            .file_type()
            .with_context(|| format!("inspecting backup source {}", source.display()))?;
        if file_type.is_symlink() {
            bail!("backup source contains a symlink: {}", source.display());
        }
        if file_type.is_dir() {
            copy_dir(&source, &destination)?;
        } else if file_type.is_file() {
            copy_regular_file(&source, &destination, "backup source")?;
        } else {
            bail!(
                "backup source contains a non-regular entry: {}",
                source.display()
            );
        }
    }
    Ok(())
}

fn copy_regular_file(src: &Path, dst: &Path, label: &str) -> Result<()> {
    let metadata = fs::symlink_metadata(src)
        .with_context(|| format!("inspecting {label} {}", src.display()))?;
    if metadata.file_type().is_symlink() {
        bail!("{label} is a symlink: {}", src.display());
    }
    if !metadata.is_file() {
        bail!("{label} is not a regular file: {}", src.display());
    }

    let mut input = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(src)
        .with_context(|| format!("opening {label} {} without following links", src.display()))?;
    if !input
        .metadata()
        .with_context(|| format!("inspecting opened {label} {}", src.display()))?
        .is_file()
    {
        bail!("{label} is not a regular file: {}", src.display());
    }

    let mode = (metadata.permissions().mode() & 0o100) | 0o600;
    let mut output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(mode)
        .open(dst)
        .with_context(|| format!("creating backup file {}", dst.display()))?;
    io::copy(&mut input, &mut output)
        .with_context(|| format!("copying {label} {}", src.display()))?;
    fs::set_permissions(dst, fs::Permissions::from_mode(mode))?;
    Ok(())
}

fn tar_create(output: &Path, staging: &Path) -> Result<()> {
    let status = Command::new("tar")
        // BSD tar otherwise emits AppleDouble `._*` entries for macOS
        // metadata. Restore intentionally rejects undeclared archive members,
        // so backups must suppress those sidecars at creation time.
        .env("COPYFILE_DISABLE", "1")
        .arg("-czf")
        .arg(output)
        .arg("-C")
        .arg(staging)
        .arg(".")
        .status()
        .with_context(|| "running tar — is it installed?")?;
    if !status.success() {
        bail!("tar create failed ({status})");
    }
    Ok(())
}

fn tar_extract(archive: &Path, dst: &Path) -> Result<()> {
    let input = fs::File::open(archive)
        .with_context(|| format!("opening restore archive {}", archive.display()))?;
    let mut archive = Archive::new(GzDecoder::new(input));
    archive.set_unpack_xattrs(false);
    archive.set_preserve_permissions(false);
    archive.set_preserve_ownerships(false);
    archive.set_preserve_mtime(false);
    archive.set_overwrite(false);

    let mut seen = HashSet::new();
    for entry in archive
        .entries()
        .with_context(|| "reading restore archive")?
    {
        let mut entry = entry.with_context(|| "reading restore archive entry")?;
        let path = entry
            .path()
            .with_context(|| "reading restore archive entry path")?
            .into_owned();
        let normalized = validate_restore_entry(&path, entry.header().entry_type())?;
        if !seen.insert(normalized) {
            bail!(
                "restore archive contains duplicate entry {}",
                path.display()
            );
        }
        let unpacked = entry
            .unpack_in(dst)
            .with_context(|| format!("extracting restore entry {}", path.display()))?;
        if !unpacked {
            bail!(
                "restore entry escaped the staging directory: {}",
                path.display()
            );
        }
    }
    validate_restore_schema(&seen)?;
    Ok(())
}

fn validate_restore_entry(path: &Path, entry_type: EntryType) -> Result<PathBuf> {
    if !entry_type.is_file() && !entry_type.is_dir() {
        bail!(
            "restore archive entry {} has forbidden type {:?}",
            path.display(),
            entry_type
        );
    }

    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::Normal(part) => normalized.push(part),
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                bail!("restore archive entry has unsafe path: {}", path.display());
            }
        }
    }

    if normalized.as_os_str().is_empty() {
        if entry_type.is_dir() {
            return Ok(normalized);
        }
        bail!("restore archive contains a file with an empty path");
    }

    if normalized == Path::new("manifest.json") {
        if !entry_type.is_file() {
            bail!("manifest.json must be one top-level regular file");
        }
    } else if normalized == Path::new("workspace") {
        if !entry_type.is_dir() {
            bail!("restore archive workspace entry must be a directory");
        }
    } else if normalized.starts_with("workspace") {
        // The workspace is the one intentionally open tree in a backup.
    } else if normalized == Path::new("data") {
        if !entry_type.is_dir() {
            bail!("restore archive data entry must be a directory");
        }
    } else if normalized == Path::new("data/memory") {
        if !entry_type.is_dir() {
            bail!("restore archive data/memory entry must be a directory");
        }
    } else if normalized.starts_with("data/memory") {
        // Memory is the only open tree below data.
    } else if normalized == Path::new("data/lethe.db") {
        if !entry_type.is_file() {
            bail!("restore archive data/lethe.db entry must be a regular file");
        }
    } else if normalized == Path::new("config") {
        if !entry_type.is_dir() {
            bail!("restore archive config entry must be a directory");
        }
    } else if normalized == Path::new("config/.env") {
        if !entry_type.is_file() {
            bail!("restore archive config/.env entry must be a regular file");
        }
    } else {
        bail!(
            "restore archive contains unexpected entry {}",
            path.display()
        );
    }

    Ok(normalized)
}

fn validate_restore_schema(seen: &HashSet<PathBuf>) -> Result<()> {
    let has = |path: &str| seen.contains(Path::new(path));
    if !has("manifest.json") {
        bail!("restore archive is missing regular manifest.json");
    }

    let workspace_children = seen
        .iter()
        .any(|path| path.starts_with("workspace") && path != Path::new("workspace"));
    if workspace_children && !has("workspace") {
        bail!("restore archive workspace tree is missing its directory entry");
    }

    let memory_children = seen
        .iter()
        .any(|path| path.starts_with("data/memory") && path != Path::new("data/memory"));
    if memory_children && !has("data/memory") {
        bail!("restore archive memory tree is missing its directory entry");
    }
    if (has("data/memory") || has("data/lethe.db")) && !has("data") {
        bail!("restore archive data content is missing its directory entry");
    }
    if has("config/.env") && !has("config") {
        bail!("restore archive config/.env is missing its directory entry");
    }
    Ok(())
}

fn create_private_staging(path: &Path) -> Result<()> {
    let mut builder = fs::DirBuilder::new();
    builder.mode(0o700);
    builder
        .create(path)
        .with_context(|| format!("creating private staging dir {}", path.display()))
}

fn ensure_private_parent(dst: &Path) -> Result<&Path> {
    let parent = dst
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .ok_or_else(|| anyhow::anyhow!("restore destination has no parent: {}", dst.display()))?;
    if !parent.exists() {
        let mut builder = fs::DirBuilder::new();
        builder.recursive(true).mode(0o700);
        builder
            .create(parent)
            .with_context(|| format!("creating restore destination parent {}", parent.display()))?;
    }
    let metadata = fs::symlink_metadata(parent)
        .with_context(|| format!("inspecting restore destination parent {}", parent.display()))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        bail!(
            "restore destination parent is not a real directory: {}",
            parent.display()
        );
    }
    Ok(parent)
}

fn sibling_staging_path(dst: &Path, kind: &str) -> Result<PathBuf> {
    let name = dst.file_name().ok_or_else(|| {
        anyhow::anyhow!("restore destination has no file name: {}", dst.display())
    })?;
    let mut staged_name = std::ffi::OsString::from(".");
    staged_name.push(name);
    staged_name.push(format!(".{kind}.{}", Uuid::new_v4()));
    Ok(dst.with_file_name(staged_name))
}

fn replace_file_atomically(src: &Path, dst: &Path, mode: u32) -> Result<()> {
    let source = fs::symlink_metadata(src)
        .with_context(|| format!("inspecting restore source {}", src.display()))?;
    if !source.is_file() || source.file_type().is_symlink() {
        bail!("restore source is not a regular file: {}", src.display());
    }
    ensure_private_parent(dst)?;
    if let Ok(existing) = fs::symlink_metadata(dst) {
        if existing.is_dir() && !existing.file_type().is_symlink() {
            bail!("restore file destination is a directory: {}", dst.display());
        }
    }

    let staged = sibling_staging_path(dst, "restore-file")?;
    let result = (|| -> Result<()> {
        let mut input = fs::File::open(src)
            .with_context(|| format!("opening restore source {}", src.display()))?;
        let mut output = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(mode)
            .open(&staged)
            .with_context(|| format!("creating restore staging file {}", staged.display()))?;
        io::copy(&mut input, &mut output)
            .with_context(|| format!("staging restored file {}", dst.display()))?;
        output
            .sync_all()
            .with_context(|| format!("syncing restore staging file {}", staged.display()))?;
        fs::set_permissions(&staged, fs::Permissions::from_mode(mode))
            .with_context(|| format!("securing restore staging file {}", staged.display()))?;
        fs::rename(&staged, dst)
            .with_context(|| format!("atomically promoting restored file {}", dst.display()))?;
        Ok(())
    })();
    if result.is_err() {
        let _ = remove_path_nofollow(&staged);
    }
    result
}

fn replace_dir_atomically(src: &Path, dst: &Path) -> Result<()> {
    let source = fs::symlink_metadata(src)
        .with_context(|| format!("inspecting restore source {}", src.display()))?;
    if !source.is_dir() || source.file_type().is_symlink() {
        bail!("restore source is not a real directory: {}", src.display());
    }
    ensure_private_parent(dst)?;
    let existing = match fs::symlink_metadata(dst) {
        Ok(metadata) => Some(metadata),
        Err(error) if error.kind() == io::ErrorKind::NotFound => None,
        Err(error) => return Err(error).with_context(|| format!("inspecting {}", dst.display())),
    };
    if existing
        .as_ref()
        .is_some_and(|metadata| metadata.file_type().is_symlink() || !metadata.is_dir())
    {
        bail!(
            "restore directory destination is not a real directory: {}",
            dst.display()
        );
    }

    let staged = sibling_staging_path(dst, "restore-dir")?;
    create_private_staging(&staged)?;
    if let Err(error) = copy_tree_contents(src, &staged) {
        let _ = remove_path_nofollow(&staged);
        return Err(error);
    }

    let promoted = if existing.is_some() {
        atomic_exchange(&staged, dst)
            .with_context(|| format!("atomically exchanging restored directory {}", dst.display()))
    } else {
        fs::rename(&staged, dst)
            .with_context(|| format!("atomically promoting restored directory {}", dst.display()))
    };
    if let Err(error) = promoted {
        let _ = remove_path_nofollow(&staged);
        return Err(error);
    }
    if existing.is_some() {
        if let Err(error) = remove_path_nofollow(&staged) {
            eprintln!(
                "warning: restored {} but could not remove the replaced tree at {}: {error}",
                dst.display(),
                staged.display()
            );
        }
    }
    Ok(())
}

fn copy_tree_contents(src: &Path, dst: &Path) -> Result<()> {
    for entry in fs::read_dir(src).with_context(|| format!("reading {}", src.display()))? {
        let entry = entry.with_context(|| format!("reading {}", src.display()))?;
        let source = entry.path();
        let destination = dst.join(entry.file_name());
        let metadata = fs::symlink_metadata(&source)
            .with_context(|| format!("inspecting restore tree entry {}", source.display()))?;
        if metadata.file_type().is_symlink() {
            bail!("restore tree contains a symlink: {}", source.display());
        }
        if metadata.is_dir() {
            create_private_staging(&destination)?;
            copy_tree_contents(&source, &destination)?;
        } else if metadata.is_file() {
            let mode = (metadata.permissions().mode() & 0o100) | 0o600;
            let mut input = fs::File::open(&source)?;
            let mut output = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(mode)
                .open(&destination)?;
            io::copy(&mut input, &mut output)?;
            fs::set_permissions(&destination, fs::Permissions::from_mode(mode))?;
        } else {
            bail!(
                "restore tree contains a non-regular entry: {}",
                source.display()
            );
        }
    }
    Ok(())
}

fn remove_path_nofollow(path: &Path) -> io::Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {
            fs::remove_dir_all(path)
        }
        Ok(_) => fs::remove_file(path),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

fn atomic_exchange(left: &Path, right: &Path) -> io::Result<()> {
    let left = CString::new(left.as_os_str().as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path contains NUL"))?;
    let right = CString::new(right.as_os_str().as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path contains NUL"))?;

    #[cfg(target_os = "linux")]
    // SAFETY: both C strings are NUL-terminated and remain alive for the call.
    let result = unsafe {
        libc::renameat2(
            libc::AT_FDCWD,
            left.as_ptr(),
            libc::AT_FDCWD,
            right.as_ptr(),
            libc::RENAME_EXCHANGE,
        )
    };
    #[cfg(target_os = "macos")]
    // SAFETY: both C strings are NUL-terminated and remain alive for the call.
    let result = unsafe {
        libc::renameatx_np(
            libc::AT_FDCWD,
            left.as_ptr(),
            libc::AT_FDCWD,
            right.as_ptr(),
            libc::RENAME_SWAP,
        )
    };
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    return Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "atomic directory exchange is only supported on Linux and macOS",
    ));

    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

fn scratch_dir(prefix: &str) -> PathBuf {
    std::env::temp_dir().join(format!("{prefix}{}", Uuid::new_v4()))
}

fn dir_exists(path: &Path) -> bool {
    path.is_dir()
}

fn dir_has_content(path: &Path) -> bool {
    fs::read_dir(path)
        .map(|mut entries| entries.next().is_some())
        .unwrap_or(false)
}

fn confirm(prompt: &str) -> Result<bool> {
    if !io::stdin().is_terminal() {
        bail!(
            "cannot prompt for confirmation: stdin is not a TTY. \
             Pass --yes to overwrite without prompting."
        );
    }
    print!("{prompt}");
    io::stdout().flush().ok();
    let mut line = String::new();
    io::stdin()
        .lock()
        .read_line(&mut line)
        .with_context(|| "reading stdin")?;
    let answer = line.trim().to_ascii_lowercase();
    Ok(answer == "y" || answer == "yes")
}

fn resolve_output_path(output: Option<String>) -> PathBuf {
    output.map(PathBuf::from).unwrap_or_else(|| {
        let ts = Local::now().format("%Y%m%d-%H%M%S");
        PathBuf::from(format!("lethe-backup-{ts}.tar.gz"))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::Compression;
    use flate2::write::GzEncoder;
    use tar::{Builder, Header};

    type TestBuilder = Builder<GzEncoder<fs::File>>;

    fn archive_builder(path: &Path) -> TestBuilder {
        let output = fs::File::create(path).unwrap();
        Builder::new(GzEncoder::new(output, Compression::default()))
    }

    fn append_file(builder: &mut TestBuilder, path: &str, contents: &[u8]) {
        let mut header = Header::new_gnu();
        header.set_entry_type(EntryType::Regular);
        header.set_mode(0o600);
        header.set_size(contents.len() as u64);
        header.set_cksum();
        builder.append_data(&mut header, path, contents).unwrap();
    }

    fn append_dir(builder: &mut TestBuilder, path: &str) {
        let mut header = Header::new_gnu();
        header.set_entry_type(EntryType::Directory);
        header.set_mode(0o700);
        header.set_size(0);
        header.set_cksum();
        builder
            .append_data(&mut header, path, std::io::empty())
            .unwrap();
    }

    fn finish_archive(builder: TestBuilder) {
        builder.into_inner().unwrap().finish().unwrap();
    }

    #[test]
    fn backup_rejects_symlinks_that_restore_would_refuse() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().unwrap();
        let settings = lethe::config::test_settings(&temp.path().join("live"));
        fs::create_dir_all(&settings.paths.workspace_dir).unwrap();
        let outside = temp.path().join("outside");
        fs::write(&outside, b"must not enter backup").unwrap();
        symlink(&outside, settings.paths.workspace_dir.join("link")).unwrap();

        let staging = temp.path().join("staging");
        create_private_staging(&staging).unwrap();
        let output = temp.path().join("backup.tar.gz");
        let error = run_backup(&settings, &staging, &output).unwrap_err();

        assert!(error.to_string().contains("symlink"));
        assert!(!output.exists());
    }

    #[test]
    fn backup_regular_tree_passes_restore_validation() {
        let temp = tempfile::tempdir().unwrap();
        let settings = lethe::config::test_settings(&temp.path().join("live"));
        let nested = settings.paths.workspace_dir.join("nested");
        fs::create_dir_all(&nested).unwrap();
        fs::write(nested.join("note.txt"), b"round trip").unwrap();

        let backup_staging = temp.path().join("backup-staging");
        create_private_staging(&backup_staging).unwrap();
        let output = temp.path().join("backup.tar.gz");
        run_backup(&settings, &backup_staging, &output).unwrap();

        let restore_staging = temp.path().join("restore-staging");
        create_private_staging(&restore_staging).unwrap();
        tar_extract(&output, &restore_staging).unwrap();
        assert_eq!(
            fs::read(restore_staging.join("workspace/nested/note.txt")).unwrap(),
            b"round trip"
        );
    }

    #[test]
    fn restore_extracts_expected_regular_files() {
        let temp = tempfile::tempdir().unwrap();
        let archive = temp.path().join("backup.tar.gz");
        let staging = temp.path().join("staging");
        create_private_staging(&staging).unwrap();

        let mut builder = archive_builder(&archive);
        append_file(&mut builder, "manifest.json", br#"{"version":1}"#);
        append_dir(&mut builder, "workspace");
        append_file(&mut builder, "workspace/note.txt", b"hello");
        append_dir(&mut builder, "data");
        append_dir(&mut builder, "data/memory");
        append_file(&mut builder, "data/memory/fact.txt", b"remember");
        append_dir(&mut builder, "config");
        append_file(&mut builder, "config/.env", b"SECRET=test");
        finish_archive(builder);

        tar_extract(&archive, &staging).unwrap();

        assert_eq!(
            fs::read(staging.join("workspace/note.txt")).unwrap(),
            b"hello"
        );
        assert_eq!(
            fs::read(staging.join("data/memory/fact.txt")).unwrap(),
            b"remember"
        );
        assert_eq!(
            fs::read(staging.join("config/.env")).unwrap(),
            b"SECRET=test"
        );
        assert_eq!(
            fs::metadata(&staging).unwrap().permissions().mode() & 0o777,
            0o700
        );
    }

    fn write_link_archive(path: &Path, entry_type: EntryType) {
        let mut builder = archive_builder(path);
        append_file(&mut builder, "manifest.json", br#"{"version":1}"#);
        append_dir(&mut builder, "workspace");

        let mut header = Header::new_gnu();
        header.set_entry_type(entry_type);
        header.set_mode(0o777);
        header.set_size(0);
        header.set_link_name("../../outside").unwrap();
        header.set_cksum();
        builder
            .append_data(&mut header, "workspace/link", std::io::empty())
            .unwrap();
        finish_archive(builder);
    }

    #[test]
    fn restore_rejects_symlinks_and_hardlinks_before_promotion() {
        let temp = tempfile::tempdir().unwrap();

        for (name, entry_type) in [
            ("symlink", EntryType::Symlink),
            ("hardlink", EntryType::Link),
        ] {
            let archive = temp.path().join(format!("{name}.tar.gz"));
            let staging = temp.path().join(format!("{name}-staging"));
            create_private_staging(&staging).unwrap();
            write_link_archive(&archive, entry_type);

            let error = tar_extract(&archive, &staging).unwrap_err();
            assert!(error.to_string().contains("forbidden type"));
            assert!(!staging.join("workspace/link").exists());
        }
    }

    #[test]
    fn restore_rejects_unsafe_paths_types_and_top_level_entries() {
        for unsafe_path in [Path::new("../outside"), Path::new("/tmp/outside")] {
            assert!(validate_restore_entry(unsafe_path, EntryType::Regular).is_err());
        }
        assert!(validate_restore_entry(Path::new("unexpected/file"), EntryType::Regular).is_err());
        assert!(validate_restore_entry(Path::new("workspace/fifo"), EntryType::Fifo).is_err());
        assert!(
            validate_restore_entry(Path::new("manifest.json/child"), EntryType::Regular).is_err()
        );
        assert!(validate_restore_entry(Path::new("workspace"), EntryType::Regular).is_err());
        assert!(validate_restore_entry(Path::new("data/memory"), EntryType::Regular).is_err());
        assert!(validate_restore_entry(Path::new("data/lethe.db"), EntryType::Directory).is_err());
        assert!(validate_restore_entry(Path::new("data/other"), EntryType::Regular).is_err());
        assert!(validate_restore_entry(Path::new("config/.env"), EntryType::Directory).is_err());
        assert!(validate_restore_entry(Path::new("config/other"), EntryType::Regular).is_err());
    }

    #[test]
    fn restore_schema_requires_manifest_and_explicit_tree_parents() {
        let missing_manifest = HashSet::from([PathBuf::from("workspace")]);
        assert!(validate_restore_schema(&missing_manifest).is_err());

        let implicit_workspace = HashSet::from([
            PathBuf::from("manifest.json"),
            PathBuf::from("workspace/file"),
        ]);
        assert!(validate_restore_schema(&implicit_workspace).is_err());

        let implicit_memory = HashSet::from([
            PathBuf::from("manifest.json"),
            PathBuf::from("data"),
            PathBuf::from("data/memory/file"),
        ]);
        assert!(validate_restore_schema(&implicit_memory).is_err());

        let implicit_data = HashSet::from([
            PathBuf::from("manifest.json"),
            PathBuf::from("data/lethe.db"),
        ]);
        assert!(validate_restore_schema(&implicit_data).is_err());

        let implicit_config =
            HashSet::from([PathBuf::from("manifest.json"), PathBuf::from("config/.env")]);
        assert!(validate_restore_schema(&implicit_config).is_err());
    }

    #[test]
    fn valid_restore_atomically_replaces_all_components() {
        let temp = tempfile::tempdir().unwrap();
        let settings = lethe::config::test_settings(&temp.path().join("live"));
        fs::create_dir_all(&settings.paths.workspace_dir).unwrap();
        fs::create_dir_all(&settings.paths.memory_dir).unwrap();
        fs::create_dir_all(settings.paths.lethe_home.join("config")).unwrap();
        fs::write(settings.paths.workspace_dir.join("old"), b"old").unwrap();
        fs::write(settings.paths.memory_dir.join("old"), b"old").unwrap();
        fs::write(&settings.paths.db_path, b"old").unwrap();
        fs::write(settings.paths.lethe_home.join("config/.env"), b"old").unwrap();

        let archive = temp.path().join("valid.tar.gz");
        let staging = temp.path().join("staging");
        create_private_staging(&staging).unwrap();
        let mut builder = archive_builder(&archive);
        append_file(&mut builder, "manifest.json", br#"{"version":1}"#);
        append_dir(&mut builder, "workspace");
        append_file(&mut builder, "workspace/new", b"workspace");
        append_dir(&mut builder, "data");
        append_dir(&mut builder, "data/memory");
        append_file(&mut builder, "data/memory/new", b"memory");
        append_file(&mut builder, "data/lethe.db", b"database");
        append_dir(&mut builder, "config");
        append_file(&mut builder, "config/.env", b"environment");
        finish_archive(builder);

        run_restore(&settings, &archive, &staging, true).unwrap();

        assert_eq!(
            fs::read(settings.paths.workspace_dir.join("new")).unwrap(),
            b"workspace"
        );
        assert!(!settings.paths.workspace_dir.join("old").exists());
        assert_eq!(
            fs::read(settings.paths.memory_dir.join("new")).unwrap(),
            b"memory"
        );
        assert!(!settings.paths.memory_dir.join("old").exists());
        assert_eq!(fs::read(&settings.paths.db_path).unwrap(), b"database");
        assert_eq!(
            fs::read(settings.paths.lethe_home.join("config/.env")).unwrap(),
            b"environment"
        );
    }

    #[test]
    fn invalid_archive_schema_never_changes_live_state() {
        let temp = tempfile::tempdir().unwrap();
        let settings = lethe::config::test_settings(&temp.path().join("live"));
        fs::create_dir_all(&settings.paths.workspace_dir).unwrap();
        fs::create_dir_all(&settings.paths.memory_dir).unwrap();
        fs::create_dir_all(settings.paths.db_path.parent().unwrap()).unwrap();
        fs::create_dir_all(settings.paths.lethe_home.join("config")).unwrap();
        fs::write(
            settings.paths.workspace_dir.join("old"),
            b"workspace sentinel",
        )
        .unwrap();
        fs::write(settings.paths.memory_dir.join("old"), b"memory sentinel").unwrap();
        fs::write(&settings.paths.db_path, b"db sentinel").unwrap();
        fs::write(
            settings.paths.lethe_home.join("config/.env"),
            b"env sentinel",
        )
        .unwrap();

        let archive = temp.path().join("invalid.tar.gz");
        let staging = temp.path().join("staging");
        create_private_staging(&staging).unwrap();
        let mut builder = archive_builder(&archive);
        append_file(&mut builder, "manifest.json", br#"{"version":1}"#);
        append_dir(&mut builder, "workspace");
        append_file(&mut builder, "workspace/new", b"new workspace");
        append_dir(&mut builder, "data");
        append_dir(&mut builder, "data/memory");
        append_file(&mut builder, "data/memory/new", b"new memory");
        append_file(&mut builder, "data/lethe.db", b"new db");
        append_dir(&mut builder, "config");
        append_file(&mut builder, "config/.env", b"new env");
        append_file(&mut builder, "data/unexpected", b"must reject");
        finish_archive(builder);

        assert!(run_restore(&settings, &archive, &staging, true).is_err());
        assert_eq!(
            fs::read(settings.paths.workspace_dir.join("old")).unwrap(),
            b"workspace sentinel"
        );
        assert_eq!(
            fs::read(settings.paths.memory_dir.join("old")).unwrap(),
            b"memory sentinel"
        );
        assert_eq!(fs::read(&settings.paths.db_path).unwrap(), b"db sentinel");
        assert_eq!(
            fs::read(settings.paths.lethe_home.join("config/.env")).unwrap(),
            b"env sentinel"
        );
    }

    #[test]
    fn atomic_file_promotion_replaces_symlink_without_following_it() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("source");
        let sentinel = temp.path().join("sentinel");
        let destination = temp.path().join("destination");
        fs::write(&source, b"restored").unwrap();
        fs::write(&sentinel, b"sentinel").unwrap();
        symlink(&sentinel, &destination).unwrap();

        replace_file_atomically(&source, &destination, 0o600).unwrap();

        assert!(
            !fs::symlink_metadata(&destination)
                .unwrap()
                .file_type()
                .is_symlink()
        );
        assert_eq!(fs::read(&destination).unwrap(), b"restored");
        assert_eq!(fs::read(&sentinel).unwrap(), b"sentinel");
    }

    #[test]
    fn failed_promotions_preserve_symlink_targets_and_existing_directories() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().unwrap();
        let source_file = temp.path().join("source-file");
        fs::write(&source_file, b"restored").unwrap();
        let file_destination = temp.path().join("file-destination");
        fs::create_dir(&file_destination).unwrap();
        fs::write(file_destination.join("sentinel"), b"directory sentinel").unwrap();
        assert!(replace_file_atomically(&source_file, &file_destination, 0o600).is_err());
        assert_eq!(
            fs::read(file_destination.join("sentinel")).unwrap(),
            b"directory sentinel"
        );

        let source_dir = temp.path().join("source-dir");
        fs::create_dir(&source_dir).unwrap();
        fs::write(source_dir.join("new"), b"restored").unwrap();
        let sentinel_dir = temp.path().join("sentinel-dir");
        fs::create_dir(&sentinel_dir).unwrap();
        fs::write(sentinel_dir.join("sentinel"), b"symlink sentinel").unwrap();
        let dir_destination = temp.path().join("dir-destination");
        symlink(&sentinel_dir, &dir_destination).unwrap();
        assert!(replace_dir_atomically(&source_dir, &dir_destination).is_err());
        assert!(
            fs::symlink_metadata(&dir_destination)
                .unwrap()
                .file_type()
                .is_symlink()
        );
        assert_eq!(
            fs::read(sentinel_dir.join("sentinel")).unwrap(),
            b"symlink sentinel"
        );
    }

    #[test]
    fn atomic_directory_exchange_replaces_nonempty_tree() {
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("source");
        let destination = temp.path().join("destination");
        fs::create_dir(&source).unwrap();
        fs::create_dir(&destination).unwrap();
        fs::write(source.join("new"), b"new").unwrap();
        fs::write(destination.join("old"), b"old").unwrap();

        replace_dir_atomically(&source, &destination).unwrap();

        assert_eq!(fs::read(destination.join("new")).unwrap(), b"new");
        assert!(!destination.join("old").exists());
        assert!(fs::read_dir(temp.path()).unwrap().all(|entry| {
            !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .contains("restore-dir")
        }));
    }
}
