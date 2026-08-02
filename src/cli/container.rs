//! `lethe container` — run Lethe in an isolated, rootless container so the
//! agent can install arbitrary software without touching the host, sharing
//! only the directories you mount.
//!
//! Engine: rootless **Podman** on Linux (and Intel macOS), **Apple Container**
//! on Apple-Silicon macOS. No host root is needed to run; the container runs
//! as root *inside* (default user namespace), so `apt install` works and files
//! written to bind mounts are owned by your host user. The container is
//! **persistent** (created once, started via the service) so installed
//! packages survive restarts.
//!
//! Safety: never recreates the container implicitly (that would drop installed
//! software); `--dry-run` prints the exact engine commands without running
//! them; engine install is always confirmed first.

use std::fs::OpenOptions;
use std::io::{self, Read};
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result, bail};
use flate2::read::GzDecoder;
use lethe::config::Settings;
use tar::Archive;

use crate::ContainerCommand;
use crate::cli::service;
use crate::cli::util::{confirm, prompt_line};

const CONTAINER_NAME: &str = "lethe";
const IMAGE: &str = "lethe:latest";
const CONTAINER_HOME: &str = "/root/.lethe";
const GENERATED_RUNTIME_BASE: &str =
    "debian:bookworm-slim@sha256:7b140f374b289a7c2befc338f42ebe6441b7ea838a042bbd5acbfca6ec875818";
const MAX_RELEASE_ARCHIVE_BYTES: u64 = 512 * 1024 * 1024;

// =============================================================================
// Engine
// =============================================================================

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Engine {
    Podman,
    AppleContainer,
}

impl Engine {
    pub fn bin(self) -> &'static str {
        match self {
            Engine::Podman => "podman",
            Engine::AppleContainer => "container",
        }
    }
    pub fn label(self) -> &'static str {
        match self {
            Engine::Podman => "Podman (rootless)",
            Engine::AppleContainer => "Apple Container",
        }
    }
}

fn which(bin: &str) -> bool {
    Command::new(bin)
        .arg("--version")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// The engine we'd prefer on this platform if none is installed yet.
fn preferred_engine() -> Engine {
    if cfg!(target_os = "macos") && cfg!(target_arch = "aarch64") {
        Engine::AppleContainer
    } else {
        Engine::Podman
    }
}

/// The engine already installed, if any (an installed `container` on macOS
/// wins; otherwise `podman`).
fn detect_engine() -> Option<Engine> {
    if cfg!(target_os = "macos") && which("container") {
        return Some(Engine::AppleContainer);
    }
    if which("podman") {
        return Some(Engine::Podman);
    }
    None
}

/// Ensure a container engine is available, offering to install one (always
/// after confirmation). `assume_yes` skips the confirmation prompt.
pub fn ensure_engine(assume_yes: bool) -> Result<Engine> {
    if let Some(engine) = detect_engine() {
        return Ok(engine);
    }
    let engine = preferred_engine();
    let Some(cmds) = install_commands(engine) else {
        bail!(
            "No container engine found and no known installer for this system.\n\
             Install {} manually, then re-run. (Linux: your package manager's \
             `podman`; macOS: `brew install {}`.)",
            engine.label(),
            engine.bin()
        );
    };
    println!(
        "No container engine found. Lethe can install {}:",
        engine.label()
    );
    for (program, args) in &cmds {
        println!("  $ {program} {}", args.join(" "));
    }
    if !assume_yes && !confirm("Install it now? [y/N]: ", false)? {
        bail!("Install a container engine and re-run `lethe container up`.");
    }
    for (program, args) in &cmds {
        let arg_refs: Vec<&str> = args.iter().map(String::as_str).collect();
        service::run_cmd(program, &arg_refs)?;
    }
    post_install(engine)?;
    detect_engine().ok_or_else(|| {
        anyhow::anyhow!(
            "{} still not on PATH after install — open a new shell and retry.",
            engine.bin()
        )
    })
}

/// Package-manager commands to install `engine` on this host, or `None` when
/// we don't know how (caller falls back to manual guidance).
fn install_commands(engine: Engine) -> Option<Vec<(String, Vec<String>)>> {
    if cfg!(target_os = "macos") {
        if !which("brew") {
            return None;
        }
        let pkg = engine.bin(); // podman | container
        return Some(vec![("brew".into(), vec!["install".into(), pkg.into()])]);
    }
    if cfg!(target_os = "linux") {
        return match linux_family()?.as_str() {
            "debian" => Some(vec![
                ("sudo".into(), vec!["apt-get".into(), "update".into()]),
                (
                    "sudo".into(),
                    vec![
                        "apt-get".into(),
                        "install".into(),
                        "-y".into(),
                        "podman".into(),
                    ],
                ),
            ]),
            "fedora" => Some(vec![(
                "sudo".into(),
                vec!["dnf".into(), "install".into(), "-y".into(), "podman".into()],
            )]),
            _ => None,
        };
    }
    None
}

/// Coarse Linux family from /etc/os-release: "debian" (ubuntu/debian),
/// "fedora" (fedora/rhel family), or None.
fn linux_family() -> Option<String> {
    let text = std::fs::read_to_string("/etc/os-release").ok()?;
    let mut id = String::new();
    let mut like = String::new();
    for line in text.lines() {
        if let Some(v) = line.strip_prefix("ID=") {
            id = v.trim_matches('"').to_ascii_lowercase();
        } else if let Some(v) = line.strip_prefix("ID_LIKE=") {
            like = v.trim_matches('"').to_ascii_lowercase();
        }
    }
    let hay = format!("{id} {like}");
    if ["ubuntu", "debian", "linuxmint", "pop"]
        .iter()
        .any(|d| hay.contains(d))
    {
        Some("debian".into())
    } else if ["fedora", "rhel", "centos", "rocky", "almalinux"]
        .iter()
        .any(|d| hay.contains(d))
    {
        Some("fedora".into())
    } else {
        None
    }
}

/// Bring the engine's backend up (macOS needs a VM / helper running).
fn post_install(engine: Engine) -> Result<()> {
    if !cfg!(target_os = "macos") {
        return Ok(());
    }
    match engine {
        Engine::Podman => {
            // Idempotent: init is a no-op if a machine already exists.
            let _ = service::run_cmd("podman", &["machine", "init"]);
            let _ = service::run_cmd("podman", &["machine", "start"]);
        }
        Engine::AppleContainer => {
            let _ = service::run_cmd("container", &["system", "start"]);
        }
    }
    Ok(())
}

// =============================================================================
// Image: Containerfile + binary source
// =============================================================================

#[derive(Clone, Debug, PartialEq, Eq)]
enum BinarySource {
    /// Copy the host's own (Linux) lethe binary into the image — offline,
    /// version-matched. Build context holds the binary as `lethe`.
    CopyLocal,
    /// Download and attest a published Linux release on the host, then copy
    /// only its `lethe` binary into the build context.
    Download {
        target: String,
        base_url: String,
        repository: String,
    },
}

/// Linux release target triple for a container CPU arch.
fn linux_target(arch: &str) -> &'static str {
    match arch {
        "aarch64" | "arm64" => "aarch64-unknown-linux-gnu",
        _ => "x86_64-unknown-linux-gnu",
    }
}

/// The container's CPU arch (Apple-Silicon container VMs are aarch64).
fn container_arch() -> &'static str {
    if cfg!(target_arch = "aarch64") {
        "aarch64"
    } else {
        "x86_64"
    }
}

fn release_settings() -> (String, String) {
    let owner = std::env::var("LETHE_REPO_OWNER").unwrap_or_else(|_| "alien-id".into());
    let name = std::env::var("LETHE_REPO_NAME").unwrap_or_else(|_| "lethe".into());
    let repository = format!("{owner}/{name}");
    let base_url = std::env::var("LETHE_RELEASE_BASE_URL")
        .unwrap_or_else(|_| format!("https://github.com/{repository}/releases/latest/download"));
    (base_url, repository)
}

/// Decide how to get the lethe binary into the image: copy the host binary
/// when the host is Linux on the same arch (fast, offline); otherwise download
/// the matching Linux release (macOS, cross-arch).
fn binary_source() -> BinarySource {
    let arch = container_arch();
    let host_is_linux_same_arch = cfg!(target_os = "linux")
        && ((arch == "x86_64" && cfg!(target_arch = "x86_64"))
            || (arch == "aarch64" && cfg!(target_arch = "aarch64")));
    if host_is_linux_same_arch {
        BinarySource::CopyLocal
    } else {
        let (base_url, repository) = release_settings();
        BinarySource::Download {
            target: linux_target(arch).to_string(),
            base_url,
            repository,
        }
    }
}

/// Heavier "batteries-included" packages, baked in only with `--with-tools`.
/// Everything here the agent can otherwise `apt install` on demand — and it
/// persists in the container's writable layer — so the default image stays
/// lean (~150–180 MB vs ~1 GB with these).
const EXTRA_TOOL_PACKAGES: &str = "ffmpeg python3 python3-venv python3-pip build-essential \\\n\
     \x20   ripgrep jq unzip xz-utils wget file diffutils procps less sudo which";

/// The runtime image: Debian slim + the lethe binary, running as root so the
/// agent can install more at runtime. Lean by default; `with_tools` bakes in
/// the common heavyweights up front.
fn render_containerfile(source: &BinarySource, with_tools: bool) -> String {
    let _ = source;
    let install_binary = "COPY lethe /usr/local/bin/lethe\nRUN chmod +x /usr/local/bin/lethe";
    let extra = if with_tools {
        format!(" \\\n\x20   {EXTRA_TOOL_PACKAGES}")
    } else {
        String::new()
    };
    format!(
        "# Generated by `lethe container` — runtime image (root inside).\n\
         # Lean base: the agent installs anything else on demand (it persists).\n\
         FROM {GENERATED_RUNTIME_BASE}\n\
         RUN apt-get update \\\n\
         \x20 && apt-get install -y --no-install-recommends \\\n\
         \x20   ca-certificates curl git{extra} \\\n\
         \x20 && rm -rf /var/lib/apt/lists/*\n\
         {install_binary}\n\
         ENV HOME=/root LETHE_HOME={CONTAINER_HOME}\n\
         WORKDIR /root\n\
         ENTRYPOINT [\"lethe\"]\n"
    )
}

fn release_tag_from_url(url: &str) -> Result<String> {
    let marker = "/releases/download/";
    let (_, remainder) = url
        .split_once(marker)
        .ok_or_else(|| anyhow::anyhow!("release URL did not resolve to a tagged GitHub release"))?;
    let (tag, _asset) = remainder
        .split_once('/')
        .filter(|(tag, asset)| !tag.is_empty() && !asset.is_empty())
        .ok_or_else(|| anyhow::anyhow!("resolved release URL did not contain a tag"))?;
    if !tag
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        bail!("resolved release URL contained an unsupported tag");
    }
    Ok(tag.to_string())
}

fn resolve_tagged_release_url(
    client: &reqwest::blocking::Client,
    url: &str,
) -> Result<(reqwest::Url, String)> {
    if let Ok(tag) = release_tag_from_url(url) {
        let tagged_url = reqwest::Url::parse(url)
            .with_context(|| format!("parsing tagged release URL {url}"))?;
        return Ok((tagged_url, tag));
    }

    // GitHub's `releases/latest/download` endpoint first redirects to a URL
    // containing the immutable tag, then redirects again to release-assets.
    // Stop after the first response so the attestation can be constrained to
    // that exact tag rather than trying to infer it from the final CDN URL.
    let response = client
        .get(url)
        .send()
        .with_context(|| format!("resolving latest release URL {url}"))?;
    if !response.status().is_redirection() {
        bail!("latest release URL did not redirect to an immutable tagged release: {url}");
    }
    let location = response
        .headers()
        .get(reqwest::header::LOCATION)
        .ok_or_else(|| anyhow::anyhow!("latest release redirect omitted Location"))?
        .to_str()
        .with_context(|| "latest release redirect contained an invalid Location")?;
    let tagged_url = response
        .url()
        .join(location)
        .with_context(|| format!("resolving release redirect {location}"))?;
    let tag = release_tag_from_url(tagged_url.as_str())?;
    Ok((tagged_url, tag))
}

fn download_release_archive(url: &str, archive_path: &Path) -> Result<String> {
    let resolver = reqwest::blocking::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .with_context(|| "creating release redirect resolver")?;
    let (tagged_url, release_tag) = resolve_tagged_release_url(&resolver, url)?;

    let downloader = reqwest::blocking::Client::builder()
        .build()
        .with_context(|| "creating release archive downloader")?;
    let response = downloader
        .get(tagged_url.clone())
        .send()
        .with_context(|| format!("downloading release archive from {tagged_url}"))?
        .error_for_status()
        .with_context(|| format!("release archive request failed for {tagged_url}"))?;

    if response
        .content_length()
        .is_some_and(|length| length > MAX_RELEASE_ARCHIVE_BYTES)
    {
        bail!(
            "release archive exceeds the {} MiB limit",
            MAX_RELEASE_ARCHIVE_BYTES / 1024 / 1024
        );
    }

    let mut output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(archive_path)
        .with_context(|| format!("creating {}", archive_path.display()))?;
    let mut limited = response.take(MAX_RELEASE_ARCHIVE_BYTES + 1);
    let copied = io::copy(&mut limited, &mut output)
        .with_context(|| format!("writing {}", archive_path.display()))?;
    if copied > MAX_RELEASE_ARCHIVE_BYTES {
        let _ = std::fs::remove_file(archive_path);
        bail!(
            "release archive exceeds the {} MiB limit",
            MAX_RELEASE_ARCHIVE_BYTES / 1024 / 1024
        );
    }
    Ok(release_tag)
}

fn verify_release_attestation(archive_path: &Path, repository: &str, tag: &str) -> Result<()> {
    let signer_workflow = format!("{repository}/.github/workflows/release.yml");
    let source_ref = format!("refs/tags/{tag}");
    let status = Command::new("gh")
        .arg("attestation")
        .arg("verify")
        .arg(archive_path)
        .arg("--repo")
        .arg(repository)
        .arg("--signer-workflow")
        .arg(&signer_workflow)
        .arg("--source-ref")
        .arg(&source_ref)
        .arg("--deny-self-hosted-runners")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .with_context(|| {
            "GitHub CLI (gh) is required to verify release provenance; \
             install gh or rerun with --from-source"
        })?;
    if !status.success() {
        bail!(
            "release provenance verification failed for {} (expected {signer_workflow} at \
             {source_ref}); rerun with --from-source",
            archive_path.display()
        );
    }
    Ok(())
}

fn extract_release_binary(archive_path: &Path, destination: &Path, target: &str) -> Result<()> {
    let input = std::fs::File::open(archive_path)
        .with_context(|| format!("opening {}", archive_path.display()))?;
    let mut archive = Archive::new(GzDecoder::new(input));
    let expected_path = PathBuf::from(format!("lethe-{target}")).join("lethe");
    let mut found = false;

    for entry in archive
        .entries()
        .with_context(|| format!("reading {}", archive_path.display()))?
    {
        let mut entry = entry.with_context(|| format!("reading {}", archive_path.display()))?;
        if entry.path()?.as_ref() != expected_path {
            continue;
        }
        if found {
            bail!(
                "release archive contained more than one {}",
                expected_path.display()
            );
        }
        if !entry.header().entry_type().is_file() {
            bail!(
                "release archive entry {} was not a regular file",
                expected_path.display()
            );
        }

        let mut output = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o700)
            .open(destination)
            .with_context(|| format!("creating {}", destination.display()))?;
        io::copy(&mut entry, &mut output)
            .with_context(|| format!("extracting {}", expected_path.display()))?;
        found = true;
    }

    if !found {
        bail!(
            "release archive did not contain the expected {} binary",
            expected_path.display()
        );
    }
    std::fs::set_permissions(destination, std::fs::Permissions::from_mode(0o755))
        .with_context(|| format!("setting permissions on {}", destination.display()))?;
    Ok(())
}

fn install_verified_release<F>(
    archive_path: &Path,
    destination: &Path,
    target: &str,
    repository: &str,
    tag: &str,
    verifier: F,
) -> Result<()>
where
    F: FnOnce(&Path, &str, &str) -> Result<()>,
{
    verifier(archive_path, repository, tag)?;
    extract_release_binary(archive_path, destination, target)
}

fn prepare_binary(source: &BinarySource, context: &Path) -> Result<()> {
    let destination = context.join("lethe");
    match source {
        BinarySource::CopyLocal => {
            let executable =
                std::env::current_exe().with_context(|| "resolving current executable")?;
            std::fs::copy(&executable, &destination).with_context(|| {
                format!(
                    "copying {} into container build context",
                    executable.display()
                )
            })?;
            Ok(())
        }
        BinarySource::Download {
            target,
            base_url,
            repository,
        } => {
            let url = format!("{}/lethe-{target}.tar.gz", base_url.trim_end_matches('/'));
            let archive_path = context.join(format!(".lethe-{target}.tar.gz"));
            let tag = download_release_archive(&url, &archive_path)?;
            let result = install_verified_release(
                &archive_path,
                &destination,
                target,
                repository,
                &tag,
                verify_release_attestation,
            );
            let _ = std::fs::remove_file(&archive_path);
            result
        }
    }
}

// =============================================================================
// Mounts
// =============================================================================

fn host_lethe_home(settings: &Settings) -> String {
    settings.paths.lethe_home.display().to_string()
}

fn mounts_file(settings: &Settings) -> PathBuf {
    settings
        .paths
        .config_file
        .parent()
        .map(|p| p.join("container-mounts"))
        .unwrap_or_else(|| PathBuf::from("container-mounts"))
}

fn load_mounts(settings: &Settings) -> Vec<String> {
    std::fs::read_to_string(mounts_file(settings))
        .map(|s| {
            s.lines()
                .map(str::trim)
                .filter(|l| !l.is_empty() && !l.starts_with('#'))
                .map(normalize_mount)
                .collect()
        })
        .unwrap_or_default()
}

fn save_mounts(settings: &Settings, mounts: &[String]) -> Result<()> {
    let path = mounts_file(settings);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let body = format!(
        "# Extra host directories shared into the Lethe container.\n\
         # One `host[:container]` per line (container path defaults to host path).\n{}\n",
        mounts.join("\n")
    );
    std::fs::write(&path, body).with_context(|| format!("writing {}", path.display()))?;
    Ok(())
}

/// `host` → `host:host`; `host:container` passes through. Used so a user can
/// just list a directory and have it mount at the same path inside.
fn normalize_mount(entry: &str) -> String {
    let entry = entry.trim();
    if entry.contains(':') {
        entry.to_string()
    } else {
        format!("{entry}:{entry}")
    }
}

/// `podman create` arguments for the persistent container. Pure so it can be
/// unit-tested. Root inside (no `--user`/`--userns`), `label=disable` to avoid
/// relabeling shared host dirs, `~/.lethe` plus any extra mounts.
fn create_args(host_home: &str, mounts: &[String]) -> Vec<String> {
    let mut a: Vec<String> = vec![
        "create".into(),
        "--name".into(),
        CONTAINER_NAME.into(),
        "--label".into(),
        "app=lethe".into(),
        "--security-opt".into(),
        "label=disable".into(),
        "-e".into(),
        format!("LETHE_HOME={CONTAINER_HOME}"),
        "-v".into(),
        format!("{host_home}:{CONTAINER_HOME}"),
    ];
    for m in mounts {
        a.push("-v".into());
        a.push(normalize_mount(m));
    }
    a.push(IMAGE.into());
    a.push("api".into());
    a
}

// =============================================================================
// Command dispatch
// =============================================================================

pub fn run(settings: &Settings, command: ContainerCommand) -> Result<()> {
    match command {
        ContainerCommand::Up {
            rebuild,
            force,
            mount,
            now,
            dry_run,
            from_source,
            with_tools,
        } => up(
            settings,
            UpArgs {
                rebuild,
                force,
                extra_mounts: mount,
                now,
                dry_run,
                from_source,
                with_tools,
            },
        ),
        ContainerCommand::Down => down(settings),
        ContainerCommand::Shell => shell(settings),
        ContainerCommand::Rebuild {
            dry_run,
            force,
            with_tools,
        } => up(
            settings,
            UpArgs {
                rebuild: true,
                force,
                extra_mounts: vec![],
                now: true,
                dry_run,
                from_source: false,
                with_tools,
            },
        ),
        ContainerCommand::Status => status(settings),
        ContainerCommand::Logs { follow } => logs(settings, follow),
        ContainerCommand::Build {
            from_source,
            dry_run,
            with_tools,
        } => {
            let engine = if dry_run {
                preferred_engine()
            } else {
                ensure_engine(false)?
            };
            build(engine, from_source, dry_run, with_tools)
        }
    }
}

pub struct UpArgs {
    pub rebuild: bool,
    pub force: bool,
    pub extra_mounts: Vec<String>,
    pub now: bool,
    pub dry_run: bool,
    pub from_source: bool,
    /// Bake the heavyweight tool set (ffmpeg/python/build-essential/…) into
    /// the image instead of leaving the agent to install them on demand.
    pub with_tools: bool,
}

/// Build (if needed) → create the persistent container (if needed) → install
/// and start the service. The default install path Lethe uses.
pub fn up(settings: &Settings, args: UpArgs) -> Result<()> {
    let engine = if args.dry_run {
        detect_engine().unwrap_or_else(preferred_engine)
    } else {
        ensure_engine(false)?
    };
    println!("Engine: {}", engine.label());

    // Persist any newly-supplied mounts so the service uses them too.
    if !args.extra_mounts.is_empty() {
        let mut all = load_mounts(settings);
        for m in &args.extra_mounts {
            let norm = normalize_mount(m);
            if !all.contains(&norm) {
                all.push(norm);
            }
        }
        save_mounts(settings, &all)?;
    }
    let mounts = load_mounts(settings);

    if args.rebuild || !image_exists(engine, args.dry_run) {
        build(engine, args.from_source, args.dry_run, args.with_tools)?;
    } else {
        println!("Image {IMAGE} already present.");
    }

    let host_home = host_lethe_home(settings);
    if args.rebuild && container_exists(engine, args.dry_run) {
        warn("Rebuild recreates the container — software installed inside it will be lost.");
        eng(engine, &["rm", "-f", CONTAINER_NAME], args.dry_run)?;
    }
    if args.dry_run || !container_exists(engine, args.dry_run) {
        let create = create_args(&host_home, &mounts);
        let refs: Vec<&str> = create.iter().map(String::as_str).collect();
        eng(engine, &refs, args.dry_run)?;
    } else {
        println!("Container '{CONTAINER_NAME}' already exists (keeping its installed software).");
    }

    install_service(engine, args.now, args.dry_run, args.force)?;
    if !args.dry_run {
        service::write_deployment(settings, "container");
        println!();
        println!("Container ready. Useful commands:");
        println!("  lethe container shell    # root shell inside (install software here)");
        println!("  lethe container status   # state + service");
        println!("  lethe container logs -f  # follow logs");
    }
    Ok(())
}

/// Run the container in the **foreground**, attached to this terminal
/// (Ctrl-C stops it). Builds the image and creates the container on first run.
/// This is `lethe run` — no service is installed.
pub fn run_foreground(settings: &Settings) -> Result<()> {
    let engine = ensure_engine(false)?;
    println!("Engine: {}", engine.label());
    if !image_exists(engine, false) {
        build(engine, false, false, false)?;
    }
    if !container_exists(engine, false) {
        let mounts = load_mounts(settings);
        let create = create_args(&host_lethe_home(settings), &mounts);
        let refs: Vec<&str> = create.iter().map(String::as_str).collect();
        eng(engine, &refs, false)?;
    }
    if service::systemd_unit_path().exists() {
        warn("A background service is also installed; `lethe run` attaches to the same container.");
    }
    println!("Starting Lethe in the foreground — Ctrl-C to stop.\n");
    let status = Command::new(engine.bin())
        .args(["start", "--attach", CONTAINER_NAME])
        .status()
        .with_context(|| "starting the container in the foreground")?;
    if !status.success() {
        bail!("container exited with {status}");
    }
    Ok(())
}

fn down(settings: &Settings) -> Result<()> {
    let engine = detect_engine().ok_or_else(|| anyhow::anyhow!("no container engine installed"))?;
    // Prefer stopping via the service so it doesn't get restarted under us.
    if service::systemd_unit_path().exists()
        && matches!(service::detect().manager, service::Manager::Systemd)
    {
        let _ = service::run_cmd("systemctl", &["--user", "stop", "lethe.service"]);
    }
    eng(engine, &["stop", CONTAINER_NAME], false)?;
    let _ = settings;
    println!("Stopped {CONTAINER_NAME}.");
    Ok(())
}

fn shell(settings: &Settings) -> Result<()> {
    let _ = settings;
    let engine = detect_engine().ok_or_else(|| anyhow::anyhow!("no container engine installed"))?;
    // Interactive: hand the terminal straight to the engine.
    let status = Command::new(engine.bin())
        .args(["exec", "-it", CONTAINER_NAME, "/bin/bash"])
        .status()
        .with_context(|| "launching container shell")?;
    if !status.success() {
        bail!("could not open a shell — is the container running? (`lethe container status`)");
    }
    Ok(())
}

fn status(settings: &Settings) -> Result<()> {
    match detect_engine() {
        Some(engine) => {
            println!("Engine: {} ({})", engine.label(), engine.bin());
            println!(
                "Deployment: {}",
                service::read_deployment(settings).unwrap_or_else(|| "unknown".into())
            );
            let _ = Command::new(engine.bin())
                .args(["ps", "-a", "--filter", &format!("name={CONTAINER_NAME}")])
                .status();
            println!();
            println!("Mounts:");
            println!("  {}:{CONTAINER_HOME}", host_lethe_home(settings));
            for m in load_mounts(settings) {
                println!("  {m}");
            }
        }
        None => println!("No container engine installed (run `lethe container up`)."),
    }
    Ok(())
}

fn logs(settings: &Settings, follow: bool) -> Result<()> {
    let _ = settings;
    let engine = detect_engine().ok_or_else(|| anyhow::anyhow!("no container engine installed"))?;
    let mut args = vec!["logs"];
    if follow {
        args.push("-f");
    }
    args.push(CONTAINER_NAME);
    let _ = Command::new(engine.bin()).args(&args).status();
    Ok(())
}

// =============================================================================
// Build + service install
// =============================================================================

fn build(engine: Engine, from_source: bool, dry_run: bool, with_tools: bool) -> Result<()> {
    if from_source {
        return build_from_source(engine, dry_run, with_tools);
    }
    let source = binary_source();
    match &source {
        BinarySource::CopyLocal => println!("Building {IMAGE} (copying host binary)..."),
        BinarySource::Download { target, .. } => {
            println!("Building {IMAGE} (downloading and verifying {target} release)...")
        }
    }

    // Prepare a build context dir with the generated Containerfile (+ binary).
    let ctx = build_context_dir();
    if !dry_run {
        let prepared = (|| -> Result<()> {
            let mut builder = std::fs::DirBuilder::new();
            builder.mode(0o700);
            builder
                .create(&ctx)
                .with_context(|| format!("creating private build context {}", ctx.display()))?;
            std::fs::write(
                ctx.join("Containerfile"),
                render_containerfile(&source, with_tools),
            )
            .with_context(|| "writing generated Containerfile")?;
            prepare_binary(&source, &ctx)
        })();
        if let Err(error) = prepared {
            let _ = std::fs::remove_dir_all(&ctx);
            return Err(error);
        }
    }

    let cf = ctx.join("Containerfile");
    let arch = container_arch();
    let build_args: Vec<String> = match engine {
        Engine::Podman => vec![
            "build".into(),
            "--platform".into(),
            format!("linux/{arch}"),
            "-t".into(),
            IMAGE.into(),
            "-f".into(),
            cf.display().to_string(),
            ctx.display().to_string(),
        ],
        Engine::AppleContainer => vec![
            "build".into(),
            "--arch".into(),
            arch.into(),
            "-t".into(),
            IMAGE.into(),
            "-f".into(),
            cf.display().to_string(),
            ctx.display().to_string(),
        ],
    };
    let refs: Vec<&str> = build_args.iter().map(String::as_str).collect();
    let result = eng(engine, &refs, dry_run);
    if !dry_run {
        let _ = std::fs::remove_dir_all(&ctx);
    }
    result
}

fn build_from_source(engine: Engine, dry_run: bool, with_tools: bool) -> Result<()> {
    let repo = repo_root().ok_or_else(|| {
        anyhow::anyhow!(
            "--from-source needs the Lethe repo (Containerfile). Run from a checkout, \
             or drop --from-source to build from the published binary."
        )
    })?;
    println!("Building {IMAGE} from source at {}...", repo.display());
    let cf = repo.join("Containerfile");
    let arch = container_arch();
    let mut build_args: Vec<String> = match engine {
        Engine::Podman => vec!["build".into(), "--platform".into(), format!("linux/{arch}")],
        Engine::AppleContainer => vec!["build".into(), "--arch".into(), arch.into()],
    };
    if with_tools {
        // The repo Containerfile honours ARG WITH_TOOLS to bake the heavy set.
        build_args.push("--build-arg".into());
        build_args.push("WITH_TOOLS=1".into());
    }
    build_args.extend([
        "-t".into(),
        IMAGE.into(),
        "-f".into(),
        cf.display().to_string(),
        repo.display().to_string(),
    ]);
    let refs: Vec<&str> = build_args.iter().map(String::as_str).collect();
    eng(engine, &refs, dry_run)
}

fn install_service(engine: Engine, now: bool, dry_run: bool, force: bool) -> Result<()> {
    let platform = service::detect();
    match platform.manager {
        service::Manager::Systemd => {
            let path = service::systemd_unit_path();
            if dry_run {
                println!(
                    "$ write {} (ExecStart={} start -a {CONTAINER_NAME})",
                    path.display(),
                    engine.bin()
                );
                println!("$ systemctl --user daemon-reload");
                println!("$ systemctl --user enable --now lethe.service");
                return Ok(());
            }
            service::guard_existing(&path, force)?;
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::write(&path, render_systemd_unit(engine))
                .with_context(|| format!("writing {}", path.display()))?;
            println!("Wrote {}", path.display());
            if now || confirm("Enable and start the container service now? [y/N]: ", false)? {
                service::enable_linger();
                service::run_cmd("systemctl", &["--user", "daemon-reload"])?;
                service::run_cmd("systemctl", &["--user", "enable", "--now", "lethe.service"])?;
                println!("Service enabled and started.");
            } else {
                println!("Next: systemctl --user enable --now lethe.service");
            }
            Ok(())
        }
        service::Manager::Launchd => {
            let path = service::launchd_plist_path();
            if dry_run {
                println!(
                    "$ write {} (ProgramArguments: {} start -a {CONTAINER_NAME})",
                    path.display(),
                    engine.bin()
                );
                return Ok(());
            }
            service::guard_existing(&path, force)?;
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::write(&path, render_launchd_plist(engine))
                .with_context(|| format!("writing {}", path.display()))?;
            println!("Wrote {}", path.display());
            if now || confirm("Load and start the container service now? [y/N]: ", false)? {
                service::run_cmd("launchctl", &["load", "-w", &path.to_string_lossy()])?;
                println!("Service loaded.");
            } else {
                println!("Next: launchctl load -w {}", path.display());
            }
            Ok(())
        }
        service::Manager::Unsupported => {
            warn("No service manager detected — start the container yourself:");
            println!("  {} start {CONTAINER_NAME}", engine.bin());
            Ok(())
        }
    }
}

fn render_systemd_unit(engine: Engine) -> String {
    let bin = engine.bin();
    format!(
        "[Unit]\n\
         Description=Lethe Autonomous AI Agent (container)\n\
         After=network-online.target\n\
         Wants=network-online.target\n\
         \n\
         [Service]\n\
         Type=simple\n\
         ExecStart={bin} start -a {CONTAINER_NAME}\n\
         ExecStop={bin} stop {CONTAINER_NAME}\n\
         Restart=always\n\
         RestartSec=10\n\
         \n\
         [Install]\n\
         WantedBy=default.target\n"
    )
}

fn render_launchd_plist(engine: Engine) -> String {
    let bin = engine.bin();
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n\
         <plist version=\"1.0\">\n\
         <dict>\n\
         \x20 <key>Label</key><string>com.lethe.lethe</string>\n\
         \x20 <key>ProgramArguments</key>\n\
         \x20 <array><string>{bin}</string><string>start</string><string>-a</string><string>{CONTAINER_NAME}</string></array>\n\
         \x20 <key>RunAtLoad</key><true/>\n\
         \x20 <key>KeepAlive</key><true/>\n\
         </dict>\n\
         </plist>\n"
    )
}

// =============================================================================
// Helpers
// =============================================================================

/// Run an engine subcommand, or print it under `--dry-run`.
fn eng(engine: Engine, args: &[&str], dry_run: bool) -> Result<()> {
    if dry_run {
        println!("$ {} {}", engine.bin(), args.join(" "));
        return Ok(());
    }
    service::run_cmd(engine.bin(), args)
}

fn image_exists(engine: Engine, dry_run: bool) -> bool {
    if dry_run {
        return false;
    }
    match engine {
        Engine::Podman => Command::new("podman")
            .args(["image", "exists", IMAGE])
            .status()
            .map(|s| s.success())
            .unwrap_or(false),
        // Apple Container has no `image exists`; assume absent so we (re)build.
        Engine::AppleContainer => false,
    }
}

fn container_exists(engine: Engine, dry_run: bool) -> bool {
    if dry_run {
        return false;
    }
    match engine {
        Engine::Podman => Command::new("podman")
            .args(["container", "exists", CONTAINER_NAME])
            .status()
            .map(|s| s.success())
            .unwrap_or(false),
        Engine::AppleContainer => false,
    }
}

fn build_context_dir() -> PathBuf {
    std::env::temp_dir().join(format!("lethe-container-build-{}", uuid::Uuid::new_v4()))
}

fn is_repo_root(dir: &Path) -> bool {
    dir.join(".git").exists() && dir.join("Containerfile").exists()
}

/// The Lethe repo root, if the current directory or running binary lives in a checkout.
fn repo_root() -> Option<PathBuf> {
    if let Ok(cwd) = std::env::current_dir() {
        if is_repo_root(&cwd) {
            return Some(cwd);
        }
    }

    let exe = std::env::current_exe().ok()?;
    // target/<profile>/lethe → repo root is two levels up; verify Containerfile.
    let candidate = exe.parent()?.parent()?.parent()?;
    is_repo_root(candidate).then(|| candidate.to_path_buf())
}

/// Prompt for extra directories to share into the container. Used by `init`.
pub fn prompt_and_save_mounts(settings: &Settings) -> Result<()> {
    println!("\nShare host directories with the container?");
    println!("  Lethe is isolated except for what you mount. ~/.lethe is always shared.");
    println!("  List directories to share (comma-separated, blank for none):");
    let line = prompt_line("  > ")?;
    let mounts: Vec<String> = line
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(normalize_mount)
        .collect();
    if mounts.is_empty() {
        return Ok(());
    }
    save_mounts(settings, &mounts)?;
    Ok(())
}

/// Tear down the container deployment (used by `lethe uninstall`). Best-effort;
/// `remove_image` also drops the built image.
pub fn teardown(settings: &Settings, remove_image: bool) -> Result<()> {
    let Some(engine) = detect_engine() else {
        return Ok(());
    };
    let _ = eng(engine, &["rm", "-f", CONTAINER_NAME], false);
    if remove_image {
        let _ = eng(engine, &["rmi", "-f", IMAGE], false);
    }
    let _ = settings;
    Ok(())
}

fn warn(message: &str) {
    println!("! {message}");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn containerfile_lean_by_default_fat_with_tools() {
        let lean = render_containerfile(&BinarySource::CopyLocal, false);
        assert!(lean.contains(&format!("FROM {GENERATED_RUNTIME_BASE}")));
        assert!(lean.contains("COPY lethe /usr/local/bin/lethe"));
        assert!(lean.contains("ENTRYPOINT [\"lethe\"]"));
        assert!(lean.contains("LETHE_HOME=/root/.lethe"));
        // root inside: no USER directive.
        assert!(!lean.contains("\nUSER "));
        // Lean: heavyweights are NOT baked in.
        assert!(!lean.contains("ffmpeg"));
        assert!(!lean.contains("build-essential"));
        // Always-present essentials.
        assert!(lean.contains("ca-certificates curl git"));

        let fat = render_containerfile(&BinarySource::CopyLocal, true);
        assert!(fat.contains("ffmpeg"));
        assert!(fat.contains("build-essential"));
        assert!(fat.contains("python3"));
    }

    #[test]
    fn containerfile_never_downloads_unverified_release() {
        let cf = render_containerfile(
            &BinarySource::Download {
                target: "aarch64-unknown-linux-gnu".into(),
                base_url: "https://example.test/dl".into(),
                repository: "example/lethe".into(),
            },
            false,
        );
        assert!(cf.contains("COPY lethe /usr/local/bin/lethe"));
        assert!(cf.contains("chmod +x /usr/local/bin/lethe"));
        assert!(!cf.contains("https://example.test"));
        assert!(!cf.contains("curl -fsSL"));
        assert!(!cf.contains("tar -x"));
    }

    #[test]
    fn release_tag_requires_resolved_conservative_tag() {
        assert_eq!(
            release_tag_from_url(
                "https://github.com/example/lethe/releases/download/v1.2.3/lethe.tar.gz"
            )
            .unwrap(),
            "v1.2.3"
        );
        assert!(
            release_tag_from_url(
                "https://github.com/example/lethe/releases/latest/download/lethe.tar.gz"
            )
            .is_err()
        );
        assert!(
            release_tag_from_url(
                "https://github.com/example/lethe/releases/download/v1%2Fevil/lethe.tar.gz"
            )
            .is_err()
        );
        assert!(
            release_tag_from_url("https://github.com/example/lethe/releases/download/v1.2.3")
                .is_err()
        );
    }

    #[test]
    fn latest_release_stops_at_tag_then_downloads_through_asset_redirect() {
        use std::io::{Read as _, Write as _};
        use std::net::TcpListener;
        use std::sync::{Arc, Mutex};

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(Vec::new()));
        let server_requests = Arc::clone(&requests);
        let server_base = base.clone();
        let body = b"mock attested archive".to_vec();
        let expected_body = body.clone();
        let server = std::thread::spawn(move || {
            for _ in 0..3 {
                let (mut stream, _) = listener.accept().unwrap();
                let mut request = [0_u8; 4096];
                let read = stream.read(&mut request).unwrap();
                let request = String::from_utf8_lossy(&request[..read]);
                let path = request
                    .lines()
                    .next()
                    .and_then(|line| line.split_whitespace().nth(1))
                    .unwrap()
                    .to_string();
                server_requests.lock().unwrap().push(path.clone());

                let response = match path.as_str() {
                    "/releases/latest/download/lethe-test.tar.gz" => format!(
                        "HTTP/1.1 302 Found\r\nLocation: {server_base}/releases/download/v1.2.3/lethe-test.tar.gz\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    )
                    .into_bytes(),
                    "/releases/download/v1.2.3/lethe-test.tar.gz" => format!(
                        "HTTP/1.1 302 Found\r\nLocation: {server_base}/release-assets/opaque-object\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    )
                    .into_bytes(),
                    "/release-assets/opaque-object" => {
                        let mut response = format!(
                            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                            body.len()
                        )
                        .into_bytes();
                        response.extend_from_slice(&body);
                        response
                    }
                    _ => panic!("unexpected request path: {path}"),
                };
                stream.write_all(&response).unwrap();
            }
        });

        let temp = tempfile::tempdir().unwrap();
        let archive = temp.path().join("lethe.tar.gz");
        let latest = format!("{base}/releases/latest/download/lethe-test.tar.gz");
        let tag = download_release_archive(&latest, &archive).unwrap();
        server.join().unwrap();

        assert_eq!(tag, "v1.2.3");
        assert_eq!(std::fs::read(&archive).unwrap(), expected_body);
        assert_eq!(
            *requests.lock().unwrap(),
            [
                "/releases/latest/download/lethe-test.tar.gz",
                "/releases/download/v1.2.3/lethe-test.tar.gz",
                "/release-assets/opaque-object",
            ]
        );
    }

    fn write_release_archive(path: &Path, target: &str, symlink: bool) {
        use flate2::Compression;
        use flate2::write::GzEncoder;
        use tar::{Builder, EntryType, Header};

        let output = std::fs::File::create(path).unwrap();
        let encoder = GzEncoder::new(output, Compression::default());
        let mut builder = Builder::new(encoder);
        let entry_path = format!("lethe-{target}/lethe");
        let mut header = Header::new_gnu();
        header.set_mode(0o755);
        if symlink {
            header.set_entry_type(EntryType::Symlink);
            header.set_size(0);
            header.set_link_name("../../outside").unwrap();
            header.set_cksum();
            builder
                .append_data(&mut header, entry_path, std::io::empty())
                .unwrap();
        } else {
            let contents = b"verified lethe";
            header.set_entry_type(EntryType::Regular);
            header.set_size(contents.len() as u64);
            header.set_cksum();
            builder
                .append_data(&mut header, entry_path, &contents[..])
                .unwrap();
        }
        builder.into_inner().unwrap().finish().unwrap();
    }

    #[test]
    fn verified_release_extracts_expected_regular_binary() {
        let temp = tempfile::tempdir().unwrap();
        let archive = temp.path().join("lethe.tar.gz");
        let destination = temp.path().join("lethe");
        let target = "aarch64-unknown-linux-gnu";
        write_release_archive(&archive, target, false);

        install_verified_release(
            &archive,
            &destination,
            target,
            "example/lethe",
            "v1.2.3",
            |_, repository, tag| {
                assert_eq!(repository, "example/lethe");
                assert_eq!(tag, "v1.2.3");
                Ok(())
            },
        )
        .unwrap();

        assert_eq!(std::fs::read(&destination).unwrap(), b"verified lethe");
        assert_eq!(
            std::fs::metadata(&destination)
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o755
        );
    }

    #[test]
    fn rejected_or_non_regular_release_is_never_installed() {
        let temp = tempfile::tempdir().unwrap();
        let archive = temp.path().join("lethe.tar.gz");
        let rejected_destination = temp.path().join("rejected-lethe");
        let symlink_destination = temp.path().join("symlink-lethe");
        let target = "aarch64-unknown-linux-gnu";
        write_release_archive(&archive, target, false);

        let rejected = install_verified_release(
            &archive,
            &rejected_destination,
            target,
            "example/lethe",
            "v1.2.3",
            |_, _, _| bail!("rejected attestation"),
        );
        assert!(rejected.is_err());
        assert!(!rejected_destination.exists());

        write_release_archive(&temp.path().join("symlink.tar.gz"), target, true);
        assert!(
            extract_release_binary(
                &temp.path().join("symlink.tar.gz"),
                &symlink_destination,
                target
            )
            .is_err()
        );
        assert!(!symlink_destination.exists());
    }

    #[test]
    fn create_args_are_root_inside_with_mounts() {
        let args = create_args("/home/x/.lethe", &["/data:/data".into()]);
        let joined = args.join(" ");
        assert!(joined.contains("create --name lethe"));
        assert!(joined.contains("-v /home/x/.lethe:/root/.lethe"));
        assert!(joined.contains("-v /data:/data"));
        assert!(joined.ends_with("lethe:latest api"));
        // No keep-id / non-root user → container runs as root so apt works.
        assert!(!joined.contains("keep-id"));
        assert!(!joined.contains("--user"));
    }

    #[test]
    fn normalize_mount_defaults_container_path() {
        assert_eq!(normalize_mount("/data"), "/data:/data");
        assert_eq!(normalize_mount("/h:/c"), "/h:/c");
    }

    #[test]
    fn linux_target_maps_arch() {
        assert_eq!(linux_target("aarch64"), "aarch64-unknown-linux-gnu");
        assert_eq!(linux_target("x86_64"), "x86_64-unknown-linux-gnu");
        assert_eq!(linux_target("anything"), "x86_64-unknown-linux-gnu");
    }

    #[test]
    fn is_repo_root_requires_git_marker_and_containerfile() {
        let unique = format!(
            "lethe-repo-root-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let dir = std::env::temp_dir().join(unique);
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        assert!(!is_repo_root(&dir));

        std::fs::write(dir.join(".git"), "").unwrap();
        assert!(!is_repo_root(&dir));

        std::fs::write(dir.join("Containerfile"), "FROM scratch\n").unwrap();
        assert!(is_repo_root(&dir));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn container_systemd_unit_starts_persistent_container() {
        let unit = render_systemd_unit(Engine::Podman);
        assert!(unit.contains("ExecStart=podman start -a lethe"));
        assert!(unit.contains("ExecStop=podman stop lethe"));
        assert!(unit.contains("WantedBy=default.target"));
    }
}
