#!/usr/bin/env bash
#
# Lethe Rust installer.
# Usage: curl -fsSL https://lethe.gg/install | bash
#
# Downloads the prebuilt `lethe` (and, when available, `lethe-migrate`)
# binary for the current platform, then hands off to `lethe init` for
# provider / model / API-key setup and an isolated rootless container
# deployment (or a native one with --yolo). Falls back to a source build
# when no binary asset matches the host (or `LETHE_INSTALL_FROM_SOURCE=1`).
#
# Env knobs:
#   LETHE_HOME                Install root (default: $HOME/.lethe)
#   LETHE_INSTALL_FROM_SOURCE Force a `cargo build --release` even if
#                             a binary release is available.
#   LETHE_SKIP_INIT           Skip the post-install `lethe init` wizard.
#   LETHE_SKIP_AGENT_ID       Skip installing the Alien agent-id CLIs
#                             (identity + vault + vault-sealed browser).
#   LETHE_REPO_URL            Override clone URL for the source path.
#   LETHE_RELEASE_BASE_URL    Override binary release base URL.

set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

REPO_URL="${LETHE_REPO_URL:-https://github.com/alien-id/lethe.git}"
REPO_OWNER="${LETHE_REPO_OWNER:-alien-id}"
REPO_NAME="${LETHE_REPO_NAME:-lethe}"
RELEASE_BASE_URL="${LETHE_RELEASE_BASE_URL:-https://github.com/$REPO_OWNER/$REPO_NAME/releases/latest/download}"
LETHE_HOME="${LETHE_HOME:-$HOME/.lethe}"
INSTALL_DIR="${LETHE_INSTALL_DIR:-$LETHE_HOME/install}"
CONFIG_DIR="$LETHE_HOME/config"
ENV_FILE="$CONFIG_DIR/.env"
BIN_DIR="$LETHE_HOME/bin"

info()    { echo -e "${BLUE}[INFO]${NC} $1"; }
success() { echo -e "${GREEN}[OK]${NC} $1"; }
warn()    { echo -e "${YELLOW}[WARN]${NC} $1"; }
error()   { echo -e "${RED}[ERROR]${NC} $1"; exit 1; }

print_header() {
    echo -e "${BLUE}"
    echo "╔═══════════════════════════════════════════════════════════╗"
    echo "║                     LETHE RUST                            ║"
    echo "║              Local AI assistant runtime                   ║"
    echo "╚═══════════════════════════════════════════════════════════╝"
    echo -e "${NC}"
}

ensure_cargo() {
    if command -v cargo >/dev/null 2>&1; then
        return
    fi

    warn "Rust/Cargo is not installed."
    if command -v curl >/dev/null 2>&1; then
        info "Installing Rust through rustup..."
        curl --proto '=https' --tlsv1.2 -fsSL https://sh.rustup.rs | sh -s -- -y
        # shellcheck disable=SC1091
        . "$HOME/.cargo/env"
    fi

    command -v cargo >/dev/null 2>&1 || error "Install Rust from https://rustup.rs and rerun this installer."
}

ensure_protoc() {
    if command -v protoc >/dev/null 2>&1; then
        return
    fi
    error "protoc is required by the migrator's LanceDB dep at build time. \
Install protobuf-compiler/libprotobuf-dev (Debian/Ubuntu), \
protobuf-compiler/protobuf-devel (Fedora), or protobuf (Homebrew), then rerun."
}

detect_release_target() {
    local os arch
    os="$(uname -s)"
    arch="$(uname -m)"
    case "$os:$arch" in
        Linux:x86_64|Linux:amd64)   echo "x86_64-unknown-linux-gnu" ;;
        Linux:aarch64|Linux:arm64)  echo "aarch64-unknown-linux-gnu" ;;
        Darwin:x86_64|Darwin:amd64) echo "x86_64-apple-darwin" ;;
        Darwin:aarch64|Darwin:arm64) echo "aarch64-apple-darwin" ;;
        *) return 1 ;;
    esac
}

# Verify that GitHub produced this exact archive for the configured repository.
# A checksum uploaded beside the archive would share the same compromise
# boundary, so release binaries require GitHub's signed artifact attestation.
verify_release_attestation() {
    local archive="$1"
    local signer_workflow="$2"
    local release_tag="$3"

    if ! command -v gh >/dev/null 2>&1; then
        warn "GitHub CLI (gh) is required to verify binary release provenance."
        return 1
    fi

    if ! gh attestation verify "$archive" \
        --repo "$REPO_OWNER/$REPO_NAME" \
        --signer-workflow "$REPO_OWNER/$REPO_NAME/.github/workflows/$signer_workflow" \
        --source-ref "refs/tags/$release_tag" \
        --deny-self-hosted-runners >/dev/null 2>&1; then
        warn "Release provenance verification failed for $(basename "$archive")."
        return 1
    fi
}

release_tag_from_url() {
    local remainder tag asset
    remainder="${1#*/releases/download/}"
    if [ "$remainder" = "$1" ]; then
        return 1
    fi
    tag="${remainder%%/*}"
    asset="${remainder#*/}"
    if [[ "$asset" == "$remainder" || -z "$asset" \
        || -z "$tag" || "$tag" == *[!A-Za-z0-9._-]* ]]; then
        return 1
    fi
    printf '%s\n' "$tag"
}

resolve_tagged_release_url() {
    local url="$1"
    local redirect_url

    if release_tag_from_url "$url" >/dev/null; then
        printf '%s\n' "$url"
        return 0
    fi

    # Do not follow this request: GitHub's first redirect names the immutable
    # tag, while the next redirect lands on release-assets without that tag.
    if ! redirect_url="$(curl -fsS -o /dev/null -w '%{redirect_url}' "$url")"; then
        return 1
    fi
    release_tag_from_url "$redirect_url" >/dev/null || return 1
    printf '%s\n' "$redirect_url"
}

install_binary_atomically() {
    local source="$1"
    local name="$2"
    local staged destination

    mkdir -p "$BIN_DIR"
    destination="$BIN_DIR/$name"
    if [ -d "$destination" ] && [ ! -L "$destination" ]; then
        warn "Refusing to replace directory $destination with a binary"
        return 1
    fi
    if ! staged="$(mktemp "$BIN_DIR/.${name}.tmp.XXXXXX")"; then
        warn "Could not create a staging file in $BIN_DIR"
        return 1
    fi
    if ! cp "$source" "$staged" || ! chmod 0755 "$staged"; then
        warn "Could not stage $name in $BIN_DIR"
        rm -f "$staged"
        return 1
    fi
    if [ -d "$destination" ] && [ ! -L "$destination" ]; then
        warn "Refusing to replace directory $destination with a binary"
        rm -f "$staged"
        return 1
    fi
    # GNU `mv -T` and BSD/macOS `mv -h` both force the destination to be
    # treated as a path entry, not as a directory after following a symlink.
    # The final operation is therefore one same-directory rename.
    case "$(uname -s)" in
        Linux)  mv -fT "$staged" "$destination" ;;
        Darwin) mv -fh "$staged" "$destination" ;;
        *)
            warn "Atomic no-follow installation is unsupported on this platform"
            rm -f "$staged"
            return 1
            ;;
    esac || {
        warn "Could not atomically install $destination"
        rm -f "$staged"
        return 1
    }
}

# Download $1 (binary name: "lethe" or "lethe-migrate") for $2 (target
# triple) into $BIN_DIR. Returns non-zero on any failure so callers
# can treat optional binaries gracefully.
download_binary() {
    local name="$1"
    local target="$2"
    local url tmp archive binary package tagged_url release_tag signer_workflow
    url="$RELEASE_BASE_URL/${name}-${target}.tar.gz"
    tmp="$(mktemp -d)"
    archive="$tmp/${name}.tar.gz"

    info "Downloading $name: $url"
    if ! tagged_url="$(resolve_tagged_release_url "$url")"; then
        warn "Could not resolve $url to an immutable tagged release URL"
        rm -rf "$tmp"
        return 1
    fi

    if ! release_tag="$(release_tag_from_url "$tagged_url")"; then
        warn "Could not determine the release tag from $tagged_url"
        rm -rf "$tmp"
        return 1
    fi

    if ! curl -fsSL "$tagged_url" -o "$archive"; then
        warn "Download failed: $tagged_url"
        rm -rf "$tmp"
        return 1
    fi

    case "$name" in
        lethe) signer_workflow="release.yml" ;;
        lethe-migrate) signer_workflow="release-migrator.yml" ;;
        *)
            warn "No trusted release workflow configured for $name"
            rm -rf "$tmp"
            return 1
            ;;
    esac

    if ! verify_release_attestation "$archive" "$signer_workflow" "$release_tag"; then
        rm -rf "$tmp"
        return 1
    fi

    package="${name}-${target}"
    binary="$tmp/$name"
    if ! tar -xOzf "$archive" "$package/$name" > "$binary" || [ ! -s "$binary" ]; then
        warn "Archive did not contain $package/$name"
        rm -rf "$tmp"
        return 1
    fi

    chmod +x "$binary"
    if ! install_binary_atomically "$binary" "$name"; then
        rm -rf "$tmp"
        return 1
    fi
    rm -rf "$tmp"
    success "Installed $BIN_DIR/$name"
    return 0
}

install_release_binaries() {
    local target

    if ! command -v curl >/dev/null 2>&1 || ! command -v tar >/dev/null 2>&1; then
        warn "curl and tar are required for binary install."
        return 1
    fi

    if ! target="$(detect_release_target)"; then
        warn "No binary release target for $(uname -s)/$(uname -m)."
        return 1
    fi

    # `lethe` is required — failure here means we fall back to source.
    download_binary lethe "$target" || return 1

    # `lethe-migrate` is optional — only useful for v0.18→v0.19
    # migration. A missing asset (e.g. older release tag) shouldn't
    # block the install.
    if ! download_binary lethe-migrate "$target"; then
        warn "lethe-migrate not available for this release — only required \
to migrate data from v0.18 or earlier."
    fi
    return 0
}

checkout_repo() {
    local script_dir
    script_dir="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"
    if [ -f "$script_dir/Cargo.toml" ] && grep -q 'name = "lethe"' "$script_dir/Cargo.toml" 2>/dev/null; then
        INSTALL_DIR="$script_dir"
        info "Using local checkout: $INSTALL_DIR"
        return
    fi

    if [ -d "$INSTALL_DIR/.git" ]; then
        info "Updating existing checkout: $INSTALL_DIR"
        git -C "$INSTALL_DIR" pull --ff-only
    else
        info "Cloning Lethe into $INSTALL_DIR"
        mkdir -p "$(dirname "$INSTALL_DIR")"
        git clone "$REPO_URL" "$INSTALL_DIR"
    fi
}

build_from_source() {
    ensure_cargo
    checkout_repo

    info "Building lethe with Cargo..."
    cargo build --release --manifest-path "$INSTALL_DIR/Cargo.toml"
    install_binary_atomically "$INSTALL_DIR/target/release/lethe" lethe \
        || error "Could not install the built lethe binary."
    success "Installed $BIN_DIR/lethe"

    # The migrator is one-shot; only build it if explicitly requested.
    if [ "${LETHE_BUILD_MIGRATOR:-0}" = "1" ]; then
        ensure_protoc
        info "Building lethe-migrate with Cargo..."
        cargo build --release --manifest-path "$INSTALL_DIR/migrator/Cargo.toml"
        install_binary_atomically \
            "$INSTALL_DIR/migrator/target/release/lethe-migrate" lethe-migrate \
            || error "Could not install the built lethe-migrate binary."
        success "Installed $BIN_DIR/lethe-migrate"
    fi
}

# Alien agent-id CLIs: identity + credential vault, and (when Chrome is
# present) the vault-sealed browser. All three come from npm as a matching
# release set — never mix npm packages with tarballs packed from a checkout,
# or the browser can end up importing core files its published dependency
# doesn't have. Best-effort: Lethe works without them.
install_agent_id() {
    if [ "${LETHE_SKIP_AGENT_ID:-0}" = "1" ]; then
        info "LETHE_SKIP_AGENT_ID=1 — skipping Alien agent-id CLIs."
        return
    fi
    if ! command -v npm >/dev/null 2>&1; then
        warn "npm not found — skipping Alien agent-id (identity + vault, optional)."
        warn "Install Node.js, then: npm i -g @alien-id/agent-id-core @alien-id/agent-id-vault"
        return
    fi

    if command -v agent-id-core >/dev/null 2>&1 && command -v agent-id-vault >/dev/null 2>&1; then
        info "Alien agent-id CLIs already on PATH."
    elif npm i -g @alien-id/agent-id-core @alien-id/agent-id-vault; then
        success "Alien agent-id CLIs installed (identity + vault)."
    else
        warn "Could not install the agent-id CLIs — Lethe works without them."
        return
    fi

    # The vault-sealed browser drives real Google Chrome; without Chrome the
    # CLI is dead weight, so only install it when Chrome is on the host.
    if command -v google-chrome-stable >/dev/null 2>&1 || command -v google-chrome >/dev/null 2>&1; then
        if command -v agent-id-browser >/dev/null 2>&1; then
            info "Vault-sealed browser CLI already on PATH."
        elif npm i -g @alien-id/agent-id-browser; then
            success "Vault-sealed browser CLI installed."
        else
            warn "Could not install @alien-id/agent-id-browser — browser tools stay off."
        fi
    else
        info "Google Chrome not found — skipping the vault-sealed browser CLI."
        info "Install Chrome, then: npm i -g @alien-id/agent-id-browser"
    fi
}

# Idempotent: creates the L0 identity + vault when the CLIs are present.
# Covers every path where the init wizard doesn't run (existing config,
# LETHE_SKIP_INIT, no TTY) — `lethe init` provisions on its own.
provision_agent_id() {
    if [ "${LETHE_SKIP_AGENT_ID:-0}" = "1" ]; then
        return
    fi
    LETHE_HOME="$LETHE_HOME" "$BIN_DIR/lethe" agent-id provision || \
        warn "agent-id provisioning failed — rerun with: $BIN_DIR/lethe agent-id provision"
}

run_init_wizard() {
    if [ "${LETHE_SKIP_INIT:-0}" = "1" ]; then
        info "LETHE_SKIP_INIT=1 — skipping setup wizard."
        return
    fi
    if [ -f "$ENV_FILE" ]; then
        info "Existing config at $ENV_FILE — skipping setup wizard."
        info "Rerun '$BIN_DIR/lethe init' anytime to reconfigure."
        return
    fi
    # `lethe init` reads from stdin; under `curl | bash` our stdin is
    # the curl pipe, so redirect explicitly from the controlling TTY.
    if [ ! -e /dev/tty ]; then
        warn "No /dev/tty available — skipping setup wizard."
        warn "Run '$BIN_DIR/lethe init' manually to configure."
        return
    fi
    echo ""
    info "Launching setup wizard: $BIN_DIR/lethe init"
    echo ""
    if ! "$BIN_DIR/lethe" init < /dev/tty; then
        warn "Setup wizard exited with an error."
        warn "You can rerun it anytime: $BIN_DIR/lethe init"
    fi
}

main() {
    print_header

    if [ "${LETHE_INSTALL_FROM_SOURCE:-0}" != "1" ] && install_release_binaries; then
        :
    else
        warn "Falling back to source build."
        build_from_source
    fi

    install_agent_id

    run_init_wizard

    provision_agent_id

    echo ""
    success "Lethe installed."
    echo "  Binary:    $BIN_DIR/lethe"
    if [ -x "$BIN_DIR/lethe-migrate" ]; then
        echo "  Migrator:  $BIN_DIR/lethe-migrate  (run only when moving data from v0.18)"
    fi
    echo "  Config:    $ENV_FILE"
    echo ""
    echo "Next:  $BIN_DIR/lethe status   ·   $BIN_DIR/lethe check"
}

if [[ -z "${BASH_SOURCE[0]:-}" || "${BASH_SOURCE[0]}" == "$0" ]]; then
    main "$@"
fi
