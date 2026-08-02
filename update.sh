#!/usr/bin/env bash
#
# Lethe Rust updater.
# Usage: curl -fsSL https://lethe.gg/update | bash

set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

LETHE_HOME="${LETHE_HOME:-$HOME/.lethe}"
INSTALL_DIR="${LETHE_INSTALL_DIR:-$LETHE_HOME/install}"
BIN_DIR="$LETHE_HOME/bin"
REPO_OWNER="${LETHE_REPO_OWNER:-alien-id}"
REPO_NAME="${LETHE_REPO_NAME:-lethe}"
RELEASE_BASE_URL="${LETHE_RELEASE_BASE_URL:-https://github.com/$REPO_OWNER/$REPO_NAME/releases/latest/download}"

info() { echo -e "${BLUE}[INFO]${NC} $1"; }
success() { echo -e "${GREEN}[OK]${NC} $1"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
error() { echo -e "${RED}[ERROR]${NC} $1"; exit 1; }

# Container-first deployments run their own copy of the binary baked into
# the image, so updating the host binary doesn't touch the running
# container. If one is present, tell the user how to roll the new version in.
post_update_notice() {
    local has_container=0
    if [ -f "$HOME/.config/systemd/user/lethe-container.service" ] \
       || [ -f "$HOME/Library/LaunchAgents/com.lethe.container.plist" ]; then
        has_container=1
    elif command -v podman >/dev/null 2>&1 && podman container exists lethe 2>/dev/null; then
        has_container=1
    fi
    if [ "$has_container" = "1" ]; then
        echo ""
        warn "A container deployment is still running the previous version."
        warn "Roll the update into it with:  $BIN_DIR/lethe container up --rebuild"
    fi
}

detect_release_target() {
    local os arch
    os="$(uname -s)"
    arch="$(uname -m)"
    case "$os:$arch" in
        Linux:x86_64|Linux:amd64) echo "x86_64-unknown-linux-gnu" ;;
        Linux:aarch64|Linux:arm64) echo "aarch64-unknown-linux-gnu" ;;
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
    local release_tag="$2"

    if ! command -v gh >/dev/null 2>&1; then
        warn "GitHub CLI (gh) is required to verify binary release provenance."
        return 1
    fi

    if ! gh attestation verify "$archive" \
        --repo "$REPO_OWNER/$REPO_NAME" \
        --signer-workflow "$REPO_OWNER/$REPO_NAME/.github/workflows/release.yml" \
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

install_release_binary() {
    local target url tmp archive binary package tagged_url release_tag

    if ! command -v curl >/dev/null 2>&1 || ! command -v tar >/dev/null 2>&1; then
        warn "curl and tar are required for binary update."
        return 1
    fi

    if ! target="$(detect_release_target)"; then
        warn "No binary release target for $(uname -s)/$(uname -m)."
        return 1
    fi

    url="$RELEASE_BASE_URL/lethe-$target.tar.gz"
    tmp="$(mktemp -d)"
    archive="$tmp/lethe.tar.gz"

    info "Downloading binary release: $url"
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
        warn "Binary release download failed: $tagged_url"
        rm -rf "$tmp"
        return 1
    fi

    if ! verify_release_attestation "$archive" "$release_tag"; then
        rm -rf "$tmp"
        return 1
    fi

    package="lethe-$target"
    binary="$tmp/lethe"
    if ! tar -xOzf "$archive" "$package/lethe" > "$binary" || [ ! -s "$binary" ]; then
        warn "Binary release archive did not contain $package/lethe."
        rm -rf "$tmp"
        return 1
    fi

    chmod +x "$binary"
    if ! install_binary_atomically "$binary" lethe; then
        rm -rf "$tmp"
        return 1
    fi
    rm -rf "$tmp"
    success "Updated $BIN_DIR/lethe from binary release"
}

main() {
    if [ "${LETHE_UPDATE_FROM_SOURCE:-0}" != "1" ] && install_release_binary; then
        post_update_notice
        return 0
    fi

    warn "Falling back to source update."

    if ! command -v cargo >/dev/null 2>&1; then
        error "Cargo is required for source update. Install Rust from https://rustup.rs."
    fi

    if ! command -v protoc >/dev/null 2>&1; then
        error "protoc is required by LanceDB source builds. Install protobuf-compiler/libprotobuf-dev (Debian/Ubuntu), protobuf-compiler/protobuf-devel (Fedora), or protobuf (Homebrew)."
    fi

    if [ ! -d "$INSTALL_DIR/.git" ]; then
        error "No Lethe checkout found at $INSTALL_DIR. Set LETHE_INSTALL_DIR or rerun install.sh."
    fi

    info "Updating checkout: $INSTALL_DIR"
    git -C "$INSTALL_DIR" pull --ff-only

    info "Building release binary..."
    cargo build --release --manifest-path "$INSTALL_DIR/Cargo.toml"

    install_binary_atomically "$INSTALL_DIR/target/release/lethe" lethe \
        || error "Could not install the built lethe binary."

    success "Updated $BIN_DIR/lethe"
    post_update_notice
}

if [[ -z "${BASH_SOURCE[0]:-}" || "${BASH_SOURCE[0]}" == "$0" ]]; then
    main "$@"
fi
