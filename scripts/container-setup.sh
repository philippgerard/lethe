#!/usr/bin/env bash
#
# Lethe Container Setup
#
# Creates an isolated container for Lethe using the platform-native tool:
#   Linux:  podman (rootless OCI container)
#   macOS:  apple/container (requires macOS 26+)
#
# Usage: ./scripts/container-setup.sh [--rebuild]
#

set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

info()    { echo -e "${BLUE}[INFO]${NC} $1"; }
success() { echo -e "${GREEN}[OK]${NC} $1"; }
warn()    { echo -e "${YELLOW}[WARN]${NC} $1"; }
error()   { echo -e "${RED}[ERROR]${NC} $1"; exit 1; }

prompt_read() {
    local prompt="$1"
    local var_name="$2"
    local value
    printf "\e[?2004l" > /dev/tty  # disable bracketed paste
    printf "%s" "$prompt" > /dev/tty
    IFS= read -r value < /dev/tty
    value="$(printf '%s' "$value" | tr -d '\r' | sed 's/[^[:print:]]//g' | xargs)"
    printf -v "$var_name" '%s' "$value"
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"
LETHE_HOME="${LETHE_HOME:-$HOME/.lethe}"
CONTAINER_NAME="lethe"
# Mount policy is host-owned and intentionally outside the broad writable
# LETHE_HOME container mount. A legacy config below LETHE_HOME must be reviewed
# and re-created here rather than silently trusted after container compromise.
HOST_CONTROL_DIR="${LETHE_HOST_CONTROL_DIR:-$HOME/.config/lethe}"
MOUNTS_CONF="${LETHE_MOUNTS_CONF:-$HOST_CONTROL_DIR/container-mounts.conf}"
REBUILD=0

for arg in "$@"; do
    case "$arg" in
        --rebuild) REBUILD=1 ;;
        --help|-h)
            echo "Usage: $0 [--rebuild]"
            echo ""
            echo "Sets up Lethe in an isolated container."
            echo "  --rebuild    Force rebuild even if container exists"
            echo ""
            echo "Linux: uses podman (rootless)"
            echo "macOS: uses apple/container (OCI image)"
            exit 0
            ;;
    esac
done

detect_os() {
    case "$(uname -s)" in
        Linux*)  echo "linux" ;;
        Darwin*) echo "mac" ;;
        *)       echo "unknown" ;;
    esac
}

# Map host arch to OCI arch names. Without this, apple/container has been
# observed to default to linux/amd64 on Apple Silicon, producing an x86_64
# rootfs that runs under emulation. Always pass it explicitly.
detect_arch() {
    case "$(uname -m)" in
        arm64|aarch64) echo "arm64" ;;
        x86_64|amd64)  echo "amd64" ;;
        *)             warn "Unknown host arch '$(uname -m)', defaulting to amd64"; echo "amd64" ;;
    esac
}

ARCH="$(detect_arch)"
PLATFORM="linux/${ARCH}"

# ---------------------------------------------------------------------------
# Directory mount configuration
# ---------------------------------------------------------------------------
canonical_directory() {
    local directory="$1"
    [[ -d "$directory" ]] || return 1
    CDPATH= cd -- "$directory" 2>/dev/null && pwd -P
}

path_is_at_or_below() {
    local path="${1%/}"
    local root="${2%/}"
    [[ -n "$path" ]] || path="/"
    [[ -n "$root" ]] || root="/"
    if [[ "$root" == "/" ]]; then
        [[ "$path" == /* ]]
    else
        [[ "$path" == "$root" || "$path" == "$root/"* ]]
    fi
}

paths_overlap() {
    path_is_at_or_below "$1" "$2" || path_is_at_or_below "$2" "$1"
}

# The mount policy and generated launchers are trusted host control data. They
# must never live below LETHE_HOME, which is writable from the container.
host_control_boundary_is_safe() {
    local mounts_parent lethe_real control_real mounts_parent_real

    case "$LETHE_HOME$HOST_CONTROL_DIR$MOUNTS_CONF" in
        *$'\n'*|*$'\r'*) return 1 ;;
    esac
    [[ ! -L "$MOUNTS_CONF" ]] || return 1
    [[ ! -e "$MOUNTS_CONF" || -f "$MOUNTS_CONF" ]] || return 1

    mounts_parent="$(dirname -- "$MOUNTS_CONF")"
    lethe_real="$(canonical_directory "$LETHE_HOME")" || return 1
    control_real="$(canonical_directory "$HOST_CONTROL_DIR")" || return 1
    mounts_parent_real="$(canonical_directory "$mounts_parent")" || return 1

    [[ "$lethe_real" != "/" ]] || return 1
    path_is_at_or_below "$control_real" "$lethe_real" && return 1
    path_is_at_or_below "$mounts_parent_real" "$lethe_real" && return 1
    return 0
}

host_control_path_is_safe() {
    local path="$1"
    local parent_real control_real

    host_control_boundary_is_safe || return 1
    parent_real="$(canonical_directory "$(dirname -- "$path")")" || return 1
    control_real="$(canonical_directory "$HOST_CONTROL_DIR")" || return 1
    path_is_at_or_below "$parent_real" "$control_real"
}

prompt_mounts() {
    host_control_boundary_is_safe || {
        printf 'Mount policy must be host-owned and outside LETHE_HOME: %s\n' \
            "$MOUNTS_CONF" >&2
        return 1
    }

    echo ""
    echo -e "${YELLOW}Directory access${NC}"
    echo ""
    echo "  Lethe runs in an isolated container. By default it can only"
    echo "  access its own data directory (~/.lethe)."
    echo ""
    echo "  You can give it access to additional directories on your system."
    echo "  These will appear at the same path inside the container"
    echo "  (under /home/lethe/)."
    echo ""

    # Suggest common directories
    local suggested_dirs=()
    for dir in Documents Downloads; do
        if [[ -d "$HOME/$dir" ]]; then
            suggested_dirs+=("$dir")
        fi
    done

    SELECTED_MOUNTS=()

    if [[ ${#suggested_dirs[@]} -gt 0 ]]; then
        echo -e "  ${BLUE}Detected directories:${NC}"
        for dir in "${suggested_dirs[@]}"; do
            prompt_read "    Mount ~/$dir? [Y/n]: " answer
            answer=${answer:-Y}
            if [[ "$answer" =~ ^[Yy] ]]; then
                SELECTED_MOUNTS+=("$dir")
                success "  ~/$dir"
            fi
        done
    fi

    # Custom directories
    echo ""
    echo "  You can also add custom directories (e.g. Projects, Music)."
    echo "  Leave blank to finish."
    echo ""
    while true; do
        prompt_read "  Additional directory (relative to ~, or absolute path): " custom
        custom="${custom%/}"  # strip trailing slash
        [[ -z "$custom" ]] && break

        # Resolve to absolute path
        local abs_path
        if [[ "$custom" = /* ]]; then
            abs_path="$custom"
        else
            abs_path="$HOME/$custom"
        fi

        if [[ ! -d "$abs_path" ]]; then
            warn "  $abs_path does not exist, skipping"
            continue
        fi
        if ! resolve_mount "$custom" >/dev/null; then
            warn "  Unsafe or unsupported mount path, skipping"
            continue
        fi

        SELECTED_MOUNTS+=("$custom")
        success "  $abs_path"
    done

    # Save mount config
    local mounts_dir
    mounts_dir="$(dirname "$MOUNTS_CONF")"
    mkdir -p "$mounts_dir"
    chmod 700 "$mounts_dir" 2>/dev/null || true
    local mounts_tmp="${MOUNTS_CONF}.tmp.$$"
    {
        echo "# Lethe container mount configuration"
        echo "# Format: directory_name (relative to \$HOME, or absolute path)"
        echo "# Edit and re-run container-setup.sh to apply changes."
        for mount in "${SELECTED_MOUNTS[@]}"; do
            echo "$mount"
        done
    } > "$mounts_tmp"
    chmod 600 "$mounts_tmp"
    mv "$mounts_tmp" "$MOUNTS_CONF"

    if [[ ${#SELECTED_MOUNTS[@]} -eq 0 ]]; then
        info "No additional directories mounted (Lethe can only access ~/.lethe)"
    else
        echo ""
        success "${#SELECTED_MOUNTS[@]} directories will be mounted"
    fi
}

load_mounts() {
    SELECTED_MOUNTS=()
    host_control_boundary_is_safe || {
        printf 'Mount policy must be host-owned and outside LETHE_HOME: %s\n' \
            "$MOUNTS_CONF" >&2
        return 1
    }
    if [[ -f "$MOUNTS_CONF" ]]; then
        while IFS= read -r line; do
            [[ -z "$line" || "$line" = \#* ]] && continue
            if ! resolve_mount "$line" >/dev/null; then
                printf 'Unsafe mount entry in %s: %q\n' "$MOUNTS_CONF" "$line" >&2
                return 1
            fi
            SELECTED_MOUNTS+=("$line")
        done < "$MOUNTS_CONF"
    fi
}

mount_entry_is_valid() {
    local entry="$1"
    [[ -n "$entry" ]] || return 1
    [[ "$entry" != " "* && "$entry" != *" " ]] || return 1
    [[ "$entry" != -* ]] || return 1
    [[ "$entry" != "/" ]] || return 1
    [[ "$entry" =~ ^[[:alnum:]\ _./,@%+=~-]+$ ]] || return 1

    local path_body="${entry#/}"
    case "/$path_body/" in
        */../*|*/./*) return 1 ;;
    esac
}

# Resolve a mount entry to host_path:container_path
resolve_mount() {
    local entry="$1"
    local host_candidate host_path container_path

    mount_entry_is_valid "$entry" || return 1
    entry="${entry%/}"

    if [[ "$entry" = /* ]]; then
        host_candidate="$entry"
        container_path="/home/lethe${entry}"
    else
        host_candidate="$HOME/$entry"
        container_path="/home/lethe/$entry"
    fi

    [[ -d "$host_candidate" ]] || return 1
    host_path="$(CDPATH= cd -- "$host_candidate" 2>/dev/null && pwd -P)" || return 1
    host_control_boundary_is_safe || return 1

    local control_real mounts_parent_real
    control_real="$(canonical_directory "$HOST_CONTROL_DIR")" || return 1
    mounts_parent_real="$(canonical_directory "$(dirname -- "$MOUNTS_CONF")")" \
        || return 1
    # Do not grant the container access to any ancestor or descendant of the
    # trusted policy/launcher directories.
    paths_overlap "$host_path" "$control_real" && return 1
    paths_overlap "$host_path" "$mounts_parent_real" && return 1

    # A colon is the runtime volume-field separator and remains ambiguous even
    # when the complete argument is shell-quoted.
    [[ "$host_path" != *:* && "$container_path" != *:* ]] || return 1
    printf '%s:%s\n' "$host_path" "$container_path"
}

# ---------------------------------------------------------------------------
# Stop and remove any existing Lethe services (native or container)
# ---------------------------------------------------------------------------
cleanup_old_services() {
    local found=0

    # Old native systemd user service
    if [[ -f "$HOME/.config/systemd/user/lethe.service" ]]; then
        info "Stopping old native service (systemd user)..."
        systemctl --user stop lethe 2>/dev/null || true
        systemctl --user disable lethe 2>/dev/null || true
        rm -f "$HOME/.config/systemd/user/lethe.service"
        systemctl --user daemon-reload 2>/dev/null || true
        success "Old native service removed"
        found=1
    fi

    # Old native systemd system service
    if [[ -f "/etc/systemd/system/lethe.service" ]]; then
        info "Stopping old native service (systemd system)..."
        sudo systemctl stop lethe 2>/dev/null || true
        sudo systemctl disable lethe 2>/dev/null || true
        sudo rm -f "/etc/systemd/system/lethe.service"
        sudo systemctl daemon-reload 2>/dev/null || true
        success "Old native system service removed"
        found=1
    fi

    # Old nspawn container service
    if [[ -f "/etc/systemd/system/lethe-container.service" ]]; then
        info "Stopping old nspawn container service..."
        sudo systemctl stop lethe-container 2>/dev/null || true
        sudo systemctl disable lethe-container 2>/dev/null || true
        sudo rm -f "/etc/systemd/system/lethe-container.service"
        sudo rm -f "/etc/systemd/nspawn/lethe.nspawn"
        sudo systemctl daemon-reload 2>/dev/null || true
        success "Old nspawn container service removed"
        found=1
    fi

    # Existing podman container service
    if [[ -f "$HOME/.config/systemd/user/lethe-container.service" ]]; then
        info "Stopping existing podman container service..."
        systemctl --user stop lethe-container 2>/dev/null || true
        systemctl --user disable lethe-container 2>/dev/null || true
        rm -f "$HOME/.config/systemd/user/lethe-container.service"
        systemctl --user daemon-reload 2>/dev/null || true
        success "Old podman container service removed"
        found=1
    fi

    # Stop and remove existing podman container
    if command -v podman &>/dev/null && podman container exists "$CONTAINER_NAME" 2>/dev/null; then
        info "Removing existing podman container..."
        podman stop "$CONTAINER_NAME" 2>/dev/null || true
        podman rm "$CONTAINER_NAME" 2>/dev/null || true
        success "Old podman container removed"
        found=1
    fi

    # Old native launchd service
    if [[ -f "$HOME/Library/LaunchAgents/com.lethe.agent.plist" ]]; then
        info "Stopping old native service (launchd)..."
        launchctl unload "$HOME/Library/LaunchAgents/com.lethe.agent.plist" 2>/dev/null || true
        rm -f "$HOME/Library/LaunchAgents/com.lethe.agent.plist"
        success "Old native launchd service removed"
        found=1
    fi

    # Existing container launchd service
    if [[ -f "$HOME/Library/LaunchAgents/com.lethe.container.plist" ]]; then
        info "Stopping existing container service (launchd)..."
        launchctl unload "$HOME/Library/LaunchAgents/com.lethe.container.plist" 2>/dev/null || true
        rm -f "$HOME/Library/LaunchAgents/com.lethe.container.plist"
        success "Old container launchd service removed"
        found=1
    fi

    if [[ "$found" == "1" ]]; then
        echo ""
    fi
}

# ---------------------------------------------------------------------------
# macOS: build OCI image via apple/container
# ---------------------------------------------------------------------------
ensure_container_system() {
    if ! container system status &>/dev/null; then
        info "Starting container system service..."
        container system start
        success "Container system service started"
    fi
}

# apple/container's buildkit VM enables Rosetta by default (build.rosetta=true).
# On arm64 hosts without Rosetta installed, the buildkit VM fails to bootstrap
# with: VZErrorDomain Code=2 "Rosetta is not installed". Detect this and
# disable build.rosetta so the build uses QEMU fallback instead. Native arm64
# builds (the Lethe default) are unaffected.
ensure_builder_rosetta_compatible() {
    [[ "$(uname -m)" == "arm64" ]] || return 0
    # Rosetta present iff a x86_64 binary can be launched via arch(1).
    if arch -arch x86_64 /usr/bin/true >/dev/null 2>&1; then
        return 0
    fi
    local current
    current="$(container system property get build.rosetta 2>/dev/null || echo true)"
    if [[ "$current" == "true" ]]; then
        warn "Rosetta not installed; disabling build.rosetta to avoid buildkit bootstrap failure"
        container system property set build.rosetta false >/dev/null
        # Restart any running builder so the new property takes effect.
        container builder stop >/dev/null 2>&1 || true
    fi
}

build_image_apple() {
    command -v container >/dev/null 2>&1 || error "'container' CLI not found. Install: brew install container (requires macOS 26+)"
    ensure_container_system
    ensure_builder_rosetta_compatible
    info "Building container image (arch: $ARCH)..."
    container build --arch "$ARCH" -t lethe:latest -f "$REPO_DIR/Containerfile" "$REPO_DIR"
    success "Container image built"
}

volume_pair() {
    local host_path="$1"
    local container_path="$2"
    local mode="${3:-}"
    case "$host_path$container_path$mode" in
        *:*)
            # Colons are added structurally below; accepting one in a field
            # would change the runtime's volume grammar.
            return 1
            ;;
        *$'\n'*|*$'\r'*) return 1 ;;
    esac
    if [[ -n "$mode" ]]; then
        printf '%s:%s:%s\n' "$host_path" "$container_path" "$mode"
    else
        printf '%s:%s\n' "$host_path" "$container_path"
    fi
}

write_bash_array_arg() {
    printf '    %q\n' "$1"
}

# Render a host-owned launcher using a Bash argv array. Every path/value is one
# `%q`-encoded array element, so mount data can never become shell source or an
# additional runtime option.
render_container_launcher() {
    local runtime_kind="$1"
    local runtime_bin="$2"
    local launch_script="$3"
    local mount mount_pair
    local mount_pairs=()

    case "$runtime_kind" in
        apple|podman-linux|podman-mac) ;;
        *) return 1 ;;
    esac

    host_control_boundary_is_safe || return 1

    for mount in "${SELECTED_MOUNTS[@]}"; do
        mount_pair="$(resolve_mount "$mount")" || return 1
        mount_pairs+=("$mount_pair")
    done

    local home_pair env_pair
    home_pair="$(volume_pair "$LETHE_HOME" "/home/lethe/.lethe")" || return 1
    env_pair="$(volume_pair "$LETHE_HOME/config/.env" "/opt/lethe/.env" "ro")" \
        || return 1

    mkdir -p "$(dirname "$launch_script")"
    host_control_path_is_safe "$launch_script" || return 1
    local launch_tmp="${launch_script}.tmp.$$"
    {
        echo '#!/usr/bin/env bash'
        echo 'set -euo pipefail'
        printf 'runtime=%q\n' "$runtime_bin"
        case "$runtime_kind" in
            apple)
                echo '"$runtime" system status &>/dev/null || "$runtime" system start'
                ;;
            podman-mac)
                echo '"$runtime" machine inspect --format '\''{{.State}}'\'' 2>/dev/null | grep -qi running || "$runtime" machine start'
                ;;
            podman-linux)
                printf '"$runtime" rm -f %q >/dev/null 2>&1 || true\n' "$CONTAINER_NAME"
                ;;
        esac
        echo 'args=('
        write_bash_array_arg "run"
        case "$runtime_kind" in
            apple)
                write_bash_array_arg "--arch"
                write_bash_array_arg "$ARCH"
                write_bash_array_arg "--memory"
                write_bash_array_arg "4G"
                ;;
            podman-linux|podman-mac)
                write_bash_array_arg "--rm"
                write_bash_array_arg "--name"
                write_bash_array_arg "$CONTAINER_NAME"
                write_bash_array_arg "--userns=keep-id"
                if [[ "$runtime_kind" == "podman-linux" ]]; then
                    write_bash_array_arg "--security-opt"
                    write_bash_array_arg "label=disable"
                fi
                ;;
        esac
        write_bash_array_arg "--env"
        write_bash_array_arg "LETHE_HOME=/home/lethe/.lethe"
        if [[ "$runtime_kind" == "apple" ]]; then
            write_bash_array_arg "--volume"
            write_bash_array_arg "$home_pair"
            write_bash_array_arg "--volume"
            write_bash_array_arg "$env_pair"
            for mount_pair in "${mount_pairs[@]}"; do
                write_bash_array_arg "--volume"
                write_bash_array_arg "$mount_pair"
            done
        else
            write_bash_array_arg "-v"
            write_bash_array_arg "$home_pair"
            write_bash_array_arg "-v"
            write_bash_array_arg "$env_pair"
            for mount_pair in "${mount_pairs[@]}"; do
                write_bash_array_arg "-v"
                write_bash_array_arg "$mount_pair"
            done
        fi
        write_bash_array_arg "lethe:latest"
        echo ')'
        echo 'exec "$runtime" "${args[@]}"'
    } > "$launch_tmp" || {
        rm -f "$launch_tmp"
        return 1
    }
    chmod 700 "$launch_tmp"
    mv "$launch_tmp" "$launch_script"
}

systemd_quote_arg() {
    local value="$1"
    value="${value//\\/\\\\}"
    value="${value//\"/\\\"}"
    value="${value//%/%%}"
    value="${value//\$/\$\$}"
    printf '"%s"' "$value"
}

render_podman_systemd_service() {
    local podman_bin="$1"
    local launch_script="$2"
    local service_file="$3"
    local service_tmp="${service_file}.tmp.$$"

    host_control_path_is_safe "$launch_script" || return 1
    [[ -f "$launch_script" && -x "$launch_script" && ! -L "$launch_script" ]] \
        || return 1

    mkdir -p "$(dirname "$service_file")"
    {
        echo "[Unit]"
        echo "Description=Lethe Autonomous AI Agent (podman)"
        echo "After=network-online.target"
        echo ""
        echo "[Service]"
        echo "Type=simple"
        printf 'ExecStart=%s\n' "$(systemd_quote_arg "$launch_script")"
        printf 'ExecStop=%s stop %s\n' \
            "$(systemd_quote_arg "$podman_bin")" \
            "$(systemd_quote_arg "$CONTAINER_NAME")"
        echo "Restart=always"
        echo "RestartSec=10"
        echo ""
        echo "[Install]"
        echo "WantedBy=default.target"
    } > "$service_tmp" || {
        rm -f "$service_tmp"
        return 1
    }
    chmod 600 "$service_tmp"
    mv "$service_tmp" "$service_file"
}

# ---------------------------------------------------------------------------
# Linux: podman (rootless)
# ---------------------------------------------------------------------------
setup_podman() {
    command -v podman >/dev/null 2>&1 || error "podman not found. Install it with your package manager (e.g. apt install podman, dnf install podman)"

    if podman image exists lethe:latest 2>/dev/null && [[ "$REBUILD" == "0" ]]; then
        info "Image lethe:latest already exists (use --rebuild to recreate)"
    else
        info "Building container image (platform: $PLATFORM)..."
        podman build --platform "$PLATFORM" -t lethe:latest -f "$REPO_DIR/Containerfile" "$REPO_DIR"
        success "Container image built"
    fi

    # Enable lingering so user services run without login session
    if command -v loginctl &>/dev/null; then
        loginctl enable-linger "$(whoami)" 2>/dev/null \
            || sudo loginctl enable-linger "$(whoami)" 2>/dev/null \
            || warn "Could not enable lingering — service may stop when you log out"
    fi

    # Create systemd user service
    local podman_bin
    podman_bin="$(command -v podman)"
    mkdir -p "$HOME/.config/systemd/user"

    local launch_script="$HOST_CONTROL_DIR/run-container.sh"
    render_container_launcher "podman-linux" "$podman_bin" "$launch_script" \
        || error "Could not render a safe Podman launcher"

    local svc="$HOME/.config/systemd/user/lethe-container.service"
    render_podman_systemd_service "$podman_bin" "$launch_script" "$svc" \
        || error "Could not render the Podman systemd service"

    systemctl --user daemon-reload
    systemctl --user enable lethe-container
    systemctl --user start lethe-container
    success "Podman container started"
    echo ""
    echo "  Image:     lethe:latest"
    echo "  Container: $CONTAINER_NAME"
    echo "  Launcher:  $launch_script"
    echo "  Service:   ~/.config/systemd/user/lethe-container.service"
    echo ""
    echo "  Commands:"
    echo "    Start:   systemctl --user start lethe-container"
    echo "    Stop:    systemctl --user stop lethe-container"
    echo "    Logs:    journalctl --user -u lethe-container -f"
    echo "    Shell:   podman exec -it $CONTAINER_NAME /bin/bash"
    echo "    Root:    podman exec -u 0 -it $CONTAINER_NAME /bin/bash"
}

# ---------------------------------------------------------------------------
# macOS: apple/container
# ---------------------------------------------------------------------------
setup_apple_container() {
    local container_bin
    container_bin="$(command -v container)"

    if container image ls 2>/dev/null | grep -q "lethe" && [[ "$REBUILD" == "0" ]]; then
        info "Image lethe:latest already exists (use --rebuild to recreate)"
    else
        build_image_apple
    fi

    # Create a host-owned argv-safe launch script.
    local launch_script="$HOST_CONTROL_DIR/run-container.sh"
    render_container_launcher "apple" "$container_bin" "$launch_script" \
        || error "Could not render a safe Apple Container launcher"

    _write_launchd_plist "$launch_script" "$(dirname "$container_bin")"

    launchctl load "$HOME/Library/LaunchAgents/com.lethe.container.plist"
    success "apple/container started"
    echo ""
    echo "  Launcher:  $launch_script"
    echo "  Service:   ~/Library/LaunchAgents/com.lethe.container.plist"
    echo ""
    echo "  Commands:"
    echo "    Start:   launchctl load ~/Library/LaunchAgents/com.lethe.container.plist"
    echo "    Stop:    launchctl unload ~/Library/LaunchAgents/com.lethe.container.plist"
    echo "    Logs:    tail -f $LETHE_HOME/logs/container.log"
    echo "    Shell:   container run --arch $ARCH --volume $LETHE_HOME:/home/lethe/.lethe -it lethe:latest /bin/bash"
}

# ---------------------------------------------------------------------------
# macOS: podman (fallback for Intel Macs / pre-Sequoia)
# ---------------------------------------------------------------------------
setup_podman_mac() {
    command -v podman >/dev/null 2>&1 || error "podman not found. Install: brew install podman"
    local podman_bin
    podman_bin="$(command -v podman)"

    # Ensure podman machine is initialized and running
    if ! podman machine inspect &>/dev/null; then
        info "Initializing podman machine..."
        podman machine init --cpus 2 --memory 4096
    fi
    if ! podman machine inspect --format '{{.State}}' 2>/dev/null | grep -qi "running"; then
        info "Starting podman machine..."
        podman machine start
    fi
    success "Podman machine running"

    if podman image exists lethe:latest 2>/dev/null && [[ "$REBUILD" == "0" ]]; then
        info "Image lethe:latest already exists (use --rebuild to recreate)"
    else
        info "Building container image (platform: $PLATFORM)..."
        podman build --platform "$PLATFORM" -t lethe:latest -f "$REPO_DIR/Containerfile" "$REPO_DIR"
        success "Container image built"
    fi

    # Create a host-owned argv-safe launch script.
    local launch_script="$HOST_CONTROL_DIR/run-container.sh"
    render_container_launcher "podman-mac" "$podman_bin" "$launch_script" \
        || error "Could not render a safe Podman launcher"

    _write_launchd_plist "$launch_script" "$(dirname "$podman_bin")"

    launchctl load "$HOME/Library/LaunchAgents/com.lethe.container.plist"
    success "Podman container started"
    echo ""
    echo "  Launcher:  $launch_script"
    echo "  Service:   ~/Library/LaunchAgents/com.lethe.container.plist"
    echo ""
    echo "  Commands:"
    echo "    Start:   launchctl load ~/Library/LaunchAgents/com.lethe.container.plist"
    echo "    Stop:    launchctl unload ~/Library/LaunchAgents/com.lethe.container.plist"
    echo "    Logs:    tail -f $LETHE_HOME/logs/container.log"
    echo "    Shell:   podman exec -it lethe /bin/bash"
    echo "    Root:    podman exec -u 0 -it lethe /bin/bash"
}

# ---------------------------------------------------------------------------
# Shared: write launchd plist for macOS container service
# ---------------------------------------------------------------------------
_write_launchd_plist() {
    local launch_script="$1"
    local bin_dir="$2"

    mkdir -p "$HOME/Library/LaunchAgents"
    mkdir -p "$LETHE_HOME/logs"
    cat > "$HOME/Library/LaunchAgents/com.lethe.container.plist" << EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.lethe.container</string>
    <key>ProgramArguments</key>
    <array>
        <string>$launch_script</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>StandardOutPath</key>
    <string>$LETHE_HOME/logs/container.log</string>
    <key>StandardErrorPath</key>
    <string>$LETHE_HOME/logs/container.error.log</string>
    <key>EnvironmentVariables</key>
    <dict>
        <key>PATH</key>
        <string>$bin_dir:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin</string>
    </dict>
</dict>
</plist>
EOF
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
main() {
    echo -e "${BLUE}Lethe Container Setup${NC}"
    echo ""

    # Establish and validate the host/container trust boundary before writing
    # policy or executable launchers.
    mkdir -p "$LETHE_HOME" "$HOST_CONTROL_DIR" "$(dirname -- "$MOUNTS_CONF")"
    host_control_boundary_is_safe \
        || error "Mount policy and launcher directory must be outside LETHE_HOME"
    chmod 700 "$HOST_CONTROL_DIR" "$(dirname -- "$MOUNTS_CONF")" 2>/dev/null || true

    # Ensure lethe home and dirs exist
    mkdir -p "$LETHE_HOME"/{config,data,logs,workspace,cache,credentials}

    # Mount configuration
    if [[ -f "$MOUNTS_CONF" && "$REBUILD" == "0" ]]; then
        info "Using existing mount config: $MOUNTS_CONF"
        load_mounts || error "Mount policy contains an unsafe or invalid path"
        if [[ ${#SELECTED_MOUNTS[@]} -gt 0 ]]; then
            echo "  Mounted directories:"
            for mount in "${SELECTED_MOUNTS[@]}"; do
                local pair
                pair=$(resolve_mount "$mount")
                echo "    ${pair//:/ → }"
            done
        fi
        echo ""
        prompt_read "Reconfigure mounts? [y/N]: " reconf
        if [[ "$reconf" =~ ^[Yy] ]]; then
            prompt_mounts
        fi
    else
        prompt_mounts
    fi

    # Stop and remove any previous services before installing new one
    cleanup_old_services

    # Platform-specific setup
    local os=$(detect_os)
    case "$os" in
        linux)
            info "Platform: Linux (podman)"
            setup_podman
            ;;
        mac)
            if command -v container >/dev/null 2>&1; then
                info "Platform: macOS (apple/container)"
                setup_apple_container
            elif command -v podman >/dev/null 2>&1; then
                info "Platform: macOS (podman)"
                setup_podman_mac
            else
                error "No container runtime found. Install apple/container (macOS 26+: brew install container) or podman (brew install podman)"
            fi
            ;;
        *)
            error "Unsupported platform: $(uname -s)"
            ;;
    esac

    echo ""
    success "Container setup complete"
    echo ""
    echo "  Lethe home:  $LETHE_HOME → /home/lethe/.lethe"
    if [[ ${#SELECTED_MOUNTS[@]} -gt 0 ]]; then
        echo "  Directories:"
        for mount in "${SELECTED_MOUNTS[@]}"; do
            local pair
            pair=$(resolve_mount "$mount")
            echo "    ${pair//:/ → }"
        done
    fi
    echo "  Mounts config: $MOUNTS_CONF"
    echo ""
    echo "  Host filesystem is isolated except for the directories above."
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
    main "$@"
fi
