#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)"
HOOK="$REPO_ROOT/.codex/hooks/require-worktree-for-mutations.sh"

tests_run=0

pass() {
    tests_run=$((tests_run + 1))
    printf 'ok %d - %s\n' "$tests_run" "$1"
}

fail() {
    printf 'not ok - %s\n' "$1" >&2
    exit 1
}

assert_eq() {
    local expected="$1" actual="$2" label="$3"
    [[ "$actual" == "$expected" ]] || fail "$label (expected '$expected', got '$actual')"
    pass "$label"
}

assert_file_absent() {
    local path="$1" label="$2"
    [[ ! -e "$path" ]] || fail "$label (unexpected file: $path)"
    pass "$label"
}

command -v jq >/dev/null 2>&1 || fail "jq is required by the worktree hook"

TEST_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/lethe-security-policy.XXXXXX")"
trap 'rm -rf "$TEST_ROOT"' EXIT

# ---------------------------------------------------------------------------
# Primary-checkout hook: exercise the actual hook from a temporary primary repo.
# ---------------------------------------------------------------------------
PRIMARY_REPO="$TEST_ROOT/primary"
mkdir -p "$PRIMARY_REPO"
git init -q "$PRIMARY_REPO"

hook_status() {
    local shell_command="$1"
    local status
    set +e
    (
        cd "$PRIMARY_REPO"
        jq -cn --arg command "$shell_command" \
            '{tool_name:"Bash",tool_input:{command:$command}}' \
            | sh "$HOOK"
    ) >/dev/null 2>&1
    status=$?
    set -e
    printf '%s\n' "$status"
}

assert_hook_denies() {
    local label="$1" shell_command="$2"
    assert_eq "2" "$(hook_status "$shell_command")" "$label"
}

assert_hook_allows() {
    local label="$1" shell_command="$2"
    assert_eq "0" "$(hook_status "$shell_command")" "$label"
}

find_out="$TEST_ROOT/find.out"
git_out="$TEST_ROOT/git.out"
sed_out="$TEST_ROOT/sed.out"
amp_out="$TEST_ROOT/amp.out"
diff_out="$TEST_ROOT/diff.out"
log_out="$TEST_ROOT/log.out"
tree_out="$TEST_ROOT/tree.out"

assert_hook_denies "find -fls is rejected" "find . -fls $find_out"
assert_hook_denies "compact find -fls is rejected" "find . -fls$find_out"
assert_file_absent "$find_out" "rejected find leaves no output file"
assert_hook_allows "read-only find predicates remain accepted" \
    "find . -maxdepth 2 -type f -print"

assert_hook_denies "git show --output= is rejected" \
    "git show --output=$git_out HEAD"
assert_hook_denies "git show --output argument is rejected" \
    "git show --output $git_out HEAD"
assert_file_absent "$git_out" "rejected git show leaves no output file"
assert_hook_allows "read-only git show remains accepted" "git show --stat HEAD"
assert_hook_denies "git diff --output= is rejected" \
    "git diff --output=$diff_out HEAD"
assert_hook_denies "git log --output argument is rejected" \
    "git log --output $log_out HEAD"
assert_hook_denies "git external diff execution is rejected" \
    "git diff --ext-diff HEAD"
assert_hook_denies "git textconv execution is rejected" \
    "git diff --textconv HEAD"
assert_hook_denies "git filter execution is rejected" \
    "git cat-file --filters HEAD:fixture.txt"
assert_hook_denies "git grep pager execution is rejected" \
    "git grep --open-files-in-pager=touch needle"
assert_hook_denies "abbreviated git grep pager execution is rejected" \
    "git grep --open-files-in-pag=touch needle"
assert_hook_denies "git signature helper execution is rejected" \
    "git log --show-signature HEAD"
assert_hook_denies "escaped git output option is rejected" \
    "git diff --out\\put=$diff_out HEAD"
assert_file_absent "$diff_out" "rejected git diff leaves no output file"
assert_file_absent "$log_out" "rejected git log leaves no output file"
assert_hook_allows "read-only git diff remains accepted" "git diff --stat HEAD"
assert_hook_allows "read-only git log remains accepted" \
    "git log --format=oneline HEAD"

assert_hook_denies "ripgrep preprocessor execution is rejected" \
    "rg --pre=/tmp/unsafe needle ."
assert_hook_denies "ripgrep hostname helper execution is rejected" \
    "rg --hostname-bin=/tmp/unsafe needle ."
assert_hook_allows "read-only ripgrep remains accepted" "rg -n needle ."

assert_hook_denies "file magic compilation is rejected" "file -C fixture.magic"
assert_hook_denies "compact file magic compilation is rejected" "file -Ck fixture.magic"
assert_hook_denies "abbreviated file magic compilation is rejected" \
    "file --comp fixture.magic"
assert_hook_allows "read-only file inspection remains accepted" "file fixture.txt"
assert_hook_denies "tree output option is rejected" "tree -o $tree_out ."
assert_file_absent "$tree_out" "rejected tree leaves no output file"

assert_hook_denies "parameter expansion is rejected" "git status \$UNSAFE"
assert_hook_denies "pathname expansion is rejected" "git diff *"

assert_hook_denies "sed w command is rejected" \
    "sed -n '1w $sed_out' fixture.txt"
assert_hook_denies "sed e command is rejected" \
    "sed -n '1e touch $sed_out' fixture.txt"
assert_hook_denies "sed in-place editing is rejected" \
    "sed -i 1s/a/b/ fixture.txt"
assert_hook_denies "sed option after script is rejected" \
    "sed -n '1p' -i fixture.txt"
assert_file_absent "$sed_out" "rejected sed leaves no output file"
assert_hook_allows "numeric sed printing remains accepted" \
    "sed -n '1,20p' fixture.txt"

assert_hook_denies "single ampersand command chaining is rejected" \
    "git status & touch $amp_out"
assert_file_absent "$amp_out" "rejected command chain leaves no output file"

# ---------------------------------------------------------------------------
# Container mount policy and generated launchers.
# ---------------------------------------------------------------------------
export LETHE_HOME="$TEST_ROOT/lethe home"
export LETHE_HOST_CONTROL_DIR="$TEST_ROOT/host control"
unset LETHE_MOUNTS_CONF || true
mkdir -p "$LETHE_HOME/config" "$LETHE_HOST_CONTROL_DIR"

# shellcheck source=../container-setup.sh
source "$REPO_ROOT/scripts/container-setup.sh"

if grep -Eq '^[[:space:]]*eval[[:space:]]' "$REPO_ROOT/scripts/container-setup.sh"; then
    fail "container setup must not evaluate prompt or mount data"
fi
pass "container setup never evaluates prompt or mount data"

assert_eq "$LETHE_HOST_CONTROL_DIR/container-mounts.conf" "$MOUNTS_CONF" \
    "mount policy defaults outside writable LETHE_HOME"
host_control_boundary_is_safe || fail "host control boundary should be valid"
pass "host control boundary is valid"

valid_mount="$TEST_ROOT/Project Files"
mkdir -p "$valid_mount"
canonical_mount="$(CDPATH= cd -- "$valid_mount" && pwd -P)"
expected_pair="$canonical_mount:/home/lethe$valid_mount"
assert_eq "$expected_pair" "$(resolve_mount "$valid_mount")" \
    "mount path with spaces resolves as one pair"

assert_mount_rejected() {
    local label="$1" entry="$2"
    if resolve_mount "$entry" >/dev/null 2>&1; then
        fail "$label"
    fi
    pass "$label"
}

quote_mount="$TEST_ROOT/bad\"mount"
substitution_mount="$TEST_ROOT/\$(touch injected)"
colon_mount="$TEST_ROOT/bad:ro"
newline_mount="$valid_mount"$'\n'"--privileged"
mkdir -p "$quote_mount" "$substitution_mount" "$colon_mount" \
    "$TEST_ROOT/escape" "$newline_mount"

assert_mount_rejected "mount quote injection is rejected" \
    "$quote_mount"
assert_mount_rejected "mount command substitution is rejected" \
    "$substitution_mount"
assert_mount_rejected "mount option separator is rejected" \
    "$colon_mount"
assert_mount_rejected "mount traversal is rejected" \
    "$valid_mount/../escape"
assert_mount_rejected "option-shaped mount is rejected" "--privileged"
assert_mount_rejected "newline mount record is rejected" \
    "$newline_mount"
assert_mount_rejected "mount containing host controls is rejected" "$TEST_ROOT"

printf '# approved mounts\n%s\n' "$valid_mount" > "$MOUNTS_CONF"
load_mounts || fail "valid host mount policy should load"
assert_eq "1" "${#SELECTED_MOUNTS[@]}" "valid host mount policy is retained"
printf '%s\n' '--privileged' > "$MOUNTS_CONF"
if load_mounts >/dev/null 2>&1; then
    fail "unsafe host mount policy must fail closed"
fi
pass "unsafe host mount policy fails closed"

SELECTED_MOUNTS=("$valid_mount")
FAKE_RUNTIME="$TEST_ROOT/fake runtime"
cat > "$FAKE_RUNTIME" <<'FAKE'
#!/usr/bin/env bash
set -euo pipefail
case "${1-}" in
    system|machine|rm) exit 0 ;;
esac
: "${CAPTURE_FILE:?}"
printf '%s\0' "$@" > "$CAPTURE_FILE"
FAKE
chmod 700 "$FAKE_RUNTIME"

read_captured_args() {
    local capture_file="$1" argument
    CAPTURED_ARGS=()
    while IFS= read -r -d '' argument; do
        CAPTURED_ARGS+=("$argument")
    done < "$capture_file"
}

assert_captured_volume() {
    local flag="$1" pair="$2" label="$3"
    local index
    for ((index = 0; index + 1 < ${#CAPTURED_ARGS[@]}; index++)); do
        if [[ "${CAPTURED_ARGS[$index]}" == "$flag" \
            && "${CAPTURED_ARGS[$((index + 1))]}" == "$pair" ]]; then
            pass "$label"
            return 0
        fi
    done
    fail "$label"
}

for runtime_kind in apple podman-mac podman-linux; do
    launcher="$LETHE_HOST_CONTROL_DIR/$runtime_kind launcher.sh"
    capture="$TEST_ROOT/$runtime_kind.args"
    render_container_launcher "$runtime_kind" "$FAKE_RUNTIME" "$launcher" \
        || fail "$runtime_kind launcher should render"
    bash -n "$launcher"
    CAPTURE_FILE="$capture" "$launcher"
    read_captured_args "$capture"
    if [[ "$runtime_kind" == "apple" ]]; then
        volume_flag="--volume"
    else
        volume_flag="-v"
    fi
    assert_captured_volume "$volume_flag" "$expected_pair" \
        "$runtime_kind receives the spaced mount as one literal argument"
done

service_file="$TEST_ROOT/lethe-container.service"
render_podman_systemd_service \
    "$FAKE_RUNTIME" \
    "$LETHE_HOST_CONTROL_DIR/podman-linux launcher.sh" \
    "$service_file"
if grep -F -- "$valid_mount" "$service_file" >/dev/null; then
    fail "systemd service must not interpolate mount data"
fi
pass "systemd service delegates to the argv-safe launcher"

malicious_launcher="$TEST_ROOT/malicious-launcher.sh"
SELECTED_MOUNTS=("$substitution_mount")
if render_container_launcher apple "$FAKE_RUNTIME" "$malicious_launcher" >/dev/null 2>&1; then
    fail "unsafe mount must stop launcher rendering"
fi
assert_file_absent "$malicious_launcher" "unsafe mount creates no launcher"

saved_mounts_conf="$MOUNTS_CONF"
MOUNTS_CONF="$LETHE_HOME/config/mounts.conf"
if host_control_boundary_is_safe; then
    fail "mount policy inside LETHE_HOME must be rejected"
fi
pass "mount policy inside LETHE_HOME is rejected"
MOUNTS_CONF="$saved_mounts_conf"

# ---------------------------------------------------------------------------
# Foreground dotenv loader: run the real wrapper with a fake local binary.
# ---------------------------------------------------------------------------
FAKE_LETHE="$TEST_ROOT/fake-lethe"
cat > "$FAKE_LETHE" <<'FAKE'
#!/usr/bin/env bash
set -euo pipefail
: "${FOREGROUND_CAPTURE:?}"
{
    printf 'SAFE_VALUE=%s\n' "${SAFE_VALUE-}"
    printf 'QUOTED_VALUE=%s\n' "${QUOTED_VALUE-}"
    printf 'URL_VALUE=%s\n' "${URL_VALUE-}"
    printf 'ARGS='
    printf '<%s>' "$@"
    printf '\n'
} > "$FOREGROUND_CAPTURE"
FAKE
chmod 700 "$FAKE_LETHE"

valid_env="$LETHE_HOST_CONTROL_DIR/foreground.env"
rm -f "$valid_env"
foreground_capture="$TEST_ROOT/foreground.capture"
FOREGROUND_CAPTURE="$foreground_capture" \
LETHE_BIN="$FAKE_LETHE" \
"$REPO_ROOT/scripts/lethe-telegram-foreground" >/dev/null
[[ -f "$valid_env" && ! -L "$valid_env" ]] \
    || fail "foreground wrapper must create one regular default control env"
[[ ! -s "$valid_env" ]] || fail "default foreground control env must start empty"
if stat -f '%Lp' "$valid_env" >/dev/null 2>&1; then
    env_mode="$(stat -f '%Lp' "$valid_env")"
else
    env_mode="$(stat -c '%a' "$valid_env")"
fi
assert_eq "600" "$env_mode" "foreground wrapper creates a private default control env"

cat > "$valid_env" <<'ENV'
# strict dotenv control
SAFE_VALUE=value with spaces
QUOTED_VALUE="literal quoted value"
URL_VALUE=https://example.test/?a=1&b=two=2
ENV
FOREGROUND_CAPTURE="$foreground_capture" \
LETHE_BIN="$FAKE_LETHE" \
"$REPO_ROOT/scripts/lethe-telegram-foreground" --control >/dev/null

pass "foreground wrapper defaults to the host-only control env"

grep -Fx 'SAFE_VALUE=value with spaces' "$foreground_capture" >/dev/null \
    || fail "strict dotenv preserves spaces"
pass "strict dotenv preserves spaces"
grep -Fx 'QUOTED_VALUE=literal quoted value' "$foreground_capture" >/dev/null \
    || fail "strict dotenv preserves quoted literals"
pass "strict dotenv preserves quoted literals"
grep -Fx 'URL_VALUE=https://example.test/?a=1&b=two=2' "$foreground_capture" >/dev/null \
    || fail "strict dotenv preserves URL and equals characters"
pass "strict dotenv preserves URL and equals characters"
grep -Fx 'ARGS=<telegram><run><--control>' "$foreground_capture" >/dev/null \
    || fail "foreground wrapper preserves binary arguments"
pass "foreground wrapper preserves binary arguments"

assert_bad_env_rejected() {
    local label="$1" payload="$2"
    local env_file="$LETHE_HOST_CONTROL_DIR/bad.env" canary="$TEST_ROOT/dotenv-canary"
    rm -f "$canary" "$foreground_capture"
    printf '%s\n' "$payload" > "$env_file"
    if FOREGROUND_CAPTURE="$foreground_capture" \
        LETHE_ENV_FILE="$env_file" \
        LETHE_BIN="$FAKE_LETHE" \
        "$REPO_ROOT/scripts/lethe-telegram-foreground" >/dev/null 2>&1; then
        fail "$label"
    fi
    [[ ! -e "$canary" ]] || fail "$label (canary executed)"
    [[ ! -e "$foreground_capture" ]] || fail "$label (binary unexpectedly started)"
    pass "$label"
}

canary="$TEST_ROOT/dotenv-canary"
assert_bad_env_rejected "dotenv command substitution is rejected without execution" \
    "EVIL=\$(touch $canary)"
assert_bad_env_rejected "dotenv semicolon command is rejected without execution" \
    "EVIL=ok; touch $canary"
assert_bad_env_rejected "dotenv function syntax is rejected without execution" \
    "EVIL_FN() { touch $canary; }"
assert_bad_env_rejected "dotenv backtick command is rejected without execution" \
    "EVIL=\`touch $canary\`"
assert_bad_env_rejected "dotenv process substitution is rejected without execution" \
    "EVIL=<(touch $canary)"
assert_bad_env_rejected "dotenv process-control key is rejected" \
    "BASH_ENV=$canary"
assert_bad_env_rejected "dotenv runtime-loader key is rejected" \
    "LD_PRELOAD=$canary"
assert_bad_env_rejected "dotenv binary override is rejected" \
    "LETHE_BIN=$canary"
assert_bad_env_rejected "dotenv injected newline is rejected without execution" \
    $'SAFE_VALUE=ok\ntouch '"$canary"

inside_env="$LETHE_HOME/config/foreground.env"
printf 'SAFE_VALUE=inside\n' > "$inside_env"
if FOREGROUND_CAPTURE="$foreground_capture" \
    LETHE_ENV_FILE="$inside_env" \
    LETHE_BIN="$FAKE_LETHE" \
    "$REPO_ROOT/scripts/lethe-telegram-foreground" >/dev/null 2>&1; then
    fail "foreground env inside LETHE_HOME must be rejected"
fi
pass "foreground env inside LETHE_HOME is rejected"

outside_env="$TEST_ROOT/outside-control.env"
printf 'SAFE_VALUE=outside\n' > "$outside_env"
if FOREGROUND_CAPTURE="$foreground_capture" \
    LETHE_ENV_FILE="$outside_env" \
    LETHE_BIN="$FAKE_LETHE" \
    "$REPO_ROOT/scripts/lethe-telegram-foreground" >/dev/null 2>&1; then
    fail "foreground env outside host control must be rejected"
fi
pass "foreground env outside host control is rejected"

symlink_env="$LETHE_HOST_CONTROL_DIR/foreground-link.env"
ln -s "$valid_env" "$symlink_env"
if FOREGROUND_CAPTURE="$foreground_capture" \
    LETHE_ENV_FILE="$symlink_env" \
    LETHE_BIN="$FAKE_LETHE" \
    "$REPO_ROOT/scripts/lethe-telegram-foreground" >/dev/null 2>&1; then
    fail "foreground env symlink must be rejected"
fi
pass "foreground env symlink is rejected"

printf '1..%d\n' "$tests_run"
