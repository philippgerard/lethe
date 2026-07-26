#!/bin/sh

set -eu

deny() {
  printf '%s\n' \
    "This command may mutate the primary Lethe checkout. Start the task with Worktree selected, use Handoff, or limit the command to a supported read-only inspection." \
    >&2
  exit 2
}

git_dir=$(git rev-parse --absolute-git-dir 2>/dev/null) || deny
common_dir=$(git rev-parse --path-format=absolute --git-common-dir 2>/dev/null) || deny

# Linked worktrees are the intended write environment.
if [ "$git_dir" != "$common_dir" ]; then
  exit 0
fi

command_is_read_only() {
  command=$1

  # Keep the primary-checkout exception intentionally simple and auditable.
  # Chained commands, substitutions, pipes, and redirects must use a worktree.
  case "$command" in
    *'
'*|*'&&'*|*'||'*|*';'*|*'|'*|*'>'*|*'<'*|*'$('*|*'`'*) return 1 ;;
  esac

  # Trim leading whitespace.
  command=${command#"${command%%[![:space:]]*}"}

  case "$command" in
    pwd|\
    "ls"|"ls "*|\
    "rg"|"rg "*|\
    "grep"|"grep "*|\
    "head"|"head "*|\
    "tail"|"tail "*|\
    "wc"|"wc "*|\
    "du"|"du "*|\
    "df"|"df "*|\
    "stat"|"stat "*|\
    "file"|"file "*|\
    "readlink"|"readlink "*|\
    "realpath"|"realpath "*|\
    "jq"|"jq "*|\
    "command -v "*|\
    "which "*|\
    "type "*|\
    "rustc --version"*|\
    "cargo --version"*|\
    "cargo metadata"*|\
    "cargo tree"*|\
    "cargo pkgid"*|\
    "cargo locate-project"*|\
    "cargo fmt --check"*|\
    "rustup show"*|\
    "git status"*|\
    "git diff"*|\
    "git log"*|\
    "git show"*|\
    "git rev-parse"*|\
    "git ls-files"*|\
    "git grep"*|\
    "git blame"*|\
    "git cat-file"*|\
    "git check-ignore"*|\
    "git describe"*|\
    "git branch"|"git branch --list"*|\
    "git remote"|"git remote -v"|"git remote get-url "*|\
    "git worktree list"*|\
    "git tag -l"*|"git tag --list"*|\
    "git config -l"*|"git config --list"*|"git config --get "*|"git config --get-all "*)
      return 0
      ;;
    "sed -n "*)
      return 0
      ;;
    "find"|"find "*)
      case "$command" in
        *" -delete"*|*" -exec"*|*" -execdir"*|*" -ok"*|*" -okdir"*|*" -fprint"*|*" -fprintf"*)
          return 1
          ;;
        *)
          return 0
          ;;
      esac
      ;;
    *)
      return 1
      ;;
  esac
}

command -v jq >/dev/null 2>&1 || deny
input=$(cat)
tool=$(printf '%s' "$input" | jq -r '.tool_name // ""' 2>/dev/null) || deny

case "$tool" in
  apply_patch|Edit|Write)
    deny
    ;;
  Bash)
    shell_command=$(printf '%s' "$input" | jq -r '.tool_input.command // .tool_input.cmd // ""' 2>/dev/null) || deny
    command_is_read_only "$shell_command" || deny
    ;;
  *)
    deny
    ;;
esac

exit 0
