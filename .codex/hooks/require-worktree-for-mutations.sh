#!/bin/sh

set -eu

deny() {
  printf '%s\n' \
    "This command may mutate the primary Lethe checkout. Start the task with Worktree selected, use Handoff, or limit the command to a supported read-only inspection." \
    >&2
  exit 2
}

simple_words_only() {
  # Deliberately exclude shell quoting, expansion, and control syntax. These
  # parsers are exceptions for primary-checkout inspection, not a shell lexer.
  printf '%s\n' "$1" \
    | LC_ALL=C grep -Eq '^[A-Za-z0-9_./,:=@%+~?*^-]+([[:space:]]+[A-Za-z0-9_./,:=@%+~?*^-]+)*$'
}

find_is_read_only() {
  command=$1
  simple_words_only "$command" || return 1

  set -f
  # `simple_words_only` makes this split data-only: no quotes, expansions, or
  # shell operators survive to be reinterpreted.
  set -- $command
  [ "${1-}" = "find" ] || return 1
  shift

  expression_started=0
  expected=
  for argument do
    if [ -n "$expected" ]; then
      case "$expected" in
        depth)
          printf '%s\n' "$argument" | LC_ALL=C grep -Eq '^[0-9]+$' || return 1
          ;;
        type)
          printf '%s\n' "$argument" | LC_ALL=C grep -Eq '^[bcdpfls]$' || return 1
          ;;
        value)
          :
          ;;
      esac
      expected=
      continue
    fi

    case "$argument" in
      -H|-L|-P)
        [ "$expression_started" -eq 0 ] || return 1
        ;;
      -maxdepth|-mindepth)
        expression_started=1
        expected=depth
        ;;
      -type|-xtype)
        expression_started=1
        expected=type
        ;;
      -name|-iname|-path|-ipath|-regex|-iregex|-lname|-ilname|\
      -user|-group|-uid|-gid|-size|-perm|-newer|-anewer|-cnewer|\
      -inum|-links|-fstype|-amin|-atime|-cmin|-ctime|-mmin|-mtime|\
      -used|-samefile)
        expression_started=1
        expected=value
        ;;
      -print|-print0|-ls|-prune|-quit|-xdev|-mount|-empty|-readable|\
      -writable|-executable|-true|-false|-depth|-follow|-daystart|\
      -ignore_readdir_race|-noignore_readdir_race|-noleaf)
        expression_started=1
        ;;
      -a|-and|-o|-or|-not)
        expression_started=1
        ;;
      -*)
        # Unknown actions/options fail closed. This includes every file-output,
        # delete, and command action: -fls/-fprint*, -delete, -exec*, and -ok*.
        return 1
        ;;
      *)
        # Search roots must precede the expression.
        [ "$expression_started" -eq 0 ] || return 1
        ;;
    esac
  done

  [ -z "$expected" ]
}

git_show_is_read_only() {
  command=$1
  simple_words_only "$command" || return 1

  set -f
  set -- $command
  [ "${1-}" = "git" ] && [ "${2-}" = "show" ] || return 1
  shift 2

  expected=
  paths_only=0
  for argument do
    if [ -n "$expected" ]; then
      expected=
      continue
    fi
    if [ "$paths_only" -eq 1 ]; then
      continue
    fi

    case "$argument" in
      --)
        paths_only=1
        ;;
      -p|-s|-u|-m|-c|--patch|--no-patch|--raw|--stat|--shortstat|\
      --numstat|--name-only|--name-status|--summary|--check|--binary|\
      --full-index|--abbrev-commit|--oneline|--no-renames|--text|-a|\
      --ignore-space-at-eol|--ignore-space-change|--ignore-all-space|\
      --ignore-blank-lines|--first-parent|--parents|--children|--cc|\
      --combined-all-paths|--no-show-signature|\
      --decorate|--no-decorate|--color|--no-color)
        ;;
      --format|--pretty|--abbrev|--date|--encoding|--src-prefix|\
      --dst-prefix|--line-prefix)
        expected=value
        ;;
      --format=*|--pretty=*|--abbrev=*|--date=*|--encoding=*|\
      --src-prefix=*|--dst-prefix=*|--line-prefix=*|--stat=*|\
      --color=*|-M|-M*|-C|-C*)
        ;;
      -*)
        # In particular, reject --output and any future output/execution
        # option until it is reviewed and added above.
        return 1
        ;;
      *)
        # Object selectors and unambiguous path operands are read-only.
        ;;
    esac
  done

  [ -z "$expected" ]
}

git_inspection_is_read_only() {
  command=$1
  simple_words_only "$command" || return 1

  set -f
  set -- $command
  [ "${1-}" = "git" ] || return 1
  case "${2-}" in
    status|diff|log|rev-parse|ls-files|grep|blame|cat-file|check-ignore|describe)
      ;;
    *)
      return 1
      ;;
  esac
  shift 2

  for argument do
    case "$argument" in
      --out*|--ext*|--textc*|--filter*|--show-sig*|--open*|-O|-O*)
        # These options write files or invoke repository/config-controlled
        # programs. Primary-checkout inspection must remain data-only.
        return 1
        ;;
    esac
  done
}

rg_is_read_only() {
  command=$1
  simple_words_only "$command" || return 1

  set -f
  set -- $command
  [ "${1-}" = "rg" ] || return 1
  shift

  for argument do
    case "$argument" in
      --pre|--pre=*|--hostname-bin|--hostname-bin=*)
        # ripgrep can otherwise execute an arbitrary preprocessor or hostname
        # helper supplied on the command line.
        return 1
        ;;
    esac
  done
}

file_is_read_only() {
  command=$1
  simple_words_only "$command" || return 1

  set -f
  set -- $command
  [ "${1-}" = "file" ] || return 1
  shift

  for argument do
    case "$argument" in
      -C|-C*|--comp*)
        # `file --compile` writes a compiled magic database.
        return 1
        ;;
    esac
  done
}

sed_is_read_only() {
  command=$1
  case "$command" in
    "sed -n "*) rest=${command#"sed -n "} ;;
    *) return 1 ;;
  esac

  case "$rest" in
    \'*)
      quoted=${rest#\'}
      case "$quoted" in *\'*) ;; *) return 1 ;; esac
      expression=${quoted%%\'*}
      files=${quoted#*\'}
      ;;
    \"*)
      quoted=${rest#\"}
      case "$quoted" in *\"*) ;; *) return 1 ;; esac
      expression=${quoted%%\"*}
      files=${quoted#*\"}
      ;;
    *)
      expression=${rest%%[[:space:]]*}
      files=${rest#"$expression"}
      ;;
  esac

  # Only numeric line/range printing is allowed. No -e/-i option, sed `w`/`W`
  # file command, GNU `e` execution command, or external program is expressible.
  printf '%s\n' "$expression" \
    | LC_ALL=C grep -Eq '^[0-9]+(,([0-9]+|[$]))?p$' \
    || return 1
  files=${files#"${files%%[![:space:]]*}"}
  printf '%s\n' "$files" \
    | LC_ALL=C grep -Eq '^[A-Za-z0-9_./~+][A-Za-z0-9_./~+-]*([[:space:]]+[A-Za-z0-9_./~+][A-Za-z0-9_./~+-]*)*$'
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
'*|*'&&'*|*'||'*|*';'*|*'&'*|*'|'*|*'>'*|*'<'*|*'$'*|*'`'*|\
    *'*'*|*'?'*|*'['*|*']'*|*'{'*|*'}'*) return 1 ;;
  esac

  # Trim leading whitespace.
  command=${command#"${command%%[![:space:]]*}"}

  case "$command" in
    pwd|\
    "ls"|"ls "*|\
    "grep"|"grep "*|\
    "head"|"head "*|\
    "tail"|"tail "*|\
    "wc"|"wc "*|\
    "du"|"du "*|\
    "df"|"df "*|\
    "stat"|"stat "*|\
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
    "git branch"|"git branch --list"*|\
    "git remote"|"git remote -v"|"git remote get-url "*|\
    "git worktree list"*|\
    "git tag -l"*|"git tag --list"*|\
    "git config -l"*|"git config --list"*|"git config --get "*|"git config --get-all "*)
      return 0
      ;;
    "git status"*|"git diff"*|"git log"*|"git rev-parse"*|\
    "git ls-files"*|"git grep"*|"git blame"*|"git cat-file"*|\
    "git check-ignore"*|"git describe"*)
      git_inspection_is_read_only "$command"
      ;;
    "git show"|"git show "*)
      git_show_is_read_only "$command"
      ;;
    "rg"|"rg "*)
      rg_is_read_only "$command"
      ;;
    "file"|"file "*)
      file_is_read_only "$command"
      ;;
    "sed -n "*)
      sed_is_read_only "$command"
      ;;
    "find"|"find "*)
      find_is_read_only "$command"
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
