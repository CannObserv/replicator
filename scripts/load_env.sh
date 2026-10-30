# Load the env files into the calling shell by parsing them, never by running
# them (#170). Source me from the repo root, in the same command as whatever
# needs the variables - an agent's tool starts a fresh shell per call:
#
#   . scripts/load_env.sh; . scripts/rcli.sh; rcli PING
#
# Sourcing an env file runs it as shell code, so a `$(...)` or a backtick in a
# value executes, and the repo .env holds org-wide PATs. load_env reads plain
# KEY=value lines (an optional `export `, matched quotes) and exports each one.
# It still exports, so whatever either file holds reaches every child process:
# keep secrets a shell has no use for out of both.
#
# load_env is lifted in place from the vendored recipe in the header of
# skills/shipping-work-python-fastapi/scripts/pre-ship.sh, which stays the only
# copy; init-project-fastapi's template lifts it with the same sed line.
#
# Order: /etc/replicator/.env, then the checkout's .env, so the developer's file
# wins. A linked worktree has no .env (it is git-ignored), so it falls back to
# the primary checkout's, as the recipe does. A missing file loads nothing.
#
# Everything runs inside one function so the caller's shell keeps no name of
# ours but load_env and what the files set. Bash only: it reads BASH_SOURCE.

_load_env_main() {
  local _le_root _le_recipe _le_env PRE_SHIP_ENV_NAMES  # the recipe appends key names to the last
  _le_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd) || return 1
  _le_recipe="$_le_root/skills/shipping-work-python-fastapi/scripts/pre-ship.sh"
  unset -f load_env
  [ -r "$_le_recipe" ] &&
    eval "$(sed -n 's/^#   //; /^load_env() [{]/,/^[}]/p' "$_le_recipe")"
  if ! declare -F load_env >/dev/null; then
    echo "load_env.sh: no load_env recipe in $_le_recipe - run: git submodule update --init" >&2
    return 1
  fi
  _le_env="$_le_root/.env"
  [ -r "$_le_env" ] ||
    _le_env="$(dirname "$(git -C "$_le_root" rev-parse --path-format=absolute --git-common-dir)")/.env"
  load_env /etc/replicator/.env && load_env "$_le_env"
}

if _load_env_main; then
  unset -f _load_env_main
else
  unset -f _load_env_main
  return 1
fi
