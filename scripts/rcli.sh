# The broker's CLI with the credential kept off the command line (#127). Source me:
#
#   . scripts/rcli.sh
#   rcli XLEN content.fetch.dlq
#
# The client is valkey-cli, the broker's engine's (broker#85), else redis-cli:
# resolved here, once, for rcli and check_redis_floor.sh alike (#256). With
# neither installed the command stays valkey-cli, so the shell's not-found
# names the package to install.
#
# `valkey-cli -u redis://user:pw@host` puts the password in argv, and
# /proc/<pid>/cmdline is readable by every local user while the call runs.
# broker#47 set the rule: REDISCLI_AUTH plus --user, never the password on a
# command line. So the URL's userinfo and query are stripped - a credential in
# either goes to REDISCLI_AUTH and --user - and the rest still goes to `-u`:
# rediss:// (TLS), host, port and db stay the client's own parsing.
#
# Verified against redis-cli 7.0.15 (#127) and valkey-cli 7.2.4 (#256), which
# is why only the userinfo, and all of it, is removed: `-u redis://svc@host`
# sends `svc` as the *password*, and `-u redis://svc:@host` sends an empty one
# that overrides REDISCLI_AUTH. valkey-cli sends AUTH, then the command: no
# HELLO, no CLIENT SETINFO, and no SELECT for db 0 (measured, #256).
#
# The username and password are percent-decoded, as both the client and redis-py
# do. An empty username passes no --user, so the client sends a one-argument
# AUTH - which is what redis-py sends for `redis://:pw@host`, so this client
# and the worker's authenticate identically.
#
# Every step is a builtin: decoding through `printf -v` forks nothing, so the
# password never reaches an argv of its own either.

# _rcli_unquote VAR STRING [+] - percent-decode STRING into VAR; a third
# argument `+` reads `+` as a space first, as a query string does. Only a `%`
# followed by two hex digits is an escape; any other `%` is kept, as urllib's
# unquote keeps it, and nothing else in STRING is interpreted (#127 CR 2).
_rcli_unquote() {
  local s="$2" out="" byte
  [ "${3:-}" = "+" ] && s="${s//+/ }"
  while [[ "${s}" == *%* ]]; do
    out+="${s%%\%*}"
    s="${s#*%}"
    if [[ "${s:0:2}" == [0-9A-Fa-f][0-9A-Fa-f] ]]; then
      printf -v byte "\\x${s:0:2}"
      out+="${byte}"
      s="${s:2}"
    else
      out+="%"
    fi
  done
  printf -v "$1" '%s' "${out}${s}"
}

# rcli_command URL - set RCLI_CMD (the client's argv, credential-free),
# RCLI_AUTH (the password) and RCLI_HAS_AUTH (1 iff there is one). Returns 1,
# building nothing, for a URL with no `scheme://`: there is no telling where its
# credential ends, and the client would refuse it only after starting with it in
# argv (#127 CR 7). The message never quotes the URL.
#
# The query and fragment never reach `-u`: both clients ignore them (verified,
# #127 CR 1, #256), but redis-py reads any query argument as a connection kwarg, so
# `?password=` authenticates the worker and would otherwise sit in argv here.
# A `username` or `password` there fills only what the userinfo left empty,
# which is redis-py's precedence too; a `db` there overrides the path's.
rcli_command() {
  local url="$1" scheme rest query="" authority tail userinfo user="" pass="" db="" pair key
  if command -v valkey-cli >/dev/null 2>&1 || ! command -v redis-cli >/dev/null 2>&1; then
    RCLI_CMD=(valkey-cli)
  else
    RCLI_CMD=(redis-cli)
  fi
  RCLI_AUTH=""
  RCLI_HAS_AUTH=0
  if [[ "${url}" != *://* ]]; then
    echo "rcli: the Redis URL has no scheme (redis:// or rediss://) - refusing to run ${RCLI_CMD[0]}" >&2
    return 1
  fi
  scheme="${url%%://*}"
  rest="${url#*://}"
  rest="${rest%%#*}"
  if [[ "${rest}" == *\?* ]]; then
    query="${rest#*\?}"
    rest="${rest%%\?*}"
  fi
  authority="${rest%%/*}"
  tail="${rest:${#authority}}"
  if [[ "${authority}" == *@* ]]; then
    userinfo="${authority%@*}"
    authority="${authority##*@}"
    _rcli_unquote user "${userinfo%%:*}"
    if [[ "${userinfo}" == *:* ]]; then
      _rcli_unquote pass "${userinfo#*:}"
    fi
  fi
  while [ -n "${query}" ]; do
    pair="${query%%&*}"
    if [[ "${query}" == *\&* ]]; then query="${query#*&}"; else query=""; fi
    [[ "${pair}" == *=* ]] || continue
    key="${pair%%=*}"
    # parse_qs reads `+` as a space before decoding; so does this.
    case "${key}" in
      username) [ -n "${user}" ] || _rcli_unquote user "${pair#*=}" "+" ;;
      password) [ -n "${pass}" ] || _rcli_unquote pass "${pair#*=}" "+" ;;
      db) [ -n "${db}" ] || _rcli_unquote db "${pair#*=}" "+" ;;
    esac
  done
  if [ -n "${user}" ]; then
    RCLI_CMD+=(--user "${user}")
  fi
  RCLI_CMD+=(-u "${scheme}://${authority}${tail}")
  # After -u, so it wins over the path's db - redis-py's precedence (CR 8).
  if [ -n "${db}" ]; then
    RCLI_CMD+=(-n "${db}")
  fi
  if [ -n "${pass}" ]; then
    RCLI_AUTH="${pass}"
    RCLI_HAS_AUTH=1
  fi
}

# rcli_exec CMD... - run CMD with REDISCLI_AUTH set iff the last rcli_command
# found a password, and unset otherwise: the URL is the whole credential, as it
# is for the worker. CMD may wrap the client (`timeout 5 valkey-cli ...`); the
# variable reaches it through the environment, which only its owner can read.
rcli_exec() {
  if [ "${RCLI_HAS_AUTH}" = 1 ]; then
    REDISCLI_AUTH="${RCLI_AUTH}" "$@"
  else
    (unset REDISCLI_AUTH; "$@")
  fi
}

# rcli ARGS... - the client against $REPLICATOR_REDIS_URL. Refuses an unset URL
# rather than defaulting: an operator with no env loaded must not be pointed at
# localhost quietly. The locals keep the password out of the calling shell.
rcli() {
  local RCLI_CMD RCLI_AUTH RCLI_HAS_AUTH
  if [ -z "${REPLICATOR_REDIS_URL:-}" ]; then
    echo "rcli: REPLICATOR_REDIS_URL is unset - load the env first (Common Commands, AGENTS.md)" >&2
    return 2
  fi
  rcli_command "${REPLICATOR_REDIS_URL}" || return 2
  rcli_exec "${RCLI_CMD[@]}" "$@"
}
