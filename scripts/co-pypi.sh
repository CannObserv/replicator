#!/usr/bin/env bash
# Run one uv command, or fetch co-core wheels, with a co-pypi token (#183).
#
#   . scripts/load_env.sh                       # REPLICATOR_CO_PYPI_CREDENTIALS
#   bash scripts/co-pypi.sh uv sync --frozen    # the deploy step
#   bash scripts/co-pypi.sh uv lock
#   bash scripts/co-pypi.sh wheels OLD NEW DIR  # the closure diff's wheels
#
# The token is minted by scripts/co_pypi_token.py and given to the wrapped uv
# alone; it is never printed. The outer `uv run` starts from an allowlisted
# environment, so the writer's GOOGLE_APPLICATION_CREDENTIALS and the repo
# .env's PATs reach neither uv. The two lists match co_pypi_token.py's ALLOWED
# and ALLOWED_PREFIXES, plus the key variable (tests/test_co_pypi_token.py).
# --no-project: the project can't be synced to run this, since that sync is what
# needs the token. Details: docs/DEPENDENCIES.md.
set -euo pipefail

ALLOWED=(PATH HOME USER LOGNAME LANG TERM TMPDIR http_proxy https_proxy no_proxy all_proxy HTTP_PROXY HTTPS_PROXY NO_PROXY ALL_PROXY REPLICATOR_CO_PYPI_CREDENTIALS)
PREFIXES=(LC_ XDG_ SSL_CERT_ UV_)

keep=()
while IFS= read -r name; do
  allowed=0
  for a in "${ALLOWED[@]}"; do [[ $name == "$a" ]] && allowed=1; done
  for p in "${PREFIXES[@]}"; do [[ $name == "$p"* ]] && allowed=1; done
  [[ $allowed == 1 ]] && keep+=("$name=${!name}")
done < <(compgen -e)

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec env -i "${keep[@]}" uv run --no-project --with 'google-auth[requests]>=2,<3' \
  python "$HERE/co_pypi_token.py" "$@"
