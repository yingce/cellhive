#!/bin/sh
# Dispatch a bare service name to its binary, so `command: ["user-runtime"]`
# (Docker replaces CMD, not ENTRYPOINT) actually switches services. Defaults to
# cell-agent when no command is given.
set -e

cmd="${1:-cell-agent}"
if [ "$#" -gt 0 ]; then shift; fi

case "$cmd" in
  /*) exec "$cmd" "$@" ;;
  cell-agent|cellhive|user-runtime|do-runtime|do-supervisor) exec "/usr/local/bin/$cmd" "$@" ;;
  do-runtime-gated) exec /usr/local/bin/do-runtime-gated.sh "$@" ;;
  *) exec "$cmd" "$@" ;;
esac
