#!/usr/bin/env bash
# Run / manage the kovanica-node explorer (TCP P2P + HTTP UI).
#
# Usage:
#   ./kovanica-node.sh build                 # rebuild release binary
#   ./kovanica-node.sh start   <peers>       # kill old, start with KOVANICA_PEERS=<peers>
#   ./kovanica-node.sh restart <peers>       # same as start
#   ./kovanica-node.sh stop                 # kill the running node
#   ./kovanica-node.sh status               # show listeners + recent log
#
# <peers> is a comma-separated list, e.g. 100.77.175.85:9000
# Env overrides: KOVANICA_DATA, KOVANICA_LISTEN, KOVANICA_HTTP, KOVANICA_POW

set -euo pipefail

CMD="${1:-start}"
PEERS="${2:-}"

DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$DIR"

BIN="$DIR/target/release/kovanica-node"
LOG="$DIR/explorer.log"
HTTP="${KOVANICA_HTTP:-0.0.0.0:8080}"
LISTEN="${KOVANICA_LISTEN:-0.0.0.0:9000}"
DATA="${KOVANICA_DATA:-./data}"

run() { echo "== $* =="; "$@"; }

case "$CMD" in
  build)
    run cargo build --release -p kovanica-node
    ;;

  stop)
    pkill -f 'target/release/kovanica-node' || true
    echo "stopped"
    ;;

  status)
    ss -ltnp 2>/dev/null | grep -E ':9000|:8080' || echo "not listening on 9000/8080"
    echo "--- recent log ---"
    tail -n 5 "$LOG" 2>/dev/null || true
    ;;

  start|restart)
    pkill -f 'target/release/kovanica-node' || true
    sleep 1
    if [ -z "$PEERS" ]; then
      echo "usage: $0 start|restart <peers>" >&2
      exit 1
    fi
    export KOVANICA_DATA="$DATA"
    export KOVANICA_LISTEN="$LISTEN"
    export KOVANICA_PEERS="$PEERS"
    export KOVANICA_POW="${KOVANICA_POW:-1}"
    echo "starting with KOVANICA_PEERS=$PEERS"
    # shellcheck disable=SC2024
    nohup "$BIN" explorer "$HTTP" > "$LOG" 2>&1 &
    echo "started pid $!"
    sleep 2
    ss -ltnp 2>/dev/null | grep -E ':9000|:8080' || echo "WARN: not listening"
    echo "--- recent log ---"
    tail -n 8 "$LOG" 2>/dev/null || true
    ;;

  *)
    echo "usage: $0 {start|stop|restart|status|build} [peers]" >&2
    exit 1
    ;;
esac
