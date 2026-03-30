#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE_ARGS=(--env-file "$ROOT_DIR/.env" -f "$ROOT_DIR/docker-compose.yml")

compose() {
  docker compose "${COMPOSE_ARGS[@]}" "$@"
}

usage() {
  cat <<'EOF'
Usage: scripts/ops/mission-control.sh <command> [service]

Commands:
  start      Build and start the core stack in detached mode
  stop       Stop and remove the stack containers
  restart    Restart the core runtime services
  rebuild    Rebuild images and recreate the stack
  logs       Follow logs for all services or one service
  ps         Show current container status
  health     Run runtime health verification

Optional:
  COMPOSE_PROFILES=ops scripts/ops/mission-control.sh start
    Starts the optional Adminer service too.
EOF
}

command="${1:-}"

case "$command" in
  start)
    compose up -d --build
    ;;
  stop)
    compose down --remove-orphans
    ;;
  restart)
    compose restart api worker scheduler redis postgres
    ;;
  rebuild)
    compose up -d --build --force-recreate
    ;;
  logs)
    service="${2:-}"
    if [[ -n "$service" ]]; then
      compose logs -f --tail=150 "$service"
    else
      compose logs -f --tail=150
    fi
    ;;
  ps)
    compose ps
    ;;
  health)
    "$ROOT_DIR/scripts/ops/verify-health.sh"
    ;;
  ""|-h|--help|help)
    usage
    ;;
  *)
    echo "Unknown command: $command" >&2
    usage >&2
    exit 1
    ;;
esac
