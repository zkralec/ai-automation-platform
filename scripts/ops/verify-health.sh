#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE_ARGS=(--env-file "$ROOT_DIR/.env" -f "$ROOT_DIR/docker-compose.yml")

compose() {
  docker compose "${COMPOSE_ARGS[@]}" "$@"
}

echo "== docker compose ps =="
compose ps
echo

echo "== runtime summary =="
compose exec -T api python runtime_healthcheck.py summary --url http://127.0.0.1:8000 --pretty
echo

echo "== heartbeat summary and queue snapshot =="
compose exec -T api python - <<'PY'
import json
from urllib.request import urlopen

BASE_URL = "http://127.0.0.1:8000"

def fetch(path: str):
    with urlopen(f"{BASE_URL}{path}", timeout=5.0) as response:
        return json.loads(response.read().decode("utf-8"))

heartbeat_summary = fetch("/telemetry/heartbeats/summary")
runtime_status = fetch("/telemetry/runtime-status")
payload = {
    "tracked_agents": heartbeat_summary.get("tracked_agents"),
    "stale_agents": heartbeat_summary.get("stale_agents"),
    "queue_depth": runtime_status.get("queue_depth"),
    "redis_reachable": runtime_status.get("redis_reachable"),
    "last_scheduler_tick_at": runtime_status.get("last_scheduler_tick_at"),
}
print(json.dumps(payload, indent=2, sort_keys=True))
PY
