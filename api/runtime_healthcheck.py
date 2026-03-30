"""Small runtime health checks for container probes and operator verification."""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

from agent_heartbeats import get_agent_heartbeat


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _parse_iso_datetime(raw: str | None) -> datetime | None:
    if not raw:
        return None
    candidate = raw.strip()
    if not candidate:
        return None
    if candidate.endswith("Z"):
        candidate = candidate[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _fetch_json(url: str, timeout: float) -> tuple[int | None, Any]:
    try:
        with urlopen(url, timeout=timeout) as response:
            payload = response.read().decode("utf-8")
            return int(response.status), json.loads(payload)
    except HTTPError as exc:
        payload = exc.read().decode("utf-8") if exc.fp is not None else ""
        try:
            data = json.loads(payload) if payload else {"error": str(exc)}
        except json.JSONDecodeError:
            data = {"error": payload or str(exc)}
        return int(exc.code), data
    except URLError as exc:
        return None, {"error": str(exc.reason)}
    except Exception as exc:  # pragma: no cover - defensive
        return None, {"error": str(exc)}


def _print(payload: dict[str, Any], pretty: bool) -> None:
    if pretty:
        print(json.dumps(payload, indent=2, sort_keys=True, default=str))
    else:
        print(json.dumps(payload, separators=(",", ":"), default=str))


def _heartbeat_status(agent_name: str, stale_after_seconds: int) -> dict[str, Any]:
    row = get_agent_heartbeat(agent_name)
    if row is None:
        return {
            "agent_name": agent_name,
            "healthy": False,
            "status": "missing",
            "age_seconds": None,
            "last_seen_at": None,
            "message": "No heartbeat record found.",
        }

    seen_at = _parse_iso_datetime(str(row.get("last_seen_at") or ""))
    age_seconds = None if seen_at is None else max(int((_now_utc() - seen_at).total_seconds()), 0)
    healthy = age_seconds is not None and age_seconds <= stale_after_seconds
    status = str(row.get("status") or ("alive" if healthy else "stale"))
    return {
        "agent_name": agent_name,
        "healthy": healthy,
        "status": status,
        "age_seconds": age_seconds,
        "last_seen_at": row.get("last_seen_at"),
        "message": "Heartbeat is recent." if healthy else f"Heartbeat age exceeds {stale_after_seconds}s.",
    }


def _scheduler_tick_health(runtime_status: dict[str, Any], tick_stale_after_seconds: int) -> dict[str, Any]:
    last_tick_raw = runtime_status.get("last_scheduler_tick_at")
    last_tick = _parse_iso_datetime(str(last_tick_raw or ""))
    age_seconds = None if last_tick is None else max(int((_now_utc() - last_tick).total_seconds()), 0)
    healthy = age_seconds is not None and age_seconds <= tick_stale_after_seconds
    return {
        "healthy": healthy,
        "age_seconds": age_seconds,
        "last_scheduler_tick_at": last_tick_raw,
        "message": (
            "Recent scheduler tick found."
            if healthy
            else f"Scheduler tick age exceeds {tick_stale_after_seconds}s."
        ),
    }


def command_api(args: argparse.Namespace) -> int:
    health_code, health_payload = _fetch_json(f"{args.url.rstrip('/')}/health", args.timeout)
    ready_code, ready_payload = _fetch_json(f"{args.url.rstrip('/')}/ready", args.timeout)
    payload = {
        "health_status_code": health_code,
        "health": health_payload,
        "ready_status_code": ready_code,
        "ready": ready_payload,
    }
    _print(payload, args.pretty)
    ok = (
        health_code == 200
        and isinstance(health_payload, dict)
        and health_payload.get("status") == "ok"
        and ready_code == 200
        and isinstance(ready_payload, dict)
        and ready_payload.get("status") == "ready"
    )
    return 0 if ok else 1


def command_heartbeat(args: argparse.Namespace) -> int:
    payload = _heartbeat_status(args.agent, args.stale_after)
    _print(payload, args.pretty)
    return 0 if payload.get("healthy") else 1


def command_summary(args: argparse.Namespace) -> int:
    base_url = args.url.rstrip("/")
    health_code, health_payload = _fetch_json(f"{base_url}/health", args.timeout)
    ready_code, ready_payload = _fetch_json(f"{base_url}/ready", args.timeout)
    runtime_code, runtime_payload = _fetch_json(f"{base_url}/telemetry/runtime-status", args.timeout)

    scheduler_interval = max(int(os.getenv("SCHEDULER_INTERVAL_SEC", "60")), 1)
    tick_stale_after_seconds = max(args.stale_after, scheduler_interval * 3)
    scheduler_tick = (
        _scheduler_tick_health(runtime_payload, tick_stale_after_seconds)
        if runtime_code == 200 and isinstance(runtime_payload, dict)
        else {
            "healthy": False,
            "age_seconds": None,
            "last_scheduler_tick_at": None,
            "message": "Runtime status unavailable.",
        }
    )
    payload = {
        "health": {
            "status_code": health_code,
            "payload": health_payload,
        },
        "ready": {
            "status_code": ready_code,
            "payload": ready_payload,
        },
        "runtime_status": runtime_payload,
        "redis_reachable": bool(isinstance(runtime_payload, dict) and runtime_payload.get("redis_reachable")),
        "queue_depth": runtime_payload.get("queue_depth") if isinstance(runtime_payload, dict) else None,
        "worker_heartbeat": runtime_payload.get("worker_heartbeat") if isinstance(runtime_payload, dict) else None,
        "scheduler_heartbeat": runtime_payload.get("scheduler_heartbeat") if isinstance(runtime_payload, dict) else None,
        "scheduler_tick": scheduler_tick,
    }
    payload["overall_healthy"] = bool(
        health_code == 200
        and ready_code == 200
        and runtime_code == 200
        and payload["redis_reachable"]
        and isinstance(payload["worker_heartbeat"], dict)
        and payload["worker_heartbeat"].get("healthy")
        and isinstance(payload["scheduler_heartbeat"], dict)
        and payload["scheduler_heartbeat"].get("healthy")
        and scheduler_tick.get("healthy")
    )
    _print(payload, args.pretty)
    return 0 if payload["overall_healthy"] else 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    api_parser = subparsers.add_parser("api", help="Check API health and readiness endpoints.")
    api_parser.add_argument("--url", default="http://127.0.0.1:8000")
    api_parser.add_argument("--timeout", type=float, default=5.0)
    api_parser.add_argument("--pretty", action="store_true")
    api_parser.set_defaults(func=command_api)

    heartbeat_parser = subparsers.add_parser("heartbeat", help="Check worker or scheduler heartbeat freshness.")
    heartbeat_parser.add_argument("--agent", required=True)
    heartbeat_parser.add_argument(
        "--stale-after",
        type=int,
        default=max(int(os.getenv("WATCHDOG_STALE_AFTER_SEC", "240")), 1),
    )
    heartbeat_parser.add_argument("--pretty", action="store_true")
    heartbeat_parser.set_defaults(func=command_heartbeat)

    summary_parser = subparsers.add_parser("summary", help="Check API, Redis, queue, heartbeats, and scheduler tick.")
    summary_parser.add_argument("--url", default="http://127.0.0.1:8000")
    summary_parser.add_argument("--timeout", type=float, default=5.0)
    summary_parser.add_argument(
        "--stale-after",
        type=int,
        default=max(int(os.getenv("WATCHDOG_STALE_AFTER_SEC", "240")), 1),
    )
    summary_parser.add_argument("--pretty", action="store_true")
    summary_parser.set_defaults(func=command_summary)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return int(args.func(args))


if __name__ == "__main__":
    sys.exit(main())
