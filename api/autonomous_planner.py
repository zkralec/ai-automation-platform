"""Safe-mode autonomous planner for recommendation and optional execution."""

from __future__ import annotations

import json
import os
import re
import uuid
from dataclasses import asdict, dataclass, fields
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import func

from ai_usage_log import get_ai_usage_summary
from event_log import log_event as persist_event_log
from system_metrics import get_latest_system_metrics
try:
    from operational_day import current_operational_day_window_utc
except ModuleNotFoundError:  # pragma: no cover - package import fallback
    from api.operational_day import current_operational_day_window_utc


class DecisionType:
    EXECUTE_TASK = "execute_task"
    CREATE_TASK = "create_task"
    DEFER = "defer"
    ALERT = "alert"


@dataclass
class PlannerPolicy:
    max_tasks_created_per_cycle: int
    max_tasks_executed_per_cycle: int
    max_pending_tasks: int
    failure_lookback_minutes: int
    failure_alert_count_threshold: int
    failure_alert_rate_threshold: float
    stale_task_age_seconds: int
    execute_task_cooldown_seconds: int
    health_cpu_max_percent: float
    health_memory_max_percent: float
    health_disk_max_percent: float
    cost_budget_usd: float | None
    token_budget: int | None
    create_task_type: str | None
    create_payload_json: str | None
    create_task_model: str | None
    create_task_max_attempts: int
    create_task_cooldown_seconds: int

    @classmethod
    def from_env(cls) -> "PlannerPolicy":
        daily_budget_raw = os.getenv("DAILY_BUDGET_USD", "").strip()
        planner_budget_raw = os.getenv("AUTONOMOUS_PLANNER_COST_BUDGET_USD", "").strip()
        if planner_budget_raw:
            try:
                cost_budget_usd = float(planner_budget_raw)
            except ValueError:
                cost_budget_usd = None
        elif daily_budget_raw:
            try:
                cost_budget_usd = float(daily_budget_raw)
            except ValueError:
                cost_budget_usd = None
        else:
            cost_budget_usd = None

        token_budget_raw = os.getenv("AUTONOMOUS_PLANNER_TOKEN_BUDGET", "").strip()
        if token_budget_raw:
            try:
                token_budget = int(token_budget_raw)
            except ValueError:
                token_budget = None
        else:
            token_budget = None

        create_task_type = os.getenv("AUTONOMOUS_PLANNER_CREATE_TASK_TYPE", "").strip() or None
        create_payload_json = os.getenv("AUTONOMOUS_PLANNER_CREATE_PAYLOAD_JSON", "").strip() or None
        create_task_model = os.getenv("AUTONOMOUS_PLANNER_CREATE_TASK_MODEL", "").strip() or None

        return cls(
            max_tasks_created_per_cycle=max(
                int(os.getenv("AUTONOMOUS_PLANNER_MAX_CREATE_PER_CYCLE", "1")),
                0,
            ),
            max_tasks_executed_per_cycle=max(
                int(os.getenv("AUTONOMOUS_PLANNER_MAX_EXECUTE_PER_CYCLE", "2")),
                0,
            ),
            max_pending_tasks=max(int(os.getenv("AUTONOMOUS_PLANNER_MAX_PENDING_TASKS", "20")), 1),
            failure_lookback_minutes=max(
                int(os.getenv("AUTONOMOUS_PLANNER_FAILURE_LOOKBACK_MINUTES", "60")),
                1,
            ),
            failure_alert_count_threshold=max(
                int(os.getenv("AUTONOMOUS_PLANNER_FAILURE_ALERT_COUNT_THRESHOLD", "5")),
                1,
            ),
            failure_alert_rate_threshold=max(
                float(os.getenv("AUTONOMOUS_PLANNER_FAILURE_ALERT_RATE_THRESHOLD", "0.5")),
                0.0,
            ),
            stale_task_age_seconds=max(
                int(os.getenv("AUTONOMOUS_PLANNER_STALE_TASK_AGE_SECONDS", "180")),
                30,
            ),
            execute_task_cooldown_seconds=max(
                int(os.getenv("AUTONOMOUS_PLANNER_EXECUTE_TASK_COOLDOWN_SECONDS", "600")),
                30,
            ),
            health_cpu_max_percent=float(os.getenv("AUTONOMOUS_PLANNER_HEALTH_CPU_MAX_PERCENT", "90")),
            health_memory_max_percent=float(os.getenv("AUTONOMOUS_PLANNER_HEALTH_MEMORY_MAX_PERCENT", "90")),
            health_disk_max_percent=float(os.getenv("AUTONOMOUS_PLANNER_HEALTH_DISK_MAX_PERCENT", "95")),
            cost_budget_usd=cost_budget_usd,
            token_budget=token_budget,
            create_task_type=create_task_type,
            create_payload_json=create_payload_json,
            create_task_model=create_task_model,
            create_task_max_attempts=max(
                int(os.getenv("AUTONOMOUS_PLANNER_CREATE_TASK_MAX_ATTEMPTS", "3")),
                1,
            ),
            create_task_cooldown_seconds=max(
                int(os.getenv("AUTONOMOUS_PLANNER_CREATE_TASK_COOLDOWN_SECONDS", "1800")),
                60,
            ),
        )


def _to_iso(ts: datetime) -> str:
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.isoformat()


def _to_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_payload_json(payload_json: str | None) -> str:
    if not payload_json:
        return "{}"
    try:
        parsed = json.loads(payload_json)
    except json.JSONDecodeError:
        return "{}"
    return json.dumps(parsed, separators=(",", ":"), ensure_ascii=True, sort_keys=True)


def _safe_log_event(event_type: str, *, level: str, message: str, metadata_json: dict[str, Any]) -> None:
    try:
        persist_event_log(
            event_type=event_type,
            source="autonomous_planner",
            level=level,
            message=message,
            metadata_json=metadata_json,
        )
    except Exception:
        return


_LAST_EXECUTED_TASK_AT: dict[str, datetime] = {}
_PLACEHOLDER_RE = re.compile(r"\{\{([a-zA-Z0-9_]+)\}\}")


def _replace_placeholders(value: str, context: dict[str, str]) -> str:
    def _repl(match: re.Match[str]) -> str:
        key = match.group(1).strip().lower()
        return context.get(key, match.group(0))

    return _PLACEHOLDER_RE.sub(_repl, value)


def _materialize_payload_json(
    payload_json: str,
    *,
    now: datetime,
    task_type: str,
    template_id: str | None = None,
    template_name: str | None = None,
    template_metadata: dict[str, Any] | None = None,
) -> str:
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    generated_at = now.astimezone(timezone.utc)
    ts_compact = generated_at.strftime("%Y%m%dT%H%M%SZ")
    unix_ts = str(int(generated_at.timestamp()))
    generation_id = str(uuid.uuid4())
    context = {
        "now_iso": _to_iso(generated_at),
        "date_utc": generated_at.strftime("%Y-%m-%d"),
        "time_utc": generated_at.strftime("%H:%M:%S"),
        "unix_ts": unix_ts,
        "ts_compact": ts_compact,
        "uuid4": generation_id,
    }

    try:
        parsed = json.loads(payload_json)
    except json.JSONDecodeError:
        parsed = {}

    if not isinstance(parsed, dict):
        parsed = {}

    def _walk(value: Any) -> Any:
        if isinstance(value, str):
            return _replace_placeholders(value, context)
        if isinstance(value, list):
            return [_walk(item) for item in value]
        if isinstance(value, dict):
            return {str(key): _walk(val) for key, val in value.items()}
        return value

    payload_obj = _walk(parsed)
    if not isinstance(payload_obj, dict):
        payload_obj = {}

    if task_type == "deals_scan_v1":
        source_base = str(payload_obj.get("source") or "autonomous-planner-deals").strip()
        payload_obj["source"] = f"{source_base}-{ts_compact}"

    if task_type in {"jobs_collect_v1", "jobs_digest_v1"}:
        request_obj = payload_obj.get("request") if isinstance(payload_obj.get("request"), dict) else payload_obj
        if not isinstance(request_obj, dict):
            request_obj = {}
        request_obj.setdefault("collectors_enabled", True)

        titles = request_obj.get("desired_title_keywords")
        if isinstance(titles, list):
            options = [str(item).strip() for item in titles if isinstance(item, str) and str(item).strip()]
            if options:
                try:
                    window_sec = max(int(request_obj.get("query_rotation_window_seconds") or 300), 60)
                except (TypeError, ValueError):
                    window_sec = 300
                idx = (int(generated_at.timestamp()) // window_sec) % len(options)
                request_obj["query"] = options[idx]

        # Backward compatibility for legacy jobs payload templates.
        if task_type == "jobs_digest_v1":
            request_obj.setdefault("sources", request_obj.get("job_boards") or ["linkedin", "indeed", "glassdoor", "handshake"])
            request_obj.setdefault("query", request_obj.get("search_query") or request_obj.get("query") or "software engineer")
            request_obj.setdefault("location", request_obj.get("search_location") or request_obj.get("location") or "United States")

        if task_type == "jobs_collect_v1":
            payload_obj["request"] = request_obj

    payload_obj["planner_generated_at"] = context["now_iso"]
    payload_obj["planner_generation_id"] = generation_id
    if template_id:
        payload_obj["planner_template_id"] = template_id
    if template_name:
        payload_obj["planner_template_name"] = template_name
    if isinstance(template_metadata, dict):
        strategy = template_metadata.get("payload_strategy")
        if isinstance(strategy, str) and strategy.strip():
            payload_obj["planner_payload_strategy"] = strategy.strip()

    return json.dumps(payload_obj, separators=(",", ":"), ensure_ascii=True, sort_keys=True)


def _runtime() -> dict[str, Any]:
    # Lazy import keeps planning functions usable even when the full API runtime
    # dependency set is unavailable (for dry verification scripts).
    from main import (
        ENQUEUE_RECOVERY_DELAY_SEC,
        Run,
        RunStatus,
        SessionLocal,
        Task,
        TaskStatus,
        choose_model,
        queue,
        schedule_enqueue_recovery,
    )

    return {
        "ENQUEUE_RECOVERY_DELAY_SEC": ENQUEUE_RECOVERY_DELAY_SEC,
        "Run": Run,
        "RunStatus": RunStatus,
        "SessionLocal": SessionLocal,
        "Task": Task,
        "TaskStatus": TaskStatus,
        "choose_model": choose_model,
        "queue": queue,
        "schedule_enqueue_recovery": schedule_enqueue_recovery,
    }


def _normalize_create_task_specs(
    raw_specs: list[dict[str, Any]] | None,
    policy: PlannerPolicy,
) -> list[dict[str, Any]]:
    if not raw_specs:
        return []

    normalized: list[dict[str, Any]] = []
    for raw in raw_specs:
        if not isinstance(raw, dict):
            continue

        task_type = str(raw.get("task_type") or "").strip()
        payload_json = _normalize_payload_json(str(raw.get("payload_json") or ""))
        if not task_type:
            continue

        model = raw.get("model")
        if not isinstance(model, str) or not model.strip():
            model = None

        try:
            max_attempts = max(int(raw.get("max_attempts") or policy.create_task_max_attempts), 1)
        except (TypeError, ValueError):
            max_attempts = policy.create_task_max_attempts

        try:
            min_interval_seconds = max(
                int(raw.get("min_interval_seconds") or policy.create_task_cooldown_seconds),
                60,
            )
        except (TypeError, ValueError):
            min_interval_seconds = policy.create_task_cooldown_seconds

        try:
            priority = int(raw.get("priority") or 100)
        except (TypeError, ValueError):
            priority = 100

        normalized.append(
            {
                "template_id": raw.get("id"),
                "name": str(raw.get("name") or "").strip() or task_type,
                "task_type": task_type,
                "payload_json": payload_json,
                "model": model,
                "max_attempts": max_attempts,
                "min_interval_seconds": min_interval_seconds,
                "priority": priority,
                "metadata_json": raw.get("metadata_json") if isinstance(raw.get("metadata_json"), dict) else None,
            }
        )

    normalized.sort(key=lambda row: (int(row.get("priority") or 100), str(row.get("name") or "")))
    return normalized


def _apply_policy_overrides(policy: PlannerPolicy, overrides: dict[str, Any] | None) -> PlannerPolicy:
    if not overrides:
        return policy

    allowed = {item.name for item in fields(policy)}
    for key in allowed:
        if key in overrides and overrides[key] is not None:
            setattr(policy, key, overrides[key])
    return policy


def collect_planner_state(
    now: datetime,
    policy: PlannerPolicy,
    *,
    create_task_specs: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)

    lookback_start = now - timedelta(minutes=policy.failure_lookback_minutes)
    day_start, day_end = current_operational_day_window_utc(now)

    runtime = _runtime()
    SessionLocal = runtime["SessionLocal"]
    Task = runtime["Task"]
    TaskStatus = runtime["TaskStatus"]
    Run = runtime["Run"]
    RunStatus = runtime["RunStatus"]

    specs = _normalize_create_task_specs(create_task_specs, policy)

    with SessionLocal() as db:
        pending_count = (
            db.query(func.count(Task.id))
            .filter(Task.status == TaskStatus.queued)
            .scalar()
            or 0
        )
        running_count = (
            db.query(func.count(Task.id))
            .filter(Task.status == TaskStatus.running)
            .scalar()
            or 0
        )

        recent_total_runs = (
            db.query(func.count(Run.id))
            .filter(Run.started_at.is_not(None), Run.started_at >= lookback_start)
            .scalar()
            or 0
        )
        recent_failed_runs = (
            db.query(func.count(Run.id))
            .filter(
                Run.started_at.is_not(None),
                Run.started_at >= lookback_start,
                Run.status == RunStatus.failed,
            )
            .scalar()
            or 0
        )

        stale_cutoff = now - timedelta(seconds=policy.stale_task_age_seconds)
        stale_candidates = (
            db.query(Task)
            .filter(
                Task.status == TaskStatus.queued,
                Task.created_at <= stale_cutoff,
                Task.next_run_at.is_(None),
            )
            .order_by(Task.created_at.asc())
            .limit(max(policy.max_tasks_executed_per_cycle * 3, 10))
            .all()
        )

        execute_candidates: list[dict[str, Any]] = []
        for task in stale_candidates:
            attempts_used = (
                db.query(func.count(Run.id))
                .filter(Run.task_id == task.id)
                .scalar()
                or 0
            )
            if attempts_used >= int(task.max_attempts or 1):
                continue
            execute_candidates.append(
                {
                    "task_id": task.id,
                    "task_type": task.task_type,
                    "created_at": _to_iso(task.created_at),
                    "attempts_used": int(attempts_used),
                    "max_attempts": int(task.max_attempts or 1),
                }
            )

        create_candidates: list[dict[str, Any]] = []
        for spec in specs:
            task_type = str(spec.get("task_type") or "")
            if not task_type:
                continue

            queued_or_running_for_type = (
                db.query(func.count(Task.id))
                .filter(
                    Task.task_type == task_type,
                    Task.status.in_((TaskStatus.queued, TaskStatus.running)),
                )
                .scalar()
                or 0
            )
            if queued_or_running_for_type > 0:
                continue

            latest_started_at = (
                db.query(Run.started_at)
                .join(Task, Task.id == Run.task_id)
                .filter(
                    Task.task_type == task_type,
                    Run.started_at.is_not(None),
                )
                .order_by(Run.started_at.desc())
                .limit(1)
                .scalar()
            )
            latest_created_at = (
                db.query(Task.created_at)
                .filter(Task.task_type == task_type)
                .order_by(Task.created_at.desc())
                .limit(1)
                .scalar()
            )

            latest_activity = latest_started_at or latest_created_at
            min_interval_seconds = int(spec.get("min_interval_seconds") or policy.create_task_cooldown_seconds)
            if latest_activity is not None:
                if latest_activity.tzinfo is None:
                    latest_activity = latest_activity.replace(tzinfo=timezone.utc)
                age_seconds = int((now - latest_activity).total_seconds())
                if age_seconds < min_interval_seconds:
                    continue
            else:
                age_seconds = None

            create_candidates.append(
                {
                    "template_id": spec.get("template_id"),
                    "name": spec.get("name"),
                    "task_type": task_type,
                    "payload_json": _materialize_payload_json(
                        str(spec.get("payload_json") or "{}"),
                        now=now,
                        task_type=task_type,
                        template_id=str(spec.get("template_id") or "") or None,
                        template_name=str(spec.get("name") or "") or None,
                        template_metadata=spec.get("metadata_json") if isinstance(spec.get("metadata_json"), dict) else None,
                    ),
                    "model": spec.get("model"),
                    "max_attempts": int(spec.get("max_attempts") or policy.create_task_max_attempts),
                    "min_interval_seconds": min_interval_seconds,
                    "seconds_since_last_activity": age_seconds,
                }
            )

    ai_summary = get_ai_usage_summary(day_start, day_end)
    latest_health = get_latest_system_metrics()

    return {
        "captured_at": _to_iso(now),
        "pending_count": int(pending_count),
        "running_count": int(running_count),
        "recent_total_runs": int(recent_total_runs),
        "recent_failed_runs": int(recent_failed_runs),
        "execute_candidates": execute_candidates,
        "create_candidates": create_candidates,
        "ai_usage_summary": ai_summary,
        "latest_system_health": latest_health,
    }


def build_planner_decisions(state: dict[str, Any], policy: PlannerPolicy, now: datetime) -> list[dict[str, Any]]:
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)

    decisions: list[dict[str, Any]] = []
    base = {
        "planned_at": _to_iso(now),
    }

    health = state.get("latest_system_health") or {}
    cpu = _to_float(health.get("cpu_percent"))
    mem = _to_float(health.get("memory_percent"))
    disk = _to_float(health.get("disk_percent"))
    health_poor_reasons: list[str] = []
    if health and cpu is not None and cpu > policy.health_cpu_max_percent:
        health_poor_reasons.append("cpu_high")
    if health and mem is not None and mem > policy.health_memory_max_percent:
        health_poor_reasons.append("memory_high")
    if health and disk is not None and disk > policy.health_disk_max_percent:
        health_poor_reasons.append("disk_high")
    if not health:
        health_poor_reasons.append("health_unavailable")

    ai_usage = state.get("ai_usage_summary") or {}
    today_cost = _to_float(ai_usage.get("cost_usd_total")) or 0.0
    today_tokens = int(ai_usage.get("total_tokens_sum") or 0)

    budget_block_reasons: list[str] = []
    if policy.cost_budget_usd is not None and today_cost >= policy.cost_budget_usd:
        budget_block_reasons.append("cost_budget_exceeded")
    if policy.token_budget is not None and today_tokens >= policy.token_budget:
        budget_block_reasons.append("token_budget_exceeded")

    recent_total_runs = int(state.get("recent_total_runs") or 0)
    recent_failed_runs = int(state.get("recent_failed_runs") or 0)
    recent_failure_rate = (
        (float(recent_failed_runs) / float(recent_total_runs))
        if recent_total_runs > 0
        else 0.0
    )
    failure_spike = (
        recent_failed_runs >= policy.failure_alert_count_threshold
        and recent_failure_rate >= policy.failure_alert_rate_threshold
    )

    pending_count = int(state.get("pending_count") or 0)
    execute_candidates = list(state.get("execute_candidates") or [])
    create_candidates = list(state.get("create_candidates") or [])

    if health_poor_reasons:
        decisions.append(
            {
                **base,
                "decision_type": DecisionType.ALERT,
                "reason": "system_health_poor",
                "details": {
                    "reasons": health_poor_reasons,
                    "cpu_percent": cpu,
                    "memory_percent": mem,
                    "disk_percent": disk,
                },
            }
        )
        decisions.append(
            {
                **base,
                "decision_type": DecisionType.DEFER,
                "reason": "guardrail_system_health",
                "details": {"pending_count": pending_count},
            }
        )
        return decisions

    if budget_block_reasons:
        decisions.append(
            {
                **base,
                "decision_type": DecisionType.ALERT,
                "reason": "budget_guardrail_blocked",
                "details": {
                    "reasons": budget_block_reasons,
                    "today_cost_usd": today_cost,
                    "today_tokens": today_tokens,
                    "cost_budget_usd": policy.cost_budget_usd,
                    "token_budget": policy.token_budget,
                },
            }
        )
        decisions.append(
            {
                **base,
                "decision_type": DecisionType.DEFER,
                "reason": "guardrail_budget",
                "details": {},
            }
        )
        return decisions

    if failure_spike:
        decisions.append(
            {
                **base,
                "decision_type": DecisionType.ALERT,
                "reason": "recent_failures_spike",
                "details": {
                    "recent_total_runs": recent_total_runs,
                    "recent_failed_runs": recent_failed_runs,
                    "recent_failure_rate": round(recent_failure_rate, 4),
                },
            }
        )
        decisions.append(
            {
                **base,
                "decision_type": DecisionType.DEFER,
                "reason": "guardrail_recent_failures",
                "details": {},
            }
        )
        return decisions

    if pending_count > policy.max_pending_tasks:
        decisions.append(
            {
                **base,
                "decision_type": DecisionType.DEFER,
                "reason": "pending_backlog_high",
                "details": {
                    "pending_count": pending_count,
                    "max_pending_tasks": policy.max_pending_tasks,
                },
            }
        )
        return decisions

    for candidate in execute_candidates[: policy.max_tasks_executed_per_cycle]:
        decisions.append(
            {
                **base,
                "decision_type": DecisionType.EXECUTE_TASK,
                "reason": "stale_queued_task",
                "details": candidate,
            }
        )

    if policy.max_tasks_created_per_cycle > 0 and pending_count < policy.max_pending_tasks and create_candidates:
        for candidate in create_candidates[: policy.max_tasks_created_per_cycle]:
            decisions.append(
                {
                    **base,
                    "decision_type": DecisionType.CREATE_TASK,
                    "reason": "template_interval_due",
                    "details": {
                        "template_id": candidate.get("template_id"),
                        "template_name": candidate.get("name"),
                        "task_type": candidate.get("task_type"),
                        "payload_json": candidate.get("payload_json"),
                        "model": candidate.get("model"),
                        "max_attempts": candidate.get("max_attempts"),
                        "create_cooldown_seconds": candidate.get("min_interval_seconds"),
                        "seconds_since_last_activity": candidate.get("seconds_since_last_activity"),
                    },
                }
            )

    has_create_decision = any(
        str(row.get("decision_type") or "") == DecisionType.CREATE_TASK for row in decisions
    )
    if (
        not has_create_decision
        and pending_count == 0
        and policy.max_tasks_created_per_cycle > 0
        and policy.create_task_type
        and policy.create_payload_json
    ):
        for _ in range(policy.max_tasks_created_per_cycle):
            decisions.append(
                {
                    **base,
                    "decision_type": DecisionType.CREATE_TASK,
                    "reason": "no_pending_tasks",
                    "details": {
                        "task_type": policy.create_task_type,
                        "payload_json": _materialize_payload_json(
                            policy.create_payload_json,
                            now=now,
                            task_type=policy.create_task_type,
                        ),
                        "model": policy.create_task_model,
                        "max_attempts": policy.create_task_max_attempts,
                        "create_cooldown_seconds": policy.create_task_cooldown_seconds,
                    },
                }
            )

    if not decisions:
        decisions.append(
            {
                **base,
                "decision_type": DecisionType.DEFER,
                "reason": "no_action_needed",
                "details": {
                    "pending_count": pending_count,
                    "execute_candidates": len(execute_candidates),
                },
            }
        )

    return decisions


def _enqueue_existing_task(task_id: str) -> dict[str, Any]:
    runtime = _runtime()
    queue = runtime["queue"]
    schedule_enqueue_recovery = runtime["schedule_enqueue_recovery"]
    retry_delay_seconds = int(runtime["ENQUEUE_RECOVERY_DELAY_SEC"])
    try:
        queue.enqueue("worker.run_task", task_id)
        return {"status": "enqueued", "task_id": task_id}
    except Exception as exc:
        failure = schedule_enqueue_recovery(
            task_id,
            source="autonomous_planner",
            error=exc,
            retry_delay_seconds=retry_delay_seconds,
        )
        return {
            "status": "enqueue_retry_scheduled",
            "task_id": task_id,
            "scheduled_retry_at": failure.get("scheduled_retry_at"),
            "error": failure.get("error"),
        }


def _create_planner_task(
    *,
    now: datetime,
    policy: PlannerPolicy,
    task_type: str,
    payload_json: str,
    model: str | None,
    max_attempts: int,
    create_cooldown_seconds: int | None = None,
) -> dict[str, Any]:
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    runtime = _runtime()
    SessionLocal = runtime["SessionLocal"]
    Task = runtime["Task"]
    TaskStatus = runtime["TaskStatus"]
    choose_model = runtime["choose_model"]
    queue = runtime["queue"]
    schedule_enqueue_recovery = runtime["schedule_enqueue_recovery"]
    retry_delay_seconds = int(runtime["ENQUEUE_RECOVERY_DELAY_SEC"])

    payload_compact = _normalize_payload_json(payload_json)
    cooldown_window = max(int(create_cooldown_seconds or policy.create_task_cooldown_seconds), 60)
    bucket = int(now.timestamp()) // cooldown_window
    idempotency_key = f"planner:{task_type}:{bucket}"

    with SessionLocal() as db:
        existing = (
            db.query(Task)
            .filter(
                Task.task_type == task_type,
                Task.idempotency_key == idempotency_key,
            )
            .order_by(Task.created_at.desc())
            .first()
        )
        if existing is not None:
            return {
                "status": "already_exists",
                "task_id": existing.id,
                "idempotency_key": idempotency_key,
            }

        chosen_model = choose_model(
            task_type=task_type,
            payload_json=payload_compact,
            remaining_budget_usd=9999.0,
            user_override=model,
        )
        task_id = str(uuid.uuid4())
        task = Task(
            id=task_id,
            created_at=now,
            updated_at=now,
            status=TaskStatus.queued,
            task_type=task_type,
            payload_json=payload_compact,
            idempotency_key=idempotency_key,
            model=chosen_model,
            max_attempts=max(max_attempts, 1),
            next_run_at=None,
        )
        db.add(task)
        db.commit()

    try:
        queue.enqueue("worker.run_task", task_id)
        return {
            "status": "created_and_enqueued",
            "task_id": task_id,
            "idempotency_key": idempotency_key,
        }
    except Exception as exc:
        failure = schedule_enqueue_recovery(
            task_id,
            source="autonomous_planner",
            error=exc,
            retry_delay_seconds=retry_delay_seconds,
        )
        return {
            "status": "enqueue_retry_scheduled",
            "task_id": task_id,
            "idempotency_key": idempotency_key,
            "scheduled_retry_at": failure.get("scheduled_retry_at"),
            "error": failure.get("error"),
        }


def execute_planner_decisions(
    decisions: list[dict[str, Any]],
    *,
    now: datetime,
    policy: PlannerPolicy,
    execution_enabled: bool,
    require_approval: bool,
    approved: bool,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    can_execute = execution_enabled and (approved or not require_approval)

    for decision in decisions:
        decision_type = str(decision.get("decision_type") or "")
        details = decision.get("details") if isinstance(decision.get("details"), dict) else {}

        result: dict[str, Any] = {
            "decision_type": decision_type,
            "reason": decision.get("reason"),
            "executed": False,
            "status": "recommendation_only",
            "details": details,
        }

        if decision_type not in {DecisionType.EXECUTE_TASK, DecisionType.CREATE_TASK}:
            result["status"] = "non_executable"
            results.append(result)
            continue

        if not execution_enabled:
            result["status"] = "safe_mode_recommendation_only"
            results.append(result)
            continue

        if require_approval and not approved:
            result["status"] = "awaiting_approval"
            results.append(result)
            continue

        if not can_execute:
            results.append(result)
            continue

        if decision_type == DecisionType.EXECUTE_TASK:
            task_id = str(details.get("task_id") or "")
            if not task_id:
                result["status"] = "invalid_task_id"
                results.append(result)
                continue

            last_executed_at = _LAST_EXECUTED_TASK_AT.get(task_id)
            if last_executed_at is not None:
                cooldown_age = int((now - last_executed_at).total_seconds())
                if cooldown_age < policy.execute_task_cooldown_seconds:
                    result["status"] = "execute_cooldown"
                    result["cooldown_remaining_seconds"] = (
                        policy.execute_task_cooldown_seconds - cooldown_age
                    )
                    results.append(result)
                    continue

            enqueue_result = _enqueue_existing_task(task_id)
            _LAST_EXECUTED_TASK_AT[task_id] = now
            result.update(
                {
                    "executed": True,
                    "status": str(enqueue_result.get("status")),
                    "task_id": enqueue_result.get("task_id"),
                }
            )
            results.append(result)
            continue

        if decision_type == DecisionType.CREATE_TASK:
            task_type = str(details.get("task_type") or "").strip()
            payload_json = str(details.get("payload_json") or "").strip()
            model = details.get("model")
            max_attempts = int(details.get("max_attempts") or policy.create_task_max_attempts)
            create_cooldown_seconds = int(
                details.get("create_cooldown_seconds") or policy.create_task_cooldown_seconds
            )
            if not task_type or not payload_json:
                result["status"] = "invalid_create_task_details"
                results.append(result)
                continue

            create_result = _create_planner_task(
                now=now,
                policy=policy,
                task_type=task_type,
                payload_json=payload_json,
                model=(str(model) if isinstance(model, str) and model.strip() else None),
                max_attempts=max_attempts,
                create_cooldown_seconds=create_cooldown_seconds,
            )
            result.update(
                {
                    "executed": True,
                    "status": str(create_result.get("status")),
                    "task_id": create_result.get("task_id"),
                    "idempotency_key": create_result.get("idempotency_key"),
                }
            )
            results.append(result)
            continue

        results.append(result)

    return results


def run_planner_cycle(
    *,
    now: datetime | None = None,
    execution_enabled: bool = False,
    require_approval: bool = True,
    approved: bool = False,
    policy_overrides: dict[str, Any] | None = None,
    create_task_specs: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    run_now = now or datetime.now(timezone.utc)
    if run_now.tzinfo is None:
        run_now = run_now.replace(tzinfo=timezone.utc)

    policy = _apply_policy_overrides(PlannerPolicy.from_env(), policy_overrides)
    state = collect_planner_state(run_now, policy, create_task_specs=create_task_specs)
    decisions = build_planner_decisions(state, policy, run_now)
    execution_results = execute_planner_decisions(
        decisions,
        now=run_now,
        policy=policy,
        execution_enabled=execution_enabled,
        require_approval=require_approval,
        approved=approved,
    )

    decision_counts: dict[str, int] = {}
    for decision in decisions:
        key = str(decision.get("decision_type") or "unknown")
        decision_counts[key] = decision_counts.get(key, 0) + 1

    executed_count = sum(1 for row in execution_results if bool(row.get("executed")))
    summary = {
        "captured_at": _to_iso(run_now),
        "mode": "execute" if execution_enabled else "recommendation",
        "require_approval": bool(require_approval),
        "approved": bool(approved),
        "decision_counts": decision_counts,
        "executed_count": int(executed_count),
        "state": state,
        "decisions": decisions,
        "execution_results": execution_results,
        "policy": asdict(policy),
    }

    _safe_log_event(
        "autonomous_planner_cycle",
        level="INFO",
        message="Autonomous planner cycle completed",
        metadata_json={
            "mode": summary["mode"],
            "decision_counts": decision_counts,
            "executed_count": executed_count,
            "approved": approved,
        },
    )
    return summary
