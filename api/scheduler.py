import json
import logging
import os
import time
import uuid
from datetime import datetime, timedelta, timezone

from croniter import croniter
import httpx
from rq.job import Job
from rq.registry import StartedJobRegistry
from sqlalchemy import func

from agent_heartbeats import (
    delete_old_agent_heartbeats,
    list_stale_agent_heartbeats,
    set_agent_heartbeat_status,
    upsert_agent_heartbeat,
)
from autonomous_planner import run_planner_cycle
from daily_ops_report import (
    generate_daily_ai_ops_report,
    get_daily_ops_report,
    mark_daily_ops_report_notification,
    upsert_daily_ops_report,
)
from event_log import log_event as persist_event_log
from planner_control import (
    get_planner_runtime_config,
    list_enabled_planner_task_templates,
)
from system_metrics import collect_system_metrics_snapshot, get_latest_system_metrics
from main import (
    ENQUEUE_RECOVERY_DELAY_SEC,
    Run,
    RunStatus,
    Schedule,
    SessionLocal,
    Task,
    TaskStatus,
    choose_model,
    now_utc,
    queue,
    schedule_enqueue_recovery,
)


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

SCHEDULER_INTERVAL_SEC = int(os.getenv("SCHEDULER_INTERVAL_SEC", "60"))
WATCHDOG_ENABLED = os.getenv("WATCHDOG_ENABLED", "true").lower() == "true"
WATCHDOG_STALE_AFTER_SEC = max(int(os.getenv("WATCHDOG_STALE_AFTER_SEC", "180")), 30)
WATCHDOG_WARNING_COOLDOWN_SEC = max(int(os.getenv("WATCHDOG_WARNING_COOLDOWN_SEC", "300")), 30)
WATCHDOG_ENABLE_RESTART = os.getenv("WATCHDOG_ENABLE_RESTART", "false").lower() == "true"
WATCHDOG_RESTART_MIN_BACKOFF_SEC = max(int(os.getenv("WATCHDOG_RESTART_MIN_BACKOFF_SEC", "60")), 30)
WATCHDOG_RESTART_MAX_BACKOFF_SEC = max(
    int(os.getenv("WATCHDOG_RESTART_MAX_BACKOFF_SEC", "3600")),
    WATCHDOG_RESTART_MIN_BACKOFF_SEC,
)
WATCHDOG_TRACKED_AGENTS = tuple(
    item.strip()
    for item in os.getenv("WATCHDOG_TRACKED_AGENTS", "scheduler,worker").split(",")
    if item.strip()
)
AGENT_HEARTBEAT_RETENTION_SEC = max(int(os.getenv("AGENT_HEARTBEAT_RETENTION_SEC", "604800")), 300)
AGENT_HEARTBEAT_PRUNE_INTERVAL_SEC = max(int(os.getenv("AGENT_HEARTBEAT_PRUNE_INTERVAL_SEC", "3600")), 60)
DAILY_OPS_REPORT_ENABLED = os.getenv("DAILY_OPS_REPORT_ENABLED", "true").lower() == "true"
DAILY_OPS_REPORT_RUN_HOUR_UTC = max(min(int(os.getenv("DAILY_OPS_REPORT_RUN_HOUR_UTC", "0")), 23), 0)
DAILY_OPS_REPORT_NOTIFY_CHANNELS = [
    item.strip()
    for item in os.getenv("DAILY_OPS_REPORT_NOTIFY_CHANNELS", "").split(",")
    if item.strip()
]
DAILY_OPS_REPORT_NOTIFY_TTL_SEC = max(int(os.getenv("DAILY_OPS_REPORT_NOTIFY_TTL_SEC", "172800")), 60)
PLANNER_CONTROL_API_ENABLED = os.getenv("PLANNER_CONTROL_API_ENABLED", "true").lower() == "true"
PLANNER_CONTROL_API_URL = os.getenv("PLANNER_CONTROL_API_URL", "http://api:8000").strip()
PLANNER_CONTROL_API_TIMEOUT_SEC = max(float(os.getenv("PLANNER_CONTROL_API_TIMEOUT_SEC", "2.5")), 0.5)
AUTONOMOUS_PLANNER_DEFAULT_ENABLED = os.getenv("AUTONOMOUS_PLANNER_ENABLED", "false").lower() == "true"
AUTONOMOUS_PLANNER_DEFAULT_INTERVAL_SEC = max(int(os.getenv("AUTONOMOUS_PLANNER_INTERVAL_SEC", "300")), 30)
AUTONOMOUS_PLANNER_DEFAULT_EXECUTE = os.getenv("AUTONOMOUS_PLANNER_EXECUTE", "false").lower() == "true"
AUTONOMOUS_PLANNER_DEFAULT_REQUIRE_APPROVAL = (
    os.getenv("AUTONOMOUS_PLANNER_REQUIRE_APPROVAL", "true").lower() == "true"
)
AUTONOMOUS_PLANNER_DEFAULT_APPROVED = os.getenv("AUTONOMOUS_PLANNER_APPROVED", "false").lower() == "true"
RUNNING_TASK_RECOVERY_ENABLED = os.getenv("RUNNING_TASK_RECOVERY_ENABLED", "true").lower() == "true"
RUNNING_TASK_STALE_AFTER_SEC = max(
    int(os.getenv("RUNNING_TASK_STALE_AFTER_SEC", os.getenv("ORPHANED_RUN_STALE_AFTER_SEC", "420"))),
    60,
)
RUNNING_TASK_AUTO_KILL_ENABLED = os.getenv("RUNNING_TASK_AUTO_KILL_ENABLED", "true").lower() == "true"
RUNNING_TASK_RECOVERY_MAX_PER_CYCLE = max(int(os.getenv("RUNNING_TASK_RECOVERY_MAX_PER_CYCLE", "25")), 1)

_WATCHDOG_LAST_WARNING_AT: dict[str, datetime] = {}
_WATCHDOG_LAST_RESTART_ATTEMPT_AT: dict[str, datetime] = {}
_WATCHDOG_RESTART_BACKOFF_SEC: dict[str, int] = {}
_AUTONOMOUS_PLANNER_LAST_RUN_AT: datetime | None = None
_LAST_HEARTBEAT_PRUNE_AT: datetime | None = None


def _resolve_scheduler_name() -> str:
    return os.getenv("SCHEDULER_NAME", "scheduler").strip() or "scheduler"


def _resolve_worker_name() -> str:
    return os.getenv("WORKER_NAME", "worker").strip() or "worker"


def _resolve_tracked_agent_names() -> set[str]:
    tracked: set[str] = set(WATCHDOG_TRACKED_AGENTS)
    tracked.add(_resolve_scheduler_name())
    tracked.add(_resolve_worker_name())
    return tracked


def maybe_prune_old_heartbeats(now: datetime) -> int:
    global _LAST_HEARTBEAT_PRUNE_AT
    if _LAST_HEARTBEAT_PRUNE_AT is not None:
        age_sec = int((now - _LAST_HEARTBEAT_PRUNE_AT).total_seconds())
        if age_sec < AGENT_HEARTBEAT_PRUNE_INTERVAL_SEC:
            return 0

    keep_names = _resolve_tracked_agent_names()
    deleted = delete_old_agent_heartbeats(
        older_than_seconds=AGENT_HEARTBEAT_RETENTION_SEC,
        now=now,
        keep_agent_names=keep_names,
    )
    _LAST_HEARTBEAT_PRUNE_AT = now
    return deleted


def _log(event: str, *, level: str = "INFO", **context: object) -> None:
    normalized_level = level.upper()
    payload = {
        "timestamp": now_utc().isoformat(),
        "level": normalized_level,
        "service": "scheduler",
        "event": event,
        "context": context,
    }
    if normalized_level == "ERROR":
        logger.error(json.dumps(payload, default=str))
    elif normalized_level == "WARNING":
        logger.warning(json.dumps(payload, default=str))
    else:
        logger.info(json.dumps(payload, default=str))
    try:
        persist_event_log(
            event_type=event,
            source="scheduler",
            level=normalized_level,
            message=event.replace("_", " ").strip().capitalize(),
            metadata_json=context,
        )
    except Exception:
        logger.exception("structured_event_log_failed")


def _next_from_cron(cron_expr: str, base_time: datetime) -> datetime:
    next_dt = croniter(cron_expr, base_time).get_next(datetime)
    if next_dt.tzinfo is None:
        next_dt = next_dt.replace(tzinfo=timezone.utc)
    return next_dt


def _enqueue_daily_report_notification(
    *,
    report_date: str,
    report_text: str,
    severity: str,
) -> tuple[str, bool]:
    dedupe_key = f"daily-ai-ops:{report_date}"
    idempotency_key = f"daily_ops_notify:{report_date}"
    payload = {
        "channels": DAILY_OPS_REPORT_NOTIFY_CHANNELS,
        "message": report_text,
        "severity": severity,
        "dedupe_key": dedupe_key,
        "dedupe_ttl_seconds": DAILY_OPS_REPORT_NOTIFY_TTL_SEC,
        "source_task_type": "ops_report_v1",
        "metadata": {"report_date": report_date, "report_type": "daily_ai_ops"},
        "include_header": False,
        "include_metadata": False,
    }
    now = now_utc()

    with SessionLocal() as db:
        existing = (
            db.query(Task)
            .filter(Task.task_type == "notify_v1", Task.idempotency_key == idempotency_key)
            .first()
        )
        if existing is not None:
            return existing.id, False

        task_id = str(uuid.uuid4())
        task = Task(
            id=task_id,
            created_at=now,
            updated_at=now,
            status=TaskStatus.queued,
            task_type="notify_v1",
            payload_json=json.dumps(payload, separators=(",", ":"), ensure_ascii=True),
            idempotency_key=idempotency_key,
            model=None,
            max_attempts=3,
            next_run_at=None,
        )
        db.add(task)
        db.commit()

    try:
        queue.enqueue("worker.run_task", task_id)
        return task_id, True
    except Exception as exc:
        schedule_enqueue_recovery(task_id, source="scheduler", error=exc, retry_delay_seconds=ENQUEUE_RECOVERY_DELAY_SEC)
        _log(
            "daily_report_notification_enqueue_failed",
            level="ERROR",
            task_id=task_id,
            report_date=report_date,
            error=str(exc),
        )
        return task_id, False


def _emit_scheduler_heartbeat(*, status: str, metadata_json: dict[str, object] | None = None) -> None:
    payload = {
        "agent_type": "scheduler",
        "scheduler_name": _resolve_scheduler_name(),
        "host_name": os.getenv("HOSTNAME", "").strip() or None,
        "interval_seconds": SCHEDULER_INTERVAL_SEC,
        "watchdog_enabled": WATCHDOG_ENABLED,
    }
    if metadata_json:
        payload.update(metadata_json)
    try:
        upsert_agent_heartbeat(
            agent_name=_resolve_scheduler_name(),
            status=status,
            metadata_json=payload,
        )
    except Exception:
        logger.exception("scheduler_heartbeat_write_failed")


def _seconds_since(iso_ts: str | None, now: datetime) -> int | None:
    if not iso_ts:
        return None
    try:
        parsed = datetime.fromisoformat(iso_ts)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    delta = now - parsed
    return max(int(delta.total_seconds()), 0)


def _can_attempt_restart(agent_name: str, now: datetime) -> tuple[bool, int]:
    backoff_sec = _WATCHDOG_RESTART_BACKOFF_SEC.get(agent_name, WATCHDOG_RESTART_MIN_BACKOFF_SEC)
    last_attempt = _WATCHDOG_LAST_RESTART_ATTEMPT_AT.get(agent_name)
    if last_attempt is not None:
        age_sec = int((now - last_attempt).total_seconds())
        if age_sec < backoff_sec:
            return False, backoff_sec
    _WATCHDOG_LAST_RESTART_ATTEMPT_AT[agent_name] = now
    _WATCHDOG_RESTART_BACKOFF_SEC[agent_name] = min(backoff_sec * 2, WATCHDOG_RESTART_MAX_BACKOFF_SEC)
    return True, backoff_sec


def _attempt_safe_restart(agent_name: str) -> tuple[bool, str]:
    # Deliberately conservative: scheduler process has no safe local control-plane restart hook.
    # Docker restart policies are available for crash recovery, but this process does not issue restarts.
    return (
        False,
        "Automatic restart is not enabled in this architecture; relying on container restart policies only.",
    )


def _fetch_remote_planner_controls() -> tuple[dict[str, object] | None, list[dict[str, object]] | None]:
    if not PLANNER_CONTROL_API_ENABLED or not PLANNER_CONTROL_API_URL:
        return None, None

    base_url = PLANNER_CONTROL_API_URL.rstrip("/")
    headers: dict[str, str] = {}
    api_key = os.getenv("API_KEY", "").strip()
    if api_key:
        headers["X-API-Key"] = api_key

    try:
        with httpx.Client(timeout=PLANNER_CONTROL_API_TIMEOUT_SEC) as client:
            cfg_resp = client.get(f"{base_url}/planner/config", headers=headers)
            cfg_resp.raise_for_status()
            templates_resp = client.get(
                f"{base_url}/planner/templates",
                headers=headers,
                params={"limit": 200, "enabled_only": "true"},
            )
            templates_resp.raise_for_status()

        cfg_json = cfg_resp.json()
        templates_json = templates_resp.json()
        cfg = cfg_json if isinstance(cfg_json, dict) else None
        templates = templates_json if isinstance(templates_json, list) else None
        return cfg, templates
    except Exception as exc:
        _log("planner_remote_control_fetch_failed", level="WARNING", error=str(exc))
        return None, None


def _started_jobs_by_task_id() -> dict[str, list[str]]:
    try:
        registry = StartedJobRegistry(queue=queue)
        task_to_job_ids: dict[str, list[str]] = {}
        for job_id in registry.get_job_ids():
            try:
                job = Job.fetch(job_id, connection=queue.connection)
            except Exception:
                continue
            args = getattr(job, "args", ())
            if not isinstance(args, tuple) or not args:
                continue
            task_id = str(args[0] or "").strip()
            if not task_id:
                continue
            task_to_job_ids.setdefault(task_id, []).append(job_id)
        return task_to_job_ids
    except Exception:
        logger.exception("stale_running_registry_read_failed")
        return {}


def _request_stop_job(job_id: str) -> tuple[bool, str | None]:
    try:
        from rq.command import send_stop_job_command
    except Exception as exc:  # pragma: no cover - requires incompatible rq version
        return False, f"rq_stop_unavailable: {type(exc).__name__}: {exc}"

    try:
        send_stop_job_command(queue.connection, job_id)
        return True, None
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"


def recover_stale_running_tasks(now: datetime | None = None) -> dict[str, int]:
    if not RUNNING_TASK_RECOVERY_ENABLED:
        return {
            "scanned": 0,
            "stop_requested": 0,
            "stop_failed": 0,
            "active_waiting_stop": 0,
            "recovered": 0,
            "reenqueued": 0,
            "failed_permanent": 0,
        }

    current_time = now or now_utc()
    stale_cutoff = current_time - timedelta(seconds=RUNNING_TASK_STALE_AFTER_SEC)
    active_jobs_by_task = _started_jobs_by_task_id()

    stats = {
        "scanned": 0,
        "stop_requested": 0,
        "stop_failed": 0,
        "active_waiting_stop": 0,
        "recovered": 0,
        "reenqueued": 0,
        "failed_permanent": 0,
    }
    to_enqueue: list[str] = []

    with SessionLocal() as db:
        stale_running_tasks = (
            db.query(Task)
            .filter(Task.status == TaskStatus.running, Task.updated_at <= stale_cutoff)
            .order_by(Task.updated_at.asc())
            .limit(RUNNING_TASK_RECOVERY_MAX_PER_CYCLE)
            .all()
        )

        for task in stale_running_tasks:
            stats["scanned"] += 1
            running_run = (
                db.query(Run)
                .filter(Run.task_id == task.id, Run.status == RunStatus.running)
                .order_by(Run.attempt.desc())
                .first()
            )
            started_at = running_run.started_at or task.updated_at
            if started_at is not None and started_at.tzinfo is None:
                started_at = started_at.replace(tzinfo=timezone.utc)
            if started_at is not None and started_at > stale_cutoff:
                continue

            active_job_ids = active_jobs_by_task.get(task.id) or []
            if active_job_ids:
                stats["active_waiting_stop"] += 1
                if RUNNING_TASK_AUTO_KILL_ENABLED:
                    for job_id in active_job_ids:
                        stopped, error = _request_stop_job(job_id)
                        if stopped:
                            stats["stop_requested"] += 1
                        else:
                            stats["stop_failed"] += 1
                            _log(
                                "stale_running_task_stop_failed",
                                level="WARNING",
                                task_id=task.id,
                                job_id=job_id,
                                error=error,
                            )
                continue

            max_attempts = max(int(task.max_attempts or 1), 1)
            attempts_used = int(db.query(func.count(Run.id)).filter(Run.task_id == task.id).scalar() or 0)
            recovery_error = (
                f"Recovered stale running task after {RUNNING_TASK_STALE_AFTER_SEC}s timeout watchdog."
            )

            if running_run is not None:
                run_started_at = running_run.started_at or task.updated_at or current_time
                if run_started_at.tzinfo is None:
                    run_started_at = run_started_at.replace(tzinfo=timezone.utc)
                running_run.status = RunStatus.failed
                running_run.ended_at = current_time
                running_run.wall_time_ms = max(
                    int((current_time - run_started_at).total_seconds() * 1000),
                    0,
                )
                running_run.error = recovery_error

            if attempts_used < max_attempts:
                task.status = TaskStatus.queued
                task.next_run_at = None
                task.error = recovery_error
                task.updated_at = current_time
                to_enqueue.append(task.id)
                stats["reenqueued"] += 1
            else:
                task.status = TaskStatus.failed_permanent
                task.next_run_at = None
                task.error = f"{recovery_error} Max attempts reached ({max_attempts})."
                task.updated_at = current_time
                stats["failed_permanent"] += 1

            stats["recovered"] += 1

        if stats["recovered"] > 0:
            db.commit()

    for task_id in to_enqueue:
        try:
            queue.enqueue("worker.run_task", task_id)
        except Exception as exc:
            schedule_enqueue_recovery(task_id, source="scheduler", error=exc, retry_delay_seconds=ENQUEUE_RECOVERY_DELAY_SEC)
            _log(
                "stale_running_task_reenqueue_failed",
                level="ERROR",
                task_id=task_id,
                error=f"{type(exc).__name__}: {exc}",
            )
            continue

    return stats


def run_watchdog_cycle() -> dict[str, int]:
    if not WATCHDOG_ENABLED:
        return {"stale_agents": 0, "warnings_logged": 0, "restart_attempts": 0}

    now = now_utc()
    tracked_names = _resolve_tracked_agent_names()
    stale_rows = list_stale_agent_heartbeats(
        stale_after_seconds=WATCHDOG_STALE_AFTER_SEC,
        now=now,
        limit=max(len(tracked_names), 10),
        agent_names=tracked_names,
    )
    stale_names = {row["agent_name"] for row in stale_rows}

    for agent_name in list(_WATCHDOG_LAST_WARNING_AT.keys()):
        if agent_name in stale_names:
            continue
        _WATCHDOG_LAST_WARNING_AT.pop(agent_name, None)
        _WATCHDOG_LAST_RESTART_ATTEMPT_AT.pop(agent_name, None)
        _WATCHDOG_RESTART_BACKOFF_SEC.pop(agent_name, None)
        _log(
            "watchdog_agent_recovered",
            level="INFO",
            agent_name=agent_name,
        )

    warnings_logged = 0
    restart_attempts = 0

    for row in stale_rows:
        agent_name = row["agent_name"]
        last_warn_at = _WATCHDOG_LAST_WARNING_AT.get(agent_name)
        if last_warn_at is not None:
            age_sec = int((now - last_warn_at).total_seconds())
            if age_sec < WATCHDOG_WARNING_COOLDOWN_SEC:
                continue

        stale_for_seconds = _seconds_since(row.get("last_seen_at"), now)
        warnings_logged += 1
        _WATCHDOG_LAST_WARNING_AT[agent_name] = now

        metadata = {
            "watchdog_marked_stale_at": now.isoformat(),
            "watchdog_stale_for_seconds": stale_for_seconds,
            "last_seen_at": row.get("last_seen_at"),
        }
        try:
            set_agent_heartbeat_status(
                agent_name=agent_name,
                status="stale",
                metadata_json=metadata,
            )
        except Exception:
            logger.exception("watchdog_set_stale_status_failed")

        _log(
            "watchdog_agent_stale",
            level="WARNING",
            agent_name=agent_name,
            stale_for_seconds=stale_for_seconds,
            last_seen_at=row.get("last_seen_at"),
            prior_status=row.get("status"),
        )

        if not WATCHDOG_ENABLE_RESTART:
            continue

        can_attempt, backoff_sec = _can_attempt_restart(agent_name, now)
        if not can_attempt:
            continue

        restart_attempts += 1
        restarted, reason = _attempt_safe_restart(agent_name)
        if restarted:
            _log(
                "watchdog_restart_triggered",
                level="WARNING",
                agent_name=agent_name,
                restart_backoff_sec=backoff_sec,
            )
        else:
            _log(
                "watchdog_restart_not_supported",
                level="WARNING",
                agent_name=agent_name,
                restart_backoff_sec=backoff_sec,
                reason=reason,
            )

    return {
        "stale_agents": len(stale_rows),
        "warnings_logged": warnings_logged,
        "restart_attempts": restart_attempts,
    }


def maybe_generate_daily_ops_report(now: datetime) -> dict[str, object]:
    if not DAILY_OPS_REPORT_ENABLED:
        return {"generated": False, "reason": "disabled"}
    if now.hour < DAILY_OPS_REPORT_RUN_HOUR_UTC:
        return {"generated": False, "reason": "before_run_hour"}

    report_date = (now - timedelta(days=1)).date()
    existing = get_daily_ops_report(report_date)
    if existing is not None:
        return {"generated": False, "reason": "already_exists", "report_date": report_date.isoformat()}

    report_payload = generate_daily_ai_ops_report(report_date)
    report_text = str(report_payload["report_text"])
    report_json = dict(report_payload["report_json"])
    severity = str(report_payload.get("severity") or "info")

    upsert_daily_ops_report(
        report_date=report_date,
        report_text=report_text,
        report_json=report_json,
        notification_status="pending",
    )

    _log(
        "daily_ops_report_generated",
        report_date=report_date.isoformat(),
        completed=report_json["tasks"]["completed"],
        failed=report_json["tasks"]["failed"],
        tokens_total=report_json["ai_usage"]["tokens_total"],
        estimated_cost_usd=report_json["ai_usage"]["estimated_cost_usd"],
        recommendation_flags=report_json.get("recommendation_flags", []),
    )

    notification_status = "skipped_no_channels"
    notify_task_id: str | None = None

    if DAILY_OPS_REPORT_NOTIFY_CHANNELS:
        try:
            notify_task_id, created = _enqueue_daily_report_notification(
                report_date=report_date.isoformat(),
                report_text=report_text,
                severity=severity,
            )
            notification_status = "queued" if created else "already_queued"
            _log(
                "daily_ops_report_notification_queued",
                report_date=report_date.isoformat(),
                notify_task_id=notify_task_id,
                created=created,
                channels=DAILY_OPS_REPORT_NOTIFY_CHANNELS,
            )
        except Exception as exc:
            notification_status = "queue_failed"
            _log(
                "daily_ops_report_notification_failed",
                level="WARNING",
                report_date=report_date.isoformat(),
                error=str(exc),
            )

    mark_daily_ops_report_notification(
        report_date=report_date,
        notification_status=notification_status,
        notified_at=now,
    )

    return {
        "generated": True,
        "report_date": report_date.isoformat(),
        "notification_status": notification_status,
        "notify_task_id": notify_task_id,
    }


def maybe_run_autonomous_planner(now: datetime) -> dict[str, object]:
    global _AUTONOMOUS_PLANNER_LAST_RUN_AT
    remote_cfg, remote_templates = _fetch_remote_planner_controls()
    if remote_cfg is not None:
        planner_cfg = remote_cfg
    else:
        try:
            planner_cfg = get_planner_runtime_config()
        except Exception as exc:
            _log(
                "planner_runtime_config_error",
                level="WARNING",
                error=str(exc),
            )
            planner_cfg = {
                "enabled": AUTONOMOUS_PLANNER_DEFAULT_ENABLED,
                "interval_sec": AUTONOMOUS_PLANNER_DEFAULT_INTERVAL_SEC,
                "execution_enabled": AUTONOMOUS_PLANNER_DEFAULT_EXECUTE,
                "require_approval": AUTONOMOUS_PLANNER_DEFAULT_REQUIRE_APPROVAL,
                "approved": AUTONOMOUS_PLANNER_DEFAULT_APPROVED,
            }

    enabled = bool(planner_cfg.get("enabled", False))
    interval_sec = max(int(planner_cfg.get("interval_sec") or AUTONOMOUS_PLANNER_DEFAULT_INTERVAL_SEC), 30)
    execution_enabled = bool(planner_cfg.get("execution_enabled", False))
    require_approval = bool(planner_cfg.get("require_approval", True))
    approved = bool(planner_cfg.get("approved", False))

    if not enabled:
        return {"ran": False, "reason": "disabled"}

    if _AUTONOMOUS_PLANNER_LAST_RUN_AT is not None:
        elapsed = int((now - _AUTONOMOUS_PLANNER_LAST_RUN_AT).total_seconds())
        if elapsed < interval_sec:
            return {"ran": False, "reason": "interval_wait", "wait_seconds": interval_sec - elapsed}

    if remote_templates is not None:
        templates = [row for row in remote_templates if isinstance(row, dict)]
    else:
        try:
            templates = list_enabled_planner_task_templates(limit=200)
        except Exception as exc:
            _log("planner_template_query_failed", level="WARNING", error=str(exc))
            templates = []

    summary = run_planner_cycle(
        now=now,
        execution_enabled=execution_enabled,
        require_approval=require_approval,
        approved=approved,
        policy_overrides=planner_cfg,
        create_task_specs=templates,
    )
    _AUTONOMOUS_PLANNER_LAST_RUN_AT = now

    decision_counts = summary.get("decision_counts") or {}
    execution_results = summary.get("execution_results") if isinstance(summary.get("execution_results"), list) else []
    alert_count = int(decision_counts.get("alert", 0))
    awaiting_approval_count = sum(
        1
        for row in execution_results
        if isinstance(row, dict) and row.get("status") == "awaiting_approval"
    )
    return {
        "ran": True,
        "mode": summary.get("mode"),
        "executed_count": int(summary.get("executed_count") or 0),
        "decision_counts": decision_counts,
        "alert_count": alert_count,
        "awaiting_approval_count": awaiting_approval_count,
        "templates_considered": len(templates),
    }


def enqueue_due_schedules() -> int:
    now = now_utc()
    created_count = 0

    with SessionLocal() as db:
        due = (
            db.query(Schedule)
            .filter(Schedule.enabled.is_(True), Schedule.next_run_at <= now)
            .order_by(Schedule.next_run_at.asc())
            .all()
        )

        for sched in due:
            idempotency_key = f"schedule:{sched.id}:{sched.next_run_at.isoformat()}"
            existing = (
                db.query(Task)
                .filter(Task.task_type == sched.task_type, Task.idempotency_key == idempotency_key)
                .first()
            )

            if existing is None:
                chosen_model = choose_model(
                    task_type=sched.task_type,
                    payload_json=sched.payload_json,
                    remaining_budget_usd=9999.0,
                    user_override=sched.model,
                )
                task_id = str(uuid.uuid4())
                task = Task(
                    id=task_id,
                    created_at=now,
                    updated_at=now,
                    status=TaskStatus.queued,
                    task_type=sched.task_type,
                    payload_json=sched.payload_json,
                    idempotency_key=idempotency_key,
                    model=chosen_model,
                    max_attempts=max(int(sched.max_attempts or 3), 1),
                    next_run_at=None,
                )
                db.add(task)
                try:
                    queue.enqueue("worker.run_task", task_id)
                    created_count += 1
                    _log(
                        "scheduled_task_enqueued",
                        schedule_id=sched.id,
                        task_id=task_id,
                        task_type=sched.task_type,
                        idempotency_key=idempotency_key,
                    )
                except Exception as exc:
                    task.error = f"QUEUE_ENQUEUE_ERROR[scheduler]: {type(exc).__name__}: {exc}"
                    task.next_run_at = now + timedelta(seconds=ENQUEUE_RECOVERY_DELAY_SEC)
                    task.updated_at = now
                    _log(
                        "scheduled_task_enqueue_failed",
                        level="ERROR",
                        schedule_id=sched.id,
                        task_id=task_id,
                        task_type=sched.task_type,
                        idempotency_key=idempotency_key,
                        scheduled_retry_at=task.next_run_at.isoformat(),
                        error=str(exc),
                    )

            sched.last_run_at = now
            sched.next_run_at = _next_from_cron(sched.cron, now)
            sched.updated_at = now

        db.commit()

    return created_count


def enqueue_due_retry_tasks() -> int:
    now = now_utc()
    enqueued_count = 0

    with SessionLocal() as db:
        due_tasks = (
            db.query(Task)
            .filter(
                Task.status == TaskStatus.queued,
                Task.next_run_at.is_not(None),
                Task.next_run_at <= now,
            )
            .order_by(Task.next_run_at.asc())
            .all()
        )

        for task in due_tasks:
            try:
                queue.enqueue("worker.run_task", task.id)
                task.next_run_at = None
                task.updated_at = now
                enqueued_count += 1
                _log(
                    "retry_task_enqueued",
                    task_id=task.id,
                    task_type=task.task_type,
                )
            except Exception as exc:
                task.error = f"QUEUE_ENQUEUE_ERROR[scheduler]: {type(exc).__name__}: {exc}"
                task.next_run_at = now + timedelta(seconds=ENQUEUE_RECOVERY_DELAY_SEC)
                task.updated_at = now
                _log(
                    "retry_task_enqueue_failed",
                    level="ERROR",
                    task_id=task.id,
                    task_type=task.task_type,
                    scheduled_retry_at=task.next_run_at.isoformat(),
                    error=str(exc),
                )

        db.commit()

    return enqueued_count


def main() -> None:
    _log(
        "scheduler_started",
        interval_seconds=SCHEDULER_INTERVAL_SEC,
        watchdog_enabled=WATCHDOG_ENABLED,
        stale_after_seconds=WATCHDOG_STALE_AFTER_SEC,
        running_task_recovery_enabled=RUNNING_TASK_RECOVERY_ENABLED,
        running_task_stale_after_seconds=RUNNING_TASK_STALE_AFTER_SEC,
        running_task_auto_kill_enabled=RUNNING_TASK_AUTO_KILL_ENABLED,
        tracked_agents=list(_resolve_tracked_agent_names()),
        heartbeat_retention_seconds=AGENT_HEARTBEAT_RETENTION_SEC,
    )
    _emit_scheduler_heartbeat(
        status="alive",
        metadata_json={"phase": "startup"},
    )
    while True:
        try:
            _emit_scheduler_heartbeat(
                status="alive",
                metadata_json={"phase": "tick"},
            )
            metrics_id = collect_system_metrics_snapshot()
            latest_metrics = get_latest_system_metrics()
            _log(
                "system_metrics_collected",
                metrics_id=metrics_id,
                cpu_percent=(latest_metrics or {}).get("cpu_percent"),
                memory_percent=(latest_metrics or {}).get("memory_percent"),
                disk_percent=(latest_metrics or {}).get("disk_percent"),
            )
            scheduled_created = enqueue_due_schedules()
            stale_recovery = recover_stale_running_tasks(now_utc())
            retries_enqueued = enqueue_due_retry_tasks()
            _log(
                "scheduler_tick",
                scheduled_created=scheduled_created,
                retries_enqueued=retries_enqueued,
                stale_running_scanned=stale_recovery["scanned"],
                stale_running_recovered=stale_recovery["recovered"],
                stale_running_reenqueued=stale_recovery["reenqueued"],
                stale_running_stop_requested=stale_recovery["stop_requested"],
                stale_running_stop_failed=stale_recovery["stop_failed"],
            )
            if (
                stale_recovery["recovered"] > 0
                or stale_recovery["stop_requested"] > 0
                or stale_recovery["stop_failed"] > 0
            ):
                _log(
                    "stale_running_recovery_tick",
                    level="WARNING",
                    **stale_recovery,
                )
            report_summary = maybe_generate_daily_ops_report(now_utc())
            if report_summary.get("generated"):
                _log("daily_ops_report_tick", **report_summary)
            planner_summary = maybe_run_autonomous_planner(now_utc())
            if planner_summary.get("ran"):
                planner_level = "WARNING" if int(planner_summary.get("alert_count") or 0) > 0 else "INFO"
                _log("autonomous_planner_tick", level=planner_level, **planner_summary)
            deleted_heartbeats = maybe_prune_old_heartbeats(now_utc())
            if deleted_heartbeats > 0:
                _log(
                    "agent_heartbeat_rows_pruned",
                    deleted_rows=deleted_heartbeats,
                    retention_seconds=AGENT_HEARTBEAT_RETENTION_SEC,
                )
            watchdog_summary = run_watchdog_cycle()
            if watchdog_summary["warnings_logged"] > 0 or watchdog_summary["restart_attempts"] > 0:
                _log(
                    "watchdog_tick",
                    level="WARNING",
                    **watchdog_summary,
                )
        except Exception as exc:
            _emit_scheduler_heartbeat(
                status="degraded",
                metadata_json={"phase": "tick_error", "error": str(exc)},
            )
            _log("scheduler_tick_error", level="ERROR", error=str(exc))

        time.sleep(SCHEDULER_INTERVAL_SEC)


if __name__ == "__main__":
    main()
