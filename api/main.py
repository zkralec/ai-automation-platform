from __future__ import annotations

import json
import logging
import os
import threading
import time
import uuid
import io
from collections import defaultdict
from decimal import Decimal, ROUND_HALF_UP
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Optional, List

from croniter import croniter
from fastapi import FastAPI, File, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, JSONResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, field_validator, model_validator
from sqlalchemy import (
    Boolean,
    create_engine,
    String,
    DateTime,
    Numeric,
    Enum as SAEnum,
    Text,
    Integer,
    Index,
    ForeignKey,
    JSON,
    UniqueConstraint,
    cast,
    func,
    inspect,
    text,
)
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker

from redis import Redis
from redis.exceptions import RedisError
from rq import Queue

from ai_usage_log import (
    get_ai_usage_summary,
    list_ai_usage_today,
    list_recent_ai_usage,
)
from candidate_profile import (
    delete_resume_profile as delete_candidate_resume_profile,
    get_resume_profile as get_candidate_resume_profile,
    upsert_resume_profile as upsert_candidate_resume_profile,
)
from agent_heartbeats import list_recent_agent_heartbeats, list_stale_agent_heartbeats
from event_log import log_event as persist_event_log
from event_log import list_recent_events
from planner_control import (
    create_planner_task_template,
    delete_planner_task_template,
    ensure_jobs_digest_template,
    ensure_rtx5090_deals_template,
    get_planner_runtime_config,
    get_planner_task_template,
    list_enabled_planner_task_templates,
    list_planner_task_templates,
    reset_planner_runtime_config,
    update_planner_runtime_config,
    update_planner_task_template,
)
from planner_status import get_planner_status_snapshot
from system_metrics import get_latest_system_metrics, list_system_metrics
from router import choose_model, validate_model, get_available_models
try:
    from operational_day import current_operational_day_window_utc
except ModuleNotFoundError:  # pragma: no cover - package import fallback
    from api.operational_day import current_operational_day_window_utc


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATABASE_URL = os.environ["DATABASE_URL"]
REDIS_URL = os.environ["REDIS_URL"]
DAILY_BUDGET_USD = Decimal(os.getenv("DAILY_BUDGET_USD", "0.50"))
BUDGET_BUFFER_USD = Decimal(os.getenv("BUDGET_BUFFER_USD", "0.02"))
COST_SCALE = Decimal("0.00000001")

API_KEY = os.getenv("API_KEY", "").strip()
RATE_LIMIT_CREATE_CAPACITY = int(os.getenv("RATE_LIMIT_CREATE_CAPACITY", "120"))
RATE_LIMIT_CREATE_REFILL_PER_SEC = float(os.getenv("RATE_LIMIT_CREATE_REFILL_PER_SEC", "2.0"))
DEFAULT_HEARTBEAT_STALE_AFTER_SEC = max(
    int(os.getenv("WATCHDOG_STALE_AFTER_SEC", os.getenv("HEARTBEAT_STALE_AFTER_SEC", "180"))),
    30,
)
ENQUEUE_RECOVERY_DELAY_SEC = max(int(os.getenv("ENQUEUE_RECOVERY_DELAY_SEC", "60")), 10)

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)

redis_conn = Redis.from_url(REDIS_URL)
RQ_JOB_TIMEOUT_SECONDS = max(int(os.getenv("RQ_JOB_TIMEOUT_SECONDS", "900")), 30)
queue = Queue("default", connection=redis_conn, default_timeout=RQ_JOB_TIMEOUT_SECONDS)

TOKEN_BUCKET_SCRIPT = """
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local requested = tonumber(ARGV[4])

local data = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(data[1])
local ts = tonumber(data[2])

if tokens == nil then
  tokens = capacity
end
if ts == nil then
  ts = now
end

local delta = math.max(0, now - ts)
tokens = math.min(capacity, tokens + (delta * refill))

local allowed = 0
if tokens >= requested then
  allowed = 1
  tokens = tokens - requested
end

redis.call('HMSET', key, 'tokens', tokens, 'ts', now)
local ttl = math.ceil((capacity / refill) * 2)
if ttl < 1 then ttl = 1 end
redis.call('EXPIRE', key, ttl)

return {allowed, tokens}
"""

METRICS_LOCK = threading.Lock()
METRICS_COUNTERS: dict[str, int] = {
    "tasks_created_total": 0,
    "tasks_deduped_total": 0,
    "tasks_blocked_budget_total": 0,
    "task_create_rate_limited_total": 0,
    "auth_rejected_total": 0,
    "schedules_created_total": 0,
}


class Base(DeclarativeBase):
    pass


class TaskStatus(str, Enum):
    queued = "queued"
    running = "running"
    success = "success"
    failed = "failed"
    failed_permanent = "failed_permanent"
    blocked_budget = "blocked_budget"


class RunStatus(str, Enum):
    queued = "queued"
    running = "running"
    success = "success"
    failed = "failed"


class Task(Base):
    __tablename__ = "tasks"
    __table_args__ = (
        UniqueConstraint("task_type", "idempotency_key", name="uq_tasks_task_type_idempotency_key"),
        Index("ix_tasks_status", "status"),
        Index("ix_tasks_next_run_at", "next_run_at"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    status: Mapped[TaskStatus] = mapped_column(SAEnum(TaskStatus), default=TaskStatus.queued)

    task_type: Mapped[str] = mapped_column(String(64))
    payload_json: Mapped[str] = mapped_column(Text)
    idempotency_key: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)

    model: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    tokens_in: Mapped[Optional[int]] = mapped_column(nullable=True)
    tokens_out: Mapped[Optional[int]] = mapped_column(nullable=True)
    cost_usd: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 8), nullable=True)
    error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    max_attempts: Mapped[int] = mapped_column(Integer, default=3)
    next_run_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    max_cost_usd: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 8), nullable=True)
    expected_tokens_in: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    expected_tokens_out: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)


class Run(Base):
    __tablename__ = "runs"
    __table_args__ = (
        Index("ix_runs_task_id", "task_id"),
        Index("ix_runs_created_at", "created_at"),
        Index("ix_runs_status", "status"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    task_id: Mapped[str] = mapped_column(String(36), nullable=False)
    attempt: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[RunStatus] = mapped_column(SAEnum(RunStatus), default=RunStatus.queued)
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    ended_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    wall_time_ms: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    model: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    tokens_in: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    tokens_out: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    cost_usd: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 8), nullable=True)
    error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class Artifact(Base):
    __tablename__ = "artifacts"
    __table_args__ = (
        Index("ix_artifacts_task_id", "task_id"),
        Index("ix_artifacts_run_id", "run_id"),
        Index("ix_artifacts_artifact_type", "artifact_type"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    task_id: Mapped[str] = mapped_column(String(36), ForeignKey("tasks.id"), nullable=False)
    run_id: Mapped[str] = mapped_column(String(36), ForeignKey("runs.id"), nullable=False)
    artifact_type: Mapped[str] = mapped_column(String(64), nullable=False)
    content_text: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    content_json: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )


class Schedule(Base):
    __tablename__ = "schedules"
    __table_args__ = (
        Index("ix_schedules_enabled_next_run_at", "enabled", "next_run_at"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    task_type: Mapped[str] = mapped_column(String(64), nullable=False)
    payload_json: Mapped[str] = mapped_column(Text, nullable=False)
    model: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    cron: Mapped[str] = mapped_column(String(128), nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    max_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=3)
    last_run_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    next_run_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


app = FastAPI(title="Mission Control API")
STATIC_DIR = Path(__file__).resolve().parent / "static"
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
_frontend_dist_env = os.getenv("FRONTEND_DIST_DIR", "").strip()
if _frontend_dist_env:
    FRONTEND_DIST_DIR = Path(_frontend_dist_env)
else:
    FRONTEND_DIST_DIR = Path(__file__).resolve().parent / "frontend_dist"
if not FRONTEND_DIST_DIR.exists():
    FRONTEND_DIST_DIR = Path(__file__).resolve().parent.parent / "frontend" / "dist"
app.mount("/app/assets", StaticFiles(directory=FRONTEND_DIST_DIR / "assets", check_dir=False), name="frontend-assets")
RESULT_ARTIFACT_TYPE = "result.json"
DEBUG_ARTIFACT_TYPE = "debug.json"


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def ensure_tables() -> None:
    Base.metadata.create_all(bind=engine)


def ensure_schema_compatibility() -> None:
    """
    Non-destructive runtime schema reconciliation for environments
    where migrations have not yet been applied.
    """
    inspector = inspect(engine)
    if "tasks" not in inspector.get_table_names():
        return

    existing_columns = {col["name"] for col in inspector.get_columns("tasks")}
    dialect = engine.dialect.name

    with engine.begin() as conn:
        if dialect == "postgresql":
            conn.execute(
                text(
                    """
                    DO $$
                    BEGIN
                        IF EXISTS (SELECT 1 FROM pg_type WHERE typname = 'taskstatus') THEN
                            ALTER TYPE taskstatus ADD VALUE IF NOT EXISTS 'failed_permanent';
                        END IF;
                    END
                    $$;
                    """
                )
            )

        if "idempotency_key" not in existing_columns:
            conn.execute(text("ALTER TABLE tasks ADD COLUMN idempotency_key VARCHAR(128)"))
        if "max_attempts" not in existing_columns:
            conn.execute(text("ALTER TABLE tasks ADD COLUMN max_attempts INTEGER NOT NULL DEFAULT 3"))
        if "next_run_at" not in existing_columns:
            conn.execute(text("ALTER TABLE tasks ADD COLUMN next_run_at TIMESTAMPTZ NULL"))
        if "max_cost_usd" not in existing_columns:
            conn.execute(text("ALTER TABLE tasks ADD COLUMN max_cost_usd NUMERIC(12, 8) NULL"))
        if "expected_tokens_in" not in existing_columns:
            conn.execute(text("ALTER TABLE tasks ADD COLUMN expected_tokens_in INTEGER NULL"))
        if "expected_tokens_out" not in existing_columns:
            conn.execute(text("ALTER TABLE tasks ADD COLUMN expected_tokens_out INTEGER NULL"))

        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_tasks_status ON tasks(status)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_tasks_next_run_at ON tasks(next_run_at)"))
        if dialect == "postgresql":
            conn.execute(
                text(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS uq_tasks_task_type_idempotency_key
                    ON tasks(task_type, idempotency_key)
                    WHERE idempotency_key IS NOT NULL
                    """
                )
            )
        else:
            conn.execute(
                text(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS uq_tasks_task_type_idempotency_key
                    ON tasks(task_type, idempotency_key)
                    """
                )
            )


def log_event(event: str, **context: object) -> None:
    payload = {
        "timestamp": now_utc().isoformat(),
        "level": "INFO",
        "service": "api",
        "event": event,
        "context": context,
    }
    logger.info(json.dumps(payload, default=str))
    try:
        message = event.replace("_", " ").strip().capitalize()
        persist_event_log(
            event_type=event,
            source="api",
            level="INFO",
            message=message,
            metadata_json=context,
        )
    except Exception:
        logger.exception("structured_event_log_failed")


def increment_metric(metric_name: str, by: int = 1) -> None:
    with METRICS_LOCK:
        METRICS_COUNTERS[metric_name] = METRICS_COUNTERS.get(metric_name, 0) + by


def to_decimal_8(value: object) -> Decimal:
    if isinstance(value, Decimal):
        dec = value
    else:
        dec = Decimal(str(value))
    return dec.quantize(COST_SCALE, rounding=ROUND_HALF_UP)


def consume_create_rate_limit_token(identity: str) -> tuple[bool, float]:
    key = f"rate:tasks:create:{identity}"
    now_ts = time.time()
    try:
        raw = redis_conn.eval(
            TOKEN_BUCKET_SCRIPT,
            1,
            key,
            RATE_LIMIT_CREATE_CAPACITY,
            RATE_LIMIT_CREATE_REFILL_PER_SEC,
            now_ts,
            1,
        )
        allowed = bool(int(raw[0]))
        remaining = float(raw[1])
        return allowed, remaining
    except RedisError:
        # Fail open if limiter backend is unavailable.
        return True, float(RATE_LIMIT_CREATE_CAPACITY)


def today_spend_usd(db) -> Decimal:
    """
    Compute today's spend as SUM(runs.cost_usd) for runs where started_at is in the
    current operational day window.
    Runs without started_at or cost_usd are excluded from the sum.
    """
    start, end = current_operational_day_window_utc()
    result = db.query(
        func.coalesce(func.sum(Run.cost_usd), cast(0, Numeric(12, 8)))
    ).filter(
        Run.started_at >= start,
        Run.started_at < end,
    ).scalar()
    if result is None:
        return Decimal("0")
    return to_decimal_8(result)


def enforce_budget(db, min_required_usd: Decimal) -> tuple[bool, Decimal, Decimal]:
    """
    Enforce budget with a required minimum remaining amount.
    Returns (allowed, spend_today_usd, remaining_usd).
    """
    spend = today_spend_usd(db)
    remaining = DAILY_BUDGET_USD - spend
    required = max(min_required_usd, Decimal("0"))
    allowed = remaining > required
    return allowed, spend, remaining


def is_budget_available(db, safety_buffer: Decimal = BUDGET_BUFFER_USD) -> tuple[bool, Decimal, Decimal]:
    """
    Check if budget is available for a new run.
    Returns (is_available, remaining_usd, spend_usd).
    Budget is not available if: spend >= budget OR remaining < buffer.
    """
    is_available, spend, remaining = enforce_budget(db, safety_buffer)
    return is_available, remaining, spend


def _is_exempt_path(path: str) -> bool:
    if path in {"/", "/legacy", "/legacy/observability", "/health", "/ready", "/metrics", "/openapi.json"}:
        return True
    if path.startswith("/static"):
        return True
    if path == "/app" or path.startswith("/app/"):
        return True
    if path.startswith("/docs") or path.startswith("/redoc"):
        return True
    return False


def _find_idempotent_task(db, task_type: str, idempotency_key: str) -> Optional[Task]:
    return (
        db.query(Task)
        .filter(Task.task_type == task_type, Task.idempotency_key == idempotency_key)
        .order_by(Task.created_at.desc())
        .first()
    )


def _parse_agent_names(raw: str | None) -> set[str]:
    if not raw:
        return set()
    return {item.strip() for item in str(raw).split(",") if item.strip()}


def _parse_iso_datetime(raw: Any) -> datetime | None:
    if not isinstance(raw, str) or not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _tracked_agent_names_for_telemetry() -> set[str]:
    tracked = _parse_agent_names(os.getenv("HEARTBEAT_TRACKED_AGENTS", "scheduler,worker"))
    scheduler_name = os.getenv("SCHEDULER_NAME", "scheduler").strip() or "scheduler"
    worker_name = os.getenv("WORKER_NAME", "worker").strip() or "worker"
    tracked.add(scheduler_name)
    tracked.add(worker_name)
    return tracked


def _safe_debug_payload(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _latest_debug_artifacts_by_task_id(db, task_ids: set[str]) -> dict[str, dict[str, Any]]:
    if not task_ids:
        return {}
    rows = (
        db.query(Artifact)
        .filter(
            Artifact.task_id.in_(sorted(task_ids)),
            Artifact.artifact_type == DEBUG_ARTIFACT_TYPE,
        )
        .order_by(Artifact.task_id.asc(), Artifact.created_at.desc(), Artifact.id.desc())
        .all()
    )
    latest: dict[str, dict[str, Any]] = {}
    for row in rows:
        if row.task_id in latest:
            continue
        latest[row.task_id] = _safe_debug_payload(row.content_json)
    return latest


def _latest_debug_artifacts_by_run_id(db, run_ids: set[str]) -> dict[str, dict[str, Any]]:
    if not run_ids:
        return {}
    rows = (
        db.query(Artifact)
        .filter(
            Artifact.run_id.in_(sorted(run_ids)),
            Artifact.artifact_type == DEBUG_ARTIFACT_TYPE,
        )
        .order_by(Artifact.run_id.asc(), Artifact.created_at.desc(), Artifact.id.desc())
        .all()
    )
    latest: dict[str, dict[str, Any]] = {}
    for row in rows:
        if row.run_id in latest:
            continue
        latest[row.run_id] = _safe_debug_payload(row.content_json)
    return latest


def _parse_prefixed_error_type(error_text: str) -> str | None:
    if not error_text:
        return None
    head = error_text.split(":", 1)[0].strip()
    if not head:
        return None
    if head.startswith("QUEUE_ENQUEUE_ERROR["):
        return "QUEUE_ENQUEUE_ERROR"
    if head.isupper() or head.endswith("Error"):
        return head
    return None


def _derive_failure_diagnostics(
    *,
    error_text: str | None,
    debug_payload: dict[str, Any] | None,
) -> dict[str, Any] | None:
    debug = debug_payload or {}
    raw_error = str(error_text or debug.get("error") or "").strip()
    error_type = str(debug.get("error_type") or _parse_prefixed_error_type(raw_error) or "").strip() or None
    retry_scheduled = bool(debug.get("retry_scheduled"))
    retry_at = _parse_iso_datetime(debug.get("retry_at"))

    if not raw_error and not error_type:
        return None

    category = "task_execution_failure"
    source = "worker"
    stage = "worker_execute"
    upstream_service: str | None = None
    is_browser_disconnect = False
    is_task_execution_failure = True
    is_queue_enqueue_failure = False
    is_scheduler_runtime_failure = False
    summary = raw_error or "Task failure recorded."

    if raw_error.startswith("QUEUE_ENQUEUE_ERROR["):
        category = "queue_enqueue_failure"
        source = raw_error.split("[", 1)[1].split("]", 1)[0] or "queue"
        stage = "queue_enqueue"
        upstream_service = "redis"
        is_task_execution_failure = False
        is_queue_enqueue_failure = True
        summary = "Task could not be enqueued to Redis; scheduler retry is scheduled."
    elif error_type == "APIConnectionError" or raw_error.startswith("OPENAI_API_CONNECTION_ERROR:") or raw_error == "Connection error.":
        category = "upstream_connection_failure"
        source = "worker"
        stage = "worker_upstream_request"
        upstream_service = "openai"
        summary = "Worker failed while calling the OpenAI API; this is an upstream connection failure."
    elif raw_error.startswith("VALIDATION_ERROR:"):
        category = "validation_failure"
        stage = "payload_validation"
        summary = "Task payload validation failed before execution could complete."
    elif raw_error.startswith("NON_RETRYABLE_ERROR:"):
        category = "non_retryable_failure"
        stage = "worker_handler"
        summary = "Worker marked the task as permanently failed with a non-retryable error."
    elif "blocked" in raw_error.lower() and "budget" in raw_error.lower():
        category = "budget_blocked"
        source = "api"
        stage = "task_creation"
        is_task_execution_failure = False
        summary = "Task creation was blocked by the current budget policy."

    return {
        "summary": summary,
        "category": category,
        "source": source,
        "stage": stage,
        "error_type": error_type,
        "error_message": raw_error or None,
        "upstream_service": upstream_service,
        "retry_scheduled": retry_scheduled,
        "retry_at": retry_at,
        "is_browser_disconnect": is_browser_disconnect,
        "is_task_execution_failure": is_task_execution_failure,
        "is_queue_enqueue_failure": is_queue_enqueue_failure,
        "is_scheduler_runtime_failure": is_scheduler_runtime_failure,
    }


def _task_to_out(task: Task, debug_payload: dict[str, Any] | None = None) -> TaskOut:
    diagnostics = _derive_failure_diagnostics(error_text=task.error, debug_payload=debug_payload)
    return TaskOut(**task.__dict__, diagnostics=diagnostics)


def _run_to_out(run: Run, debug_payload: dict[str, Any] | None = None) -> RunOut:
    diagnostics = _derive_failure_diagnostics(error_text=run.error, debug_payload=debug_payload)
    return RunOut(**run.__dict__, diagnostics=diagnostics)


def _build_enqueue_failure_error(source: str, exc: Exception) -> str:
    source_label = source.strip() or "unknown"
    return f"QUEUE_ENQUEUE_ERROR[{source_label}]: {type(exc).__name__}: {exc}"


def schedule_enqueue_recovery(task_id: str, *, source: str, error: Exception, retry_delay_seconds: int = ENQUEUE_RECOVERY_DELAY_SEC) -> dict[str, Any]:
    scheduled_retry_at = now_utc() + timedelta(seconds=max(int(retry_delay_seconds), 10))
    error_text = _build_enqueue_failure_error(source, error)
    task_type: str | None = None

    with SessionLocal() as db:
        task = db.get(Task, task_id)
        if task is not None:
            task.status = TaskStatus.queued
            task.next_run_at = scheduled_retry_at
            task.error = error_text
            task.updated_at = now_utc()
            task_type = task.task_type
            db.commit()

    try:
        persist_event_log(
            event_type="task_enqueue_failed",
            source=source,
            level="ERROR",
            message="Task enqueue failed",
            metadata_json={
                "task_id": task_id,
                "task_type": task_type,
                "error": error_text,
                "error_type": type(error).__name__,
                "scheduled_retry_at": scheduled_retry_at.isoformat(),
                "upstream_service": "redis",
            },
        )
    except Exception:
        logger.exception("enqueue_failure_event_log_failed")

    logger.error(
        "task_enqueue_failed source=%s task_id=%s task_type=%s retry_at=%s error=%s",
        source,
        task_id,
        task_type,
        scheduled_retry_at.isoformat(),
        error_text,
    )
    return {
        "task_id": task_id,
        "task_type": task_type,
        "error": error_text,
        "error_type": type(error).__name__,
        "scheduled_retry_at": scheduled_retry_at,
    }


@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    if API_KEY and not _is_exempt_path(request.url.path):
        if request.headers.get("X-API-Key") != API_KEY:
            increment_metric("auth_rejected_total")
            return JSONResponse(status_code=401, content={"detail": "Unauthorized"})
    return await call_next(request)


@app.on_event("startup")
def startup() -> None:
    ensure_tables()
    ensure_schema_compatibility()


class TaskCreate(BaseModel):
    task_type: str = Field(..., examples=["jobs_collect_v1", "deals_scan_v1", "slides_outline_v1"])
    payload_json: str = Field(..., description="Raw JSON string for now (we'll formalize later).")
    model: Optional[str] = None
    idempotency_key: Optional[str] = Field(default=None, max_length=128)
    max_attempts: int = Field(default=3, ge=1, le=10)
    max_cost_usd: Optional[Decimal] = None
    expected_tokens_in: Optional[int] = Field(default=None, ge=0)
    expected_tokens_out: Optional[int] = Field(default=None, ge=0)


class FailureDiagnosticsOut(BaseModel):
    summary: str
    category: str
    source: str
    stage: str
    error_type: Optional[str] = None
    error_message: Optional[str] = None
    upstream_service: Optional[str] = None
    retry_scheduled: bool = False
    retry_at: Optional[datetime] = None
    is_browser_disconnect: bool = False
    is_task_execution_failure: bool = False
    is_queue_enqueue_failure: bool = False
    is_scheduler_runtime_failure: bool = False


class TaskOut(BaseModel):
    id: str
    created_at: datetime
    updated_at: datetime
    status: TaskStatus
    task_type: str
    payload_json: str
    idempotency_key: Optional[str] = None
    model: Optional[str] = None
    tokens_in: Optional[int] = None
    tokens_out: Optional[int] = None
    cost_usd: Optional[Decimal] = None
    error: Optional[str] = None
    max_attempts: int
    next_run_at: Optional[datetime] = None
    max_cost_usd: Optional[Decimal] = None
    expected_tokens_in: Optional[int] = None
    expected_tokens_out: Optional[int] = None
    diagnostics: Optional[FailureDiagnosticsOut] = None


class RunOut(BaseModel):
    id: str
    task_id: str
    attempt: int
    status: RunStatus
    started_at: Optional[datetime] = None
    ended_at: Optional[datetime] = None
    wall_time_ms: Optional[int] = None
    model: Optional[str] = None
    tokens_in: Optional[int] = None
    tokens_out: Optional[int] = None
    cost_usd: Optional[Decimal] = None
    error: Optional[str] = None
    created_at: datetime
    diagnostics: Optional[FailureDiagnosticsOut] = None


class TaskResultOut(BaseModel):
    task_id: str
    artifact_type: str
    content_text: Optional[str] = None
    content_json: Optional[Any] = None
    created_at: datetime


class RuntimeAgentStatusOut(BaseModel):
    name: str
    healthy: bool
    status: str
    last_seen_at: Optional[datetime] = None
    age_seconds: Optional[int] = None
    message: Optional[str] = None


class RuntimeStatusOut(BaseModel):
    captured_at: datetime
    api_healthy: bool
    ready_status: str
    ready_error: Optional[str] = None
    redis_reachable: bool
    queue_depth: Optional[int] = None
    stale_after_seconds: int
    scheduler_heartbeat: RuntimeAgentStatusOut
    worker_heartbeat: RuntimeAgentStatusOut
    last_scheduler_tick_at: Optional[datetime] = None


class HealthOut(BaseModel):
    status: str
    service: str
    utc_now: Optional[str] = None


class ReadyOut(BaseModel):
    status: str
    error: Optional[str] = None


class ScheduleCreate(BaseModel):
    task_type: str
    payload_json: str
    cron: str
    model: Optional[str] = None
    enabled: bool = True
    max_attempts: int = Field(default=3, ge=1, le=10)


class ScheduleOut(BaseModel):
    id: str
    task_type: str
    payload_json: str
    model: Optional[str] = None
    cron: str
    enabled: bool
    max_attempts: int
    last_run_at: Optional[datetime] = None
    next_run_at: datetime
    created_at: datetime
    updated_at: datetime


class ResumeProfileUpsert(BaseModel):
    resume_text: str = Field(..., min_length=1, max_length=500000)
    resume_name: Optional[str] = Field(default=None, max_length=255)
    metadata_json: Optional[dict[str, Any]] = None


class ResumeProfileOut(BaseModel):
    has_resume: bool
    resume_name: Optional[str] = None
    resume_sha256: Optional[str] = None
    resume_char_count: int = 0
    resume_preview: Optional[str] = None
    metadata_json: Optional[dict[str, Any]] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    resume_text: Optional[str] = None


@app.get("/", include_in_schema=False)
def ui() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/observability", include_in_schema=False)
def observability_ui() -> FileResponse:
    return FileResponse(STATIC_DIR / "observability.html")


def _frontend_index_or_503() -> FileResponse:
    index_file = FRONTEND_DIST_DIR / "index.html"
    if not index_file.is_file():
        raise HTTPException(
            status_code=503,
            detail="React app build not found. Build frontend to enable /app routes.",
        )
    return FileResponse(index_file)


@app.get("/legacy", include_in_schema=False)
def legacy_ui() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/legacy/observability", include_in_schema=False)
def legacy_observability_ui() -> FileResponse:
    return FileResponse(STATIC_DIR / "observability.html")


@app.get("/app", include_in_schema=False)
def app_ui_root() -> FileResponse:
    return _frontend_index_or_503()


@app.get("/app/{full_path:path}", include_in_schema=False)
def app_ui_fallback(full_path: str) -> FileResponse:
    # Serve hashed assets and fallback all client routes to SPA index.
    candidate = (FRONTEND_DIST_DIR / full_path).resolve()
    try:
        in_frontend_dist = str(candidate).startswith(str(FRONTEND_DIST_DIR.resolve()))
    except Exception:
        in_frontend_dist = False
    if full_path and in_frontend_dist and candidate.is_file():
        return FileResponse(candidate)
    return _frontend_index_or_503()


@app.get("/health", response_model=HealthOut)
def health() -> HealthOut:
    return HealthOut(status="ok", service="api", utc_now=now_utc().isoformat())


@app.get("/ready", response_model=ReadyOut)
def ready() -> ReadyOut | JSONResponse:
    try:
        with SessionLocal() as db:
            db.execute(text("SELECT 1"))
        redis_conn.ping()
    except Exception as exc:
        return JSONResponse(
            status_code=503,
            content={"status": "not_ready", "error": str(exc)},
        )
    return ReadyOut(status="ready")


@app.get("/telemetry/runtime-status", response_model=RuntimeStatusOut)
def get_runtime_status() -> RuntimeStatusOut:
    captured_at = now_utc()
    ready_status = "ready"
    ready_error: str | None = None
    redis_reachable = False
    queue_depth: int | None = None

    try:
        with SessionLocal() as db:
            db.execute(text("SELECT 1"))
    except Exception as exc:
        ready_status = "not_ready"
        ready_error = str(exc)

    try:
        redis_conn.ping()
        redis_reachable = True
        queue_depth = int(queue.count)
    except Exception as exc:
        redis_reachable = False
        if ready_error is None:
            ready_status = "not_ready"
            ready_error = str(exc)

    stale_after_seconds = DEFAULT_HEARTBEAT_STALE_AFTER_SEC
    tracked = _tracked_agent_names_for_telemetry()
    heartbeats = list_recent_agent_heartbeats(limit=max(len(tracked) + 8, 20))
    latest_by_agent: dict[str, dict[str, Any]] = {}
    for row in heartbeats:
        agent_name = str(row.get("agent_name") or "").strip()
        if agent_name and agent_name not in latest_by_agent:
            latest_by_agent[agent_name] = row

    def _agent_status(agent_name: str) -> RuntimeAgentStatusOut:
        row = latest_by_agent.get(agent_name)
        if row is None:
            return RuntimeAgentStatusOut(
                name=agent_name,
                healthy=False,
                status="missing",
                last_seen_at=None,
                age_seconds=None,
                message="No recent heartbeat recorded.",
            )
        seen_at = _parse_iso_datetime(row.get("last_seen_at"))
        age_seconds = None if seen_at is None else max(int((captured_at - seen_at).total_seconds()), 0)
        healthy = age_seconds is not None and age_seconds <= stale_after_seconds
        status = str(row.get("status") or ("alive" if healthy else "stale"))
        message = "Heartbeat is recent." if healthy else f"Heartbeat age exceeds {stale_after_seconds}s."
        return RuntimeAgentStatusOut(
            name=agent_name,
            healthy=healthy,
            status=status,
            last_seen_at=seen_at,
            age_seconds=age_seconds,
            message=message,
        )

    scheduler_name = os.getenv("SCHEDULER_NAME", "scheduler").strip() or "scheduler"
    worker_name = os.getenv("WORKER_NAME", "worker").strip() or "worker"

    last_scheduler_tick_at: datetime | None = None
    recent_events = list_recent_events(limit=200)
    for row in recent_events:
        if str(row.get("event_type") or "") == "scheduler_tick":
            last_scheduler_tick_at = _parse_iso_datetime(row.get("created_at"))
            if last_scheduler_tick_at is not None:
                break

    return RuntimeStatusOut(
        captured_at=captured_at,
        api_healthy=ready_status == "ready",
        ready_status=ready_status,
        ready_error=ready_error,
        redis_reachable=redis_reachable,
        queue_depth=queue_depth,
        stale_after_seconds=stale_after_seconds,
        scheduler_heartbeat=_agent_status(scheduler_name),
        worker_heartbeat=_agent_status(worker_name),
        last_scheduler_tick_at=last_scheduler_tick_at,
    )


@app.get("/metrics")
def metrics() -> PlainTextResponse:
    with METRICS_LOCK:
        counters = dict(METRICS_COUNTERS)

    with SessionLocal() as db:
        tasks_total = db.query(func.count(Task.id)).scalar() or 0
        runs_total = db.query(func.count(Run.id)).scalar() or 0
        artifacts_total = db.query(func.count(Artifact.id)).scalar() or 0

    lines = [
        "# TYPE mission_control_tasks_created_total counter",
        f"mission_control_tasks_created_total {counters['tasks_created_total']}",
        "# TYPE mission_control_tasks_deduped_total counter",
        f"mission_control_tasks_deduped_total {counters['tasks_deduped_total']}",
        "# TYPE mission_control_tasks_blocked_budget_total counter",
        f"mission_control_tasks_blocked_budget_total {counters['tasks_blocked_budget_total']}",
        "# TYPE mission_control_task_create_rate_limited_total counter",
        f"mission_control_task_create_rate_limited_total {counters['task_create_rate_limited_total']}",
        "# TYPE mission_control_auth_rejected_total counter",
        f"mission_control_auth_rejected_total {counters['auth_rejected_total']}",
        "# TYPE mission_control_schedules_created_total counter",
        f"mission_control_schedules_created_total {counters['schedules_created_total']}",
        "# TYPE mission_control_tasks_db_total gauge",
        f"mission_control_tasks_db_total {tasks_total}",
        "# TYPE mission_control_runs_db_total gauge",
        f"mission_control_runs_db_total {runs_total}",
        "# TYPE mission_control_artifacts_db_total gauge",
        f"mission_control_artifacts_db_total {artifacts_total}",
    ]
    return PlainTextResponse("\n".join(lines) + "\n")


@app.post("/tasks", response_model=TaskOut)
def create_task(req: TaskCreate, request: Request):
    if req.model is not None and not validate_model(req.model):
        available = get_available_models()
        raise HTTPException(
            status_code=400,
            detail=f"Invalid model '{req.model}'. Available models: {available}"
        )

    identity = request.headers.get("X-API-Key") if API_KEY else None
    if identity is None:
        identity = request.client.host if request.client is not None else "anonymous"
    allowed_rate, _ = consume_create_rate_limit_token(identity)
    if not allowed_rate:
        increment_metric("task_create_rate_limited_total")
        raise HTTPException(status_code=429, detail="Task creation rate limited")

    with SessionLocal() as db:
        if req.idempotency_key:
            existing = _find_idempotent_task(db, req.task_type, req.idempotency_key)
            if existing is not None:
                increment_metric("tasks_deduped_total")
                log_event(
                    "task_deduplicated",
                    task_id=existing.id,
                    task_type=req.task_type,
                    idempotency_key=req.idempotency_key,
                )
                return _task_to_out(existing)

        is_available, spend, remaining = enforce_budget(db, BUDGET_BUFFER_USD)
        task_id = str(uuid.uuid4())
        if not is_available:
            error_msg = (
                f"Daily budget blocked (${spend:.4f} spent, "
                f"${remaining:.4f} remaining, ${BUDGET_BUFFER_USD:.4f} buffer)"
            )
            t = Task(
                id=task_id,
                created_at=now_utc(),
                updated_at=now_utc(),
                status=TaskStatus.blocked_budget,
                task_type=req.task_type,
                payload_json=req.payload_json,
                idempotency_key=req.idempotency_key,
                model=req.model,
                error=error_msg,
                max_attempts=req.max_attempts,
                max_cost_usd=to_decimal_8(req.max_cost_usd) if req.max_cost_usd is not None else None,
                expected_tokens_in=req.expected_tokens_in,
                expected_tokens_out=req.expected_tokens_out,
            )
            db.add(t)
            blocked_run = Run(
                id=str(uuid.uuid4()),
                task_id=task_id,
                attempt=1,
                status=RunStatus.failed,
                started_at=now_utc(),
                ended_at=now_utc(),
                wall_time_ms=0,
                error=error_msg,
                created_at=now_utc(),
            )
            db.add(blocked_run)
            try:
                db.commit()
            except IntegrityError:
                db.rollback()
                if req.idempotency_key:
                    existing = _find_idempotent_task(db, req.task_type, req.idempotency_key)
                    if existing is not None:
                        increment_metric("tasks_deduped_total")
                        log_event(
                            "task_deduplicated_race",
                            task_id=existing.id,
                            task_type=req.task_type,
                            idempotency_key=req.idempotency_key,
                        )
                        return _task_to_out(existing)
                raise HTTPException(status_code=409, detail="Task idempotency conflict")
            db.refresh(t)
            increment_metric("tasks_blocked_budget_total")
            return _task_to_out(t)

        chosen_model = choose_model(
            task_type=req.task_type,
            payload_json=req.payload_json,
            remaining_budget_usd=float(remaining),
            user_override=req.model,
        )

        t = Task(
            id=task_id,
            created_at=now_utc(),
            updated_at=now_utc(),
            status=TaskStatus.queued,
            task_type=req.task_type,
            payload_json=req.payload_json,
            idempotency_key=req.idempotency_key,
            model=chosen_model,
            max_attempts=req.max_attempts,
            next_run_at=None,
            max_cost_usd=to_decimal_8(req.max_cost_usd) if req.max_cost_usd is not None else None,
            expected_tokens_in=req.expected_tokens_in,
            expected_tokens_out=req.expected_tokens_out,
        )
        db.add(t)
        try:
            db.commit()
        except IntegrityError:
            db.rollback()
            if req.idempotency_key:
                existing = _find_idempotent_task(db, req.task_type, req.idempotency_key)
                if existing is not None:
                    increment_metric("tasks_deduped_total")
                    log_event(
                        "task_deduplicated_race",
                        task_id=existing.id,
                        task_type=req.task_type,
                        idempotency_key=req.idempotency_key,
                    )
                    return _task_to_out(existing)
            raise HTTPException(status_code=409, detail="Task idempotency conflict")

    try:
        queue.enqueue("worker.run_task", task_id)
        increment_metric("tasks_created_total")
        log_event("task_queued", task_id=task_id, task_type=req.task_type, model=chosen_model)
    except Exception as exc:
        schedule_enqueue_recovery(task_id, source="api", error=exc)
        log_event("task_queue_recovery_scheduled", task_id=task_id, task_type=req.task_type, model=chosen_model)

    with SessionLocal() as db:
        t2 = db.get(Task, task_id)
        return _task_to_out(t2)


@app.get("/tasks", response_model=List[TaskOut])
def list_tasks(limit: int = 50):
    with SessionLocal() as db:
        rows = db.query(Task).order_by(Task.created_at.desc()).limit(limit).all()
        debug_by_task_id = _latest_debug_artifacts_by_task_id(db, {row.id for row in rows})
        return [_task_to_out(task, debug_by_task_id.get(task.id)) for task in rows]


@app.get("/tasks/{task_id}", response_model=TaskOut)
def get_task(task_id: str):
    with SessionLocal() as db:
        t = db.get(Task, task_id)
        if not t:
            raise HTTPException(status_code=404, detail="Task not found")
        debug_by_task_id = _latest_debug_artifacts_by_task_id(db, {t.id})
        return _task_to_out(t, debug_by_task_id.get(t.id))


@app.get("/tasks/{task_id}/result", response_model=TaskResultOut)
def get_task_result(task_id: str):
    with SessionLocal() as db:
        artifact = (
            db.query(Artifact)
            .filter(Artifact.task_id == task_id, Artifact.artifact_type == RESULT_ARTIFACT_TYPE)
            .order_by(Artifact.created_at.desc(), Artifact.id.desc())
            .first()
        )
        if artifact is None:
            artifact = (
                db.query(Artifact)
                .filter(Artifact.task_id == task_id)
                .order_by(Artifact.created_at.desc(), Artifact.id.desc())
                .first()
            )
        if artifact is None:
            raise HTTPException(status_code=404, detail=f"No result found for task '{task_id}'")

        return TaskResultOut(
            task_id=artifact.task_id,
            artifact_type=artifact.artifact_type,
            content_text=artifact.content_text,
            content_json=artifact.content_json,
            created_at=artifact.created_at,
        )


@app.get("/runs", response_model=List[RunOut])
def list_runs(limit: int = 50):
    with SessionLocal() as db:
        rows = db.query(Run).order_by(Run.created_at.desc()).limit(limit).all()
        debug_by_run_id = _latest_debug_artifacts_by_run_id(db, {row.id for row in rows})
        return [_run_to_out(run, debug_by_run_id.get(run.id)) for run in rows]


@app.get("/tasks/{task_id}/runs", response_model=List[RunOut])
def get_task_runs(task_id: str, limit: int = 50):
    with SessionLocal() as db:
        t = db.get(Task, task_id)
        if not t:
            raise HTTPException(status_code=404, detail="Task not found")
        runs = db.query(Run).filter(Run.task_id == task_id).order_by(Run.attempt.desc()).limit(limit).all()
        debug_by_run_id = _latest_debug_artifacts_by_run_id(db, {row.id for row in runs})
        return [_run_to_out(run, debug_by_run_id.get(run.id)) for run in runs]


@app.post("/schedules", response_model=ScheduleOut)
def create_schedule(req: ScheduleCreate):
    if req.model is not None and not validate_model(req.model):
        available = get_available_models()
        raise HTTPException(
            status_code=400,
            detail=f"Invalid model '{req.model}'. Available models: {available}"
        )

    try:
        next_run = croniter(req.cron, now_utc()).get_next(datetime)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid cron expression")

    if next_run.tzinfo is None:
        next_run = next_run.replace(tzinfo=timezone.utc)

    schedule = Schedule(
        id=str(uuid.uuid4()),
        task_type=req.task_type,
        payload_json=req.payload_json,
        model=req.model,
        cron=req.cron,
        enabled=req.enabled,
        max_attempts=req.max_attempts,
        last_run_at=None,
        next_run_at=next_run,
        created_at=now_utc(),
        updated_at=now_utc(),
    )

    with SessionLocal() as db:
        db.add(schedule)
        db.commit()
        db.refresh(schedule)

    increment_metric("schedules_created_total")
    return ScheduleOut(**schedule.__dict__)


@app.get("/schedules", response_model=List[ScheduleOut])
def list_schedules(limit: int = 50):
    with SessionLocal() as db:
        rows = db.query(Schedule).order_by(Schedule.created_at.desc()).limit(limit).all()
        return [ScheduleOut(**s.__dict__) for s in rows]


def _extract_resume_text_from_upload(filename: str, file_bytes: bytes) -> str:
    lowered = (filename or "").strip().lower()
    extension = Path(lowered).suffix

    if extension == ".pdf":
        try:
            from pypdf import PdfReader  # type: ignore
        except Exception as exc:
            raise ValueError("PDF parsing dependency is unavailable on the API service") from exc

        try:
            reader = PdfReader(io.BytesIO(file_bytes))
        except Exception as exc:
            raise ValueError(f"Unable to parse PDF file: {exc}") from exc

        chunks: list[str] = []
        for page in reader.pages:
            try:
                page_text = page.extract_text() or ""
            except Exception:
                page_text = ""
            if page_text.strip():
                chunks.append(page_text.strip())
        text = "\n\n".join(chunks).strip()
        if not text:
            raise ValueError("No extractable text was found in the PDF")
        return text

    if extension == ".docx":
        try:
            from docx import Document  # type: ignore
        except Exception as exc:
            raise ValueError("DOCX parsing dependency is unavailable on the API service") from exc

        try:
            document = Document(io.BytesIO(file_bytes))
        except Exception as exc:
            raise ValueError(f"Unable to parse DOCX file: {exc}") from exc

        lines = [paragraph.text.strip() for paragraph in document.paragraphs if paragraph.text and paragraph.text.strip()]
        text = "\n".join(lines).strip()
        if not text:
            raise ValueError("No extractable text was found in the DOCX file")
        return text

    if extension == ".doc":
        raise ValueError("Legacy .doc is not supported. Please upload .docx or PDF.")

    decoded = file_bytes.decode("utf-8", errors="ignore").strip()
    if not decoded:
        raise ValueError("Uploaded file has no readable text content.")
    return decoded


@app.get("/profile/resume", response_model=ResumeProfileOut)
def get_resume_profile_route(include_text: bool = False):
    try:
        row = get_candidate_resume_profile(include_text=include_text)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to read resume profile: {exc}")
    if row is None:
        return ResumeProfileOut(has_resume=False)
    return ResumeProfileOut(has_resume=True, **row)


@app.post("/profile/resume/upload", response_model=ResumeProfileOut)
async def upload_resume_profile_route(file: UploadFile = File(...)):
    filename = (file.filename or "resume").strip() or "resume"
    raw = await file.read()
    if not raw:
        raise HTTPException(status_code=400, detail="Uploaded file is empty")
    if len(raw) > 12 * 1024 * 1024:
        raise HTTPException(status_code=400, detail="Uploaded file is too large (max 12MB)")

    try:
        resume_text = _extract_resume_text_from_upload(filename, raw)
        row = upsert_candidate_resume_profile(
            resume_text=resume_text,
            resume_name=filename,
            metadata_json={
                "uploaded_filename": filename,
                "uploaded_size_bytes": len(raw),
                "content_type": file.content_type,
            },
        )
        log_event(
            "resume_profile_uploaded",
            resume_name=row.get("resume_name"),
            resume_char_count=row.get("resume_char_count"),
            content_type=file.content_type,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to process uploaded resume: {exc}")
    return ResumeProfileOut(has_resume=True, **row)


@app.put("/profile/resume", response_model=ResumeProfileOut)
def put_resume_profile_route(req: ResumeProfileUpsert):
    resume_text = req.resume_text.strip()
    if not resume_text:
        raise HTTPException(status_code=400, detail="resume_text cannot be empty")
    try:
        row = upsert_candidate_resume_profile(
            resume_text=resume_text,
            resume_name=req.resume_name,
            metadata_json=req.metadata_json,
        )
        log_event(
            "resume_profile_upserted",
            resume_name=row.get("resume_name"),
            resume_char_count=row.get("resume_char_count"),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to persist resume profile: {exc}")
    return ResumeProfileOut(has_resume=True, **row)


@app.delete("/profile/resume")
def delete_resume_profile_route():
    try:
        deleted = delete_candidate_resume_profile()
        if deleted:
            log_event("resume_profile_deleted")
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to delete resume profile: {exc}")
    return {"deleted": deleted}


class StatsToday(BaseModel):
    spend_usd: Decimal
    budget_usd: Decimal
    remaining_usd: Decimal
    buffer_usd: Decimal
    runs_count: int
    success_count: int
    failed_count: int


class TelemetryRange(BaseModel):
    start: datetime
    end: datetime


class SystemMetricsOut(BaseModel):
    id: str
    cpu_percent: Optional[float] = None
    memory_percent: Optional[float] = None
    disk_percent: Optional[float] = None
    load_avg_json: Optional[List[float]] = None
    created_at: datetime


class PlannerConfigOut(BaseModel):
    enabled: bool
    execution_enabled: bool
    require_approval: bool
    approved: bool
    interval_sec: int
    max_create_per_cycle: int
    max_execute_per_cycle: int
    max_pending_tasks: int
    failure_lookback_minutes: int
    failure_alert_count_threshold: int
    failure_alert_rate_threshold: float
    stale_task_age_seconds: int
    execute_task_cooldown_seconds: int
    health_cpu_max_percent: float
    health_memory_max_percent: float
    health_disk_max_percent: float
    cost_budget_usd: Optional[float] = None
    token_budget: Optional[int] = None
    create_task_cooldown_seconds: int
    create_task_max_attempts: int
    updated_at: str
    updated_by: Optional[str] = None


class PlannerConfigPatch(BaseModel):
    enabled: Optional[bool] = None
    execution_enabled: Optional[bool] = None
    require_approval: Optional[bool] = None
    approved: Optional[bool] = None
    interval_sec: Optional[int] = Field(default=None, ge=30)
    max_create_per_cycle: Optional[int] = Field(default=None, ge=0)
    max_execute_per_cycle: Optional[int] = Field(default=None, ge=0)
    max_pending_tasks: Optional[int] = Field(default=None, ge=1)
    failure_lookback_minutes: Optional[int] = Field(default=None, ge=1)
    failure_alert_count_threshold: Optional[int] = Field(default=None, ge=1)
    failure_alert_rate_threshold: Optional[float] = Field(default=None, ge=0.0)
    stale_task_age_seconds: Optional[int] = Field(default=None, ge=30)
    execute_task_cooldown_seconds: Optional[int] = Field(default=None, ge=30)
    health_cpu_max_percent: Optional[float] = Field(default=None, ge=1.0)
    health_memory_max_percent: Optional[float] = Field(default=None, ge=1.0)
    health_disk_max_percent: Optional[float] = Field(default=None, ge=1.0)
    cost_budget_usd: Optional[float] = Field(default=None, ge=0.0)
    token_budget: Optional[int] = Field(default=None, ge=0)
    create_task_cooldown_seconds: Optional[int] = Field(default=None, ge=60)
    create_task_max_attempts: Optional[int] = Field(default=None, ge=1)


class PlannerTemplateOut(BaseModel):
    id: str
    name: str
    task_type: str
    payload_json: str
    model: Optional[str] = None
    max_attempts: int
    min_interval_seconds: int
    enabled: bool
    priority: int
    metadata_json: Optional[dict[str, Any]] = None
    created_at: str
    updated_at: str


class PlannerTemplateCreate(BaseModel):
    name: str
    task_type: str
    payload_json: str
    model: Optional[str] = None
    max_attempts: int = Field(default=3, ge=1, le=20)
    min_interval_seconds: int = Field(default=300, ge=60, le=86400)
    enabled: bool = True
    priority: int = Field(default=100, ge=-1000, le=1000)
    metadata_json: Optional[dict[str, Any]] = None
    id: Optional[str] = None


class PlannerTemplatePatch(BaseModel):
    name: Optional[str] = None
    task_type: Optional[str] = None
    payload_json: Optional[str] = None
    model: Optional[str] = None
    max_attempts: Optional[int] = Field(default=None, ge=1, le=20)
    min_interval_seconds: Optional[int] = Field(default=None, ge=60, le=86400)
    enabled: Optional[bool] = None
    priority: Optional[int] = Field(default=None, ge=-1000, le=1000)
    metadata_json: Optional[dict[str, Any]] = None


class PlannerRtxPresetCreate(BaseModel):
    interval_seconds: int = Field(default=300, ge=60, le=86400)
    gpu_max_price: Optional[float] = Field(default=None, gt=0)
    pc_max_price: Optional[float] = Field(default=None, gt=0)
    enabled: bool = True


ALLOWED_JOB_WATCHER_SOURCES = {"linkedin", "indeed", "glassdoor", "handshake"}
ALLOWED_JOB_WATCHER_WORK_MODES = {"remote", "hybrid", "onsite"}
ALLOWED_JOB_WATCHER_FRESHNESS = {"off", "prefer_recent", "strong_prefer_recent"}


class PlannerJobsPresetCreate(BaseModel):
    interval_seconds: int = Field(default=300, ge=60, le=86400)
    desired_title: Optional[str] = None
    desired_titles: Optional[List[str]] = None
    keywords: Optional[List[str]] = None
    excluded_keywords: Optional[List[str]] = None
    preferred_locations: Optional[List[str]] = None
    remote_preference: Optional[List[str]] = None
    minimum_salary: Optional[float] = Field(default=None, gt=0)
    experience_level: Optional[str] = None
    enabled_sources: Optional[List[str]] = None
    result_limit_per_source: Optional[int] = Field(default=None, ge=1, le=100)
    shortlist_count: Optional[int] = Field(default=None, ge=1, le=10)
    freshness_preference: Optional[str] = None
    desired_salary_min: Optional[float] = Field(default=None, gt=0)
    desired_salary_max: Optional[float] = Field(default=None, gt=0)
    experience_levels: Optional[List[str]] = None
    clearance_required: Optional[bool] = None
    location: Optional[str] = None
    boards: Optional[List[str]] = None
    enabled: bool = True

    @staticmethod
    def _normalize_text_list(value: Any, *, lower: bool = False) -> Optional[List[str]]:
        if value is None:
            return None
        if isinstance(value, str):
            value = [value]
        if not isinstance(value, list):
            raise ValueError("must be an array of strings")

        output: List[str] = []
        seen: set[str] = set()
        for item in value:
            if not isinstance(item, str):
                continue
            text = item.strip()
            if not text:
                continue
            normalized = text.lower() if lower else text
            key = normalized.lower()
            if key in seen:
                continue
            seen.add(key)
            output.append(normalized)
        return output

    @staticmethod
    def _normalize_work_mode(value: str) -> str:
        low = value.strip().lower().replace("_", "-")
        if low in {"on-site", "onsite"}:
            return "onsite"
        if low in {"remote", "hybrid"}:
            return low
        raise ValueError("remote_preference must contain only remote, hybrid, or onsite")

    @staticmethod
    def _normalize_experience_level(value: str) -> str:
        low = value.strip().lower()
        if low in {"intern", "internship", "co-op", "coop"}:
            return "internship"
        if low in {"entry", "entry-level", "junior", "new grad", "associate"}:
            return "entry"
        if low in {"mid", "mid-level", "intermediate"}:
            return "mid"
        if low in {"senior", "lead", "staff", "principal", "manager", "director"}:
            return "senior"
        raise ValueError("experience level must map to internship, entry, mid, or senior")

    @field_validator("desired_title", "location", mode="before")
    @classmethod
    def _strip_text(cls, value: Any) -> Any:
        if value is None:
            return None
        if not isinstance(value, str):
            raise ValueError("must be a string")
        text = value.strip()
        return text or None

    @field_validator(
        "desired_titles",
        "keywords",
        "excluded_keywords",
        "preferred_locations",
        mode="before",
    )
    @classmethod
    def _validate_text_lists(cls, value: Any) -> Any:
        return cls._normalize_text_list(value)

    @field_validator("boards", "enabled_sources", mode="before")
    @classmethod
    def _validate_sources(cls, value: Any) -> Any:
        normalized = cls._normalize_text_list(value, lower=True)
        if normalized is None:
            return None
        invalid = [row for row in normalized if row not in ALLOWED_JOB_WATCHER_SOURCES]
        if invalid:
            raise ValueError(
                "enabled sources must be one or more of: linkedin, indeed, glassdoor, handshake"
            )
        return normalized

    @field_validator("remote_preference", mode="before")
    @classmethod
    def _validate_remote_preference(cls, value: Any) -> Any:
        normalized = cls._normalize_text_list(value, lower=True)
        if normalized is None:
            return None
        output: List[str] = []
        for item in normalized:
            output.append(cls._normalize_work_mode(item))
        invalid = [row for row in output if row not in ALLOWED_JOB_WATCHER_WORK_MODES]
        if invalid:
            raise ValueError("remote_preference must contain only remote, hybrid, or onsite")
        deduped: List[str] = []
        seen: set[str] = set()
        for item in output:
            if item in seen:
                continue
            seen.add(item)
            deduped.append(item)
        return deduped

    @field_validator("experience_level", mode="before")
    @classmethod
    def _validate_experience_level(cls, value: Any) -> Any:
        if value is None:
            return None
        if not isinstance(value, str):
            raise ValueError("must be a string")
        return cls._normalize_experience_level(value)

    @field_validator("experience_levels", mode="before")
    @classmethod
    def _validate_experience_levels(cls, value: Any) -> Any:
        normalized = cls._normalize_text_list(value, lower=True)
        if normalized is None:
            return None
        output: List[str] = []
        for item in normalized:
            output.append(cls._normalize_experience_level(item))
        deduped: List[str] = []
        seen: set[str] = set()
        for item in output:
            if item in seen:
                continue
            seen.add(item)
            deduped.append(item)
        return deduped

    @field_validator("freshness_preference", mode="before")
    @classmethod
    def _validate_freshness_preference(cls, value: Any) -> Any:
        if value is None:
            return None
        if not isinstance(value, str):
            raise ValueError("must be a string")
        normalized = value.strip().lower().replace("-", "_").replace(" ", "_")
        if normalized not in ALLOWED_JOB_WATCHER_FRESHNESS:
            raise ValueError("freshness_preference must be one of: off, prefer_recent, strong_prefer_recent")
        return normalized

    @model_validator(mode="after")
    def _validate_salary_bounds(self) -> "PlannerJobsPresetCreate":
        salary_min = self.minimum_salary if self.minimum_salary is not None else self.desired_salary_min
        salary_max = self.desired_salary_max
        if salary_min is not None and salary_max is not None and salary_max < salary_min:
            raise ValueError("desired_salary_max must be greater than or equal to desired_salary_min/minimum_salary")
        return self


class WatcherRunSummaryOut(BaseModel):
    task_id: str
    task_status: str
    task_updated_at: datetime
    task_created_at: datetime
    error: Optional[str] = None
    run_id: Optional[str] = None
    run_attempt: Optional[int] = None
    run_status: Optional[str] = None
    run_started_at: Optional[datetime] = None
    run_ended_at: Optional[datetime] = None
    run_wall_time_ms: Optional[int] = None


class WatcherOutcomeSummaryOut(BaseModel):
    status: Optional[str] = None
    message: Optional[str] = None
    artifact_type: Optional[str] = None
    created_at: Optional[datetime] = None


class WatcherOut(BaseModel):
    id: str
    name: str
    task_type: str
    payload_json: str
    model: Optional[str] = None
    max_attempts: int
    interval_seconds: int
    min_interval_seconds: int
    enabled: bool
    priority: int
    notification_behavior: Optional[dict[str, Any]] = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    created_at: str
    updated_at: str
    last_run_summary: Optional[WatcherRunSummaryOut] = None
    last_outcome_summary: Optional[WatcherOutcomeSummaryOut] = None


class WatcherCreate(BaseModel):
    name: str
    task_type: str
    payload_json: str
    model: Optional[str] = None
    max_attempts: int = Field(default=3, ge=1, le=20)
    interval_seconds: int = Field(default=300, ge=60, le=86400)
    min_interval_seconds: Optional[int] = Field(default=None, ge=60, le=86400)
    enabled: bool = True
    priority: int = Field(default=100, ge=-1000, le=1000)
    notification_behavior: Optional[dict[str, Any]] = None
    metadata: Optional[dict[str, Any]] = None
    id: Optional[str] = None


class WatcherPatch(BaseModel):
    name: Optional[str] = None
    task_type: Optional[str] = None
    payload_json: Optional[str] = None
    model: Optional[str] = None
    max_attempts: Optional[int] = Field(default=None, ge=1, le=20)
    interval_seconds: Optional[int] = Field(default=None, ge=60, le=86400)
    min_interval_seconds: Optional[int] = Field(default=None, ge=60, le=86400)
    enabled: Optional[bool] = None
    priority: Optional[int] = Field(default=None, ge=-1000, le=1000)
    notification_behavior: Optional[dict[str, Any]] = None
    metadata: Optional[dict[str, Any]] = None


def _status_to_str(value: Any) -> str:
    if value is None:
        return "unknown"
    if hasattr(value, "value"):
        return str(getattr(value, "value"))
    return str(value)


def _parse_payload_obj(payload_json: Any) -> dict[str, Any]:
    if not isinstance(payload_json, str) or not payload_json:
        return {}
    try:
        parsed = json.loads(payload_json)
    except json.JSONDecodeError:
        return {}
    if isinstance(parsed, dict):
        return parsed
    return {}


def _normalize_metadata_obj(metadata_json: Any) -> dict[str, Any]:
    if metadata_json is None:
        return {}
    if isinstance(metadata_json, dict):
        return dict(metadata_json)
    if isinstance(metadata_json, str):
        try:
            parsed = json.loads(metadata_json)
        except json.JSONDecodeError:
            return {}
        if isinstance(parsed, dict):
            return parsed
    return {}


def _coerce_notification_behavior(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    if isinstance(value, dict):
        return value
    return None


def _compact_preview_text(value: str, limit: int = 220) -> str:
    collapsed = " ".join(value.split())
    if len(collapsed) <= limit:
        return collapsed
    return f"{collapsed[: max(limit - 1, 1)].rstrip()}…"


def _artifact_preview(artifact: Artifact) -> str | None:
    if artifact.content_text:
        return _compact_preview_text(str(artifact.content_text))
    if isinstance(artifact.content_json, dict):
        for key in ("summary", "message", "title", "error", "status"):
            candidate = artifact.content_json.get(key)
            if isinstance(candidate, str) and candidate.strip():
                return _compact_preview_text(candidate.strip())
        try:
            return _compact_preview_text(
                json.dumps(artifact.content_json, separators=(",", ":"), ensure_ascii=True)
            )
        except Exception:
            return None
    return None


def _resolve_watcher_interval(
    *,
    min_interval_seconds: Optional[int],
    interval_seconds: Optional[int],
    default_value: int,
) -> int:
    if min_interval_seconds is not None:
        return max(int(min_interval_seconds), 60)
    if interval_seconds is not None:
        return max(int(interval_seconds), 60)
    return max(int(default_value), 60)


def _build_watcher_summaries(
    db,
    template_rows: List[dict[str, Any]],
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    if not template_rows:
        return {}, {}

    watcher_counts_by_task_type: dict[str, int] = defaultdict(int)
    task_types: set[str] = set()
    for row in template_rows:
        task_type = str(row.get("task_type") or "").strip()
        if not task_type:
            continue
        task_types.add(task_type)
        watcher_counts_by_task_type[task_type] += 1

    if not task_types:
        return {}, {}

    recent_tasks = (
        db.query(Task)
        .filter(Task.task_type.in_(list(task_types)))
        .order_by(Task.updated_at.desc(), Task.created_at.desc())
        .limit(5000)
        .all()
    )

    tasks_by_type: dict[str, list[Task]] = defaultdict(list)
    task_template_id: dict[str, str | None] = {}
    for task in recent_tasks:
        tasks_by_type[str(task.task_type)].append(task)
        payload = _parse_payload_obj(task.payload_json)
        marker = payload.get("planner_template_id")
        if isinstance(marker, str) and marker.strip():
            task_template_id[task.id] = marker.strip()
        else:
            task_template_id[task.id] = None

    selected_task_by_watcher_id: dict[str, Task] = {}
    for row in template_rows:
        watcher_id = str(row.get("id") or "")
        task_type = str(row.get("task_type") or "")
        if not watcher_id or not task_type:
            continue
        task_rows = tasks_by_type.get(task_type) or []
        direct_match = next((task for task in task_rows if task_template_id.get(task.id) == watcher_id), None)
        if direct_match is not None:
            selected_task_by_watcher_id[watcher_id] = direct_match
            continue
        if watcher_counts_by_task_type.get(task_type, 0) == 1 and task_rows:
            selected_task_by_watcher_id[watcher_id] = task_rows[0]

    task_ids = [task.id for task in selected_task_by_watcher_id.values()]
    if not task_ids:
        return {}, {}

    runs = (
        db.query(Run)
        .filter(Run.task_id.in_(task_ids))
        .order_by(Run.created_at.desc(), Run.attempt.desc())
        .all()
    )
    latest_run_by_task_id: dict[str, Run] = {}
    for run in runs:
        if run.task_id not in latest_run_by_task_id:
            latest_run_by_task_id[run.task_id] = run

    artifacts = (
        db.query(Artifact)
        .filter(Artifact.task_id.in_(task_ids))
        .order_by(Artifact.created_at.desc())
        .all()
    )
    latest_artifact_by_task_id: dict[str, Artifact] = {}
    result_artifact_by_task_id: dict[str, Artifact] = {}
    for artifact in artifacts:
        if artifact.task_id not in latest_artifact_by_task_id:
            latest_artifact_by_task_id[artifact.task_id] = artifact
        if artifact.artifact_type == RESULT_ARTIFACT_TYPE and artifact.task_id not in result_artifact_by_task_id:
            result_artifact_by_task_id[artifact.task_id] = artifact

    run_summary_by_watcher_id: dict[str, dict[str, Any]] = {}
    outcome_summary_by_watcher_id: dict[str, dict[str, Any]] = {}
    for watcher_id, task in selected_task_by_watcher_id.items():
        latest_run = latest_run_by_task_id.get(task.id)
        run_summary_by_watcher_id[watcher_id] = {
            "task_id": task.id,
            "task_status": _status_to_str(task.status),
            "task_updated_at": task.updated_at,
            "task_created_at": task.created_at,
            "error": task.error,
            "run_id": latest_run.id if latest_run else None,
            "run_attempt": latest_run.attempt if latest_run else None,
            "run_status": _status_to_str(latest_run.status) if latest_run else None,
            "run_started_at": latest_run.started_at if latest_run else None,
            "run_ended_at": latest_run.ended_at if latest_run else None,
            "run_wall_time_ms": latest_run.wall_time_ms if latest_run else None,
        }

        artifact = result_artifact_by_task_id.get(task.id) or latest_artifact_by_task_id.get(task.id)
        if artifact is not None:
            outcome_summary_by_watcher_id[watcher_id] = {
                "status": _status_to_str(task.status),
                "message": _artifact_preview(artifact),
                "artifact_type": artifact.artifact_type,
                "created_at": artifact.created_at,
            }
        else:
            outcome_summary_by_watcher_id[watcher_id] = {
                "status": _status_to_str(task.status),
                "message": _compact_preview_text(task.error) if task.error else None,
                "artifact_type": None,
                "created_at": task.updated_at,
            }

    return run_summary_by_watcher_id, outcome_summary_by_watcher_id


def _template_row_to_watcher(
    row: dict[str, Any],
    *,
    last_run_summary: Optional[dict[str, Any]] = None,
    last_outcome_summary: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    metadata_full = _normalize_metadata_obj(row.get("metadata_json"))
    notification_behavior = _coerce_notification_behavior(metadata_full.get("notification_behavior"))
    metadata_without_notification = dict(metadata_full)
    metadata_without_notification.pop("notification_behavior", None)
    interval = max(int(row.get("min_interval_seconds") or 60), 60)
    return {
        "id": row.get("id"),
        "name": row.get("name"),
        "task_type": row.get("task_type"),
        "payload_json": row.get("payload_json"),
        "model": row.get("model"),
        "max_attempts": int(row.get("max_attempts") or 1),
        "interval_seconds": interval,
        "min_interval_seconds": interval,
        "enabled": bool(row.get("enabled")),
        "priority": int(row.get("priority") or 0),
        "notification_behavior": notification_behavior,
        "metadata": metadata_without_notification,
        "created_at": row.get("created_at"),
        "updated_at": row.get("updated_at"),
        "last_run_summary": last_run_summary,
        "last_outcome_summary": last_outcome_summary,
    }


def _hydrate_watcher_rows(template_rows: List[dict[str, Any]]) -> list[dict[str, Any]]:
    if not template_rows:
        return []
    with SessionLocal() as db:
        run_summaries, outcome_summaries = _build_watcher_summaries(db, template_rows)
    return [
        _template_row_to_watcher(
            row,
            last_run_summary=run_summaries.get(str(row.get("id") or "")),
            last_outcome_summary=outcome_summaries.get(str(row.get("id") or "")),
        )
        for row in template_rows
    ]


@app.get("/stats/today", response_model=StatsToday)
def get_stats_today():
    """
    Return today's cost stats and run counts.
    Computed from runs in the current operational day window.
    """
    with SessionLocal() as db:
        start, end = current_operational_day_window_utc()

        spend = today_spend_usd(db)

        runs_count = (
            db.query(func.count(Run.id))
            .filter(Run.started_at >= start, Run.started_at < end)
            .scalar()
            or 0
        )
        success_count = (
            db.query(func.count(Run.id))
            .filter(Run.started_at >= start, Run.started_at < end, Run.status == RunStatus.success)
            .scalar() or 0
        )
        failed_count = (
            db.query(func.count(Run.id))
            .filter(Run.started_at >= start, Run.started_at < end, Run.status == RunStatus.failed)
            .scalar() or 0
        )

        remaining = DAILY_BUDGET_USD - spend

        return StatsToday(
            spend_usd=to_decimal_8(spend),
            budget_usd=DAILY_BUDGET_USD,
            remaining_usd=to_decimal_8(remaining),
            buffer_usd=BUDGET_BUFFER_USD,
            runs_count=runs_count,
            success_count=success_count,
            failed_count=failed_count,
        )


@app.get("/telemetry/events")
def get_recent_events(limit: int = 100):
    return list_recent_events(limit=limit)


@app.get("/telemetry/ai-usage")
def get_recent_ai_usage(limit: int = 100):
    return list_recent_ai_usage(limit=limit)


@app.get("/telemetry/ai-usage/today")
def get_today_ai_usage(limit: int = 100):
    return list_ai_usage_today(limit=limit)


@app.get("/telemetry/ai-usage/summary")
def get_ai_usage_summary_route(start: Optional[datetime] = None, end: Optional[datetime] = None):
    if start is None and end is None:
        resolved_start, resolved_end = current_operational_day_window_utc(now_utc())
    elif start is not None and end is None:
        resolved_start = start
        resolved_end = start + timedelta(days=1)
    elif start is None and end is not None:
        resolved_end = end
        resolved_start = end - timedelta(days=1)
    else:
        resolved_start = start
        resolved_end = end
    if resolved_start.tzinfo is None:
        resolved_start = resolved_start.replace(tzinfo=timezone.utc)
    if resolved_end.tzinfo is None:
        resolved_end = resolved_end.replace(tzinfo=timezone.utc)
    if resolved_end <= resolved_start:
        raise HTTPException(status_code=400, detail="end must be after start")
    return get_ai_usage_summary(resolved_start, resolved_end)


@app.get("/telemetry/system-metrics/latest", response_model=Optional[SystemMetricsOut])
def get_latest_system_metrics_route():
    latest = get_latest_system_metrics()
    if latest is None:
        return None
    return SystemMetricsOut(**latest)


@app.get("/telemetry/system-metrics", response_model=List[SystemMetricsOut])
def get_system_metrics_route(limit: int = 100):
    rows = list_system_metrics(limit=limit)
    return [SystemMetricsOut(**row) for row in rows]


@app.get("/telemetry/heartbeats")
def get_agent_heartbeats(limit: int = 100):
    return list_recent_agent_heartbeats(limit=limit)


@app.get("/telemetry/heartbeats/stale")
def get_stale_agent_heartbeats(
    stale_after_seconds: int = DEFAULT_HEARTBEAT_STALE_AFTER_SEC,
    limit: int = 1000,
    tracked_only: bool = True,
    include_historical: bool = False,
):
    safe_limit = max(1, min(int(limit), 5000))
    safe_stale_after_seconds = max(int(stale_after_seconds), 1)
    now = now_utc()
    if not tracked_only:
        return list_stale_agent_heartbeats(
            stale_after_seconds=safe_stale_after_seconds,
            now=now,
            limit=safe_limit,
        )

    tracked = _tracked_agent_names_for_telemetry()
    tracked_rows = list_stale_agent_heartbeats(
        stale_after_seconds=safe_stale_after_seconds,
        now=now,
        limit=max(safe_limit, len(tracked)),
        agent_names=tracked,
    )
    if not include_historical:
        return tracked_rows[:safe_limit]

    historical_rows = list_stale_agent_heartbeats(
        stale_after_seconds=safe_stale_after_seconds,
        now=now,
        limit=safe_limit,
    )
    merged = list(tracked_rows)
    for row in historical_rows:
        if len(merged) >= safe_limit:
            break
        if str(row.get("agent_name") or "") in tracked:
            continue
        merged.append(row)
    return merged


@app.get("/telemetry/heartbeats/summary")
def get_agent_heartbeat_summary(stale_after_seconds: int = DEFAULT_HEARTBEAT_STALE_AFTER_SEC, limit: int = 200):
    safe_limit = max(1, min(int(limit), 1000))
    now = now_utc()
    recent_rows = list_recent_agent_heartbeats(limit=max(safe_limit, 300))
    stale_rows = list_stale_agent_heartbeats(
        stale_after_seconds=stale_after_seconds,
        now=now,
        limit=max(safe_limit, 300),
    )
    tracked = _tracked_agent_names_for_telemetry()
    cutoff = now - timedelta(seconds=max(int(stale_after_seconds), 1))

    latest_by_agent: dict[str, dict[str, Any]] = {}
    for row in recent_rows:
        agent_name = str(row.get("agent_name") or "").strip()
        if not agent_name or agent_name in latest_by_agent:
            continue
        latest_by_agent[agent_name] = row

    stale_by_agent: dict[str, dict[str, Any]] = {}
    for row in stale_rows:
        agent_name = str(row.get("agent_name") or "").strip()
        if not agent_name or agent_name in stale_by_agent:
            continue
        stale_by_agent[agent_name] = row

    active_rows: list[dict[str, Any]] = []
    stale_current_rows: list[dict[str, Any]] = []
    for agent_name in sorted(tracked):
        row = latest_by_agent.get(agent_name)
        if row is None:
            stale_current_rows.append(
                {
                    "agent_name": agent_name,
                    "last_seen_at": "",
                    "status": "missing",
                    "metadata_json": {"agent_type": "unknown"},
                }
            )
            continue

        seen_at = _parse_iso_datetime(row.get("last_seen_at"))
        if seen_at is None or seen_at < cutoff:
            stale_current_rows.append(row)
        else:
            active_rows.append(row)

    historical_rows = [
        row
        for agent_name, row in stale_by_agent.items()
        if agent_name not in tracked
    ]

    def _trim(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return rows[:safe_limit]

    return {
        "captured_at": now.isoformat(),
        "stale_cutoff_at": cutoff.isoformat(),
        "stale_after_seconds": max(int(stale_after_seconds), 1),
        "tracked_agent_names": sorted(tracked),
        "tracked_agents_total": len(tracked),
        "active_tracked_agents": len(active_rows),
        "stale_current_agents": len(stale_current_rows),
        "historical_dead_agents": len(historical_rows),
        "active_tracked_rows": _trim(active_rows),
        "stale_current_rows": _trim(stale_current_rows),
        "historical_dead_rows": _trim(historical_rows),
    }


@app.get("/telemetry/planner/status")
def get_planner_status(event_limit: int = 300):
    return get_planner_status_snapshot(event_limit=event_limit)


@app.get("/planner/config", response_model=PlannerConfigOut)
def get_planner_config_route():
    try:
        cfg = get_planner_runtime_config()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to read planner config: {exc}")
    return PlannerConfigOut(**cfg)


@app.patch("/planner/config", response_model=PlannerConfigOut)
def patch_planner_config_route(req: PlannerConfigPatch):
    patch = req.model_dump(exclude_unset=True)
    try:
        if patch:
            cfg = update_planner_runtime_config(patch, updated_by="api")
            log_event("planner_config_updated", patch=patch)
        else:
            cfg = get_planner_runtime_config()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to update planner config: {exc}")
    return PlannerConfigOut(**cfg)


@app.post("/planner/config/reset", response_model=PlannerConfigOut)
def reset_planner_config_route():
    try:
        cfg = reset_planner_runtime_config(updated_by="api")
        log_event("planner_config_reset")
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to reset planner config: {exc}")
    return PlannerConfigOut(**cfg)


@app.get("/watchers", response_model=List[WatcherOut])
def list_watchers_route(limit: int = 100, enabled_only: bool = False):
    safe_limit = max(1, min(int(limit), 500))
    try:
        template_rows = list_planner_task_templates(limit=safe_limit, enabled_only=enabled_only)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to list watchers: {exc}")
    return [WatcherOut(**row) for row in _hydrate_watcher_rows(template_rows)]


@app.get("/watchers/{watcher_id}", response_model=WatcherOut)
def get_watcher_route(watcher_id: str):
    try:
        row = get_planner_task_template(watcher_id)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to read watcher: {exc}")
    if row is None:
        raise HTTPException(status_code=404, detail="Watcher not found")
    hydrated = _hydrate_watcher_rows([row])
    if not hydrated:
        raise HTTPException(status_code=404, detail="Watcher not found")
    return WatcherOut(**hydrated[0])


@app.post("/watchers", response_model=WatcherOut)
def create_watcher_route(req: WatcherCreate):
    interval = _resolve_watcher_interval(
        min_interval_seconds=req.min_interval_seconds,
        interval_seconds=req.interval_seconds,
        default_value=300,
    )
    metadata = dict(req.metadata or {})
    if req.notification_behavior is not None:
        metadata["notification_behavior"] = req.notification_behavior

    try:
        row = create_planner_task_template(
            template_id=req.id,
            name=req.name,
            task_type=req.task_type,
            payload_json=req.payload_json,
            model=req.model,
            max_attempts=req.max_attempts,
            min_interval_seconds=interval,
            enabled=req.enabled,
            priority=req.priority,
            metadata_json=metadata or None,
        )
        log_event(
            "watcher_created",
            watcher_id=row.get("id"),
            task_type=row.get("task_type"),
            interval_seconds=interval,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to create watcher: {exc}")

    hydrated = _hydrate_watcher_rows([row])
    return WatcherOut(**hydrated[0])


@app.patch("/watchers/{watcher_id}", response_model=WatcherOut)
def patch_watcher_route(watcher_id: str, req: WatcherPatch):
    patch = req.model_dump(exclude_unset=True)
    try:
        current = get_planner_task_template(watcher_id)
        if current is None:
            raise HTTPException(status_code=404, detail="Watcher not found")

        template_patch: dict[str, Any] = {}
        for key in ("name", "task_type", "payload_json", "model", "max_attempts", "enabled", "priority"):
            if key in patch:
                template_patch[key] = patch[key]

        if "interval_seconds" in patch or "min_interval_seconds" in patch:
            template_patch["min_interval_seconds"] = _resolve_watcher_interval(
                min_interval_seconds=patch.get("min_interval_seconds"),
                interval_seconds=patch.get("interval_seconds"),
                default_value=int(current.get("min_interval_seconds") or 300),
            )

        if "metadata" in patch or "notification_behavior" in patch:
            if "metadata" in patch:
                metadata = dict(patch.get("metadata") or {})
            else:
                metadata = _normalize_metadata_obj(current.get("metadata_json"))

            if "notification_behavior" in patch:
                notification_behavior = patch.get("notification_behavior")
                if isinstance(notification_behavior, dict):
                    metadata["notification_behavior"] = notification_behavior
                else:
                    metadata.pop("notification_behavior", None)

            template_patch["metadata_json"] = metadata or None

        if not template_patch:
            row = current
        else:
            row = update_planner_task_template(watcher_id, template_patch)
            log_event("watcher_updated", watcher_id=watcher_id, patch=template_patch)
    except HTTPException:
        raise
    except ValueError as exc:
        if str(exc) == "template not found":
            raise HTTPException(status_code=404, detail="Watcher not found")
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to update watcher: {exc}")

    hydrated = _hydrate_watcher_rows([row])
    if not hydrated:
        raise HTTPException(status_code=404, detail="Watcher not found")
    return WatcherOut(**hydrated[0])


@app.delete("/watchers/{watcher_id}")
def delete_watcher_route(watcher_id: str):
    try:
        deleted = delete_planner_task_template(watcher_id)
        if deleted:
            log_event("watcher_deleted", watcher_id=watcher_id)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to delete watcher: {exc}")
    if not deleted:
        raise HTTPException(status_code=404, detail="Watcher not found")
    return {"deleted": True, "watcher_id": watcher_id}


@app.get("/planner/templates", response_model=List[PlannerTemplateOut])
def list_planner_templates_route(limit: int = 100, enabled_only: bool = False):
    safe_limit = max(1, min(int(limit), 500))
    try:
        rows = list_planner_task_templates(limit=safe_limit, enabled_only=enabled_only)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to list planner templates: {exc}")
    return [PlannerTemplateOut(**row) for row in rows]


@app.post("/planner/templates", response_model=PlannerTemplateOut)
def create_planner_template_route(req: PlannerTemplateCreate):
    try:
        row = create_planner_task_template(
            template_id=req.id,
            name=req.name,
            task_type=req.task_type,
            payload_json=req.payload_json,
            model=req.model,
            max_attempts=req.max_attempts,
            min_interval_seconds=req.min_interval_seconds,
            enabled=req.enabled,
            priority=req.priority,
            metadata_json=req.metadata_json,
        )
        log_event(
            "planner_template_created",
            template_id=row.get("id"),
            task_type=row.get("task_type"),
            min_interval_seconds=row.get("min_interval_seconds"),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to create planner template: {exc}")
    return PlannerTemplateOut(**row)


@app.post("/planner/templates/presets/rtx5090", response_model=PlannerTemplateOut)
def upsert_rtx5090_planner_template_route(req: PlannerRtxPresetCreate):
    try:
        row = ensure_rtx5090_deals_template(
            interval_seconds=req.interval_seconds,
            gpu_max_price=req.gpu_max_price,
            pc_max_price=req.pc_max_price,
            enabled=req.enabled,
        )
        log_event(
            "planner_template_rtx5090_upserted",
            template_id=row.get("id"),
            interval_seconds=req.interval_seconds,
            gpu_max_price=req.gpu_max_price,
            pc_max_price=req.pc_max_price,
            enabled=req.enabled,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to configure RTX 5090 preset: {exc}")
    return PlannerTemplateOut(**row)


@app.post("/planner/templates/presets/jobs-digest", response_model=PlannerTemplateOut)
def upsert_jobs_digest_planner_template_route(req: PlannerJobsPresetCreate):
    try:
        row = ensure_jobs_digest_template(
            interval_seconds=req.interval_seconds,
            desired_title=req.desired_title,
            desired_titles=req.desired_titles,
            keywords=req.keywords,
            excluded_keywords=req.excluded_keywords,
            preferred_locations=req.preferred_locations,
            remote_preference=req.remote_preference,
            minimum_salary=req.minimum_salary,
            experience_level=req.experience_level,
            enabled_sources=req.enabled_sources,
            result_limit_per_source=req.result_limit_per_source,
            shortlist_count=req.shortlist_count,
            freshness_preference=req.freshness_preference,
            desired_salary_min=req.desired_salary_min,
            desired_salary_max=req.desired_salary_max,
            experience_levels=req.experience_levels,
            clearance_required=req.clearance_required,
            location=req.location,
            boards=req.boards,
            enabled=req.enabled,
        )
        log_event(
            "planner_template_jobs_digest_upserted",
            template_id=row.get("id"),
            interval_seconds=req.interval_seconds,
            desired_title=req.desired_title,
            desired_titles=req.desired_titles,
            keywords=req.keywords,
            excluded_keywords=req.excluded_keywords,
            preferred_locations=req.preferred_locations,
            remote_preference=req.remote_preference,
            minimum_salary=req.minimum_salary,
            experience_level=req.experience_level,
            enabled_sources=req.enabled_sources,
            result_limit_per_source=req.result_limit_per_source,
            shortlist_count=req.shortlist_count,
            freshness_preference=req.freshness_preference,
            desired_salary_min=req.desired_salary_min,
            desired_salary_max=req.desired_salary_max,
            location=req.location,
            boards=req.boards,
            enabled=req.enabled,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to configure jobs digest preset: {exc}")
    return PlannerTemplateOut(**row)


@app.patch("/planner/templates/{template_id}", response_model=PlannerTemplateOut)
def patch_planner_template_route(template_id: str, req: PlannerTemplatePatch):
    patch = req.model_dump(exclude_unset=True)
    try:
        if not patch:
            row = get_planner_task_template(template_id)
            if row is None:
                raise HTTPException(status_code=404, detail="Planner template not found")
        else:
            row = update_planner_task_template(template_id, patch)
            log_event(
                "planner_template_updated",
                template_id=template_id,
                patch=patch,
            )
    except HTTPException:
        raise
    except ValueError as exc:
        if str(exc) == "template not found":
            raise HTTPException(status_code=404, detail="Planner template not found")
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to update planner template: {exc}")
    if row is None:
        raise HTTPException(status_code=404, detail="Planner template not found")
    return PlannerTemplateOut(**row)


@app.delete("/planner/templates/{template_id}")
def delete_planner_template_route(template_id: str):
    try:
        deleted = delete_planner_task_template(template_id)
        if deleted:
            log_event("planner_template_deleted", template_id=template_id)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to delete planner template: {exc}")
    if not deleted:
        raise HTTPException(status_code=404, detail="Planner template not found")
    return {"deleted": True, "template_id": template_id}


@app.post("/planner/run-once")
def run_planner_once_route():
    try:
        cfg = get_planner_runtime_config()
        templates = list_enabled_planner_task_templates(limit=200)
        from autonomous_planner import run_planner_cycle

        summary = run_planner_cycle(
            now=now_utc(),
            execution_enabled=bool(cfg.get("execution_enabled", False)),
            require_approval=bool(cfg.get("require_approval", True)),
            approved=bool(cfg.get("approved", False)),
            policy_overrides=cfg,
            create_task_specs=templates,
        )
        log_event(
            "planner_manual_run",
            mode=summary.get("mode"),
            executed_count=summary.get("executed_count"),
            decision_counts=summary.get("decision_counts"),
            template_count=len(templates),
        )
        return summary
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to run planner cycle: {exc}")
