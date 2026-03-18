import json
import logging
import os
import sys
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from enum import Enum
from typing import Any

from redis import Redis
from rq import Queue
from rq.job import Job
from rq.registry import StartedJobRegistry
from sqlalchemy import (
    JSON,
    DateTime,
    Enum as SAEnum,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    cast,
    create_engine,
    func,
    inspect,
    text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker

from llm.openai_adapter import run_chat_completion
from router import choose_model, validate_model
from core.schema_validate import PayloadValidationError, validate_payload
from core.operational_day import current_operational_day_window_utc
from agent_heartbeats import upsert_agent_heartbeat
from event_log import log_event as persist_event_log
from task_run_history import complete_task_run, create_task_run, fail_task_run
from task_handlers.deals_scan_v1 import (
    build_unicorn_notify_request,
    execute as deals_scan_execute,
)
from task_handlers.errors import NonRetryableTaskError
from task_handlers.jobs_collect_v1 import execute as jobs_collect_execute
from task_handlers.jobs_digest_v1 import execute as jobs_digest_execute
from task_handlers.jobs_digest_v2 import execute as jobs_digest_v2_execute
from task_handlers.jobs_normalize_v1 import execute as jobs_normalize_execute
from task_handlers.jobs_rank_v1 import execute as jobs_rank_execute
from task_handlers.jobs_shortlist_v1 import execute as jobs_shortlist_execute
from task_handlers.notify_v1 import execute as notify_execute
from task_handlers.slides_outline_v1 import execute as slides_outline_execute


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATABASE_URL = os.environ["DATABASE_URL"]
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
DAILY_BUDGET_USD = Decimal(os.getenv("DAILY_BUDGET_USD", "0.50"))
BUDGET_BUFFER_USD = Decimal(os.getenv("BUDGET_BUFFER_USD", "0.02"))
USE_LLM = os.getenv("USE_LLM", "false").lower() == "true"
COST_SCALE = Decimal("0.00000001")
RETRY_BASE_SECONDS = int(os.getenv("RETRY_BASE_SECONDS", "30"))
RETRY_MAX_SECONDS = int(os.getenv("RETRY_MAX_SECONDS", "900"))
WORKER_INLINE_HEARTBEAT_ENABLED = os.getenv("WORKER_INLINE_HEARTBEAT_ENABLED", "false").lower() == "true"
RQ_JOB_TIMEOUT_SECONDS = max(int(os.getenv("RQ_JOB_TIMEOUT_SECONDS", "900")), 30)
ORPHANED_RUN_RECOVERY_ENABLED = os.getenv("ORPHANED_RUN_RECOVERY_ENABLED", "true").lower() == "true"
ORPHANED_RUN_STALE_AFTER_SEC = max(int(os.getenv("ORPHANED_RUN_STALE_AFTER_SEC", "300")), 60)
ORPHANED_RUN_RECOVERY_INTERVAL_SEC = max(int(os.getenv("ORPHANED_RUN_RECOVERY_INTERVAL_SEC", "60")), 10)
WORKER_HEARTBEAT_ENABLED = (
    WORKER_INLINE_HEARTBEAT_ENABLED
    and os.getenv("WORKER_HEARTBEAT_ENABLED", "true").lower() == "true"
    and not bool(os.getenv("PYTEST_CURRENT_TEST"))
    and "pytest" not in os.path.basename(sys.argv[0]).lower()
)
WORKER_HEARTBEAT_INTERVAL_SEC = max(int(os.getenv("WORKER_HEARTBEAT_INTERVAL_SEC", "15")), 5)

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)

redis_conn = Redis.from_url(REDIS_URL)
queue = Queue("default", connection=redis_conn, default_timeout=RQ_JOB_TIMEOUT_SECONDS)

HANDLERS = {
    "jobs_digest_v1": jobs_digest_execute,
    "jobs_collect_v1": jobs_collect_execute,
    "jobs_normalize_v1": jobs_normalize_execute,
    "jobs_rank_v1": jobs_rank_execute,
    "jobs_shortlist_v1": jobs_shortlist_execute,
    "jobs_digest_v2": jobs_digest_v2_execute,
    "deals_scan_v1": deals_scan_execute,
    "slides_outline_v1": slides_outline_execute,
    "notify_v1": notify_execute,
}
RESULT_ARTIFACT_TYPE = "result.json"
DEBUG_ARTIFACT_TYPE = "debug.json"
FOLLOWUP_ARTIFACT_TYPE = "followup.json"


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
    idempotency_key: Mapped[str | None] = mapped_column(String(128), nullable=True)

    model: Mapped[str | None] = mapped_column(String(64), nullable=True)
    tokens_in: Mapped[int | None] = mapped_column(Integer, nullable=True)
    tokens_out: Mapped[int | None] = mapped_column(Integer, nullable=True)
    cost_usd: Mapped[Decimal | None] = mapped_column(Numeric(12, 8), nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)

    max_attempts: Mapped[int] = mapped_column(Integer, default=3)
    next_run_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    max_cost_usd: Mapped[Decimal | None] = mapped_column(Numeric(12, 8), nullable=True)
    expected_tokens_in: Mapped[int | None] = mapped_column(Integer, nullable=True)
    expected_tokens_out: Mapped[int | None] = mapped_column(Integer, nullable=True)


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
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    ended_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    wall_time_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    model: Mapped[str | None] = mapped_column(String(64), nullable=True)
    tokens_in: Mapped[int | None] = mapped_column(Integer, nullable=True)
    tokens_out: Mapped[int | None] = mapped_column(Integer, nullable=True)
    cost_usd: Mapped[Decimal | None] = mapped_column(Numeric(12, 8), nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
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
    content_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    content_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def ensure_schema_compatibility() -> None:
    """
    Non-destructive runtime schema reconciliation for worker compatibility.
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


def to_decimal_8(value: object) -> Decimal:
    if isinstance(value, Decimal):
        dec = value
    else:
        dec = Decimal(str(value))
    return dec.quantize(COST_SCALE, rounding=ROUND_HALF_UP)


def today_spend_usd(db) -> Decimal:
    """
    Compute today's spend as SUM(runs.cost_usd) for runs in the current
    operational day window.
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
    """
    is_available, spend, remaining = enforce_budget(db, safety_buffer)
    return is_available, remaining, spend


def is_transient_error(exc: Exception) -> bool:
    status_code = getattr(exc, "status_code", None)
    if isinstance(status_code, int) and (status_code >= 500 or status_code == 429):
        return True

    transient_types = (TimeoutError, ConnectionError)
    if isinstance(exc, transient_types):
        return True

    message = str(exc).lower()
    keywords = (
        "timeout",
        "temporar",
        "connection reset",
        "connection refused",
        "service unavailable",
        "bad gateway",
        "gateway timeout",
        "rate limit",
        "429",
        "502",
        "503",
        "504",
    )
    return any(keyword in message for keyword in keywords)


def describe_runtime_error(exc: Exception) -> str:
    raw = str(exc).strip() or type(exc).__name__
    error_type = type(exc).__name__
    if error_type == "APIConnectionError":
        return f"OPENAI_API_CONNECTION_ERROR: {raw}"
    if isinstance(exc, ConnectionError):
        return f"CONNECTION_ERROR[{error_type}]: {raw}"
    return raw


def retry_delay_seconds(attempt: int) -> int:
    exp = max(attempt - 1, 0)
    delay = RETRY_BASE_SECONDS * (2 ** exp)
    return min(delay, RETRY_MAX_SECONDS)


def _as_int(value: object, default: int = 0) -> int:
    if value is None:
        return default
    return int(value)


def _as_non_negative_int(value: object, default: int = 0) -> int:
    try:
        parsed = _as_int(value, default=default)
    except Exception:
        return default
    return parsed if parsed >= 0 else default


def _parse_handler_usage(raw_usage: object) -> tuple[int, int, Decimal, list[str], list[str]]:
    if raw_usage is None:
        return 0, 0, Decimal("0"), [], []
    if not isinstance(raw_usage, dict):
        raise RuntimeError("Task handler 'usage' must be an object when provided")

    tokens_in = _as_non_negative_int(raw_usage.get("tokens_in"), default=0)
    tokens_out = _as_non_negative_int(raw_usage.get("tokens_out"), default=0)

    try:
        cost_usd = Decimal(str(raw_usage.get("cost_usd") or "0"))
    except Exception as exc:
        raise RuntimeError(f"Task handler 'usage.cost_usd' must be numeric: {exc}") from exc

    request_ids: list[str] = []
    ids_value = raw_usage.get("openai_request_ids")
    if isinstance(ids_value, list):
        for item in ids_value:
            if isinstance(item, str) and item.strip():
                request_ids.append(item.strip())
    single_id = raw_usage.get("openai_request_id")
    if isinstance(single_id, str) and single_id.strip():
        request_ids.append(single_id.strip())
    if request_ids:
        request_ids = list(dict.fromkeys(request_ids))

    ai_usage_task_run_ids: list[str] = []
    ai_ids_value = raw_usage.get("ai_usage_task_run_ids")
    if isinstance(ai_ids_value, list):
        for item in ai_ids_value:
            if isinstance(item, str) and item.strip():
                ai_usage_task_run_ids.append(item.strip())
    single_ai_id = raw_usage.get("ai_usage_task_run_id")
    if isinstance(single_ai_id, str) and single_ai_id.strip():
        ai_usage_task_run_ids.append(single_ai_id.strip())
    if ai_usage_task_run_ids:
        ai_usage_task_run_ids = list(dict.fromkeys(ai_usage_task_run_ids))

    return tokens_in, tokens_out, cost_usd, request_ids, ai_usage_task_run_ids


def _parse_exception_usage(exc: Exception) -> tuple[int, int, Decimal, list[str], list[str]]:
    raw_usage = getattr(exc, "usage", None)
    if raw_usage is None:
        return 0, 0, Decimal("0"), [], []
    try:
        return _parse_handler_usage(raw_usage)
    except Exception:
        return 0, 0, Decimal("0"), [], []


def _task_usage_totals(db, task_id: str) -> tuple[int, int, Decimal]:
    tokens_in_total, tokens_out_total, cost_total = (
        db.query(
            func.coalesce(func.sum(Run.tokens_in), 0),
            func.coalesce(func.sum(Run.tokens_out), 0),
            func.coalesce(func.sum(Run.cost_usd), cast(0, Numeric(12, 8))),
        )
        .filter(Run.task_id == task_id)
        .one()
    )
    return (
        _as_non_negative_int(tokens_in_total, default=0),
        _as_non_negative_int(tokens_out_total, default=0),
        to_decimal_8(cost_total or Decimal("0")),
    )


def _refresh_task_usage_totals(db, task: Task) -> None:
    tokens_in_total, tokens_out_total, cost_total = _task_usage_totals(db, task.id)
    task.tokens_in = tokens_in_total
    task.tokens_out = tokens_out_total
    task.cost_usd = cost_total


def _build_result_json(content_json: object, content_text: object) -> dict:
    if isinstance(content_json, dict):
        return content_json
    if content_json is not None:
        return {"value": content_json}
    return {"text": "" if content_text is None else str(content_text)}


def _store_artifact(
    db,
    *,
    task_id: str,
    run_id: str,
    artifact_type: str,
    content_text: str | None = None,
    content_json: dict | None = None,
) -> None:
    artifact = Artifact(
        id=str(uuid.uuid4()),
        task_id=task_id,
        run_id=run_id,
        artifact_type=artifact_type,
        content_text=content_text,
        content_json=content_json,
    )
    db.add(artifact)


def _payload_disables_notify_dedupe(payload: object) -> bool:
    if not isinstance(payload, dict):
        return False
    return bool(payload.get("disable_dedupe", False))


def _enqueue_notify_task(db, *, payload: dict) -> str:
    dedupe_key_raw = payload.get("dedupe_key")
    idempotency_key: str | None = None
    if not _payload_disables_notify_dedupe(payload) and isinstance(dedupe_key_raw, str) and dedupe_key_raw.strip():
        idempotency_key = f"notify:{dedupe_key_raw.strip()}"
        existing = (
            db.query(Task)
            .filter(Task.task_type == "notify_v1", Task.idempotency_key == idempotency_key)
            .order_by(Task.created_at.desc())
            .first()
        )
        if existing is not None:
            return existing.id

    notify_task_id = str(uuid.uuid4())
    now = now_utc()
    task = Task(
        id=notify_task_id,
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
    db.flush()
    queue.enqueue("worker.run_task", notify_task_id)
    db.commit()
    return notify_task_id


def _compact_payload_json(payload: Any) -> str:
    if isinstance(payload, str):
        parsed = json.loads(payload)
    elif isinstance(payload, dict):
        parsed = payload
    else:
        raise ValueError("payload_json must be a JSON object or JSON object string")

    if not isinstance(parsed, dict):
        raise ValueError("payload_json must decode to a JSON object")
    return json.dumps(parsed, separators=(",", ":"), ensure_ascii=True)


def _enqueue_followup_task(
    db,
    *,
    spec: dict[str, Any],
    parent_task_id: str,
    parent_run_id: str,
) -> tuple[str, bool]:
    task_type = str(spec.get("task_type") or "").strip()
    if not task_type:
        raise ValueError("follow-up task spec requires task_type")

    payload_json = _compact_payload_json(spec.get("payload_json"))
    raw_idempotency_key = spec.get("idempotency_key")
    idempotency_key = str(raw_idempotency_key).strip() if isinstance(raw_idempotency_key, str) else None
    if not idempotency_key:
        idempotency_key = None

    if task_type == "notify_v1":
        parsed_payload = json.loads(payload_json)
        if _payload_disables_notify_dedupe(parsed_payload):
            idempotency_key = None

    model = str(spec.get("model")).strip() if isinstance(spec.get("model"), str) and str(spec.get("model")).strip() else None
    try:
        max_attempts = int(spec.get("max_attempts") or 3)
    except (TypeError, ValueError):
        max_attempts = 3
    max_attempts = max(max_attempts, 1)

    if idempotency_key is not None:
        existing = (
            db.query(Task)
            .filter(Task.task_type == task_type, Task.idempotency_key == idempotency_key)
            .order_by(Task.created_at.desc())
            .first()
        )
        if existing is not None:
            _safe_persist_event(
                event_type="followup_task_deduped",
                source="worker",
                level="INFO",
                message=f"Follow-up task deduped: {task_type}",
                metadata_json={
                    "task_id": existing.id,
                    "task_type": task_type,
                    "parent_task_id": parent_task_id,
                    "parent_run_id": parent_run_id,
                    "idempotency_key": idempotency_key,
                },
            )
            return existing.id, False

    now = now_utc()
    followup_task_id = str(uuid.uuid4())
    followup_task = Task(
        id=followup_task_id,
        created_at=now,
        updated_at=now,
        status=TaskStatus.queued,
        task_type=task_type,
        payload_json=payload_json,
        idempotency_key=idempotency_key,
        model=model,
        max_attempts=max_attempts,
        next_run_at=None,
    )
    db.add(followup_task)
    db.flush()
    queue.enqueue("worker.run_task", followup_task_id)
    db.commit()
    _safe_persist_event(
        event_type="followup_task_enqueued",
        source="worker",
        level="INFO",
        message=f"Follow-up task enqueued: {task_type}",
        metadata_json={
            "task_id": followup_task_id,
            "task_type": task_type,
            "parent_task_id": parent_task_id,
            "parent_run_id": parent_run_id,
            "idempotency_key": idempotency_key,
        },
    )
    return followup_task_id, True


def _safe_persist_event(
    *,
    event_type: str,
    source: str,
    level: str,
    message: str,
    metadata_json: dict[str, object] | None = None,
) -> None:
    try:
        persist_event_log(
            event_type=event_type,
            source=source,
            level=level,
            message=message,
            metadata_json=metadata_json,
        )
    except Exception:
        logger.exception("structured_event_log_failed")


_WORKER_STARTED_LOGGED = False
_WORKER_HEARTBEAT_THREAD_STARTED = False
_WORKER_HEARTBEAT_STATE_LOCK = threading.Lock()
_WORKER_HEARTBEAT_STATE: dict[str, object] = {
    "status": "starting",
    "metadata": {},
}
_ORPHANED_RECOVERY_LOCK = threading.Lock()
_ORPHANED_RECOVERY_LAST_AT: datetime | None = None


def _started_task_ids() -> set[str]:
    try:
        registry = StartedJobRegistry(queue=queue)
        task_ids: set[str] = set()
        for job_id in registry.get_job_ids():
            try:
                job = Job.fetch(job_id, connection=redis_conn)
            except Exception:
                continue
            args = getattr(job, "args", ())
            if isinstance(args, tuple) and args and isinstance(args[0], str) and args[0].strip():
                task_ids.add(args[0].strip())
        return task_ids
    except Exception:
        logger.exception("orphaned_run_recovery_started_registry_read_failed")
        return set()


def _recover_orphaned_running_tasks_once() -> None:
    if not ORPHANED_RUN_RECOVERY_ENABLED:
        return

    now = now_utc()
    stale_cutoff = now - timedelta(seconds=ORPHANED_RUN_STALE_AFTER_SEC)
    active_task_ids = _started_task_ids()
    tasks_to_reenqueue: list[str] = []
    recovered_count = 0

    with SessionLocal() as db:
        running_tasks = (
            db.query(Task)
            .filter(Task.status == TaskStatus.running)
            .filter(Task.updated_at <= stale_cutoff)
            .order_by(Task.updated_at.asc())
            .all()
        )

        for task in running_tasks:
            if task.id in active_task_ids:
                continue

            running_run = (
                db.query(Run)
                .filter(Run.task_id == task.id, Run.status == RunStatus.running)
                .order_by(Run.attempt.desc())
                .first()
            )
            if running_run is not None:
                started_at = running_run.started_at or task.updated_at
                if started_at is not None and started_at.tzinfo is None:
                    started_at = started_at.replace(tzinfo=timezone.utc)
                if started_at is not None and started_at > stale_cutoff:
                    continue

            attempts_used = int(db.query(func.count(Run.id)).filter(Run.task_id == task.id).scalar() or 0)
            max_attempts = max(int(task.max_attempts or 1), 1)
            recovery_error = "Recovered orphaned running task after work-horse termination."

            if running_run is not None:
                started_at = running_run.started_at or task.updated_at or now
                if started_at.tzinfo is None:
                    started_at = started_at.replace(tzinfo=timezone.utc)
                running_run.status = RunStatus.failed
                running_run.ended_at = now
                running_run.wall_time_ms = max(int((now - started_at).total_seconds() * 1000), 0)
                running_run.error = recovery_error

            if attempts_used < max_attempts:
                task.status = TaskStatus.queued
                task.next_run_at = None
                task.error = recovery_error
                task.updated_at = now
                tasks_to_reenqueue.append(task.id)
            else:
                task.status = TaskStatus.failed_permanent
                task.next_run_at = None
                task.error = f"{recovery_error} Max attempts reached ({max_attempts})."
                task.updated_at = now

            recovered_count += 1

        if recovered_count:
            db.commit()

    for task_id in tasks_to_reenqueue:
        try:
            queue.enqueue("worker.run_task", task_id)
        except Exception:
            logger.exception("orphaned_run_recovery_reenqueue_failed task_id=%s", task_id)

    if recovered_count:
        _safe_persist_event(
            event_type="worker_orphaned_run_recovered",
            source="worker",
            level="WARNING",
            message=f"Recovered {recovered_count} orphaned running task(s).",
            metadata_json={
                "recovered_count": recovered_count,
                "reenqueued_count": len(tasks_to_reenqueue),
                "stale_after_seconds": ORPHANED_RUN_STALE_AFTER_SEC,
            },
        )


def _maybe_recover_orphaned_running_tasks() -> None:
    global _ORPHANED_RECOVERY_LAST_AT
    if not ORPHANED_RUN_RECOVERY_ENABLED:
        return

    now = now_utc()
    with _ORPHANED_RECOVERY_LOCK:
        if _ORPHANED_RECOVERY_LAST_AT is not None:
            age = int((now - _ORPHANED_RECOVERY_LAST_AT).total_seconds())
            if age < ORPHANED_RUN_RECOVERY_INTERVAL_SEC:
                return
        _ORPHANED_RECOVERY_LAST_AT = now

    _recover_orphaned_running_tasks_once()


def _log_worker_started_once() -> None:
    global _WORKER_STARTED_LOGGED
    if _WORKER_STARTED_LOGGED:
        return
    _WORKER_STARTED_LOGGED = True
    _safe_persist_event(
        event_type="worker_started",
        source="worker",
        level="INFO",
        message="Worker process started.",
        metadata_json={"worker_name": _resolve_worker_name()},
    )


def _resolve_worker_name() -> str:
    return os.getenv("WORKER_NAME", "worker").strip() or "worker"


def _set_worker_heartbeat_state(*, status: str | None = None, metadata_updates: dict[str, object] | None = None) -> None:
    with _WORKER_HEARTBEAT_STATE_LOCK:
        if status is not None:
            _WORKER_HEARTBEAT_STATE["status"] = status
        if metadata_updates:
            current = dict(_WORKER_HEARTBEAT_STATE.get("metadata") or {})
            current.update(metadata_updates)
            _WORKER_HEARTBEAT_STATE["metadata"] = current


def _emit_worker_heartbeat() -> None:
    if not WORKER_HEARTBEAT_ENABLED:
        return
    with _WORKER_HEARTBEAT_STATE_LOCK:
        status = str(_WORKER_HEARTBEAT_STATE.get("status") or "alive")
        metadata = dict(_WORKER_HEARTBEAT_STATE.get("metadata") or {})

    metadata.update(
        {
            "agent_type": "worker",
            "worker_name": _resolve_worker_name(),
            "host_name": os.getenv("HOSTNAME", "").strip() or None,
            "pid": os.getpid(),
            "heartbeat_interval_sec": WORKER_HEARTBEAT_INTERVAL_SEC,
        }
    )
    try:
        upsert_agent_heartbeat(
            agent_name=_resolve_worker_name(),
            status=status,
            metadata_json=metadata,
        )
    except Exception:
        logger.exception("worker_heartbeat_write_failed")


def _worker_heartbeat_loop() -> None:
    while True:
        _emit_worker_heartbeat()
        time.sleep(WORKER_HEARTBEAT_INTERVAL_SEC)


def _start_worker_heartbeat_once() -> None:
    global _WORKER_HEARTBEAT_THREAD_STARTED
    if not WORKER_HEARTBEAT_ENABLED:
        return
    if _WORKER_HEARTBEAT_THREAD_STARTED:
        return
    _WORKER_HEARTBEAT_THREAD_STARTED = True
    _set_worker_heartbeat_state(
        status="alive",
        metadata_updates={"started_at": now_utc().isoformat()},
    )
    _emit_worker_heartbeat()
    thread = threading.Thread(
        target=_worker_heartbeat_loop,
        name="worker-heartbeat",
        daemon=True,
    )
    thread.start()


_log_worker_started_once()
_start_worker_heartbeat_once()


def _safe_create_task_run(
    *,
    task_name: str,
    input_json: object | None,
    worker_name: str,
    started_at: datetime | None = None,
) -> str | None:
    try:
        return create_task_run(
            task_name=task_name,
            input_json=input_json,
            worker_name=worker_name,
            status="running",
            started_at=started_at,
        )
    except Exception:
        logger.exception("task_run_history_create_failed")
        return None


def _safe_complete_task_run(
    task_run_id: str | None,
    *,
    output_json: object | None,
    duration_ms: int | None,
    ended_at: datetime | None = None,
) -> None:
    if not task_run_id:
        return
    try:
        complete_task_run(
            task_run_id=task_run_id,
            output_json=output_json,
            duration_ms=duration_ms,
            ended_at=ended_at,
        )
    except Exception:
        logger.exception("task_run_history_complete_failed")


def _safe_fail_task_run(
    task_run_id: str | None,
    *,
    error_text: str,
    output_json: object | None = None,
    duration_ms: int | None = None,
    ended_at: datetime | None = None,
) -> None:
    if not task_run_id:
        return
    try:
        fail_task_run(
            task_run_id=task_run_id,
            error_text=error_text,
            output_json=output_json,
            duration_ms=duration_ms,
            ended_at=ended_at,
        )
    except Exception:
        logger.exception("task_run_history_fail_failed")


def run_task(task_id: str) -> None:
    """
    Task runner with catalog-based model routing, budget enforcement, handler dispatch, and artifacts.
    """
    run_id: str | None = None
    attempt: int | None = None
    task_type: str | None = None
    chosen_model: str | None = None
    history_task_run_id: str | None = None
    worker_name = _resolve_worker_name()
    _log_worker_started_once()
    _start_worker_heartbeat_once()
    _maybe_recover_orphaned_running_tasks()
    _set_worker_heartbeat_state(
        status="alive",
        metadata_updates={
            "last_task_id": task_id,
            "last_task_seen_at": now_utc().isoformat(),
        },
    )

    def log_event(level: int, event: str, **context: object) -> None:
        payload: dict[str, object] = {
            "timestamp": now_utc().isoformat(),
            "level": logging.getLevelName(level),
            "service": "worker",
            "event": event,
            "task_id": task_id,
            "run_id": run_id,
            "attempt": attempt,
            "task_type": task_type,
            "chosen_model": chosen_model,
            "context": context,
        }
        logger.log(level, json.dumps(payload, default=str))

    ensure_schema_compatibility()

    with SessionLocal() as db:
        t = db.get(Task, task_id)
        if not t:
            log_event(logging.WARNING, "task_not_found_skip", final_run_status="skipped")
            return

        task_type = t.task_type
        max_attempts = max(int(t.max_attempts or 3), 1)
        if t.status in (TaskStatus.success, TaskStatus.failed_permanent, TaskStatus.blocked_budget):
            log_event(
                logging.INFO,
                "task_terminal_skip",
                task_status=t.status.value,
                final_run_status="skipped",
            )
            return

        if t.next_run_at is not None and t.next_run_at > now_utc():
            log_event(
                logging.INFO,
                "task_not_due_yet",
                next_run_at=t.next_run_at.isoformat(),
                final_run_status="skipped",
            )
            return

        latest_run = (
            db.query(Run)
            .filter(Run.task_id == task_id)
            .order_by(Run.attempt.desc())
            .first()
        )
        attempt = (latest_run.attempt + 1) if latest_run else 1

        if attempt > max_attempts:
            t.status = TaskStatus.failed_permanent
            t.error = f"Max attempts exceeded ({max_attempts})"
            t.updated_at = now_utc()
            db.commit()
            log_event(
                logging.ERROR,
                "task_failed_permanent_max_attempts",
                max_attempts=max_attempts,
                final_task_status=TaskStatus.failed_permanent.value,
            )
            return

        run_id = str(uuid.uuid4())

        run = Run(
            id=run_id,
            task_id=task_id,
            attempt=attempt,
            status=RunStatus.queued,
            created_at=now_utc(),
        )
        db.add(run)
        db.commit()
        log_event(logging.INFO, "run_queued", run_status=RunStatus.queued.value)
        _safe_persist_event(
            event_type="task_queued",
            source="worker",
            level="INFO",
            message=f"Task queued for execution: {task_type}",
            metadata_json={"task_id": task_id, "run_id": run_id, "attempt": attempt, "task_type": task_type},
        )

    with SessionLocal() as db:
        run = db.get(Run, run_id)
        t = db.get(Task, task_id)
        if run is None:
            raise RuntimeError(f"Run '{run_id}' no longer exists")
        if t is None:
            raise RuntimeError(f"Task '{task_id}' no longer exists")

        handler = HANDLERS.get(t.task_type)

        run.status = RunStatus.running
        run.started_at = now_utc()
        t.status = TaskStatus.running
        t.updated_at = now_utc()
        history_input_json: object
        try:
            history_input_json = json.loads(t.payload_json)
        except json.JSONDecodeError as exc:
            history_input_json = {
                "raw_payload_json": t.payload_json,
                "parse_error": exc.msg,
            }
        history_task_run_id = _safe_create_task_run(
            task_name=t.task_type,
            input_json=history_input_json,
            worker_name=worker_name,
            started_at=run.started_at,
        )

        if handler is None:
            error_msg = f"Unknown task_type '{t.task_type}' (no handler registry entry)"
            run.status = RunStatus.failed
            run.ended_at = now_utc()
            run.wall_time_ms = 0
            run.tokens_in = 0
            run.tokens_out = 0
            run.cost_usd = to_decimal_8(Decimal("0"))
            run.error = error_msg
            t.status = TaskStatus.failed
            t.error = error_msg
            t.updated_at = now_utc()
            db.flush()
            _refresh_task_usage_totals(db, t)
            db.commit()
            _safe_fail_task_run(
                history_task_run_id,
                error_text=error_msg,
                duration_ms=0,
                ended_at=run.ended_at,
            )
            _safe_persist_event(
                event_type="task_failed",
                source="worker",
                level="ERROR",
                message=f"Task failed: {t.task_type}",
                metadata_json={"task_id": task_id, "run_id": run_id, "attempt": attempt, "error": error_msg},
            )
            log_event(
                logging.ERROR,
                "run_finished",
                final_run_status=RunStatus.failed.value,
                final_task_status=TaskStatus.failed.value,
                spend_today_usd=None,
                remaining_budget_usd=None,
                tokens_in=0,
                tokens_out=0,
                cost_usd=str(to_decimal_8(Decimal("0"))),
                error=error_msg,
            )
            return

        spend_today_usd = today_spend_usd(db)
        remaining_budget_usd = to_decimal_8(DAILY_BUDGET_USD - spend_today_usd - BUDGET_BUFFER_USD)

        user_override = t.model
        if (
            user_override is not None
            and user_override not in {"cheap", "standard", "advanced"}
            and not validate_model(user_override)
        ):
            log_event(
                logging.WARNING,
                "invalid_model_override_ignored",
                requested_model=user_override,
            )

        chosen_model = choose_model(
            task_type=t.task_type,
            payload_json=t.payload_json,
            remaining_budget_usd=remaining_budget_usd,
            user_override=user_override,
        )
        run.model = chosen_model
        t.model = chosen_model

        log_event(
            logging.INFO,
            "model_chosen_for_run",
            remaining_budget_usd=str(to_decimal_8(remaining_budget_usd)),
            user_override=user_override,
        )

        execution_allowed, spend, remaining = enforce_budget(db, BUDGET_BUFFER_USD)
        log_event(
            logging.INFO,
            "budget_execution_gate",
            spend_today_usd=str(spend),
            remaining_budget_usd=str(remaining),
            min_required_usd=str(BUDGET_BUFFER_USD),
            allowed=execution_allowed,
        )
        if not execution_allowed:
            error_msg = (
                f"blocked_budget: Daily budget blocked (${spend:.4f} spent, "
                f"${remaining:.4f} remaining, ${BUDGET_BUFFER_USD:.4f} buffer)"
            )
            run.status = RunStatus.failed
            run.ended_at = now_utc()
            run.wall_time_ms = 0
            run.tokens_in = 0
            run.tokens_out = 0
            run.cost_usd = to_decimal_8(Decimal("0"))
            run.error = error_msg
            t.status = TaskStatus.blocked_budget
            t.error = error_msg
            t.updated_at = now_utc()
            db.flush()
            _refresh_task_usage_totals(db, t)
            db.commit()
            _safe_fail_task_run(
                history_task_run_id,
                error_text=error_msg,
                output_json={"error_type": "blocked_budget"},
                duration_ms=0,
                ended_at=run.ended_at,
            )
            _safe_persist_event(
                event_type="task_failed",
                source="worker",
                level="WARNING",
                message=f"Task blocked by budget: {t.task_type}",
                metadata_json={"task_id": task_id, "run_id": run_id, "attempt": attempt, "error": error_msg},
            )
            log_event(
                logging.WARNING,
                "run_blocked_budget_before_handler",
                spend_today_usd=str(spend),
                remaining_budget_usd=str(remaining),
                min_required_usd=str(BUDGET_BUFFER_USD),
                final_run_status=RunStatus.failed.value,
                final_task_status=TaskStatus.blocked_budget.value,
                tokens_in=0,
                tokens_out=0,
                cost_usd=str(to_decimal_8(Decimal("0"))),
                error=error_msg,
            )
            return

        db.commit()
        log_event(logging.INFO, "run_started", run_status=RunStatus.running.value)
        _safe_persist_event(
            event_type="task_started",
            source="worker",
            level="INFO",
            message=f"Task execution started: {t.task_type}",
            metadata_json={"task_id": task_id, "run_id": run_id, "attempt": attempt, "task_type": t.task_type},
        )
        start_time = time.time()

    try:
        with SessionLocal() as db:
            t = db.get(Task, task_id)
            if t is None:
                raise RuntimeError(f"Task '{task_id}' no longer exists")

            try:
                payload_obj = json.loads(t.payload_json)
            except json.JSONDecodeError as exc:
                raise PayloadValidationError(
                    f"Invalid JSON payload for task_type '{t.task_type}': {exc.msg}"
                ) from exc
            validate_payload(t.task_type, payload_obj)

            handler = HANDLERS.get(t.task_type)
            if handler is None:
                raise RuntimeError(f"Unknown task_type '{t.task_type}' (no handler registry entry)")
            setattr(t, "_run_id", run_id)
            handler_result = handler(t, db)
            if not isinstance(handler_result, dict):
                raise RuntimeError(f"Task handler '{t.task_type}' must return a dict")

        wall_time_ms = int((time.time() - start_time) * 1000)
        llm_config = handler_result.get("llm")
        content_text = handler_result.get("content_text")
        content_json = handler_result.get("content_json")
        debug_json = handler_result.get("debug_json")
        if debug_json is not None and not isinstance(debug_json, dict):
            raise RuntimeError("Task handler 'debug_json' must be an object")
        next_tasks = handler_result.get("next_tasks")
        if next_tasks is None:
            next_tasks = []
        if not isinstance(next_tasks, list):
            raise RuntimeError("Task handler 'next_tasks' must be an array when provided")
        tokens_in = 0
        tokens_out = 0
        cost_usd = Decimal("0")
        openai_request_id = None
        openai_request_ids: list[str] = []
        ai_usage_task_run_ids: list[str] = []
        handler_usage = handler_result.get("usage")

        if llm_config is not None and USE_LLM:
            if not isinstance(llm_config, dict):
                raise RuntimeError("Task handler 'llm' config must be an object")
            messages = llm_config.get("messages")
            if not isinstance(messages, list) or not messages:
                raise RuntimeError("Task handler 'llm.messages' must be a non-empty list")
            if not chosen_model:
                raise RuntimeError("No chosen model available for LLM execution")
            llm_result = run_chat_completion(
                model=chosen_model or "",
                messages=messages,
                temperature=float(llm_config.get("temperature", 0.2)),
                max_completion_tokens=llm_config.get("max_completion_tokens", llm_config.get("max_tokens")),
                task_run_id=history_task_run_id,
                agent_name=t.task_type,
            )
            content_text = llm_result.get("output_text")
            tokens_in = _as_int(llm_result.get("tokens_in"))
            tokens_out = _as_int(llm_result.get("tokens_out"))
            cost_usd += Decimal(str(llm_result.get("cost_usd") or "0"))
            request_id = llm_result.get("openai_request_id")
            if isinstance(request_id, str) and request_id.strip():
                openai_request_ids.append(request_id.strip())
            ai_usage_task_run_ids.append(history_task_run_id)

        usage_tokens_in, usage_tokens_out, usage_cost_usd, usage_request_ids, usage_ai_task_run_ids = _parse_handler_usage(handler_usage)
        tokens_in += usage_tokens_in
        tokens_out += usage_tokens_out
        cost_usd += usage_cost_usd
        openai_request_ids.extend(usage_request_ids)
        ai_usage_task_run_ids.extend(usage_ai_task_run_ids)
        if openai_request_ids:
            openai_request_ids = list(dict.fromkeys(openai_request_ids))
            openai_request_id = openai_request_ids[0]
        if ai_usage_task_run_ids:
            ai_usage_task_run_ids = list(dict.fromkeys(ai_usage_task_run_ids))
        resolved_cost_usd = to_decimal_8(cost_usd)

        if content_text is None and content_json is None:
            raise RuntimeError("Task handler must return content_text or content_json")

        result_json = _build_result_json(content_json, content_text)

        with SessionLocal() as db:
            run = db.get(Run, run_id)
            t = db.get(Task, task_id)
            if run is None:
                raise RuntimeError(f"Run '{run_id}' no longer exists")
            if t is None:
                raise RuntimeError(f"Task '{task_id}' no longer exists")

            max_cost = t.max_cost_usd
            if max_cost is not None and resolved_cost_usd > max_cost:
                raise RuntimeError(
                    f"Task cost cap exceeded: cost_usd={resolved_cost_usd} max_cost_usd={max_cost}"
                )

            run.status = RunStatus.success
            run.ended_at = now_utc()
            run.wall_time_ms = wall_time_ms
            run.tokens_in = _as_int(tokens_in)
            run.tokens_out = _as_int(tokens_out)
            run.cost_usd = resolved_cost_usd
            run.error = None

            t.status = TaskStatus.success
            t.error = None
            t.next_run_at = None
            t.updated_at = now_utc()
            db.flush()
            _refresh_task_usage_totals(db, t)

            _store_artifact(
                db,
                task_id=task_id,
                run_id=run_id,
                artifact_type=RESULT_ARTIFACT_TYPE,
                content_text=content_text,
                content_json=result_json,
            )

            debug_payload = {
                "model": chosen_model,
                "llm_used": bool((llm_config is not None and USE_LLM) or handler_usage is not None),
                "tokens_in": _as_int(tokens_in),
                "tokens_out": _as_int(tokens_out),
                "cost_usd": str(resolved_cost_usd),
                "wall_time_ms": wall_time_ms,
                "openai_request_id": openai_request_id,
                "openai_request_ids": openai_request_ids,
                "ai_usage_task_run_ids": ai_usage_task_run_ids,
                "task_run_history_id": history_task_run_id,
                "followup_requested": len(next_tasks),
            }
            if debug_json:
                debug_payload["handler_debug"] = debug_json
            _store_artifact(
                db,
                task_id=task_id,
                run_id=run_id,
                artifact_type=DEBUG_ARTIFACT_TYPE,
                content_json=debug_payload,
            )
            db.commit()

            if t.task_type == "deals_scan_v1":
                notify_request = build_unicorn_notify_request(
                    payload_json=t.payload_json,
                    result_json=result_json,
                    run_timestamp=run.started_at or now_utc(),
                )
                deals_count = int(notify_request.get("deals_count") or 0)
                unicorn_count = int(notify_request.get("unicorn_count") or 0)
                alertable_unicorn_count = int(
                    notify_request.get("alertable_unicorn_count") or unicorn_count
                )
                notify_payload = notify_request.get("notify_payload")
                notify_task_id = None
                notify_enqueued = False

                if isinstance(notify_payload, dict):
                    try:
                        notify_task_id = _enqueue_notify_task(db, payload=notify_payload)
                        notify_enqueued = True
                    except Exception as notify_exc:
                        db.rollback()
                        log_event(
                            logging.ERROR,
                            "deals_scan_notify_enqueue_failed",
                            deals_count=deals_count,
                            unicorn_count=unicorn_count,
                            error=f"{type(notify_exc).__name__}: {notify_exc}",
                        )

                log_event(
                    logging.INFO,
                    "deals_scan_unicorn_notification_evaluated",
                    deals_count=deals_count,
                    unicorn_count=unicorn_count,
                    alertable_unicorn_count=alertable_unicorn_count,
                    notify_enqueued=notify_enqueued,
                    notify_task_id=notify_task_id,
                    dedupe_key=(notify_payload.get("dedupe_key") if isinstance(notify_payload, dict) else None),
                )

            handler_notify_decision = (
                debug_json.get("notify_decision")
                if isinstance(debug_json, dict) and isinstance(debug_json.get("notify_decision"), dict)
                else None
            )
            followup_outcomes: list[dict[str, Any]] = []
            followup_requested = len(next_tasks)
            if next_tasks:
                for idx, raw_spec in enumerate(next_tasks):
                    outcome: dict[str, Any] = {"index": idx}
                    if isinstance(raw_spec, dict):
                        raw_task_type = raw_spec.get("task_type")
                        task_type_hint = str(raw_task_type).strip() if raw_task_type is not None else ""
                        if task_type_hint:
                            outcome["task_type"] = task_type_hint
                        raw_idempotency = raw_spec.get("idempotency_key")
                        if isinstance(raw_idempotency, str) and raw_idempotency.strip():
                            outcome["idempotency_key"] = raw_idempotency.strip()
                        payload_hint = raw_spec.get("payload_json")
                        if isinstance(payload_hint, dict) and task_type_hint == "notify_v1":
                            outcome["source_task_type"] = payload_hint.get("source_task_type")
                            dedupe_key = payload_hint.get("dedupe_key")
                            if isinstance(dedupe_key, str) and dedupe_key.strip():
                                outcome["dedupe_key"] = dedupe_key.strip()
                            outcome["disable_dedupe"] = bool(payload_hint.get("disable_dedupe", False))
                    if not isinstance(raw_spec, dict):
                        outcome["status"] = "invalid_spec"
                        outcome["error"] = f"invalid spec type: {type(raw_spec).__name__}"
                        followup_outcomes.append(outcome)
                        log_event(
                            logging.ERROR,
                            "followup_task_spec_invalid",
                            index=idx,
                            parent_task_id=task_id,
                            parent_run_id=run_id,
                            error=str(outcome["error"]),
                        )
                        continue
                    try:
                        followup_task_id, created = _enqueue_followup_task(
                            db,
                            spec=raw_spec,
                            parent_task_id=task_id,
                            parent_run_id=run_id,
                        )
                        outcome["task_id"] = followup_task_id
                        outcome["status"] = "enqueued" if created else "deduped_existing"
                    except Exception as followup_exc:
                        db.rollback()
                        outcome["status"] = "enqueue_failed"
                        outcome["error"] = f"{type(followup_exc).__name__}: {followup_exc}"
                        log_event(
                            logging.ERROR,
                            "followup_task_enqueue_failed",
                            index=idx,
                            parent_task_id=task_id,
                            parent_run_id=run_id,
                            error=str(outcome["error"]),
                        )
                    followup_outcomes.append(outcome)

            if followup_requested > 0 or handler_notify_decision is not None:
                counts = {
                    "enqueued": 0,
                    "deduped_existing": 0,
                    "enqueue_failed": 0,
                    "invalid_spec": 0,
                }
                for row in followup_outcomes:
                    status = str(row.get("status") or "")
                    if status in counts:
                        counts[status] += 1
                followup_debug_payload = {
                    "parent_task_id": task_id,
                    "parent_task_type": t.task_type,
                    "parent_run_id": run_id,
                    "requested_count": followup_requested,
                    "notify_decision": handler_notify_decision,
                    "counts": counts,
                    "outcomes": followup_outcomes,
                }
                try:
                    _store_artifact(
                        db,
                        task_id=task_id,
                        run_id=run_id,
                        artifact_type=FOLLOWUP_ARTIFACT_TYPE,
                        content_json=followup_debug_payload,
                    )
                    db.commit()
                except Exception as followup_artifact_exc:
                    db.rollback()
                    log_event(
                        logging.ERROR,
                        "followup_artifact_persist_failed",
                        parent_task_id=task_id,
                        parent_run_id=run_id,
                        error=f"{type(followup_artifact_exc).__name__}: {followup_artifact_exc}",
                    )

        _safe_complete_task_run(
            history_task_run_id,
            output_json=result_json,
            duration_ms=wall_time_ms,
            ended_at=now_utc(),
        )
        _safe_persist_event(
            event_type="task_succeeded",
            source="worker",
            level="INFO",
            message=f"Task succeeded: {task_type}",
            metadata_json={"task_id": task_id, "run_id": run_id, "attempt": attempt, "duration_ms": wall_time_ms},
        )
        log_event(
            logging.INFO,
            "run_finished",
            final_run_status=RunStatus.success.value,
            final_task_status=TaskStatus.success.value,
            tokens_in=_as_int(tokens_in),
            tokens_out=_as_int(tokens_out),
            cost_usd=str(resolved_cost_usd),
            wall_time_ms=wall_time_ms,
            openai_request_id=openai_request_id,
            openai_request_ids=openai_request_ids,
        )

    except PayloadValidationError as e:
        usage_tokens_in, usage_tokens_out, usage_cost_usd, usage_request_ids, usage_ai_task_run_ids = _parse_exception_usage(e)
        resolved_failure_cost = to_decimal_8(usage_cost_usd)
        with SessionLocal() as db:
            run = db.get(Run, run_id)
            t = db.get(Task, task_id)
            wall_time_ms = int((time.time() - start_time) * 1000)
            err_msg = f"VALIDATION_ERROR: {e}"

            if run is not None:
                run.status = RunStatus.failed
                run.ended_at = now_utc()
                run.wall_time_ms = wall_time_ms
                run.tokens_in = _as_int(usage_tokens_in)
                run.tokens_out = _as_int(usage_tokens_out)
                run.cost_usd = resolved_failure_cost
                run.error = err_msg

            if t is not None:
                t.status = TaskStatus.failed_permanent
                t.next_run_at = None
                t.error = err_msg
                t.updated_at = now_utc()
                db.flush()
                _refresh_task_usage_totals(db, t)

            if run is not None and t is not None:
                _store_artifact(
                    db,
                    task_id=task_id,
                    run_id=run_id,
                    artifact_type=RESULT_ARTIFACT_TYPE,
                    content_json={
                        "error_type": "VALIDATION_ERROR",
                        "message": str(e),
                    },
                )
                _store_artifact(
                    db,
                    task_id=task_id,
                    run_id=run_id,
                    artifact_type=DEBUG_ARTIFACT_TYPE,
                    content_json={
                        "model": chosen_model,
                        "llm_used": bool(
                            usage_tokens_in
                            or usage_tokens_out
                            or usage_request_ids
                            or usage_ai_task_run_ids
                        ),
                        "tokens_in": _as_int(usage_tokens_in),
                        "tokens_out": _as_int(usage_tokens_out),
                        "cost_usd": str(resolved_failure_cost),
                        "wall_time_ms": wall_time_ms,
                        "openai_request_id": usage_request_ids[0] if usage_request_ids else None,
                        "openai_request_ids": usage_request_ids,
                        "ai_usage_task_run_ids": usage_ai_task_run_ids,
                        "task_run_history_id": history_task_run_id,
                        "error_type": "VALIDATION_ERROR",
                        "error": str(e),
                    },
                )
            db.commit()
        _safe_fail_task_run(
            history_task_run_id,
            error_text=err_msg,
            output_json={"error_type": "VALIDATION_ERROR", "message": str(e)},
            duration_ms=wall_time_ms,
            ended_at=now_utc(),
        )
        _safe_persist_event(
            event_type="task_failed",
            source="worker",
            level="ERROR",
            message=f"Task failed validation: {task_type}",
            metadata_json={"task_id": task_id, "run_id": run_id, "attempt": attempt, "error": err_msg},
        )

        log_event(
            logging.ERROR,
            "run_failed_validation",
            final_run_status=RunStatus.failed.value,
            final_task_status=TaskStatus.failed_permanent.value,
            wall_time_ms=wall_time_ms,
            error=str(e),
        )
        return

    except NonRetryableTaskError as e:
        usage_tokens_in, usage_tokens_out, usage_cost_usd, usage_request_ids, usage_ai_task_run_ids = _parse_exception_usage(e)
        resolved_failure_cost = to_decimal_8(usage_cost_usd)
        with SessionLocal() as db:
            run = db.get(Run, run_id)
            t = db.get(Task, task_id)
            wall_time_ms = int((time.time() - start_time) * 1000)
            err_msg = f"NON_RETRYABLE_ERROR: {e}"

            if run is not None:
                run.status = RunStatus.failed
                run.ended_at = now_utc()
                run.wall_time_ms = wall_time_ms
                run.tokens_in = _as_int(usage_tokens_in)
                run.tokens_out = _as_int(usage_tokens_out)
                run.cost_usd = resolved_failure_cost
                run.error = err_msg

            if t is not None:
                t.status = TaskStatus.failed_permanent
                t.next_run_at = None
                t.error = err_msg
                t.updated_at = now_utc()
                db.flush()
                _refresh_task_usage_totals(db, t)

            if run is not None and t is not None:
                _store_artifact(
                    db,
                    task_id=task_id,
                    run_id=run_id,
                    artifact_type=RESULT_ARTIFACT_TYPE,
                    content_json={
                        "error_type": "NON_RETRYABLE_ERROR",
                        "message": str(e),
                    },
                )
                _store_artifact(
                    db,
                    task_id=task_id,
                    run_id=run_id,
                    artifact_type=DEBUG_ARTIFACT_TYPE,
                    content_json={
                        "model": chosen_model,
                        "llm_used": bool(
                            usage_tokens_in
                            or usage_tokens_out
                            or usage_request_ids
                            or usage_ai_task_run_ids
                        ),
                        "tokens_in": _as_int(usage_tokens_in),
                        "tokens_out": _as_int(usage_tokens_out),
                        "cost_usd": str(resolved_failure_cost),
                        "wall_time_ms": wall_time_ms,
                        "openai_request_id": usage_request_ids[0] if usage_request_ids else None,
                        "openai_request_ids": usage_request_ids,
                        "ai_usage_task_run_ids": usage_ai_task_run_ids,
                        "task_run_history_id": history_task_run_id,
                        "error_type": "NON_RETRYABLE_ERROR",
                        "error": str(e),
                    },
                )
            db.commit()
        _safe_fail_task_run(
            history_task_run_id,
            error_text=err_msg,
            output_json={"error_type": "NON_RETRYABLE_ERROR", "message": str(e)},
            duration_ms=wall_time_ms,
            ended_at=now_utc(),
        )
        _safe_persist_event(
            event_type="task_failed",
            source="worker",
            level="ERROR",
            message=f"Task failed permanently: {task_type}",
            metadata_json={"task_id": task_id, "run_id": run_id, "attempt": attempt, "error": err_msg},
        )

        log_event(
            logging.ERROR,
            "run_failed_non_retryable",
            final_run_status=RunStatus.failed.value,
            final_task_status=TaskStatus.failed_permanent.value,
            wall_time_ms=wall_time_ms,
            error=str(e),
        )
        return

    except Exception as e:
        final_task_status_value = TaskStatus.failed_permanent.value
        usage_tokens_in, usage_tokens_out, usage_cost_usd, usage_request_ids, usage_ai_task_run_ids = _parse_exception_usage(e)
        resolved_failure_cost = to_decimal_8(usage_cost_usd)
        described_error = describe_runtime_error(e)
        with SessionLocal() as db:
            run = db.get(Run, run_id)
            t = db.get(Task, task_id)
            wall_time_ms = int((time.time() - start_time) * 1000)

            transient = is_transient_error(e)
            task_max_attempts = max(int(t.max_attempts if t is not None else 3), 1)
            can_retry = transient and attempt is not None and attempt < task_max_attempts
            retry_at = None

            if run is not None:
                run.status = RunStatus.failed
                run.ended_at = now_utc()
                run.wall_time_ms = wall_time_ms
                run.tokens_in = _as_int(usage_tokens_in)
                run.tokens_out = _as_int(usage_tokens_out)
                run.cost_usd = resolved_failure_cost
                run.error = described_error

            if t is not None:
                if can_retry:
                    delay_s = retry_delay_seconds(attempt or 1)
                    retry_at = now_utc() + timedelta(seconds=delay_s)
                    t.status = TaskStatus.queued
                    t.next_run_at = retry_at
                    t.error = described_error
                    t.updated_at = now_utc()
                else:
                    if attempt is not None and attempt >= task_max_attempts:
                        t.status = TaskStatus.failed_permanent
                    else:
                        t.status = TaskStatus.failed
                    t.next_run_at = None
                    t.error = described_error
                    t.updated_at = now_utc()
                final_task_status_value = t.status.value
                db.flush()
                _refresh_task_usage_totals(db, t)

            if run is not None and t is not None:
                _store_artifact(
                    db,
                    task_id=task_id,
                    run_id=run_id,
                    artifact_type=DEBUG_ARTIFACT_TYPE,
                    content_json={
                        "model": chosen_model,
                        "llm_used": bool(
                            usage_tokens_in
                            or usage_tokens_out
                            or usage_request_ids
                            or usage_ai_task_run_ids
                        ),
                        "tokens_in": _as_int(usage_tokens_in),
                        "tokens_out": _as_int(usage_tokens_out),
                        "cost_usd": str(resolved_failure_cost),
                        "wall_time_ms": wall_time_ms,
                        "openai_request_id": usage_request_ids[0] if usage_request_ids else None,
                        "openai_request_ids": usage_request_ids,
                        "ai_usage_task_run_ids": usage_ai_task_run_ids,
                        "task_run_history_id": history_task_run_id,
                        "retry_scheduled": bool(can_retry),
                        "retry_at": retry_at.isoformat() if retry_at is not None else None,
                        "error_type": type(e).__name__,
                        "error": described_error,
                    },
                )

            db.commit()

        if can_retry and retry_at is not None:
            try:
                queue.enqueue_at(retry_at, "worker.run_task", task_id)
            except Exception as enqueue_exc:
                logger.exception("retry_enqueue_at_failed task_id=%s retry_at=%s", task_id, retry_at.isoformat())
                _safe_persist_event(
                    event_type="task_enqueue_failed",
                    source="worker",
                    level="ERROR",
                    message=f"Retry enqueue failed: {task_type}",
                    metadata_json={
                        "task_id": task_id,
                        "run_id": run_id,
                        "attempt": attempt,
                        "error": f"{type(enqueue_exc).__name__}: {enqueue_exc}",
                        "scheduled_retry_at": retry_at.isoformat(),
                        "upstream_service": "redis",
                    },
                )
                return
            log_event(
                logging.WARNING,
                "run_retry_scheduled",
                final_run_status=RunStatus.failed.value,
                final_task_status=TaskStatus.queued.value,
                wall_time_ms=wall_time_ms,
                error=described_error,
                retry_at=retry_at.isoformat(),
                max_attempts=task_max_attempts,
            )
            _safe_fail_task_run(
                history_task_run_id,
                error_text=described_error,
                output_json={"will_retry": True, "retry_at": retry_at.isoformat()},
                duration_ms=wall_time_ms,
                ended_at=now_utc(),
            )
            _safe_persist_event(
                event_type="task_failed",
                source="worker",
                level="WARNING",
                message=f"Task failed and scheduled retry: {task_type}",
                metadata_json={
                    "task_id": task_id,
                    "run_id": run_id,
                    "attempt": attempt,
                    "error": described_error,
                    "retry_at": retry_at.isoformat(),
                },
            )
            return

        _safe_fail_task_run(
            history_task_run_id,
            error_text=described_error,
            output_json={"will_retry": False, "task_status": final_task_status_value},
            duration_ms=wall_time_ms,
            ended_at=now_utc(),
        )
        _safe_persist_event(
            event_type="task_failed",
            source="worker",
            level="ERROR",
            message=f"Task failed: {task_type}",
            metadata_json={"task_id": task_id, "run_id": run_id, "attempt": attempt, "error": described_error},
        )
        log_event(
            logging.ERROR,
            "run_finished",
            final_run_status=RunStatus.failed.value,
            final_task_status=final_task_status_value,
            wall_time_ms=wall_time_ms,
            error=described_error,
            max_attempts=task_max_attempts,
        )
        raise
