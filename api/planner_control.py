"""Persistent runtime controls for the autonomous planner."""

from __future__ import annotations

import json
import os
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

DB_PATH_ENV = "PLANNER_CONTROL_DB_PATH"
FALLBACK_ENV_PATHS = (
    "EVENT_LOG_DB_PATH",
    "AI_USAGE_DB_PATH",
    "TASK_RUN_HISTORY_DB_PATH",
)
DEFAULT_DB_FILENAME = "task_run_history.sqlite3"

PRESET_RTX5090_TEMPLATE_ID = "preset-rtx5090-deals-scan"
PRESET_JOBS_DIGEST_TEMPLATE_ID = "preset-jobs-digest-scan"


def _to_iso(ts: datetime | None = None) -> str:
    value = ts or datetime.now(timezone.utc)
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.isoformat()


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() == "true"


def _env_int(name: str, default: int, minimum: int) -> int:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return max(default, minimum)
    try:
        value = int(raw.strip())
    except ValueError:
        return max(default, minimum)
    return max(value, minimum)


def _env_float(name: str, default: float, minimum: float | None = None) -> float:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        value = float(default)
    else:
        try:
            value = float(raw.strip())
        except ValueError:
            value = float(default)
    if minimum is not None:
        value = max(value, minimum)
    return value


def _env_optional_float(name: str) -> float | None:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return None
    try:
        return float(raw.strip())
    except ValueError:
        return None


def _env_optional_int(name: str, minimum: int = 0) -> int | None:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return None
    try:
        value = int(raw.strip())
    except ValueError:
        return None
    return max(value, minimum)


def get_planner_control_db_path() -> Path:
    raw_path = os.getenv(DB_PATH_ENV)
    if not raw_path:
        for env_name in FALLBACK_ENV_PATHS:
            raw_path = os.getenv(env_name)
            if raw_path:
                break
    if not raw_path:
        raw_path = DEFAULT_DB_FILENAME

    path = Path(raw_path).expanduser()
    if not path.is_absolute():
        path = Path.cwd() / path
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(get_planner_control_db_path(), timeout=5.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS planner_runtime_config (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            enabled INTEGER NOT NULL,
            execution_enabled INTEGER NOT NULL,
            require_approval INTEGER NOT NULL,
            approved INTEGER NOT NULL,
            interval_sec INTEGER NOT NULL,
            max_create_per_cycle INTEGER NOT NULL,
            max_execute_per_cycle INTEGER NOT NULL,
            max_pending_tasks INTEGER NOT NULL,
            failure_lookback_minutes INTEGER NOT NULL,
            failure_alert_count_threshold INTEGER NOT NULL,
            failure_alert_rate_threshold REAL NOT NULL,
            stale_task_age_seconds INTEGER NOT NULL,
            execute_task_cooldown_seconds INTEGER NOT NULL,
            health_cpu_max_percent REAL NOT NULL,
            health_memory_max_percent REAL NOT NULL,
            health_disk_max_percent REAL NOT NULL,
            cost_budget_usd REAL,
            token_budget INTEGER,
            create_task_cooldown_seconds INTEGER NOT NULL,
            create_task_max_attempts INTEGER NOT NULL,
            updated_at TEXT NOT NULL,
            updated_by TEXT
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS planner_task_templates (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            task_type TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            model TEXT,
            max_attempts INTEGER NOT NULL,
            min_interval_seconds INTEGER NOT NULL,
            enabled INTEGER NOT NULL DEFAULT 1,
            priority INTEGER NOT NULL DEFAULT 100,
            metadata_json TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS ix_planner_task_templates_enabled_priority "
        "ON planner_task_templates(enabled, priority, updated_at DESC)"
    )
    conn.commit()


def _default_runtime_config() -> dict[str, Any]:
    return {
        "enabled": _env_bool("AUTONOMOUS_PLANNER_ENABLED", False),
        "execution_enabled": _env_bool("AUTONOMOUS_PLANNER_EXECUTE", False),
        "require_approval": _env_bool("AUTONOMOUS_PLANNER_REQUIRE_APPROVAL", True),
        "approved": _env_bool("AUTONOMOUS_PLANNER_APPROVED", False),
        "interval_sec": _env_int("AUTONOMOUS_PLANNER_INTERVAL_SEC", 300, minimum=30),
        "max_create_per_cycle": _env_int("AUTONOMOUS_PLANNER_MAX_CREATE_PER_CYCLE", 1, minimum=0),
        "max_execute_per_cycle": _env_int("AUTONOMOUS_PLANNER_MAX_EXECUTE_PER_CYCLE", 2, minimum=0),
        "max_pending_tasks": _env_int("AUTONOMOUS_PLANNER_MAX_PENDING_TASKS", 20, minimum=1),
        "failure_lookback_minutes": _env_int("AUTONOMOUS_PLANNER_FAILURE_LOOKBACK_MINUTES", 60, minimum=1),
        "failure_alert_count_threshold": _env_int(
            "AUTONOMOUS_PLANNER_FAILURE_ALERT_COUNT_THRESHOLD",
            5,
            minimum=1,
        ),
        "failure_alert_rate_threshold": _env_float(
            "AUTONOMOUS_PLANNER_FAILURE_ALERT_RATE_THRESHOLD",
            0.5,
            minimum=0.0,
        ),
        "stale_task_age_seconds": _env_int("AUTONOMOUS_PLANNER_STALE_TASK_AGE_SECONDS", 180, minimum=30),
        "execute_task_cooldown_seconds": _env_int(
            "AUTONOMOUS_PLANNER_EXECUTE_TASK_COOLDOWN_SECONDS",
            600,
            minimum=30,
        ),
        "health_cpu_max_percent": _env_float("AUTONOMOUS_PLANNER_HEALTH_CPU_MAX_PERCENT", 90.0, minimum=1.0),
        "health_memory_max_percent": _env_float(
            "AUTONOMOUS_PLANNER_HEALTH_MEMORY_MAX_PERCENT",
            90.0,
            minimum=1.0,
        ),
        "health_disk_max_percent": _env_float("AUTONOMOUS_PLANNER_HEALTH_DISK_MAX_PERCENT", 95.0, minimum=1.0),
        "cost_budget_usd": _env_optional_float("AUTONOMOUS_PLANNER_COST_BUDGET_USD")
        if os.getenv("AUTONOMOUS_PLANNER_COST_BUDGET_USD")
        else _env_optional_float("DAILY_BUDGET_USD"),
        "token_budget": _env_optional_int("AUTONOMOUS_PLANNER_TOKEN_BUDGET", minimum=0),
        "create_task_cooldown_seconds": _env_int(
            "AUTONOMOUS_PLANNER_CREATE_TASK_COOLDOWN_SECONDS",
            1800,
            minimum=60,
        ),
        "create_task_max_attempts": _env_int("AUTONOMOUS_PLANNER_CREATE_TASK_MAX_ATTEMPTS", 3, minimum=1),
    }


def _validate_runtime_config(cfg: dict[str, Any]) -> dict[str, Any]:
    validated = dict(cfg)

    validated["enabled"] = bool(validated.get("enabled", False))
    validated["execution_enabled"] = bool(validated.get("execution_enabled", False))
    validated["require_approval"] = bool(validated.get("require_approval", True))
    validated["approved"] = bool(validated.get("approved", False))

    def as_int(key: str, minimum: int) -> int:
        value = validated.get(key)
        if value is None:
            raise ValueError(f"{key} cannot be null")
        try:
            parsed = int(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{key} must be an integer") from exc
        return max(parsed, minimum)

    def as_float(key: str, minimum: float | None = None) -> float:
        value = validated.get(key)
        if value is None:
            raise ValueError(f"{key} cannot be null")
        try:
            parsed = float(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{key} must be a number") from exc
        if minimum is not None:
            parsed = max(parsed, minimum)
        return parsed

    validated["interval_sec"] = as_int("interval_sec", 30)
    validated["max_create_per_cycle"] = as_int("max_create_per_cycle", 0)
    validated["max_execute_per_cycle"] = as_int("max_execute_per_cycle", 0)
    validated["max_pending_tasks"] = as_int("max_pending_tasks", 1)
    validated["failure_lookback_minutes"] = as_int("failure_lookback_minutes", 1)
    validated["failure_alert_count_threshold"] = as_int("failure_alert_count_threshold", 1)
    validated["failure_alert_rate_threshold"] = as_float("failure_alert_rate_threshold", 0.0)
    validated["stale_task_age_seconds"] = as_int("stale_task_age_seconds", 30)
    validated["execute_task_cooldown_seconds"] = as_int("execute_task_cooldown_seconds", 30)
    validated["health_cpu_max_percent"] = as_float("health_cpu_max_percent", 1.0)
    validated["health_memory_max_percent"] = as_float("health_memory_max_percent", 1.0)
    validated["health_disk_max_percent"] = as_float("health_disk_max_percent", 1.0)
    validated["create_task_cooldown_seconds"] = as_int("create_task_cooldown_seconds", 60)
    validated["create_task_max_attempts"] = as_int("create_task_max_attempts", 1)

    cost_budget_usd = validated.get("cost_budget_usd")
    if cost_budget_usd in (None, ""):
        validated["cost_budget_usd"] = None
    else:
        try:
            parsed_cost = float(cost_budget_usd)
        except (TypeError, ValueError) as exc:
            raise ValueError("cost_budget_usd must be a number or null") from exc
        validated["cost_budget_usd"] = max(parsed_cost, 0.0)

    token_budget = validated.get("token_budget")
    if token_budget in (None, ""):
        validated["token_budget"] = None
    else:
        try:
            parsed_token_budget = int(token_budget)
        except (TypeError, ValueError) as exc:
            raise ValueError("token_budget must be an integer or null") from exc
        validated["token_budget"] = max(parsed_token_budget, 0)

    return validated


def _row_to_runtime_config(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "enabled": bool(row["enabled"]),
        "execution_enabled": bool(row["execution_enabled"]),
        "require_approval": bool(row["require_approval"]),
        "approved": bool(row["approved"]),
        "interval_sec": int(row["interval_sec"]),
        "max_create_per_cycle": int(row["max_create_per_cycle"]),
        "max_execute_per_cycle": int(row["max_execute_per_cycle"]),
        "max_pending_tasks": int(row["max_pending_tasks"]),
        "failure_lookback_minutes": int(row["failure_lookback_minutes"]),
        "failure_alert_count_threshold": int(row["failure_alert_count_threshold"]),
        "failure_alert_rate_threshold": float(row["failure_alert_rate_threshold"]),
        "stale_task_age_seconds": int(row["stale_task_age_seconds"]),
        "execute_task_cooldown_seconds": int(row["execute_task_cooldown_seconds"]),
        "health_cpu_max_percent": float(row["health_cpu_max_percent"]),
        "health_memory_max_percent": float(row["health_memory_max_percent"]),
        "health_disk_max_percent": float(row["health_disk_max_percent"]),
        "cost_budget_usd": row["cost_budget_usd"],
        "token_budget": row["token_budget"],
        "create_task_cooldown_seconds": int(row["create_task_cooldown_seconds"]),
        "create_task_max_attempts": int(row["create_task_max_attempts"]),
        "updated_at": row["updated_at"],
        "updated_by": row["updated_by"],
    }


def _upsert_runtime_config(conn: sqlite3.Connection, cfg: dict[str, Any], updated_by: str | None) -> None:
    safe_cfg = _validate_runtime_config(cfg)
    now_iso = _to_iso()
    conn.execute(
        """
        INSERT INTO planner_runtime_config (
            id,
            enabled,
            execution_enabled,
            require_approval,
            approved,
            interval_sec,
            max_create_per_cycle,
            max_execute_per_cycle,
            max_pending_tasks,
            failure_lookback_minutes,
            failure_alert_count_threshold,
            failure_alert_rate_threshold,
            stale_task_age_seconds,
            execute_task_cooldown_seconds,
            health_cpu_max_percent,
            health_memory_max_percent,
            health_disk_max_percent,
            cost_budget_usd,
            token_budget,
            create_task_cooldown_seconds,
            create_task_max_attempts,
            updated_at,
            updated_by
        ) VALUES (
            1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
        ON CONFLICT(id) DO UPDATE SET
            enabled = excluded.enabled,
            execution_enabled = excluded.execution_enabled,
            require_approval = excluded.require_approval,
            approved = excluded.approved,
            interval_sec = excluded.interval_sec,
            max_create_per_cycle = excluded.max_create_per_cycle,
            max_execute_per_cycle = excluded.max_execute_per_cycle,
            max_pending_tasks = excluded.max_pending_tasks,
            failure_lookback_minutes = excluded.failure_lookback_minutes,
            failure_alert_count_threshold = excluded.failure_alert_count_threshold,
            failure_alert_rate_threshold = excluded.failure_alert_rate_threshold,
            stale_task_age_seconds = excluded.stale_task_age_seconds,
            execute_task_cooldown_seconds = excluded.execute_task_cooldown_seconds,
            health_cpu_max_percent = excluded.health_cpu_max_percent,
            health_memory_max_percent = excluded.health_memory_max_percent,
            health_disk_max_percent = excluded.health_disk_max_percent,
            cost_budget_usd = excluded.cost_budget_usd,
            token_budget = excluded.token_budget,
            create_task_cooldown_seconds = excluded.create_task_cooldown_seconds,
            create_task_max_attempts = excluded.create_task_max_attempts,
            updated_at = excluded.updated_at,
            updated_by = excluded.updated_by
        """,
        (
            1 if safe_cfg["enabled"] else 0,
            1 if safe_cfg["execution_enabled"] else 0,
            1 if safe_cfg["require_approval"] else 0,
            1 if safe_cfg["approved"] else 0,
            safe_cfg["interval_sec"],
            safe_cfg["max_create_per_cycle"],
            safe_cfg["max_execute_per_cycle"],
            safe_cfg["max_pending_tasks"],
            safe_cfg["failure_lookback_minutes"],
            safe_cfg["failure_alert_count_threshold"],
            safe_cfg["failure_alert_rate_threshold"],
            safe_cfg["stale_task_age_seconds"],
            safe_cfg["execute_task_cooldown_seconds"],
            safe_cfg["health_cpu_max_percent"],
            safe_cfg["health_memory_max_percent"],
            safe_cfg["health_disk_max_percent"],
            safe_cfg["cost_budget_usd"],
            safe_cfg["token_budget"],
            safe_cfg["create_task_cooldown_seconds"],
            safe_cfg["create_task_max_attempts"],
            now_iso,
            updated_by,
        ),
    )
    conn.commit()


def get_planner_runtime_config() -> dict[str, Any]:
    with _connect() as conn:
        _ensure_schema(conn)
        row = conn.execute(
            "SELECT * FROM planner_runtime_config WHERE id = 1"
        ).fetchone()
        if row is None:
            _upsert_runtime_config(conn, _default_runtime_config(), updated_by="bootstrap")
            row = conn.execute(
                "SELECT * FROM planner_runtime_config WHERE id = 1"
            ).fetchone()
        if row is None:
            raise RuntimeError("planner_runtime_config bootstrap failed")
    return _row_to_runtime_config(row)


def update_planner_runtime_config(patch: dict[str, Any], *, updated_by: str | None = None) -> dict[str, Any]:
    current = get_planner_runtime_config()
    merged = dict(current)
    for key, value in patch.items():
        if key in {"updated_at", "updated_by"}:
            continue
        if key not in current:
            raise ValueError(f"Unsupported planner config key: {key}")
        merged[key] = value

    with _connect() as conn:
        _ensure_schema(conn)
        _upsert_runtime_config(conn, merged, updated_by=updated_by)

    return get_planner_runtime_config()


def reset_planner_runtime_config(*, updated_by: str | None = None) -> dict[str, Any]:
    defaults = _default_runtime_config()
    with _connect() as conn:
        _ensure_schema(conn)
        _upsert_runtime_config(conn, defaults, updated_by=updated_by)
    return get_planner_runtime_config()


def _normalize_payload_json(payload_json: str) -> str:
    try:
        parsed = json.loads(payload_json)
    except json.JSONDecodeError as exc:
        raise ValueError(f"payload_json must be valid JSON: {exc.msg}") from exc
    return json.dumps(parsed, separators=(",", ":"), ensure_ascii=True, sort_keys=True)


def _normalize_metadata_json(metadata_json: Any) -> str | None:
    if metadata_json is None:
        return None
    if isinstance(metadata_json, str):
        try:
            parsed = json.loads(metadata_json)
        except json.JSONDecodeError as exc:
            raise ValueError(f"metadata_json must be valid JSON if provided: {exc.msg}") from exc
        return json.dumps(parsed, separators=(",", ":"), ensure_ascii=True, sort_keys=True)
    if isinstance(metadata_json, dict):
        return json.dumps(metadata_json, separators=(",", ":"), ensure_ascii=True, sort_keys=True)
    raise ValueError("metadata_json must be an object, JSON string, or null")


def _template_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    metadata_obj: Any = None
    raw_metadata = row["metadata_json"]
    if raw_metadata is not None:
        try:
            metadata_obj = json.loads(raw_metadata)
        except json.JSONDecodeError:
            metadata_obj = None

    return {
        "id": row["id"],
        "name": row["name"],
        "task_type": row["task_type"],
        "payload_json": row["payload_json"],
        "model": row["model"],
        "max_attempts": int(row["max_attempts"]),
        "min_interval_seconds": int(row["min_interval_seconds"]),
        "enabled": bool(row["enabled"]),
        "priority": int(row["priority"]),
        "metadata_json": metadata_obj,
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def list_planner_task_templates(limit: int = 100, *, enabled_only: bool = False) -> list[dict[str, Any]]:
    safe_limit = max(1, min(int(limit), 1000))
    with _connect() as conn:
        _ensure_schema(conn)
        if enabled_only:
            rows = conn.execute(
                """
                SELECT *
                FROM planner_task_templates
                WHERE enabled = 1
                ORDER BY priority ASC, updated_at DESC
                LIMIT ?
                """,
                (safe_limit,),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT *
                FROM planner_task_templates
                ORDER BY enabled DESC, priority ASC, updated_at DESC
                LIMIT ?
                """,
                (safe_limit,),
            ).fetchall()
    return [_template_row_to_dict(row) for row in rows]


def get_planner_task_template(template_id: str) -> dict[str, Any] | None:
    with _connect() as conn:
        _ensure_schema(conn)
        row = conn.execute(
            "SELECT * FROM planner_task_templates WHERE id = ?",
            (template_id,),
        ).fetchone()
    if row is None:
        return None
    return _template_row_to_dict(row)


def create_planner_task_template(
    *,
    name: str,
    task_type: str,
    payload_json: str,
    model: str | None = None,
    max_attempts: int = 3,
    min_interval_seconds: int = 300,
    enabled: bool = True,
    priority: int = 100,
    metadata_json: Any = None,
    template_id: str | None = None,
) -> dict[str, Any]:
    name_clean = name.strip()
    task_type_clean = task_type.strip()
    if not name_clean:
        raise ValueError("name is required")
    if not task_type_clean:
        raise ValueError("task_type is required")

    payload_compact = _normalize_payload_json(payload_json)
    metadata_compact = _normalize_metadata_json(metadata_json)
    max_attempts_safe = max(int(max_attempts), 1)
    min_interval_safe = max(int(min_interval_seconds), 60)
    priority_safe = int(priority)
    now_iso = _to_iso()
    row_id = template_id or str(uuid.uuid4())

    with _connect() as conn:
        _ensure_schema(conn)
        conn.execute(
            """
            INSERT INTO planner_task_templates (
                id, name, task_type, payload_json, model, max_attempts,
                min_interval_seconds, enabled, priority, metadata_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                row_id,
                name_clean,
                task_type_clean,
                payload_compact,
                model.strip() if isinstance(model, str) and model.strip() else None,
                max_attempts_safe,
                min_interval_safe,
                1 if enabled else 0,
                priority_safe,
                metadata_compact,
                now_iso,
                now_iso,
            ),
        )
        conn.commit()

    created = get_planner_task_template(row_id)
    if created is None:
        raise RuntimeError("planner task template creation failed")
    return created


def update_planner_task_template(template_id: str, patch: dict[str, Any]) -> dict[str, Any]:
    current = get_planner_task_template(template_id)
    if current is None:
        raise ValueError("template not found")

    merged = dict(current)
    for key, value in patch.items():
        if key in {"id", "created_at", "updated_at"}:
            continue
        if key not in merged:
            raise ValueError(f"Unsupported template key: {key}")
        merged[key] = value

    name_clean = str(merged.get("name") or "").strip()
    task_type_clean = str(merged.get("task_type") or "").strip()
    if not name_clean:
        raise ValueError("name is required")
    if not task_type_clean:
        raise ValueError("task_type is required")

    payload_compact = _normalize_payload_json(str(merged.get("payload_json") or "{}"))
    metadata_compact = _normalize_metadata_json(merged.get("metadata_json"))
    max_attempts_safe = max(int(merged.get("max_attempts") or 1), 1)
    min_interval_safe = max(int(merged.get("min_interval_seconds") or 60), 60)
    priority_safe = int(merged.get("priority") or 0)

    with _connect() as conn:
        _ensure_schema(conn)
        conn.execute(
            """
            UPDATE planner_task_templates
            SET name = ?,
                task_type = ?,
                payload_json = ?,
                model = ?,
                max_attempts = ?,
                min_interval_seconds = ?,
                enabled = ?,
                priority = ?,
                metadata_json = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (
                name_clean,
                task_type_clean,
                payload_compact,
                str(merged.get("model")).strip() if isinstance(merged.get("model"), str) and str(merged.get("model")).strip() else None,
                max_attempts_safe,
                min_interval_safe,
                1 if bool(merged.get("enabled")) else 0,
                priority_safe,
                metadata_compact,
                _to_iso(),
                template_id,
            ),
        )
        conn.commit()

    updated = get_planner_task_template(template_id)
    if updated is None:
        raise RuntimeError("planner task template update failed")
    return updated


def delete_planner_task_template(template_id: str) -> bool:
    with _connect() as conn:
        _ensure_schema(conn)
        cursor = conn.execute(
            "DELETE FROM planner_task_templates WHERE id = ?",
            (template_id,),
        )
        conn.commit()
    return int(cursor.rowcount or 0) > 0


def list_enabled_planner_task_templates(limit: int = 200) -> list[dict[str, Any]]:
    return list_planner_task_templates(limit=limit, enabled_only=True)


def ensure_rtx5090_deals_template(
    *,
    interval_seconds: int = 300,
    gpu_max_price: float | None = None,
    pc_max_price: float | None = None,
    enabled: bool = True,
) -> dict[str, Any]:
    safe_interval = max(int(interval_seconds), 60)
    payload_obj: dict[str, Any] = {
        "source": "autonomous-planner-rtx5090",
        "collectors_enabled": True,
        "payload_nonce": "{{uuid4}}",
    }
    if gpu_max_price is not None:
        payload_obj["unicorn_gpu_5090_max_price"] = float(gpu_max_price)
    if pc_max_price is not None:
        payload_obj["unicorn_pc_5090_max_price"] = float(pc_max_price)

    payload_json = json.dumps(payload_obj, separators=(",", ":"), ensure_ascii=True, sort_keys=True)

    existing = get_planner_task_template(PRESET_RTX5090_TEMPLATE_ID)
    if existing is None:
        return create_planner_task_template(
            template_id=PRESET_RTX5090_TEMPLATE_ID,
            name="RTX 5090 deals scan",
            task_type="deals_scan_v1",
            payload_json=payload_json,
            model=None,
            max_attempts=3,
            min_interval_seconds=safe_interval,
            enabled=enabled,
            priority=10,
            metadata_json={
                "preset": "rtx5090",
                "description": "Scan RTX 5090 deals and trigger unicorn notifications",
                "payload_strategy": "runtime_nonce",
                "watcher_category": "deals",
                "notification_behavior": {
                    "mode": "notify_on_unicorn",
                    "channel": "operator_default",
                },
            },
        )

    return update_planner_task_template(
        PRESET_RTX5090_TEMPLATE_ID,
        {
            "name": "RTX 5090 deals scan",
            "task_type": "deals_scan_v1",
            "payload_json": payload_json,
            "model": None,
            "max_attempts": 3,
            "min_interval_seconds": safe_interval,
            "enabled": enabled,
            "priority": 10,
            "metadata_json": {
                "preset": "rtx5090",
                "description": "Scan RTX 5090 deals and trigger unicorn notifications",
                "payload_strategy": "runtime_nonce",
                "watcher_category": "deals",
                "notification_behavior": {
                    "mode": "notify_on_unicorn",
                    "channel": "operator_default",
                },
            },
        },
    )


def ensure_jobs_digest_template(
    *,
    interval_seconds: int = 300,
    search_mode: str | None = None,
    desired_title: str | None = None,
    desired_titles: list[str] | None = None,
    keywords: list[str] | None = None,
    excluded_keywords: list[str] | None = None,
    preferred_locations: list[str] | None = None,
    remote_preference: list[str] | None = None,
    minimum_salary: float | None = None,
    experience_level: str | None = None,
    enabled_sources: list[str] | None = None,
    result_limit_per_source: int | None = None,
    minimum_raw_jobs_total: int | None = None,
    minimum_unique_jobs_total: int | None = None,
    minimum_jobs_per_source: int | None = None,
    stop_when_minimum_reached: bool | None = None,
    collection_time_cap_seconds: int | None = None,
    max_queries_per_run: int | None = None,
    shortlist_count: int | None = None,
    freshness_preference: str | None = None,
    jobs_notification_cooldown_days: int | None = None,
    jobs_shortlist_repeat_penalty: float | None = None,
    resurface_seen_jobs: bool | None = None,
    desired_salary_min: float | None = None,
    desired_salary_max: float | None = None,
    experience_levels: list[str] | None = None,
    clearance_required: bool | None = None,
    location: str | None = None,
    enabled: bool = True,
    boards: list[str] | None = None,
) -> dict[str, Any]:
    active_sources = ["linkedin", "indeed"]
    legacy_disabled_sources = {"glassdoor", "handshake"}
    allowed_sources = set(active_sources) | legacy_disabled_sources
    allowed_work_modes = {"remote", "hybrid", "onsite"}
    allowed_freshness = {"off", "prefer_recent", "strong_prefer_recent"}
    allowed_search_modes = {"broad_discovery", "precision_match"}

    def _normalize_text_list(values: list[str] | None, *, lower: bool = False) -> list[str]:
        output: list[str] = []
        seen: set[str] = set()
        for item in values or []:
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

    def _normalize_experience(value: str | None) -> str | None:
        if not isinstance(value, str) or not value.strip():
            return None
        low = value.strip().lower()
        if low in {"intern", "internship", "co-op", "coop"}:
            return "internship"
        if low in {"entry", "entry-level", "junior", "new grad", "associate"}:
            return "entry"
        if low in {"mid", "mid-level", "intermediate"}:
            return "mid"
        if low in {"senior", "lead", "staff", "principal", "manager", "director"}:
            return "senior"
        return None

    def _normalize_work_mode(value: str) -> str | None:
        low = value.strip().lower().replace("_", "-")
        if low in {"on-site", "onsite"}:
            return "onsite"
        if low in {"remote", "hybrid"}:
            return low
        return None

    safe_interval = max(int(interval_seconds), 60)
    normalized_search_mode = str(search_mode or "broad_discovery").strip().lower().replace("-", "_").replace(" ", "_")
    if normalized_search_mode not in allowed_search_modes:
        normalized_search_mode = "broad_discovery"
    title_list = _normalize_text_list(desired_titles)
    if isinstance(desired_title, str) and desired_title.strip():
        desired_title_text = desired_title.strip()
        if desired_title_text.lower() not in {row.lower() for row in title_list}:
            title_list.insert(0, desired_title_text)
    if not title_list:
        title_list = ["machine learning engineer", "ai engineer", "data scientist"]

    keyword_list = _normalize_text_list(keywords)
    if not keyword_list:
        keyword_list = list(title_list)
    excluded_keyword_list = _normalize_text_list(excluded_keywords)

    location_list = _normalize_text_list(preferred_locations)
    if isinstance(location, str) and location.strip():
        location_value = location.strip()
        if location_value.lower() not in {row.lower() for row in location_list}:
            location_list.insert(0, location_value)
    if not location_list:
        location_list = ["United States"]

    preferred_modes = _normalize_text_list(remote_preference, lower=True)
    normalized_modes: list[str] = []
    for item in preferred_modes:
        parsed = _normalize_work_mode(item)
        if parsed and parsed in allowed_work_modes and parsed not in normalized_modes:
            normalized_modes.append(parsed)
    if not normalized_modes:
        normalized_modes = ["remote", "hybrid"]

    normalized_experience_levels = [_normalize_experience(row) for row in (experience_levels or [])]
    normalized_experience_levels = [row for row in normalized_experience_levels if row]
    normalized_experience_levels = _normalize_text_list(normalized_experience_levels, lower=True)
    experience_value = _normalize_experience(experience_level)
    if experience_value and experience_value not in normalized_experience_levels:
        normalized_experience_levels.insert(0, experience_value)
    if not normalized_experience_levels:
        normalized_experience_levels = ["entry", "mid", "senior"]
    if not experience_value:
        experience_value = normalized_experience_levels[0]

    preferred_sources = _normalize_text_list(enabled_sources, lower=True)
    fallback_boards = _normalize_text_list(boards, lower=True)
    requested_sources = _normalize_text_list(
        [row for row in preferred_sources + fallback_boards if row in allowed_sources],
        lower=True,
    )
    disabled_sources = [row for row in requested_sources if row in legacy_disabled_sources]
    safe_boards = [row for row in requested_sources if row in active_sources]
    source_configuration_notes: list[str] = []
    if disabled_sources:
        source_configuration_notes.append(
            "Inactive legacy job sources were ignored. Only linkedin and indeed are active."
        )
    if not safe_boards:
        safe_boards = list(active_sources)
        if requested_sources:
            source_configuration_notes.append(
                "No active job sources remained after filtering inactive or unsupported sources; defaulted to linkedin and indeed."
            )

    recommended_result_limit = 120 if normalized_search_mode == "broad_discovery" else 60
    recommended_minimum_raw = 120 if normalized_search_mode == "broad_discovery" else 60
    recommended_minimum_unique = 80 if normalized_search_mode == "broad_discovery" else 40
    recommended_minimum_per_source = 25 if normalized_search_mode == "broad_discovery" else 15
    recommended_time_cap = 120 if normalized_search_mode == "broad_discovery" else 90

    max_jobs_source = result_limit_per_source if result_limit_per_source is not None else recommended_result_limit
    minimum_raw_jobs_source = minimum_raw_jobs_total if minimum_raw_jobs_total is not None else recommended_minimum_raw
    minimum_unique_jobs_source = (
        minimum_unique_jobs_total if minimum_unique_jobs_total is not None else recommended_minimum_unique
    )
    minimum_per_source_source = (
        minimum_jobs_per_source if minimum_jobs_per_source is not None else recommended_minimum_per_source
    )
    time_cap_source = collection_time_cap_seconds if collection_time_cap_seconds is not None else recommended_time_cap

    max_jobs = max(1, min(int(max_jobs_source), 1000))
    minimum_raw_jobs = max(0, min(int(minimum_raw_jobs_source), 5000))
    minimum_unique_jobs = max(0, min(int(minimum_unique_jobs_source), 5000))
    minimum_per_source_jobs = max(0, min(int(minimum_per_source_source), 1000))
    stop_on_minimum = True if stop_when_minimum_reached is None else bool(stop_when_minimum_reached)
    time_cap_seconds = max(1, min(int(time_cap_source), 3600))
    max_queries_default = 14 if normalized_search_mode == "broad_discovery" else 8
    max_queries = max(1, min(int(max_queries_per_run or max_queries_default), 20))
    shortlist_size = max(1, min(int(shortlist_count or 10), 10))
    freshness = str(freshness_preference or "off").strip().lower().replace("-", "_").replace(" ", "_")
    if freshness not in allowed_freshness:
        freshness = "off"
    cooldown_days = max(0, min(int(jobs_notification_cooldown_days or 3), 30))
    repeat_penalty = max(0.0, min(float(jobs_shortlist_repeat_penalty or 4.0), 20.0))
    resurface_seen = True if resurface_seen_jobs is None else bool(resurface_seen_jobs)

    salary_min = minimum_salary if minimum_salary is not None else desired_salary_min
    salary_max = desired_salary_max
    if salary_min is not None and salary_max is not None and float(salary_max) < float(salary_min):
        salary_min, salary_max = salary_max, salary_min

    request_obj: dict[str, Any] = {
        "query": title_list[0],
        "location": location_list[0],
        "search_mode": normalized_search_mode,
        "collectors_enabled": True,
        "sources": safe_boards,
        "enabled_sources": safe_boards,
        "disabled_sources": disabled_sources,
        "source_configuration_notes": source_configuration_notes,
        "max_jobs_per_source": max_jobs,
        "result_limit_per_source": max_jobs,
        "minimum_raw_jobs_total": minimum_raw_jobs,
        "minimum_unique_jobs_total": minimum_unique_jobs,
        "minimum_jobs_per_source": minimum_per_source_jobs,
        "stop_when_minimum_reached": stop_on_minimum,
        "collection_time_cap_seconds": time_cap_seconds,
        "max_queries_per_run": max_queries,
        "enable_query_expansion": normalized_search_mode == "broad_discovery",
        "profile_mode": "resume_profile",
        "titles": title_list,
        "desired_title_keywords": title_list,
        "keywords": keyword_list,
        "excluded_keywords": excluded_keyword_list,
        "locations": location_list,
        "work_mode_preference": normalized_modes,
        "work_modes": normalized_modes,
        "minimum_salary": float(salary_min) if salary_min is not None else None,
        "desired_salary_min": float(salary_min) if salary_min is not None else None,
        "experience_level": experience_value,
        "experience_levels": normalized_experience_levels,
        "shortlist_max_items": shortlist_size,
        "shortlist_freshness_preference": freshness,
        "shortlist_freshness_weight_enabled": freshness in {"prefer_recent", "strong_prefer_recent"},
        "shortlist_freshness_max_bonus": 6.0 if freshness == "prefer_recent" else (12.0 if freshness == "strong_prefer_recent" else 0.0),
        "jobs_notification_cooldown_days": cooldown_days,
        "jobs_shortlist_repeat_penalty": repeat_penalty,
        "resurface_seen_jobs": resurface_seen,
        "shortlist_min_score": 0.5 if normalized_search_mode == "broad_discovery" else 0.85,
        "shortlist_fail_soft_enabled": normalized_search_mode == "broad_discovery",
        "shortlist_fallback_min_items": 5 if normalized_search_mode == "broad_discovery" else 0,
        "notify_on_empty": True,
        "payload_nonce": "{{uuid4}}",
    }
    if salary_max is not None:
        request_obj["desired_salary_max"] = float(salary_max)
    if clearance_required is not None:
        request_obj["clearance_required"] = bool(clearance_required)

    payload_obj: dict[str, Any] = {
        "pipeline_id": "{{uuid4}}",
        "request": request_obj,
    }

    payload_json = json.dumps(payload_obj, separators=(",", ":"), ensure_ascii=True, sort_keys=True)

    existing = get_planner_task_template(PRESET_JOBS_DIGEST_TEMPLATE_ID)
    if existing is None:
        return create_planner_task_template(
            template_id=PRESET_JOBS_DIGEST_TEMPLATE_ID,
            name="Autonomous jobs digest",
            task_type="jobs_collect_v1",
            payload_json=payload_json,
            model=None,
            max_attempts=3,
            min_interval_seconds=safe_interval,
            enabled=enabled,
            priority=20,
            metadata_json={
                "preset": "jobs_digest",
                "description": "Scrape supported job boards and summarize top matches",
                "payload_strategy": "runtime_nonce",
                "watcher_category": "jobs",
                "source_policy": {
                    "active_sources": list(active_sources),
                    "disabled_sources": disabled_sources,
                    "notes": source_configuration_notes,
                },
                "notification_behavior": {
                    "mode": "digest",
                    "channel": "operator_default",
                },
            },
        )

    return update_planner_task_template(
        PRESET_JOBS_DIGEST_TEMPLATE_ID,
        {
            "name": "Autonomous jobs digest",
            "task_type": "jobs_collect_v1",
            "payload_json": payload_json,
            "model": None,
            "max_attempts": 3,
            "min_interval_seconds": safe_interval,
            "enabled": enabled,
            "priority": 20,
            "metadata_json": {
                "preset": "jobs_digest",
                "description": "Scrape supported job boards and summarize top matches",
                "payload_strategy": "runtime_nonce",
                "watcher_category": "jobs",
                "source_policy": {
                    "active_sources": list(active_sources),
                    "disabled_sources": disabled_sources,
                    "notes": source_configuration_notes,
                },
                "notification_behavior": {
                    "mode": "digest",
                    "channel": "operator_default",
                },
            },
        },
    )
