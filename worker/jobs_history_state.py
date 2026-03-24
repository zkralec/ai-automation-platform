from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import text


def _db_usable(db: Any) -> bool:
    return hasattr(db, "execute") and hasattr(db, "connection")


def _to_iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _json_safe_timestamp(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return _to_iso(value)
    if isinstance(value, str):
        text = value.strip()
        return text or None
    return str(value)


def ensure_jobs_history_table(conn: Any) -> None:
    dialect = getattr(getattr(conn, "dialect", None), "name", "")
    if dialect == "sqlite":
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS jobs_v2_history (
                    canonical_job_key TEXT PRIMARY KEY,
                    first_seen_at TEXT NOT NULL,
                    last_seen_at TEXT NOT NULL,
                    times_seen INTEGER NOT NULL DEFAULT 0,
                    times_shortlisted INTEGER NOT NULL DEFAULT 0,
                    times_notified INTEGER NOT NULL DEFAULT 0,
                    last_shortlisted_at TEXT,
                    last_notified_at TEXT,
                    last_title TEXT,
                    last_company TEXT,
                    last_source TEXT,
                    last_source_url TEXT
                )
                """
            )
        )
    else:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS jobs_v2_history (
                    canonical_job_key TEXT PRIMARY KEY,
                    first_seen_at TIMESTAMPTZ NOT NULL,
                    last_seen_at TIMESTAMPTZ NOT NULL,
                    times_seen INTEGER NOT NULL DEFAULT 0,
                    times_shortlisted INTEGER NOT NULL DEFAULT 0,
                    times_notified INTEGER NOT NULL DEFAULT 0,
                    last_shortlisted_at TIMESTAMPTZ,
                    last_notified_at TIMESTAMPTZ,
                    last_title TEXT,
                    last_company TEXT,
                    last_source TEXT,
                    last_source_url TEXT
                )
                """
            )
        )


def _history_row_to_dict(row: Any) -> dict[str, Any]:
    mapping = getattr(row, "_mapping", row)
    return {
        "canonical_job_key": str(mapping["canonical_job_key"]),
        "first_seen_at": _json_safe_timestamp(mapping["first_seen_at"]),
        "last_seen_at": _json_safe_timestamp(mapping["last_seen_at"]),
        "times_seen": int(mapping["times_seen"] or 0),
        "times_shortlisted": int(mapping["times_shortlisted"] or 0),
        "times_notified": int(mapping["times_notified"] or 0),
        "last_shortlisted_at": _json_safe_timestamp(mapping["last_shortlisted_at"]),
        "last_notified_at": _json_safe_timestamp(mapping["last_notified_at"]),
        "last_title": mapping["last_title"],
        "last_company": mapping["last_company"],
        "last_source": mapping["last_source"],
        "last_source_url": mapping["last_source_url"],
    }


def load_jobs_history(db: Any, canonical_job_keys: list[str]) -> dict[str, dict[str, Any]]:
    if not _db_usable(db):
        return {}
    keys = [str(value).strip() for value in canonical_job_keys if str(value).strip()]
    if not keys:
        return {}

    ensure_jobs_history_table(db.connection())
    placeholders = ", ".join(f":key_{idx}" for idx in range(len(keys)))
    params = {f"key_{idx}": value for idx, value in enumerate(keys)}
    rows = db.execute(
        text(
            f"""
            SELECT canonical_job_key, first_seen_at, last_seen_at, times_seen,
                   times_shortlisted, times_notified, last_shortlisted_at, last_notified_at,
                   last_title, last_company, last_source, last_source_url
            FROM jobs_v2_history
            WHERE canonical_job_key IN ({placeholders})
            """
        ),
        params,
    ).fetchall()
    return {
        str(row._mapping["canonical_job_key"]): _history_row_to_dict(row)
        for row in rows
    }


def _aggregate_jobs(jobs: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    aggregated: dict[str, dict[str, Any]] = {}
    for row in jobs:
        if not isinstance(row, dict):
            continue
        key = str(row.get("canonical_job_key") or "").strip()
        if not key:
            continue
        current = aggregated.setdefault(
            key,
            {
                "count": 0,
                "canonical_job_key": key,
                "title": None,
                "company": None,
                "source": None,
                "source_url": None,
            },
        )
        current["count"] += 1
        for field, source_field in (
            ("title", "title"),
            ("company", "company"),
            ("source", "source"),
            ("source_url", "source_url"),
        ):
            value = row.get(source_field)
            if current.get(field) or not isinstance(value, str) or not value.strip():
                continue
            current[field] = value.strip()
    return aggregated


def _upsert_jobs_history(
    db: Any,
    jobs: list[dict[str, Any]],
    *,
    seen_at: datetime | None = None,
    shortlisted_at: datetime | None = None,
    notified_at: datetime | None = None,
) -> None:
    if not _db_usable(db):
        return

    ensure_jobs_history_table(db.connection())
    aggregated = _aggregate_jobs(jobs)
    if not aggregated:
        return

    existing = load_jobs_history(db, list(aggregated.keys()))
    seen_at_text = _to_iso(seen_at)
    shortlisted_at_text = _to_iso(shortlisted_at)
    notified_at_text = _to_iso(notified_at)

    for key, row in aggregated.items():
        previous = existing.get(key)
        count = int(row.get("count") or 0)
        title = row.get("title")
        company = row.get("company")
        source = row.get("source")
        source_url = row.get("source_url")
        if previous is None:
            db.execute(
                text(
                    """
                    INSERT INTO jobs_v2_history (
                        canonical_job_key,
                        first_seen_at,
                        last_seen_at,
                        times_seen,
                        times_shortlisted,
                        times_notified,
                        last_shortlisted_at,
                        last_notified_at,
                        last_title,
                        last_company,
                        last_source,
                        last_source_url
                    ) VALUES (
                        :canonical_job_key,
                        :first_seen_at,
                        :last_seen_at,
                        :times_seen,
                        :times_shortlisted,
                        :times_notified,
                        :last_shortlisted_at,
                        :last_notified_at,
                        :last_title,
                        :last_company,
                        :last_source,
                        :last_source_url
                    )
                    """
                ),
                {
                    "canonical_job_key": key,
                    "first_seen_at": seen_at_text or shortlisted_at_text or notified_at_text or _to_iso(datetime.now(timezone.utc)),
                    "last_seen_at": seen_at_text or shortlisted_at_text or notified_at_text or _to_iso(datetime.now(timezone.utc)),
                    "times_seen": count if seen_at_text else 0,
                    "times_shortlisted": count if shortlisted_at_text else 0,
                    "times_notified": count if notified_at_text else 0,
                    "last_shortlisted_at": shortlisted_at_text,
                    "last_notified_at": notified_at_text,
                    "last_title": title,
                    "last_company": company,
                    "last_source": source,
                    "last_source_url": source_url,
                },
            )
            continue

        db.execute(
            text(
                """
                UPDATE jobs_v2_history
                SET last_seen_at = :last_seen_at,
                    times_seen = :times_seen,
                    times_shortlisted = :times_shortlisted,
                    times_notified = :times_notified,
                    last_shortlisted_at = :last_shortlisted_at,
                    last_notified_at = :last_notified_at,
                    last_title = :last_title,
                    last_company = :last_company,
                    last_source = :last_source,
                    last_source_url = :last_source_url
                WHERE canonical_job_key = :canonical_job_key
                """
            ),
            {
                "canonical_job_key": key,
                "last_seen_at": seen_at_text or previous.get("last_seen_at") or _to_iso(datetime.now(timezone.utc)),
                "times_seen": int(previous.get("times_seen") or 0) + (count if seen_at_text else 0),
                "times_shortlisted": int(previous.get("times_shortlisted") or 0) + (count if shortlisted_at_text else 0),
                "times_notified": int(previous.get("times_notified") or 0) + (count if notified_at_text else 0),
                "last_shortlisted_at": shortlisted_at_text or previous.get("last_shortlisted_at"),
                "last_notified_at": notified_at_text or previous.get("last_notified_at"),
                "last_title": title or previous.get("last_title"),
                "last_company": company or previous.get("last_company"),
                "last_source": source or previous.get("last_source"),
                "last_source_url": source_url or previous.get("last_source_url"),
            },
        )


def record_jobs_seen(db: Any, jobs: list[dict[str, Any]], *, seen_at: datetime | None = None) -> None:
    _upsert_jobs_history(db, jobs, seen_at=seen_at or datetime.now(timezone.utc))


def record_jobs_shortlisted(db: Any, jobs: list[dict[str, Any]], *, shortlisted_at: datetime | None = None) -> None:
    _upsert_jobs_history(db, jobs, shortlisted_at=shortlisted_at or datetime.now(timezone.utc))


def record_jobs_notified(db: Any, jobs: list[dict[str, Any]], *, notified_at: datetime | None = None) -> None:
    _upsert_jobs_history(db, jobs, notified_at=notified_at or datetime.now(timezone.utc))
