from __future__ import annotations

import importlib
import logging
import time
from typing import Any

from task_handlers.jobs_pipeline_common import (
    DEFAULT_JOB_BOARDS,
    build_upstream_ref,
    new_pipeline_id,
    payload_object,
    resolve_request,
    source_counts,
    stage_idempotency_key,
    utc_iso,
)

logger = logging.getLogger(__name__)

ACTIVE_COLLECTOR_SOURCES = DEFAULT_JOB_BOARDS
ACTIVE_OBSERVABILITY_SOURCES = tuple(ACTIVE_COLLECTOR_SOURCES)
SUCCESSFUL_SOURCE_STATUSES = {"success", "under_target"}
HEALTHY_SOURCE_STATUSES = {"success", "empty_success"}
EMPTY_SOURCE_STATUSES = {"empty_success"}
DEGRADED_SOURCE_STATUSES = {"under_target"}
FAILED_SOURCE_STATUSES = {"auth_blocked", "consent_blocked", "anti_bot_blocked", "layout_mismatch", "upstream_failure"}
KNOWN_SOURCE_STATUSES = (
    SUCCESSFUL_SOURCE_STATUSES
    | HEALTHY_SOURCE_STATUSES
    | DEGRADED_SOURCE_STATUSES
    | FAILED_SOURCE_STATUSES
    | {"skipped"}
)
DISPLAY_SOURCE_NAMES = {
    "linkedin": "LinkedIn",
    "indeed": "Indeed",
    "manual": "Manual",
}
BLOCKING_REASON_LABELS = {
    "login_wall": "login wall",
    "login_wall_detected": "login wall",
    "auth_required": "auth required",
    "anti_bot_detected": "anti-bot defenses",
    "consent_wall_detected": "consent wall",
    "unexpected_redirect": "unexpected redirect",
    "layout_mismatch": "layout mismatch",
    "selector_mismatch": "layout mismatch",
    "fetch_blocked_403": "blocked fetch",
    "auth_blocked": "auth blocked",
    "consent_blocked": "consent blocked",
    "anti_bot_blocked": "anti-bot defenses",
}
MANUAL_SUPPORTED_FIELDS = {
    "source": "manual",
    "titles": True,
    "keywords": True,
    "excluded_keywords": True,
    "locations": True,
    "work_mode_preference": True,
    "minimum_salary": True,
    "experience_level": True,
    "result_limit_per_source": True,
    "max_pages_per_source": True,
    "max_jobs_per_source": True,
    "minimum_raw_jobs_total": True,
    "minimum_unique_jobs_total": True,
    "minimum_jobs_per_source": True,
    "stop_when_minimum_reached": True,
    "collection_time_cap_seconds": True,
    "max_queries_per_title_location_pair": True,
    "max_queries_per_run": True,
    "enable_query_expansion": True,
    "max_total_jobs": True,
    "jobs_notification_cooldown_days": True,
    "jobs_shortlist_repeat_penalty": True,
    "resurface_seen_jobs": True,
    "early_stop_when_no_new_results": True,
    "enabled_sources": True,
    "input_mode": {
        "titles": "manual_input",
        "keywords": "manual_input",
        "excluded_keywords": "manual_input",
        "locations": "manual_input",
        "work_mode_preference": "manual_input",
        "minimum_salary": "manual_input",
        "experience_level": "manual_input",
        "result_limit_per_source": "manual_input",
        "max_pages_per_source": "manual_input",
        "max_jobs_per_source": "manual_input",
        "minimum_raw_jobs_total": "manual_input",
        "minimum_unique_jobs_total": "manual_input",
        "minimum_jobs_per_source": "manual_input",
        "stop_when_minimum_reached": "manual_input",
        "collection_time_cap_seconds": "manual_input",
        "max_queries_per_title_location_pair": "manual_input",
        "max_queries_per_run": "manual_input",
        "enable_query_expansion": "manual_input",
        "max_total_jobs": "manual_input",
        "jobs_notification_cooldown_days": "manual_input",
        "jobs_shortlist_repeat_penalty": "manual_input",
        "resurface_seen_jobs": "manual_input",
        "early_stop_when_no_new_results": "manual_input",
        "enabled_sources": "pipeline_routing",
    },
    "source_metadata_fields": ["raw"],
}


def _load_collector_module(source: str):
    return importlib.import_module(f"integrations.jobs_collectors.{source}")


def _prefix_source_message(source: str, value: str) -> str:
    text = str(value).strip()
    if not text:
        return ""
    low = text.lower()
    if low.startswith(f"{source}:"):
        return text
    return f"{source}: {text}"


def _normalize_manual_jobs(request: dict[str, Any]) -> list[dict[str, Any]]:
    manual_jobs = request.get("manual_jobs") if isinstance(request.get("manual_jobs"), list) else []
    normalized: list[dict[str, Any]] = []
    for row in manual_jobs:
        if not isinstance(row, dict):
            continue
        source_url = row.get("source_url")
        if not isinstance(source_url, str) or not source_url.strip():
            source_url = row.get("url") if isinstance(row.get("url"), str) and row.get("url", "").strip() else None
        normalized.append(
            {
                "source": "manual",
                "source_url": source_url,
                "title": row.get("title"),
                "company": row.get("company"),
                "location": row.get("location"),
                "url": row.get("url"),
                "salary_min": row.get("salary_min"),
                "salary_max": row.get("salary_max"),
                "salary_currency": row.get("salary_currency"),
                "experience_level": row.get("experience_level"),
                "work_mode": row.get("work_mode"),
                "posted_at": row.get("posted_at"),
                "scraped_at": row.get("scraped_at"),
                "description_snippet": row.get("description_snippet"),
                "source_metadata": row.get("raw") if isinstance(row.get("raw"), dict) else {},
            }
        )
    return normalized


def _display_source_name(source: str) -> str:
    key = str(source).strip().lower()
    return DISPLAY_SOURCE_NAMES.get(key, key.title() or source)


def _source_error_type(meta: dict[str, Any]) -> str | None:
    value = str(meta.get("source_error_type") or meta.get("error_type") or "").strip()
    return value or None


def _active_sources_label(sources: list[str]) -> str:
    labels = [_display_source_name(source) for source in sources if source]
    if not labels:
        return "No active sources"
    if len(labels) == 1:
        return f"{labels[0]} active"
    if len(labels) == 2:
        return f"{labels[0]} + {labels[1]} active"
    return ", ".join(labels[:-1]) + f", and {labels[-1]} active"


def _suspected_blocking_signal(*, status: str, meta: dict[str, Any]) -> tuple[bool, str | None]:
    source_error_type = _source_error_type(meta)
    low_status = str(status or "").strip().lower()

    if bool(meta.get("login_wall_detected")) or bool(meta.get("auth_required_detected")):
        return True, "login wall"
    if bool(meta.get("anti_bot_detected")):
        return True, "anti-bot defenses"
    if bool(meta.get("consent_wall_detected")):
        return True, "consent wall"
    if bool(meta.get("unexpected_redirect_detected")):
        return True, "unexpected redirect"
    if bool(meta.get("layout_mismatch_detected")):
        return True, "layout mismatch"
    if source_error_type and source_error_type in BLOCKING_REASON_LABELS:
        return True, BLOCKING_REASON_LABELS[source_error_type]
    if low_status in BLOCKING_REASON_LABELS:
        return True, BLOCKING_REASON_LABELS[low_status]
    return False, None


def _source_focus_snapshot(source_key: str, result: dict[str, Any]) -> dict[str, Any]:
    meta = result.get("meta") if isinstance(result.get("meta"), dict) else {}
    jobs_count = int(result.get("jobs_count", 0) or 0)
    raw_jobs_found = _meta_count(meta, "discovered_raw_count", jobs_count)
    jobs_kept = _meta_count(meta, "jobs_kept", jobs_count)
    pages_attempted = _meta_count(meta, "pages_attempted", _meta_count(meta, "pages_fetched", 0))
    requested_limit = _meta_count(meta, "requested_limit", 0)
    status = str(result.get("status") or "").strip().lower()
    under_target = status in DEGRADED_SOURCE_STATUSES
    suspected_blocking, blocking_reason = _suspected_blocking_signal(status=status, meta=meta)

    return {
        "source": source_key,
        "source_label": _display_source_name(source_key),
        "status": status or "unknown",
        "raw_jobs_found": raw_jobs_found,
        "jobs_kept": jobs_kept,
        "pages_attempted": pages_attempted,
        "requested_limit": requested_limit,
        "under_target": under_target,
        "under_target_reason": "below target for current run" if under_target else None,
        "suspected_blocking": suspected_blocking,
        "suspected_blocking_reason": blocking_reason,
    }


def _active_source_focus_snapshots(source_results: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    snapshots: list[dict[str, Any]] = []
    for source_key in ACTIVE_OBSERVABILITY_SOURCES:
        result = source_results.get(source_key)
        if not isinstance(result, dict):
            continue
        snapshots.append(_source_focus_snapshot(source_key, result))
    return snapshots


def _promote_source_status(status: str, source_error_type: str | None, *, collected_count: int) -> str:
    if collected_count > 0:
        return status
    if source_error_type in {"login_wall", "login_wall_detected", "auth_required"}:
        return "auth_blocked"
    if source_error_type == "anti_bot_detected":
        return "anti_bot_blocked"
    if source_error_type == "consent_wall_detected":
        return "consent_blocked"
    if source_error_type in {"layout_mismatch", "selector_mismatch"}:
        return "layout_mismatch"
    return status


def _normalize_source_status(
    *,
    status_raw: str,
    collected_count: int,
    source_errors: list[str],
    source_meta: dict[str, Any],
) -> str:
    normalized = status_raw.strip().lower()
    error_type = _source_error_type(source_meta)

    if normalized == "partial_success":
        normalized = "under_target" if collected_count > 0 else "upstream_failure"
    elif normalized == "failed":
        normalized = "upstream_failure"
    elif normalized == "empty":
        normalized = "empty_success"
    elif normalized == "success" and collected_count <= 0:
        normalized = "upstream_failure" if source_errors else "empty_success"
    elif normalized not in KNOWN_SOURCE_STATUSES:
        if collected_count > 0:
            normalized = "under_target" if source_errors else "success"
        elif source_errors:
            normalized = "upstream_failure"
        else:
            normalized = "empty_success"

    normalized = _promote_source_status(normalized, error_type, collected_count=collected_count)
    return normalized


def _meta_count(meta: dict[str, Any], key: str, fallback: int = 0) -> int:
    try:
        return int(meta.get(key, fallback))
    except (TypeError, ValueError):
        return fallback


def _job_key(job: dict[str, Any]) -> tuple[str, str, str]:
    return (
        str(job.get("source") or "").strip().lower(),
        str(job.get("url") or "").strip(),
        str(job.get("title") or "").strip().lower(),
    )


def _unique_job_count(jobs: list[dict[str, Any]]) -> int:
    seen: set[tuple[str, str, str]] = set()
    for row in jobs:
        if not isinstance(row, dict):
            continue
        seen.add(_job_key(row))
    return len(seen)


def _rate(count: int, total: int) -> float:
    if total <= 0:
        return 0.0
    return round((float(count) / float(total)) * 100.0, 1)


def _empty_metadata_summary() -> dict[str, int]:
    return {
        "job_count": 0,
        "missing_company": 0,
        "missing_posted_at": 0,
        "missing_source_url": 0,
        "missing_location": 0,
    }


def _compact_missing_summary(missing_rates: dict[str, float]) -> str:
    labels = {
        "missing_company_rate": "company",
        "missing_posted_at_rate": "post date",
        "missing_source_url_rate": "link",
        "missing_location_rate": "location",
    }
    gaps = [
        (label, float(missing_rates.get(key) or 0.0))
        for key, label in labels.items()
        if float(missing_rates.get(key) or 0.0) > 0.0
    ]
    gaps.sort(key=lambda item: item[1], reverse=True)
    if not gaps:
        return "metadata mostly complete"
    return ", ".join(f"{label} {rate:.0f}%" for label, rate in gaps[:2])


def _build_run_preview_messages(
    *,
    source_results: dict[str, dict[str, Any]],
) -> list[str]:
    messages: list[str] = []

    source_focus = _active_source_focus_snapshots(source_results)
    if source_focus:
        messages.append(_active_sources_label([str(row.get("source") or "") for row in source_focus]))

    for row in source_focus:
        label = str(row.get("source_label") or row.get("source") or "").strip()
        raw_jobs_found = int(row.get("raw_jobs_found", 0) or 0)
        jobs_kept = int(row.get("jobs_kept", 0) or 0)
        pages_attempted = int(row.get("pages_attempted", 0) or 0)
        messages.append(f"{label} contributed {raw_jobs_found} raw jobs")
        messages.append(f"{label} kept {jobs_kept} jobs after filtering")
        messages.append(f"{label} attempted {pages_attempted} pages")
        if bool(row.get("under_target")):
            messages.append(f"{label} is under target for this run")
        if bool(row.get("suspected_blocking")) and row.get("suspected_blocking_reason"):
            messages.append(f"{label} suspected blocking: {row['suspected_blocking_reason']}")

    return messages


def _build_collection_observability(
    *,
    source_results: dict[str, dict[str, Any]],
    source_metadata_quality: dict[str, dict[str, int]],
    discovered_raw_count: int,
    kept_after_basic_filter_count: int,
    dropped_by_basic_filter_count: int,
    deduped_count: int,
    raw_job_count: int,
    successful_sources: list[str],
    healthy_sources: list[str],
    max_total_jobs: int,
    truncated_by_run_limit_count: int,
    run_preview_messages: list[str],
    minimum_targets: dict[str, Any],
) -> dict[str, Any]:
    by_source: dict[str, dict[str, Any]] = {}
    source_focus = _active_source_focus_snapshots(source_results)
    breadth_candidates: list[tuple[str, int]] = []
    weak_candidates: list[tuple[str, float]] = []
    query_examples: list[str] = []
    query_runs: list[dict[str, Any]] = []
    total_queries_executed = 0
    total_empty_queries = 0

    for source_key, result in source_results.items():
        meta = result.get("meta") if isinstance(result.get("meta"), dict) else {}
        jobs_count = int(result.get("jobs_count", 0) or 0)
        discovered = _meta_count(meta, "discovered_raw_count", jobs_count)
        kept = _meta_count(meta, "kept_after_basic_filter_count", jobs_count)
        dropped = _meta_count(meta, "dropped_by_basic_filter_count", max(discovered - kept, 0))
        deduped = _meta_count(meta, "deduped_count", max(kept - jobs_count, 0))
        pages_fetched = _meta_count(meta, "pages_fetched", 0)
        queries_attempted = meta.get("queries_attempted") if isinstance(meta.get("queries_attempted"), list) else []
        queries_executed = _meta_count(meta, "queries_executed_count", len(queries_attempted))
        empty_queries = _meta_count(meta, "empty_queries_count", 0)
        metadata = source_metadata_quality.get(source_key) or _empty_metadata_summary()
        job_count = max(_meta_count(metadata, "job_count", jobs_count), jobs_count)
        missing_rates = {
            "missing_company_rate": _rate(_meta_count(metadata, "missing_company", 0), job_count),
            "missing_posted_at_rate": _rate(_meta_count(metadata, "missing_posted_at", 0), job_count),
            "missing_source_url_rate": _rate(_meta_count(metadata, "missing_source_url", 0), job_count),
            "missing_location_rate": _rate(_meta_count(metadata, "missing_location", 0), job_count),
        }
        highest_gap = max(missing_rates.values()) if missing_rates else 0.0
        if result.get("status") in SUCCESSFUL_SOURCE_STATUSES:
            breadth_candidates.append((source_key, discovered))
            weak_candidates.append((source_key, highest_gap))

        total_queries_executed += queries_executed
        total_empty_queries += empty_queries
        source_examples = meta.get("query_examples") if isinstance(meta.get("query_examples"), list) else []
        for value in source_examples:
            if not isinstance(value, str):
                continue
            trimmed = value.strip()
            if trimmed and trimmed not in query_examples:
                query_examples.append(trimmed)
        search_attempts = meta.get("search_attempts") if isinstance(meta.get("search_attempts"), list) else []
        for row in search_attempts:
            if not isinstance(row, dict):
                continue
            query_runs.append(
                {
                    "source": source_key,
                    "query": str(row.get("query") or "").strip(),
                    "location": str(row.get("location") or "").strip(),
                    "expansion_type": str(row.get("expansion_type") or "").strip() or None,
                    "jobs_found": _meta_count(row, "jobs_found", 0),
                    "new_unique_jobs": _meta_count(row, "new_unique_jobs", 0),
                    "returned_count": _meta_count(row, "returned_count", 0),
                    "pages_attempted": _meta_count(row, "pages_attempted", _meta_count(row, "pages_fetched", 0)),
                    "request_urls_tried": row.get("request_urls_tried") if isinstance(row.get("request_urls_tried"), list) else [],
                    "last_request_url": str(row.get("last_request_url") or "").strip() or None,
                    "error_type": str(row.get("error_type") or "").strip() or None,
                    "source_status": str(row.get("source_status") or "").strip() or None,
                    "source_error_type": str(row.get("source_error_type") or row.get("error_type") or "").strip() or None,
                    "error_status": row.get("error_status") if isinstance(row.get("error_status"), int) else None,
                    "cards_seen": _meta_count(row, "cards_seen", 0),
                    "listing_cards_seen": _meta_count(row, "listing_cards_seen", _meta_count(row, "cards_seen", 0)),
                    "jobs_raw": _meta_count(row, "jobs_raw", _meta_count(row, "discovered_raw_count", 0)),
                    "jobs_kept": _meta_count(row, "jobs_kept", _meta_count(row, "returned_count", 0)),
                    "wall_detected": bool(row.get("wall_detected", False)),
                    "auth_required_detected": bool(row.get("auth_required_detected", False)),
                    "login_wall_detected": bool(row.get("login_wall_detected", False))
                    or str(row.get("source_error_type") or "").strip() in {"login_wall", "login_wall_detected"},
                    "anti_bot_detected": bool(row.get("anti_bot_detected", False))
                    or str(row.get("source_error_type") or "").strip() == "anti_bot_detected",
                    "consent_wall_detected": bool(row.get("consent_wall_detected", False))
                    or str(row.get("source_error_type") or "").strip() == "consent_wall_detected",
                    "unexpected_redirect_detected": bool(row.get("unexpected_redirect_detected", False)),
                    "layout_mismatch_detected": bool(row.get("layout_mismatch_detected", False))
                    or str(row.get("source_status") or "").strip() == "layout_mismatch",
                    "parsing_strategy_used": str(row.get("parsing_strategy_used") or "").strip() or None,
                    "browser_fallback_used": bool(row.get("browser_fallback_used", False)),
                    "stop_reason": str(row.get("stop_reason") or "").strip() or None,
                }
            )

        by_source[source_key] = {
            "status": result.get("status"),
            "source_label": _display_source_name(source_key),
            "source_status": str(meta.get("source_status") or result.get("status") or "").strip() or None,
            "source_error_type": str(meta.get("source_error_type") or meta.get("error_type") or "").strip() or None,
            "raw_jobs_discovered": discovered,
            "kept_after_basic_filter": kept,
            "jobs_dropped": dropped,
            "deduped_in_collection": deduped,
            "final_raw_jobs": jobs_count,
            "pages_fetched": pages_fetched,
            "pages_attempted": _meta_count(meta, "pages_attempted", pages_fetched),
            "cards_seen": _meta_count(meta, "cards_seen", discovered),
            "listing_cards_seen": _meta_count(meta, "listing_cards_seen", _meta_count(meta, "cards_seen", discovered)),
            "jobs_raw": _meta_count(meta, "jobs_raw", discovered),
            "jobs_kept": _meta_count(meta, "jobs_kept", jobs_count),
            "jobs_found_per_source": discovered,
            "requested_limit": _meta_count(meta, "requested_limit", 0),
            "under_target": str(result.get("status") or "").strip().lower() in DEGRADED_SOURCE_STATUSES,
            "under_target_reason": (
                "below target for current run"
                if str(result.get("status") or "").strip().lower() in DEGRADED_SOURCE_STATUSES
                else None
            ),
            "healthy": result.get("status") in HEALTHY_SOURCE_STATUSES,
            "contributed_usable_jobs": result.get("status") in SUCCESSFUL_SOURCE_STATUSES,
            "queries_executed_count": queries_executed,
            "queries_attempted_count": len([row for row in queries_attempted if isinstance(row, str) and row.strip()]),
            "empty_queries_count": empty_queries,
            "query_examples": source_examples[:3],
            "request_urls_tried": meta.get("request_urls_tried") if isinstance(meta.get("request_urls_tried"), list) else [],
            "last_request_url": str(meta.get("last_request_url") or "").strip() or None,
            "error_type": str(meta.get("error_type") or "").strip() or None,
            "error_status": meta.get("error_status") if isinstance(meta.get("error_status"), int) else None,
            "wall_detected": bool(meta.get("wall_detected", False)),
            "auth_required_detected": bool(meta.get("auth_required_detected", False)),
            "login_wall_detected": bool(meta.get("login_wall_detected", False))
            or str(meta.get("source_error_type") or meta.get("error_type") or "").strip() in {"login_wall", "login_wall_detected"},
            "anti_bot_detected": bool(meta.get("anti_bot_detected", False))
            or str(meta.get("source_error_type") or meta.get("error_type") or "").strip() == "anti_bot_detected",
            "consent_wall_detected": bool(meta.get("consent_wall_detected", False))
            or str(meta.get("source_error_type") or meta.get("error_type") or "").strip() == "consent_wall_detected",
            "unexpected_redirect_detected": bool(meta.get("unexpected_redirect_detected", False)),
            "layout_mismatch_detected": bool(meta.get("layout_mismatch_detected", False))
            or str(meta.get("source_status") or result.get("status") or "").strip() == "layout_mismatch",
            "parsing_strategy_used": str(meta.get("parsing_strategy_used") or "").strip() or None,
            "browser_fallback_used": bool(meta.get("browser_fallback_used", False)),
            "suspected_blocking": _suspected_blocking_signal(
                status=str(result.get("status") or "").strip().lower(),
                meta=meta,
            )[0],
            "suspected_blocking_reason": _suspected_blocking_signal(
                status=str(result.get("status") or "").strip().lower(),
                meta=meta,
            )[1],
            "missing_counts": {
                "company": _meta_count(metadata, "missing_company", 0),
                "posted_at": _meta_count(metadata, "missing_posted_at", 0),
                "source_url": _meta_count(metadata, "missing_source_url", 0),
                "location": _meta_count(metadata, "missing_location", 0),
            },
            "missing_rates": missing_rates,
            "weakness_summary": _compact_missing_summary(missing_rates),
        }

    strongest_source = max(breadth_candidates, key=lambda item: item[1])[0] if breadth_candidates else None
    weakest_source = min(breadth_candidates, key=lambda item: item[1])[0] if breadth_candidates else None
    weakest_metadata_entry = max(weak_candidates, key=lambda item: item[1]) if weak_candidates else None
    weakest_metadata_source = (
        weakest_metadata_entry[0]
        if weakest_metadata_entry and float(weakest_metadata_entry[1]) > 0.0
        else None
    )

    active_sources_label = _active_sources_label([str(row.get("source") or "") for row in source_focus])
    contribution_summary = "; ".join(
        f"{row['source_label']} contributed {int(row.get('raw_jobs_found', 0) or 0)} raw jobs"
        for row in source_focus
    )
    searched_enough = (
        f"{active_sources_label}. {contribution_summary}. {total_queries_executed} queries executed."
        if contribution_summary
        else f"{active_sources_label}. {total_queries_executed} queries executed."
    )
    collapse_reason = (
        f"{dropped_by_basic_filter_count} dropped in basic filtering and {deduped_count} deduped before returning {raw_job_count} raw jobs."
    )
    if truncated_by_run_limit_count > 0:
        collapse_reason += f" Run cap truncated another {truncated_by_run_limit_count} jobs at the artifact level."
    if weakest_metadata_source:
        metadata_gap = f"Weakest metadata source: {_display_source_name(weakest_metadata_source)}."
    else:
        metadata_gap = "LinkedIn + Indeed metadata look stable."

    weak_source_signals = [
        row
        for row in source_focus
        if bool(row.get("under_target")) or bool(row.get("suspected_blocking"))
    ]
    if weak_source_signals:
        weak_source_summary = "; ".join(
            (
                f"{row['source_label']} under target"
                + (f" after {int(row.get('pages_attempted', 0) or 0)} pages attempted" if int(row.get("pages_attempted", 0) or 0) > 0 else "")
                + (
                    f" with suspected blocking ({row['suspected_blocking_reason']})"
                    if row.get("suspected_blocking") and row.get("suspected_blocking_reason")
                    else ""
                )
            )
            for row in weak_source_signals
        )
    elif weakest_source:
        weak_source_summary = f"Lowest raw contribution came from {_display_source_name(weakest_source)}."
    else:
        weak_source_summary = "LinkedIn + Indeed are both contributing normally."

    minimum_shortfall_summary = str(minimum_targets.get("shortfall_summary") or "").strip()
    searched_enough_suffix = ""
    if minimum_shortfall_summary:
        searched_enough_suffix = f" Minimum target shortfall: {minimum_shortfall_summary}."
    elif bool(minimum_targets.get("minimum_target_requested")) and bool(minimum_targets.get("minimum_reached")):
        searched_enough_suffix = " Minimum jobs target reached."

    return {
        "waterfall": {
            "raw_jobs_discovered": discovered_raw_count,
            "kept_after_basic_filter": kept_after_basic_filter_count,
            "jobs_dropped": dropped_by_basic_filter_count,
            "deduped_in_collection": deduped_count,
            "final_raw_jobs": raw_job_count,
            "queries_executed": total_queries_executed,
        },
        "query_summary": {
            "queries_executed": total_queries_executed,
            "empty_queries_count": total_empty_queries,
            "max_total_jobs": max_total_jobs,
            "minimum_raw_jobs_total_requested": minimum_targets.get("minimum_raw_jobs_total_requested", 0),
            "minimum_unique_jobs_total_requested": minimum_targets.get("minimum_unique_jobs_total_requested", 0),
            "minimum_reached": bool(minimum_targets.get("minimum_reached")),
            "reason_stopped": minimum_targets.get("reason_stopped"),
            "query_examples": query_examples[:10],
            "query_runs": query_runs,
        },
        "by_source": by_source,
        "active_sources_label": active_sources_label,
        "source_focus": source_focus,
        "minimum_targets": minimum_targets,
        "run_preview": {
            "messages": run_preview_messages,
        },
        "operator_questions": {
            "searched_enough": searched_enough + searched_enough_suffix,
            "did_we_search_enough": searched_enough + searched_enough_suffix,
            "which_source_is_weak": weak_source_summary,
            "why_raw_count_collapsed": collapse_reason,
            "why_did_raw_count_collapse": collapse_reason,
            "are_we_missing_metadata": metadata_gap,
        },
    }


def execute(task: Any, db: Any) -> dict[str, Any]:
    del db

    payload = payload_object(task.payload_json)
    request = resolve_request(payload.get("request") if isinstance(payload.get("request"), dict) else payload)
    if not isinstance(request.get("enabled_sources"), list) or not request.get("enabled_sources"):
        request["enabled_sources"] = list(DEFAULT_JOB_BOARDS)
    if not isinstance(request.get("sources"), list) or not request.get("sources"):
        request["sources"] = list(request["enabled_sources"])
    pipeline_id = new_pipeline_id(payload.get("pipeline_id"))

    raw_jobs: list[dict[str, Any]] = []
    warnings: list[str] = []
    collector_errors: list[str] = []
    source_results: dict[str, dict[str, Any]] = {}
    supported_fields_by_source: dict[str, dict[str, Any]] = {}

    sources = list(request.get("sources") or [])
    disabled_sources = [
        str(source).strip().lower()
        for source in (request.get("disabled_sources") if isinstance(request.get("disabled_sources"), list) else [])
        if isinstance(source, str) and str(source).strip()
    ]
    source_configuration_notes = [
        str(note).strip()
        for note in (request.get("source_configuration_notes") if isinstance(request.get("source_configuration_notes"), list) else [])
        if isinstance(note, str) and str(note).strip()
    ]
    max_total_jobs = max(1, int(request.get("max_total_jobs") or len(sources) or 1))
    board_url_overrides = request.get("board_url_overrides") if isinstance(request.get("board_url_overrides"), dict) else {}
    collectors_enabled = bool(request.get("collectors_enabled", True))
    truncated_by_run_limit_count = 0
    minimum_raw_jobs_total = max(0, int(request.get("minimum_raw_jobs_total") or 0))
    minimum_unique_jobs_total = max(0, int(request.get("minimum_unique_jobs_total") or 0))
    minimum_jobs_per_source = max(0, int(request.get("minimum_jobs_per_source") or 0))
    stop_when_minimum_reached = bool(request.get("stop_when_minimum_reached", True))
    raw_collection_time_cap = request.get("collection_time_cap_seconds")
    try:
        collection_time_cap_seconds = max(1, int(raw_collection_time_cap)) if raw_collection_time_cap not in (None, "") else None
    except (TypeError, ValueError):
        collection_time_cap_seconds = None
    collection_started_monotonic = time.monotonic()
    collection_deadline_monotonic = (
        collection_started_monotonic + collection_time_cap_seconds
        if collection_time_cap_seconds is not None
        else None
    )
    minimum_target_requested = any(
        value > 0 for value in (minimum_raw_jobs_total, minimum_unique_jobs_total, minimum_jobs_per_source)
    )
    stopped_by_minimum = False
    stopped_by_time_cap = False
    stopped_by_safety_cap = False

    def _current_discovered_raw_total() -> int:
        total = 0
        for result in source_results.values():
            meta = result.get("meta") if isinstance(result.get("meta"), dict) else {}
            jobs_count = int(result.get("jobs_count", 0) or 0)
            total += _meta_count(meta, "discovered_raw_count", jobs_count)
        return total

    def _minimum_state() -> dict[str, Any]:
        current_raw_jobs_total = _current_discovered_raw_total()
        current_unique_jobs_total = _unique_job_count(raw_jobs)
        per_source_shortfalls: dict[str, int] = {}
        if minimum_jobs_per_source > 0:
            for configured_source in [str(item).strip().lower() for item in sources if str(item).strip()]:
                current_count = int(source_results.get(configured_source, {}).get("jobs_count", 0) or 0)
                shortfall = max(minimum_jobs_per_source - current_count, 0)
                if shortfall > 0:
                    per_source_shortfalls[configured_source] = shortfall
        raw_jobs_shortfall = max(minimum_raw_jobs_total - current_raw_jobs_total, 0)
        unique_jobs_shortfall = max(minimum_unique_jobs_total - current_unique_jobs_total, 0)
        minimum_reached = not minimum_target_requested or (
            raw_jobs_shortfall <= 0
            and unique_jobs_shortfall <= 0
            and not per_source_shortfalls
        )
        shortfall_parts: list[str] = []
        if raw_jobs_shortfall > 0:
            shortfall_parts.append(f"{raw_jobs_shortfall} raw jobs")
        if unique_jobs_shortfall > 0:
            shortfall_parts.append(f"{unique_jobs_shortfall} unique jobs")
        for source_key, shortfall in per_source_shortfalls.items():
            shortfall_parts.append(f"{_display_source_name(source_key)} needs {shortfall} more")
        return {
            "minimum_target_requested": minimum_target_requested,
            "minimum_raw_jobs_total_requested": minimum_raw_jobs_total,
            "minimum_unique_jobs_total_requested": minimum_unique_jobs_total,
            "minimum_jobs_per_source_requested": minimum_jobs_per_source,
            "stop_when_minimum_reached": stop_when_minimum_reached,
            "collection_time_cap_seconds": collection_time_cap_seconds,
            "current_raw_jobs_total": current_raw_jobs_total,
            "current_unique_jobs_total": current_unique_jobs_total,
            "per_source_shortfalls": per_source_shortfalls,
            "raw_jobs_shortfall": raw_jobs_shortfall,
            "unique_jobs_shortfall": unique_jobs_shortfall,
            "minimum_reached": minimum_reached,
            "shortfall_summary": ", ".join(shortfall_parts),
        }

    for note in source_configuration_notes:
        if note not in warnings:
            warnings.append(note)

    for source_key in disabled_sources:
        message = f"{source_key}: source_disabled currently unsupported and ignored"
        logger.info("jobs_collect source=%s skipped: %s", source_key, message)
        source_results[source_key] = {
            "source": source_key,
            "status": "skipped",
            "jobs_count": 0,
            "warnings": [message],
            "errors": [],
            "error": None,
            "meta": {
                "reason": "source_disabled",
                "source_status": "skipped",
                "source_error_type": "source_disabled",
                "currently_supported_sources": list(DEFAULT_JOB_BOARDS),
            },
        }

    for source_index, source in enumerate(sources):
        source_key = str(source).strip().lower()
        if not source_key:
            continue

        minimum_state_before_source = _minimum_state()
        if minimum_target_requested and stop_when_minimum_reached and minimum_state_before_source["minimum_reached"]:
            stopped_by_minimum = True
            source_results[source_key] = {
                "source": source_key,
                "status": "skipped",
                "jobs_count": 0,
                "warnings": [],
                "errors": [],
                "error": None,
                "meta": {
                    "reason": "minimum_reached",
                    "source_status": "skipped",
                    "collection_stop_reason": "minimum_reached",
                },
            }
            continue

        if collection_deadline_monotonic is not None and time.monotonic() >= collection_deadline_monotonic:
            stopped_by_time_cap = True
            source_results[source_key] = {
                "source": source_key,
                "status": "skipped",
                "jobs_count": 0,
                "warnings": [],
                "errors": [],
                "error": None,
                "meta": {
                    "reason": "time_cap_reached",
                    "source_status": "skipped",
                    "collection_stop_reason": "time_cap",
                },
            }
            continue

        remaining_total_jobs = max_total_jobs - len(raw_jobs)
        if remaining_total_jobs <= 0:
            stopped_by_safety_cap = True
            source_results[source_key] = {
                "source": source_key,
                "status": "skipped",
                "jobs_count": 0,
                "warnings": [],
                "errors": [],
                "error": None,
                "meta": {
                    "reason": "max_total_jobs_reached",
                    "remaining_total_jobs": 0,
                    "source_status": "skipped",
                    "collection_stop_reason": "safety_cap",
                },
            }
            continue

        if source_key == "manual":
            manual_jobs = _normalize_manual_jobs(request)
            if len(manual_jobs) > remaining_total_jobs:
                stopped_by_safety_cap = True
                truncated_by_run_limit_count += len(manual_jobs) - remaining_total_jobs
                manual_jobs = manual_jobs[:remaining_total_jobs]
            raw_jobs.extend(manual_jobs)
            logger.info("jobs_collect source=%s jobs=%s status=success", source_key, len(manual_jobs))
            if not manual_jobs:
                logger.warning("jobs_collect source=%s jobs=0 status=empty", source_key)
            source_results[source_key] = {
                "source": source_key,
                "status": "success",
                "jobs_count": len(manual_jobs),
                "warnings": [],
                "errors": [],
                "error": None,
                "meta": {
                    "requested_limit": len(manual_jobs),
                    "discovered_raw_count": len(manual_jobs),
                    "kept_after_basic_filter_count": len(manual_jobs),
                    "dropped_by_basic_filter_count": 0,
                    "deduped_count": 0,
                    "returned_count": len(manual_jobs),
                    "jobs_found_per_source": len(manual_jobs),
                    "queries_executed_count": 0,
                    "empty_queries_count": 0,
                    "query_examples": [],
                    "search_attempts": [],
                    "truncated_by_run_limit_count": truncated_by_run_limit_count,
                    "minimum_jobs_per_source_requested": minimum_jobs_per_source,
                    "stop_when_minimum_reached": stop_when_minimum_reached,
                    "collection_stop_reason": "safety_cap" if len(manual_jobs) >= remaining_total_jobs and remaining_total_jobs > 0 and stopped_by_safety_cap else "exhausted",
                },
            }
            supported_fields_by_source[source_key] = dict(MANUAL_SUPPORTED_FIELDS)
            continue

        if source_key not in ACTIVE_COLLECTOR_SOURCES:
            message = f"{source_key}: unsupported_source"
            collector_errors.append(message)
            logger.error("jobs_collect source=%s failed: %s", source_key, message)
            source_results[source_key] = {
                "source": source_key,
                "status": "upstream_failure",
                "jobs_count": 0,
                "warnings": [],
                "errors": [message],
                "error": message,
                "meta": {"source_status": "upstream_failure", "source_error_type": "unsupported_source"},
            }
            continue

        if not collectors_enabled:
            source_results[source_key] = {
                "source": source_key,
                "status": "skipped",
                "jobs_count": 0,
                "warnings": [],
                "errors": [],
                "error": None,
                "meta": {"reason": "collectors_disabled"},
            }
            continue

        try:
            logger.info("jobs_collect source=%s status=start", source_key)
            collector = _load_collector_module(source_key)
            source_request = dict(request)
            remaining_sources_count = max(
                1,
                len([row for row in sources[source_index:] if str(row).strip()]),
            )
            source_minimum_shortfall = int(minimum_state_before_source["per_source_shortfalls"].get(source_key, 0) or 0)
            unique_share_target = (
                (int(minimum_state_before_source["unique_jobs_shortfall"]) + remaining_sources_count - 1)
                // remaining_sources_count
                if int(minimum_state_before_source["unique_jobs_shortfall"]) > 0
                else 0
            )
            raw_share_target = (
                (int(minimum_state_before_source["raw_jobs_shortfall"]) + remaining_sources_count - 1)
                // remaining_sources_count
                if int(minimum_state_before_source["raw_jobs_shortfall"]) > 0
                else 0
            )
            configured_limit = int(source_request.get("result_limit_per_source") or remaining_total_jobs)
            per_source_limit = min(
                max(configured_limit, source_minimum_shortfall, unique_share_target, raw_share_target),
                remaining_total_jobs,
            )
            source_request["minimum_jobs_per_source"] = max(source_minimum_shortfall, unique_share_target)
            source_request["result_limit_per_source"] = per_source_limit
            source_request["max_jobs_per_source"] = per_source_limit
            source_request["max_jobs_per_board"] = per_source_limit
            if collection_deadline_monotonic is not None:
                source_request["_collection_deadline_monotonic"] = collection_deadline_monotonic
            collector_result = collector.collect_jobs(
                source_request,
                url_override=(str(board_url_overrides.get(source_key) or "").strip() or None),
            )

            collected = collector_result.get("jobs") if isinstance(collector_result.get("jobs"), list) else []
            source_warnings_raw = collector_result.get("warnings") if isinstance(collector_result.get("warnings"), list) else []
            source_errors_raw = collector_result.get("errors") if isinstance(collector_result.get("errors"), list) else []
            source_warnings = [_prefix_source_message(source_key, str(row)) for row in source_warnings_raw if str(row).strip()]
            source_errors = [_prefix_source_message(source_key, str(row)) for row in source_errors_raw if str(row).strip()]
            source_meta = dict(collector_result.get("meta") if isinstance(collector_result.get("meta"), dict) else {})
            source_truncated_by_run_limit = 0

            if len(collected) > remaining_total_jobs:
                source_truncated_by_run_limit = len(collected) - remaining_total_jobs
                stopped_by_safety_cap = True
                truncated_by_run_limit_count += source_truncated_by_run_limit
                collected = collected[:remaining_total_jobs]
                source_warnings.append(
                    _prefix_source_message(source_key, f"truncated_to_run_limit:{remaining_total_jobs}")
                )
            source_meta["truncated_by_run_limit_count"] = max(
                _meta_count(source_meta, "truncated_by_run_limit_count", 0),
                source_truncated_by_run_limit,
            )
            source_meta["jobs_found_per_source"] = _meta_count(source_meta, "jobs_found_per_source", len(collected))
            source_meta["minimum_jobs_per_source_requested"] = max(
                _meta_count(source_meta, "minimum_jobs_per_source_requested", 0),
                int(source_request.get("minimum_jobs_per_source") or 0),
            )
            source_meta["stop_when_minimum_reached"] = stop_when_minimum_reached
            if not str(source_meta.get("collection_stop_reason") or "").strip():
                if source_truncated_by_run_limit > 0:
                    source_meta["collection_stop_reason"] = "safety_cap"
                else:
                    source_meta["collection_stop_reason"] = "exhausted"

            status_raw = str(collector_result.get("status") or "").strip().lower()
            status_raw = _normalize_source_status(
                status_raw=status_raw,
                collected_count=len(collected),
                source_errors=source_errors,
                source_meta=source_meta,
            )

            if status_raw in FAILED_SOURCE_STATUSES and not source_errors:
                source_errors = [f"{source_key}: collector_failed_without_error_details"]

            raw_jobs.extend([row for row in collected if isinstance(row, dict)])
            warnings.extend(source_warnings)
            collector_errors.extend(source_errors)

            if status_raw in FAILED_SOURCE_STATUSES:
                logger.error(
                    "jobs_collect source=%s failed: %s",
                    source_key,
                    source_errors[0] if source_errors else "collector_failed_without_error_details",
                )
            else:
                logger.info("jobs_collect source=%s jobs=%s status=%s", source_key, len(collected), status_raw)
                if status_raw in EMPTY_SOURCE_STATUSES:
                    logger.warning("jobs_collect source=%s jobs=0 status=empty", source_key)

            source_results[source_key] = {
                "source": source_key,
                "status": status_raw,
                "jobs_count": len(collected),
                "warnings": source_warnings,
                "errors": source_errors,
                "error": source_errors[0] if source_errors else None,
                "meta": source_meta,
            }
            supported_fields_by_source[source_key] = getattr(collector, "SUPPORTED_FIELDS", {})
        except Exception as exc:
            error_text = f"{source_key}: collector_failed: {type(exc).__name__}: {exc}"
            collector_errors.append(error_text)
            logger.error("jobs_collect source=%s failed: %s", source_key, error_text)
            source_results[source_key] = {
                "source": source_key,
                "status": "upstream_failure",
                "jobs_count": 0,
                "warnings": [],
                "errors": [error_text],
                "error": error_text,
                "meta": {"source_status": "upstream_failure", "source_error_type": type(exc).__name__},
            }

    successful_sources = [key for key, row in source_results.items() if row.get("status") in SUCCESSFUL_SOURCE_STATUSES]
    healthy_sources = [key for key, row in source_results.items() if row.get("status") in HEALTHY_SOURCE_STATUSES]
    empty_sources = [key for key, row in source_results.items() if row.get("status") in EMPTY_SOURCE_STATUSES]
    under_target_sources = [key for key, row in source_results.items() if row.get("status") in DEGRADED_SOURCE_STATUSES]
    failed_sources = [key for key, row in source_results.items() if row.get("status") in FAILED_SOURCE_STATUSES]
    skipped_sources = [key for key, row in source_results.items() if row.get("status") == "skipped"]
    minimum_state = _minimum_state()
    if minimum_target_requested and stop_when_minimum_reached and minimum_state["minimum_reached"]:
        stopped_by_minimum = True

    # All enabled sources failing is treated as transient so retries can recover.
    enabled_non_manual_sources = [
        source
        for source in sources
        if str(source).strip().lower() in ACTIVE_COLLECTOR_SOURCES and collectors_enabled
    ]
    if enabled_non_manual_sources and failed_sources and len(failed_sources) == len(enabled_non_manual_sources) and not raw_jobs:
        raise RuntimeError(
            "jobs_collect_v1 all enabled sources failed: "
            + "; ".join(source_results[source]["error"] for source in failed_sources if source_results[source].get("error"))
        )

    board_counts = {board: int(source_results.get(board, {}).get("jobs_count", 0)) for board in DEFAULT_JOB_BOARDS}
    collection_status = "success"
    if failed_sources or under_target_sources or (minimum_target_requested and not minimum_state["minimum_reached"]):
        collection_status = "partial_success"

    discovered_raw_count = 0
    kept_after_basic_filter_count = 0
    dropped_by_basic_filter_count = 0
    deduped_count = 0
    pages_fetched = 0
    queries_attempted: list[str] = []
    queries_executed_count = 0
    empty_queries_count = 0
    query_examples: list[str] = []
    metadata_completeness_summary = _empty_metadata_summary()
    source_metadata_quality: dict[str, dict[str, int]] = {}
    for source_key, result in source_results.items():
        meta = result.get("meta") if isinstance(result.get("meta"), dict) else {}
        jobs_count = int(result.get("jobs_count", 0) or 0)
        discovered_raw_count += _meta_count(meta, "discovered_raw_count", jobs_count)
        kept_after_basic_filter_count += _meta_count(meta, "kept_after_basic_filter_count", jobs_count)
        dropped_by_basic_filter_count += _meta_count(meta, "dropped_by_basic_filter_count", 0)
        deduped_count += _meta_count(meta, "deduped_count", 0)
        pages_fetched += _meta_count(meta, "pages_fetched", 0)
        queries_executed_count += _meta_count(meta, "queries_executed_count", 0)
        empty_queries_count += _meta_count(meta, "empty_queries_count", 0)
        if isinstance(meta.get("queries_attempted"), list):
            for value in meta.get("queries_attempted") or []:
                if isinstance(value, str) and value.strip() and value.strip() not in queries_attempted:
                    queries_attempted.append(value.strip())
        if isinstance(meta.get("query_examples"), list):
            for value in meta.get("query_examples") or []:
                if isinstance(value, str) and value.strip() and value.strip() not in query_examples:
                    query_examples.append(value.strip())
        source_summary = meta.get("metadata_completeness_summary") if isinstance(meta.get("metadata_completeness_summary"), dict) else None
        if source_summary:
            normalized_summary = {
                "job_count": _meta_count(source_summary, "job_count", jobs_count),
                "missing_company": _meta_count(source_summary, "missing_company", 0),
                "missing_posted_at": _meta_count(source_summary, "missing_posted_at", 0),
                "missing_source_url": _meta_count(source_summary, "missing_source_url", 0),
                "missing_location": _meta_count(source_summary, "missing_location", 0),
            }
            source_metadata_quality[source_key] = normalized_summary
            for key, value in normalized_summary.items():
                metadata_completeness_summary[key] += value

    reason_stopped = "exhausted"
    if minimum_target_requested and stop_when_minimum_reached and minimum_state["minimum_reached"]:
        reason_stopped = "minimum_reached"
    elif stopped_by_time_cap:
        reason_stopped = "time_cap"
    elif stopped_by_safety_cap or len(raw_jobs) >= max_total_jobs:
        reason_stopped = "safety_cap"
    minimum_state["reason_stopped"] = reason_stopped

    run_preview_messages = _build_run_preview_messages(
        source_results=source_results,
    )
    for note in source_configuration_notes:
        if note not in run_preview_messages:
            run_preview_messages.insert(0, note)
    if minimum_target_requested:
        if minimum_state["minimum_reached"]:
            run_preview_messages.append("Minimum jobs target reached before ranking.")
        elif minimum_state["shortfall_summary"]:
            run_preview_messages.append(
                f"Minimum jobs target shortfall: {minimum_state['shortfall_summary']}"
            )

    collection_observability = _build_collection_observability(
        source_results=source_results,
        source_metadata_quality=source_metadata_quality,
        discovered_raw_count=discovered_raw_count,
        kept_after_basic_filter_count=kept_after_basic_filter_count,
        dropped_by_basic_filter_count=dropped_by_basic_filter_count,
        deduped_count=deduped_count,
        raw_job_count=len(raw_jobs),
        successful_sources=successful_sources,
        healthy_sources=healthy_sources,
        max_total_jobs=max_total_jobs,
        truncated_by_run_limit_count=truncated_by_run_limit_count,
        run_preview_messages=run_preview_messages,
        minimum_targets=minimum_state,
    )

    artifact = {
        "artifact_type": "jobs.collect.v1",
        "artifact_schema": "jobs_raw.v1",
        "pipeline_id": pipeline_id,
        "scanned_at": utc_iso(),
        "request": request,
        "raw_jobs": raw_jobs,
        "source_counts": source_counts(raw_jobs),
        "board_counts": board_counts,
        "source_results": source_results,
        "supported_fields_by_source": supported_fields_by_source,
        "warnings": warnings,
        "collector_errors": collector_errors,
        "collection_status": collection_status,
        "partial_success": collection_status == "partial_success",
        "successful_sources": successful_sources,
        "healthy_sources": healthy_sources,
        "empty_sources": empty_sources,
        "under_target_sources": under_target_sources,
        "failed_sources": failed_sources,
        "skipped_sources": skipped_sources,
        "collection_counts": {
            "raw_job_count": len(raw_jobs),
            "unique_job_count": minimum_state["current_unique_jobs_total"],
            "discovered_raw_count": discovered_raw_count,
            "kept_after_basic_filter_count": kept_after_basic_filter_count,
            "dropped_by_basic_filter_count": dropped_by_basic_filter_count,
            "deduped_count": deduped_count,
            "queries_executed_count": queries_executed_count,
            "empty_queries_count": empty_queries_count,
            "truncated_by_run_limit_count": truncated_by_run_limit_count,
            "minimum_raw_jobs_total_requested": minimum_state["minimum_raw_jobs_total_requested"],
            "minimum_unique_jobs_total_requested": minimum_state["minimum_unique_jobs_total_requested"],
            "minimum_jobs_per_source_requested": minimum_state["minimum_jobs_per_source_requested"],
            "minimum_reached": minimum_state["minimum_reached"],
            "reason_stopped": reason_stopped,
        },
        "source_metadata_quality": source_metadata_quality,
        "metadata_completeness_summary": metadata_completeness_summary,
        "collection_observability": collection_observability,
        "collection_summary": {
            "requested_sources": sources,
            "disabled_sources": disabled_sources,
            "source_configuration_notes": source_configuration_notes,
            "collectors_enabled": collectors_enabled,
            "successful_source_count": len(successful_sources),
            "healthy_source_count": len(healthy_sources),
            "empty_source_count": len(empty_sources),
            "under_target_source_count": len(under_target_sources),
            "failed_source_count": len(failed_sources),
            "skipped_source_count": len(skipped_sources),
            "raw_job_count": len(raw_jobs),
            "discovered_raw_count": discovered_raw_count,
            "kept_after_basic_filter_count": kept_after_basic_filter_count,
            "dropped_by_basic_filter_count": dropped_by_basic_filter_count,
            "deduped_count": deduped_count,
            "pages_fetched": pages_fetched,
            "queries_attempted": queries_attempted,
            "queries_executed_count": queries_executed_count,
            "empty_queries_count": empty_queries_count,
            "query_examples": query_examples[:10],
            "max_total_jobs": max_total_jobs,
            "truncated_by_run_limit_count": truncated_by_run_limit_count,
            "minimum_raw_jobs_total_requested": minimum_state["minimum_raw_jobs_total_requested"],
            "minimum_unique_jobs_total_requested": minimum_state["minimum_unique_jobs_total_requested"],
            "minimum_jobs_per_source_requested": minimum_state["minimum_jobs_per_source_requested"],
            "stop_when_minimum_reached": minimum_state["stop_when_minimum_reached"],
            "minimum_reached": minimum_state["minimum_reached"],
            "reason_stopped": reason_stopped,
            "minimum_shortfall_summary": minimum_state["shortfall_summary"] or None,
            "missing_company": metadata_completeness_summary["missing_company"],
            "missing_posted_at": metadata_completeness_summary["missing_posted_at"],
            "missing_source_url": metadata_completeness_summary["missing_source_url"],
            "missing_location": metadata_completeness_summary["missing_location"],
        },
        "lineage": payload.get("lineage") if isinstance(payload.get("lineage"), dict) else {},
    }

    debug_artifact = {
        "artifact_type": "debug.json",
        "pipeline_id": pipeline_id,
        "sources_attempted": sources,
        "disabled_sources": disabled_sources,
        "source_configuration_notes": source_configuration_notes,
        "sources_succeeded": successful_sources,
        "sources_healthy": healthy_sources,
        "sources_empty": empty_sources,
        "sources_under_target": under_target_sources,
        "sources_failed": failed_sources,
        "sources_skipped": skipped_sources,
        "per_source_job_counts": {key: int(row.get("jobs_count", 0) or 0) for key, row in source_results.items()},
        "minimum_raw_jobs_total_requested": minimum_state["minimum_raw_jobs_total_requested"],
        "minimum_unique_jobs_total_requested": minimum_state["minimum_unique_jobs_total_requested"],
        "minimum_jobs_per_source_requested": minimum_state["minimum_jobs_per_source_requested"],
        "minimum_reached": minimum_state["minimum_reached"],
        "minimum_shortfall_summary": minimum_state["shortfall_summary"] or None,
        "reason_stopped": reason_stopped,
        "per_source_status": {
            key: {
                "source": key,
                "status": str(row.get("status") or "").strip() or "unknown",
                "error": row.get("error"),
                "jobs_count": int(row.get("jobs_count", 0) or 0),
                "source_status": str(
                    ((row.get("meta") if isinstance(row.get("meta"), dict) else {}) or {}).get("source_status")
                    or row.get("status")
                    or ""
                ).strip()
                or None,
                "source_error_type": str(
                    ((row.get("meta") if isinstance(row.get("meta"), dict) else {}) or {}).get("source_error_type")
                    or ((row.get("meta") if isinstance(row.get("meta"), dict) else {}) or {}).get("error_type")
                    or ""
                ).strip()
                or None,
                "collection_stop_reason": str(
                    ((row.get("meta") if isinstance(row.get("meta"), dict) else {}) or {}).get("collection_stop_reason")
                    or ((row.get("meta") if isinstance(row.get("meta"), dict) else {}) or {}).get("reason")
                    or ""
                ).strip()
                or None,
                "pages_attempted": _meta_count(row.get("meta") if isinstance(row.get("meta"), dict) else {}, "pages_attempted", 0),
                "cards_seen": _meta_count(row.get("meta") if isinstance(row.get("meta"), dict) else {}, "cards_seen", 0),
                "listing_cards_seen": _meta_count(
                    row.get("meta") if isinstance(row.get("meta"), dict) else {},
                    "listing_cards_seen",
                    _meta_count(row.get("meta") if isinstance(row.get("meta"), dict) else {}, "cards_seen", 0),
                ),
                "jobs_raw": _meta_count(row.get("meta") if isinstance(row.get("meta"), dict) else {}, "jobs_raw", int(row.get("jobs_count", 0) or 0)),
                "jobs_kept": _meta_count(row.get("meta") if isinstance(row.get("meta"), dict) else {}, "jobs_kept", int(row.get("jobs_count", 0) or 0)),
                "wall_detected": bool(
                    ((row.get("meta") if isinstance(row.get("meta"), dict) else {}) or {}).get("wall_detected", False)
                ),
                "auth_required_detected": bool(
                    ((row.get("meta") if isinstance(row.get("meta"), dict) else {}) or {}).get("auth_required_detected", False)
                ),
                "login_wall_detected": bool(
                    ((row.get("meta") if isinstance(row.get("meta"), dict) else {}) or {}).get("login_wall_detected", False)
                )
                or str(
                    ((row.get("meta") if isinstance(row.get("meta"), dict) else {}) or {}).get("source_error_type")
                    or ((row.get("meta") if isinstance(row.get("meta"), dict) else {}) or {}).get("error_type")
                    or ""
                ).strip()
                in {"login_wall", "login_wall_detected"},
                "anti_bot_detected": bool(
                    ((row.get("meta") if isinstance(row.get("meta"), dict) else {}) or {}).get("anti_bot_detected", False)
                )
                or str(
                    ((row.get("meta") if isinstance(row.get("meta"), dict) else {}) or {}).get("source_error_type")
                    or ((row.get("meta") if isinstance(row.get("meta"), dict) else {}) or {}).get("error_type")
                    or ""
                ).strip()
                == "anti_bot_detected",
                "consent_wall_detected": bool(
                    ((row.get("meta") if isinstance(row.get("meta"), dict) else {}) or {}).get("consent_wall_detected", False)
                )
                or str(
                    ((row.get("meta") if isinstance(row.get("meta"), dict) else {}) or {}).get("source_error_type")
                    or ((row.get("meta") if isinstance(row.get("meta"), dict) else {}) or {}).get("error_type")
                    or ""
                ).strip()
                == "consent_wall_detected",
                "unexpected_redirect_detected": bool(
                    ((row.get("meta") if isinstance(row.get("meta"), dict) else {}) or {}).get("unexpected_redirect_detected", False)
                ),
                "layout_mismatch_detected": bool(
                    ((row.get("meta") if isinstance(row.get("meta"), dict) else {}) or {}).get("layout_mismatch_detected", False)
                )
                or str(
                    ((row.get("meta") if isinstance(row.get("meta"), dict) else {}) or {}).get("source_status")
                    or row.get("status")
                    or ""
                ).strip()
                == "layout_mismatch",
                "parsing_strategy_used": str(
                    ((row.get("meta") if isinstance(row.get("meta"), dict) else {}) or {}).get("parsing_strategy_used")
                    or ""
                ).strip()
                or None,
                "browser_fallback_used": bool(
                    ((row.get("meta") if isinstance(row.get("meta"), dict) else {}) or {}).get("browser_fallback_used", False)
                ),
            }
            for key, row in source_results.items()
        },
        "warnings_count": len(warnings),
        "collector_error_count": len(collector_errors),
        "partial_success": artifact["partial_success"],
    }

    upstream = build_upstream_ref(task, "jobs_collect_v1")
    upstream_run_id = upstream.get("run_id") or str(getattr(task, "id", ""))
    next_payload = {
        "pipeline_id": pipeline_id,
        "upstream": upstream,
        "request": request,
        "normalization_policy": {
            "dedupe_keys": ["source", "url", "title"],
            "canonicalization_version": "1.0",
        },
    }

    next_task = {
        "task_type": "jobs_normalize_v1",
        "payload_json": next_payload,
        "idempotency_key": stage_idempotency_key(pipeline_id, "jobs_normalize_v1", upstream_run_id),
        "max_attempts": 3,
    }

    return {
        "artifact_type": "jobs.collect.v1",
        "content_text": (
            f"jobs_collect_v1 collected {len(raw_jobs)} jobs across {len(successful_sources)} usable sources"
            f" and {len(healthy_sources)} healthy sources"
            f" from {discovered_raw_count} discovered candidates"
            f" with {len(failed_sources)} blocked or failed sources."
        ),
        "content_json": artifact,
        "debug_json": debug_artifact,
        "next_tasks": [next_task],
    }
