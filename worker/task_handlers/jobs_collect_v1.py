from __future__ import annotations

import importlib
import logging
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

SUPPORTED_COLLECTOR_SOURCES = ("linkedin", "indeed", "glassdoor", "handshake")
SUCCESSFUL_SOURCE_STATUSES = {"success", "partial_success"}
EMPTY_SOURCE_STATUSES = {"empty", "empty_success"}
FAILED_SOURCE_STATUSES = {"failed", "auth_blocked", "layout_mismatch", "upstream_failure"}
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


def _meta_count(meta: dict[str, Any], key: str, fallback: int = 0) -> int:
    try:
        return int(meta.get(key, fallback))
    except (TypeError, ValueError):
        return fallback


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
    max_total_jobs: int,
    truncated_by_run_limit_count: int,
) -> dict[str, Any]:
    by_source: dict[str, dict[str, Any]] = {}
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
                    "jobs_raw": _meta_count(row, "jobs_raw", _meta_count(row, "discovered_raw_count", 0)),
                    "jobs_kept": _meta_count(row, "jobs_kept", _meta_count(row, "returned_count", 0)),
                    "auth_required_detected": bool(row.get("auth_required_detected", False)),
                    "login_wall_detected": bool(row.get("login_wall_detected", False)),
                    "stop_reason": str(row.get("stop_reason") or "").strip() or None,
                }
            )

        by_source[source_key] = {
            "status": result.get("status"),
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
            "jobs_raw": _meta_count(meta, "jobs_raw", discovered),
            "jobs_kept": _meta_count(meta, "jobs_kept", jobs_count),
            "jobs_found_per_source": discovered,
            "queries_executed_count": queries_executed,
            "queries_attempted_count": len([row for row in queries_attempted if isinstance(row, str) and row.strip()]),
            "empty_queries_count": empty_queries,
            "query_examples": source_examples[:3],
            "request_urls_tried": meta.get("request_urls_tried") if isinstance(meta.get("request_urls_tried"), list) else [],
            "last_request_url": str(meta.get("last_request_url") or "").strip() or None,
            "error_type": str(meta.get("error_type") or "").strip() or None,
            "error_status": meta.get("error_status") if isinstance(meta.get("error_status"), int) else None,
            "auth_required_detected": bool(meta.get("auth_required_detected", False)),
            "login_wall_detected": bool(meta.get("login_wall_detected", False)),
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

    searched_enough = (
        f"{discovered_raw_count} raw discovered across {len(successful_sources)} live sources"
        f" from {total_queries_executed} executed queries."
        + (f" Strongest {strongest_source}." if strongest_source else "")
        + (f" Weakest {weakest_source}." if weakest_source and weakest_source != strongest_source else "")
    )
    collapse_reason = (
        f"{dropped_by_basic_filter_count} dropped in basic filtering and {deduped_count} deduped before returning {raw_job_count} raw jobs."
    )
    if truncated_by_run_limit_count > 0:
        collapse_reason += f" Run cap truncated another {truncated_by_run_limit_count} jobs at the artifact level."
    metadata_gap = (
        f"Weakest metadata source: {weakest_metadata_source}."
        if weakest_metadata_source
        else "No source-level metadata gaps were detected."
    )

    return {
        "waterfall": {
            "raw_jobs_discovered": discovered_raw_count,
            "kept_after_basic_filter": kept_after_basic_filter_count,
            "jobs_dropped": dropped_by_basic_filter_count,
            "deduped_in_collection": deduped_count,
            "final_raw_jobs": raw_job_count,
        },
        "query_summary": {
            "queries_executed": total_queries_executed,
            "empty_queries_count": total_empty_queries,
            "max_total_jobs": max_total_jobs,
            "query_examples": query_examples[:10],
            "query_runs": query_runs,
        },
        "by_source": by_source,
        "operator_questions": {
            "searched_enough": searched_enough,
            "which_source_is_weak": metadata_gap,
            "why_raw_count_collapsed": collapse_reason,
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
    max_total_jobs = max(1, int(request.get("max_total_jobs") or len(sources) or 1))
    board_url_overrides = request.get("board_url_overrides") if isinstance(request.get("board_url_overrides"), dict) else {}
    collectors_enabled = bool(request.get("collectors_enabled", True))
    truncated_by_run_limit_count = 0

    for source in sources:
        source_key = str(source).strip().lower()
        if not source_key:
            continue

        remaining_total_jobs = max_total_jobs - len(raw_jobs)
        if remaining_total_jobs <= 0:
            source_results[source_key] = {
                "source": source_key,
                "status": "skipped",
                "jobs_count": 0,
                "warnings": [],
                "errors": [],
                "error": None,
                "meta": {"reason": "max_total_jobs_reached", "remaining_total_jobs": 0},
            }
            continue

        if source_key == "manual":
            manual_jobs = _normalize_manual_jobs(request)
            if len(manual_jobs) > remaining_total_jobs:
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
                },
            }
            supported_fields_by_source[source_key] = dict(MANUAL_SUPPORTED_FIELDS)
            continue

        if source_key not in SUPPORTED_COLLECTOR_SOURCES:
            message = f"{source_key}: unsupported_source"
            collector_errors.append(message)
            logger.error("jobs_collect source=%s failed: %s", source_key, message)
            source_results[source_key] = {
                "source": source_key,
                "status": "failed",
                "jobs_count": 0,
                "warnings": [],
                "errors": [message],
                "error": message,
                "meta": {},
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
            per_source_limit = min(int(source_request.get("result_limit_per_source") or remaining_total_jobs), remaining_total_jobs)
            source_request["result_limit_per_source"] = per_source_limit
            source_request["max_jobs_per_source"] = per_source_limit
            source_request["max_jobs_per_board"] = per_source_limit
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

            status_raw = str(collector_result.get("status") or "").strip().lower()
            if status_raw not in SUCCESSFUL_SOURCE_STATUSES | EMPTY_SOURCE_STATUSES | FAILED_SOURCE_STATUSES:
                status_raw = "failed" if source_errors and not collected else "partial_success" if source_errors else "success"
            if status_raw == "success" and not collected and not source_errors:
                status_raw = "empty"

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
                "status": "failed",
                "jobs_count": 0,
                "warnings": [],
                "errors": [error_text],
                "error": error_text,
                "meta": {},
            }

    successful_sources = [key for key, row in source_results.items() if row.get("status") in SUCCESSFUL_SOURCE_STATUSES]
    empty_sources = [key for key, row in source_results.items() if row.get("status") in EMPTY_SOURCE_STATUSES]
    partial_sources = [key for key, row in source_results.items() if row.get("status") == "partial_success"]
    failed_sources = [key for key, row in source_results.items() if row.get("status") in FAILED_SOURCE_STATUSES]
    skipped_sources = [key for key, row in source_results.items() if row.get("status") == "skipped"]

    # All enabled sources failing is treated as transient so retries can recover.
    enabled_non_manual_sources = [
        source
        for source in sources
        if str(source).strip().lower() in SUPPORTED_COLLECTOR_SOURCES and collectors_enabled
    ]
    if enabled_non_manual_sources and failed_sources and len(failed_sources) == len(enabled_non_manual_sources) and not raw_jobs:
        raise RuntimeError(
            "jobs_collect_v1 all enabled sources failed: "
            + "; ".join(source_results[source]["error"] for source in failed_sources if source_results[source].get("error"))
        )

    board_counts = {board: int(source_results.get(board, {}).get("jobs_count", 0)) for board in DEFAULT_JOB_BOARDS}
    collection_status = "success"
    if failed_sources or partial_sources:
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

    collection_observability = _build_collection_observability(
        source_results=source_results,
        source_metadata_quality=source_metadata_quality,
        discovered_raw_count=discovered_raw_count,
        kept_after_basic_filter_count=kept_after_basic_filter_count,
        dropped_by_basic_filter_count=dropped_by_basic_filter_count,
        deduped_count=deduped_count,
        raw_job_count=len(raw_jobs),
        successful_sources=successful_sources,
        max_total_jobs=max_total_jobs,
        truncated_by_run_limit_count=truncated_by_run_limit_count,
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
        "empty_sources": empty_sources,
        "partial_sources": partial_sources,
        "failed_sources": failed_sources,
        "skipped_sources": skipped_sources,
        "collection_counts": {
            "raw_job_count": len(raw_jobs),
            "discovered_raw_count": discovered_raw_count,
            "kept_after_basic_filter_count": kept_after_basic_filter_count,
            "dropped_by_basic_filter_count": dropped_by_basic_filter_count,
            "deduped_count": deduped_count,
            "queries_executed_count": queries_executed_count,
            "empty_queries_count": empty_queries_count,
            "truncated_by_run_limit_count": truncated_by_run_limit_count,
        },
        "source_metadata_quality": source_metadata_quality,
        "metadata_completeness_summary": metadata_completeness_summary,
        "collection_observability": collection_observability,
        "collection_summary": {
            "requested_sources": sources,
            "collectors_enabled": collectors_enabled,
            "successful_source_count": len(successful_sources),
            "empty_source_count": len(empty_sources),
            "partial_source_count": len(partial_sources),
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
        "sources_succeeded": successful_sources,
        "sources_empty": empty_sources,
        "sources_failed": failed_sources,
        "sources_skipped": skipped_sources,
        "per_source_job_counts": {key: int(row.get("jobs_count", 0) or 0) for key, row in source_results.items()},
        "per_source_status": {
            key: {
                "source": key,
                "status": str(row.get("status") or "").strip() or "unknown",
                "error": row.get("error"),
                "jobs_count": int(row.get("jobs_count", 0) or 0),
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
            f"jobs_collect_v1 collected {len(raw_jobs)} jobs across {len(successful_sources)} successful sources"
            f" from {discovered_raw_count} discovered candidates"
            f" with {len(failed_sources)} failed sources."
        ),
        "content_json": artifact,
        "debug_json": debug_artifact,
        "next_tasks": [next_task],
    }
