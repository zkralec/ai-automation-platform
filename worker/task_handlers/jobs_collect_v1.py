from __future__ import annotations

import importlib
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

SUPPORTED_COLLECTOR_SOURCES = ("linkedin", "indeed", "glassdoor", "handshake")
SUCCESSFUL_SOURCE_STATUSES = {"success", "partial_success"}
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


def _empty_metadata_summary() -> dict[str, int]:
    return {
        "job_count": 0,
        "missing_company": 0,
        "missing_posted_at": 0,
        "missing_source_url": 0,
        "missing_location": 0,
    }


def execute(task: Any, db: Any) -> dict[str, Any]:
    del db

    payload = payload_object(task.payload_json)
    request = resolve_request(payload.get("request") if isinstance(payload.get("request"), dict) else payload)
    pipeline_id = new_pipeline_id(payload.get("pipeline_id"))

    raw_jobs: list[dict[str, Any]] = []
    warnings: list[str] = []
    collector_errors: list[str] = []
    source_results: dict[str, dict[str, Any]] = {}
    supported_fields_by_source: dict[str, dict[str, Any]] = {}

    sources = list(request.get("sources") or [])
    board_url_overrides = request.get("board_url_overrides") if isinstance(request.get("board_url_overrides"), dict) else {}
    collectors_enabled = bool(request.get("collectors_enabled", True))

    for source in sources:
        source_key = str(source).strip().lower()
        if not source_key:
            continue

        if source_key == "manual":
            manual_jobs = _normalize_manual_jobs(request)
            raw_jobs.extend(manual_jobs)
            source_results[source_key] = {
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
                },
            }
            supported_fields_by_source[source_key] = dict(MANUAL_SUPPORTED_FIELDS)
            continue

        if source_key not in SUPPORTED_COLLECTOR_SOURCES:
            message = f"{source_key}: unsupported_source"
            collector_errors.append(message)
            source_results[source_key] = {
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
                "status": "skipped",
                "jobs_count": 0,
                "warnings": [],
                "errors": [],
                "error": None,
                "meta": {"reason": "collectors_disabled"},
            }
            continue

        try:
            collector = _load_collector_module(source_key)
            collector_result = collector.collect_jobs(
                request,
                url_override=(str(board_url_overrides.get(source_key) or "").strip() or None),
            )

            collected = collector_result.get("jobs") if isinstance(collector_result.get("jobs"), list) else []
            source_warnings_raw = collector_result.get("warnings") if isinstance(collector_result.get("warnings"), list) else []
            source_errors_raw = collector_result.get("errors") if isinstance(collector_result.get("errors"), list) else []
            source_warnings = [_prefix_source_message(source_key, str(row)) for row in source_warnings_raw if str(row).strip()]
            source_errors = [_prefix_source_message(source_key, str(row)) for row in source_errors_raw if str(row).strip()]
            source_meta = collector_result.get("meta") if isinstance(collector_result.get("meta"), dict) else {}

            status_raw = str(collector_result.get("status") or "").strip().lower()
            if status_raw not in {"success", "partial_success", "failed"}:
                status_raw = "failed" if source_errors and not collected else "partial_success" if source_errors else "success"

            if status_raw == "failed" and not source_errors:
                source_errors = [f"{source_key}: collector_failed_without_error_details"]

            raw_jobs.extend([row for row in collected if isinstance(row, dict)])
            warnings.extend(source_warnings)
            collector_errors.extend(source_errors)

            source_results[source_key] = {
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
            source_results[source_key] = {
                "status": "failed",
                "jobs_count": 0,
                "warnings": [],
                "errors": [error_text],
                "error": error_text,
                "meta": {},
            }

    successful_sources = [key for key, row in source_results.items() if row.get("status") in SUCCESSFUL_SOURCE_STATUSES]
    partial_sources = [key for key, row in source_results.items() if row.get("status") == "partial_success"]
    failed_sources = [key for key, row in source_results.items() if row.get("status") == "failed"]
    skipped_sources = [key for key, row in source_results.items() if row.get("status") == "skipped"]

    # All enabled sources failing is treated as transient so retries can recover.
    enabled_non_manual_sources = [
        source
        for source in sources
        if str(source).strip().lower() in SUPPORTED_COLLECTOR_SOURCES and collectors_enabled
    ]
    if enabled_non_manual_sources and not successful_sources and not raw_jobs:
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
        if isinstance(meta.get("queries_attempted"), list):
            for value in meta.get("queries_attempted") or []:
                if isinstance(value, str) and value.strip() and value.strip() not in queries_attempted:
                    queries_attempted.append(value.strip())
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
        "partial_sources": partial_sources,
        "failed_sources": failed_sources,
        "skipped_sources": skipped_sources,
        "collection_counts": {
            "raw_job_count": len(raw_jobs),
            "discovered_raw_count": discovered_raw_count,
            "kept_after_basic_filter_count": kept_after_basic_filter_count,
            "dropped_by_basic_filter_count": dropped_by_basic_filter_count,
            "deduped_count": deduped_count,
        },
        "source_metadata_quality": source_metadata_quality,
        "metadata_completeness_summary": metadata_completeness_summary,
        "collection_summary": {
            "requested_sources": sources,
            "collectors_enabled": collectors_enabled,
            "successful_source_count": len(successful_sources),
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
            "missing_company": metadata_completeness_summary["missing_company"],
            "missing_posted_at": metadata_completeness_summary["missing_posted_at"],
            "missing_source_url": metadata_completeness_summary["missing_source_url"],
            "missing_location": metadata_completeness_summary["missing_location"],
        },
        "lineage": payload.get("lineage") if isinstance(payload.get("lineage"), dict) else {},
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
        "debug_json": {
            "pipeline_id": pipeline_id,
            "warnings_count": len(warnings),
            "collector_error_count": len(collector_errors),
            "partial_success": artifact["partial_success"],
        },
        "next_tasks": [next_task],
    }
