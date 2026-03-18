from __future__ import annotations

from typing import Any

from task_handlers.jobs_normalize_helpers import dedupe_normalized_jobs, normalize_jobs
from task_handlers.jobs_pipeline_common import (
    build_upstream_ref,
    expect_artifact_type,
    fetch_upstream_result_content_json,
    new_pipeline_id,
    payload_object,
    resolve_request,
    source_counts,
    stage_idempotency_key,
    utc_iso,
)


def _dedupe_policy(normalization_policy: dict[str, Any]) -> dict[str, Any]:
    fuzzy_cfg = normalization_policy.get("fuzzy_matching")
    if not isinstance(fuzzy_cfg, dict):
        fuzzy_cfg = {}

    try:
        threshold = float(fuzzy_cfg.get("threshold") or 0.84)
    except (TypeError, ValueError):
        threshold = 0.84
    try:
        ambiguous_threshold = float(fuzzy_cfg.get("ambiguous_threshold") or 0.68)
    except (TypeError, ValueError):
        ambiguous_threshold = 0.68
    threshold = max(0.5, min(threshold, 1.0))
    ambiguous_threshold = max(0.4, min(ambiguous_threshold, threshold))

    return {
        "enabled": bool(fuzzy_cfg.get("enabled", True)),
        "threshold": threshold,
        "ambiguous_threshold": ambiguous_threshold,
        "strategy": str(normalization_policy.get("dedupe_strategy") or "company_title_location"),
    }


def _strip_internal_keys(job: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in job.items() if not key.startswith("_")}


def execute(task: Any, db: Any) -> dict[str, Any]:
    payload = payload_object(task.payload_json)
    upstream = payload.get("upstream") if isinstance(payload.get("upstream"), dict) else {}
    request = resolve_request(payload.get("request") if isinstance(payload.get("request"), dict) else payload)
    pipeline_id = new_pipeline_id(payload.get("pipeline_id"))

    upstream_result = fetch_upstream_result_content_json(db, upstream)
    expect_artifact_type(upstream_result, "jobs.collect.v1")

    raw_jobs = upstream_result.get("raw_jobs")
    raw_count = len(raw_jobs) if isinstance(raw_jobs, list) else 0
    upstream_collection_summary = (
        upstream_result.get("collection_summary") if isinstance(upstream_result.get("collection_summary"), dict) else {}
    )
    normalized_jobs, drop_reasons = normalize_jobs(raw_jobs)

    normalization_policy = payload.get("normalization_policy") if isinstance(payload.get("normalization_policy"), dict) else {}
    dedupe_policy = _dedupe_policy(normalization_policy)

    deduped_jobs, duplicate_groups, ambiguous_cases, duplicates_collapsed = dedupe_normalized_jobs(
        normalized_jobs,
        fuzzy_enabled=bool(dedupe_policy["enabled"]),
        fuzzy_threshold=float(dedupe_policy["threshold"]),
        fuzzy_ambiguous_threshold=float(dedupe_policy["ambiguous_threshold"]),
    )

    warnings = []
    for warning in upstream_result.get("warnings") or []:
        if isinstance(warning, str):
            warnings.append(warning)

    normalized_clean = [_strip_internal_keys(row) for row in normalized_jobs]
    deduped_clean = [_strip_internal_keys(row) for row in deduped_jobs]
    normalized_count = len(normalized_clean)
    deduped_count = len(deduped_clean)

    jobs_normalized_artifact = {
        "artifact_type": "jobs_normalized.v1",
        "artifact_schema": "jobs_normalized.v1",
        "pipeline_id": pipeline_id,
        "normalized_at": utc_iso(),
        "request": request,
        "raw_count": raw_count,
        "normalized_count": normalized_count,
        "jobs": normalized_clean,
        "drop_reasons": drop_reasons,
        "warnings": warnings,
        "upstream": upstream,
    }

    jobs_deduped_artifact = {
        "artifact_type": "jobs_deduped.v1",
        "artifact_schema": "jobs_deduped.v1",
        "pipeline_id": pipeline_id,
        "deduped_at": utc_iso(),
        "request": request,
        "raw_count": raw_count,
        "normalized_count": normalized_count,
        "deduped_count": deduped_count,
        "duplicates_collapsed": duplicates_collapsed,
        "dedupe_strategy": {
            "primary_key": "company + normalized_title + normalized_location",
            "fuzzy_enabled": bool(dedupe_policy["enabled"]),
            "fuzzy_threshold": float(dedupe_policy["threshold"]),
            "fuzzy_ambiguous_threshold": float(dedupe_policy["ambiguous_threshold"]),
            "strategy": str(dedupe_policy["strategy"]),
        },
        "jobs": deduped_clean,
        "duplicate_groups": duplicate_groups,
        "ambiguous_duplicate_cases": ambiguous_cases,
        "warnings": warnings,
        "upstream": upstream,
    }

    artifact = {
        "artifact_type": "jobs.normalize.v1",
        "artifact_schema": "jobs.normalize.v2",
        "pipeline_id": pipeline_id,
        "normalized_at": utc_iso(),
        "request": request,
        "normalization_policy": {
            "canonicalization_version": str(normalization_policy.get("canonicalization_version") or "2.0"),
            "dedupe_strategy": str(dedupe_policy["strategy"]),
            "fuzzy_matching": {
                "enabled": bool(dedupe_policy["enabled"]),
                "threshold": float(dedupe_policy["threshold"]),
                "ambiguous_threshold": float(dedupe_policy["ambiguous_threshold"]),
            },
        },
        "counts": {
            "raw_count": raw_count,
            "normalized_count": normalized_count,
            "deduped_count": deduped_count,
            "duplicates_collapsed": duplicates_collapsed,
            "discovered_raw_count": int(upstream_collection_summary.get("discovered_raw_count") or raw_count),
            "kept_after_basic_filter_count": int(
                upstream_collection_summary.get("kept_after_basic_filter_count") or raw_count
            ),
            "dropped_by_basic_filter_count": int(upstream_collection_summary.get("dropped_by_basic_filter_count") or 0),
            "collection_deduped_count": int(upstream_collection_summary.get("deduped_count") or 0),
        },
        "jobs_normalized_artifact": jobs_normalized_artifact,
        "jobs_deduped_artifact": jobs_deduped_artifact,
        # Backwards-compatible field consumed by jobs_rank_v1.
        "normalized_jobs": deduped_clean,
        "source_counts": source_counts(deduped_clean),
        "dropped_count": int(drop_reasons.get("invalid_item_type", 0) + drop_reasons.get("missing_title", 0) + duplicates_collapsed),
        "drop_reasons": {
            **drop_reasons,
            "duplicate": duplicates_collapsed,
        },
        "dedupe_stats": {
            "before": normalized_count,
            "after": deduped_count,
            "duplicates": duplicates_collapsed,
            "group_count": len(duplicate_groups),
            "ambiguous_case_count": len(ambiguous_cases),
        },
        "upstream_collection_counts": {
            "discovered_raw_count": int(upstream_collection_summary.get("discovered_raw_count") or raw_count),
            "kept_after_basic_filter_count": int(upstream_collection_summary.get("kept_after_basic_filter_count") or raw_count),
            "dropped_by_basic_filter_count": int(upstream_collection_summary.get("dropped_by_basic_filter_count") or 0),
            "deduped_count": int(upstream_collection_summary.get("deduped_count") or 0),
        },
        "ambiguous_duplicate_cases": ambiguous_cases,
        "warnings": warnings,
        "upstream": upstream,
    }

    next_upstream = build_upstream_ref(task, "jobs_normalize_v1")
    upstream_run_id = next_upstream.get("run_id") or str(getattr(task, "id", ""))
    next_payload = {
        "pipeline_id": pipeline_id,
        "upstream": next_upstream,
        "request": request,
        "rank_policy": payload.get("rank_policy") if isinstance(payload.get("rank_policy"), dict) else {
            "weights": {
                "title": 0.35,
                "salary": 0.25,
                "location": 0.2,
                "work_mode": 0.2,
            },
            "max_ranked": 200,
            "llm_enabled": bool(request.get("rank_llm_enabled", True)),
            "prompt_version": "jobs-rank-v1",
        },
    }

    return {
        "artifact_type": "jobs.normalize.v1",
        "content_text": (
            f"Normalized {normalized_count} jobs from {raw_count} raw inputs and deduped to {deduped_count} "
            f"(collapsed={duplicates_collapsed})."
        ),
        "content_json": artifact,
        "debug_json": {
            "pipeline_id": pipeline_id,
            "raw_count": raw_count,
            "normalized_count": normalized_count,
            "deduped_count": deduped_count,
            "duplicates_collapsed": duplicates_collapsed,
            "ambiguous_case_count": len(ambiguous_cases),
        },
        "next_tasks": [
            {
                "task_type": "jobs_rank_v1",
                "payload_json": next_payload,
                "idempotency_key": stage_idempotency_key(pipeline_id, "jobs_rank_v1", upstream_run_id),
                "max_attempts": 3,
            }
        ],
    }
