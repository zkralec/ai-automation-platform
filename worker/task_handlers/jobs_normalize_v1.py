from __future__ import annotations

from datetime import datetime, timezone
import re
from typing import Any

from task_handlers.jobs_normalize_helpers import dedupe_normalized_jobs, normalize_jobs, resolve_posted_age_days
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


def _safe_int(value: Any, fallback: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return fallback


def _rate(count: int, total: int) -> float:
    if total <= 0:
        return 0.0
    return round((float(count) / float(total)) * 100.0, 1)


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


def _build_normalization_observability(
    *,
    upstream_result: dict[str, Any],
    raw_count: int,
    normalized_clean: list[dict[str, Any]],
    deduped_clean: list[dict[str, Any]],
    drop_reasons: dict[str, int],
    duplicates_collapsed: int,
) -> dict[str, Any]:
    upstream_collection_summary = (
        upstream_result.get("collection_summary") if isinstance(upstream_result.get("collection_summary"), dict) else {}
    )
    upstream_observability = (
        upstream_result.get("collection_observability")
        if isinstance(upstream_result.get("collection_observability"), dict)
        else {}
    )
    upstream_by_source = (
        upstream_observability.get("by_source") if isinstance(upstream_observability.get("by_source"), dict) else {}
    )

    normalized_by_source: dict[str, dict[str, Any]] = {}
    deduped_unique_groups_by_source: dict[str, int] = {}

    for row in normalized_clean:
        source = str(row.get("source") or "unknown").strip().lower() or "unknown"
        stats = normalized_by_source.setdefault(
            source,
            {
                "normalized_count": 0,
                "missing_company": 0,
                "missing_posted_at": 0,
                "missing_source_url": 0,
                "missing_location": 0,
            },
        )
        stats["normalized_count"] += 1
        for key in ("missing_company", "missing_posted_at", "missing_source_url", "missing_location"):
            if row.get(key) is True:
                stats[key] += 1

    for row in deduped_clean:
        duplicate_sources = row.get("duplicate_sources") if isinstance(row.get("duplicate_sources"), list) else None
        sources = [
            str(value).strip().lower()
            for value in (duplicate_sources or [row.get("source")])
            if str(value).strip()
        ]
        unique_sources = set(sources or [str(row.get("source") or "unknown").strip().lower() or "unknown"])
        for source in unique_sources:
            deduped_unique_groups_by_source[source] = deduped_unique_groups_by_source.get(source, 0) + 1

    all_sources = set(upstream_by_source.keys()) | set(normalized_by_source.keys()) | set(deduped_unique_groups_by_source.keys())
    by_source: dict[str, dict[str, Any]] = {}
    collapse_candidates: list[tuple[str, int]] = []
    weak_candidates: list[tuple[str, float]] = []

    for source in sorted(all_sources):
        upstream_source = upstream_by_source.get(source) if isinstance(upstream_by_source.get(source), dict) else {}
        normalized_source = normalized_by_source.get(source) or {}
        normalized_count = _safe_int(normalized_source.get("normalized_count"), 0)
        unique_groups = deduped_unique_groups_by_source.get(source, 0)
        dedupe_collapsed = max(normalized_count - unique_groups, 0)
        discovered = _safe_int(
            upstream_source.get("raw_jobs_discovered"),
            _safe_int(upstream_source.get("final_raw_jobs"), normalized_count),
        )
        kept = _safe_int(upstream_source.get("kept_after_basic_filter"), normalized_count)
        dropped = _safe_int(upstream_source.get("jobs_dropped"), max(discovered - kept, 0))
        missing_rates = {
            "missing_company_rate": _rate(_safe_int(normalized_source.get("missing_company"), 0), normalized_count),
            "missing_posted_at_rate": _rate(_safe_int(normalized_source.get("missing_posted_at"), 0), normalized_count),
            "missing_source_url_rate": _rate(_safe_int(normalized_source.get("missing_source_url"), 0), normalized_count),
            "missing_location_rate": _rate(_safe_int(normalized_source.get("missing_location"), 0), normalized_count),
        }
        highest_gap = max(missing_rates.values()) if missing_rates else 0.0
        collapse_candidates.append((source, dedupe_collapsed))
        weak_candidates.append((source, highest_gap))

        by_source[source] = {
            "raw_jobs_discovered": discovered,
            "kept_after_basic_filter": kept,
            "jobs_dropped": dropped,
            "normalized_count": normalized_count,
            "deduped_unique_groups": unique_groups,
            "dedupe_collapsed": dedupe_collapsed,
            "missing_counts": {
                "company": _safe_int(normalized_source.get("missing_company"), 0),
                "posted_at": _safe_int(normalized_source.get("missing_posted_at"), 0),
                "source_url": _safe_int(normalized_source.get("missing_source_url"), 0),
                "location": _safe_int(normalized_source.get("missing_location"), 0),
            },
            "missing_rates": missing_rates,
            "weakness_summary": _compact_missing_summary(missing_rates),
        }

    largest_collapse_entry = max(collapse_candidates, key=lambda item: item[1]) if collapse_candidates else None
    largest_collapse_source = (
        largest_collapse_entry[0]
        if largest_collapse_entry and int(largest_collapse_entry[1]) > 0
        else None
    )
    weakest_metadata_entry = max(weak_candidates, key=lambda item: item[1]) if weak_candidates else None
    weakest_metadata_source = (
        weakest_metadata_entry[0]
        if weakest_metadata_entry and float(weakest_metadata_entry[1]) > 0.0
        else None
    )
    invalid_dropped = int(drop_reasons.get("invalid_item_type", 0) + drop_reasons.get("missing_title", 0))

    dedupe_reason = (
        f"{duplicates_collapsed} collapsed in normalization dedupe."
        + (f" Largest collapse: {largest_collapse_source}." if largest_collapse_source else "")
    )
    metadata_gap = (
        f"Weakest normalized metadata source: {weakest_metadata_source}."
        if weakest_metadata_source
        else "No source-level metadata gaps were detected."
    )

    return {
        "waterfall": {
            "raw_jobs_discovered": _safe_int(upstream_collection_summary.get("discovered_raw_count"), raw_count),
            "kept_after_basic_filter": _safe_int(upstream_collection_summary.get("kept_after_basic_filter_count"), raw_count),
            "jobs_dropped_in_collection": _safe_int(upstream_collection_summary.get("dropped_by_basic_filter_count"), 0),
            "normalized_count": len(normalized_clean),
            "invalid_dropped_before_normalize": invalid_dropped,
            "deduped_count": len(deduped_clean),
            "duplicates_collapsed": duplicates_collapsed,
        },
        "by_source": by_source,
        "operator_questions": {
            "searched_enough": (
                f"{_safe_int(upstream_collection_summary.get('discovered_raw_count'), raw_count)} raw discovered,"
                f" {_safe_int(upstream_collection_summary.get('kept_after_basic_filter_count'), raw_count)} kept after filtering,"
                f" {len(deduped_clean)} unique after normalization."
            ),
            "which_source_is_weak": metadata_gap,
            "why_raw_count_collapsed": (
                f"{_safe_int(upstream_collection_summary.get('dropped_by_basic_filter_count'), 0)} dropped in collection,"
                f" {invalid_dropped} invalid before normalize,"
                f" {duplicates_collapsed} deduped."
            ),
            "are_we_missing_metadata": metadata_gap,
            "dedupe_impact": dedupe_reason,
        },
    }


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


_SENIOR_TITLE_KEYWORD_RE = re.compile(r"\b(senior|sr|lead|staff|principal|manager)\b", re.IGNORECASE)


def _requested_experience_level(request: dict[str, Any]) -> str:
    explicit = str(request.get("experience_level") or "").strip().lower()
    if explicit:
        return "entry" if explicit in {"entry-level", "entry level", "junior", "new grad", "associate"} else explicit

    values = request.get("experience_levels")
    if isinstance(values, list):
        for value in values:
            text = str(value or "").strip().lower()
            if text:
                return "entry" if text in {"entry-level", "entry level", "junior", "new grad", "associate"} else text
    return ""


def matches_experience_level(job: dict[str, Any], requested_level: str) -> bool:
    level = str(job.get("experience_level") or "").strip().lower()
    title = str(job.get("title") or "").strip().lower()

    if requested_level == "entry":
        if level == "senior":
            return False
        return _SENIOR_TITLE_KEYWORD_RE.search(title) is None

    return True


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value or "").strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _job_age_days(posted_at: Any, *, now_utc: datetime | None = None) -> int | None:
    parsed = _parse_datetime(posted_at)
    if parsed is None:
        return None
    now = now_utc or datetime.now(timezone.utc)
    delta = now - parsed
    return max(int(delta.total_seconds() // 86400), 0)


def _row_age_days(row: dict[str, Any], *, now_utc: datetime | None = None) -> int | None:
    return resolve_posted_age_days(
        posted_age_days=row.get("posted_age_days"),
        posted_at=row.get("posted_at_normalized") or row.get("posted_at"),
        posted_at_raw=row.get("posted_at_raw"),
        reference_time=now_utc or datetime.now(timezone.utc),
    )


def _prefer_recent_enabled(request: dict[str, Any]) -> bool:
    if bool(request.get("prefer_recent")):
        return True
    freshness = str(request.get("shortlist_freshness_preference") or "").strip().lower()
    return freshness in {"prefer_recent", "strong_prefer_recent"}


def is_recent(row: dict[str, Any], max_age_days: int, *, now_utc: datetime | None = None) -> bool:
    age_days = _row_age_days(row, now_utc=now_utc)
    if age_days is None:
        return False
    return age_days <= max_age_days


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
    requested_experience_level = _requested_experience_level(request)
    experience_filter_applied = bool(requested_experience_level)
    filtered_out_by_experience_count = 0
    if experience_filter_applied:
        filtered_normalized_jobs: list[dict[str, Any]] = []
        for row in normalized_jobs:
            if matches_experience_level(row, requested_experience_level):
                filtered_normalized_jobs.append(row)
            else:
                filtered_out_by_experience_count += 1
        normalized_jobs = filtered_normalized_jobs
    kept_after_experience_filter_count = len(normalized_jobs)
    recency_filter_applied = _prefer_recent_enabled(request)
    dropped_old_jobs_count = 0
    now_utc = datetime.now(timezone.utc)
    if recency_filter_applied:
        recent_normalized_jobs: list[dict[str, Any]] = []
        for row in normalized_jobs:
            if is_recent(row, 14, now_utc=now_utc):
                recent_normalized_jobs.append(row)
            else:
                dropped_old_jobs_count += 1
        normalized_jobs = recent_normalized_jobs
    kept_after_recency_filter_count = len(normalized_jobs)
    remaining_age_days = [
        age_days
        for row in normalized_jobs
        if (age_days := _row_age_days(row, now_utc=now_utc)) is not None
    ]
    average_job_age_days = round(sum(remaining_age_days) / len(remaining_age_days), 1) if remaining_age_days else 0.0
    oldest_job_age = max(remaining_age_days) if remaining_age_days else 0

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
    normalization_observability = _build_normalization_observability(
        upstream_result=upstream_result,
        raw_count=raw_count,
        normalized_clean=normalized_clean,
        deduped_clean=deduped_clean,
        drop_reasons=drop_reasons,
        duplicates_collapsed=duplicates_collapsed,
    )

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
            "kept_after_experience_filter_count": kept_after_experience_filter_count,
            "filtered_out_by_experience_count": filtered_out_by_experience_count,
            "kept_after_recency_filter_count": kept_after_recency_filter_count,
            "dropped_old_jobs_count": dropped_old_jobs_count,
            "average_job_age_days": average_job_age_days,
            "oldest_job_age": oldest_job_age,
            "deduped_count": deduped_count,
            "duplicates_collapsed": duplicates_collapsed,
            "discovered_raw_count": int(upstream_collection_summary.get("discovered_raw_count") or raw_count),
            "kept_after_basic_filter_count": int(
                upstream_collection_summary.get("kept_after_basic_filter_count") or raw_count
            ),
            "dropped_by_basic_filter_count": int(upstream_collection_summary.get("dropped_by_basic_filter_count") or 0),
            "collection_deduped_count": int(upstream_collection_summary.get("deduped_count") or 0),
        },
        "experience_filter_applied": experience_filter_applied,
        "recency_filter_applied": recency_filter_applied,
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
        "normalization_observability": normalization_observability,
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
            "experience_filter_applied": experience_filter_applied,
            "filtered_out_by_experience_count": filtered_out_by_experience_count,
            "kept_after_experience_filter_count": kept_after_experience_filter_count,
            "recency_filter_applied": recency_filter_applied,
            "dropped_old_jobs_count": dropped_old_jobs_count,
            "kept_after_recency_filter_count": kept_after_recency_filter_count,
            "average_job_age_days": average_job_age_days,
            "oldest_job_age": oldest_job_age,
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
