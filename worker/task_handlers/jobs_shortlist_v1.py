from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any

from jobs_history_state import load_jobs_history, record_jobs_seen, record_jobs_shortlisted
from task_handlers.errors import NonRetryableTaskError
from task_handlers.jobs_pipeline_common import (
    build_upstream_ref,
    fetch_upstream_result_content_json,
    new_pipeline_id,
    payload_object,
    resolve_request,
    stage_idempotency_key,
    utc_iso,
)
from task_handlers.jobs_shortlist_helpers import (
    normalize_scored_jobs,
    resolve_min_score_100,
    shortlist_jobs,
)


def _extract_scored_jobs(upstream_result: dict[str, Any]) -> tuple[str, list[dict[str, Any]], dict[str, Any]]:
    artifact_type = str(upstream_result.get("artifact_type") or "").strip()
    if artifact_type == "jobs.rank.v1":
        pipeline_counts = upstream_result.get("pipeline_counts") if isinstance(upstream_result.get("pipeline_counts"), dict) else {}
        jobs_scored_artifact = upstream_result.get("jobs_scored_artifact")
        if isinstance(jobs_scored_artifact, dict):
            if isinstance(jobs_scored_artifact.get("pipeline_counts"), dict):
                pipeline_counts = jobs_scored_artifact.get("pipeline_counts")
            rows = jobs_scored_artifact.get("jobs_scored")
            if isinstance(rows, list):
                return artifact_type, rows, pipeline_counts
        rows = upstream_result.get("ranked_jobs")
        if isinstance(rows, list):
            return artifact_type, rows, pipeline_counts
        return artifact_type, [], pipeline_counts

    if artifact_type == "jobs_scored.v1":
        pipeline_counts = upstream_result.get("pipeline_counts") if isinstance(upstream_result.get("pipeline_counts"), dict) else {}
        rows = upstream_result.get("jobs_scored")
        if isinstance(rows, list):
            return artifact_type, rows, pipeline_counts
        return artifact_type, [], pipeline_counts

    raise NonRetryableTaskError(
        "upstream contract mismatch: jobs_shortlist_v1 expects artifact_type "
        "'jobs.rank.v1' or 'jobs_scored.v1'"
    )


def _db_supports_history(db: Any) -> bool:
    return hasattr(db, "execute") and hasattr(db, "connection")


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    if not isinstance(value, str):
        return None
    text = value.strip()
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


def _history_timestamp_text(value: Any) -> str | None:
    parsed = _parse_datetime(value)
    if parsed is not None:
        return parsed.replace(microsecond=0).isoformat().replace("+00:00", "Z")
    if isinstance(value, str):
        text = value.strip()
        return text or None
    if value is None:
        return None
    return str(value)


def _resolve_history_policy(shortlist_policy: dict[str, Any], request: dict[str, Any]) -> dict[str, Any]:
    try:
        notification_cooldown_days = int(
            shortlist_policy.get("jobs_notification_cooldown_days")
            or request.get("jobs_notification_cooldown_days")
            or 3
        )
    except (TypeError, ValueError):
        notification_cooldown_days = 3
    notification_cooldown_days = max(0, min(notification_cooldown_days, 30))

    try:
        repeat_penalty = float(
            shortlist_policy.get("jobs_shortlist_repeat_penalty")
            if shortlist_policy.get("jobs_shortlist_repeat_penalty") is not None
            else request.get("jobs_shortlist_repeat_penalty", 4.0)
        )
    except (TypeError, ValueError):
        repeat_penalty = 4.0
    repeat_penalty = max(0.0, min(repeat_penalty, 20.0))

    resurface_seen_jobs = shortlist_policy.get("resurface_seen_jobs")
    if resurface_seen_jobs is None:
        resurface_seen_jobs = request.get("resurface_seen_jobs", True)

    return {
        "jobs_notification_cooldown_days": notification_cooldown_days,
        "jobs_shortlist_repeat_penalty": repeat_penalty,
        "resurface_seen_jobs": bool(resurface_seen_jobs),
    }


def _resolve_ranking_context(upstream_result: dict[str, Any], scored_jobs: list[dict[str, Any]]) -> dict[str, Any]:
    fallback_modes = {"deterministic_fallback", "deterministic_broad_discovery"}
    scoring_modes = {
        str(row.get("scoring_mode") or "").strip().lower()
        for row in scored_jobs
        if str(row.get("scoring_mode") or "").strip()
    }
    ranking_mode = "unknown"
    if scoring_modes == {"deterministic_broad_discovery"}:
        ranking_mode = "deterministic_broad_discovery"
    if scoring_modes == {"deterministic_fallback"}:
        ranking_mode = "deterministic_fallback"
    elif scoring_modes.intersection(fallback_modes) and "llm_structured" in scoring_modes:
        ranking_mode = "mixed"
    elif "llm_structured" in scoring_modes:
        ranking_mode = "llm_structured"

    warnings = upstream_result.get("warnings") if isinstance(upstream_result.get("warnings"), list) else []
    llm_meta = {}
    jobs_scored_artifact = upstream_result.get("jobs_scored_artifact")
    if isinstance(jobs_scored_artifact, dict):
        llm_meta = jobs_scored_artifact.get("llm") if isinstance(jobs_scored_artifact.get("llm"), dict) else {}

    model_usage = upstream_result.get("model_usage") if isinstance(upstream_result.get("model_usage"), dict) else {}
    fallback_used = bool(llm_meta.get("fallback_used")) or ranking_mode in {"deterministic_fallback", "deterministic_broad_discovery", "mixed"}
    llm_failed = False
    if bool(model_usage.get("llm_runtime_enabled")) and fallback_used:
        llm_failed = True
    if any(str(item or "").startswith("llm_") for item in warnings):
        llm_failed = True

    if ranking_mode == "unknown":
        if fallback_used:
            ranking_mode = "deterministic_broad_discovery" if "deterministic_broad_discovery" in scoring_modes else "deterministic_fallback"
        elif bool(model_usage.get("llm_runtime_enabled")) and scored_jobs:
            ranking_mode = "llm_structured"

    return {
        "ranking_mode": ranking_mode,
        "fallback_used": fallback_used,
        "llm_failed": llm_failed,
        "scoring_modes": sorted(scoring_modes),
    }


def _resolve_fail_soft_policy(shortlist_policy: dict[str, Any], request: dict[str, Any]) -> dict[str, Any]:
    enabled = shortlist_policy.get("shortlist_fail_soft_enabled")
    if enabled is None:
        enabled = request.get("shortlist_fail_soft_enabled", True)
    enabled = bool(enabled)

    try:
        min_items = int(
            shortlist_policy.get("shortlist_fallback_min_items")
            if shortlist_policy.get("shortlist_fallback_min_items") is not None
            else request.get("shortlist_fallback_min_items", 3)
        )
    except (TypeError, ValueError):
        min_items = 3
    min_items = max(0, min(min_items, 20))

    raw_min_score = shortlist_policy.get("shortlist_fallback_min_score")
    if raw_min_score is None:
        raw_min_score = request.get("shortlist_fallback_min_score", 0.08)
    min_score_100 = resolve_min_score_100(raw_min_score)

    return {
        "enabled": enabled,
        "min_items": min_items,
        "min_score_100": round(min_score_100, 2),
        "raw_min_score": raw_min_score,
    }


def _annotate_jobs_with_history(
    scored_jobs: list[dict[str, Any]],
    history_by_key: dict[str, dict[str, Any]],
    *,
    now_utc: datetime,
    notification_cooldown_days: int,
) -> dict[str, Any]:
    observability = {
        "history_rows_loaded": len(history_by_key),
        "seen_before_count": 0,
        "previously_shortlisted_count": 0,
        "previously_notified_count": 0,
        "cooldown_suppressed_count": 0,
    }

    for row in scored_jobs:
        canonical_job_key = str(row.get("canonical_job_key") or "").strip()
        history = history_by_key.get(canonical_job_key) or {}
        times_seen = int(history.get("times_seen") or 0)
        times_shortlisted = int(history.get("times_shortlisted") or 0)
        times_notified = int(history.get("times_notified") or 0)
        previously_seen = times_seen > 0
        previously_shortlisted = times_shortlisted > 0
        previously_notified = times_notified > 0
        first_seen_at = _history_timestamp_text(history.get("first_seen_at"))
        last_seen_at = _history_timestamp_text(history.get("last_seen_at"))
        last_shortlisted_at = _history_timestamp_text(history.get("last_shortlisted_at"))
        last_notified_at = _history_timestamp_text(history.get("last_notified_at"))
        last_notified_dt = _parse_datetime(last_notified_at)
        cooldown_active = False
        cooldown_remaining_days = 0
        if previously_notified and notification_cooldown_days > 0 and last_notified_dt is not None:
            elapsed_days = max((now_utc - last_notified_dt).total_seconds() / 86400.0, 0.0)
            if elapsed_days < float(notification_cooldown_days):
                cooldown_active = True
                cooldown_remaining_days = int(max(math.ceil(notification_cooldown_days - elapsed_days), 0))

        if previously_seen:
            observability["seen_before_count"] += 1
        if previously_shortlisted:
            observability["previously_shortlisted_count"] += 1
        if previously_notified:
            observability["previously_notified_count"] += 1
        if cooldown_active:
            observability["cooldown_suppressed_count"] += 1

        row["newly_discovered"] = not previously_seen
        row["resurfaced_from_prior_runs"] = previously_seen
        row["previously_shortlisted"] = previously_shortlisted
        row["previously_notified"] = previously_notified
        row["suppressed_due_to_cooldown"] = cooldown_active
        row["history_first_seen_at"] = first_seen_at
        row["history_last_seen_at"] = last_seen_at
        row["history_last_shortlisted_at"] = last_shortlisted_at
        row["history_last_notified_at"] = last_notified_at
        row["history_times_seen"] = times_seen
        row["history_times_shortlisted"] = times_shortlisted
        row["history_times_notified"] = times_notified
        row["history_cooldown_remaining_days"] = cooldown_remaining_days
        row["historical_state"] = {
            "canonical_job_key": canonical_job_key,
            "first_seen_at": first_seen_at,
            "last_seen_at": last_seen_at,
            "times_seen": times_seen,
            "times_shortlisted": times_shortlisted,
            "times_notified": times_notified,
            "last_shortlisted_at": last_shortlisted_at,
            "last_notified_at": last_notified_at,
            "newly_discovered": not previously_seen,
            "resurfaced_from_prior_runs": previously_seen,
            "previously_shortlisted": previously_shortlisted,
            "previously_notified": previously_notified,
            "suppressed_due_to_cooldown": cooldown_active,
            "cooldown_remaining_days": cooldown_remaining_days,
        }
    return observability


def execute(task: Any, db: Any) -> dict[str, Any]:
    payload = payload_object(task.payload_json)
    upstream = payload.get("upstream") if isinstance(payload.get("upstream"), dict) else {}
    request = resolve_request(payload.get("request") if isinstance(payload.get("request"), dict) else payload)
    pipeline_id = new_pipeline_id(payload.get("pipeline_id"))

    upstream_result = fetch_upstream_result_content_json(db, upstream)
    upstream_type, scored_rows, pipeline_counts = _extract_scored_jobs(upstream_result)
    scored_jobs = normalize_scored_jobs(scored_rows)
    now_utc = datetime.now(timezone.utc)
    ranking_context = _resolve_ranking_context(upstream_result, scored_jobs)

    shortlist_policy = payload.get("shortlist_policy") if isinstance(payload.get("shortlist_policy"), dict) else {}
    try:
        max_items = int(shortlist_policy.get("max_items") or request.get("shortlist_max_items") or 10)
    except (TypeError, ValueError):
        max_items = 10
    max_items = max(1, min(max_items, 100))

    min_score_value = shortlist_policy.get("min_score")
    if min_score_value is None:
        min_score_value = request.get("shortlist_min_score")
    min_score_100 = resolve_min_score_100(min_score_value)

    try:
        per_source_cap = int(shortlist_policy.get("per_source_cap") or request.get("shortlist_per_source_cap") or 3)
    except (TypeError, ValueError):
        per_source_cap = 3
    per_source_cap = max(1, min(per_source_cap, 50))

    try:
        per_company_cap = int(shortlist_policy.get("per_company_cap") or 2)
    except (TypeError, ValueError):
        per_company_cap = 2
    per_company_cap = max(1, min(per_company_cap, 10))

    try:
        source_diversity_weight = float(shortlist_policy.get("source_diversity_weight") or 4.0)
    except (TypeError, ValueError):
        source_diversity_weight = 4.0
    source_diversity_weight = max(0.0, min(source_diversity_weight, 20.0))

    try:
        company_repetition_penalty = float(shortlist_policy.get("company_repetition_penalty") or 8.0)
    except (TypeError, ValueError):
        company_repetition_penalty = 8.0
    company_repetition_penalty = max(0.0, min(company_repetition_penalty, 30.0))

    try:
        near_duplicate_title_similarity_threshold = float(
            shortlist_policy.get("near_duplicate_title_similarity_threshold") or 0.82
        )
    except (TypeError, ValueError):
        near_duplicate_title_similarity_threshold = 0.82
    near_duplicate_title_similarity_threshold = max(0.5, min(near_duplicate_title_similarity_threshold, 1.0))

    freshness_weight_enabled = bool(shortlist_policy.get("freshness_weight_enabled", False))
    try:
        freshness_max_bonus = float(shortlist_policy.get("freshness_max_bonus") or 8.0)
    except (TypeError, ValueError):
        freshness_max_bonus = 8.0
    freshness_max_bonus = max(0.0, min(freshness_max_bonus, 20.0))

    history_policy = _resolve_history_policy(shortlist_policy, request)
    fail_soft_policy = _resolve_fail_soft_policy(shortlist_policy, request)
    history_observability = {
        "enabled": _db_supports_history(db),
        "history_rows_loaded": 0,
        "seen_before_count": 0,
        "previously_shortlisted_count": 0,
        "previously_notified_count": 0,
        "cooldown_suppressed_count": 0,
    }
    if _db_supports_history(db) and scored_jobs:
        history_by_key = load_jobs_history(
            db,
            [str(row.get("canonical_job_key") or "").strip() for row in scored_jobs],
        )
        history_observability = {
            "enabled": True,
            **_annotate_jobs_with_history(
                scored_jobs,
                history_by_key,
                now_utc=now_utc,
                notification_cooldown_days=int(history_policy["jobs_notification_cooldown_days"]),
            ),
        }

    shortlist_raw, rejected_summary, diagnostics = shortlist_jobs(
        scored_jobs,
        max_items=max_items,
        min_score_100=min_score_100,
        per_source_cap=per_source_cap,
        per_company_cap=per_company_cap,
        source_diversity_weight=source_diversity_weight,
        company_repetition_penalty=company_repetition_penalty,
        near_duplicate_title_similarity_threshold=near_duplicate_title_similarity_threshold,
        freshness_weight_enabled=freshness_weight_enabled,
        freshness_max_bonus=freshness_max_bonus,
        jobs_shortlist_repeat_penalty=float(history_policy["jobs_shortlist_repeat_penalty"]),
        jobs_notification_cooldown_days=int(history_policy["jobs_notification_cooldown_days"]),
        resurface_seen_jobs=bool(history_policy["resurface_seen_jobs"]),
        score_field="_base_score_100",
        now_utc=now_utc,
    )

    fail_soft_applied = False
    if fail_soft_policy["enabled"] and ranking_context["fallback_used"] and scored_jobs:
        target_size = min(max_items, max(int(fail_soft_policy["min_items"]), len(shortlist_raw)))
        if target_size > len(shortlist_raw):
            fail_soft_shortlist_raw, fail_soft_rejected_summary, fail_soft_diagnostics = shortlist_jobs(
                scored_jobs,
                max_items=target_size,
                min_score_100=float(fail_soft_policy["min_score_100"]),
                per_source_cap=per_source_cap,
                per_company_cap=per_company_cap,
                source_diversity_weight=source_diversity_weight,
                company_repetition_penalty=company_repetition_penalty,
                near_duplicate_title_similarity_threshold=near_duplicate_title_similarity_threshold,
                freshness_weight_enabled=freshness_weight_enabled,
                freshness_max_bonus=freshness_max_bonus,
                jobs_shortlist_repeat_penalty=float(history_policy["jobs_shortlist_repeat_penalty"]),
                jobs_notification_cooldown_days=int(history_policy["jobs_notification_cooldown_days"]),
                resurface_seen_jobs=bool(history_policy["resurface_seen_jobs"]),
                score_field="_fallback_score_100",
                now_utc=now_utc,
            )
            if len(fail_soft_shortlist_raw) > len(shortlist_raw):
                fail_soft_applied = True
                standard_job_ids = {
                    str(row.get("job_id") or "").strip()
                    for row in shortlist_raw
                    if str(row.get("job_id") or "").strip()
                }
                shortlist_raw = fail_soft_shortlist_raw
                rejected_summary = fail_soft_rejected_summary
                diagnostics = fail_soft_diagnostics
                diagnostics["standard_selected_size"] = len(standard_job_ids)
                diagnostics["fail_soft_min_score_100"] = float(fail_soft_policy["min_score_100"])
                diagnostics["fail_soft_target_size"] = target_size
                for row in shortlist_raw:
                    job_id = str(row.get("job_id") or "").strip()
                    row["fail_soft_selected"] = job_id not in standard_job_ids
                    row["selection_basis"] = "fail_soft" if row["fail_soft_selected"] else "standard_threshold"

    for row in shortlist_raw:
        if "fail_soft_selected" not in row:
            row["fail_soft_selected"] = False
        if "selection_basis" not in row:
            row["selection_basis"] = "standard_threshold"

    shortlist_confidence = "low" if ranking_context["fallback_used"] else "normal"
    if fail_soft_applied:
        shortlist_confidence = "low"
    diagnostics["fail_soft_applied"] = fail_soft_applied
    diagnostics["ranking_mode"] = ranking_context["ranking_mode"]
    diagnostics["shortlist_confidence"] = shortlist_confidence

    shortlist: list[dict[str, Any]] = []
    for row in shortlist_raw:
        item = {
            key: value
            for key, value in row.items()
            if not key.startswith("_")
        }
        shortlist.append(item)

    if _db_supports_history(db):
        if scored_jobs:
            record_jobs_seen(db, scored_jobs, seen_at=now_utc)
        if shortlist:
            record_jobs_shortlisted(db, shortlist, shortlisted_at=now_utc)
        db.commit()

    history_observability.update(
        {
            "selected_newly_discovered_count": sum(1 for row in shortlist if row.get("newly_discovered") is True),
            "selected_resurfaced_count": sum(1 for row in shortlist if row.get("resurfaced_from_prior_runs") is True),
            "selected_previously_shortlisted_count": sum(
                1 for row in shortlist if row.get("previously_shortlisted") is True
            ),
            "selected_previously_notified_count": sum(
                1 for row in shortlist if row.get("previously_notified") is True
            ),
        }
    )

    shortlist_summary_metadata = {
        "requested_size": max_items,
        "selected_size": len(shortlist),
        "input_scored_count": len(scored_jobs),
        "min_score_100": round(min_score_100, 2),
        "ranking_mode": ranking_context["ranking_mode"],
        "fallback_used": bool(ranking_context["fallback_used"]),
        "shortlist_confidence": shortlist_confidence,
        "fail_soft_applied": fail_soft_applied,
        "fail_soft_min_score_100": float(fail_soft_policy["min_score_100"]),
        "fail_soft_target_items": int(fail_soft_policy["min_items"]),
        "upstream_artifact_type": upstream_type,
        "pipeline_counts": {
            "collected_count": pipeline_counts.get("collected_count"),
            "normalized_count": pipeline_counts.get("normalized_count"),
            "deduped_count": pipeline_counts.get("deduped_count"),
            "duplicates_collapsed": pipeline_counts.get("duplicates_collapsed"),
            "scored_count": pipeline_counts.get("scored_count") or len(scored_jobs),
            "shortlisted_count": len(shortlist),
        },
    }

    anti_repetition_summary = {
        "enabled": True,
        "constraints": {
            "per_source_cap": per_source_cap,
            "per_company_cap": per_company_cap,
            "near_duplicate_title_similarity_threshold": near_duplicate_title_similarity_threshold,
            "source_diversity_weight": source_diversity_weight,
            "company_repetition_penalty": company_repetition_penalty,
            "freshness_weight_enabled": freshness_weight_enabled,
            "freshness_max_bonus": freshness_max_bonus,
            "jobs_notification_cooldown_days": int(history_policy["jobs_notification_cooldown_days"]),
            "jobs_shortlist_repeat_penalty": float(history_policy["jobs_shortlist_repeat_penalty"]),
            "resurface_seen_jobs": bool(history_policy["resurface_seen_jobs"]),
            "shortlist_fail_soft_enabled": bool(fail_soft_policy["enabled"]),
            "shortlist_fallback_min_items": int(fail_soft_policy["min_items"]),
            "shortlist_fallback_min_score_100": float(fail_soft_policy["min_score_100"]),
        },
        "rejected_summary": rejected_summary,
    }

    jobs_top_artifact = {
        "artifact_type": "jobs_top.v1",
        "artifact_schema": "jobs_top.v1",
        "pipeline_id": pipeline_id,
        "shortlisted_at": utc_iso(),
        "request": request,
        "shortlist_policy": {
            "max_items": max_items,
            "min_score_100": round(min_score_100, 2),
            "per_source_cap": per_source_cap,
            "per_company_cap": per_company_cap,
            "source_diversity_weight": source_diversity_weight,
            "company_repetition_penalty": company_repetition_penalty,
            "near_duplicate_title_similarity_threshold": near_duplicate_title_similarity_threshold,
            "freshness_weight_enabled": freshness_weight_enabled,
            "freshness_max_bonus": freshness_max_bonus,
            "jobs_notification_cooldown_days": int(history_policy["jobs_notification_cooldown_days"]),
            "jobs_shortlist_repeat_penalty": float(history_policy["jobs_shortlist_repeat_penalty"]),
            "resurface_seen_jobs": bool(history_policy["resurface_seen_jobs"]),
            "shortlist_fail_soft_enabled": bool(fail_soft_policy["enabled"]),
            "shortlist_fallback_min_items": int(fail_soft_policy["min_items"]),
            "shortlist_fallback_min_score_100": float(fail_soft_policy["min_score_100"]),
        },
        "top_jobs": shortlist,
        "summary": {
            **shortlist_summary_metadata,
            "rejected_summary": rejected_summary,
        },
        "pipeline_counts": shortlist_summary_metadata["pipeline_counts"],
        "anti_repetition_summary": anti_repetition_summary,
        "ranking_mode": ranking_context["ranking_mode"],
        "fallback_used": bool(ranking_context["fallback_used"]),
        "llm_failed": bool(ranking_context["llm_failed"]),
        "shortlist_confidence": shortlist_confidence,
        "fail_soft_applied": fail_soft_applied,
        "history_observability": history_observability,
        "upstream": upstream,
    }

    # Structured seed payloads for downstream notifications and application drafting.
    action_seed = {
        "cover_letter": {
            "status": "not_started",
            "jobs": [
                {
                    "job_id": row.get("job_id"),
                    "title": row.get("title"),
                    "company": row.get("company"),
                    "url": row.get("url"),
                    "explanation_summary": row.get("explanation_summary"),
                }
                for row in shortlist[:5]
            ],
        },
        "application_draft": {
            "status": "not_started",
            "jobs": [
                {
                    "job_id": row.get("job_id"),
                    "title": row.get("title"),
                    "company": row.get("company"),
                    "source": row.get("source"),
                    "source_url": row.get("source_url"),
                }
                for row in shortlist[:5]
            ],
        },
        "interview_prep": {
            "status": "not_started",
            "jobs": [
                {
                    "job_id": row.get("job_id"),
                    "title": row.get("title"),
                    "company": row.get("company"),
                    "fit_tier": row.get("fit_tier"),
                }
                for row in shortlist[:5]
            ],
        },
        "follow_up": {
            "status": "not_started",
            "jobs": [
                {
                    "job_id": row.get("job_id"),
                    "title": row.get("title"),
                    "company": row.get("company"),
                    "url": row.get("url"),
                }
                for row in shortlist[:5]
            ],
        },
    }

    notification_candidates = [
        {
            "job_id": row.get("job_id"),
            "title": row.get("title"),
            "company": row.get("company"),
            "score": row.get("score"),
            "overall_score": row.get("overall_score"),
            "source": row.get("source"),
            "url": row.get("url"),
            "summary": row.get("explanation_summary"),
        }
        for row in shortlist[:10]
    ]

    artifact = {
        "artifact_type": "jobs.shortlist.v1",
        "artifact_schema": "jobs.shortlist.v2",
        "pipeline_id": pipeline_id,
        "shortlisted_at": utc_iso(),
        "request": request,
        "shortlist_policy": jobs_top_artifact["shortlist_policy"],
        "ranking_mode": ranking_context["ranking_mode"],
        "fallback_used": bool(ranking_context["fallback_used"]),
        "llm_failed": bool(ranking_context["llm_failed"]),
        "shortlist_confidence": shortlist_confidence,
        "fail_soft_applied": fail_soft_applied,
        "shortlist": shortlist,
        "shortlist_count": len(shortlist),
        "jobs_top_artifact": jobs_top_artifact,
        "pipeline_counts": shortlist_summary_metadata["pipeline_counts"],
        "shortlist_summary_metadata": shortlist_summary_metadata,
        "anti_repetition_summary": anti_repetition_summary,
        "history_policy": history_policy,
        "history_observability": history_observability,
        "rejected_summary": rejected_summary,
        "selection_diagnostics": diagnostics,
        "selection_reasons": [
            {
                "job_id": row.get("job_id"),
                "canonical_job_key": row.get("canonical_job_key"),
                "title": row.get("title"),
                "company": row.get("company"),
                "source": row.get("source"),
                "score": row.get("score"),
                "overall_score": row.get("overall_score"),
                "fit_tier": row.get("fit_tier"),
                "explanation_summary": row.get("explanation_summary"),
                "location_match_reason": row.get("location_match_reason"),
                "recency_match_reason": row.get("recency_match_reason"),
                "posted_age_days": row.get("posted_age_days"),
                "stale_rejected": bool(row.get("stale_rejected")),
                "selection_basis": row.get("selection_basis"),
                "fail_soft_selected": row.get("fail_soft_selected"),
                "newly_discovered": row.get("newly_discovered"),
                "resurfaced_from_prior_runs": row.get("resurfaced_from_prior_runs"),
                "previously_shortlisted": row.get("previously_shortlisted"),
                "previously_notified": row.get("previously_notified"),
                "suppressed_due_to_cooldown": row.get("suppressed_due_to_cooldown"),
            }
            for row in shortlist
        ],
        "notification_candidates": notification_candidates,
        "action_seed": action_seed,
        "upstream": upstream,
    }

    next_upstream = build_upstream_ref(task, "jobs_shortlist_v1")
    upstream_run_id = next_upstream.get("run_id") or str(getattr(task, "id", ""))
    next_payload = {
        "pipeline_id": pipeline_id,
        "upstream": next_upstream,
        "request": request,
        "digest_policy": payload.get("digest_policy") if isinstance(payload.get("digest_policy"), dict) else {
            "max_items": max_items,
            "format": str(request.get("digest_format") or "compact"),
            "notify_channels": request.get("notify_channels") or ["discord"],
            "notify_on_empty": bool(request.get("notify_on_empty", False)),
            "llm_enabled": bool(request.get("digest_llm_enabled", True)),
        },
    }

    return {
        "artifact_type": "jobs.shortlist.v1",
        "content_text": (
            f"Shortlisted {len(shortlist)} jobs from {len(scored_jobs)} scored candidates "
            f"(target={max_items}, min_score_100={round(min_score_100, 2)})."
        ),
        "content_json": artifact,
        "debug_json": {
            "pipeline_id": pipeline_id,
            "selected_size": len(shortlist),
            "input_scored_count": len(scored_jobs),
            "upstream_artifact_type": upstream_type,
            "ranking_mode": ranking_context["ranking_mode"],
            "fallback_used": bool(ranking_context["fallback_used"]),
            "llm_failed": bool(ranking_context["llm_failed"]),
            "shortlist_confidence": shortlist_confidence,
            "fail_soft_applied": fail_soft_applied,
        },
        "next_tasks": [
            {
                "task_type": "jobs_digest_v2",
                "payload_json": next_payload,
                "idempotency_key": stage_idempotency_key(pipeline_id, "jobs_digest_v2", upstream_run_id),
                "max_attempts": 3,
            }
        ],
    }
