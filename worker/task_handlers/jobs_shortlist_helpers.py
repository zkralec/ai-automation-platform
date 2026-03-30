from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any

from task_handlers.jobs_normalize_helpers import (
    canonical_job_key as build_canonical_job_key,
    metadata_quality_details,
    resolve_posted_age_days,
)


def _canonical_text(value: Any) -> str:
    text = str(value or "").strip().lower()
    text = "".join(ch if ch.isalnum() or ch.isspace() else " " for ch in text)
    return " ".join(text.split())


def _token_set(value: str) -> set[str]:
    return {token for token in _canonical_text(value).split() if token}


def _title_similarity(left: str, right: str) -> float:
    left_tokens = _token_set(left)
    right_tokens = _token_set(right)
    if not left_tokens or not right_tokens:
        return 0.0
    inter = left_tokens.intersection(right_tokens)
    union = left_tokens.union(right_tokens)
    if not union:
        return 0.0
    return len(inter) / float(len(union))


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        text = value.strip().replace(",", "")
        if text.startswith("$"):
            text = text[1:]
        if not text:
            return None
        try:
            return float(text)
        except ValueError:
            return None
    return None


def _parse_datetime(value: Any) -> datetime | None:
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


def _history_recency_factor(value: Any, *, now_utc: datetime | None = None) -> float:
    parsed = _parse_datetime(value)
    if parsed is None:
        return 0.35
    now = now_utc or datetime.now(timezone.utc)
    delta_days = max((now - parsed).total_seconds() / 86400.0, 0.0)
    if delta_days <= 7.0:
        return 1.0
    if delta_days <= 30.0:
        return 0.65
    if delta_days <= 90.0:
        return 0.35
    return 0.2


def _freshness_factor(posted_at: Any, *, posted_age_days: Any = None, posted_at_raw: Any = None, now_utc: datetime | None = None) -> float:
    age_days = resolve_posted_age_days(
        posted_age_days=posted_age_days,
        posted_at=posted_at,
        posted_at_raw=posted_at_raw,
        reference_time=now_utc or datetime.now(timezone.utc),
    )
    if age_days is None:
        return 0.35
    # Fast early decay, then gradual flattening.
    return max(0.0, min(math.exp(-float(age_days) / 21.0), 1.0))


def _structured_match_adjustment(row: dict[str, Any]) -> float:
    adjustment = 0.0
    location_reason = str(row.get("location_match_reason") or "").strip().lower()
    recency_reason = str(row.get("recency_match_reason") or "").strip().lower()
    if location_reason == "exact_location_match":
        adjustment += 1.5
    elif location_reason in {"near_location_match", "remote_match_outside_target_geo", "work_mode_match"}:
        adjustment += 0.75
    elif location_reason == "location_unknown":
        adjustment -= 1.75
    elif location_reason in {"location_mismatch", "remote_hybrid_mismatch", "work_mode_unknown"}:
        adjustment -= 2.75

    if bool(row.get("stale_rejected")):
        adjustment -= 3.0
    elif recency_reason == "recent_match":
        adjustment += 0.75
    elif recency_reason == "missing_posted_age":
        adjustment -= 1.25
    elif recency_reason in {"older_than_recent_window", "stale_30_plus_days"}:
        adjustment -= 2.0
    return round(max(-5.0, min(adjustment, 3.0)), 4)


def _score_100(row: dict[str, Any]) -> float:
    adjusted = _as_float(row.get("overall_score_adjusted"))
    if adjusted is not None:
        return max(0.0, min(adjusted, 100.0))

    direct = _as_float(row.get("overall_score"))
    if direct is not None:
        return max(0.0, min(direct, 100.0))

    scaled = _as_float(row.get("score"))
    if scaled is not None:
        if scaled <= 2.5:
            return max(0.0, min(scaled * 50.0, 100.0))
        return max(0.0, min(scaled, 100.0))
    return 0.0


def _fallback_score_100(row: dict[str, Any]) -> float:
    direct = _as_float(row.get("overall_score"))
    scaled = _as_float(row.get("score"))
    scaled_100 = 0.0
    if scaled is not None:
        if scaled <= 2.5:
            scaled_100 = max(0.0, min(scaled * 50.0, 100.0))
        else:
            scaled_100 = max(0.0, min(scaled, 100.0))
    if direct is None:
        return scaled_100
    return max(0.0, min(max(direct, scaled_100), 100.0))


def _metadata_quality(row: dict[str, Any]) -> dict[str, Any]:
    details = metadata_quality_details(row)
    if isinstance(row.get("metadata_quality_score"), (int, float)):
        details["metadata_quality_score"] = max(0.0, min(float(row.get("metadata_quality_score")), 100.0))
    for key in (
        "missing_company",
        "missing_source_url",
        "missing_posted_at",
        "missing_location",
        "has_direct_source_url",
    ):
        if isinstance(row.get(key), bool):
            details[key] = bool(row.get(key))
    return details


def _metadata_quality_adjustment(row: dict[str, Any]) -> tuple[dict[str, Any], float]:
    details = _metadata_quality(row)
    score = float(details.get("metadata_quality_score") or 0.0)

    adjustment = 0.0
    if score >= 85.0:
        adjustment += min((score - 85.0) * 0.08, 1.2)
    elif score < 60.0:
        adjustment -= min((60.0 - score) * 0.08, 3.0)

    if details.get("has_direct_source_url"):
        adjustment += 0.75
    elif details.get("missing_source_url"):
        adjustment -= 1.5
    if details.get("missing_company"):
        adjustment -= 1.5
    if details.get("missing_posted_at"):
        adjustment -= 1.0
    if details.get("missing_location"):
        adjustment -= 0.75

    return details, round(max(-6.0, min(adjustment, 3.0)), 4)


def normalize_scored_jobs(rows: Any) -> list[dict[str, Any]]:
    if not isinstance(rows, list):
        return []
    output: list[dict[str, Any]] = []
    for idx, raw in enumerate(rows, start=1):
        if not isinstance(raw, dict):
            continue
        title = str(raw.get("title") or "").strip()
        if not title:
            continue
        source = str(raw.get("source") or "unknown").strip().lower() or "unknown"
        company = str(raw.get("company") or "").strip()
        duplicate_group_id = str(raw.get("duplicate_group_id") or "").strip() or None
        duplicate_count = int(raw.get("duplicate_count") or 1)
        duplicate_count = max(1, duplicate_count)

        item = dict(raw)
        item["job_id"] = str(raw.get("job_id") or raw.get("normalized_job_id") or f"short-{idx:06d}").strip()
        item["source"] = source
        item["company"] = company
        item["title"] = title
        item["canonical_job_key"] = str(raw.get("canonical_job_key") or build_canonical_job_key(raw) or item["job_id"]).strip()
        item["duplicate_group_id"] = duplicate_group_id
        item["duplicate_count"] = duplicate_count
        item["_base_score_100"] = _score_100(raw)
        item["_fallback_score_100"] = _fallback_score_100(raw)
        item["_company_key"] = _canonical_text(company) or "_unknown_company"
        item["_title_key"] = _canonical_text(title)
        item["_source_key"] = source
        item["_freshness_factor"] = _freshness_factor(
            raw.get("posted_at_normalized") or raw.get("posted_at"),
            posted_age_days=raw.get("posted_age_days"),
            posted_at_raw=raw.get("posted_at_raw"),
        )
        quality_details, quality_adjustment = _metadata_quality_adjustment(raw)
        item["metadata_quality_score"] = float(quality_details.get("metadata_quality_score") or 0.0)
        item["missing_company"] = bool(quality_details.get("missing_company"))
        item["missing_source_url"] = bool(quality_details.get("missing_source_url"))
        item["missing_posted_at"] = bool(quality_details.get("missing_posted_at"))
        item["missing_location"] = bool(quality_details.get("missing_location"))
        item["has_direct_source_url"] = bool(quality_details.get("has_direct_source_url"))
        item["_metadata_quality_adjustment"] = quality_adjustment
        item["_structured_match_adjustment"] = _structured_match_adjustment(raw)
        output.append(item)
    return output


def resolve_min_score_100(raw_min_score: Any) -> float:
    parsed = _as_float(raw_min_score)
    if parsed is None:
        return 37.5  # legacy 0.75 on 0..2 scale
    if parsed <= 2.5:
        return max(0.0, min(parsed * 50.0, 100.0))
    return max(0.0, min(parsed, 100.0))


def shortlist_jobs(
    scored_jobs: list[dict[str, Any]],
    *,
    max_items: int,
    min_score_100: float,
    per_source_cap: int,
    per_company_cap: int,
    source_diversity_weight: float,
    company_repetition_penalty: float,
    near_duplicate_title_similarity_threshold: float,
    freshness_weight_enabled: bool,
    freshness_max_bonus: float,
    jobs_shortlist_repeat_penalty: float = 0.0,
    jobs_notification_cooldown_days: int = 0,
    resurface_seen_jobs: bool = True,
    score_field: str = "_base_score_100",
    now_utc: datetime | None = None,
) -> tuple[list[dict[str, Any]], dict[str, int], dict[str, Any]]:
    remaining = scored_jobs[:]
    shortlist: list[dict[str, Any]] = []
    source_counts: dict[str, int] = {}
    company_counts: dict[str, int] = {}
    selected_duplicate_groups: set[str] = set()
    now = now_utc or datetime.now(timezone.utc)

    rejected_summary = {
        "below_min_score": 0,
        "notification_cooldown": 0,
        "resurface_disabled": 0,
        "per_source_cap": 0,
        "per_company_cap": 0,
        "duplicate_group_repeat": 0,
        "near_duplicate_company_title": 0,
        "max_items": 0,
    }

    diagnostics: dict[str, Any] = {
        "iterations": 0,
        "picked_job_ids": [],
        "weights": {
            "source_diversity_weight": source_diversity_weight,
            "company_repetition_penalty": company_repetition_penalty,
            "freshness_weight_enabled": freshness_weight_enabled,
            "freshness_max_bonus": freshness_max_bonus,
            "jobs_shortlist_repeat_penalty": jobs_shortlist_repeat_penalty,
            "jobs_notification_cooldown_days": jobs_notification_cooldown_days,
            "resurface_seen_jobs": resurface_seen_jobs,
        },
        "score_field": score_field,
        "cooldown_suppressed_job_ids": [],
    }

    while remaining and len(shortlist) < max_items:
        diagnostics["iterations"] += 1
        best_idx = -1
        best_effective = float("-inf")
        best_row: dict[str, Any] | None = None

        for idx, row in enumerate(remaining):
            base = float(row.get(score_field) or 0.0)
            source_key = str(row.get("_source_key") or "unknown")
            company_key = str(row.get("_company_key") or "_unknown_company")
            duplicate_group_id = row.get("duplicate_group_id")

            if base < min_score_100:
                continue
            if bool(row.get("suppressed_due_to_cooldown")):
                continue
            if (
                not resurface_seen_jobs
                and bool(row.get("resurfaced_from_prior_runs"))
                and not bool(row.get("previously_shortlisted"))
                and not bool(row.get("previously_notified"))
            ):
                continue
            if source_counts.get(source_key, 0) >= per_source_cap:
                continue
            if company_counts.get(company_key, 0) >= per_company_cap:
                continue
            if isinstance(duplicate_group_id, str) and duplicate_group_id and duplicate_group_id in selected_duplicate_groups:
                continue

            source_penalty = float(source_counts.get(source_key, 0)) * source_diversity_weight
            company_penalty = float(company_counts.get(company_key, 0)) * company_repetition_penalty
            freshness_bonus = float(row.get("_freshness_factor") or 0.0) * freshness_max_bonus if freshness_weight_enabled else 0.0
            metadata_quality_adjustment = float(row.get("_metadata_quality_adjustment") or 0.0)
            structured_match_adjustment = float(row.get("_structured_match_adjustment") or 0.0)
            repeat_penalty = 0.0
            if bool(row.get("previously_shortlisted")):
                repeat_penalty += jobs_shortlist_repeat_penalty * _history_recency_factor(
                    row.get("history_last_shortlisted_at"),
                    now_utc=now,
                )
            elif bool(row.get("previously_notified")):
                repeat_penalty += (jobs_shortlist_repeat_penalty * 0.5) * _history_recency_factor(
                    row.get("history_last_notified_at"),
                    now_utc=now,
                )

            near_duplicate_penalty = 0.0
            title_key = str(row.get("_title_key") or "")
            for picked in shortlist:
                if str(picked.get("_company_key") or "") != company_key:
                    continue
                similarity = _title_similarity(title_key, str(picked.get("_title_key") or ""))
                if similarity >= near_duplicate_title_similarity_threshold:
                    near_duplicate_penalty = max(near_duplicate_penalty, 10.0 + similarity * 10.0)

            effective = (
                base
                + freshness_bonus
                + metadata_quality_adjustment
                + structured_match_adjustment
                - source_penalty
                - company_penalty
                - repeat_penalty
                - near_duplicate_penalty
            )
            if effective > best_effective:
                best_effective = effective
                best_idx = idx
                best_row = row
                row["_historical_repeat_penalty"] = round(repeat_penalty, 4)

        if best_idx < 0 or best_row is None:
            break

        picked = remaining.pop(best_idx)
        source_key = str(picked.get("_source_key") or "unknown")
        company_key = str(picked.get("_company_key") or "_unknown_company")
        source_counts[source_key] = source_counts.get(source_key, 0) + 1
        company_counts[company_key] = company_counts.get(company_key, 0) + 1
        duplicate_group_id = picked.get("duplicate_group_id")
        if isinstance(duplicate_group_id, str) and duplicate_group_id:
            selected_duplicate_groups.add(duplicate_group_id)

        picked["_shortlist_effective_score"] = round(best_effective, 4)
        shortlist.append(picked)
        diagnostics["picked_job_ids"].append(picked.get("job_id"))

    for row in remaining:
        base = float(row.get(score_field) or 0.0)
        source_key = str(row.get("_source_key") or "unknown")
        company_key = str(row.get("_company_key") or "_unknown_company")
        duplicate_group_id = row.get("duplicate_group_id")
        title_key = str(row.get("_title_key") or "")

        if base < min_score_100:
            rejected_summary["below_min_score"] += 1
            continue
        if bool(row.get("suppressed_due_to_cooldown")):
            rejected_summary["notification_cooldown"] += 1
            diagnostics["cooldown_suppressed_job_ids"].append(row.get("job_id"))
            continue
        if (
            not resurface_seen_jobs
            and bool(row.get("resurfaced_from_prior_runs"))
            and not bool(row.get("previously_shortlisted"))
            and not bool(row.get("previously_notified"))
        ):
            rejected_summary["resurface_disabled"] += 1
            continue
        if source_counts.get(source_key, 0) >= per_source_cap:
            rejected_summary["per_source_cap"] += 1
            continue
        if company_counts.get(company_key, 0) >= per_company_cap:
            rejected_summary["per_company_cap"] += 1
            continue
        if isinstance(duplicate_group_id, str) and duplicate_group_id and duplicate_group_id in selected_duplicate_groups:
            rejected_summary["duplicate_group_repeat"] += 1
            continue

        near_duplicate = False
        for picked in shortlist:
            if str(picked.get("_company_key") or "") != company_key:
                continue
            similarity = _title_similarity(title_key, str(picked.get("_title_key") or ""))
            if similarity >= near_duplicate_title_similarity_threshold:
                near_duplicate = True
                break
        if near_duplicate:
            rejected_summary["near_duplicate_company_title"] += 1
            continue
        if len(shortlist) >= max_items:
            rejected_summary["max_items"] += 1
            continue
        rejected_summary["max_items"] += 1

    return shortlist, rejected_summary, diagnostics
