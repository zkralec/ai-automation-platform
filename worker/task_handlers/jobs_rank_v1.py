from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from jsonschema import Draft7Validator

from llm.openai_adapter import run_chat_completion
from models.catalog import get_model_info, get_model_price, tier_model
from task_handlers.jobs_normalize_helpers import metadata_quality_details
from task_handlers.jobs_pipeline_common import (
    build_upstream_ref,
    deterministic_job_signals,
    expect_artifact_type,
    fetch_upstream_result_content_json,
    fit_tier,
    is_broad_discovery_request,
    matches_filters,
    new_pipeline_id,
    payload_object,
    resolve_profile_context,
    resolve_request,
    score_job,
    stage_idempotency_key,
    utc_iso,
)
from task_handlers.prompts.jobs_rank_v1 import (
    RANK_PROMPT_VERSION,
    SCORING_OUTPUT_SCHEMA,
    build_scoring_messages,
)

MAX_LLM_JOBS = 200
DEFAULT_BATCH_SIZE = 12
MAX_BATCH_SIZE = 25
DEFAULT_LLM_MAX_RETRIES = 3
MAX_LLM_MAX_RETRIES = 5
DEFAULT_LLM_RETRY_COST_CAP_USD = Decimal("0.00300000")
STRICT_LLM_RETRY_COST_CAP_USD = Decimal("0.00600000")
MAX_LLM_RETRY_COST_CAP_USD = Decimal("0.10000000")
_SCORING_OUTPUT_VALIDATOR = Draft7Validator(SCORING_OUTPUT_SCHEMA)
_RESUME_OVERLAP_STOPWORDS = {
    "with",
    "from",
    "that",
    "this",
    "into",
    "your",
    "their",
    "have",
    "using",
    "build",
    "built",
    "role",
    "team",
    "work",
    "years",
    "year",
}


def _round(value: float, ndigits: int = 4) -> float:
    return round(float(value), ndigits)


def _bounded_score(value: Any) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        parsed = 0.0
    return max(0.0, min(parsed, 100.0))


def _as_non_negative_int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    if parsed < 0:
        return None
    return parsed


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
    posted = _parse_datetime(posted_at)
    if posted is None:
        return None
    now = now_utc or datetime.now(timezone.utc)
    delta = now - posted
    return max(int(delta.total_seconds() // 86400), 0)


def _prefer_recent_enabled(request: dict[str, Any]) -> bool:
    if bool(request.get("prefer_recent")):
        return True
    freshness = str(request.get("shortlist_freshness_preference") or "").strip().lower()
    return freshness in {"prefer_recent", "strong_prefer_recent"}


def _recency_details(
    row: dict[str, Any],
    *,
    request: dict[str, Any],
    now_utc: datetime | None = None,
) -> dict[str, Any]:
    age_days = _job_age_days(row.get("posted_at"), now_utc=now_utc)
    prefer_recent = _prefer_recent_enabled(request)
    if age_days is None:
        return {
            "age_days": None,
            "recency_score": 0.0,
            "missing_posted_at_penalty": 20.0,
            "old_job_penalty": 0.0,
        }
    return {
        "age_days": age_days,
        "recency_score": _bounded_score(max(0.0, 100.0 - age_days * 5.0)),
        "missing_posted_at_penalty": 0.0,
        "old_job_penalty": float(age_days * 2) if prefer_recent and age_days > 14 else 0.0,
    }


def _short_summary(text: Any, max_chars: int = 180) -> str:
    raw = str(text or "").strip()
    if not raw:
        return "Limited signal. Used conservative ranking."
    compact = " ".join(raw.split())
    if len(compact) <= max_chars:
        return compact
    return compact[: max_chars - 1].rstrip() + "…"


def _canonical_text(value: Any) -> str:
    text = str(value or "").strip().lower()
    text = "".join(ch if ch.isalnum() or ch.isspace() else " " for ch in text)
    return " ".join(text.split())


def _metadata_quality(row: dict[str, Any]) -> dict[str, Any]:
    computed = metadata_quality_details(row)
    details = dict(computed)
    if isinstance(row.get("metadata_quality_score"), (int, float)):
        details["metadata_quality_score"] = _bounded_score(row.get("metadata_quality_score"))
    for key in (
        "missing_company",
        "missing_source_url",
        "missing_posted_at",
        "missing_location",
        "has_direct_source_url",
    ):
        if isinstance(row.get(key), bool):
            details[key] = bool(row.get(key))
    source_url_kind = row.get("source_url_kind")
    if isinstance(source_url_kind, str) and source_url_kind.strip():
        details["source_url_kind"] = source_url_kind.strip().lower()
    return details


def _metadata_quality_penalty(row: dict[str, Any]) -> tuple[dict[str, Any], float]:
    details = _metadata_quality(row)
    score = float(details.get("metadata_quality_score") or 0.0)
    penalty = max(0.0, (100.0 - score) * 0.06)
    if details.get("missing_company"):
        penalty += 3.0
    if details.get("missing_source_url"):
        penalty += 2.5
    elif not details.get("has_direct_source_url"):
        penalty += 0.75
    if details.get("missing_posted_at"):
        penalty += 1.5
    if details.get("missing_location"):
        penalty += 1.0
    if not str(row.get("work_mode") or row.get("remote_type") or "").strip():
        penalty += 0.75
    return details, _round(min(max(penalty, 0.0), 15.0), 2)


def _pre_llm_priority(job: dict[str, Any], request: dict[str, Any]) -> tuple[float, float, float]:
    details, penalty = _metadata_quality_penalty(job)
    deterministic = float(score_job(job, request))
    quality_score = float(details.get("metadata_quality_score") or 0.0)
    return (
        _round(deterministic - (penalty / 100.0), 4),
        quality_score,
        float(job.get("salary_max") or job.get("salary_min") or 0.0),
    )


def _token_set(value: Any) -> set[str]:
    return {
        token
        for token in _canonical_text(value).split()
        if len(token) >= 3 and token not in _RESUME_OVERLAP_STOPWORDS and not token.isdigit()
    }


def _resume_overlap_ratio(job: dict[str, Any], profile_context: dict[str, Any]) -> float:
    resume_text = profile_context.get("resume_text")
    if not isinstance(resume_text, str) or not resume_text.strip():
        return 0.0
    resume_tokens = _token_set(resume_text)
    if not resume_tokens:
        return 0.0
    job_tokens = _token_set(" ".join(str(job.get(key) or "") for key in ("title", "description_snippet", "company")))
    if not job_tokens:
        return 0.0
    overlap = len(job_tokens.intersection(resume_tokens))
    denominator = max(min(len(job_tokens), 12), 1)
    return min(overlap / float(denominator), 1.0)


def _llm_runtime_enabled() -> bool:
    return os.getenv("USE_LLM", "false").strip().lower() == "true"


def _canonical_error_code(exc: Exception) -> str:
    message = str(exc or "").strip()
    if not message:
        return type(exc).__name__.lower()
    if ":" in message:
        return message.split(":", 1)[0].strip().lower()
    return message.replace(" ", "_").lower()


def _resolve_retry_cost_cap(raw_value: Any, *, strict_llm_output: bool) -> Decimal:
    env_value = os.getenv("JOBS_RANK_LLM_RETRY_COST_CAP_USD_DEFAULT")
    if raw_value is None and env_value is not None:
        raw_value = env_value
    if raw_value is None:
        return STRICT_LLM_RETRY_COST_CAP_USD if strict_llm_output else DEFAULT_LLM_RETRY_COST_CAP_USD
    try:
        parsed = Decimal(str(raw_value))
    except Exception:
        return STRICT_LLM_RETRY_COST_CAP_USD if strict_llm_output else DEFAULT_LLM_RETRY_COST_CAP_USD
    if parsed <= 0:
        return STRICT_LLM_RETRY_COST_CAP_USD if strict_llm_output else DEFAULT_LLM_RETRY_COST_CAP_USD
    return min(parsed.quantize(Decimal("0.00000001")), MAX_LLM_RETRY_COST_CAP_USD)


def _is_repetitive_score_pattern(score_map: dict[str, dict[str, Any]]) -> bool:
    if len(score_map) < 3:
        return False
    numeric_keys = (
        "resume_match_score",
        "title_match_score",
        "salary_score",
        "location_score",
        "seniority_score",
        "overall_score",
    )
    numeric_signatures: set[tuple[float, ...]] = set()
    explanations: set[str] = set()
    for row in score_map.values():
        signature = tuple(_bounded_score(row.get(key)) for key in numeric_keys)
        numeric_signatures.add(signature)
        explanations.add(_canonical_text(row.get("explanation_summary") or row.get("explanation")))
    # A single repeated signature + repeated explanation across a full batch
    # is typically a malformed low-signal response.
    return len(numeric_signatures) == 1 and len(explanations) <= 1


def _extract_json(text: Any) -> dict[str, Any]:
    if not isinstance(text, str) or not text.strip():
        raise ValueError("empty_llm_output")
    candidate = text.strip()
    if candidate.startswith("```"):
        lines = candidate.splitlines()
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip().startswith("```"):
            lines = lines[:-1]
        candidate = "\n".join(lines).strip()
    decoder = json.JSONDecoder()
    try:
        parsed_obj, _ = decoder.raw_decode(candidate)
    except json.JSONDecodeError:
        start = candidate.find("{")
        if start < 0:
            raise ValueError("llm_output_not_json") from None
        try:
            parsed_obj, _ = decoder.raw_decode(candidate[start:])
        except json.JSONDecodeError as exc:
            raise ValueError("llm_output_not_json") from exc
    parsed = parsed_obj
    if not isinstance(parsed, dict):
        raise ValueError("llm_output_must_be_object")
    return parsed


def _validate_schema_shape(payload: dict[str, Any]) -> None:
    errors = sorted(_SCORING_OUTPUT_VALIDATOR.iter_errors(payload), key=lambda err: list(err.path))
    if not errors:
        return
    first = errors[0]
    path = ".".join(str(part) for part in first.path) or "$"
    raise ValueError(f"llm_output_schema_error:{path}:{first.message}")


def _parse_llm_scores(payload: dict[str, Any], expected_job_ids: set[str]) -> tuple[dict[str, dict[str, Any]], str | None]:
    _validate_schema_shape(payload)
    rows = payload.get("scores")
    if not isinstance(rows, list):
        raise ValueError("llm_output_scores_must_be_array")

    parsed: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError("llm_output_score_row_must_be_object")
        job_id = str(row.get("job_id") or "").strip()
        if not job_id:
            raise ValueError("llm_output_missing_job_id")
        if job_id not in expected_job_ids:
            raise ValueError(f"llm_output_unknown_job_id:{job_id}")
        if job_id in parsed:
            raise ValueError(f"llm_output_duplicate_job_id:{job_id}")
        parsed[job_id] = {
            "resume_match_score": _bounded_score(row.get("resume_match_score")),
            "title_match_score": _bounded_score(row.get("title_match_score")),
            "salary_score": _bounded_score(row.get("salary_score")),
            "location_score": _bounded_score(row.get("location_score")),
            "seniority_score": _bounded_score(row.get("seniority_score")),
            "overall_score": _bounded_score(row.get("overall_score")),
            "explanation": _short_summary(row.get("explanation"), 220),
            "explanation_summary": _short_summary(row.get("explanation"), 140),
        }

    missing = [job_id for job_id in expected_job_ids if job_id not in parsed]
    if missing:
        raise ValueError("llm_output_partial_missing_job_ids:" + ",".join(sorted(missing)))
    summary = payload.get("summary")
    if summary is not None and not isinstance(summary, str):
        raise ValueError("llm_output_summary_must_be_string")
    return parsed, (_short_summary(summary, 280) if summary else None)


def _iter_batches(jobs: list[dict[str, Any]], size: int) -> list[list[dict[str, Any]]]:
    return [jobs[idx : idx + size] for idx in range(0, len(jobs), size)]


def _fallback_scores(job: dict[str, Any], request: dict[str, Any], profile_context: dict[str, Any]) -> dict[str, Any]:
    signals = deterministic_job_signals(job, request)
    resume_overlap = _resume_overlap_ratio(job, profile_context)
    broad_discovery = bool(signals.get("broad_discovery"))
    scaling_denominator = 2.8 if broad_discovery else 2.6
    recency_details = _recency_details(job, request=request)
    recency_score = float(recency_details.get("recency_score") or 0.0)
    deterministic = float(signals.get("score") or score_job(job, request))
    score_100 = max(0.0, min(deterministic / scaling_denominator * 100.0, 100.0))
    metadata_score = float(signals.get("metadata_quality_score") or 0.0)
    metadata_norm = max(0.0, min(metadata_score / 100.0, 1.0))
    title_signal = float(signals.get("title_signal") or 0.0)
    keyword_signal = float(signals.get("keyword_signal") or 0.0)
    salary_signal = float(signals.get("salary_signal") or 0.0)
    work_mode_signal = float(signals.get("work_mode_signal") or 0.0)
    experience_signal = float(signals.get("experience_signal") or 0.0)
    has_direct_source_url = bool(signals.get("has_direct_source_url"))
    inferred_experience = str(signals.get("inferred_experience_level") or "").strip().lower()

    title_match_score = _bounded_score(
        18.0
        + title_signal * 34.0
        + keyword_signal * 18.0
        + (8.0 if broad_discovery and int(signals.get("title_family_hits") or 0) > 0 else 0.0)
    )
    resume_match_score = _bounded_score(16.0 + score_100 * 0.24 + title_match_score * 0.16 + resume_overlap * 48.0 + metadata_norm * 8.0)
    salary_score = _bounded_score(10.0 + salary_signal * 100.0 + metadata_norm * 10.0 + (6.0 if has_direct_source_url else 0.0))
    location_score = _bounded_score(14.0 + work_mode_signal * 210.0 + metadata_norm * 8.0)
    seniority_score = _bounded_score(
        12.0 + experience_signal * 220.0 + (8.0 if broad_discovery and inferred_experience == "entry" else 0.0)
    )
    weighted_overall = _weighted_overall(
        {
            "resume_match_score": resume_match_score,
            "title_match_score": title_match_score,
            "salary_score": salary_score,
            "location_score": location_score,
            "seniority_score": seniority_score,
            "recency_score": recency_score,
        }
    )
    overall_score = _bounded_score(
        max(score_100, weighted_overall)
        - float(recency_details.get("missing_posted_at_penalty") or 0.0)
        - float(recency_details.get("old_job_penalty") or 0.0)
    )
    scoring_mode = "deterministic_broad_discovery" if broad_discovery else "deterministic_fallback"
    explanation = (
        "Deterministic broad-discovery ranking used after LLM scoring was unavailable; score reflects title-family, work-mode, experience, resume, and metadata signals."
        if broad_discovery
        else "Deterministic fallback ranking used after LLM scoring was unavailable; score reflects title, work-mode, resume, and metadata signals."
    )
    return {
        "resume_match_score": _round(resume_match_score, 2),
        "title_match_score": _round(title_match_score, 2),
        "salary_score": _round(salary_score, 2),
        "location_score": _round(location_score, 2),
        "seniority_score": _round(seniority_score, 2),
        "recency_score": _round(recency_score, 2),
        "overall_score": _round(overall_score, 2),
        "explanation": explanation,
        "explanation_summary": _short_summary(explanation, 140),
        "scoring_mode": scoring_mode,
        "deterministic_signals": {
            "broad_discovery": broad_discovery,
            "resume_overlap_ratio": _round(resume_overlap, 4),
            "title_family_hits": int(signals.get("title_family_hits") or 0),
            "title_exact_hits": int(signals.get("title_exact_hits") or 0),
            "keyword_hits": int(signals.get("keyword_hits") or 0),
            "inferred_work_mode": signals.get("inferred_work_mode"),
            "inferred_experience_level": signals.get("inferred_experience_level"),
            "metadata_quality_score": _round(metadata_score, 2),
            "has_direct_source_url": has_direct_source_url,
            "age_days": recency_details.get("age_days"),
        },
    }


def _weighted_overall(score_row: dict[str, Any]) -> float:
    return _round(
        (
            float(score_row.get("resume_match_score") or 0.0) * 0.30
            + float(score_row.get("title_match_score") or 0.0) * 0.20
            + float(score_row.get("salary_score") or 0.0) * 0.12
            + float(score_row.get("location_score") or 0.0) * 0.13
            + float(score_row.get("seniority_score") or 0.0) * 0.12
            + float(score_row.get("recency_score") or 0.0) * 0.13
        ),
        2,
    )


def _llm_score_batch(
    *,
    model: str,
    task_id: str,
    run_id: str,
    batch_index: int,
    jobs_batch: list[dict[str, Any]],
    request: dict[str, Any],
    profile_context: dict[str, Any],
    prompt_version: str,
    max_retries: int,
    retry_cost_cap_usd: Decimal,
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    expected_ids = {
        str(row.get("normalized_job_id") or row.get("job_id") or "").strip()
        for row in jobs_batch
        if str(row.get("normalized_job_id") or row.get("job_id") or "").strip()
    }
    if not expected_ids:
        return {}, {
            "attempts": 0,
            "openai_request_ids": [],
            "ai_usage_task_run_ids": [],
            "tokens_in": 0,
            "tokens_out": 0,
            "cost_usd": "0.00000000",
            "attempt_errors": [],
            "stop_reason": "empty_batch",
            "retry_cost_cap_usd": str(retry_cost_cap_usd),
        }

    attempts_budget = max(1, max_retries)
    attempts_made = 0
    last_error: Exception | None = None
    openai_request_ids: list[str] = []
    ai_usage_task_run_ids: list[str] = []
    tokens_in_total = 0
    tokens_out_total = 0
    cost_total = Decimal("0")
    attempt_errors: list[dict[str, Any]] = []
    last_error_code: str | None = None
    repeated_error_count = 0
    stop_reason = "max_retries_exhausted"
    fast_fail_codes = {
        "empty_llm_output",
        "llm_output_not_json",
        "llm_output_must_be_object",
        "llm_output_schema_error",
        "llm_output_repetitive_scores_pattern",
    }

    for attempt in range(1, attempts_budget + 1):
        attempts_made = attempt
        messages = build_scoring_messages(
            jobs_batch=jobs_batch,
            request=request,
            profile_context=profile_context,
            prompt_version=prompt_version,
        )
        batch_task_run_id = f"{task_id}:{run_id}:jobs_rank_batch_{batch_index}_{attempt}"
        ai_usage_task_run_ids.append(batch_task_run_id)
        llm_result = run_chat_completion(
            model=model,
            messages=messages,
            temperature=0.1,
            max_completion_tokens=2200,
            task_run_id=batch_task_run_id,
            agent_name="jobs_rank_v1",
        )
        tokens_in_total += int(llm_result.get("tokens_in") or 0)
        tokens_out_total += int(llm_result.get("tokens_out") or 0)
        cost_total += Decimal(str(llm_result.get("cost_usd") or "0"))
        req_id = llm_result.get("openai_request_id")
        if isinstance(req_id, str) and req_id.strip():
            openai_request_ids.append(req_id.strip())

        try:
            parsed = _extract_json(llm_result.get("output_text"))
            score_map, summary = _parse_llm_scores(parsed, expected_ids)
            if _is_repetitive_score_pattern(score_map):
                raise ValueError("llm_output_repetitive_scores_pattern")
            stop_reason = "success"
            return score_map, {
                "attempts": attempt,
                "summary": summary,
                "openai_request_ids": openai_request_ids,
                "ai_usage_task_run_ids": ai_usage_task_run_ids,
                "tokens_in": tokens_in_total,
                "tokens_out": tokens_out_total,
                "cost_usd": str(cost_total.quantize(Decimal("0.00000001"))),
                "attempt_errors": attempt_errors,
                "stop_reason": stop_reason,
                "retry_cost_cap_usd": str(retry_cost_cap_usd),
            }
        except Exception as exc:
            last_error = exc
            error_code = _canonical_error_code(exc)
            if error_code.startswith("llm_output_schema_error:"):
                error_code = "llm_output_schema_error"
            attempt_errors.append(
                {
                    "attempt": attempt,
                    "error_code": error_code,
                    "error": f"{type(exc).__name__}: {exc}",
                }
            )
            if error_code == last_error_code:
                repeated_error_count += 1
            else:
                repeated_error_count = 1
            last_error_code = error_code
            if repeated_error_count >= 2 and error_code in fast_fail_codes and attempt < attempts_budget:
                stop_reason = "fast_fail_repeated_output_pattern"
                break
            if cost_total >= retry_cost_cap_usd and attempt < attempts_budget:
                stop_reason = "retry_cost_cap_reached"
                break
            continue

    return {}, {
        "attempts": attempts_made,
        "summary": None,
        "error": f"{type(last_error).__name__}: {last_error}",
        "openai_request_ids": openai_request_ids,
        "ai_usage_task_run_ids": ai_usage_task_run_ids,
        "tokens_in": tokens_in_total,
        "tokens_out": tokens_out_total,
        "cost_usd": str(cost_total.quantize(Decimal("0.00000001"))),
        "attempt_errors": attempt_errors,
        "stop_reason": stop_reason,
        "retry_cost_cap_usd": str(retry_cost_cap_usd),
    }


def _apply_diversity_controls(scored_jobs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not scored_jobs:
        return []
    base_ordered = sorted(
        scored_jobs,
        key=lambda row: (
            float(row.get("overall_score") or 0.0),
            float(row.get("resume_match_score") or 0.0),
            float(row.get("title_match_score") or 0.0),
        ),
        reverse=True,
    )

    source_totals: dict[str, int] = {}
    for row in base_ordered:
        source = str(row.get("source") or "unknown").strip().lower()
        source_totals[source] = source_totals.get(source, 0) + 1
    total = max(len(base_ordered), 1)

    company_seen: dict[str, int] = {}
    title_seen: dict[str, int] = {}
    source_seen: dict[str, int] = {}
    adjusted: list[dict[str, Any]] = []
    for row in base_ordered:
        entry = dict(row)
        company_key = _canonical_text(entry.get("company"))
        title_key = _canonical_text(entry.get("title"))
        source = str(entry.get("source") or "unknown").strip().lower() or "unknown"

        company_count = company_seen.get(company_key, 0)
        title_count = title_seen.get(title_key, 0)
        source_count = source_seen.get(source, 0)
        company_seen[company_key] = company_count + 1
        title_seen[title_key] = title_count + 1
        source_seen[source] = source_count + 1

        company_penalty = min(company_count * 6.0, 18.0)
        title_penalty = min(title_count * 3.0, 9.0)
        source_ratio = source_totals.get(source, 1) / float(total)
        source_penalty = 2.5 if source_ratio > 0.5 and source_count > 0 else 0.0
        source_bonus = max(0.0, (1.0 - source_ratio) * 2.0)
        scoring_mode = str(entry.get("scoring_mode") or "").strip().lower()
        low_signal_penalty = 0.0 if scoring_mode.startswith("deterministic_") else 4.0 if len(str(entry.get("explanation_summary") or "")) < 30 else 0.0
        metadata_quality_penalty_raw = float(entry.get("metadata_quality_penalty") or 0.0)
        if scoring_mode == "deterministic_broad_discovery":
            metadata_quality_penalty = min(metadata_quality_penalty_raw * 0.35, 4.0)
        elif scoring_mode == "deterministic_fallback":
            metadata_quality_penalty = min(metadata_quality_penalty_raw * 0.6, 7.0)
        else:
            metadata_quality_penalty = metadata_quality_penalty_raw

        adjusted_100 = (
            float(entry.get("overall_score") or 0.0)
            - company_penalty
            - title_penalty
            - source_penalty
            - low_signal_penalty
            - metadata_quality_penalty
            + source_bonus
        )
        adjusted_100 = max(0.0, min(adjusted_100, 100.0))

        entry["diversity_adjustment"] = {
            "company_penalty": _round(company_penalty, 2),
            "title_penalty": _round(title_penalty, 2),
            "source_penalty": _round(source_penalty, 2),
            "source_bonus": _round(source_bonus, 2),
            "low_signal_penalty": _round(low_signal_penalty, 2),
            "metadata_quality_penalty": _round(metadata_quality_penalty, 2),
            "metadata_quality_penalty_raw": _round(metadata_quality_penalty_raw, 2),
        }
        entry["overall_score_adjusted"] = _round(adjusted_100, 2)
        entry["score"] = _round(adjusted_100 / 50.0, 4)
        adjusted.append(entry)

    adjusted.sort(
        key=lambda row: (
            float(row.get("overall_score_adjusted") or 0.0),
            float(row.get("overall_score") or 0.0),
            float(row.get("resume_match_score") or 0.0),
        ),
        reverse=True,
    )
    return adjusted


def _category_counts(jobs: list[dict[str, Any]]) -> dict[str, int]:
    counts = {
        "strong_match": 0,
        "good_match": 0,
        "stretch_match": 0,
        "low_match": 0,
    }
    for row in jobs:
        key = str(row.get("fit_tier") or "low_match")
        counts[key] = counts.get(key, 0) + 1
    return counts


def _model_tradeoffs(model: str | None) -> dict[str, Any]:
    if not model:
        return {"selected_model": None}
    model_info = get_model_info(model) or {}
    input_per_token, output_per_token = get_model_price(model)
    input_1k = (input_per_token * Decimal("1000")).quantize(Decimal("0.0001"))
    output_1k = (output_per_token * Decimal("1000")).quantize(Decimal("0.0001"))
    return {
        "selected_model": model,
        "selected_model_info": model_info,
        "estimated_cost_per_1k": {
            "input_usd": str(input_1k),
            "output_usd": str(output_1k),
            "total_usd": str((input_1k + output_1k).quantize(Decimal("0.0001"))),
        },
        "tier_recommendations": {
            "cheap": tier_model("cheap"),
            "standard": tier_model("standard"),
            "advanced": tier_model("advanced"),
        },
        "tradeoff_notes": [
            "Use cheap tier for high-volume pre-screening.",
            "Use standard tier for balanced quality/cost on daily ranking.",
            "Use advanced tier for small, high-priority candidate pools.",
        ],
    }


def _runtime_error_with_usage(
    message: str,
    *,
    tokens_in: int = 0,
    tokens_out: int = 0,
    cost_usd: Decimal | str = Decimal("0"),
    request_ids: list[str] | None = None,
    task_run_ids: list[str] | None = None,
) -> RuntimeError:
    err = RuntimeError(message)
    err.usage = {
        "tokens_in": max(int(tokens_in), 0),
        "tokens_out": max(int(tokens_out), 0),
        "cost_usd": str(cost_usd),
        "openai_request_ids": request_ids or [],
        "ai_usage_task_run_ids": task_run_ids or [],
    }
    return err


def execute(task: Any, db: Any) -> dict[str, Any]:
    payload = payload_object(task.payload_json)
    upstream = payload.get("upstream") if isinstance(payload.get("upstream"), dict) else {}
    request = resolve_request(payload.get("request") if isinstance(payload.get("request"), dict) else payload)
    pipeline_id = new_pipeline_id(payload.get("pipeline_id"))

    upstream_result = fetch_upstream_result_content_json(db, upstream)
    expect_artifact_type(upstream_result, "jobs.normalize.v1")

    normalized_jobs = upstream_result.get("normalized_jobs")
    if not isinstance(normalized_jobs, list):
        normalized_jobs = []

    normalize_counts = upstream_result.get("counts") if isinstance(upstream_result.get("counts"), dict) else {}
    pipeline_counts = {
        "collected_count": _as_non_negative_int(normalize_counts.get("raw_count")),
        "normalized_count": _as_non_negative_int(normalize_counts.get("normalized_count")),
        "deduped_count": _as_non_negative_int(normalize_counts.get("deduped_count")),
        "duplicates_collapsed": _as_non_negative_int(normalize_counts.get("duplicates_collapsed")),
    }

    rank_policy = payload.get("rank_policy") if isinstance(payload.get("rank_policy"), dict) else {}
    try:
        max_ranked = int(rank_policy.get("max_ranked") or 200)
    except (TypeError, ValueError):
        max_ranked = 200
    max_ranked = max(1, min(max_ranked, MAX_LLM_JOBS))

    try:
        llm_batch_size = int(rank_policy.get("llm_batch_size") or DEFAULT_BATCH_SIZE)
    except (TypeError, ValueError):
        llm_batch_size = DEFAULT_BATCH_SIZE
    llm_batch_size = max(1, min(llm_batch_size, MAX_BATCH_SIZE))

    try:
        llm_max_retries = int(rank_policy.get("llm_max_retries") or DEFAULT_LLM_MAX_RETRIES)
    except (TypeError, ValueError):
        llm_max_retries = DEFAULT_LLM_MAX_RETRIES
    llm_max_retries = max(1, min(llm_max_retries, MAX_LLM_MAX_RETRIES))

    llm_enabled = bool(rank_policy.get("llm_enabled", bool(request.get("rank_llm_enabled", True))))
    strict_llm_output = bool(rank_policy.get("strict_llm_output", False))
    prompt_version = str(rank_policy.get("prompt_version") or RANK_PROMPT_VERSION)
    retry_cost_cap_raw = rank_policy.get("llm_retry_cost_cap_usd")
    if retry_cost_cap_raw is None:
        retry_cost_cap_raw = request.get("rank_llm_retry_cost_cap_usd")
    llm_retry_cost_cap_usd = _resolve_retry_cost_cap(
        retry_cost_cap_raw,
        strict_llm_output=strict_llm_output,
    )

    filtered_jobs = [job for job in normalized_jobs if isinstance(job, dict) and matches_filters(job, request)]
    base_ranked = sorted(
        filtered_jobs,
        key=lambda row: _pre_llm_priority(row, request),
        reverse=True,
    )[:max_ranked]
    prepared_jobs: list[dict[str, Any]] = []
    for idx, row in enumerate(base_ranked, start=1):
        item = dict(row)
        normalized_job_id = str(item.get("normalized_job_id") or item.get("job_id") or "").strip()
        if not normalized_job_id:
            normalized_job_id = f"rank-{idx:06d}"
        item["normalized_job_id"] = normalized_job_id
        prepared_jobs.append(item)

    profile_context = resolve_profile_context(request)
    runtime_llm = llm_enabled and _llm_runtime_enabled()
    model_id = str(getattr(task, "model", "") or "").strip() or tier_model("standard")
    llm_warnings: list[str] = []
    llm_scores_by_id: dict[str, dict[str, Any]] = {}
    llm_request_ids: list[str] = []
    llm_ai_usage_task_run_ids: list[str] = []
    llm_tokens_in_total = 0
    llm_tokens_out_total = 0
    llm_cost_total = Decimal("0")
    llm_batch_summaries: list[str] = []
    llm_failed_batches: list[int] = []
    llm_attempt_errors: list[dict[str, Any]] = []
    llm_batch_stop_reasons: list[str] = []
    llm_attempts_total = 0
    now_utc = datetime.now(timezone.utc)

    if runtime_llm and prepared_jobs:
        batches = _iter_batches(prepared_jobs, llm_batch_size)
        task_id = str(getattr(task, "id", "") or "")
        run_id = str(getattr(task, "_run_id", "") or "")
        for idx, batch in enumerate(batches, start=1):
            batch_scores, batch_meta = _llm_score_batch(
                model=model_id,
                task_id=task_id,
                run_id=run_id,
                batch_index=idx,
                jobs_batch=batch,
                request=request,
                profile_context=profile_context,
                prompt_version=prompt_version,
                max_retries=llm_max_retries,
                retry_cost_cap_usd=llm_retry_cost_cap_usd,
            )
            llm_request_ids.extend(batch_meta.get("openai_request_ids") or [])
            llm_ai_usage_task_run_ids.extend(batch_meta.get("ai_usage_task_run_ids") or [])
            llm_tokens_in_total += int(batch_meta.get("tokens_in") or 0)
            llm_tokens_out_total += int(batch_meta.get("tokens_out") or 0)
            llm_cost_total += Decimal(str(batch_meta.get("cost_usd") or "0"))
            llm_attempts_total += int(batch_meta.get("attempts") or 0)
            llm_attempt_errors.extend(batch_meta.get("attempt_errors") or [])
            stop_reason = str(batch_meta.get("stop_reason") or "").strip()
            if stop_reason:
                llm_batch_stop_reasons.append(f"batch_{idx}:{stop_reason}")

            batch_error = batch_meta.get("error")
            if isinstance(batch_error, str) and batch_error.strip():
                llm_failed_batches.append(idx)
                llm_warnings.append(f"llm_batch_{idx}_failed: {batch_error.strip()}")
                if stop_reason:
                    llm_warnings.append(f"llm_batch_{idx}_stop_reason:{stop_reason}")
                # If early batches all fail and strict mode is off, stop burning tokens.
                if not strict_llm_output and not llm_scores_by_id and len(llm_failed_batches) >= 2:
                    llm_warnings.append("llm_scoring_disabled_after_repeated_batch_failures")
                    break
                continue

            llm_scores_by_id.update(batch_scores)
            summary = batch_meta.get("summary")
            if isinstance(summary, str) and summary.strip():
                llm_batch_summaries.append(summary.strip())

        if llm_failed_batches and strict_llm_output and not llm_scores_by_id:
            raise _runtime_error_with_usage(
                "temporary llm scoring failure (strict_llm_output=true): all LLM scoring batches failed",
                tokens_in=llm_tokens_in_total,
                tokens_out=llm_tokens_out_total,
                cost_usd=str(llm_cost_total.quantize(Decimal("0.00000001"))),
                request_ids=llm_request_ids,
                task_run_ids=llm_ai_usage_task_run_ids,
            )
    elif llm_enabled and not runtime_llm:
        llm_warnings.append("llm_disabled_at_runtime_use_llm_false")

    scored_jobs: list[dict[str, Any]] = []
    for idx, job in enumerate(prepared_jobs, start=1):
        row = dict(job)
        job_id = str(row.get("normalized_job_id") or row.get("job_id") or f"job-{idx}").strip()
        row["job_id"] = job_id

        llm_score = llm_scores_by_id.get(job_id)
        if llm_score is None:
            fallback = _fallback_scores(row, request, profile_context)
            llm_score = fallback
            if runtime_llm:
                llm_warnings.append(f"llm_missing_score_for_job:{job_id}")

        recency_details = _recency_details(row, request=request, now_utc=now_utc)
        recency_score = float(llm_score.get("recency_score") or recency_details.get("recency_score") or 0.0)
        computed_overall = _weighted_overall({**llm_score, "recency_score": recency_score})
        overall_raw = float(llm_score.get("overall_score") or computed_overall)
        if overall_raw <= 0:
            overall_raw = computed_overall
        elif job_id in llm_scores_by_id:
            overall_raw = _bounded_score(overall_raw * 0.88 + recency_score * 0.12)
        else:
            overall_raw = _bounded_score(max(overall_raw, computed_overall))
        overall_raw = _bounded_score(
            overall_raw
            - float(recency_details.get("missing_posted_at_penalty") or 0.0)
            - float(recency_details.get("old_job_penalty") or 0.0)
        )
        quality_details, quality_penalty = _metadata_quality_penalty(row)

        row.update(
            {
                "resume_match_score": _bounded_score(llm_score.get("resume_match_score")),
                "title_match_score": _bounded_score(llm_score.get("title_match_score")),
                "salary_score": _bounded_score(llm_score.get("salary_score")),
                "location_score": _bounded_score(llm_score.get("location_score")),
                "seniority_score": _bounded_score(llm_score.get("seniority_score")),
                "recency_score": _round(recency_score, 2),
                "age_days": recency_details.get("age_days"),
                "missing_posted_at_penalty": _round(float(recency_details.get("missing_posted_at_penalty") or 0.0), 2),
                "old_job_penalty": _round(float(recency_details.get("old_job_penalty") or 0.0), 2),
                "overall_score": _round(overall_raw, 2),
                "explanation": _short_summary(llm_score.get("explanation"), 220),
                "explanation_summary": _short_summary(llm_score.get("explanation_summary") or llm_score.get("explanation"), 140),
                "scoring_mode": str(
                    llm_score.get("scoring_mode")
                    or (
                        "llm_structured"
                        if job_id in llm_scores_by_id
                        else "deterministic_broad_discovery"
                        if is_broad_discovery_request(request)
                        else "deterministic_fallback"
                    )
                ),
                "metadata_quality_score": float(quality_details.get("metadata_quality_score") or 0.0),
                "missing_company": bool(quality_details.get("missing_company")),
                "missing_source_url": bool(quality_details.get("missing_source_url")),
                "missing_posted_at": bool(quality_details.get("missing_posted_at")),
                "missing_location": bool(quality_details.get("missing_location")),
                "source_url_kind": quality_details.get("source_url_kind"),
                "has_direct_source_url": bool(quality_details.get("has_direct_source_url")),
                "metadata_quality_penalty": quality_penalty,
            }
        )
        scored_jobs.append(row)

    ranked_jobs = _apply_diversity_controls(scored_jobs)
    fallback_used = any(
        str(row.get("scoring_mode") or "") in {"deterministic_fallback", "deterministic_broad_discovery"}
        for row in ranked_jobs
    )
    for idx, job in enumerate(ranked_jobs, start=1):
        score_scaled = float(job.get("score") or 0.0)
        job["rank"] = idx
        job["fit_tier"] = fit_tier(score_scaled)
        job["fit_reason"] = job.get("explanation_summary") or "Scored by candidate-fit criteria."

    jobs_scored_artifact = {
        "artifact_type": "jobs_scored.v1",
        "artifact_schema": "jobs_scored.v1",
        "pipeline_id": pipeline_id,
        "scored_at": utc_iso(),
        "request": request,
        "input_jobs_count": len(normalized_jobs),
        "filtered_jobs_count": len(filtered_jobs),
        "scored_jobs_count": len(ranked_jobs),
        "pipeline_counts": {
            **pipeline_counts,
            "scored_count": len(ranked_jobs),
        },
        "jobs_scored": ranked_jobs,
        "llm": {
            "enabled": llm_enabled,
            "runtime_enabled": runtime_llm,
            "model": model_id if runtime_llm else None,
            "prompt_version": prompt_version,
            "batch_size": llm_batch_size,
            "max_retries": llm_max_retries,
            "retry_cost_cap_usd": str(llm_retry_cost_cap_usd),
            "attempts_total": llm_attempts_total,
            "failed_batches": llm_failed_batches,
            "batch_stop_reasons": llm_batch_stop_reasons,
            "request_ids": llm_request_ids,
            "ai_usage_task_run_ids": llm_ai_usage_task_run_ids,
            "tokens_in_total": llm_tokens_in_total,
            "tokens_out_total": llm_tokens_out_total,
            "cost_usd_total": str(llm_cost_total.quantize(Decimal("0.00000001"))),
            "summaries": llm_batch_summaries[:5],
            "attempt_errors": llm_attempt_errors[-10:],
            "fallback_used": fallback_used,
        },
        "warnings": llm_warnings,
        "upstream": upstream,
    }

    artifact = {
        "artifact_type": "jobs.rank.v1",
        "artifact_schema": "jobs.rank.v2",
        "pipeline_id": pipeline_id,
        "ranked_at": utc_iso(),
        "request": request,
        "rank_policy": {
            "weights": rank_policy.get("weights") if isinstance(rank_policy.get("weights"), dict) else {
                "resume_match_score": 0.30,
                "title_match_score": 0.20,
                "salary_score": 0.12,
                "location_score": 0.13,
                "seniority_score": 0.12,
                "recency_score": 0.13,
            },
            "max_ranked": max_ranked,
            "llm_enabled": llm_enabled,
            "llm_runtime_enabled": runtime_llm,
            "llm_batch_size": llm_batch_size,
            "llm_max_retries": llm_max_retries,
            "llm_retry_cost_cap_usd": str(llm_retry_cost_cap_usd),
            "strict_llm_output": strict_llm_output,
            "prompt_version": prompt_version,
        },
        "profile_context": {
            "enabled": bool(profile_context.get("enabled")),
            "applied": bool(profile_context.get("applied")),
            "source": profile_context.get("source"),
            "resume_name": profile_context.get("resume_name"),
            "updated_at": profile_context.get("updated_at"),
            "resume_char_count": int(profile_context.get("resume_char_count") or 0),
            "resume_sent_char_count": int(profile_context.get("resume_sent_char_count") or 0),
            "resume_truncated": bool(profile_context.get("resume_truncated")),
        },
        "input_jobs_count": len(normalized_jobs),
        "filtered_jobs_count": len(filtered_jobs),
        "pipeline_counts": {
            **pipeline_counts,
            "scored_count": len(ranked_jobs),
        },
        "ranked_jobs": ranked_jobs,
        "category_counts": _category_counts(ranked_jobs),
        "jobs_scored_artifact": jobs_scored_artifact,
        "model_usage": {
            "llm_requested": llm_enabled,
            "llm_runtime_enabled": runtime_llm,
            "model": model_id if runtime_llm else None,
            "prompt_version": prompt_version,
            "openai_request_ids": llm_request_ids,
            "ai_usage_task_run_ids": llm_ai_usage_task_run_ids,
        },
        "model_tradeoffs": _model_tradeoffs(model_id if runtime_llm else None),
        "warnings": llm_warnings + [
            "resume_context_unavailable" if profile_context.get("enabled") and not profile_context.get("applied") else ""
        ],
        "upstream": upstream,
    }
    artifact["warnings"] = [row for row in artifact["warnings"] if row]

    next_upstream = build_upstream_ref(task, "jobs_rank_v1")
    upstream_run_id = next_upstream.get("run_id") or str(getattr(task, "id", ""))
    incoming_shortlist_policy = payload.get("shortlist_policy") if isinstance(payload.get("shortlist_policy"), dict) else {}
    next_shortlist_policy = dict(incoming_shortlist_policy)
    if "max_items" not in next_shortlist_policy:
        next_shortlist_policy["max_items"] = int(request.get("shortlist_max_items") or 10)
    if "min_score" not in next_shortlist_policy:
        next_shortlist_policy["min_score"] = float(request.get("shortlist_min_score") or 0.75)
    if "per_source_cap" not in next_shortlist_policy:
        next_shortlist_policy["per_source_cap"] = int(request.get("shortlist_per_source_cap") or 3)
    if "diversity_mode" not in next_shortlist_policy:
        next_shortlist_policy["diversity_mode"] = str(request.get("shortlist_diversity_mode") or "balanced_sources")
    if "freshness_preference" not in next_shortlist_policy:
        next_shortlist_policy["freshness_preference"] = str(request.get("shortlist_freshness_preference") or "off")
    if "freshness_weight_enabled" not in next_shortlist_policy:
        next_shortlist_policy["freshness_weight_enabled"] = bool(request.get("shortlist_freshness_weight_enabled", False))
    if "freshness_max_bonus" not in next_shortlist_policy:
        next_shortlist_policy["freshness_max_bonus"] = float(request.get("shortlist_freshness_max_bonus") or 0.0)
    if "jobs_notification_cooldown_days" not in next_shortlist_policy:
        next_shortlist_policy["jobs_notification_cooldown_days"] = int(request.get("jobs_notification_cooldown_days") or 3)
    if "jobs_shortlist_repeat_penalty" not in next_shortlist_policy:
        next_shortlist_policy["jobs_shortlist_repeat_penalty"] = float(request.get("jobs_shortlist_repeat_penalty") or 4.0)
    if "resurface_seen_jobs" not in next_shortlist_policy:
        next_shortlist_policy["resurface_seen_jobs"] = bool(request.get("resurface_seen_jobs", True))

    next_payload = {
        "pipeline_id": pipeline_id,
        "upstream": next_upstream,
        "request": request,
        "shortlist_policy": next_shortlist_policy,
    }

    result: dict[str, Any] = {
        "artifact_type": "jobs.rank.v1",
        "content_text": (
            f"Scored and ranked {len(ranked_jobs)} jobs from {len(normalized_jobs)} normalized inputs "
            f"(llm_runtime={runtime_llm})."
        ),
        "content_json": artifact,
        "debug_json": {
            "pipeline_id": pipeline_id,
            "llm_enabled": llm_enabled,
            "llm_runtime_enabled": runtime_llm,
            "filtered_jobs_count": len(filtered_jobs),
            "failed_batches": llm_failed_batches,
            "llm_attempts_total": llm_attempts_total,
            "llm_batch_stop_reasons": llm_batch_stop_reasons,
            "llm_attempt_errors": llm_attempt_errors[-10:],
            "fallback_used": fallback_used,
            "strict_llm_output": strict_llm_output,
            "ai_usage_task_run_ids": llm_ai_usage_task_run_ids,
        },
        "next_tasks": [
            {
                "task_type": "jobs_shortlist_v1",
                "payload_json": next_payload,
                "idempotency_key": stage_idempotency_key(pipeline_id, "jobs_shortlist_v1", upstream_run_id),
                "max_attempts": 3,
            }
        ],
    }
    if runtime_llm:
        result["usage"] = {
            "tokens_in": llm_tokens_in_total,
            "tokens_out": llm_tokens_out_total,
            "cost_usd": str(llm_cost_total.quantize(Decimal("0.00000001"))),
            "openai_request_ids": llm_request_ids,
            "ai_usage_task_run_ids": llm_ai_usage_task_run_ids,
        }
    return result
