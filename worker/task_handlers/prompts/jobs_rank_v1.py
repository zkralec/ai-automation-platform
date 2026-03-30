from __future__ import annotations

import json
from typing import Any

RANK_PROMPT_VERSION = "jobs-rank-v3-structured"

SCORING_OUTPUT_SCHEMA = {
    "type": "object",
    "required": ["scores"],
    "properties": {
        "scores": {
            "type": "array",
            "items": {
                "type": "object",
                "required": [
                    "job_id",
                    "resume_match_score",
                    "title_match_score",
                    "salary_score",
                    "location_score",
                    "seniority_score",
                    "overall_score",
                    "explanation",
                ],
                "properties": {
                    "job_id": {"type": "string"},
                    "resume_match_score": {"type": "number", "minimum": 0, "maximum": 100},
                    "title_match_score": {"type": "number", "minimum": 0, "maximum": 100},
                    "salary_score": {"type": "number", "minimum": 0, "maximum": 100},
                    "location_score": {"type": "number", "minimum": 0, "maximum": 100},
                    "seniority_score": {"type": "number", "minimum": 0, "maximum": 100},
                    "overall_score": {"type": "number", "minimum": 0, "maximum": 100},
                    "explanation": {"type": "string"},
                },
                "additionalProperties": False,
            },
        },
        "summary": {"type": "string"},
    },
    "additionalProperties": False,
}


def _trim(value: Any, max_chars: int) -> str | None:
    if not isinstance(value, str):
        return None
    text = " ".join(value.strip().split())
    if not text:
        return None
    if len(text) <= max_chars:
        return text
    return text[: max_chars - 1].rstrip() + "…"


def build_scoring_messages(
    *,
    jobs_batch: list[dict[str, Any]],
    request: dict[str, Any],
    profile_context: dict[str, Any],
    prompt_version: str = RANK_PROMPT_VERSION,
) -> list[dict[str, str]]:
    candidate_profile = {
        "resume_applied": bool(profile_context.get("applied")),
        "resume_source": profile_context.get("source"),
        "resume_name": profile_context.get("resume_name"),
        "resume_text_excerpt": _trim(profile_context.get("resume_text"), 2200),
    }
    preferences = {
        "desired_titles": request.get("titles") or request.get("desired_title_keywords") or [],
        "keywords": request.get("keywords") or [],
        "excluded_keywords": request.get("excluded_keywords") or request.get("excluded_title_keywords") or [],
        "locations": request.get("locations") or [request.get("location")],
        "remote_preference": request.get("work_mode_preference") or request.get("work_modes") or [],
        "minimum_salary": request.get("minimum_salary") or request.get("desired_salary_min"),
        "experience_level": request.get("experience_level") or request.get("experience_levels"),
        "preferred_industries": request.get("preferred_industries") or [],
        "preferred_company_traits": request.get("preferred_company_traits") or [],
    }

    jobs_for_prompt: list[dict[str, Any]] = []
    for row in jobs_batch:
        jobs_for_prompt.append(
            {
                "job_id": row.get("normalized_job_id") or row.get("job_id"),
                "title": row.get("title"),
                "company": row.get("company"),
                "location": row.get("location"),
                "location_normalized": row.get("location_normalized"),
                "remote_type": row.get("remote_type") or row.get("work_mode"),
                "metadata_quality_location": row.get("metadata_quality_location"),
                "salary_min": row.get("salary_min"),
                "salary_max": row.get("salary_max"),
                "salary_text": row.get("salary_text"),
                "posted_at_normalized": row.get("posted_at_normalized") or row.get("posted_at"),
                "posted_age_days": row.get("posted_age_days"),
                "metadata_quality_recency": row.get("metadata_quality_recency"),
                "experience_level": row.get("experience_level"),
                "source": row.get("source"),
                "source_url": row.get("source_url"),
                "description_snippet": _trim(row.get("description_snippet"), 420),
            }
        )

    expected_job_ids = [
        str(row.get("job_id") or "")
        for row in jobs_for_prompt
        if str(row.get("job_id") or "").strip()
    ]
    output_template = {
        "scores": [
            {
                "job_id": expected_job_ids[0] if expected_job_ids else "job_id",
                "resume_match_score": 0,
                "title_match_score": 0,
                "salary_score": 0,
                "location_score": 0,
                "seniority_score": 0,
                "overall_score": 0,
                "explanation": "short rationale",
            }
        ],
        "summary": "optional brief summary",
    }

    user_payload = {
        "prompt_version": prompt_version,
        "task": "Score each job independently against the candidate profile and preferences.",
        "candidate_profile": candidate_profile,
        "preferences": preferences,
        "jobs": jobs_for_prompt,
        "expected_job_ids": expected_job_ids,
        "output_contract": SCORING_OUTPUT_SCHEMA,
        "output_template": output_template,
        "rules": [
            "Return strict JSON only. No markdown fences, no prose, no trailing text.",
            "Top-level JSON must be an object with key 'scores' and optional key 'summary'.",
            (
                "Each scores item must include exactly: job_id, resume_match_score, title_match_score, "
                "salary_score, location_score, seniority_score, overall_score, explanation."
            ),
            "scores length must equal the number of input jobs.",
            "Use only job_id values from expected_job_ids and include each exactly once.",
            "Score each field from 0 to 100.",
            "Use concise explanations under 180 characters per job.",
            "Penalize clear mismatches and excluded-keyword conflicts.",
        ],
    }

    system_prompt = (
        "You are a rigorous recruiting analyst. "
        "You score jobs against a candidate profile with calibrated, non-inflated scoring. "
        "Always produce strict JSON that matches the given output contract."
    )

    return [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": json.dumps(user_payload, ensure_ascii=True)},
    ]
