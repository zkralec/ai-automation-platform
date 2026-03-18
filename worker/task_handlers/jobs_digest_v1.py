from __future__ import annotations

from typing import Any

from task_handlers.jobs_pipeline_common import (
    build_upstream_ref,
    new_pipeline_id,
    payload_object,
    stage_idempotency_key,
    utc_iso,
)


def _legacy_to_collect_request(payload: dict[str, Any]) -> dict[str, Any]:
    board_sources: list[str] = []
    if isinstance(payload.get("job_boards"), list):
        for row in payload.get("job_boards") or []:
            if isinstance(row, str) and row.strip():
                low = row.strip().lower()
                if low not in board_sources:
                    board_sources.append(low)

    has_manual_jobs = isinstance(payload.get("jobs"), list) and bool(payload.get("jobs"))
    if has_manual_jobs and "manual" not in board_sources:
        board_sources.append("manual")

    collectors_enabled = bool(payload.get("collectors_enabled", True))
    if not board_sources:
        board_sources = ["linkedin", "indeed", "glassdoor", "handshake"]

    profile_mode = "resume_profile"
    if bool(payload.get("use_resume_profile", True)) is False:
        profile_mode = "none"
    if isinstance(payload.get("resume_text"), str) and payload.get("resume_text", "").strip():
        profile_mode = "inline_resume"

    request: dict[str, Any] = {
        "query": payload.get("search_query") or payload.get("query") or "software engineer",
        "location": payload.get("location") or payload.get("search_location") or "United States",
        "collectors_enabled": collectors_enabled,
        "sources": board_sources,
        "max_jobs_per_source": payload.get("max_jobs_per_board") or 25,
        "max_pages_per_source": payload.get("max_pages_per_source"),
        "max_queries_per_title_location_pair": payload.get("max_queries_per_title_location_pair"),
        "early_stop_when_no_new_results": payload.get("early_stop_when_no_new_results"),
        "manual_jobs": payload.get("jobs") if isinstance(payload.get("jobs"), list) else [],
        "board_url_overrides": payload.get("board_url_overrides") if isinstance(payload.get("board_url_overrides"), dict) else {},
        "profile_mode": profile_mode,
        "resume_text": payload.get("resume_text"),
        "resume_name": payload.get("resume_name"),
        "notify_on_empty": False,
        "desired_title": payload.get("desired_title"),
        "desired_title_keywords": payload.get("desired_title_keywords") if isinstance(payload.get("desired_title_keywords"), list) else [],
        "excluded_title_keywords": payload.get("excluded_title_keywords") if isinstance(payload.get("excluded_title_keywords"), list) else [],
        "desired_salary_min": payload.get("desired_salary_min"),
        "desired_salary_max": payload.get("desired_salary_max"),
        "require_salary_data": bool(payload.get("require_salary_data", False)),
        "experience_levels": payload.get("experience_levels") if isinstance(payload.get("experience_levels"), list) else [],
        "require_experience_match": bool(payload.get("require_experience_match", False)),
        "clearance_required": payload.get("clearance_required"),
        "required_clearances": payload.get("required_clearances") if isinstance(payload.get("required_clearances"), list) else [],
        "require_clearance_match": bool(payload.get("require_clearance_match", False)),
        "work_modes": payload.get("work_modes") if isinstance(payload.get("work_modes"), list) else [],
        "require_work_mode_match": bool(payload.get("require_work_mode_match", False)),
        "location_keywords": payload.get("location_keywords") if isinstance(payload.get("location_keywords"), list) else [],
        "rank_llm_enabled": True,
        "digest_llm_enabled": True,
    }
    return request


def execute(task: Any, db: Any) -> dict[str, Any]:
    del db

    payload = payload_object(task.payload_json)
    pipeline_id = new_pipeline_id(payload.get("pipeline_id"))
    request = _legacy_to_collect_request(payload)

    upstream = build_upstream_ref(task, "jobs_digest_v1")
    upstream_run_id = upstream.get("run_id") or str(getattr(task, "id", ""))

    next_payload = {
        "pipeline_id": pipeline_id,
        "request": request,
        "lineage": {
            "shim_task_type": "jobs_digest_v1",
            "shim_task_id": str(getattr(task, "id", "") or ""),
            "shim_run_id": upstream.get("run_id"),
        },
    }

    forwarded_summary = {
        "artifact_type": "jobs.digest.v1.compat_shim",
        "forwarded_to": "jobs_collect_v1",
        "pipeline_id": pipeline_id,
        "request": request,
        "shim_created_at": utc_iso(),
        "upstream": upstream,
        "deprecation": {
            "status": "deprecated",
            "message": "jobs_digest_v1 is now a compatibility shim. Use jobs_collect_v1 for new workflows.",
        },
    }

    return {
        "artifact_type": "jobs.digest.v1.compat_shim",
        "content_text": "jobs_digest_v1 accepted payload and forwarded to jobs_collect_v1 pipeline.",
        "content_json": forwarded_summary,
        "debug_json": {
            "pipeline_id": pipeline_id,
            "forwarded_task_type": "jobs_collect_v1",
        },
        "next_tasks": [
            {
                "task_type": "jobs_collect_v1",
                "payload_json": next_payload,
                "idempotency_key": stage_idempotency_key(pipeline_id, "jobs_collect_v1", upstream_run_id),
                "max_attempts": int(getattr(task, "max_attempts", 3) or 3),
            }
        ],
    }
