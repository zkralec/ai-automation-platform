import json
import uuid
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import text

from candidate_profile import get_resume_profile as get_stored_resume_profile
from task_handlers.errors import NonRetryableTaskError

SUPPORTED_JOB_SOURCES = ("linkedin", "indeed", "glassdoor", "handshake", "manual")
DEFAULT_JOB_BOARDS = ("linkedin", "indeed", "glassdoor", "handshake")
DEFAULT_QUERY = "software engineer"
DEFAULT_LOCATION = "United States"
MAX_RESUME_CHARS_FOR_LLM = 16_000
DEFAULT_RESULT_LIMIT_PER_SOURCE = 250
MAX_RESULT_LIMIT_PER_SOURCE = 1000
DEFAULT_MAX_PAGES_PER_SOURCE = 5
MAX_MAX_PAGES_PER_SOURCE = 20
DEFAULT_MAX_QUERIES_PER_TITLE_LOCATION_PAIR = 4
MAX_MAX_QUERIES_PER_TITLE_LOCATION_PAIR = 10


def utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def payload_object(payload_json: str) -> dict[str, Any]:
    try:
        parsed = json.loads(payload_json)
    except json.JSONDecodeError as exc:
        raise NonRetryableTaskError(f"payload is invalid JSON: {exc.msg}") from exc
    if not isinstance(parsed, dict):
        raise NonRetryableTaskError("payload must be a JSON object")
    return parsed


def _as_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        stripped = value.strip().replace(",", "")
        if not stripped:
            return None
        if stripped.startswith("$"):
            stripped = stripped[1:]
        try:
            return float(stripped)
        except ValueError:
            return None
    return None


def _as_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        low = value.strip().lower()
        if low in {"true", "yes", "required", "1"}:
            return True
        if low in {"false", "no", "none", "0"}:
            return False
    return None


def _as_bounded_int(value: Any, *, default: int, minimum: int, maximum: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = default
    return max(minimum, min(parsed, maximum))


def _as_text_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    output: list[str] = []
    for item in value:
        if not isinstance(item, str):
            continue
        trimmed = item.strip()
        if trimmed:
            output.append(trimmed)
    return output


def _dedupe_text_list(values: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        key = value.strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(value.strip())
    return deduped


def _normalize_experience_level(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    low = value.strip().lower()
    if not low:
        return None
    if low in {"intern", "internship", "co-op", "coop"}:
        return "internship"
    if low in {"entry", "entry-level", "junior", "new grad", "associate"}:
        return "entry"
    if low in {"mid", "mid-level", "intermediate"}:
        return "mid"
    if low in {"senior", "lead", "staff", "principal", "manager", "director"}:
        return "senior"
    return low


def _normalize_work_mode(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    low = value.strip().lower()
    if low in {"remote", "hybrid", "onsite", "on-site"}:
        return "onsite" if low in {"onsite", "on-site"} else low
    return None


def _pick_text(job: dict[str, Any], keys: tuple[str, ...]) -> str | None:
    for key in keys:
        value = job.get(key)
        if isinstance(value, str):
            trimmed = value.strip()
            if trimmed:
                return trimmed
    return None


def _pick_numeric(job: dict[str, Any], keys: tuple[str, ...]) -> float | None:
    for key in keys:
        parsed = _as_float(job.get(key))
        if parsed is not None:
            return parsed
    return None


def new_pipeline_id(provided: Any = None) -> str:
    if isinstance(provided, str) and provided.strip():
        return provided.strip()
    return str(uuid.uuid4())


def resolve_request(raw_request: dict[str, Any] | None) -> dict[str, Any]:
    request = raw_request if isinstance(raw_request, dict) else {}

    query = _pick_text(request, ("query", "search_query")) or DEFAULT_QUERY
    location = _pick_text(request, ("location", "search_location")) or DEFAULT_LOCATION
    collectors_enabled = bool(request.get("collectors_enabled", True))

    desired_title = _pick_text(request, ("desired_title",))
    desired_title_keywords = _as_text_list(request.get("desired_title_keywords"))
    excluded_title_keywords = _as_text_list(request.get("excluded_title_keywords"))

    titles = _dedupe_text_list(_as_text_list(request.get("titles")))
    if desired_title and desired_title not in titles:
        titles.insert(0, desired_title)

    keywords = _dedupe_text_list(_as_text_list(request.get("keywords")) + desired_title_keywords)
    excluded_keywords = _dedupe_text_list(_as_text_list(request.get("excluded_keywords")) + excluded_title_keywords)

    locations = _dedupe_text_list(_as_text_list(request.get("locations")))
    if not locations:
        locations = [location]

    raw_work_mode_pref = _as_text_list(request.get("work_mode_preference"))
    if not raw_work_mode_pref:
        single_work_mode_pref = _pick_text(request, ("work_mode_preference",))
        if single_work_mode_pref:
            raw_work_mode_pref = [single_work_mode_pref]
    if not raw_work_mode_pref:
        raw_work_mode_pref = _as_text_list(request.get("work_modes"))
    work_mode_preference = [
        item for item in (_normalize_work_mode(row) for row in raw_work_mode_pref) if item
    ]

    minimum_salary = _as_float(request.get("minimum_salary"))
    if minimum_salary is None:
        minimum_salary = _as_float(request.get("desired_salary_min"))
    desired_salary_min = _as_float(request.get("desired_salary_min"))
    if desired_salary_min is None:
        desired_salary_min = minimum_salary

    experience_level = _normalize_experience_level(_pick_text(request, ("experience_level",)))
    experience_levels = [
        item for item in (_normalize_experience_level(row) for row in _as_text_list(request.get("experience_levels"))) if item
    ]
    if experience_level and experience_level not in experience_levels:
        experience_levels.insert(0, experience_level)
    if not experience_level and experience_levels:
        experience_level = experience_levels[0]

    max_jobs = _as_bounded_int(
        request.get("result_limit_per_source")
        or request.get("max_jobs_per_source")
        or request.get("max_jobs_per_board")
        or DEFAULT_RESULT_LIMIT_PER_SOURCE,
        default=DEFAULT_RESULT_LIMIT_PER_SOURCE,
        minimum=1,
        maximum=MAX_RESULT_LIMIT_PER_SOURCE,
    )

    max_pages_per_source = _as_bounded_int(
        request.get("max_pages_per_source") or DEFAULT_MAX_PAGES_PER_SOURCE,
        default=DEFAULT_MAX_PAGES_PER_SOURCE,
        minimum=1,
        maximum=MAX_MAX_PAGES_PER_SOURCE,
    )

    max_queries_per_title_location_pair = _as_bounded_int(
        request.get("max_queries_per_title_location_pair") or DEFAULT_MAX_QUERIES_PER_TITLE_LOCATION_PAIR,
        default=DEFAULT_MAX_QUERIES_PER_TITLE_LOCATION_PAIR,
        minimum=1,
        maximum=MAX_MAX_QUERIES_PER_TITLE_LOCATION_PAIR,
    )

    early_stop_when_no_new_results = _as_bool(request.get("early_stop_when_no_new_results"))
    if early_stop_when_no_new_results is None:
        early_stop_when_no_new_results = True

    requested_sources = [item.strip().lower() for item in _as_text_list(request.get("sources"))]
    if not requested_sources:
        requested_sources = [item.strip().lower() for item in _as_text_list(request.get("enabled_sources"))]
    if not requested_sources:
        requested_sources = list(DEFAULT_JOB_BOARDS)
        if isinstance(request.get("manual_jobs"), list) and request.get("manual_jobs"):
            requested_sources.append("manual")
    sources: list[str] = []
    for source in requested_sources:
        if source in SUPPORTED_JOB_SOURCES and source not in sources:
            sources.append(source)
    if not sources:
        sources = list(DEFAULT_JOB_BOARDS)

    profile_mode = str(request.get("profile_mode") or "resume_profile").strip().lower()
    if profile_mode not in {"resume_profile", "inline_resume", "none"}:
        profile_mode = "resume_profile"

    try:
        shortlist_max_items = int(request.get("shortlist_max_items") or request.get("shortlist_count") or 10)
    except (TypeError, ValueError):
        shortlist_max_items = 10
    shortlist_max_items = max(1, min(shortlist_max_items, 100))
    shortlist_min_score = _as_float(request.get("shortlist_min_score"))
    if shortlist_min_score is None:
        shortlist_min_score = 0.75
    try:
        shortlist_per_source_cap = int(request.get("shortlist_per_source_cap") or 3)
    except (TypeError, ValueError):
        shortlist_per_source_cap = 3
    shortlist_per_source_cap = max(1, min(shortlist_per_source_cap, 50))

    freshness_preference = _pick_text(request, ("shortlist_freshness_preference", "freshness_preference")) or "off"
    freshness_preference = freshness_preference.strip().lower().replace("-", "_").replace(" ", "_")
    if freshness_preference not in {"off", "prefer_recent", "strong_prefer_recent"}:
        freshness_preference = "off"

    freshness_weight_enabled = _as_bool(request.get("shortlist_freshness_weight_enabled"))
    if freshness_weight_enabled is None:
        freshness_weight_enabled = freshness_preference in {"prefer_recent", "strong_prefer_recent"}

    freshness_max_bonus = _as_float(request.get("shortlist_freshness_max_bonus"))
    if freshness_max_bonus is None:
        if freshness_preference == "prefer_recent":
            freshness_max_bonus = 6.0
        elif freshness_preference == "strong_prefer_recent":
            freshness_max_bonus = 12.0
        else:
            freshness_max_bonus = 0.0
    freshness_max_bonus = max(0.0, min(float(freshness_max_bonus), 20.0))

    digest_format = str(request.get("digest_format") or "compact").strip().lower() or "compact"

    notify_channels = [row.strip().lower() for row in _as_text_list(request.get("notify_channels"))]
    if not notify_channels:
        notify_channels = ["discord"]

    rank_llm_enabled = bool(request.get("rank_llm_enabled", True))
    digest_llm_enabled = bool(request.get("digest_llm_enabled", True))

    return {
        "query": query,
        "location": location,
        "titles": titles,
        "keywords": keywords,
        "excluded_keywords": excluded_keywords,
        "locations": locations,
        "work_mode_preference": work_mode_preference,
        "minimum_salary": minimum_salary,
        "experience_level": experience_level,
        "result_limit_per_source": max_jobs,
        "max_pages_per_source": max_pages_per_source,
        "max_queries_per_title_location_pair": max_queries_per_title_location_pair,
        "early_stop_when_no_new_results": bool(early_stop_when_no_new_results),
        "enabled_sources": list(sources),
        "collectors_enabled": collectors_enabled,
        "sources": sources,
        "max_jobs_per_source": max_jobs,
        "max_jobs_per_board": max_jobs,
        "manual_jobs": request.get("manual_jobs") if isinstance(request.get("manual_jobs"), list) else [],
        "board_url_overrides": request.get("board_url_overrides") if isinstance(request.get("board_url_overrides"), dict) else {},
        "profile_mode": profile_mode,
        "resume_text": _pick_text(request, ("resume_text",)),
        "resume_name": _pick_text(request, ("resume_name",)),
        "notify_on_empty": bool(request.get("notify_on_empty", False)),
        "desired_title": desired_title,
        "desired_title_keywords": desired_title_keywords,
        "excluded_title_keywords": excluded_title_keywords,
        "desired_salary_min": desired_salary_min,
        "desired_salary_max": _as_float(request.get("desired_salary_max")),
        "require_salary_data": bool(request.get("require_salary_data", False)),
        "experience_levels": experience_levels,
        "require_experience_match": bool(request.get("require_experience_match", False)),
        "clearance_required": request.get("clearance_required"),
        "required_clearances": _as_text_list(request.get("required_clearances")),
        "require_clearance_match": bool(request.get("require_clearance_match", False)),
        "work_modes": work_mode_preference,
        "require_work_mode_match": bool(request.get("require_work_mode_match", False)),
        "location_keywords": _as_text_list(request.get("location_keywords")),
        "shortlist_max_items": shortlist_max_items,
        "shortlist_min_score": float(shortlist_min_score),
        "shortlist_per_source_cap": shortlist_per_source_cap,
        "shortlist_diversity_mode": str(request.get("shortlist_diversity_mode") or "balanced_sources").strip().lower(),
        "shortlist_freshness_preference": freshness_preference,
        "shortlist_freshness_weight_enabled": bool(freshness_weight_enabled),
        "shortlist_freshness_max_bonus": float(freshness_max_bonus),
        "digest_format": digest_format,
        "notify_channels": notify_channels,
        "rank_llm_enabled": rank_llm_enabled,
        "digest_llm_enabled": digest_llm_enabled,
    }


def normalize_raw_jobs(raw: Any) -> tuple[list[dict[str, Any]], dict[str, int]]:
    if not isinstance(raw, list):
        return [], {"invalid_item_type": 0, "missing_title": 0}

    normalized: list[dict[str, Any]] = []
    drop_reasons = {"invalid_item_type": 0, "missing_title": 0}
    for item in raw:
        if not isinstance(item, dict):
            drop_reasons["invalid_item_type"] += 1
            continue

        title = _pick_text(item, ("title", "job_title", "name"))
        if not title:
            drop_reasons["missing_title"] += 1
            continue

        salary_min = _pick_numeric(item, ("salary_min", "min_salary", "salary_low"))
        salary_max = _pick_numeric(item, ("salary_max", "max_salary", "salary_high"))
        if salary_min is not None and salary_max is not None and salary_max < salary_min:
            salary_min, salary_max = salary_max, salary_min

        normalized.append(
            {
                "title": title,
                "company": _pick_text(item, ("company", "employer", "organization")),
                "location": _pick_text(item, ("location", "city", "region")),
                "url": _pick_text(item, ("url", "link", "job_url")),
                "source": _pick_text(item, ("source", "board")) or "manual",
                "salary_min": salary_min,
                "salary_max": salary_max,
                "salary_currency": _pick_text(item, ("salary_currency", "currency"))
                or ("USD" if salary_min is not None or salary_max is not None else None),
                "experience_level": _normalize_experience_level(item.get("experience_level")),
                "clearance_required": _as_bool(item.get("clearance_required")),
                "clearance_type": _pick_text(item, ("clearance_type", "clearance")),
                "work_mode": _normalize_work_mode(_pick_text(item, ("work_mode", "remote_type", "location_type"))),
                "posted_at": _pick_text(item, ("posted_at", "posted_date")),
                "scraped_at": _pick_text(item, ("scraped_at",)),
                "description_snippet": _pick_text(item, ("description_snippet", "summary", "description")),
                "raw": item.get("raw") if isinstance(item.get("raw"), dict) else {},
            }
        )

    return normalized, drop_reasons


def dedupe_jobs(jobs: list[dict[str, Any]], dedupe_keys: list[str]) -> tuple[list[dict[str, Any]], int]:
    if not dedupe_keys:
        dedupe_keys = ["source", "url", "title"]

    seen: set[tuple[str, ...]] = set()
    deduped: list[dict[str, Any]] = []
    duplicates = 0

    for job in jobs:
        key_parts: list[str] = []
        for key in dedupe_keys:
            value = job.get(key)
            if value is None:
                key_parts.append("")
            else:
                key_parts.append(str(value).strip().lower())
        key = tuple(key_parts)
        if key in seen:
            duplicates += 1
            continue
        seen.add(key)
        deduped.append(job)

    return deduped, duplicates


def source_counts(jobs: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for job in jobs:
        source = str(job.get("source") or "unknown").strip().lower()
        counts[source] = counts.get(source, 0) + 1
    return counts


def build_upstream_ref(task: Any, task_type: str) -> dict[str, str]:
    run_id = str(getattr(task, "_run_id", "") or "")
    return {
        "task_id": str(getattr(task, "id", "") or ""),
        "run_id": run_id,
        "task_type": task_type,
    }


def stage_idempotency_key(pipeline_id: str, next_task_type: str, upstream_run_id: str, *, prefix: str = "jobsv2") -> str:
    return f"{prefix}:{pipeline_id}:{next_task_type}:{upstream_run_id}"


def fetch_upstream_result_content_json(db: Any, upstream: dict[str, Any]) -> dict[str, Any]:
    task_id = str(upstream.get("task_id") or "").strip()
    run_id = str(upstream.get("run_id") or "").strip()
    if not task_id or not run_id:
        raise NonRetryableTaskError("upstream.task_id and upstream.run_id are required")

    row = db.execute(
        text(
            """
            SELECT content_json
            FROM artifacts
            WHERE task_id = :task_id
              AND run_id = :run_id
              AND artifact_type = 'result.json'
            ORDER BY created_at DESC, id DESC
            LIMIT 1
            """
        ),
        {"task_id": task_id, "run_id": run_id},
    ).first()

    if row is None:
        raise RuntimeError(f"upstream result artifact not found for task_id={task_id} run_id={run_id}")

    content = row[0]
    if isinstance(content, str):
        try:
            content = json.loads(content)
        except json.JSONDecodeError as exc:
            raise NonRetryableTaskError("upstream result artifact content_json is invalid JSON") from exc

    if not isinstance(content, dict):
        raise NonRetryableTaskError("upstream result artifact content_json must be an object")

    return content


def expect_artifact_type(content_json: dict[str, Any], expected: str) -> None:
    actual = str(content_json.get("artifact_type") or "")
    if actual != expected:
        raise NonRetryableTaskError(
            f"upstream contract mismatch: expected artifact_type='{expected}' got '{actual or 'missing'}'"
        )


def _matches_keywords(text: str, include: list[str], exclude: list[str]) -> bool:
    low = text.lower()
    if include and not any(keyword.lower() in low for keyword in include):
        return False
    if exclude and any(keyword.lower() in low for keyword in exclude):
        return False
    return True


def matches_filters(job: dict[str, Any], request: dict[str, Any]) -> bool:
    title = str(job.get("title") or "")
    location = str(job.get("location") or "")
    description = str(job.get("description_snippet") or "")
    experience = _normalize_experience_level(job.get("experience_level"))
    work_mode = _normalize_work_mode(job.get("work_mode"))

    include_titles = list(request.get("desired_title_keywords") or [])
    desired_title = request.get("desired_title")
    if isinstance(desired_title, str) and desired_title.strip():
        include_titles = [desired_title.strip()] + include_titles
    exclude_titles = list(request.get("excluded_title_keywords") or [])
    if not _matches_keywords(title, include_titles, exclude_titles):
        return False

    location_keywords = list(request.get("location_keywords") or [])
    if location_keywords:
        haystack = " ".join((title, location, description)).lower()
        if not any(keyword.lower() in haystack for keyword in location_keywords):
            return False

    desired_salary_min = _as_float(request.get("desired_salary_min"))
    desired_salary_max = _as_float(request.get("desired_salary_max"))
    require_salary_data = bool(request.get("require_salary_data"))
    salary_min = _as_float(job.get("salary_min"))
    salary_max = _as_float(job.get("salary_max"))
    if salary_min is not None and salary_max is not None and salary_max < salary_min:
        salary_min, salary_max = salary_max, salary_min

    if desired_salary_min is not None:
        if salary_max is None and salary_min is None and require_salary_data:
            return False
        if salary_max is not None and salary_max < desired_salary_min:
            return False
    if desired_salary_max is not None:
        if salary_max is None and salary_min is None and require_salary_data:
            return False
        if salary_min is not None and salary_min > desired_salary_max:
            return False

    desired_experience = {_normalize_experience_level(item) for item in list(request.get("experience_levels") or [])}
    desired_experience.discard(None)
    require_experience_match = bool(request.get("require_experience_match"))
    if desired_experience:
        if experience is None:
            if require_experience_match:
                return False
        elif experience not in desired_experience:
            return False

    clearance_pref = request.get("clearance_required")
    clearance_required = _as_bool(job.get("clearance_required"))
    if isinstance(clearance_pref, str):
        pref_low = clearance_pref.strip().lower()
        if pref_low in {"required", "true", "yes"}:
            if clearance_required is not True:
                return False
        elif pref_low in {"none", "false", "no"}:
            if clearance_required is True:
                return False
    else:
        pref_bool = _as_bool(clearance_pref)
        if pref_bool is True and clearance_required is not True:
            return False
        if pref_bool is False and clearance_required is True:
            return False

    required_clearances = [item.lower() for item in list(request.get("required_clearances") or [])]
    require_clearance_match = bool(request.get("require_clearance_match"))
    clearance_type = str(job.get("clearance_type") or "").strip().lower()
    if required_clearances:
        if not clearance_type:
            if require_clearance_match:
                return False
        elif not any(item in clearance_type for item in required_clearances):
            return False

    desired_work_modes = {_normalize_work_mode(item) for item in list(request.get("work_modes") or [])}
    desired_work_modes.discard(None)
    require_work_mode_match = bool(request.get("require_work_mode_match"))
    if desired_work_modes:
        if work_mode is None:
            if require_work_mode_match:
                return False
        elif work_mode not in desired_work_modes:
            return False

    return True


def score_job(job: dict[str, Any], request: dict[str, Any]) -> float:
    title = str(job.get("title") or "").lower()
    description = str(job.get("description_snippet") or "").lower()
    salary_min = _as_float(job.get("salary_min")) or 0.0
    salary_max = _as_float(job.get("salary_max")) or 0.0
    work_mode = _normalize_work_mode(job.get("work_mode"))
    experience_level = _normalize_experience_level(job.get("experience_level"))
    clearance_required = _as_bool(job.get("clearance_required")) is True

    desired_titles = list(request.get("desired_title_keywords") or [])
    desired_title = request.get("desired_title")
    if isinstance(desired_title, str) and desired_title.strip():
        desired_titles = [desired_title.strip()] + desired_titles

    title_hits = sum(1 for keyword in desired_titles if keyword.lower() in title)
    text_hits = sum(1 for keyword in desired_titles if keyword.lower() in description)
    salary_anchor = max(salary_min, salary_max)
    salary_score = min(salary_anchor / 250000.0, 1.4) if salary_anchor > 0 else 0.0

    score = 0.0
    score += min(title_hits * 0.25, 1.0)
    score += min(text_hits * 0.12, 0.36)
    score += salary_score
    if work_mode == "remote":
        score += 0.2
    elif work_mode == "hybrid":
        score += 0.1
    if clearance_required:
        score += 0.1
    if experience_level in {"entry", "mid"}:
        score += 0.08
    if experience_level == "senior":
        score += 0.03
    return round(score, 4)


def fit_tier(score: float) -> str:
    if score >= 1.6:
        return "strong_match"
    if score >= 1.15:
        return "good_match"
    if score >= 0.75:
        return "stretch_match"
    return "low_match"


def resolve_profile_context(request: dict[str, Any]) -> dict[str, Any]:
    profile_mode = str(request.get("profile_mode") or "resume_profile").strip().lower()

    inline_resume_text = request.get("resume_text")
    inline_resume_name = request.get("resume_name")

    if profile_mode == "none":
        return {
            "enabled": False,
            "applied": False,
            "source": "disabled",
            "resume_name": None,
            "updated_at": None,
            "resume_char_count": 0,
            "resume_sent_char_count": 0,
            "resume_truncated": False,
            "resume_text": None,
        }

    if profile_mode == "inline_resume" and isinstance(inline_resume_text, str) and inline_resume_text.strip():
        normalized = inline_resume_text.replace("\r\n", "\n").strip()
        truncated = normalized[:MAX_RESUME_CHARS_FOR_LLM]
        return {
            "enabled": True,
            "applied": True,
            "source": "payload",
            "resume_name": inline_resume_name,
            "updated_at": None,
            "resume_char_count": len(normalized),
            "resume_sent_char_count": len(truncated),
            "resume_truncated": len(normalized) > len(truncated),
            "resume_text": truncated,
        }

    try:
        stored = get_stored_resume_profile(include_text=True)
    except Exception:
        stored = None

    if isinstance(stored, dict):
        stored_text = stored.get("resume_text")
        if isinstance(stored_text, str) and stored_text.strip():
            normalized = stored_text.replace("\r\n", "\n").strip()
            truncated = normalized[:MAX_RESUME_CHARS_FOR_LLM]
            return {
                "enabled": True,
                "applied": True,
                "source": "stored_profile",
                "resume_name": stored.get("resume_name"),
                "updated_at": stored.get("updated_at"),
                "resume_char_count": len(normalized),
                "resume_sent_char_count": len(truncated),
                "resume_truncated": len(normalized) > len(truncated),
                "resume_text": truncated,
            }

    return {
        "enabled": True,
        "applied": False,
        "source": "stored_profile_missing",
        "resume_name": None,
        "updated_at": None,
        "resume_char_count": 0,
        "resume_sent_char_count": 0,
        "resume_truncated": False,
        "resume_text": None,
    }
