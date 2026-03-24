import json
import uuid
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import text

from candidate_profile import get_resume_profile as get_stored_resume_profile
from task_handlers.errors import NonRetryableTaskError
from task_handlers.jobs_normalize_helpers import metadata_quality_details

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
DEFAULT_MAX_QUERIES_PER_RUN = 12
MAX_MAX_QUERIES_PER_RUN = 20
DEFAULT_ENABLE_QUERY_EXPANSION = True
DEFAULT_MAX_TOTAL_JOBS = 2000
MAX_MAX_TOTAL_JOBS = 5000
DEFAULT_JOBS_NOTIFICATION_COOLDOWN_DAYS = 3
MAX_JOBS_NOTIFICATION_COOLDOWN_DAYS = 30
DEFAULT_JOBS_SHORTLIST_REPEAT_PENALTY = 4.0
MAX_JOBS_SHORTLIST_REPEAT_PENALTY = 20.0
DEFAULT_RESURFACE_SEEN_JOBS = True
DEFAULT_SHORTLIST_FAIL_SOFT_ENABLED = True
DEFAULT_SHORTLIST_FALLBACK_MIN_ITEMS = 3
MAX_SHORTLIST_FALLBACK_MIN_ITEMS = 20
DEFAULT_SHORTLIST_FALLBACK_MIN_SCORE = 0.08
DEFAULT_SEARCH_MODE = "broad_discovery"
ALLOWED_SEARCH_MODES = {"broad_discovery", "precision_match"}

_TITLE_FAMILY_ALIASES: dict[str, tuple[str, ...]] = {
    "software_engineering": (
        "software engineer",
        "software developer",
        "backend engineer",
        "backend developer",
        "backend software engineer",
        "backend software developer",
        "full stack engineer",
        "full stack developer",
        "fullstack engineer",
        "fullstack developer",
        "application engineer",
        "swe",
    ),
}
_ENTRY_HINTS = (
    "entry level",
    "entry-level",
    "junior",
    "jr ",
    " jr",
    "new grad",
    "new graduate",
    "graduate",
    "software engineer i",
    "engineer i",
    "associate",
)
_WORK_MODE_HINTS: dict[str, tuple[str, ...]] = {
    "remote": (" remote ", "remote-friendly", "remote friendly", "work from home", "wfh", "anywhere"),
    "hybrid": (" hybrid ",),
    "onsite": ("on site", "on-site", " onsite ", "in office", "in-office"),
}


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


def _canonical_text(value: Any) -> str:
    text = str(value or "").strip().lower()
    text = "".join(ch if ch.isalnum() or ch.isspace() else " " for ch in text)
    return " ".join(text.split())


def _contains_phrase(text: str, phrase: str) -> bool:
    candidate = _canonical_text(text)
    target = _canonical_text(phrase)
    if not candidate or not target:
        return False
    if candidate == target:
        return True
    return f" {target} " in f" {candidate} "


def _contains_any_phrase(text: str, phrases: tuple[str, ...]) -> bool:
    return any(_contains_phrase(text, phrase) for phrase in phrases)


def _request_title_phrases(request: dict[str, Any]) -> list[str]:
    phrases = _dedupe_text_list(
        [
            _pick_text(request, ("desired_title",)) or "",
            _pick_text(request, ("query", "search_query")) or "",
            *_as_text_list(request.get("titles")),
        ]
    )
    return [_canonical_text(value) for value in phrases if _canonical_text(value)]


def _active_title_families(request: dict[str, Any]) -> set[str]:
    families: set[str] = set()
    for phrase in _request_title_phrases(request):
        for family_name, aliases in _TITLE_FAMILY_ALIASES.items():
            if _contains_any_phrase(phrase, aliases):
                families.add(family_name)
    return families


def _normalize_search_mode(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip().lower().replace("-", "_").replace(" ", "_")
    if normalized in ALLOWED_SEARCH_MODES:
        return normalized
    return None


def is_broad_discovery_request(request: dict[str, Any]) -> bool:
    explicit_titles = _dedupe_text_list(
        [value for value in [_pick_text(request, ("desired_title",)), *_as_text_list(request.get("titles"))] if value]
    )
    explicit_title_phrases = [_canonical_text(value) for value in explicit_titles if _canonical_text(value)]
    if explicit_title_phrases:
        generic_title_request = all(
            any(_contains_any_phrase(phrase, aliases) for aliases in _TITLE_FAMILY_ALIASES.values())
            for phrase in explicit_title_phrases
        )
    else:
        generic_title_request = bool(_active_title_families(request))

    if not generic_title_request:
        return False

    has_specific_constraints = bool(
        _as_text_list(request.get("desired_title_keywords"))
        or _as_text_list(request.get("keywords"))
        or _as_text_list(request.get("work_modes"))
        or _as_text_list(request.get("work_mode_preference"))
        or _as_text_list(request.get("experience_levels"))
        or _as_text_list(request.get("location_keywords"))
        or _pick_text(request, ("experience_level",))
        or _as_float(request.get("minimum_salary"))
        or _as_float(request.get("desired_salary_min"))
        or _as_float(request.get("desired_salary_max"))
    )
    return not has_specific_constraints


def _infer_work_mode_from_text(*parts: Any) -> str | None:
    haystack = f" {' '.join(_canonical_text(part) for part in parts if part)} "
    if not haystack.strip():
        return None
    for mode, hints in _WORK_MODE_HINTS.items():
        if any(hint in haystack for hint in hints):
            return mode
    return None


def _infer_experience_from_text(*parts: Any) -> str | None:
    haystack = f" {' '.join(_canonical_text(part) for part in parts if part)} "
    if not haystack.strip():
        return None
    if any(hint in haystack for hint in _ENTRY_HINTS):
        return "entry"
    return None


def deterministic_job_signals(job: dict[str, Any], request: dict[str, Any]) -> dict[str, Any]:
    title = _canonical_text(job.get("title"))
    description = _canonical_text(job.get("description_snippet"))
    location = _canonical_text(job.get("location"))
    haystack = " ".join(part for part in (title, description, location) if part)
    request_titles = _request_title_phrases(request)
    request_keywords = [_canonical_text(value) for value in _as_text_list(request.get("keywords")) if _canonical_text(value)]
    active_families = _active_title_families(request)

    title_family_hits = 0
    for family_name in active_families:
        aliases = _TITLE_FAMILY_ALIASES.get(family_name, ())
        if _contains_any_phrase(title, aliases):
            title_family_hits += 1

    title_exact_hits = sum(1 for phrase in request_titles if phrase and _contains_phrase(title, phrase))
    keyword_hits = sum(1 for phrase in request_keywords if phrase and _contains_phrase(haystack, phrase))

    work_mode = _normalize_work_mode(job.get("work_mode")) or _infer_work_mode_from_text(job.get("title"), job.get("location"), job.get("description_snippet"))
    experience_level = _normalize_experience_level(job.get("experience_level")) or _infer_experience_from_text(job.get("title"), job.get("description_snippet"))

    salary_min = _as_float(job.get("salary_min")) or 0.0
    salary_max = _as_float(job.get("salary_max")) or 0.0
    salary_anchor = max(salary_min, salary_max)

    metadata = metadata_quality_details(job)
    metadata_quality_score = max(0.0, min(float(metadata.get("metadata_quality_score") or 0.0), 100.0))
    source_url_kind = str(metadata.get("source_url_kind") or "").strip().lower()
    has_direct_source_url = bool(metadata.get("has_direct_source_url"))
    broad_discovery = is_broad_discovery_request(request)

    title_signal = min(title_family_hits * 0.62 + title_exact_hits * 0.28, 1.7)
    keyword_signal = min(keyword_hits * 0.14, 0.42)
    salary_signal = min(salary_anchor / 260000.0, 0.55) if salary_anchor > 0 else 0.0
    work_mode_signal = 0.22 if work_mode == "remote" else 0.12 if work_mode == "hybrid" else 0.0
    experience_signal = (
        0.18
        if experience_level in {"entry", "internship"}
        else 0.1
        if experience_level == "mid"
        else 0.05
        if experience_level == "senior"
        else 0.0
    )
    metadata_signal = min(metadata_quality_score / 100.0 * 0.28, 0.28)
    direct_source_bonus = 0.12 if has_direct_source_url else 0.05 if source_url_kind not in {"", "missing"} else 0.0
    broad_discovery_bonus = 0.18 if broad_discovery and title_family_hits > 0 else 0.0

    total_score = round(
        title_signal
        + keyword_signal
        + salary_signal
        + work_mode_signal
        + experience_signal
        + metadata_signal
        + direct_source_bonus
        + broad_discovery_bonus,
        4,
    )

    return {
        "broad_discovery": broad_discovery,
        "title_family_hits": title_family_hits,
        "title_exact_hits": title_exact_hits,
        "keyword_hits": keyword_hits,
        "title_signal": round(title_signal, 4),
        "keyword_signal": round(keyword_signal, 4),
        "salary_signal": round(salary_signal, 4),
        "work_mode_signal": round(work_mode_signal, 4),
        "experience_signal": round(experience_signal, 4),
        "metadata_signal": round(metadata_signal, 4),
        "direct_source_bonus": round(direct_source_bonus, 4),
        "metadata_quality_score": round(metadata_quality_score, 2),
        "source_url_kind": source_url_kind or None,
        "has_direct_source_url": has_direct_source_url,
        "inferred_work_mode": work_mode,
        "inferred_experience_level": experience_level,
        "score": total_score,
    }


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

    search_mode = _normalize_search_mode(request.get("search_mode"))
    if search_mode is None:
        inferred_mode = {
            "query": query,
            "desired_title": desired_title,
            "titles": titles,
            "keywords": keywords,
            "work_modes": work_mode_preference,
            "experience_levels": experience_levels,
            "location_keywords": _as_text_list(request.get("location_keywords")),
            "minimum_salary": minimum_salary,
            "desired_salary_min": desired_salary_min,
            "desired_salary_max": _as_float(request.get("desired_salary_max")),
        }
        search_mode = "broad_discovery" if is_broad_discovery_request(inferred_mode) else "precision_match"

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

    max_queries_per_run = _as_bounded_int(
        request.get("max_queries_per_run") or (14 if search_mode == "broad_discovery" else 8),
        default=14 if search_mode == "broad_discovery" else 8,
        minimum=1,
        maximum=MAX_MAX_QUERIES_PER_RUN,
    )

    enable_query_expansion = _as_bool(request.get("enable_query_expansion"))
    if enable_query_expansion is None:
        enable_query_expansion = True if search_mode == "broad_discovery" else False

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

    max_total_jobs = _as_bounded_int(
        request.get("max_total_jobs")
        or min(max_jobs * max(len(sources), 1), DEFAULT_MAX_TOTAL_JOBS),
        default=min(max_jobs * max(len(sources), 1), DEFAULT_MAX_TOTAL_JOBS),
        minimum=1,
        maximum=MAX_MAX_TOTAL_JOBS,
    )

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
        shortlist_min_score = 0.5 if search_mode == "broad_discovery" else 0.85
    try:
        shortlist_per_source_cap = int(request.get("shortlist_per_source_cap") or 3)
    except (TypeError, ValueError):
        shortlist_per_source_cap = 3
    shortlist_per_source_cap = max(1, min(shortlist_per_source_cap, 50))

    freshness_preference = _pick_text(request, ("shortlist_freshness_preference", "freshness_preference")) or "off"
    freshness_preference = freshness_preference.strip().lower().replace("-", "_").replace(" ", "_")
    if freshness_preference not in {"off", "prefer_recent", "strong_prefer_recent"}:
        freshness_preference = "off"

    prefer_recent = _as_bool(request.get("prefer_recent"))
    if prefer_recent is None:
        prefer_recent = freshness_preference in {"prefer_recent", "strong_prefer_recent"}
    if prefer_recent and freshness_preference == "off":
        freshness_preference = "prefer_recent"

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

    jobs_notification_cooldown_days = _as_bounded_int(
        request.get("jobs_notification_cooldown_days") or (3 if search_mode == "broad_discovery" else 7),
        default=3 if search_mode == "broad_discovery" else 7,
        minimum=0,
        maximum=MAX_JOBS_NOTIFICATION_COOLDOWN_DAYS,
    )
    repeat_penalty_raw = request.get("jobs_shortlist_repeat_penalty")
    try:
        jobs_shortlist_repeat_penalty = float(
            repeat_penalty_raw if repeat_penalty_raw is not None else DEFAULT_JOBS_SHORTLIST_REPEAT_PENALTY
        )
    except (TypeError, ValueError):
        jobs_shortlist_repeat_penalty = DEFAULT_JOBS_SHORTLIST_REPEAT_PENALTY
    jobs_shortlist_repeat_penalty = max(0.0, min(jobs_shortlist_repeat_penalty, MAX_JOBS_SHORTLIST_REPEAT_PENALTY))
    resurface_seen_jobs = _as_bool(request.get("resurface_seen_jobs"))
    if resurface_seen_jobs is None:
        resurface_seen_jobs = DEFAULT_RESURFACE_SEEN_JOBS

    notify_channels = [row.strip().lower() for row in _as_text_list(request.get("notify_channels"))]
    if not notify_channels:
        notify_channels = ["discord"]

    rank_llm_enabled = bool(request.get("rank_llm_enabled", True))
    digest_llm_enabled = bool(request.get("digest_llm_enabled", True))

    shortlist_fail_soft_enabled = _as_bool(request.get("shortlist_fail_soft_enabled"))
    if shortlist_fail_soft_enabled is None:
        shortlist_fail_soft_enabled = search_mode == "broad_discovery"

    shortlist_fallback_min_items = _as_bounded_int(
        request.get("shortlist_fallback_min_items") or (5 if search_mode == "broad_discovery" else 0),
        default=5 if search_mode == "broad_discovery" else 0,
        minimum=0,
        maximum=MAX_SHORTLIST_FALLBACK_MIN_ITEMS,
    )

    shortlist_fallback_min_score = _as_float(request.get("shortlist_fallback_min_score"))
    if shortlist_fallback_min_score is None:
        shortlist_fallback_min_score = DEFAULT_SHORTLIST_FALLBACK_MIN_SCORE if search_mode == "broad_discovery" else 0.2

    require_experience_match = _as_bool(request.get("require_experience_match"))
    if require_experience_match is None:
        require_experience_match = search_mode == "precision_match" and bool(experience_levels)

    require_work_mode_match = _as_bool(request.get("require_work_mode_match"))
    if require_work_mode_match is None:
        require_work_mode_match = search_mode == "precision_match" and bool(work_mode_preference)

    require_keyword_match = _as_bool(request.get("require_keyword_match"))
    if require_keyword_match is None:
        require_keyword_match = search_mode == "precision_match" and bool(keywords)

    return {
        "query": query,
        "location": location,
        "search_mode": search_mode,
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
        "max_queries_per_run": max_queries_per_run,
        "enable_query_expansion": bool(enable_query_expansion),
        "early_stop_when_no_new_results": bool(early_stop_when_no_new_results),
        "enabled_sources": list(sources),
        "collectors_enabled": collectors_enabled,
        "sources": sources,
        "max_jobs_per_source": max_jobs,
        "max_jobs_per_board": max_jobs,
        "max_total_jobs": max_total_jobs,
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
        "require_experience_match": bool(require_experience_match),
        "clearance_required": request.get("clearance_required"),
        "required_clearances": _as_text_list(request.get("required_clearances")),
        "require_clearance_match": bool(request.get("require_clearance_match", False)),
        "work_modes": work_mode_preference,
        "require_work_mode_match": bool(require_work_mode_match),
        "require_keyword_match": bool(require_keyword_match),
        "location_keywords": _as_text_list(request.get("location_keywords")),
        "shortlist_max_items": shortlist_max_items,
        "shortlist_min_score": float(shortlist_min_score),
        "shortlist_per_source_cap": shortlist_per_source_cap,
        "shortlist_diversity_mode": str(request.get("shortlist_diversity_mode") or "balanced_sources").strip().lower(),
        "prefer_recent": bool(prefer_recent),
        "shortlist_freshness_preference": freshness_preference,
        "shortlist_freshness_weight_enabled": bool(freshness_weight_enabled),
        "shortlist_freshness_max_bonus": float(freshness_max_bonus),
        "shortlist_fail_soft_enabled": bool(shortlist_fail_soft_enabled),
        "shortlist_fallback_min_items": shortlist_fallback_min_items,
        "shortlist_fallback_min_score": float(shortlist_fallback_min_score),
        "digest_format": digest_format,
        "jobs_notification_cooldown_days": jobs_notification_cooldown_days,
        "jobs_shortlist_repeat_penalty": float(jobs_shortlist_repeat_penalty),
        "resurface_seen_jobs": bool(resurface_seen_jobs),
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
    search_mode = _normalize_search_mode(request.get("search_mode")) or DEFAULT_SEARCH_MODE
    experience = _normalize_experience_level(job.get("experience_level"))
    work_mode = _normalize_work_mode(job.get("work_mode"))

    include_titles = list(request.get("desired_title_keywords") or [])
    desired_title = request.get("desired_title")
    if isinstance(desired_title, str) and desired_title.strip():
        include_titles = [desired_title.strip()] + include_titles
    if search_mode == "precision_match" and not include_titles:
        include_titles = [value for value in list(request.get("titles") or []) if isinstance(value, str) and value.strip()]
    exclude_titles = list(request.get("excluded_title_keywords") or [])
    if not _matches_keywords(title, include_titles, exclude_titles):
        return False

    require_keyword_match = bool(request.get("require_keyword_match"))
    keywords = [value for value in list(request.get("keywords") or []) if isinstance(value, str) and value.strip()]
    if keywords and require_keyword_match:
        haystack = " ".join((title, location, description)).lower()
        if not any(keyword.lower() in haystack for keyword in keywords):
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
    signals = deterministic_job_signals(job, request)
    score = float(signals.get("score") or 0.0)
    if _as_bool(job.get("clearance_required")) is True:
        score += 0.08
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
