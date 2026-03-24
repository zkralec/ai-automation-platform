from __future__ import annotations

from typing import Any

from integrations.job_boards_scrape import collect_jobs_from_board
from integrations.jobs_collectors.query_expansion import (
    DEFAULT_CONSECUTIVE_EMPTY_QUERIES_STOP,
    DEFAULT_MAX_QUERIES_PER_RUN,
    MAX_MAX_QUERIES_PER_RUN,
    build_query_plan,
)

DEFAULT_QUERY = "software engineer"
DEFAULT_LOCATION = "United States"
DEFAULT_RESULT_LIMIT_PER_SOURCE = 250
MAX_RESULT_LIMIT_PER_SOURCE = 1000
MIN_RESULT_LIMIT_PER_SOURCE = 1
DEFAULT_MAX_PAGES_PER_SOURCE = 5
MAX_MAX_PAGES_PER_SOURCE = 20
DEFAULT_MAX_QUERIES_PER_TITLE_LOCATION_PAIR = 4
MAX_MAX_QUERIES_PER_TITLE_LOCATION_PAIR = 10
DEFAULT_ENABLE_QUERY_EXPANSION = True


def _as_text_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    output: list[str] = []
    for item in value:
        if isinstance(item, str) and item.strip():
            output.append(item.strip())
    return output


def _dedupe_text_list(values: list[str]) -> list[str]:
    output: list[str] = []
    seen: set[str] = set()
    for value in values:
        low = value.strip().lower()
        if not low or low in seen:
            continue
        seen.add(low)
        output.append(value.strip())
    return output


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        stripped = value.strip().replace(",", "")
        if stripped.startswith("$"):
            stripped = stripped[1:]
        if not stripped:
            return None
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
        if low in {"true", "yes", "1"}:
            return True
        if low in {"false", "no", "0"}:
            return False
    return None


def _normalize_query(request: dict[str, Any]) -> str:
    query = _explicit_query(request)
    titles = _as_text_list(request.get("titles"))
    keywords = _as_text_list(request.get("keywords"))

    if query:
        return query

    parts: list[str] = []
    if titles:
        parts.append(titles[0])
    if keywords:
        parts.extend(keywords[:3])
    query = " ".join(parts).strip()
    return query or DEFAULT_QUERY


def _explicit_query(request: dict[str, Any]) -> str:
    return str(request.get("query") or request.get("search_query") or "").strip()


def _normalize_locations(request: dict[str, Any]) -> list[str]:
    locations = _dedupe_text_list(_as_text_list(request.get("locations")))
    if locations:
        return locations

    location = str(request.get("location") or request.get("search_location") or "").strip()
    if location:
        return [location]
    return [DEFAULT_LOCATION]


def _normalize_result_limit(request: dict[str, Any]) -> int:
    try:
        max_jobs = int(
            request.get("result_limit_per_source")
            or request.get("max_jobs_per_source")
            or request.get("max_jobs_per_board")
            or DEFAULT_RESULT_LIMIT_PER_SOURCE
        )
    except (TypeError, ValueError):
        max_jobs = DEFAULT_RESULT_LIMIT_PER_SOURCE
    return max(MIN_RESULT_LIMIT_PER_SOURCE, min(max_jobs, MAX_RESULT_LIMIT_PER_SOURCE))


def _normalize_max_pages(request: dict[str, Any]) -> int:
    try:
        value = int(request.get("max_pages_per_source") or DEFAULT_MAX_PAGES_PER_SOURCE)
    except (TypeError, ValueError):
        value = DEFAULT_MAX_PAGES_PER_SOURCE
    return max(1, min(value, MAX_MAX_PAGES_PER_SOURCE))


def _normalize_max_queries(request: dict[str, Any]) -> int:
    try:
        value = int(
            request.get("max_queries_per_title_location_pair") or DEFAULT_MAX_QUERIES_PER_TITLE_LOCATION_PAIR
        )
    except (TypeError, ValueError):
        value = DEFAULT_MAX_QUERIES_PER_TITLE_LOCATION_PAIR
    return max(1, min(value, MAX_MAX_QUERIES_PER_TITLE_LOCATION_PAIR))


def _normalize_max_queries_per_run(request: dict[str, Any]) -> int:
    try:
        value = int(request.get("max_queries_per_run") or DEFAULT_MAX_QUERIES_PER_RUN)
    except (TypeError, ValueError):
        value = DEFAULT_MAX_QUERIES_PER_RUN
    return max(1, min(value, MAX_MAX_QUERIES_PER_RUN))


def _normalize_enable_query_expansion(request: dict[str, Any]) -> bool:
    parsed = _as_bool(request.get("enable_query_expansion"))
    if parsed is None:
        return DEFAULT_ENABLE_QUERY_EXPANSION
    return parsed


def _normalize_early_stop(request: dict[str, Any]) -> bool:
    parsed = _as_bool(request.get("early_stop_when_no_new_results"))
    if parsed is None:
        return True
    return parsed


def _normalize_work_mode_preferences(request: dict[str, Any]) -> set[str]:
    values = _as_text_list(request.get("work_mode_preference"))
    if not values:
        single = str(request.get("work_mode_preference") or "").strip()
        if single:
            values = [single]
    if not values:
        values = _as_text_list(request.get("work_modes"))

    normalized: set[str] = set()
    for row in values:
        low = row.lower().strip()
        if low in {"remote", "hybrid", "onsite", "on-site"}:
            normalized.add("onsite" if low == "on-site" else low)
    return normalized


def _normalize_experience_preferences(request: dict[str, Any]) -> set[str]:
    values = _as_text_list(request.get("experience_levels"))
    single = str(request.get("experience_level") or "").strip()
    if single:
        values.append(single)

    normalized: set[str] = set()
    for row in values:
        low = row.lower().strip()
        if low in {"intern", "internship", "co-op", "coop"}:
            normalized.add("internship")
        elif low in {"entry", "entry-level", "junior", "new grad", "associate"}:
            normalized.add("entry")
        elif low in {"mid", "mid-level", "intermediate"}:
            normalized.add("mid")
        elif low in {"senior", "lead", "staff", "principal", "manager", "director"}:
            normalized.add("senior")
        elif low:
            normalized.add(low)
    return normalized


def _job_matches_basic_filters(job: dict[str, Any], request: dict[str, Any]) -> bool:
    title = str(job.get("title") or "")
    description = str(job.get("description_snippet") or "")
    haystack = f"{title} {description}".lower()

    exclude_terms = _as_text_list(request.get("excluded_keywords")) + _as_text_list(request.get("excluded_title_keywords"))
    if exclude_terms and any(term.lower() in haystack for term in exclude_terms):
        return False

    return True


def _job_key(job: dict[str, Any]) -> tuple[str, str, str]:
    return (
        str(job.get("source") or "").strip().lower(),
        str(job.get("url") or "").strip(),
        str(job.get("title") or "").strip().lower(),
    )


def _metadata_diagnostics(job: dict[str, Any]) -> dict[str, bool]:
    return {
        "missing_company": not bool(str(job.get("company") or "").strip()),
        "missing_posted_at": not bool(str(job.get("posted_at") or "").strip()),
        "missing_source_url": not bool(str(job.get("source_url") or "").strip()),
        "missing_location": not bool(str(job.get("location") or "").strip()),
    }


def _empty_metadata_summary() -> dict[str, int]:
    return {
        "job_count": 0,
        "missing_company": 0,
        "missing_posted_at": 0,
        "missing_source_url": 0,
        "missing_location": 0,
    }


def _accumulate_metadata_summary(summary: dict[str, int], diagnostics: dict[str, bool]) -> None:
    summary["job_count"] = int(summary.get("job_count", 0)) + 1
    for key in ("missing_company", "missing_posted_at", "missing_source_url", "missing_location"):
        summary[key] = int(summary.get(key, 0)) + int(bool(diagnostics.get(key)))


def _company_frequency(jobs: list[dict[str, Any]], *, limit: int = 5) -> list[dict[str, Any]]:
    counts: dict[str, int] = {}
    labels: dict[str, str] = {}
    for job in jobs:
        company = str(job.get("company") or "").strip()
        if not company:
            continue
        key = company.lower()
        counts[key] = counts.get(key, 0) + 1
        labels.setdefault(key, company)
    ordered = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    return [{"company": labels[key], "count": count} for key, count in ordered[:limit]]


def _title_seeds(request: dict[str, Any]) -> list[str]:
    titles = _dedupe_text_list(_as_text_list(request.get("titles")))
    desired_title = str(request.get("desired_title") or "").strip()
    if desired_title and desired_title.lower() not in {row.lower() for row in titles}:
        titles.insert(0, desired_title)
    explicit_query = _explicit_query(request)
    if explicit_query and explicit_query.lower() not in {row.lower() for row in titles}:
        titles.insert(0, explicit_query)
    return titles or [_normalize_query(request)]


def _query_variants(request: dict[str, Any], *, title_seed: str, max_queries: int) -> list[str]:
    explicit_query = str(request.get("query") or request.get("search_query") or "").strip()
    keywords = _dedupe_text_list(
        _as_text_list(request.get("keywords")) + _as_text_list(request.get("desired_title_keywords"))
    )

    queries: list[str] = []
    seen: set[str] = set()

    def add(value: str) -> None:
        normalized = value.strip()
        low = normalized.lower()
        if not normalized or low in seen:
            return
        seen.add(low)
        queries.append(normalized)

    add(explicit_query)
    add(title_seed)
    if title_seed and keywords:
        for keyword in keywords[: max_queries]:
            add(f"{title_seed} {keyword}")
        if len(keywords) >= 2:
            add(f"{title_seed} {' '.join(keywords[:2])}")
    elif explicit_query and keywords:
        for keyword in keywords[: max_queries]:
            add(f"{explicit_query} {keyword}")

    add(_normalize_query(request))
    return queries[:max_queries]


def _normalize_job(board: str, row: dict[str, Any], *, url_override: str | None) -> dict[str, Any]:
    raw = row.get("raw") if isinstance(row.get("raw"), dict) else {}
    source_url = str(row.get("url") or raw.get("job_url") or raw.get("search_url") or url_override or "").strip() or None
    job = {
        "source": board,
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
        "source_metadata": raw,
    }
    job["metadata_diagnostics"] = _metadata_diagnostics(job)
    return job


def _split_warnings_and_errors(board: str, messages: list[str]) -> tuple[list[str], list[str]]:
    warnings: list[str] = []
    errors: list[str] = []
    for message in messages:
        text = str(message).strip()
        if not text:
            continue
        low = text.lower()
        prefixed = text if low.startswith(f"{board}:") else f"{board}: {text}"
        if (
            "fetch_failed" in low
            or "fetch_blocked_" in low
            or "fetch_not_found_" in low
            or "fetch_http_" in low
            or "unsupported_board" in low
        ):
            errors.append(prefixed)
        else:
            warnings.append(prefixed)
    return warnings, errors


def supported_fields(board: str | None = None) -> dict[str, Any]:
    source = (board or "").strip().lower() or "generic"
    return {
        "source": source,
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
        "max_queries_per_run": True,
        "enable_query_expansion": True,
        "max_total_jobs": True,
        "jobs_notification_cooldown_days": True,
        "jobs_shortlist_repeat_penalty": True,
        "resurface_seen_jobs": True,
        "early_stop_when_no_new_results": True,
        "enabled_sources": True,
        "input_mode": {
            "titles": "query_plus_post_filter",
            "keywords": "query_plus_post_filter",
            "excluded_keywords": "post_filter",
            "locations": "multi_location_search",
            "work_mode_preference": "rank_and_shortlist_filter",
            "minimum_salary": "rank_and_shortlist_filter",
            "experience_level": "rank_and_shortlist_filter",
            "result_limit_per_source": "collector_limit",
            "max_pages_per_source": "pagination_limit",
            "max_jobs_per_source": "collector_limit",
            "max_queries_per_title_location_pair": "query_expansion_limit",
            "max_queries_per_run": "run_level_query_cap",
            "enable_query_expansion": "query_expansion_toggle",
            "max_total_jobs": "pipeline_run_cap",
            "jobs_notification_cooldown_days": "shortlist_history_policy",
            "jobs_shortlist_repeat_penalty": "shortlist_history_policy",
            "resurface_seen_jobs": "shortlist_history_policy",
            "early_stop_when_no_new_results": "pagination_stop_condition",
            "enabled_sources": "pipeline_routing",
        },
        "source_metadata_fields": ["search_url"],
    }


def collect_board_jobs(board: str, request: dict[str, Any], *, url_override: str | None = None) -> dict[str, Any]:
    query = _normalize_query(request)
    explicit_query = _explicit_query(request)
    title_seeds = _title_seeds(request)
    locations = _normalize_locations(request)
    max_jobs = _normalize_result_limit(request)
    max_pages = _normalize_max_pages(request)
    max_queries = _normalize_max_queries(request)
    max_queries_per_run = _normalize_max_queries_per_run(request)
    enable_query_expansion = _normalize_enable_query_expansion(request)
    early_stop_when_no_new_results = _normalize_early_stop(request)
    work_mode_preferences = _normalize_work_mode_preferences(request)
    experience_preferences = sorted(_normalize_experience_preferences(request))

    query_plan = build_query_plan(
        explicit_query=explicit_query,
        title_seeds=title_seeds,
        locations=locations,
        keywords=_dedupe_text_list(
            _as_text_list(request.get("keywords")) + _as_text_list(request.get("desired_title_keywords"))
        ),
        experience_levels=experience_preferences,
        work_mode_preferences=work_mode_preferences,
        max_queries_per_run=max_queries_per_run,
        max_queries_per_title_location_pair=max_queries,
        enable_query_expansion=enable_query_expansion,
    )

    raw_candidates: list[dict[str, Any]] = []
    warnings: list[str] = []
    errors: list[str] = []
    locations_attempted: list[str] = []
    queries_attempted: list[str] = []
    failed_locations: list[str] = []
    search_attempts: list[dict[str, Any]] = []
    jobs_found_per_query: list[dict[str, Any]] = []
    discovered_raw_count = 0
    kept_after_basic_filter_count = 0
    dropped_by_basic_filter_count = 0
    deduped_count = 0
    pages_fetched = 0
    metadata_completeness_summary = _empty_metadata_summary()
    empty_queries_count = 0
    consecutive_empty_queries = 0
    seen_candidate_keys: set[tuple[str, str, str]] = set()

    for query_index, query_spec in enumerate(query_plan, start=1):
        query_variant = str(query_spec.get("query") or "").strip()
        location = str(query_spec.get("location") or "").strip() or (locations[0] if locations else DEFAULT_LOCATION)
        title_seed = str(query_spec.get("title_seed") or "").strip() or (title_seeds[0] if title_seeds else query_variant)
        expansion_type = str(query_spec.get("expansion_type") or "base_title").strip()
        if not query_variant:
            continue

        if location not in locations_attempted:
            locations_attempted.append(location)
        queries_attempted.append(query_variant)

        jobs, board_messages, board_meta = collect_jobs_from_board(
            board,
            query=query_variant,
            location=location,
            max_jobs=max_jobs,
            max_pages=max_pages,
            early_stop_when_no_new_results=early_stop_when_no_new_results,
            url_override=url_override,
        )
        pages_fetched += int(board_meta.get("pages_fetched") or 0)
        discovered_raw_count += int(board_meta.get("discovered_raw_count") or len(jobs))

        board_warnings, board_errors = _split_warnings_and_errors(board, board_messages)
        warnings.extend(board_warnings)
        errors.extend(board_errors)
        if board_errors and location not in failed_locations:
            failed_locations.append(location)

        new_unique_candidates = 0
        for row in jobs:
            if not isinstance(row, dict):
                continue
            normalized = _normalize_job(board, row, url_override=url_override)
            source_metadata = dict(normalized.get("source_metadata") or {})
            source_metadata.update(
                {
                    "query_text": query_variant,
                    "query_index": query_index,
                    "query_location": location,
                    "query_title_seed": title_seed,
                    "query_expansion_type": expansion_type,
                }
            )
            normalized["source_metadata"] = source_metadata
            normalized["query_context"] = {
                "query": query_variant,
                "query_index": query_index,
                "location": location,
                "title_seed": title_seed,
                "expansion_type": expansion_type,
            }
            raw_candidates.append(normalized)
            key = _job_key(normalized)
            if key not in seen_candidate_keys:
                seen_candidate_keys.add(key)
                new_unique_candidates += 1

        if new_unique_candidates <= 0:
            empty_queries_count += 1
            consecutive_empty_queries += 1
        else:
            consecutive_empty_queries = 0

        jobs_found_per_query.append(
            {
                "query": query_variant,
                "location": location,
                "title_seed": title_seed,
                "expansion_type": expansion_type,
                "query_index": query_index,
                "jobs_found": len(jobs),
                "new_unique_jobs": new_unique_candidates,
            }
        )
        search_attempts.append(
            {
                "query": query_variant,
                "location": location,
                "title_seed": title_seed,
                "expansion_type": expansion_type,
                "query_index": query_index,
                "pages_fetched": int(board_meta.get("pages_fetched") or 0),
                "pages_attempted": int(board_meta.get("pages_attempted") or board_meta.get("pages_fetched") or 0),
                "pages_with_results": int(board_meta.get("pages_with_results") or 0),
                "discovered_raw_count": int(board_meta.get("discovered_raw_count") or len(jobs)),
                "jobs_found": len(jobs),
                "new_unique_jobs": new_unique_candidates,
                "stop_reason": str(board_meta.get("stop_reason") or ""),
                "request_urls_tried": list(board_meta.get("request_urls_tried") or []),
                "last_request_url": str(board_meta.get("last_request_url") or "").strip() or None,
                "error_type": str(board_meta.get("error_type") or "").strip() or None,
                "error_status": (
                    int(board_meta.get("error_status"))
                    if isinstance(board_meta.get("error_status"), int)
                    else None
                ),
            }
        )

        if consecutive_empty_queries >= DEFAULT_CONSECUTIVE_EMPTY_QUERIES_STOP:
            break

    filtered_candidates: list[dict[str, Any]] = []
    for row in raw_candidates:
        if not _job_matches_basic_filters(row, request):
            dropped_by_basic_filter_count += 1
            continue
        kept_after_basic_filter_count += 1
        filtered_candidates.append(row)

    collected: list[dict[str, Any]] = []
    seen_jobs: set[tuple[str, str, str]] = set()
    for row in filtered_candidates:
        key = _job_key(row)
        if key in seen_jobs:
            deduped_count += 1
            continue
        seen_jobs.add(key)
        _accumulate_metadata_summary(
            metadata_completeness_summary,
            row.get("metadata_diagnostics") if isinstance(row.get("metadata_diagnostics"), dict) else {},
        )
        collected.append(row)

    truncated_by_source_limit_count = 0
    if len(collected) > max_jobs:
        truncated_by_source_limit_count = len(collected) - max_jobs
        collected = collected[:max_jobs]

    search_attempts_by_query = {
        f"{row.get('query_index')}:{row.get('query')}": row for row in search_attempts
    }
    for row in jobs_found_per_query:
        key = f"{row.get('query_index')}:{row.get('query')}"
        attempt = search_attempts_by_query.get(key)
        if attempt is None:
            continue
        attempt["kept_after_basic_filter_count"] = 0
        attempt["dropped_by_basic_filter_count"] = 0
        attempt["deduped_count"] = 0
        attempt["returned_count"] = 0

    seen_jobs_by_query: set[tuple[str, str, str]] = set()
    collected_by_query: dict[str, int] = {}
    deduped_by_query: dict[str, int] = {}
    dropped_by_query: dict[str, int] = {}
    kept_by_query: dict[str, int] = {}
    for row in raw_candidates:
        query_context = row.get("query_context") if isinstance(row.get("query_context"), dict) else {}
        key = f"{query_context.get('query_index')}:{query_context.get('query')}"
        if not _job_matches_basic_filters(row, request):
            dropped_by_query[key] = dropped_by_query.get(key, 0) + 1
            continue
        kept_by_query[key] = kept_by_query.get(key, 0) + 1
        job_key = _job_key(row)
        if job_key in seen_jobs_by_query:
            deduped_by_query[key] = deduped_by_query.get(key, 0) + 1
            continue
        seen_jobs_by_query.add(job_key)
        collected_by_query[key] = collected_by_query.get(key, 0) + 1

    for row in search_attempts:
        key = f"{row.get('query_index')}:{row.get('query')}"
        row["kept_after_basic_filter_count"] = kept_by_query.get(key, 0)
        row["dropped_by_basic_filter_count"] = dropped_by_query.get(key, 0)
        row["deduped_count"] = deduped_by_query.get(key, 0)
        row["returned_count"] = collected_by_query.get(key, 0)

    request_urls_tried: list[str] = []
    for row in search_attempts:
        values = row.get("request_urls_tried") if isinstance(row.get("request_urls_tried"), list) else []
        for value in values:
            if isinstance(value, str) and value.strip() and value.strip() not in request_urls_tried:
                request_urls_tried.append(value.strip())

    status = "success"
    if errors and collected:
        status = "partial_success"
    elif errors and not collected:
        status = "failed"

    return {
        "status": status,
        "jobs": collected,
        "warnings": warnings,
        "errors": errors,
        "meta": {
            "query": query,
            "title_seeds": title_seeds,
            "query_variants_per_title_location_pair": max_queries,
            "max_queries_per_run": max_queries_per_run,
            "enable_query_expansion": enable_query_expansion,
            "locations": locations,
            "locations_attempted": locations_attempted,
            "queries_attempted": _dedupe_text_list(queries_attempted),
            "query_plan": query_plan,
            "queries_executed_count": len(search_attempts),
            "empty_queries_count": empty_queries_count,
            "consecutive_empty_queries_stop": DEFAULT_CONSECUTIVE_EMPTY_QUERIES_STOP,
            "failed_locations": failed_locations,
            "requested_limit": max_jobs,
            "max_pages_per_source": max_pages,
            "early_stop_when_no_new_results": early_stop_when_no_new_results,
            "discovered_raw_count": discovered_raw_count,
            "kept_after_basic_filter_count": kept_after_basic_filter_count,
            "dropped_by_basic_filter_count": dropped_by_basic_filter_count,
            "deduped_count": deduped_count,
            "truncated_by_source_limit_count": truncated_by_source_limit_count,
            "returned_count": len(collected),
            "pages_fetched": pages_fetched,
            "pages_attempted": pages_fetched,
            "jobs_found_per_query": jobs_found_per_query,
            "jobs_found_per_source": len(collected),
            "query_examples": [row.get("query") for row in query_plan[:5] if isinstance(row.get("query"), str)],
            "company_frequency": _company_frequency(collected),
            "search_attempts": search_attempts,
            "request_urls_tried": request_urls_tried,
            "last_request_url": next(
                (
                    row.get("last_request_url")
                    for row in reversed(search_attempts)
                    if isinstance(row.get("last_request_url"), str) and str(row.get("last_request_url")).strip()
                ),
                None,
            ),
            "error_type": next(
                (
                    row.get("error_type")
                    for row in reversed(search_attempts)
                    if isinstance(row.get("error_type"), str) and str(row.get("error_type")).strip()
                ),
                None,
            ),
            "error_status": next(
                (
                    row.get("error_status")
                    for row in reversed(search_attempts)
                    if isinstance(row.get("error_status"), int)
                ),
                None,
            ),
            "basic_filter_mode": "minimal_exclude_only",
            "metadata_completeness_summary": metadata_completeness_summary,
        },
    }
