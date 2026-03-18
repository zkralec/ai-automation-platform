from __future__ import annotations

from typing import Any

from integrations.job_boards_scrape import collect_jobs_from_board

DEFAULT_QUERY = "software engineer"
DEFAULT_LOCATION = "United States"
DEFAULT_RESULT_LIMIT_PER_SOURCE = 250
MAX_RESULT_LIMIT_PER_SOURCE = 1000
MIN_RESULT_LIMIT_PER_SOURCE = 1
DEFAULT_MAX_PAGES_PER_SOURCE = 5
MAX_MAX_PAGES_PER_SOURCE = 20
DEFAULT_MAX_QUERIES_PER_TITLE_LOCATION_PAIR = 4
MAX_MAX_QUERIES_PER_TITLE_LOCATION_PAIR = 10


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
    query = str(request.get("query") or request.get("search_query") or "").strip()
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


def _title_seeds(request: dict[str, Any]) -> list[str]:
    titles = _dedupe_text_list(_as_text_list(request.get("titles")))
    desired_title = str(request.get("desired_title") or "").strip()
    if desired_title and desired_title.lower() not in {row.lower() for row in titles}:
        titles.insert(0, desired_title)
    explicit_query = str(request.get("query") or request.get("search_query") or "").strip()
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
        if "fetch_failed" in low or "unsupported_board" in low:
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
            "early_stop_when_no_new_results": "pagination_stop_condition",
            "enabled_sources": "pipeline_routing",
        },
        "source_metadata_fields": ["search_url"],
    }


def collect_board_jobs(board: str, request: dict[str, Any], *, url_override: str | None = None) -> dict[str, Any]:
    query = _normalize_query(request)
    title_seeds = _title_seeds(request)
    locations = _normalize_locations(request)
    max_jobs = _normalize_result_limit(request)
    max_pages = _normalize_max_pages(request)
    max_queries = _normalize_max_queries(request)
    early_stop_when_no_new_results = _normalize_early_stop(request)

    collected: list[dict[str, Any]] = []
    seen_jobs: set[tuple[str, str, str]] = set()
    warnings: list[str] = []
    errors: list[str] = []
    locations_attempted: list[str] = []
    queries_attempted: list[str] = []
    failed_locations: list[str] = []
    search_attempts: list[dict[str, Any]] = []
    discovered_raw_count = 0
    kept_after_basic_filter_count = 0
    dropped_by_basic_filter_count = 0
    deduped_count = 0
    pages_fetched = 0
    metadata_completeness_summary = _empty_metadata_summary()

    for location in locations:
        locations_attempted.append(location)
        for title_seed in title_seeds:
            if len(collected) >= max_jobs:
                break
            for query_variant in _query_variants(request, title_seed=title_seed, max_queries=max_queries):
                if len(collected) >= max_jobs:
                    break
                remaining = max_jobs - len(collected)
                queries_attempted.append(query_variant)
                attempt_limit = min(max_jobs, max(remaining * 3, remaining + 25))
                jobs, board_messages, board_meta = collect_jobs_from_board(
                    board,
                    query=query_variant,
                    location=location,
                    max_jobs=attempt_limit,
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

                kept_before = kept_after_basic_filter_count
                dropped_before = dropped_by_basic_filter_count
                deduped_before = deduped_count
                collected_before = len(collected)

                for row in jobs:
                    if not isinstance(row, dict):
                        continue
                    if not _job_matches_basic_filters(row, request):
                        dropped_by_basic_filter_count += 1
                        continue

                    kept_after_basic_filter_count += 1
                    normalized = _normalize_job(board, row, url_override=url_override)
                    key = _job_key(normalized)
                    if key in seen_jobs:
                        deduped_count += 1
                        continue
                    seen_jobs.add(key)
                    _accumulate_metadata_summary(
                        metadata_completeness_summary,
                        normalized.get("metadata_diagnostics") if isinstance(normalized.get("metadata_diagnostics"), dict) else {},
                    )
                    collected.append(normalized)
                    if len(collected) >= max_jobs:
                        break

                search_attempts.append(
                    {
                        "query": query_variant,
                        "location": location,
                        "title_seed": title_seed,
                        "pages_fetched": int(board_meta.get("pages_fetched") or 0),
                        "pages_with_results": int(board_meta.get("pages_with_results") or 0),
                        "discovered_raw_count": int(board_meta.get("discovered_raw_count") or len(jobs)),
                        "kept_after_basic_filter_count": kept_after_basic_filter_count - kept_before,
                        "dropped_by_basic_filter_count": dropped_by_basic_filter_count - dropped_before,
                        "deduped_count": deduped_count - deduped_before,
                        "returned_count": len(collected) - collected_before,
                        "stop_reason": str(board_meta.get("stop_reason") or ""),
                    }
                )

            if len(collected) >= max_jobs:
                break

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
            "locations": locations,
            "locations_attempted": locations_attempted,
            "queries_attempted": _dedupe_text_list(queries_attempted),
            "failed_locations": failed_locations,
            "requested_limit": max_jobs,
            "max_pages_per_source": max_pages,
            "early_stop_when_no_new_results": early_stop_when_no_new_results,
            "discovered_raw_count": discovered_raw_count,
            "kept_after_basic_filter_count": kept_after_basic_filter_count,
            "dropped_by_basic_filter_count": dropped_by_basic_filter_count,
            "deduped_count": deduped_count,
            "returned_count": len(collected),
            "pages_fetched": pages_fetched,
            "search_attempts": search_attempts,
            "basic_filter_mode": "minimal_exclude_only",
            "metadata_completeness_summary": metadata_completeness_summary,
        },
    }
