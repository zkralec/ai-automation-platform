"""Multi-board job scraping helpers for jobs_digest_v1."""

from __future__ import annotations

from html import unescape
import re
from typing import Any
from urllib.error import HTTPError
from urllib.parse import parse_qsl, quote_plus, urlencode, urlsplit, urlunsplit

from integrations.scrape_common import (
    absolute_url,
    clean_html_text,
    fetch_html,
    now_utc_iso,
    parse_price,
)

SUPPORTED_JOB_BOARDS = ("linkedin", "indeed", "glassdoor", "handshake")

_ANCHOR_RE = re.compile(
    r'<a[^>]+href="(?P<href>[^"]+)"[^>]*>(?P<title>.*?)</a>',
    re.IGNORECASE | re.DOTALL,
)
_STRIP_TAGS_RE = re.compile(r"<[^>]+>")

_SALARY_RANGE_RE = re.compile(
    r"\$?\s*(?P<low>[0-9][0-9,]*(?:\.[0-9]{1,2})?)\s*(?P<low_suffix>[kK]?)"
    r"\s*(?:-|to)\s*"
    r"\$?\s*(?P<high>[0-9][0-9,]*(?:\.[0-9]{1,2})?)\s*(?P<high_suffix>[kK]?)",
    re.IGNORECASE,
)
_SALARY_SINGLE_RE = re.compile(
    r"\$?\s*(?P<value>[0-9][0-9,]*(?:\.[0-9]{1,2})?)\s*(?P<suffix>[kK]?)\s*/?\s*(?:year|yr|annum)",
    re.IGNORECASE,
)

_COMPANY_HINT_RE = re.compile(
    r"(?:company|employer|hiring(?:\s+company)?|organization)\s*[:|-]?\s*([A-Za-z0-9&().,\- ]{2,80})",
    re.IGNORECASE,
)
_LOCATION_HINT_RE = re.compile(
    r"(?:location|based in|city|area)\s*[:|-]?\s*([A-Za-z0-9&().,\- ]{2,80})",
    re.IGNORECASE,
)

_REMOTE_RE = re.compile(r"\bremote\b", re.IGNORECASE)
_HYBRID_RE = re.compile(r"\bhybrid\b", re.IGNORECASE)
_ONSITE_RE = re.compile(r"\b(?:on[- ]?site|onsite)\b", re.IGNORECASE)

_EXPERIENCE_LEVEL_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("internship", re.compile(r"\b(intern|internship|co-op)\b", re.IGNORECASE)),
    ("entry", re.compile(r"\b(entry[- ]?level|junior|new grad|associate)\b", re.IGNORECASE)),
    ("mid", re.compile(r"\b(mid[- ]?level|level ii|level iii)\b", re.IGNORECASE)),
    ("senior", re.compile(r"\b(senior|lead|staff|principal|director|manager)\b", re.IGNORECASE)),
)

_CLEARANCE_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("top_secret", re.compile(r"\btop secret\b", re.IGNORECASE)),
    ("secret", re.compile(r"\bsecret clearance\b", re.IGNORECASE)),
    ("public_trust", re.compile(r"\bpublic trust\b", re.IGNORECASE)),
    ("clearance_required", re.compile(r"\bsecurity clearance\b", re.IGNORECASE)),
)

_JOB_URL_PATTERNS: dict[str, tuple[re.Pattern[str], ...]] = {
    "linkedin": (
        re.compile(r"linkedin\.com/jobs/view/", re.IGNORECASE),
        re.compile(r"/jobs/view/", re.IGNORECASE),
    ),
    "indeed": (
        re.compile(r"indeed\.com/(?:viewjob|rc/clk)", re.IGNORECASE),
        re.compile(r"/viewjob", re.IGNORECASE),
    ),
    "glassdoor": (
        re.compile(r"glassdoor\.com/.+?-job-listing-", re.IGNORECASE),
        re.compile(r"/Job/.+?-job-listing-", re.IGNORECASE),
        re.compile(r"glassdoor\.com/partner/jobListing\.htm", re.IGNORECASE),
        re.compile(r"/partner/jobListing\.htm", re.IGNORECASE),
    ),
    "handshake": (
        re.compile(r"joinhandshake\.com/.*/jobs/", re.IGNORECASE),
        re.compile(r"/jobs/[0-9]+", re.IGNORECASE),
        re.compile(r"/stu/jobs/[0-9]+", re.IGNORECASE),
    ),
}

_BOARD_BASE_URLS = {
    "linkedin": "https://www.linkedin.com",
    "indeed": "https://www.indeed.com",
    "glassdoor": "https://www.glassdoor.com",
    "handshake": "https://joinhandshake.com",
}
_BROWSER_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36"
)
_BOARD_REFERERS = {
    "linkedin": "https://www.linkedin.com/jobs/search/",
    "indeed": "https://www.indeed.com/",
    "glassdoor": "https://www.glassdoor.com/",
    "handshake": "https://joinhandshake.com/",
}
_BOARD_RATE_LIMIT_SECONDS = {
    "linkedin": 2.0,
    "indeed": 4.0,
    "glassdoor": 4.0,
    "handshake": 3.0,
}

_RELATIVE_POSTED_AT_RE = re.compile(
    r"\b(?:just posted|today|yesterday|[0-9]{1,3}\+?\s*(?:minute|hour|day|week|month)s?\s+ago|[0-9]{1,3}d)\b",
    re.IGNORECASE,
)

_BOARD_COMPANY_PATTERNS: dict[str, tuple[re.Pattern[str], ...]] = {
    "linkedin": (
        re.compile(r'base-search-card__subtitle[^>]*>(.*?)</', re.IGNORECASE | re.DOTALL),
        re.compile(r'job-search-card__subtitle[^>]*>(.*?)</', re.IGNORECASE | re.DOTALL),
    ),
    "indeed": (
        re.compile(r'data-testid="company-name"[^>]*>(.*?)</', re.IGNORECASE | re.DOTALL),
        re.compile(r'companyName[^>]*>(.*?)</', re.IGNORECASE | re.DOTALL),
    ),
    "glassdoor": (
        re.compile(r'Employer[^>]*>(.*?)</', re.IGNORECASE | re.DOTALL),
        re.compile(r'compactEmployerName[^>]*>(.*?)</', re.IGNORECASE | re.DOTALL),
    ),
    "handshake": (
        re.compile(r'employer[^>]*>(.*?)</', re.IGNORECASE | re.DOTALL),
    ),
}

_BOARD_LOCATION_PATTERNS: dict[str, tuple[re.Pattern[str], ...]] = {
    "linkedin": (
        re.compile(r'job-search-card__location[^>]*>(.*?)</', re.IGNORECASE | re.DOTALL),
    ),
    "indeed": (
        re.compile(r'data-testid="text-location"[^>]*>(.*?)</', re.IGNORECASE | re.DOTALL),
        re.compile(r'companyLocation[^>]*>(.*?)</', re.IGNORECASE | re.DOTALL),
    ),
    "glassdoor": (
        re.compile(r'data-test="location"[^>]*>(.*?)</', re.IGNORECASE | re.DOTALL),
        re.compile(r'location[^>]*>(.*?)</', re.IGNORECASE | re.DOTALL),
    ),
    "handshake": (
        re.compile(r'location[^>]*>(.*?)</', re.IGNORECASE | re.DOTALL),
    ),
}

_BOARD_POSTED_AT_PATTERNS: dict[str, tuple[re.Pattern[str], ...]] = {
    "linkedin": (
        re.compile(r'<time[^>]*datetime="([^"]+)"', re.IGNORECASE | re.DOTALL),
        re.compile(r'<time[^>]*>(.*?)</time>', re.IGNORECASE | re.DOTALL),
    ),
    "indeed": (
        re.compile(r'data-testid="myJobsStateDate"[^>]*>(.*?)</', re.IGNORECASE | re.DOTALL),
        re.compile(r'data-testid="date"[^>]*>(.*?)</', re.IGNORECASE | re.DOTALL),
    ),
    "glassdoor": (
        re.compile(r'data-test="detailText"[^>]*>(.*?)</', re.IGNORECASE | re.DOTALL),
    ),
    "handshake": (
        re.compile(r'posted[^>]*>(.*?)</', re.IGNORECASE | re.DOTALL),
    ),
}

_BOARD_DESCRIPTION_PATTERNS: dict[str, tuple[re.Pattern[str], ...]] = {
    "linkedin": (
        re.compile(r'job-search-card__snippet[^>]*>(.*?)</', re.IGNORECASE | re.DOTALL),
    ),
    "indeed": (
        re.compile(r'job-snippet[^>]*>(.*?)</', re.IGNORECASE | re.DOTALL),
        re.compile(r'data-testid="job-snippet"[^>]*>(.*?)</', re.IGNORECASE | re.DOTALL),
    ),
    "glassdoor": (
        re.compile(r'jobDescriptionSnippet[^>]*>(.*?)</', re.IGNORECASE | re.DOTALL),
    ),
    "handshake": (
        re.compile(r'description[^>]*>(.*?)</', re.IGNORECASE | re.DOTALL),
    ),
}


def _compact_ws(value: str) -> str:
    return " ".join(value.split())


def _strip_html(value: str) -> str:
    return _compact_ws(clean_html_text(_STRIP_TAGS_RE.sub(" ", value)))


def _number_from_match(raw: str, suffix: str) -> float | None:
    number = parse_price(raw)
    if number is None:
        return None
    if suffix.strip().lower() == "k":
        return number * 1000.0
    return number


def _extract_salary_range(snippet: str) -> tuple[float | None, float | None]:
    range_match = _SALARY_RANGE_RE.search(snippet)
    if range_match:
        low = _number_from_match(range_match.group("low"), range_match.group("low_suffix"))
        high = _number_from_match(range_match.group("high"), range_match.group("high_suffix"))
        if low is not None and high is not None:
            if high < low:
                low, high = high, low
            return low, high

    single_match = _SALARY_SINGLE_RE.search(snippet)
    if single_match:
        value = _number_from_match(single_match.group("value"), single_match.group("suffix"))
        return value, value

    return None, None


def _extract_company(snippet: str) -> str | None:
    match = _COMPANY_HINT_RE.search(snippet)
    if not match:
        return None
    value = _compact_ws(match.group(1))
    return value or None


def _extract_location(snippet: str) -> str | None:
    match = _LOCATION_HINT_RE.search(snippet)
    if not match:
        return None
    value = _compact_ws(match.group(1))
    return value or None


def _extract_experience_level(text: str) -> str | None:
    for label, pattern in _EXPERIENCE_LEVEL_PATTERNS:
        if pattern.search(text):
            return label
    return None


def _extract_clearance(text: str) -> tuple[bool, str | None]:
    for clearance_type, pattern in _CLEARANCE_PATTERNS:
        if pattern.search(text):
            return True, clearance_type
    return False, None


def _extract_work_mode(text: str) -> str | None:
    if _REMOTE_RE.search(text):
        return "remote"
    if _HYBRID_RE.search(text):
        return "hybrid"
    if _ONSITE_RE.search(text):
        return "onsite"
    return None


def _extract_pattern_text(patterns: tuple[re.Pattern[str], ...], html_text: str) -> str | None:
    for pattern in patterns:
        match = pattern.search(html_text)
        if not match:
            continue
        value = _strip_html(match.group(1) or "")
        if value:
            return value
    return None


def _extract_posted_at(board_key: str, html_text: str, snippet_text: str) -> str | None:
    board_value = _extract_pattern_text(_BOARD_POSTED_AT_PATTERNS.get(board_key) or (), html_text)
    if board_value:
        return board_value
    relative = _RELATIVE_POSTED_AT_RE.search(snippet_text)
    if relative:
        return _compact_ws(relative.group(0))
    return None


def _extract_description(board_key: str, html_text: str, snippet_text: str) -> str | None:
    board_value = _extract_pattern_text(_BOARD_DESCRIPTION_PATTERNS.get(board_key) or (), html_text)
    if board_value:
        return board_value[:500]
    return snippet_text[:500] if snippet_text else None


def _replace_query_params(url: str, params: dict[str, str]) -> str:
    parts = urlsplit(url)
    query_items = dict(parse_qsl(parts.query, keep_blank_values=True))
    for key, value in params.items():
        query_items[key] = value
    return urlunsplit(parts._replace(query=urlencode(query_items, doseq=True)))


def _compact_text(value: str) -> str:
    return " ".join(str(value or "").split())


def _strip_location_suffix(query: str, location: str) -> str:
    normalized_query = _compact_text(query)
    normalized_location = _compact_text(location)
    if not normalized_query or not normalized_location:
        return normalized_query
    if normalized_query.lower().endswith(normalized_location.lower()):
        trimmed = normalized_query[: -len(normalized_location)].rstrip(" ,-")
        return _compact_text(trimmed)
    return normalized_query


def _sanitize_search_terms(board: str, *, query: str, location: str) -> tuple[str, str]:
    del board
    normalized_query = _strip_location_suffix(query, location)
    normalized_location = _compact_text(location)
    return normalized_query or _compact_text(query), normalized_location


def _page_url_for_board(board: str, *, search_url: str, page_index: int) -> str:
    if page_index <= 0:
        return search_url
    if board == "linkedin":
        return _replace_query_params(search_url, {"start": str(page_index * 25)})
    if board == "indeed":
        return _replace_query_params(search_url, {"start": str(page_index * 10)})
    if board == "glassdoor":
        return _replace_query_params(search_url, {"p": str(page_index + 1)})
    if board == "handshake":
        return _replace_query_params(search_url, {"page": str(page_index + 1)})
    return search_url


def _build_board_search_url(board: str, *, query: str, location: str, page_index: int = 0) -> str:
    query_value, location_value = _sanitize_search_terms(board, query=query, location=location)
    q = quote_plus(query_value or "")
    loc = quote_plus(location_value or "")
    if board == "linkedin":
        search_url = (
            "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search"
            f"?keywords={q}&location={loc}"
        )
        return _page_url_for_board(board, search_url=search_url, page_index=page_index)
    if board == "indeed":
        search_url = f"https://www.indeed.com/jobs?q={q}&l={loc}&from=searchOnHP&sort=date"
        return _page_url_for_board(board, search_url=search_url, page_index=page_index)
    if board == "glassdoor":
        if loc:
            search_url = f"https://www.glassdoor.com/Job/jobs.htm?sc.keyword={q}&locKeyword={loc}"
        else:
            search_url = f"https://www.glassdoor.com/Job/jobs.htm?sc.keyword={q}"
        return _page_url_for_board(board, search_url=search_url, page_index=page_index)
    if board == "handshake":
        search_url = f"https://app.joinhandshake.com/stu/postings?query={q}"
        return _page_url_for_board(board, search_url=search_url, page_index=page_index)
    raise ValueError(f"Unsupported board '{board}'")


def _candidate_search_urls(board: str, *, query: str, location: str, page_index: int = 0) -> list[str]:
    query_value, location_value = _sanitize_search_terms(board, query=query, location=location)
    q = quote_plus(query_value or "")
    loc = quote_plus(location_value or "")
    candidates: list[str] = []

    def add(url: str) -> None:
        if url and url not in candidates:
            candidates.append(url)

    if board == "linkedin":
        add(_build_board_search_url(board, query=query_value, location=location_value, page_index=page_index))
        legacy_url = _page_url_for_board(
            board,
            search_url=f"https://www.linkedin.com/jobs/search/?keywords={q}&location={loc}",
            page_index=page_index,
        )
        add(legacy_url)
        return candidates
    if board == "indeed":
        add(_build_board_search_url(board, query=query_value, location=location_value, page_index=page_index))
        add(_page_url_for_board(board, search_url=f"https://www.indeed.com/jobs?q={q}&l={loc}", page_index=page_index))
        add(_page_url_for_board(board, search_url=f"https://www.indeed.com/m/jobs?q={q}&l={loc}", page_index=page_index))
        return candidates
    if board == "glassdoor":
        add(_build_board_search_url(board, query=query_value, location=location_value, page_index=page_index))
        add(_page_url_for_board(board, search_url=f"https://www.glassdoor.com/Job/jobs.htm?sc.keyword={q}", page_index=page_index))
        return candidates
    if board == "handshake":
        add(_build_board_search_url(board, query=query_value, location=location_value, page_index=page_index))
        add(_page_url_for_board(board, search_url=f"https://joinhandshake.com/students/jobs/search/?query={q}", page_index=page_index))
        return candidates
    add(_build_board_search_url(board, query=query_value, location=location_value, page_index=page_index))
    return candidates


def _request_options_for_board(board: str, *, search_url: str) -> dict[str, Any]:
    headers = {
        "User-Agent": _BROWSER_USER_AGENT,
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": _BOARD_REFERERS.get(board, search_url),
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }
    return {
        "extra_headers": headers,
        "rate_limit_seconds": _BOARD_RATE_LIMIT_SECONDS.get(board, 2.0),
    }


def _error_details(exc: Exception) -> tuple[str, int | None]:
    if isinstance(exc, HTTPError):
        if exc.code == 403:
            return "fetch_blocked_403", 403
        if exc.code == 404:
            return "fetch_not_found_404", 404
        return f"fetch_http_{exc.code}", int(exc.code)
    return type(exc).__name__, None


def _is_job_url_for_board(board: str, url: str) -> bool:
    patterns = _JOB_URL_PATTERNS.get(board) or ()
    return any(pattern.search(url) for pattern in patterns)


def _dedupe_jobs(jobs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[str, str, str]] = set()
    output: list[dict[str, Any]] = []
    for row in jobs:
        source = str(row.get("source") or "").strip().lower()
        url = str(row.get("url") or "").strip()
        title = str(row.get("title") or "").strip().lower()
        key = (source, url, title)
        if key in seen:
            continue
        seen.add(key)
        output.append(row)
    return output


def _job_key(job: dict[str, Any]) -> tuple[str, str, str]:
    return (
        str(job.get("source") or "").strip().lower(),
        str(job.get("url") or "").strip(),
        str(job.get("title") or "").strip().lower(),
    )


def _extract_jobs_from_html(board_key: str, *, html_text: str, base_url: str, search_url: str, location: str) -> list[dict[str, Any]]:
    jobs: list[dict[str, Any]] = []
    scraped_at = now_utc_iso()
    for match in _ANCHOR_RE.finditer(html_text):
        raw_title = _strip_html(match.group("title") or "")
        if not raw_title or len(raw_title) < 4:
            continue

        href = unescape((match.group("href") or "").strip())
        if not href:
            continue
        url = absolute_url(base_url, href)
        if not _is_job_url_for_board(board_key, url):
            continue

        start, end = match.span()
        snippet_html = html_text[max(0, start - 900): min(len(html_text), end + 2000)]
        snippet = _strip_html(snippet_html)
        company = _extract_pattern_text(_BOARD_COMPANY_PATTERNS.get(board_key) or (), snippet_html) or _extract_company(snippet)
        location_text = _extract_pattern_text(_BOARD_LOCATION_PATTERNS.get(board_key) or (), snippet_html) or _extract_location(snippet)
        posted_at = _extract_posted_at(board_key, snippet_html, snippet)
        description_snippet = _extract_description(board_key, snippet_html, snippet)
        salary_min, salary_max = _extract_salary_range(snippet)
        experience_level = _extract_experience_level((raw_title + " " + snippet).lower())
        clearance_required, clearance_type = _extract_clearance((raw_title + " " + snippet).lower())
        work_mode = _extract_work_mode((raw_title + " " + snippet).lower())

        jobs.append(
            {
                "source": board_key,
                "title": raw_title,
                "company": company,
                "location": location_text or location or None,
                "url": url,
                "salary_min": salary_min,
                "salary_max": salary_max,
                "salary_currency": "USD" if salary_min is not None or salary_max is not None else None,
                "experience_level": experience_level,
                "clearance_required": clearance_required,
                "clearance_type": clearance_type,
                "work_mode": work_mode,
                "posted_at": posted_at,
                "scraped_at": scraped_at,
                "description_snippet": description_snippet,
                "raw": {
                    "board": board_key,
                    "search_url": search_url,
                    "job_url": url,
                    "company_text": company,
                    "location_text": location_text,
                    "posted_at_text": posted_at,
                    "description_text": description_snippet,
                },
            }
        )
    return jobs


def collect_jobs_from_board(
    board: str,
    *,
    query: str,
    location: str,
    max_jobs: int = 25,
    max_pages: int = 1,
    early_stop_when_no_new_results: bool = True,
    url_override: str | None = None,
) -> tuple[list[dict[str, Any]], list[str], dict[str, Any]]:
    board_key = board.strip().lower()
    if board_key not in SUPPORTED_JOB_BOARDS:
        return [], [f"{board_key}: unsupported_board"], {
            "discovered_raw_count": 0,
            "pages_fetched": 0,
            "pages_with_results": 0,
            "stop_reason": "unsupported_board",
        }

    base_url = _BOARD_BASE_URLS[board_key]
    base_search_url = str(url_override or "").strip() or None
    warnings: list[str] = []
    jobs: list[dict[str, Any]] = []
    seen_unique: set[tuple[str, str, str]] = set()
    max_items = max(1, int(max_jobs))
    max_page_count = max(1, int(max_pages))
    pages_fetched = 0
    pages_with_results = 0
    discovered_raw_count = 0
    stop_reason = "max_pages_reached"
    request_attempts: list[dict[str, Any]] = []
    request_urls_tried: list[str] = []
    last_request_url: str | None = None
    error_type: str | None = None
    error_status: int | None = None

    for page_index in range(max_page_count):
        candidate_urls = (
            [_page_url_for_board(board_key, search_url=base_search_url, page_index=page_index)]
            if base_search_url
            else _candidate_search_urls(board_key, query=query, location=location, page_index=page_index)
        )
        html_text = None
        search_url = None
        page_errors: list[str] = []

        for candidate_index, candidate_url in enumerate(candidate_urls, start=1):
            pages_fetched += 1
            request_urls_tried.append(candidate_url)
            last_request_url = candidate_url
            try:
                html_text = fetch_html(candidate_url, **_request_options_for_board(board_key, search_url=candidate_url))
                search_url = candidate_url
                request_attempts.append(
                    {
                        "page_index": page_index + 1,
                        "candidate_index": candidate_index,
                        "url": candidate_url,
                        "status": "ok",
                    }
                )
                break
            except Exception as exc:
                error_type, error_status = _error_details(exc)
                request_attempts.append(
                    {
                        "page_index": page_index + 1,
                        "candidate_index": candidate_index,
                        "url": candidate_url,
                        "status": "error",
                        "error_type": error_type,
                        "error_status": error_status,
                    }
                )
                page_errors.append(
                    f"{board_key}: {error_type} url={candidate_url} error={type(exc).__name__}: {exc}"
                )
                if candidate_index == len(candidate_urls):
                    stop_reason = error_type
                continue

        if html_text is None or search_url is None:
            return jobs, page_errors or [f"{board_key}: fetch_failed"], {
                "discovered_raw_count": discovered_raw_count,
                "pages_fetched": pages_fetched,
                "pages_attempted": pages_fetched,
                "pages_with_results": pages_with_results,
                "request_attempts": request_attempts,
                "request_urls_tried": request_urls_tried,
                "last_request_url": last_request_url,
                "error_type": error_type,
                "error_status": error_status,
                "stop_reason": stop_reason,
            }

        page_jobs = _extract_jobs_from_html(
            board_key,
            html_text=html_text,
            base_url=base_url,
            search_url=search_url,
            location=location,
        )
        discovered_raw_count += len(page_jobs)
        if page_jobs:
            pages_with_results += 1
        jobs.extend(page_jobs)

        new_unique = 0
        for row in page_jobs:
            key = _job_key(row)
            if key in seen_unique:
                continue
            seen_unique.add(key)
            new_unique += 1

        if len(jobs) >= max_items:
            stop_reason = "max_jobs_reached"
            jobs = jobs[:max_items]
            break
        if early_stop_when_no_new_results and new_unique == 0:
            stop_reason = "no_new_results"
            break

    if not jobs:
        warnings.append(f"{board_key}: no_jobs_found")
        stop_reason = "no_jobs_found"

    return jobs, warnings, {
        "discovered_raw_count": discovered_raw_count,
        "pages_fetched": pages_fetched,
        "pages_attempted": pages_fetched,
        "pages_with_results": pages_with_results,
        "request_attempts": request_attempts,
        "request_urls_tried": request_urls_tried,
        "last_request_url": last_request_url,
        "error_type": error_type,
        "error_status": error_status,
        "stop_reason": stop_reason,
    }


def collect_jobs_from_boards(
    *,
    query: str,
    location: str,
    boards: list[str],
    max_jobs_per_board: int = 25,
    max_pages_per_board: int = 1,
    early_stop_when_no_new_results: bool = True,
    board_url_overrides: dict[str, str] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, int], list[str]]:
    all_jobs: list[dict[str, Any]] = []
    counts: dict[str, int] = {}
    warnings: list[str] = []
    overrides = board_url_overrides or {}

    for raw_board in boards:
        board = str(raw_board or "").strip().lower()
        if not board:
            continue
        jobs, board_warnings, _board_meta = collect_jobs_from_board(
            board,
            query=query,
            location=location,
            max_jobs=max_jobs_per_board,
            max_pages=max_pages_per_board,
            early_stop_when_no_new_results=early_stop_when_no_new_results,
            url_override=overrides.get(board),
        )
        counts[board] = len(jobs)
        all_jobs.extend(jobs)
        warnings.extend(board_warnings)

    return _dedupe_jobs(all_jobs), counts, warnings
