"""Multi-board job scraping helpers for jobs_digest_v1."""

from __future__ import annotations

import re
from typing import Any
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
    ),
    "handshake": (
        re.compile(r"joinhandshake\.com/.*/jobs/", re.IGNORECASE),
        re.compile(r"/jobs/[0-9]+", re.IGNORECASE),
    ),
}

_BOARD_BASE_URLS = {
    "linkedin": "https://www.linkedin.com",
    "indeed": "https://www.indeed.com",
    "glassdoor": "https://www.glassdoor.com",
    "handshake": "https://joinhandshake.com",
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


def _replace_query_params(url: str, params: dict[str, str]) -> str:
    parts = urlsplit(url)
    query_items = dict(parse_qsl(parts.query, keep_blank_values=True))
    for key, value in params.items():
        query_items[key] = value
    return urlunsplit(parts._replace(query=urlencode(query_items, doseq=True)))


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
    q = quote_plus(query or "")
    loc = quote_plus(location or "")
    if board == "linkedin":
        search_url = f"https://www.linkedin.com/jobs/search/?keywords={q}&location={loc}"
        return _page_url_for_board(board, search_url=search_url, page_index=page_index)
    if board == "indeed":
        search_url = f"https://www.indeed.com/jobs?q={q}&l={loc}"
        return _page_url_for_board(board, search_url=search_url, page_index=page_index)
    if board == "glassdoor":
        search_url = f"https://www.glassdoor.com/Job/jobs.htm?sc.keyword={q}"
        return _page_url_for_board(board, search_url=search_url, page_index=page_index)
    if board == "handshake":
        search_url = f"https://joinhandshake.com/students/jobs/search?query={q}"
        return _page_url_for_board(board, search_url=search_url, page_index=page_index)
    raise ValueError(f"Unsupported board '{board}'")


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

        href = (match.group("href") or "").strip()
        if not href:
            continue
        url = absolute_url(base_url, href)
        if not _is_job_url_for_board(board_key, url):
            continue

        start, end = match.span()
        snippet_html = html_text[max(0, start - 900): min(len(html_text), end + 2000)]
        snippet = _strip_html(snippet_html)
        salary_min, salary_max = _extract_salary_range(snippet)
        experience_level = _extract_experience_level((raw_title + " " + snippet).lower())
        clearance_required, clearance_type = _extract_clearance((raw_title + " " + snippet).lower())
        work_mode = _extract_work_mode((raw_title + " " + snippet).lower())

        jobs.append(
            {
                "source": board_key,
                "title": raw_title,
                "company": _extract_company(snippet),
                "location": _extract_location(snippet) or location or None,
                "url": url,
                "salary_min": salary_min,
                "salary_max": salary_max,
                "salary_currency": "USD" if salary_min is not None or salary_max is not None else None,
                "experience_level": experience_level,
                "clearance_required": clearance_required,
                "clearance_type": clearance_type,
                "work_mode": work_mode,
                "posted_at": None,
                "scraped_at": scraped_at,
                "description_snippet": snippet[:500] if snippet else None,
                "raw": {"search_url": search_url},
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
    base_search_url = (str(url_override or "").strip() or _build_board_search_url(board_key, query=query, location=location))
    warnings: list[str] = []
    jobs: list[dict[str, Any]] = []
    seen_unique: set[tuple[str, str, str]] = set()
    max_items = max(1, int(max_jobs))
    max_page_count = max(1, int(max_pages))
    pages_fetched = 0
    pages_with_results = 0
    discovered_raw_count = 0
    stop_reason = "max_pages_reached"

    for page_index in range(max_page_count):
        search_url = _page_url_for_board(
            board_key,
            search_url=base_search_url,
            page_index=page_index,
        )
        pages_fetched += 1
        try:
            html_text = fetch_html(search_url)
        except Exception as exc:
            return jobs, [f"{board_key}: fetch_failed url={search_url} error={type(exc).__name__}: {exc}"], {
                "discovered_raw_count": discovered_raw_count,
                "pages_fetched": pages_fetched,
                "pages_with_results": pages_with_results,
                "stop_reason": "fetch_failed",
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
        "pages_with_results": pages_with_results,
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
