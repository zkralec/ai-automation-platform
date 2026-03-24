"""Multi-board job scraping helpers for jobs_digest_v1."""

from __future__ import annotations

from importlib import import_module
from html import unescape
import os
import re
from typing import Any
from urllib.error import HTTPError
from urllib.parse import parse_qsl, quote_plus, urlencode, urlsplit, urlunsplit

from integrations.scrape_common import (
    absolute_url,
    clean_html_text,
    fetch_html,
    fetch_html_response,
    now_utc_iso,
    parse_price,
)

SUPPORTED_JOB_BOARDS = ("linkedin", "indeed", "glassdoor", "handshake")

_ANCHOR_RE = re.compile(
    r"""<a[^>]+href=["'](?P<href>[^"']+)["'][^>]*>(?P<title>.*?)</a>""",
    re.IGNORECASE | re.DOTALL,
)
_STRIP_TAGS_RE = re.compile(r"<[^>]+>")
_HANDSHAKE_CARD_RE = re.compile(
    r"""href=["'](?P<href>[^"']*(?:/stu/jobs/[0-9]+|/jobs/[0-9]+)[^"']*)["']""",
    re.IGNORECASE,
)

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

_HANDSHAKE_LOGIN_WALL_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"\blog in\b", re.IGNORECASE),
    re.compile(r"\bsign in\b", re.IGNORECASE),
    re.compile(r"continue\s+with\s+your\s+school", re.IGNORECASE),
    re.compile(r"sign in to view", re.IGNORECASE),
    re.compile(r"please log in", re.IGNORECASE),
)
_HANDSHAKE_EMPTY_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"\bno results\b", re.IGNORECASE),
    re.compile(r"\b0 jobs\b", re.IGNORECASE),
    re.compile(r"no jobs found", re.IGNORECASE),
    re.compile(r"we couldn'?t find any jobs", re.IGNORECASE),
    re.compile(r"try adjusting your filters", re.IGNORECASE),
)
_HANDSHAKE_SEARCH_CONTEXT_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"\bjob postings\b", re.IGNORECASE),
    re.compile(r"\bfilters\b", re.IGNORECASE),
    re.compile(r"\bsearch results\b", re.IGNORECASE),
    re.compile(r"\bjoinhandshake\b", re.IGNORECASE),
)
_HANDSHAKE_SEARCH_PATH_MARKERS = ("/stu/postings", "/students/jobs/search", "/jobs/search")
_HANDSHAKE_AUTH_PATH_MARKERS = ("/login", "/sign_in", "/users/sign_in", "/session", "/sessions", "/auth")


def _env_flag(name: str, *, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    low = raw.strip().lower()
    if not low:
        return default
    if low in {"1", "true", "yes", "on"}:
        return True
    if low in {"0", "false", "no", "off"}:
        return False
    return default


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


def _request_options_for_board_with_headers(
    board: str,
    *,
    search_url: str,
    extra_headers: dict[str, str] | None = None,
    cache_ttl_seconds: int | None = None,
) -> dict[str, Any]:
    options = _request_options_for_board(board, search_url=search_url)
    headers = dict(options.get("extra_headers") or {})
    if extra_headers:
        headers.update({key: value for key, value in extra_headers.items() if str(value).strip()})
    options["extra_headers"] = headers
    if cache_ttl_seconds is not None:
        options["cache_ttl_seconds"] = cache_ttl_seconds
    return options


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


def _normalize_url_for_compare(url: str | None) -> str:
    if not url:
        return ""
    parts = urlsplit(str(url).strip())
    query = urlencode(sorted(parse_qsl(parts.query, keep_blank_values=True)))
    return urlunsplit((parts.scheme.lower(), parts.netloc.lower(), parts.path.rstrip("/"), query, ""))


def _handshake_cookie_header() -> str | None:
    cookie_header = str(os.getenv("HANDSHAKE_COOKIE_HEADER") or "").strip()
    if cookie_header:
        return cookie_header
    session_cookie = str(os.getenv("HANDSHAKE_SESSION_COOKIE") or "").strip()
    if not session_cookie:
        return None
    return session_cookie if "=" in session_cookie else f"_session_id={session_cookie}"


def _handshake_session_headers() -> dict[str, str]:
    cookie = _handshake_cookie_header()
    return {"Cookie": cookie} if cookie else {}


def _is_handshake_auth_url(url: str | None) -> bool:
    if not url:
        return False
    low = urlsplit(url).path.lower()
    return any(marker in low for marker in _HANDSHAKE_AUTH_PATH_MARKERS)


def _is_handshake_search_url(url: str | None) -> bool:
    if not url:
        return False
    low = urlsplit(url).path.lower()
    return any(marker in low for marker in _HANDSHAKE_SEARCH_PATH_MARKERS)


def _count_handshake_cards(html_text: str, *, base_url: str) -> int:
    seen: set[str] = set()
    for match in _HANDSHAKE_CARD_RE.finditer(html_text):
        href = str(match.group("href") or "").strip()
        if not href:
            continue
        url = absolute_url(base_url, unescape(href))
        if _is_job_url_for_board("handshake", url):
            seen.add(url)
    return len(seen)


def _handshake_page_diagnostics(
    *,
    requested_url: str,
    final_url: str | None,
    html_text: str,
    status_code: int | None,
    cards_seen: int,
) -> dict[str, Any]:
    final_value = str(final_url or "").strip() or requested_url
    unexpected_redirect = bool(
        final_value
        and _normalize_url_for_compare(final_value) != _normalize_url_for_compare(requested_url)
        and not _is_handshake_search_url(final_value)
    )
    login_wall = _is_handshake_auth_url(final_value) or any(pattern.search(html_text) for pattern in _HANDSHAKE_LOGIN_WALL_PATTERNS)
    auth_required = bool(login_wall or status_code in {401, 403})
    empty_results = any(pattern.search(html_text) for pattern in _HANDSHAKE_EMPTY_PATTERNS)
    search_context = _is_handshake_search_url(requested_url) or any(
        pattern.search(html_text) for pattern in _HANDSHAKE_SEARCH_CONTEXT_PATTERNS
    )
    layout_mismatch = bool(cards_seen == 0 and not empty_results and not auth_required and search_context)

    source_status = "success"
    source_error_type = None
    page_state = "results"
    if cards_seen <= 0:
        if auth_required:
            source_status = "auth_blocked"
            page_state = "auth_blocked"
            if unexpected_redirect and not login_wall:
                source_error_type = "unexpected_redirect"
            elif login_wall:
                source_error_type = "login_wall"
            else:
                source_error_type = "auth_required"
        elif unexpected_redirect:
            source_status = "upstream_failure"
            source_error_type = "unexpected_redirect"
            page_state = "unexpected_redirect"
        elif empty_results:
            source_status = "empty_success"
            source_error_type = "empty_results"
            page_state = "empty_results"
        elif layout_mismatch:
            source_status = "layout_mismatch"
            source_error_type = "selector_mismatch"
            page_state = "selector_mismatch"
        else:
            source_status = "empty_success"
            source_error_type = "no_cards_found"
            page_state = "no_cards_found"

    return {
        "source_status": source_status,
        "source_error_type": source_error_type,
        "page_state": page_state,
        "cards_seen": int(cards_seen),
        "auth_required_detected": auth_required,
        "login_wall_detected": login_wall,
        "unexpected_redirect_detected": unexpected_redirect,
        "layout_mismatch_detected": layout_mismatch,
    }


def _handshake_browser_fallback_enabled() -> bool:
    return _env_flag("HANDSHAKE_USE_BROWSER_FALLBACK", default=False)


def _fetch_handshake_browser_response(url: str, *, extra_headers: dict[str, str] | None = None) -> dict[str, Any] | None:
    try:
        module = import_module("integrations.handshake_browser_fallback")
    except ImportError:
        return None

    fetcher = getattr(module, "fetch_handshake_page", None)
    if not callable(fetcher):
        return None

    response = fetcher(url=url, extra_headers=extra_headers or {})
    if isinstance(response, str):
        return {
            "html_text": response,
            "final_url": url,
            "status_code": None,
            "from_cache": False,
        }
    if isinstance(response, dict):
        return {
            "html_text": str(response.get("html_text") or ""),
            "final_url": str(response.get("final_url") or url),
            "status_code": int(response.get("status_code")) if isinstance(response.get("status_code"), int) else None,
            "from_cache": bool(response.get("from_cache", False)),
        }
    raise TypeError("fetch_handshake_page must return str or dict")


def _handshake_status_priority(status: str) -> int:
    return {
        "empty_success": 1,
        "upstream_failure": 2,
        "layout_mismatch": 3,
        "auth_blocked": 4,
    }.get(status, 0)


def _handshake_final_meta(
    *,
    jobs: list[dict[str, Any]],
    discovered_raw_count: int,
    pages_fetched: int,
    pages_with_results: int,
    request_attempts: list[dict[str, Any]],
    request_urls_tried: list[str],
    last_request_url: str | None,
    stop_reason: str,
    source_status: str,
    source_error_type: str | None,
    error_status: int | None,
    cards_seen: int,
    auth_required_detected: bool,
    login_wall_detected: bool,
    unexpected_redirect_detected: bool,
    layout_mismatch_detected: bool,
) -> dict[str, Any]:
    return {
        "discovered_raw_count": discovered_raw_count,
        "pages_fetched": pages_fetched,
        "pages_attempted": pages_fetched,
        "pages_with_results": pages_with_results,
        "request_attempts": request_attempts,
        "request_urls_tried": request_urls_tried,
        "last_request_url": last_request_url,
        "error_type": source_error_type,
        "source_error_type": source_error_type,
        "error_status": error_status,
        "stop_reason": stop_reason,
        "source_status": source_status,
        "cards_seen": cards_seen,
        "jobs_raw": discovered_raw_count,
        "jobs_kept": len(jobs),
        "auth_required_detected": auth_required_detected,
        "login_wall_detected": login_wall_detected,
        "unexpected_redirect_detected": unexpected_redirect_detected,
        "layout_mismatch_detected": layout_mismatch_detected,
        "authenticated_session_supported": bool(_handshake_cookie_header()),
        "browser_fallback_requested": _handshake_browser_fallback_enabled(),
    }


def _collect_jobs_from_handshake(
    *,
    query: str,
    location: str,
    max_jobs: int,
    max_pages: int,
    early_stop_when_no_new_results: bool,
    url_override: str | None,
) -> tuple[list[dict[str, Any]], list[str], dict[str, Any]]:
    board_key = "handshake"
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
    error_status: int | None = None
    cards_seen_total = 0
    auth_required_detected = False
    login_wall_detected = False
    unexpected_redirect_detected = False
    layout_mismatch_detected = False
    best_failure_status = "empty_success"
    best_failure_error_type: str | None = "empty_results"

    session_headers = _handshake_session_headers()
    browser_fallback_enabled = _handshake_browser_fallback_enabled()

    for page_index in range(max_page_count):
        candidate_urls = (
            [_page_url_for_board(board_key, search_url=base_search_url, page_index=page_index)]
            if base_search_url
            else _candidate_search_urls(board_key, query=query, location=location, page_index=page_index)
        )
        page_selected = False
        page_max_cards_seen = 0

        for candidate_index, candidate_url in enumerate(candidate_urls, start=1):
            modes: list[tuple[str, dict[str, str], bool]] = [("http_anonymous", {}, False)]
            if session_headers:
                modes.append(("http_authenticated", dict(session_headers), False))
            if browser_fallback_enabled:
                modes.append(("browser_fallback", dict(session_headers), True))

            for mode_index, (mode, mode_headers, use_browser) in enumerate(modes, start=1):
                pages_fetched += 1
                request_urls_tried.append(candidate_url)
                last_request_url = candidate_url

                try:
                    if use_browser:
                        response = _fetch_handshake_browser_response(candidate_url, extra_headers=mode_headers)
                        if response is None:
                            request_attempts.append(
                                {
                                    "page_index": page_index + 1,
                                    "candidate_index": candidate_index,
                                    "mode_index": mode_index,
                                    "mode": mode,
                                    "url": candidate_url,
                                    "status": "error",
                                    "error_type": "browser_fallback_unavailable",
                                }
                            )
                            continue
                    else:
                        response = fetch_html_response(
                            candidate_url,
                            **_request_options_for_board_with_headers(
                                board_key,
                                search_url=candidate_url,
                                extra_headers=mode_headers,
                                cache_ttl_seconds=0 if mode_headers.get("Cookie") else None,
                            ),
                        )

                    html_text = str(response.get("html_text") or "")
                    final_url = str(response.get("final_url") or candidate_url)
                    status_code = response.get("status_code") if isinstance(response.get("status_code"), int) else None
                    cards_seen = _count_handshake_cards(html_text, base_url=base_url)
                    page_max_cards_seen = max(page_max_cards_seen, cards_seen)
                    page_diag = _handshake_page_diagnostics(
                        requested_url=candidate_url,
                        final_url=final_url,
                        html_text=html_text,
                        status_code=status_code,
                        cards_seen=cards_seen,
                    )
                    page_jobs: list[dict[str, Any]] = []
                    if page_diag["source_status"] == "success":
                        page_jobs = _extract_jobs_from_html(
                            board_key,
                            html_text=html_text,
                            base_url=base_url,
                            search_url=final_url,
                            location=location,
                        )
                        if not page_jobs:
                            page_diag.update(
                                {
                                    "source_status": "layout_mismatch",
                                    "source_error_type": "selector_mismatch",
                                    "page_state": "selector_mismatch",
                                    "layout_mismatch_detected": True,
                                }
                            )

                    auth_required_detected = auth_required_detected or bool(page_diag["auth_required_detected"])
                    login_wall_detected = login_wall_detected or bool(page_diag["login_wall_detected"])
                    unexpected_redirect_detected = unexpected_redirect_detected or bool(page_diag["unexpected_redirect_detected"])
                    layout_mismatch_detected = layout_mismatch_detected or bool(page_diag["layout_mismatch_detected"])
                    if _handshake_status_priority(str(page_diag["source_status"])) >= _handshake_status_priority(best_failure_status):
                        best_failure_status = str(page_diag["source_status"])
                        best_failure_error_type = str(page_diag.get("source_error_type") or "").strip() or None

                    request_attempts.append(
                        {
                            "page_index": page_index + 1,
                            "candidate_index": candidate_index,
                            "mode_index": mode_index,
                            "mode": mode,
                            "url": candidate_url,
                            "final_url": final_url,
                            "status": "ok",
                            "response_status": status_code,
                            "cards_seen": int(page_diag["cards_seen"]),
                            "page_state": page_diag["page_state"],
                            "source_status": page_diag["source_status"],
                            "source_error_type": page_diag["source_error_type"],
                            "auth_required_detected": bool(page_diag["auth_required_detected"]),
                            "login_wall_detected": bool(page_diag["login_wall_detected"]),
                            "unexpected_redirect_detected": bool(page_diag["unexpected_redirect_detected"]),
                            "layout_mismatch_detected": bool(page_diag["layout_mismatch_detected"]),
                        }
                    )

                    if page_diag["source_status"] == "success":
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
                        page_selected = True
                        if len(jobs) >= max_items:
                            stop_reason = "max_jobs_reached"
                            jobs = jobs[:max_items]
                            cards_seen_total += page_max_cards_seen
                            return jobs, warnings, _handshake_final_meta(
                                jobs=jobs,
                                discovered_raw_count=discovered_raw_count,
                                pages_fetched=pages_fetched,
                                pages_with_results=pages_with_results,
                                request_attempts=request_attempts,
                                request_urls_tried=request_urls_tried,
                                last_request_url=last_request_url,
                                stop_reason=stop_reason,
                                source_status="success",
                                source_error_type=None,
                                error_status=error_status,
                                cards_seen=cards_seen_total,
                                auth_required_detected=auth_required_detected,
                                login_wall_detected=login_wall_detected,
                                unexpected_redirect_detected=unexpected_redirect_detected,
                                layout_mismatch_detected=layout_mismatch_detected,
                            )
                        if early_stop_when_no_new_results and new_unique == 0:
                            stop_reason = "no_new_results"
                        else:
                            stop_reason = "page_results"
                        break

                    if page_diag["source_status"] == "empty_success":
                        page_selected = True
                        stop_reason = page_diag["page_state"]
                        break
                except Exception as exc:
                    error_type, error_status = _error_details(exc)
                    if _handshake_status_priority("upstream_failure") >= _handshake_status_priority(best_failure_status):
                        best_failure_status = "upstream_failure"
                        best_failure_error_type = error_type
                    request_attempts.append(
                        {
                            "page_index": page_index + 1,
                            "candidate_index": candidate_index,
                            "mode_index": mode_index,
                            "mode": mode,
                            "url": candidate_url,
                            "status": "error",
                            "error_type": error_type,
                            "error_status": error_status,
                        }
                    )
                    continue

            if page_selected:
                break

        cards_seen_total += page_max_cards_seen
        if page_selected and stop_reason in {"empty_results", "no_cards_found"}:
            break
        if stop_reason == "no_new_results":
            break

    final_status = "success" if jobs else best_failure_status
    final_error_type = None if jobs else best_failure_error_type
    if not jobs and final_status == "empty_success":
        warnings.append(f"{board_key}: empty_success")
    elif not jobs and final_status in {"auth_blocked", "layout_mismatch", "upstream_failure"}:
        warnings.append(
            f"{board_key}: {final_status}"
            + (f" error_type={final_error_type}" if final_error_type else "")
        )

    return jobs, warnings, _handshake_final_meta(
        jobs=jobs,
        discovered_raw_count=discovered_raw_count,
        pages_fetched=pages_fetched,
        pages_with_results=pages_with_results,
        request_attempts=request_attempts,
        request_urls_tried=request_urls_tried,
        last_request_url=last_request_url,
        stop_reason=stop_reason if jobs or final_status == "empty_success" else final_status,
        source_status=final_status,
        source_error_type=final_error_type,
        error_status=error_status,
        cards_seen=cards_seen_total,
        auth_required_detected=auth_required_detected,
        login_wall_detected=login_wall_detected,
        unexpected_redirect_detected=unexpected_redirect_detected,
        layout_mismatch_detected=layout_mismatch_detected,
    )


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
            "pages_attempted": 0,
            "source_status": "upstream_failure",
            "source_error_type": "unsupported_board",
            "jobs_raw": 0,
            "jobs_kept": 0,
            "cards_seen": 0,
            "auth_required_detected": False,
            "login_wall_detected": False,
            "stop_reason": "unsupported_board",
        }
    if board_key == "handshake":
        return _collect_jobs_from_handshake(
            query=query,
            location=location,
            max_jobs=max_jobs,
            max_pages=max_pages,
            early_stop_when_no_new_results=early_stop_when_no_new_results,
            url_override=url_override,
        )

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
                "source_error_type": error_type,
                "error_status": error_status,
                "source_status": "upstream_failure",
                "cards_seen": 0,
                "jobs_raw": discovered_raw_count,
                "jobs_kept": len(jobs),
                "auth_required_detected": False,
                "login_wall_detected": False,
                "unexpected_redirect_detected": False,
                "layout_mismatch_detected": False,
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
        "source_error_type": "no_jobs_found" if not jobs and error_type is None else error_type,
        "error_status": error_status,
        "source_status": "success" if jobs else "empty_success",
        "cards_seen": discovered_raw_count,
        "jobs_raw": discovered_raw_count,
        "jobs_kept": len(jobs),
        "auth_required_detected": False,
        "login_wall_detected": False,
        "unexpected_redirect_detected": False,
        "layout_mismatch_detected": False,
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
