from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import parse_qs, urlparse

_WS_RE = re.compile(r"\s+")
_ALNUM_WS_RE = re.compile(r"[^a-z0-9\s]+")
_SALARY_RANGE_RE = re.compile(
    r"(?P<currency>USD|\$)?\s*(?P<low>\d[\d,]*(?:\.\d+)?)\s*(?P<low_suffix>[kK]?)\s*(?:-|to)\s*(?P<high>\d[\d,]*(?:\.\d+)?)\s*(?P<high_suffix>[kK]?)",
    re.IGNORECASE,
)
_SALARY_SINGLE_RE = re.compile(
    r"(?P<currency>USD|\$)?\s*(?P<value>\d[\d,]*(?:\.\d+)?)\s*(?P<suffix>[kK]?)\s*/?\s*(?:year|yr|annum)",
    re.IGNORECASE,
)
_SALARY_MIN_ONLY_RE = re.compile(
    r"(?:from|starting\s+at|minimum|min\.?)\s*(?P<currency>USD|\$)?\s*(?P<value>\d[\d,]*(?:\.\d+)?)\s*(?P<suffix>[kK]?)",
    re.IGNORECASE,
)
_SALARY_MAX_ONLY_RE = re.compile(
    r"(?:up\s+to|maximum|max\.?|capped\s+at)\s*(?P<currency>USD|\$)?\s*(?P<value>\d[\d,]*(?:\.\d+)?)\s*(?P<suffix>[kK]?)",
    re.IGNORECASE,
)
_REMOTE_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("remote", re.compile(r"\b(remote|work\s*from\s*home|wfh|anywhere)\b", re.IGNORECASE)),
    ("hybrid", re.compile(r"\bhybrid\b", re.IGNORECASE)),
    ("onsite", re.compile(r"\b(on[-\s]?site|onsite|in\s+office)\b", re.IGNORECASE)),
)
_EXPERIENCE_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("internship", re.compile(r"\b(intern|internship|co[-\s]?op)\b", re.IGNORECASE)),
    ("entry", re.compile(r"\b(entry[-\s]?level|junior|new\s*grad|associate)\b", re.IGNORECASE)),
    ("mid", re.compile(r"\b(mid[-\s]?level|intermediate|level\s*ii|level\s*2)\b", re.IGNORECASE)),
    ("senior", re.compile(r"\b(senior|sr\.?|lead|staff|principal|manager|director|head)\b", re.IGNORECASE)),
)
_COMPANY_SUFFIX_RE = re.compile(r"\b(inc|inc\.|llc|l\.l\.c\.|corp|corporation|co|company|ltd|limited)\b", re.IGNORECASE)
_LOCATION_ALIAS: tuple[tuple[re.Pattern[str], str], ...] = (
    (re.compile(r"\bnew york( city)?\b(?!\s+ny\b)", re.IGNORECASE), "new york ny"),
    (re.compile(r"\bsan francisco\b", re.IGNORECASE), "san francisco ca"),
    (re.compile(r"\blos angeles\b", re.IGNORECASE), "los angeles ca"),
)
_LOCATION_DISPLAY_ALIAS: tuple[tuple[re.Pattern[str], str], ...] = (
    (re.compile(r"^new york(?: city)?(?:,?\s*ny)?$", re.IGNORECASE), "New York, NY"),
    (re.compile(r"^nyc$", re.IGNORECASE), "New York, NY"),
    (re.compile(r"^san francisco(?:,?\s*ca)?$", re.IGNORECASE), "San Francisco, CA"),
    (re.compile(r"^los angeles(?:,?\s*ca)?$", re.IGNORECASE), "Los Angeles, CA"),
    (re.compile(r"^remote(?:\s+[a-z]{2,})?$", re.IGNORECASE), "Remote"),
    (re.compile(r"^hybrid$", re.IGNORECASE), "Hybrid"),
    (re.compile(r"^onsite$", re.IGNORECASE), "Onsite"),
)
_TITLE_TOKEN_ALIAS = {
    "sr": "senior",
    "sr.": "senior",
    "jr": "junior",
    "jr.": "junior",
    "eng": "engineer",
    "dev": "developer",
}
_TITLE_STOPWORDS = {"the", "a", "an", "for", "of", "to", "and", "with"}
_ACRONYMS = {"ai", "ml", "nlp", "llm", "sre", "qa", "ui", "ux", "gpu", "cpu", "api", "sql"}
_UNKNOWN_COMPANY_CANONICAL = {
    "unknown",
    "unknown company",
    "unknown employer",
    "unknown organization",
    "not provided",
    "not listed",
    "n a",
    "na",
    "none",
}
_RELATIVE_POSTED_AT_RE = re.compile(
    r"(?P<amount>\d+|a|an)\s*(?P<unit>minute|min|hour|hr|day|d|week|wk|month|mo)s?\s*(?:ago)?",
    re.IGNORECASE,
)
_RELATIVE_PLUS_DAYS_RE = re.compile(r"(?P<amount>\d+)\+\s*days?", re.IGNORECASE)
_POSTED_TODAY_RE = re.compile(r"\b(today|just posted|posted today)\b", re.IGNORECASE)
_POSTED_YESTERDAY_RE = re.compile(r"\byesterday\b", re.IGNORECASE)
_BROAD_LOCATION_KEYS = {
    "united states",
    "united states of america",
    "usa",
    "us",
    "u s",
    "north america",
}


def _as_text(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    text = " ".join(value.strip().split())
    return text if text else None


def _as_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        text = value.strip().replace(",", "")
        if text.startswith("$"):
            text = text[1:]
        if not text:
            return None
        try:
            return float(text)
        except ValueError:
            return None
    return None


def _normalize_work_mode(value: Any) -> str | None:
    text = _as_text(value)
    if not text:
        return None
    low = text.lower()
    if low in {"remote", "hybrid", "onsite", "on-site"}:
        return "onsite" if low == "on-site" else low
    return None


def _normalize_experience_level(value: Any) -> str | None:
    text = _as_text(value)
    if not text:
        return None
    low = text.lower()
    if low in {"intern", "internship", "co-op", "coop"}:
        return "internship"
    if low in {"entry", "entry-level", "junior", "new grad", "associate"}:
        return "entry"
    if low in {"mid", "mid-level", "intermediate"}:
        return "mid"
    if low in {"senior", "lead", "staff", "principal", "manager", "director"}:
        return "senior"
    return low


def _compact_text(text: str) -> str:
    return _WS_RE.sub(" ", text.strip())


def _canonical_text(text: str) -> str:
    low = text.lower()
    low = _ALNUM_WS_RE.sub(" ", low)
    return _compact_text(low)


def _normalize_company_key(company: str | None) -> str:
    if not company:
        return ""
    low = _canonical_text(company)
    low = _COMPANY_SUFFIX_RE.sub(" ", low)
    low = _compact_text(low)
    return low


def _normalize_company(value: Any) -> str | None:
    text = _as_text(value)
    if not text:
        return None
    if _canonical_text(text) in _UNKNOWN_COMPANY_CANONICAL:
        return None
    if text.lower().startswith("at "):
        text = text[3:].strip()
    text = re.sub(r"[\s|/,-]+$", "", text).strip()
    if not text:
        return None
    return normalize_title_case(text) or text


def _normalize_location_key(location: str | None, remote_type: str | None) -> str:
    # Deduplication is anchored on location text first; remote_type is fallback only.
    if not location and remote_type in {"remote", "hybrid", "onsite"}:
        return remote_type
    if not location:
        return "unspecified"
    low = _canonical_text(location)
    for pattern, replacement in _LOCATION_ALIAS:
        low = pattern.sub(replacement, low)
    low = low.replace(",", " ")
    low = _compact_text(low)
    return low or "unspecified"


def normalize_location_for_matching(value: Any, remote_type: Any = None) -> str:
    location_text = _normalize_location(value) or _as_text(value)
    normalized_remote_type = _normalize_work_mode(remote_type)
    if normalized_remote_type is None and isinstance(value, str):
        normalized_remote_type = _normalize_work_mode(value)
    return _normalize_location_key(location_text, normalized_remote_type)


def classify_location_quality(
    location: Any,
    *,
    location_normalized: Any = None,
    remote_type: Any = None,
) -> str:
    location_text = _normalize_location(location) or _as_text(location)
    normalized_key = _as_text(location_normalized) or normalize_location_for_matching(location_text, remote_type)
    work_mode = _normalize_work_mode(remote_type) or _normalize_work_mode(location_text)
    if not location_text and not work_mode:
        return "missing"
    if normalized_key in {"remote", "hybrid", "onsite"}:
        return "mode_only"
    if normalized_key in _BROAD_LOCATION_KEYS:
        return "broad"
    if location_text and len(_canonical_text(location_text)) <= 2:
        return "weak"
    return "structured"


def _canonicalize_title(title: str | None) -> str:
    if not title:
        return ""
    tokens = _canonical_text(title).split()
    output: list[str] = []
    for token in tokens:
        if token in _TITLE_TOKEN_ALIAS:
            token = _TITLE_TOKEN_ALIAS[token]
        if token in _TITLE_STOPWORDS:
            continue
        output.append(token)
    return " ".join(output)


def _normalize_location(value: Any) -> str | None:
    text = _as_text(value)
    if not text:
        return None
    normalized = _compact_text(text.replace(" - ", ", "))
    for pattern, replacement in _LOCATION_DISPLAY_ALIAS:
        if pattern.search(normalized):
            return replacement

    parts = [part.strip() for part in normalized.split(",")]
    output_parts: list[str] = []
    for part in parts:
        if not part:
            continue
        words = []
        for word in part.split():
            low = word.lower()
            if low in _ACRONYMS or (len(low) == 2 and low.isalpha()) or low in {"us", "usa"}:
                words.append(low.upper())
            else:
                words.append(low.capitalize())
        output_parts.append(" ".join(words))
    result = ", ".join(output_parts).strip()
    return result or None


def _apply_title_case_word(word: str, *, is_first: bool) -> str:
    if not word:
        return word
    low = word.lower()
    if low in _ACRONYMS:
        return low.upper()
    if not is_first and low in _TITLE_STOPWORDS:
        return low
    return low[:1].upper() + low[1:]


def normalize_title_case(title: str | None) -> str | None:
    text = _as_text(title)
    if not text:
        return None

    # Keep mixed-case titles as provided, normalize mostly-all-caps/lowercase titles.
    letters = [ch for ch in text if ch.isalpha()]
    if not letters:
        return text
    upper_ratio = sum(1 for ch in letters if ch.isupper()) / float(len(letters))
    lower_ratio = sum(1 for ch in letters if ch.islower()) / float(len(letters))
    if upper_ratio < 0.65 and lower_ratio < 0.95:
        return text

    words = text.split()
    normalized = [
        _apply_title_case_word(word, is_first=(idx == 0))
        for idx, word in enumerate(words)
    ]
    return " ".join(normalized)


def _number_from_salary_token(value: str, suffix: str) -> float | None:
    number = _as_float(value)
    if number is None:
        return None
    if suffix.lower() == "k":
        return number * 1000.0
    return number


def _parse_salary_text(salary_text: str) -> tuple[float | None, float | None, str | None]:
    text = salary_text.strip()
    if not text:
        return None, None, None

    range_match = _SALARY_RANGE_RE.search(text)
    if range_match:
        low = _number_from_salary_token(range_match.group("low"), range_match.group("low_suffix"))
        high = _number_from_salary_token(range_match.group("high"), range_match.group("high_suffix"))
        currency_raw = (range_match.group("currency") or "").strip().upper()
        currency = "USD" if currency_raw in {"$", "USD"} else (currency_raw or None)
        if low is not None and high is not None and high < low:
            low, high = high, low
        return low, high, currency

    single_match = _SALARY_SINGLE_RE.search(text)
    if single_match:
        value = _number_from_salary_token(single_match.group("value"), single_match.group("suffix"))
        currency_raw = (single_match.group("currency") or "").strip().upper()
        currency = "USD" if currency_raw in {"$", "USD"} else (currency_raw or None)
        return value, value, currency

    min_only_match = _SALARY_MIN_ONLY_RE.search(text)
    if min_only_match:
        value = _number_from_salary_token(min_only_match.group("value"), min_only_match.group("suffix"))
        currency_raw = (min_only_match.group("currency") or "").strip().upper()
        currency = "USD" if currency_raw in {"$", "USD"} else (currency_raw or None)
        return value, None, currency

    max_only_match = _SALARY_MAX_ONLY_RE.search(text)
    if max_only_match:
        value = _number_from_salary_token(max_only_match.group("value"), max_only_match.group("suffix"))
        currency_raw = (max_only_match.group("currency") or "").strip().upper()
        currency = "USD" if currency_raw in {"$", "USD"} else (currency_raw or None)
        return None, value, currency

    return None, None, None


def _format_money(value: float) -> str:
    rounded = int(round(value))
    return f"${rounded:,}"


def _format_salary_text(salary_min: float | None, salary_max: float | None, currency: str | None) -> str | None:
    if salary_min is None and salary_max is None:
        return None
    if currency and currency.upper() != "USD":
        prefix = f"{currency.upper()} "
        if salary_min is not None and salary_max is not None:
            return f"{prefix}{int(round(salary_min)):,} - {int(round(salary_max)):,}"
        anchor = salary_max if salary_max is not None else salary_min
        return f"{prefix}{int(round(anchor or 0)):,}"

    if salary_min is not None and salary_max is not None:
        return f"{_format_money(salary_min)} - {_format_money(salary_max)}"
    anchor = salary_max if salary_max is not None else salary_min
    return _format_money(anchor or 0.0)


def _parse_datetime(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
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


def _isoformat_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _normalize_posted_at(value: Any, *, reference_time: datetime | None) -> tuple[str | None, str | None]:
    text = _as_text(value)
    if not text:
        return None, None

    parsed = _parse_datetime(text)
    if parsed is not None:
        return _isoformat_utc(parsed), None

    if reference_time is None:
        reference_time = datetime.now(timezone.utc)
    normalized = text.lower().replace("+", " + ")
    if _POSTED_TODAY_RE.search(normalized):
        return _isoformat_utc(reference_time), text
    if _POSTED_YESTERDAY_RE.search(normalized):
        return _isoformat_utc(reference_time - timedelta(days=1)), text

    plus_match = _RELATIVE_PLUS_DAYS_RE.search(normalized)
    if plus_match:
        amount = max(int(plus_match.group("amount")), 0)
        return _isoformat_utc(reference_time - timedelta(days=amount)), text

    relative = _RELATIVE_POSTED_AT_RE.search(normalized)
    if relative:
        raw_amount = relative.group("amount").lower()
        amount = 1 if raw_amount in {"a", "an"} else max(int(raw_amount), 0)
        unit = relative.group("unit").lower()
        if unit.startswith("min"):
            delta = timedelta(minutes=amount)
        elif unit.startswith("h"):
            delta = timedelta(hours=amount)
        elif unit.startswith("w"):
            delta = timedelta(days=amount * 7)
        elif unit.startswith("mo"):
            delta = timedelta(days=amount * 30)
        else:
            delta = timedelta(days=amount)
        return _isoformat_utc(reference_time - delta), text

    return None, text


def _normalize_posted_age_days(
    *,
    posted_at: str | None,
    posted_at_raw: str | None,
    raw_value: Any,
    reference_time: datetime,
) -> int | None:
    if isinstance(raw_value, int):
        return max(raw_value, 0)
    if isinstance(raw_value, float):
        return max(int(raw_value), 0)

    parsed = _parse_datetime(posted_at)
    if parsed is not None:
        delta = reference_time - parsed
        return max(int(delta.total_seconds() // 86400), 0)

    _, fallback_raw = _normalize_posted_at(posted_at_raw, reference_time=reference_time)
    if fallback_raw is not None:
        posted_at_raw = fallback_raw
    if not posted_at_raw:
        return None
    normalized, _ = _normalize_posted_at(posted_at_raw, reference_time=reference_time)
    parsed_fallback = _parse_datetime(normalized)
    if parsed_fallback is None:
        return None
    delta = reference_time - parsed_fallback
    return max(int(delta.total_seconds() // 86400), 0)


def resolve_posted_age_days(
    *,
    posted_age_days: Any = None,
    posted_at: Any = None,
    posted_at_raw: Any = None,
    reference_time: datetime | None = None,
) -> int | None:
    reference = reference_time or datetime.now(timezone.utc)
    return _normalize_posted_age_days(
        posted_at=_as_text(posted_at),
        posted_at_raw=_as_text(posted_at_raw),
        raw_value=posted_age_days,
        reference_time=reference,
    )


def classify_recency_quality(
    *,
    posted_at_normalized: Any = None,
    posted_at_raw: Any = None,
    posted_age_days: Any = None,
    reference_time: datetime | None = None,
) -> str:
    age_days = resolve_posted_age_days(
        posted_age_days=posted_age_days,
        posted_at=posted_at_normalized,
        posted_at_raw=posted_at_raw,
        reference_time=reference_time,
    )
    normalized = _as_text(posted_at_normalized)
    raw = _as_text(posted_at_raw)
    if age_days is not None and normalized:
        if raw:
            return "relative_normalized"
        return "timestamp"
    if age_days is not None:
        return "age_only"
    return "missing"


def infer_remote_type(*, title: str | None, location: str | None, description_snippet: str | None) -> str | None:
    haystack = " ".join(
        part.strip()
        for part in (title or "", location or "", description_snippet or "")
        if isinstance(part, str) and part.strip()
    )
    if not haystack:
        return None
    for label, pattern in _REMOTE_PATTERNS:
        if pattern.search(haystack):
            return label
    return None


def infer_experience_level(*, title: str | None, description_snippet: str | None) -> str | None:
    haystack = " ".join(
        part.strip()
        for part in (title or "", description_snippet or "")
        if isinstance(part, str) and part.strip()
    )
    if not haystack:
        return None
    for label, pattern in _EXPERIENCE_PATTERNS:
        if pattern.search(haystack):
            return label
    return None


def _source_url_kind(url: str | None) -> str:
    if not url:
        return "missing"
    parsed = urlparse(url)
    path = (parsed.path or "").lower()
    query_keys = {key.lower() for key in parse_qs(parsed.query).keys()}
    if any(key in query_keys for key in {"q", "query", "keywords", "sc.keyword"}):
        return "search"
    if "search" in path and not any(token in path for token in ("view", "listing")):
        return "search"
    if any(token in path for token in ("/jobs/view", "/viewjob", "joblisting", "/stu/jobs/")):
        return "direct"
    if path and path not in {"/", "/jobs"}:
        return "direct"
    return "unknown"


def _build_source_url(raw_job: dict[str, Any]) -> str | None:
    metadata = raw_job.get("source_metadata") if isinstance(raw_job.get("source_metadata"), dict) else {}
    legacy = raw_job.get("raw") if isinstance(raw_job.get("raw"), dict) else {}
    candidates = [
        _as_text(raw_job.get("url")),
        _as_text(raw_job.get("source_url")),
        _as_text(metadata.get("job_url")),
        _as_text(legacy.get("job_url")),
        _as_text(metadata.get("search_url")),
        _as_text(legacy.get("search_url")),
    ]
    for candidate in candidates:
        if candidate and _source_url_kind(candidate) == "direct":
            return candidate
    for candidate in candidates:
        if candidate:
            return candidate
    return None


def metadata_quality_details(job: dict[str, Any]) -> dict[str, Any]:
    company = _as_text(job.get("company"))
    location = _as_text(job.get("location"))
    posted_at = _as_text(job.get("posted_at_normalized")) or _as_text(job.get("posted_at"))
    posted_at_raw = _as_text(job.get("posted_at_raw"))
    posted_age_days = resolve_posted_age_days(
        posted_age_days=job.get("posted_age_days"),
        posted_at=posted_at,
        posted_at_raw=posted_at_raw,
    )
    location_normalized = _as_text(job.get("location_normalized")) or normalize_location_for_matching(
        location,
        job.get("remote_type") or job.get("work_mode"),
    )
    source_url = _as_text(job.get("source_url"))
    work_mode = _normalize_work_mode(job.get("work_mode")) or _normalize_work_mode(job.get("remote_type"))
    location_quality = classify_location_quality(
        location,
        location_normalized=location_normalized,
        remote_type=work_mode,
    )
    recency_quality = classify_recency_quality(
        posted_at_normalized=posted_at,
        posted_at_raw=posted_at_raw,
        posted_age_days=posted_age_days,
    )
    salary_present = any(
        value is not None
        for value in (_as_float(job.get("salary_min")), _as_float(job.get("salary_max")))
    ) or bool(_as_text(job.get("salary_text")))
    description_present = bool(_as_text(job.get("description_snippet")))
    experience_present = bool(_normalize_experience_level(job.get("experience_level")))
    source_url_kind = _source_url_kind(source_url)

    score = 0.0
    if _as_text(job.get("title")):
        score += 2.0
    if company:
        score += 25.0
    if location_quality == "structured":
        score += 12.0
    elif location_quality == "mode_only":
        score += 8.0
    elif location_quality == "broad":
        score += 5.0
    elif location:
        score += 3.0
    if source_url:
        score += 18.0 if source_url_kind == "direct" else 8.0
    if recency_quality in {"timestamp", "relative_normalized"}:
        score += 15.0
    elif recency_quality == "age_only":
        score += 11.0
    if work_mode:
        score += 10.0
    if salary_present:
        score += 8.0
    if description_present:
        score += 6.0
    if experience_present:
        score += 4.0

    return {
        "metadata_quality_score": round(max(0.0, min(score, 100.0)), 2),
        "missing_company": not bool(company),
        "missing_source_url": not bool(source_url),
        "missing_posted_at": not bool(posted_at),
        "missing_location": location_quality in {"missing", "weak"},
        "source_url_kind": source_url_kind,
        "has_direct_source_url": source_url_kind == "direct",
        "metadata_quality_location": location_quality,
        "metadata_quality_recency": recency_quality,
    }


def normalize_job_record(raw_job: dict[str, Any], *, index: int) -> dict[str, Any] | None:
    title_raw = _as_text(raw_job.get("title"))
    if not title_raw:
        return None

    title = normalize_title_case(title_raw) or title_raw
    company = _normalize_company(raw_job.get("company"))
    location = _normalize_location(raw_job.get("location"))
    description_snippet = _as_text(raw_job.get("description_snippet"))
    source = (_as_text(raw_job.get("source")) or "unknown").lower()
    source_url = _build_source_url(raw_job)
    source_url_kind = _source_url_kind(source_url)

    salary_text_in = _as_text(raw_job.get("salary_text"))
    salary_min = _as_float(raw_job.get("salary_min"))
    salary_max = _as_float(raw_job.get("salary_max"))
    salary_currency = _as_text(raw_job.get("salary_currency"))
    parsed_min, parsed_max, parsed_currency = _parse_salary_text(salary_text_in or "")
    if salary_min is None:
        salary_min = parsed_min
    if salary_max is None:
        salary_max = parsed_max
    if salary_min is not None and salary_max is not None and salary_max < salary_min:
        salary_min, salary_max = salary_max, salary_min
    if not salary_currency:
        salary_currency = parsed_currency
    formatted_salary_text = _format_salary_text(salary_min, salary_max, salary_currency)
    if parsed_min is not None or parsed_max is not None:
        salary_text = formatted_salary_text or salary_text_in
    else:
        salary_text = salary_text_in or formatted_salary_text

    remote_type = _normalize_work_mode(raw_job.get("remote_type")) or _normalize_work_mode(raw_job.get("work_mode"))
    if not remote_type:
        remote_type = infer_remote_type(title=title, location=location, description_snippet=description_snippet)

    experience_level = _normalize_experience_level(raw_job.get("experience_level"))
    if not experience_level:
        experience_level = infer_experience_level(title=title, description_snippet=description_snippet)

    source_metadata = raw_job.get("source_metadata") if isinstance(raw_job.get("source_metadata"), dict) else {}
    reference_time = (
        _parse_datetime(raw_job.get("scraped_at"))
        or _parse_datetime(source_metadata.get("scraped_at"))
        or datetime.now(timezone.utc)
    )
    posted_at, posted_at_raw = _normalize_posted_at(raw_job.get("posted_at"), reference_time=reference_time)
    if posted_at_raw is None:
        posted_at_raw = _as_text(source_metadata.get("posted_at_text"))
    posted_age_days = _normalize_posted_age_days(
        posted_at=posted_at,
        posted_at_raw=posted_at_raw,
        raw_value=raw_job.get("posted_age_days"),
        reference_time=reference_time,
    )
    location_normalized = _normalize_location_key(location, remote_type)
    metadata_quality_location = classify_location_quality(
        location,
        location_normalized=location_normalized,
        remote_type=remote_type,
    )
    metadata_quality_recency = classify_recency_quality(
        posted_at_normalized=posted_at,
        posted_at_raw=posted_at_raw,
        posted_age_days=posted_age_days,
        reference_time=reference_time,
    )

    normalized = {
        "normalized_job_id": f"norm-{index + 1:06d}",
        "title": title,
        "company": company,
        "location": location,
        "location_normalized": location_normalized,
        "remote_type": remote_type,
        "work_mode": remote_type,
        "salary_min": salary_min,
        "salary_max": salary_max,
        "salary_text": salary_text,
        "salary_currency": salary_currency or ("USD" if salary_min is not None or salary_max is not None else None),
        "source": source,
        "source_url": source_url,
        "source_url_kind": source_url_kind,
        "url": _as_text(raw_job.get("url")),
        "description_snippet": description_snippet,
        "posted_at": posted_at,
        "posted_at_normalized": posted_at,
        "posted_at_raw": posted_at_raw,
        "posted_age_days": posted_age_days,
        "metadata_quality_location": metadata_quality_location,
        "metadata_quality_recency": metadata_quality_recency,
        "experience_level": experience_level,
        "seniority": experience_level,
        "source_metadata": source_metadata,
        "_dedupe_company_key": _normalize_company_key(company),
        "_dedupe_title_key": _canonicalize_title(title),
        "_dedupe_location_key": _normalize_location_key(location, remote_type),
    }
    normalized["canonical_job_key"] = canonical_job_key(normalized)
    normalized.update(metadata_quality_details(normalized))
    return normalized


def normalize_jobs(raw_jobs: Any) -> tuple[list[dict[str, Any]], dict[str, int]]:
    if not isinstance(raw_jobs, list):
        return [], {"invalid_item_type": 0, "missing_title": 0}

    normalized: list[dict[str, Any]] = []
    drop_reasons = {"invalid_item_type": 0, "missing_title": 0}
    for idx, row in enumerate(raw_jobs):
        if not isinstance(row, dict):
            drop_reasons["invalid_item_type"] += 1
            continue
        item = normalize_job_record(row, index=idx)
        if item is None:
            drop_reasons["missing_title"] += 1
            continue
        normalized.append(item)
    return normalized, drop_reasons


def _token_set(value: str) -> set[str]:
    return {token for token in _canonical_text(value).split() if token}


def _title_similarity(left: str, right: str) -> float:
    left_tokens = _token_set(left)
    right_tokens = _token_set(right)
    if not left_tokens or not right_tokens:
        return 0.0
    inter = left_tokens.intersection(right_tokens)
    union = left_tokens.union(right_tokens)
    return len(inter) / float(len(union)) if union else 0.0


def _best_representative(jobs: list[dict[str, Any]]) -> dict[str, Any]:
    def _quality_score(job: dict[str, Any]) -> tuple[float, int, float, str]:
        non_empty_fields = [
            "title",
            "company",
            "location",
            "remote_type",
            "salary_text",
            "source_url",
            "url",
            "description_snippet",
            "posted_at",
            "experience_level",
        ]
        filled = sum(1 for key in non_empty_fields if job.get(key))
        metadata_quality = float(job.get("metadata_quality_score") or 0.0)
        salary_anchor = float(job.get("salary_max") or job.get("salary_min") or 0.0)
        return (metadata_quality, filled, salary_anchor, str(job.get("normalized_job_id") or ""))

    return max(jobs, key=_quality_score)


def _group_key(job: dict[str, Any]) -> str:
    company_key = str(job.get("_dedupe_company_key") or "").strip()
    title_key = str(job.get("_dedupe_title_key") or "").strip()
    location_key = str(job.get("_dedupe_location_key") or "unspecified").strip() or "unspecified"
    if company_key and title_key:
        return f"{company_key}|{title_key}|{location_key}"
    if title_key:
        return f"title:{title_key}|{location_key}"
    return f"id:{job.get('normalized_job_id')}"


def canonical_job_key(job: dict[str, Any]) -> str:
    if not isinstance(job, dict):
        return ""

    company_key = str(job.get("_dedupe_company_key") or "").strip()
    if not company_key:
        company_key = _normalize_company_key(_normalize_company(job.get("company")))

    title_key = str(job.get("_dedupe_title_key") or "").strip()
    if not title_key:
        title_raw = normalize_title_case(_as_text(job.get("title"))) or _as_text(job.get("title")) or ""
        title_key = _canonicalize_title(title_raw)

    location_key = str(job.get("_dedupe_location_key") or "").strip()
    if not location_key:
        location_value = _normalize_location(job.get("location"))
        remote_type = _normalize_work_mode(job.get("remote_type")) or _normalize_work_mode(job.get("work_mode"))
        location_key = _normalize_location_key(location_value, remote_type)

    if company_key and title_key:
        return f"job:{company_key}|{title_key}|{location_key or 'unspecified'}"
    if title_key:
        return f"job:title:{title_key}|{location_key or 'unspecified'}"

    source_url = _build_source_url(job)
    if source_url:
        return f"url:{source_url.lower()}"

    normalized_job_id = str(job.get("normalized_job_id") or job.get("job_id") or "").strip()
    if normalized_job_id:
        return f"id:{normalized_job_id}"
    return ""


def _build_group_output(
    *,
    group_id: str,
    group_jobs: list[dict[str, Any]],
    match_method: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    representative = _best_representative(group_jobs)
    deduped_job = {key: value for key, value in representative.items() if not key.startswith("_")}

    # Merge useful fields from all members so deduped output keeps the richest available data.
    salary_mins = [_as_float(job.get("salary_min")) for job in group_jobs]
    salary_mins = [value for value in salary_mins if value is not None]
    salary_maxes = [_as_float(job.get("salary_max")) for job in group_jobs]
    salary_maxes = [value for value in salary_maxes if value is not None]
    if salary_mins:
        deduped_job["salary_min"] = min(salary_mins)
    if salary_maxes:
        deduped_job["salary_max"] = max(salary_maxes)
    if deduped_job.get("salary_min") is not None and deduped_job.get("salary_max") is not None:
        if float(deduped_job["salary_max"]) < float(deduped_job["salary_min"]):
            deduped_job["salary_min"], deduped_job["salary_max"] = deduped_job["salary_max"], deduped_job["salary_min"]

    if not deduped_job.get("salary_text"):
        deduped_job["salary_text"] = _format_salary_text(
            _as_float(deduped_job.get("salary_min")),
            _as_float(deduped_job.get("salary_max")),
            _as_text(deduped_job.get("salary_currency")),
        )

    for field in (
        "remote_type",
        "experience_level",
        "seniority",
        "posted_at",
        "posted_at_raw",
        "source_url",
        "source_url_kind",
        "url",
        "location",
        "company",
        "title",
    ):
        if deduped_job.get(field):
            continue
        for job in group_jobs:
            candidate = job.get(field)
            if candidate:
                deduped_job[field] = candidate
                break

    if not deduped_job.get("work_mode"):
        deduped_job["work_mode"] = deduped_job.get("remote_type")
    if not deduped_job.get("seniority"):
        deduped_job["seniority"] = deduped_job.get("experience_level")

    description_candidates = [
        str(job.get("description_snippet") or "").strip()
        for job in group_jobs
        if str(job.get("description_snippet") or "").strip()
    ]
    if description_candidates and not deduped_job.get("description_snippet"):
        deduped_job["description_snippet"] = max(description_candidates, key=len)

    sources = sorted({str(job.get("source") or "").strip().lower() for job in group_jobs if str(job.get("source") or "").strip()})
    source_urls = sorted({str(job.get("source_url") or "").strip() for job in group_jobs if str(job.get("source_url") or "").strip()})
    member_ids = [str(job.get("normalized_job_id") or "") for job in group_jobs if str(job.get("normalized_job_id") or "")]

    deduped_job["duplicate_group_id"] = group_id
    deduped_job["duplicate_count"] = len(group_jobs)
    deduped_job["duplicate_sources"] = sources
    deduped_job["duplicate_source_urls"] = source_urls
    deduped_job["duplicate_member_ids"] = member_ids
    deduped_job["duplicate_match_method"] = match_method
    deduped_job["canonical_job_key"] = canonical_job_key(representative)
    deduped_job.update(metadata_quality_details(deduped_job))

    group_summary = {
        "group_id": group_id,
        "match_method": match_method,
        "canonical_key": _group_key(representative),
        "representative_id": representative.get("normalized_job_id"),
        "member_ids": member_ids,
        "member_sources": sources,
        "member_count": len(group_jobs),
        "collapsed_count": max(len(group_jobs) - 1, 0),
    }
    return deduped_job, group_summary


def dedupe_normalized_jobs(
    normalized_jobs: list[dict[str, Any]],
    *,
    fuzzy_enabled: bool = True,
    fuzzy_threshold: float = 0.84,
    fuzzy_ambiguous_threshold: float = 0.68,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], int]:
    if not normalized_jobs:
        return [], [], [], 0

    exact_groups: dict[str, list[dict[str, Any]]] = {}
    for job in normalized_jobs:
        key = _group_key(job)
        exact_groups.setdefault(key, []).append(job)

    groups: list[dict[str, Any]] = [
        {"jobs": rows[:], "match_method": "exact"}
        for rows in exact_groups.values()
    ]

    ambiguous_cases: list[dict[str, Any]] = []
    if fuzzy_enabled:
        merged: list[dict[str, Any]] = []
        by_bucket: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for group in groups:
            repr_job = _best_representative(group["jobs"])
            bucket = (
                str(repr_job.get("_dedupe_company_key") or "").strip(),
                str(repr_job.get("_dedupe_location_key") or "").strip(),
            )
            by_bucket.setdefault(bucket, []).append(group)

        for _, bucket_groups in by_bucket.items():
            used = [False] * len(bucket_groups)
            for idx, group in enumerate(bucket_groups):
                if used[idx]:
                    continue
                current_jobs = group["jobs"][:]
                current_method = group["match_method"]
                base_title = str(_best_representative(current_jobs).get("_dedupe_title_key") or "")
                used[idx] = True
                for jdx in range(idx + 1, len(bucket_groups)):
                    if used[jdx]:
                        continue
                    candidate_jobs = bucket_groups[jdx]["jobs"]
                    candidate_title = str(_best_representative(candidate_jobs).get("_dedupe_title_key") or "")
                    similarity = _title_similarity(base_title, candidate_title)
                    if similarity >= fuzzy_threshold:
                        current_jobs.extend(candidate_jobs)
                        current_method = "fuzzy"
                        used[jdx] = True
                    elif similarity >= fuzzy_ambiguous_threshold:
                        ambiguous_cases.append(
                            {
                                "left_group_key": _group_key(_best_representative(current_jobs)),
                                "right_group_key": _group_key(_best_representative(candidate_jobs)),
                                "title_similarity": round(similarity, 4),
                                "reason": "similar_title_same_company_location_not_auto_merged",
                            }
                        )
                merged.append({"jobs": current_jobs, "match_method": current_method})
        groups = merged

    deduped_jobs: list[dict[str, Any]] = []
    group_summaries: list[dict[str, Any]] = []
    for idx, group in enumerate(groups, start=1):
        deduped_job, summary = _build_group_output(
            group_id=f"dup-{idx:05d}",
            group_jobs=group["jobs"],
            match_method=str(group.get("match_method") or "exact"),
        )
        deduped_jobs.append(deduped_job)
        group_summaries.append(summary)

    duplicates_collapsed = max(len(normalized_jobs) - len(deduped_jobs), 0)
    return deduped_jobs, group_summaries, ambiguous_cases, duplicates_collapsed
