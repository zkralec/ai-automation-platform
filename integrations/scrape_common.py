import html
import re
import threading
import time
from datetime import datetime, timezone
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin
from urllib.request import Request, urlopen

USER_AGENT = "MissionControl/1.0"
DEFAULT_SCRAPE_TIMEOUT_SECONDS = 15.0
DEFAULT_SCRAPE_CACHE_TTL_SECONDS = 120
DEFAULT_SCRAPE_RATE_LIMIT_SECONDS = 2.0
DEFAULT_SCRAPE_RETRY_ATTEMPTS = 3
TRANSIENT_HTTP_CODES = {429, 500, 502, 503, 504}

_TAG_RE = re.compile(r"<[^>]+>")
_PRICE_RE = re.compile(r"\$\s*([0-9][0-9,]*(?:\.[0-9]{2})?)")
_HTML_CACHE: dict[str, tuple[float, str]] = {}
_CACHE_LOCK = threading.Lock()
_RATE_LIMIT_LOCK = threading.Lock()
_LAST_REQUEST_TS = 0.0


def _env_float(name: str, default: float) -> float:
    import os

    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        return float(raw.strip())
    except ValueError:
        return default


def _env_int(name: str, default: int) -> int:
    import os

    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        return int(raw.strip())
    except ValueError:
        return default


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def clean_html_text(value: str) -> str:
    stripped = _TAG_RE.sub(" ", value)
    unescaped = html.unescape(stripped)
    return " ".join(unescaped.split())


def parse_price(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if not isinstance(value, str):
        return None

    text = value.strip()
    if not text:
        return None
    text = text.replace("$", "").replace(",", "")
    try:
        return float(text)
    except ValueError:
        return None


def extract_price_values(text: str) -> list[float]:
    prices: list[float] = []
    for value in _PRICE_RE.findall(text):
        parsed = parse_price(value)
        if parsed is not None:
            prices.append(parsed)
    return prices


def compute_discount_pct(price: float | None, old_price: float | None) -> float | None:
    if price is None or old_price is None:
        return None
    if old_price <= 0 or price < 0:
        return None
    discount = ((old_price - price) / old_price) * 100.0
    return round(discount, 2)


def absolute_url(base_url: str, href: str) -> str:
    return urljoin(base_url, href)


def infer_stock(snippet: str) -> bool | None:
    low = snippet.lower()
    if "sold out" in low or "out of stock" in low or "unavailable" in low:
        return False
    if "add to cart" in low or "pickup" in low or "ship it" in low:
        return True
    return None


def dedupe_deals(deals: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[str, str, str]] = set()
    output: list[dict[str, Any]] = []
    for deal in deals:
        source = str(deal.get("source") or "").strip().lower()
        url = str(deal.get("url") or "").strip()
        title = str(deal.get("title") or "").strip().lower()
        key = (source, url, title)
        if key in seen:
            continue
        seen.add(key)
        output.append(deal)
    return output


def _respect_rate_limit(wait_seconds: float) -> None:
    global _LAST_REQUEST_TS
    wait_for = max(wait_seconds, 0.0)
    if wait_for <= 0:
        return
    with _RATE_LIMIT_LOCK:
        now = time.monotonic()
        delta = now - _LAST_REQUEST_TS
        remaining = wait_for - delta
        if remaining > 0:
            time.sleep(remaining)
        _LAST_REQUEST_TS = time.monotonic()


def pick_plausible_price(prices: list[float], *, title: str | None = None) -> float | None:
    if not prices:
        return None

    t = (title or "").lower()

    # Heuristic floors by category
    floor = 0.0
    if "rtx 5090" in t or "5090" in t:
        floor = 800.0
    elif "rtx" in t or "geforce" in t:
        floor = 250.0

    # Filter obvious junk (shipping, warranty, financing)
    candidates = [p for p in prices if p >= floor and p <= 10000]
    if not candidates:
        # Fallback: at least avoid super tiny numbers
        candidates = [p for p in prices if p >= 50.0]
    if not candidates:
        return None

    # Pick the best guess. For these pages, the "real price" is usually the largest plausible $ value.
    return max(candidates)


def fetch_html(
    url: str,
    *,
    timeout_seconds: float | None = None,
    cache_ttl_seconds: int | None = None,
    rate_limit_seconds: float | None = None,
    retry_attempts: int | None = None,
    extra_headers: dict[str, str] | None = None,
) -> str:
    timeout = timeout_seconds if timeout_seconds is not None else _env_float(
        "SCRAPE_TIMEOUT_SECONDS",
        DEFAULT_SCRAPE_TIMEOUT_SECONDS,
    )
    cache_ttl = cache_ttl_seconds if cache_ttl_seconds is not None else _env_int(
        "SCRAPE_CACHE_TTL_SECONDS",
        DEFAULT_SCRAPE_CACHE_TTL_SECONDS,
    )
    rate_limit = rate_limit_seconds if rate_limit_seconds is not None else _env_float(
        "SCRAPE_RATE_LIMIT_SECONDS",
        DEFAULT_SCRAPE_RATE_LIMIT_SECONDS,
    )
    attempts = retry_attempts if retry_attempts is not None else _env_int(
        "SCRAPE_RETRY_ATTEMPTS",
        DEFAULT_SCRAPE_RETRY_ATTEMPTS,
    )
    attempts = max(int(attempts), 1)

    if cache_ttl > 0:
        with _CACHE_LOCK:
            cached = _HTML_CACHE.get(url)
            if cached:
                expires_at, html_text = cached
                if expires_at >= time.time():
                    return html_text
                del _HTML_CACHE[url]

    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }
    if extra_headers:
        headers.update(extra_headers)
    request = Request(url=url, headers=headers)

    last_error: Exception | None = None
    for attempt in range(attempts):
        _respect_rate_limit(rate_limit)
        try:
            with urlopen(request, timeout=timeout) as response:
                raw = response.read()
                charset = response.headers.get_content_charset() or "utf-8"
                html_text = raw.decode(charset, errors="replace")
                if cache_ttl > 0:
                    with _CACHE_LOCK:
                        _HTML_CACHE[url] = (time.time() + float(cache_ttl), html_text)
                return html_text
        except HTTPError as exc:
            last_error = exc
            if exc.code in TRANSIENT_HTTP_CODES and attempt < attempts - 1:
                time.sleep(min(2 ** attempt, 8))
                continue
            raise
        except (URLError, TimeoutError) as exc:
            last_error = exc
            if attempt < attempts - 1:
                time.sleep(min(2 ** attempt, 8))
                continue
            raise

    if last_error is not None:
        raise last_error
    raise RuntimeError(f"fetch_html failed for URL '{url}'")


def fetch_html_response(
    url: str,
    *,
    timeout_seconds: float | None = None,
    cache_ttl_seconds: int | None = None,
    rate_limit_seconds: float | None = None,
    retry_attempts: int | None = None,
    extra_headers: dict[str, str] | None = None,
) -> dict[str, Any]:
    timeout = timeout_seconds if timeout_seconds is not None else _env_float(
        "SCRAPE_TIMEOUT_SECONDS",
        DEFAULT_SCRAPE_TIMEOUT_SECONDS,
    )
    cache_ttl = cache_ttl_seconds if cache_ttl_seconds is not None else _env_int(
        "SCRAPE_CACHE_TTL_SECONDS",
        DEFAULT_SCRAPE_CACHE_TTL_SECONDS,
    )
    rate_limit = rate_limit_seconds if rate_limit_seconds is not None else _env_float(
        "SCRAPE_RATE_LIMIT_SECONDS",
        DEFAULT_SCRAPE_RATE_LIMIT_SECONDS,
    )
    attempts = retry_attempts if retry_attempts is not None else _env_int(
        "SCRAPE_RETRY_ATTEMPTS",
        DEFAULT_SCRAPE_RETRY_ATTEMPTS,
    )
    attempts = max(int(attempts), 1)

    if cache_ttl > 0:
        with _CACHE_LOCK:
            cached = _HTML_CACHE.get(url)
            if cached:
                expires_at, html_text = cached
                if expires_at >= time.time():
                    return {
                        "html_text": html_text,
                        "final_url": url,
                        "status_code": None,
                        "from_cache": True,
                    }
                del _HTML_CACHE[url]

    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }
    if extra_headers:
        headers.update(extra_headers)
    request = Request(url=url, headers=headers)

    last_error: Exception | None = None
    for attempt in range(attempts):
        _respect_rate_limit(rate_limit)
        try:
            with urlopen(request, timeout=timeout) as response:
                raw = response.read()
                charset = response.headers.get_content_charset() or "utf-8"
                html_text = raw.decode(charset, errors="replace")
                final_url = response.geturl()
                status_code = getattr(response, "status", None) or response.getcode()
                if cache_ttl > 0:
                    with _CACHE_LOCK:
                        _HTML_CACHE[url] = (time.time() + float(cache_ttl), html_text)
                return {
                    "html_text": html_text,
                    "final_url": final_url,
                    "status_code": int(status_code) if status_code is not None else None,
                    "from_cache": False,
                }
        except HTTPError as exc:
            last_error = exc
            if exc.code in TRANSIENT_HTTP_CODES and attempt < attempts - 1:
                time.sleep(min(2 ** attempt, 8))
                continue
            raise
        except (URLError, TimeoutError) as exc:
            last_error = exc
            if attempt < attempts - 1:
                time.sleep(min(2 ** attempt, 8))
                continue
            raise

    if last_error is not None:
        raise last_error
    raise RuntimeError(f"fetch_html_response failed for URL '{url}'")
