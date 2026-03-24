import os
import sys
from urllib.error import HTTPError

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "worker"))

from integrations import job_boards_scrape
from integrations.jobs_collectors import base


def test_collect_board_jobs_expands_queries_and_defers_strict_filtering(monkeypatch) -> None:
    def _job(title: str, url: str, *, description: str = "", location: str = "Austin, TX") -> dict:
        return {
            "source": "linkedin",
            "title": title,
            "company": "Acme",
            "location": location,
            "url": url,
            "description_snippet": description,
            "raw": {"search_url": "https://example.test/search"},
        }

    calls: list[tuple[str, str, int, bool]] = []

    def _fake_collect(
        board: str,
        *,
        query: str,
        location: str,
        max_jobs: int = 25,
        max_pages: int = 1,
        early_stop_when_no_new_results: bool = True,
        url_override: str | None = None,
    ) -> tuple[list[dict], list[str], dict[str, object]]:
        del board, max_jobs, url_override
        calls.append((query, location, max_pages, early_stop_when_no_new_results))
        if query == "Software Engineer remote":
            jobs = [
                _job("Software Engineer", "https://www.linkedin.com/jobs/view/1", description="distributed systems"),
                _job("Software Engineer", "https://www.linkedin.com/jobs/view/2", description="python systems"),
            ]
        elif query == "Backend Software Engineer remote":
            jobs = [
                _job("Software Engineer", "https://www.linkedin.com/jobs/view/2", description="python systems"),
                _job(
                    "Contract Software Engineer",
                    "https://www.linkedin.com/jobs/view/3",
                    description="contract role",
                ),
            ]
        elif query == "Backend Engineer remote":
            jobs = [
                _job("Backend Engineer", "https://www.linkedin.com/jobs/view/4", description="api services"),
            ]
        elif query == "Software Engineer New York":
            jobs = [
                _job("Software Engineer", "https://www.linkedin.com/jobs/view/5", location="New York, NY"),
            ]
        elif query == "Backend Software Engineer New York":
            jobs = [
                _job(
                    "Backend Software Engineer",
                    "https://www.linkedin.com/jobs/view/6",
                    location="New York, NY",
                ),
            ]
        else:
            jobs = []
        return jobs, [], {
            "discovered_raw_count": len(jobs),
            "pages_fetched": max_pages,
            "pages_with_results": 1 if jobs else 0,
            "stop_reason": "max_pages_reached",
        }

    monkeypatch.setattr(base, "collect_jobs_from_board", _fake_collect)

    result = base.collect_board_jobs(
        "linkedin",
        {
            "titles": ["Software Engineer"],
            "keywords": ["backend"],
            "excluded_keywords": ["contract"],
            "locations": ["Remote", "New York"],
            "result_limit_per_source": 10,
            "max_pages_per_source": 4,
            "max_queries_per_title_location_pair": 3,
            "max_queries_per_run": 5,
            "early_stop_when_no_new_results": False,
        },
    )

    assert result["status"] == "success"
    assert [query for query, _location, _pages, _early_stop in calls] == [
        "Software Engineer remote",
        "Backend Software Engineer remote",
        "Backend Engineer remote",
        "Software Engineer New York",
        "Backend Software Engineer New York",
    ]
    assert all(max_pages == 4 for _query, _location, max_pages, _early_stop in calls)
    assert all(early_stop is False for _query, _location, _pages, early_stop in calls)

    jobs = result["jobs"]
    assert len(jobs) == 5
    assert any(job["title"] == "Backend Engineer" for job in jobs)
    assert any(job["title"] == "Backend Software Engineer" for job in jobs)
    assert all(job["title"] != "Contract Software Engineer" for job in jobs)
    assert jobs[0]["query_context"]["query"] == "Software Engineer remote"
    assert jobs[0]["source_metadata"]["query_text"] == "Software Engineer remote"

    meta = result["meta"]
    assert meta["discovered_raw_count"] == 7
    assert meta["kept_after_basic_filter_count"] == 6
    assert meta["dropped_by_basic_filter_count"] == 1
    assert meta["deduped_count"] == 1
    assert meta["returned_count"] == 5
    assert meta["queries_executed_count"] == 5
    assert meta["empty_queries_count"] == 0
    assert meta["queries_attempted"] == [
        "Software Engineer remote",
        "Backend Software Engineer remote",
        "Backend Engineer remote",
        "Software Engineer New York",
        "Backend Software Engineer New York",
    ]
    assert meta["query_examples"] == [
        "Software Engineer remote",
        "Backend Software Engineer remote",
        "Backend Engineer remote",
        "Software Engineer New York",
        "Backend Software Engineer New York",
    ]
    assert meta["basic_filter_mode"] == "minimal_exclude_only"


def test_collect_board_jobs_stops_after_consecutive_empty_queries(monkeypatch) -> None:
    calls: list[str] = []

    def _fake_collect(
        board: str,
        *,
        query: str,
        location: str,
        max_jobs: int = 25,
        max_pages: int = 1,
        early_stop_when_no_new_results: bool = True,
        url_override: str | None = None,
    ) -> tuple[list[dict], list[str], dict[str, object]]:
        del board, location, max_jobs, max_pages, early_stop_when_no_new_results, url_override
        calls.append(query)
        jobs = []
        if len(calls) == 1:
            jobs = [
                {
                    "source": "linkedin",
                    "title": "Software Engineer",
                    "company": "Acme",
                    "location": "Remote",
                    "url": "https://www.linkedin.com/jobs/view/100",
                    "raw": {"search_url": "https://example.test/search"},
                }
            ]
        return jobs, [], {
            "discovered_raw_count": len(jobs),
            "pages_fetched": 1,
            "pages_with_results": 1 if jobs else 0,
            "stop_reason": "max_pages_reached",
        }

    monkeypatch.setattr(base, "collect_jobs_from_board", _fake_collect)

    result = base.collect_board_jobs(
        "linkedin",
        {
            "titles": ["Software Engineer"],
            "locations": ["Remote"],
            "experience_level": "entry",
            "max_queries_per_title_location_pair": 4,
            "max_queries_per_run": 8,
        },
    )

    assert result["status"] == "success"
    assert len(calls) == 4
    assert result["meta"]["queries_executed_count"] == 4
    assert result["meta"]["empty_queries_count"] == 3
    assert result["meta"]["queries_attempted"] == calls
    assert result["meta"]["jobs_found_per_query"][0]["new_unique_jobs"] == 1
    assert result["meta"]["jobs_found_per_query"][1]["new_unique_jobs"] == 0


def test_collect_jobs_from_board_paginates_until_no_new_results(monkeypatch) -> None:
    requested_urls: list[str] = []

    def _html(ids: list[int]) -> str:
        chunks: list[str] = []
        for job_id in ids:
            chunks.append(
                (
                    f'<a href="/jobs/view/{job_id}">Machine Learning Engineer {job_id}</a>'
                    f"<div>Company: Acme {job_id} Location: Remote Salary: $150000 - $180000</div>"
                )
            )
        return "".join(chunks)

    def _fake_fetch(url: str, **_kwargs: object) -> str:
        requested_urls.append(url)
        if "start=25" in url:
            return _html([2, 3])
        if "start=50" in url:
            return _html([2])
        return _html([1, 2])

    monkeypatch.setattr(job_boards_scrape, "fetch_html", _fake_fetch)

    jobs, warnings, meta = job_boards_scrape.collect_jobs_from_board(
        "linkedin",
        query="machine learning engineer",
        location="Remote",
        max_jobs=10,
        max_pages=3,
        early_stop_when_no_new_results=True,
    )

    assert warnings == []
    assert len(jobs) == 5
    assert meta["discovered_raw_count"] == 5
    assert meta["pages_fetched"] == 3
    assert meta["pages_with_results"] == 3
    assert meta["stop_reason"] == "no_new_results"
    assert requested_urls[0] == "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?keywords=machine+learning+engineer&location=Remote"
    assert "start=25" in requested_urls[1]
    assert "start=50" in requested_urls[2]


def test_collect_jobs_from_board_uses_source_specific_search_urls_and_headers(monkeypatch) -> None:
    requested: list[tuple[str, dict[str, object]]] = []
    html_by_board = {
        "linkedin": '<a href="https://www.linkedin.com/jobs/view/1?position=1&amp;pageNum=0">Software Engineer</a><div>Company: Acme Location: Remote</div>',
        "indeed": '<a href="/viewjob?jk=123">Software Engineer</a><div data-testid="company-name">Acme</div><div data-testid="text-location">Remote</div>',
        "glassdoor": '<a href="/partner/jobListing.htm?pos=101">Software Engineer</a><div>Employer Acme</div><div data-test="location">Remote</div>',
        "handshake": '<a href="/stu/jobs/3001">Software Engineer</a><div class="employer">Acme</div><div class="location">Remote</div>',
    }
    expected_urls = {
        "linkedin": "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?keywords=software+engineer&location=United+States",
        "indeed": "https://www.indeed.com/jobs?q=software+engineer&l=United+States&from=searchOnHP&sort=date",
        "glassdoor": "https://www.glassdoor.com/Job/jobs.htm?sc.keyword=software+engineer&locKeyword=United+States",
        "handshake": "https://app.joinhandshake.com/stu/postings?query=software+engineer",
    }

    def _fake_fetch(url: str, **kwargs: object) -> str:
        requested.append((url, kwargs))
        for board, expected_url in expected_urls.items():
            if url == expected_url:
                return html_by_board[board]
        raise AssertionError(f"unexpected url {url}")

    monkeypatch.setattr(job_boards_scrape, "fetch_html", _fake_fetch)

    for board in ("linkedin", "indeed", "glassdoor", "handshake"):
        jobs, warnings, meta = job_boards_scrape.collect_jobs_from_board(
            board,
            query="software engineer United States",
            location="United States",
            max_jobs=5,
            max_pages=1,
        )

        assert warnings == []
        assert len(jobs) == 1
        assert meta["last_request_url"] == expected_urls[board]
        if board == "linkedin":
            assert jobs[0]["url"] == "https://www.linkedin.com/jobs/view/1?position=1&pageNum=0"

    requested_by_url = {url: kwargs for url, kwargs in requested}
    for board, expected_url in expected_urls.items():
        kwargs = requested_by_url[expected_url]
        headers = kwargs.get("extra_headers")
        assert isinstance(headers, dict)
        assert str(headers["User-Agent"]).startswith("Mozilla/5.0")
        assert headers["Referer"]
        assert float(kwargs["rate_limit_seconds"]) >= 2.0


def test_collect_jobs_from_board_falls_back_from_linkedin_guest_endpoint(monkeypatch) -> None:
    requested_urls: list[str] = []

    def _fake_fetch(url: str, **_kwargs: object) -> str:
        requested_urls.append(url)
        if "jobs-guest/jobs/api/seeMoreJobPostings/search" in url:
            raise HTTPError(url, 999, "Request denied", hdrs=None, fp=None)
        return '<a href="/jobs/view/123">Software Engineer</a><div>Company: Acme Location: Remote</div>'

    monkeypatch.setattr(job_boards_scrape, "fetch_html", _fake_fetch)

    jobs, warnings, meta = job_boards_scrape.collect_jobs_from_board(
        "linkedin",
        query="software engineer United States",
        location="United States",
        max_jobs=5,
        max_pages=1,
    )

    assert warnings == []
    assert len(jobs) == 1
    assert len(requested_urls) == 2
    assert "jobs-guest/jobs/api/seeMoreJobPostings/search" in requested_urls[0]
    assert meta["request_attempts"][0]["error_type"] == "fetch_http_999"
    assert meta["request_attempts"][1]["status"] == "ok"


def test_collect_jobs_from_board_falls_back_to_alternate_indeed_search_url(monkeypatch) -> None:
    requested_urls: list[str] = []

    def _fake_fetch(url: str, **_kwargs: object) -> str:
        requested_urls.append(url)
        if "from=searchOnHP" in url:
            raise HTTPError(url, 403, "Forbidden", hdrs=None, fp=None)
        return '<a href="/viewjob?jk=123">Software Engineer</a><div data-testid="company-name">Acme</div>'

    monkeypatch.setattr(job_boards_scrape, "fetch_html", _fake_fetch)

    jobs, warnings, meta = job_boards_scrape.collect_jobs_from_board(
        "indeed",
        query="software engineer United States",
        location="United States",
        max_jobs=5,
        max_pages=1,
    )

    assert warnings == []
    assert len(jobs) == 1
    assert len(requested_urls) == 2
    assert "from=searchOnHP" in requested_urls[0]
    assert meta["request_attempts"][0]["error_type"] == "fetch_blocked_403"
    assert meta["request_attempts"][1]["status"] == "ok"


def test_collect_jobs_from_board_classifies_source_specific_failures(monkeypatch) -> None:
    def _blocked(url: str, **_kwargs: object) -> str:
        raise HTTPError(url, 403, "Forbidden", hdrs=None, fp=None)

    monkeypatch.setattr(job_boards_scrape, "fetch_html", _blocked)
    _jobs, warnings, meta = job_boards_scrape.collect_jobs_from_board(
        "glassdoor",
        query="software engineer",
        location="United States",
        max_jobs=5,
        max_pages=1,
    )

    assert "fetch_blocked_403" in warnings[0]
    assert meta["error_type"] == "fetch_blocked_403"
    assert meta["error_status"] == 403
    assert meta["request_urls_tried"]

    def _missing(url: str, **_kwargs: object) -> str:
        raise HTTPError(url, 404, "Not Found", hdrs=None, fp=None)

    monkeypatch.setattr(job_boards_scrape, "fetch_html", _missing)
    _jobs, warnings, meta = job_boards_scrape.collect_jobs_from_board(
        "handshake",
        query="software engineer",
        location="United States",
        max_jobs=5,
        max_pages=1,
        url_override="https://joinhandshake.com/students/jobs/search/?query=software+engineer",
    )

    assert "fetch_not_found_404" in warnings[0]
    assert meta["error_type"] == "fetch_not_found_404"
    assert meta["error_status"] == 404
