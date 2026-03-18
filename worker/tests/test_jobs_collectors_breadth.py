import os
import sys

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
        if query == "Machine Learning Engineer":
            jobs = [
                _job("Platform Engineer", "https://www.linkedin.com/jobs/view/1", description="distributed systems"),
                _job("Machine Learning Engineer", "https://www.linkedin.com/jobs/view/2", description="python systems"),
                _job(
                    "Contract Machine Learning Engineer",
                    "https://www.linkedin.com/jobs/view/3",
                    description="contract role",
                ),
            ]
        elif query == "Machine Learning Engineer python":
            jobs = [
                _job("Machine Learning Engineer", "https://www.linkedin.com/jobs/view/2", description="python systems"),
                _job("Data Engineer", "https://www.linkedin.com/jobs/view/4", description="warehouse pipelines"),
                _job("Backend Engineer", "https://www.linkedin.com/jobs/view/5", description="api services"),
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
            "titles": ["Machine Learning Engineer"],
            "keywords": ["python", "llm"],
            "excluded_keywords": ["contract"],
            "locations": ["Remote"],
            "result_limit_per_source": 3,
            "max_pages_per_source": 4,
            "max_queries_per_title_location_pair": 2,
            "early_stop_when_no_new_results": False,
        },
    )

    assert result["status"] == "success"
    assert [query for query, _location, _pages, _early_stop in calls] == [
        "Machine Learning Engineer",
        "Machine Learning Engineer python",
    ]
    assert all(max_pages == 4 for _query, _location, max_pages, _early_stop in calls)
    assert all(early_stop is False for _query, _location, _pages, early_stop in calls)

    jobs = result["jobs"]
    assert len(jobs) == 3
    assert any(job["title"] == "Platform Engineer" for job in jobs)
    assert any(job["title"] == "Data Engineer" for job in jobs)
    assert all(job["title"] != "Contract Machine Learning Engineer" for job in jobs)

    meta = result["meta"]
    assert meta["discovered_raw_count"] == 6
    assert meta["kept_after_basic_filter_count"] == 4
    assert meta["dropped_by_basic_filter_count"] == 1
    assert meta["deduped_count"] == 1
    assert meta["returned_count"] == 3
    assert meta["queries_attempted"] == ["Machine Learning Engineer", "Machine Learning Engineer python"]
    assert meta["basic_filter_mode"] == "minimal_exclude_only"


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
    assert requested_urls[0] == "https://www.linkedin.com/jobs/search/?keywords=machine+learning+engineer&location=Remote"
    assert "start=25" in requested_urls[1]
    assert "start=50" in requested_urls[2]
