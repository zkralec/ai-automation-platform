import importlib
import os
import sys
from pathlib import Path

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "worker"))

from integrations import job_boards_scrape

FIXTURE_DIR = Path(__file__).parent / "fixtures"


def _fixture(name: str) -> str:
    return (FIXTURE_DIR / name).read_text(encoding="utf-8")


@pytest.mark.parametrize(
    ("source", "fixture_name", "expected_company", "expected_location", "expected_posted_at", "expected_salary_min", "expected_salary_max"),
    [
        ("linkedin", "jobs_linkedin_messy.html", "Acme Labs", "Remote - New York, NY", "2026-03-17T12:00:00Z", 180000.0, 220000.0),
        ("indeed", "jobs_indeed_messy.html", "Beta Corp", "Austin, TX", "Posted 2 days ago", 150000.0, 170000.0),
        ("glassdoor", "jobs_glassdoor_messy.html", "Gamma Inc", "San Francisco, CA", "5d", None, None),
        ("handshake", "jobs_handshake_messy.html", "Delta University", "Chicago, IL", "Posted 7 days ago", None, None),
    ],
)
def test_source_collectors_extract_useful_metadata_from_messy_html(
    monkeypatch,
    source: str,
    fixture_name: str,
    expected_company: str,
    expected_location: str,
    expected_posted_at: str,
    expected_salary_min: float | None,
    expected_salary_max: float | None,
) -> None:
    fixture_html = _fixture(fixture_name)
    monkeypatch.setattr(job_boards_scrape, "fetch_html", lambda url, **_kwargs: fixture_html)

    module = importlib.import_module(f"integrations.jobs_collectors.{source}")
    result = module.collect_jobs(
        {
            "query": "senior software engineer",
            "location": "United States",
            "result_limit_per_source": 5,
            "max_pages_per_source": 1,
        }
    )

    assert result["status"] == "success"
    assert len(result["jobs"]) == 1

    job = result["jobs"][0]
    assert job["title"] == "Senior Software Engineer"
    assert job["company"] == expected_company
    assert job["location"] == expected_location
    assert job["posted_at"] == expected_posted_at
    assert job["salary_min"] == expected_salary_min
    assert job["salary_max"] == expected_salary_max
    assert job["url"] == job["source_url"]
    assert job["source_metadata"]["search_url"]
    assert job["source_metadata"]["job_url"] == job["url"]
    assert job["description_snippet"]
    assert job["metadata_diagnostics"] == {
        "missing_company": False,
        "missing_posted_at": False,
        "missing_source_url": False,
        "missing_location": False,
    }

    summary = result["meta"]["metadata_completeness_summary"]
    assert summary["job_count"] == 1
    assert summary["missing_company"] == 0
    assert summary["missing_posted_at"] == 0
    assert summary["missing_source_url"] == 0
    assert summary["missing_location"] == 0


def test_source_collectors_report_missing_metadata_honestly(monkeypatch) -> None:
    html = """
    <div class="job_seen_beacon">
      <a href="/viewjob?jk=missing123">Staff Backend Engineer</a>
      <div data-testid="text-location">Remote</div>
      <div class="job-snippet">Platform role with service ownership.</div>
    </div>
    """
    monkeypatch.setattr(job_boards_scrape, "fetch_html", lambda url, **_kwargs: html)

    module = importlib.import_module("integrations.jobs_collectors.indeed")
    result = module.collect_jobs(
        {
            "query": "backend engineer",
            "location": "United States",
            "result_limit_per_source": 5,
        }
    )

    job = result["jobs"][0]
    assert job["company"] is None
    assert job["posted_at"] is None
    assert job["location"] == "Remote"
    assert job["source_url"] == job["url"]
    assert job["metadata_diagnostics"]["missing_company"] is True
    assert job["metadata_diagnostics"]["missing_posted_at"] is True
    assert job["metadata_diagnostics"]["missing_source_url"] is False
    assert job["metadata_diagnostics"]["missing_location"] is False

    summary = result["meta"]["metadata_completeness_summary"]
    assert summary["job_count"] == 1
    assert summary["missing_company"] == 1
    assert summary["missing_posted_at"] == 1
    assert summary["missing_source_url"] == 0
    assert summary["missing_location"] == 0
