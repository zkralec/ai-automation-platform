import json
import os
import sys
from types import SimpleNamespace

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "worker"))

from task_handlers import jobs_collect_v1
from task_handlers.jobs_pipeline_common import resolve_request


def _task(payload: dict, *, task_id: str = "task-collect-1", run_id: str = "run-collect-1") -> SimpleNamespace:
    return SimpleNamespace(
        id=task_id,
        max_attempts=3,
        _run_id=run_id,
        payload_json=json.dumps(payload, separators=(",", ":"), ensure_ascii=True),
    )


class _FailCollector:
    SUPPORTED_FIELDS = {"source": "linkedin"}

    @staticmethod
    def collect_jobs(request: dict, *, url_override: str | None = None) -> dict:
        del request, url_override
        return {
            "status": "failed",
            "jobs": [],
            "warnings": [],
            "errors": ["fetch_failed"],
            "meta": {"requested_limit": 5},
        }


class _SuccessCollector:
    SUPPORTED_FIELDS = {"source": "indeed"}

    @staticmethod
    def collect_jobs(request: dict, *, url_override: str | None = None) -> dict:
        del request
        return {
            "status": "success",
            "jobs": [
                {
                    "source": "indeed",
                    "source_url": url_override or "https://www.indeed.com/jobs?q=ml+engineer",
                    "title": "Machine Learning Engineer",
                    "company": "Acme",
                    "location": "Remote",
                    "url": "https://www.indeed.com/viewjob?jk=123",
                    "source_metadata": {"search_url": "https://www.indeed.com/jobs?q=ml+engineer"},
                }
            ],
            "warnings": [],
            "errors": [],
            "meta": {"requested_limit": 5, "returned_count": 1},
        }


def test_resolve_request_preserves_structured_collect_inputs() -> None:
    request = resolve_request(
        {
            "titles": ["ML Engineer"],
            "keywords": ["python", "llm"],
            "excluded_keywords": ["senior"],
            "locations": ["Remote", "New York, NY"],
            "work_mode_preference": ["remote", "hybrid"],
            "minimum_salary": 150000,
            "experience_level": "entry-level",
            "result_limit_per_source": 450,
            "max_pages_per_source": 7,
            "max_queries_per_title_location_pair": 6,
            "early_stop_when_no_new_results": False,
            "enabled_sources": ["linkedin", "indeed", "glassdoor", "handshake"],
            "shortlist_count": 6,
            "shortlist_freshness_preference": "strong-prefer-recent",
        }
    )

    assert request["titles"] == ["ML Engineer"]
    assert request["keywords"] == ["python", "llm"]
    assert request["excluded_keywords"] == ["senior"]
    assert request["locations"] == ["Remote", "New York, NY"]
    assert request["work_mode_preference"] == ["remote", "hybrid"]
    assert request["minimum_salary"] == 150000.0
    assert request["experience_level"] == "entry"
    assert request["result_limit_per_source"] == 450
    assert request["max_jobs_per_source"] == 450
    assert request["max_jobs_per_board"] == 450
    assert request["max_pages_per_source"] == 7
    assert request["max_queries_per_title_location_pair"] == 6
    assert request["early_stop_when_no_new_results"] is False
    assert request["sources"] == ["linkedin", "indeed", "glassdoor", "handshake"]
    assert request["enabled_sources"] == ["linkedin", "indeed", "glassdoor", "handshake"]
    assert request["shortlist_max_items"] == 6
    assert request["shortlist_freshness_preference"] == "strong_prefer_recent"
    assert request["shortlist_freshness_weight_enabled"] is True
    assert request["shortlist_freshness_max_bonus"] == 12.0


def test_jobs_collect_v1_reports_partial_success_when_one_source_fails(monkeypatch) -> None:
    def _fake_load(source: str):
        if source == "linkedin":
            return _FailCollector
        if source == "indeed":
            return _SuccessCollector
        raise AssertionError(f"unexpected source requested: {source}")

    monkeypatch.setattr(jobs_collect_v1, "_load_collector_module", _fake_load)

    payload = {
        "pipeline_id": "pipe-partial",
        "request": {
            "collectors_enabled": True,
            "sources": ["linkedin", "indeed"],
            "titles": ["Machine Learning Engineer"],
            "locations": ["Remote"],
            "result_limit_per_source": 5,
        },
    }
    result = jobs_collect_v1.execute(_task(payload), db=None)
    artifact = result["content_json"]

    assert result["artifact_type"] == "jobs.collect.v1"
    assert artifact["artifact_type"] == "jobs.collect.v1"
    assert artifact["artifact_schema"] == "jobs_raw.v1"
    assert artifact["partial_success"] is True
    assert artifact["source_results"]["linkedin"]["status"] == "failed"
    assert artifact["source_results"]["indeed"]["status"] == "success"
    assert artifact["failed_sources"] == ["linkedin"]
    assert artifact["successful_sources"] == ["indeed"]
    assert len(artifact["raw_jobs"]) == 1
    assert artifact["raw_jobs"][0]["source"] == "indeed"
    assert artifact["raw_jobs"][0]["source_url"] == "https://www.indeed.com/jobs?q=ml+engineer"


def test_jobs_collect_v1_raises_when_all_enabled_sources_fail(monkeypatch) -> None:
    monkeypatch.setattr(jobs_collect_v1, "_load_collector_module", lambda source: _FailCollector)

    payload = {
        "pipeline_id": "pipe-fail",
        "request": {
            "collectors_enabled": True,
            "sources": ["linkedin", "indeed"],
            "titles": ["Machine Learning Engineer"],
            "result_limit_per_source": 5,
        },
    }

    with pytest.raises(RuntimeError, match="all enabled sources failed"):
        jobs_collect_v1.execute(_task(payload), db=None)


def test_jobs_collect_v1_success_multisource_fixture(monkeypatch, jobs_v2_samples) -> None:
    source_jobs = jobs_v2_samples["collect_multisource_by_source"]

    def _collector_for(source: str):
        jobs = [dict(row) for row in source_jobs[source]]

        class _Collector:
            SUPPORTED_FIELDS = {
                "source": source,
                "titles": True,
                "keywords": True,
                "excluded_keywords": True,
                "locations": True,
                "work_mode_preference": True,
                "minimum_salary": True,
                "experience_level": True,
                "result_limit_per_source": True,
                "enabled_sources": True,
                "source_metadata_fields": ["source_metadata"],
            }

            @staticmethod
            def collect_jobs(request: dict, *, url_override: str | None = None) -> dict:
                output: list[dict] = []
                for row in jobs:
                    item = dict(row)
                    if url_override:
                        item["source_url"] = url_override
                    output.append(item)
                return {
                    "status": "success",
                    "jobs": output,
                    "warnings": [],
                    "errors": [],
                    "meta": {
                        "requested_limit": int(request.get("result_limit_per_source") or 25),
                        "returned_count": len(output),
                    },
                }

        return _Collector

    def _fake_load(source: str):
        return _collector_for(source)

    monkeypatch.setattr(jobs_collect_v1, "_load_collector_module", _fake_load)

    payload = {
        "pipeline_id": "pipe-success-all",
        "request": {
            "collectors_enabled": True,
            "sources": ["linkedin", "indeed", "glassdoor", "handshake"],
            "titles": ["Machine Learning Engineer"],
            "locations": ["Remote", "New York, NY"],
            "result_limit_per_source": 10,
        },
    }
    result = jobs_collect_v1.execute(_task(payload), db=None)
    artifact = result["content_json"]

    expected_count = sum(len(rows) for rows in source_jobs.values())
    assert artifact["artifact_type"] == "jobs.collect.v1"
    assert artifact["artifact_schema"] == "jobs_raw.v1"
    assert artifact["collection_status"] == "success"
    assert artifact["partial_success"] is False
    assert len(artifact["raw_jobs"]) == expected_count
    assert artifact["collection_summary"]["discovered_raw_count"] == expected_count
    assert artifact["collection_summary"]["kept_after_basic_filter_count"] == expected_count
    assert artifact["collection_summary"]["dropped_by_basic_filter_count"] == 0
    assert artifact["collection_summary"]["deduped_count"] == 0
    assert sorted(artifact["successful_sources"]) == ["glassdoor", "handshake", "indeed", "linkedin"]
    assert artifact["failed_sources"] == []
    assert artifact["collector_errors"] == []
    assert result["next_tasks"][0]["task_type"] == "jobs_normalize_v1"
    assert all(str(job.get("source", "")).strip() for job in artifact["raw_jobs"])
    assert all(str(job.get("source_url", "")).strip() for job in artifact["raw_jobs"])
    for source in ["linkedin", "indeed", "glassdoor", "handshake"]:
        assert artifact["source_results"][source]["status"] == "success"
        assert source in artifact["supported_fields_by_source"]


def test_jobs_collect_v1_empty_success_when_sources_return_no_jobs(monkeypatch) -> None:
    class _EmptyCollector:
        SUPPORTED_FIELDS = {"source": "empty"}

        @staticmethod
        def collect_jobs(request: dict, *, url_override: str | None = None) -> dict:
            del request, url_override
            return {"status": "success", "jobs": [], "warnings": [], "errors": [], "meta": {"returned_count": 0}}

    monkeypatch.setattr(jobs_collect_v1, "_load_collector_module", lambda source: _EmptyCollector)

    payload = {
        "pipeline_id": "pipe-empty-success",
        "request": {
            "collectors_enabled": True,
            "sources": ["linkedin", "indeed"],
            "titles": ["ML Engineer"],
            "result_limit_per_source": 5,
        },
    }
    result = jobs_collect_v1.execute(_task(payload), db=None)
    artifact = result["content_json"]

    assert artifact["collection_status"] == "success"
    assert artifact["partial_success"] is False
    assert artifact["raw_jobs"] == []
    assert sorted(artifact["successful_sources"]) == ["indeed", "linkedin"]
    assert artifact["failed_sources"] == []
    assert artifact["collection_summary"]["raw_job_count"] == 0
    assert artifact["collection_summary"]["discovered_raw_count"] == 0
    assert artifact["collection_summary"]["kept_after_basic_filter_count"] == 0
    assert artifact["collection_summary"]["dropped_by_basic_filter_count"] == 0
    assert artifact["collection_summary"]["deduped_count"] == 0
    assert result["next_tasks"][0]["task_type"] == "jobs_normalize_v1"
