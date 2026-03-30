import json
import logging
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


class _BlockedCollector:
    SUPPORTED_FIELDS = {"source": "glassdoor"}

    @staticmethod
    def collect_jobs(request: dict, *, url_override: str | None = None) -> dict:
        del request, url_override
        return {
            "status": "failed",
            "jobs": [],
            "warnings": [],
            "errors": [
                "glassdoor: fetch_blocked_403 url=https://www.glassdoor.com/Job/jobs.htm?sc.keyword=ml+engineer error=HTTPError: Forbidden"
            ],
            "meta": {
                "queries_attempted": ["ml engineer"],
                "queries_executed_count": 1,
                "pages_fetched": 1,
                "pages_attempted": 1,
                "search_attempts": [
                    {
                        "query": "ml engineer",
                        "location": "Remote",
                        "query_index": 1,
                        "pages_fetched": 1,
                        "pages_attempted": 1,
                        "pages_with_results": 0,
                        "jobs_found": 0,
                        "new_unique_jobs": 0,
                        "returned_count": 0,
                        "request_urls_tried": ["https://www.glassdoor.com/Job/jobs.htm?sc.keyword=ml+engineer"],
                        "last_request_url": "https://www.glassdoor.com/Job/jobs.htm?sc.keyword=ml+engineer",
                        "error_type": "fetch_blocked_403",
                        "error_status": 403,
                        "stop_reason": "fetch_blocked_403",
                    }
                ],
                "request_urls_tried": ["https://www.glassdoor.com/Job/jobs.htm?sc.keyword=ml+engineer"],
                "last_request_url": "https://www.glassdoor.com/Job/jobs.htm?sc.keyword=ml+engineer",
                "error_type": "fetch_blocked_403",
                "error_status": 403,
            },
        }


def _single_job(source: str) -> dict:
    return {
        "source": source,
        "source_url": f"https://example.test/{source}/1",
        "title": f"{source.title()} Software Engineer",
        "company": "Acme",
        "location": "Remote",
        "url": f"https://example.test/{source}/1",
        "source_metadata": {"search_url": f"https://example.test/{source}/search"},
    }


def test_resolve_request_preserves_structured_collect_inputs() -> None:
    request = resolve_request(
        {
            "search_mode": "precision_match",
            "titles": ["ML Engineer"],
            "keywords": ["python", "llm"],
            "excluded_keywords": ["senior"],
            "locations": ["Remote", "New York, NY"],
            "work_mode_preference": ["remote", "hybrid"],
            "minimum_salary": 150000,
            "experience_level": "entry-level",
            "result_limit_per_source": 450,
            "max_total_jobs": 1200,
            "max_pages_per_source": 7,
            "max_queries_per_title_location_pair": 6,
            "max_queries_per_run": 14,
            "enable_query_expansion": False,
            "jobs_notification_cooldown_days": 5,
            "jobs_shortlist_repeat_penalty": 6,
            "resurface_seen_jobs": True,
            "early_stop_when_no_new_results": False,
            "enabled_sources": ["linkedin", "indeed", "glassdoor", "handshake"],
            "shortlist_count": 6,
            "shortlist_freshness_preference": "strong-prefer-recent",
        }
    )

    assert request["search_mode"] == "precision_match"
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
    assert request["max_total_jobs"] == 1200
    assert request["max_pages_per_source"] == 7
    assert request["max_queries_per_title_location_pair"] == 6
    assert request["max_queries_per_run"] == 14
    assert request["enable_query_expansion"] is False
    assert request["jobs_notification_cooldown_days"] == 5
    assert request["jobs_shortlist_repeat_penalty"] == 6.0
    assert request["resurface_seen_jobs"] is True
    assert request["early_stop_when_no_new_results"] is False
    assert request["sources"] == ["linkedin", "indeed"]
    assert request["enabled_sources"] == ["linkedin", "indeed"]
    assert request["disabled_sources"] == ["glassdoor", "handshake"]
    assert request["source_configuration_notes"]
    assert request["shortlist_max_items"] == 6
    assert request["shortlist_freshness_preference"] == "strong_prefer_recent"
    assert request["shortlist_freshness_weight_enabled"] is True
    assert request["shortlist_freshness_max_bonus"] == 12.0


def test_resolve_request_applies_broad_discovery_defaults() -> None:
    request = resolve_request(
        {
            "query": "software engineer",
            "location": "United States",
        }
    )

    assert request["search_mode"] == "broad_discovery"
    assert request["max_queries_per_run"] == 14
    assert request["enable_query_expansion"] is True
    assert request["shortlist_min_score"] == 0.5
    assert request["shortlist_fail_soft_enabled"] is True
    assert request["shortlist_fallback_min_items"] == 5
    assert request["jobs_notification_cooldown_days"] == 3
    assert request["minimum_raw_jobs_total"] == 0
    assert request["minimum_unique_jobs_total"] == 0
    assert request["minimum_jobs_per_source"] == 0
    assert request["stop_when_minimum_reached"] is True
    assert request["collection_time_cap_seconds"] is None


def test_resolve_request_applies_precision_match_defaults() -> None:
    request = resolve_request(
        {
            "desired_title": "Machine Learning Engineer",
            "keywords": ["python", "llm"],
            "work_mode_preference": ["remote"],
        }
    )

    assert request["search_mode"] == "precision_match"
    assert request["max_queries_per_run"] == 8
    assert request["enable_query_expansion"] is False
    assert request["shortlist_min_score"] == 0.85
    assert request["shortlist_fail_soft_enabled"] is False
    assert request["shortlist_fallback_min_items"] == 0
    assert request["jobs_notification_cooldown_days"] == 7
    assert request["require_keyword_match"] is True
    assert request["require_work_mode_match"] is True


def test_resolve_request_raises_max_total_jobs_to_honor_minimums() -> None:
    request = resolve_request(
        {
            "sources": ["linkedin", "indeed"],
            "result_limit_per_source": 10,
            "max_total_jobs": 8,
            "minimum_unique_jobs_total": 25,
            "minimum_jobs_per_source": 15,
        }
    )

    assert request["minimum_unique_jobs_total"] == 25
    assert request["minimum_jobs_per_source"] == 15
    assert request["max_total_jobs"] == 30
    assert any("raised to honor the configured minimum jobs target" in note for note in request["source_configuration_notes"])


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
    assert artifact["source_results"]["linkedin"]["status"] == "upstream_failure"
    assert artifact["source_results"]["indeed"]["status"] == "success"
    assert artifact["source_results"]["linkedin"]["source"] == "linkedin"
    assert artifact["source_results"]["indeed"]["source"] == "indeed"
    assert artifact["failed_sources"] == ["linkedin"]
    assert artifact["successful_sources"] == ["indeed"]
    assert artifact["healthy_sources"] == ["indeed"]
    assert len(artifact["raw_jobs"]) == 1
    assert artifact["raw_jobs"][0]["source"] == "indeed"
    assert artifact["raw_jobs"][0]["source_url"] == "https://www.indeed.com/jobs?q=ml+engineer"
    debug_payload = result["debug_json"]
    assert debug_payload["artifact_type"] == "debug.json"
    assert debug_payload["sources_attempted"] == ["linkedin", "indeed"]
    assert debug_payload["sources_succeeded"] == ["indeed"]
    assert debug_payload["sources_healthy"] == ["indeed"]
    assert debug_payload["sources_failed"] == ["linkedin"]
    assert debug_payload["per_source_job_counts"] == {"linkedin": 0, "indeed": 1}
    assert debug_payload["per_source_status"]["linkedin"]["status"] == "upstream_failure"
    assert debug_payload["per_source_status"]["linkedin"]["jobs_count"] == 0
    assert debug_payload["per_source_status"]["indeed"]["status"] == "success"
    assert debug_payload["per_source_status"]["indeed"]["jobs_count"] == 1


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
            "sources": ["linkedin", "indeed"],
            "titles": ["Machine Learning Engineer"],
            "locations": ["Remote", "New York, NY"],
            "result_limit_per_source": 10,
        },
    }
    result = jobs_collect_v1.execute(_task(payload), db=None)
    artifact = result["content_json"]

    expected_count = len(source_jobs["linkedin"]) + len(source_jobs["indeed"])
    assert artifact["artifact_type"] == "jobs.collect.v1"
    assert artifact["artifact_schema"] == "jobs_raw.v1"
    assert artifact["collection_status"] == "success"
    assert artifact["partial_success"] is False
    assert len(artifact["raw_jobs"]) == expected_count
    assert artifact["collection_summary"]["discovered_raw_count"] == expected_count
    assert artifact["collection_summary"]["kept_after_basic_filter_count"] == expected_count
    assert artifact["collection_summary"]["dropped_by_basic_filter_count"] == 0
    assert artifact["collection_summary"]["deduped_count"] == 0


def test_jobs_collect_v1_surfaces_disabled_source_observability(monkeypatch) -> None:
    def _fake_load(source: str):
        if source == "linkedin":
            return _SuccessCollector
        raise AssertionError(f"unexpected source requested: {source}")

    monkeypatch.setattr(jobs_collect_v1, "_load_collector_module", _fake_load)

    payload = {
        "pipeline_id": "pipe-observability",
        "request": {
            "collectors_enabled": True,
            "sources": ["linkedin", "glassdoor"],
            "titles": ["Machine Learning Engineer"],
            "locations": ["Remote"],
            "result_limit_per_source": 5,
        },
    }
    result = jobs_collect_v1.execute(_task(payload), db=None)
    artifact = result["content_json"]
    source_view = artifact["collection_observability"]["by_source"]["glassdoor"]

    assert artifact["partial_success"] is False
    assert artifact["successful_sources"] == ["linkedin"]
    assert artifact["healthy_sources"] == ["linkedin"]
    assert artifact["failed_sources"] == []
    assert artifact["skipped_sources"] == ["glassdoor"]
    assert any("inactive legacy job sources were ignored" in warning.lower() for warning in artifact["warnings"])
    assert result["next_tasks"][0]["task_type"] == "jobs_normalize_v1"
    assert all(str(job.get("source", "")).strip() for job in artifact["raw_jobs"])
    assert all(str(job.get("source_url", "")).strip() for job in artifact["raw_jobs"])
    assert artifact["source_results"]["linkedin"]["status"] == "success"
    assert artifact["source_results"]["glassdoor"]["status"] == "skipped"
    assert source_view["source_status"] == "skipped"
    assert source_view["source_error_type"] == "source_disabled"
    assert source_view["queries_executed_count"] == 0
    assert "linkedin" in artifact["supported_fields_by_source"]
    assert "glassdoor" not in artifact["supported_fields_by_source"]


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
    assert artifact["successful_sources"] == []
    assert sorted(artifact["healthy_sources"]) == ["indeed", "linkedin"]
    assert sorted(artifact["empty_sources"]) == ["indeed", "linkedin"]
    assert artifact["failed_sources"] == []
    assert artifact["collection_summary"]["healthy_source_count"] == 2
    assert artifact["collection_summary"]["empty_source_count"] == 2
    assert artifact["collection_summary"]["raw_job_count"] == 0
    assert artifact["collection_summary"]["discovered_raw_count"] == 0
    assert artifact["collection_summary"]["kept_after_basic_filter_count"] == 0
    assert artifact["collection_summary"]["dropped_by_basic_filter_count"] == 0
    assert artifact["collection_summary"]["deduped_count"] == 0
    assert result["next_tasks"][0]["task_type"] == "jobs_normalize_v1"
    assert result["debug_json"]["artifact_type"] == "debug.json"
    assert result["debug_json"]["per_source_job_counts"] == {"linkedin": 0, "indeed": 0}
    assert result["debug_json"]["sources_succeeded"] == []
    assert sorted(result["debug_json"]["sources_healthy"]) == ["indeed", "linkedin"]
    assert sorted(result["debug_json"]["sources_empty"]) == ["indeed", "linkedin"]
    assert result["debug_json"]["sources_failed"] == []


@pytest.mark.parametrize(
    ("status", "source_error_type", "jobs", "expect_successful", "expect_healthy", "expect_under_target", "expect_failed"),
    [
        ("success", None, [_single_job("linkedin")], True, True, False, False),
        ("empty_success", "empty_results", [], False, True, False, False),
        ("under_target", "below_requested_limit", [_single_job("linkedin")], True, False, True, False),
        ("auth_blocked", "login_wall", [], False, False, False, True),
        ("consent_blocked", "consent_wall_detected", [], False, False, False, True),
        ("anti_bot_blocked", "anti_bot_detected", [], False, False, False, True),
        ("layout_mismatch", "layout_mismatch", [], False, False, False, True),
        ("upstream_failure", "fetch_blocked_403", [], False, False, False, True),
    ],
)
def test_jobs_collect_v1_classifies_truthful_source_health(
    monkeypatch,
    status: str,
    source_error_type: str | None,
    jobs: list[dict],
    expect_successful: bool,
    expect_healthy: bool,
    expect_under_target: bool,
    expect_failed: bool,
) -> None:
    class _Collector:
        SUPPORTED_FIELDS = {"source": "linkedin"}

        @staticmethod
        def collect_jobs(request: dict, *, url_override: str | None = None) -> dict:
            del request, url_override
            meta = {
                "requested_limit": 5,
                "returned_count": len(jobs),
                "source_status": status,
                "source_error_type": source_error_type,
            }
            if source_error_type:
                meta["error_type"] = source_error_type
            return {
                "status": status,
                "jobs": [dict(row) for row in jobs],
                "warnings": [],
                "errors": [f"linkedin: {status} error_type={source_error_type}"] if not jobs and source_error_type else [],
                "meta": meta,
            }

    monkeypatch.setattr(jobs_collect_v1, "_load_collector_module", lambda _source: _Collector)

    payload = {
        "pipeline_id": f"pipe-{status}",
        "request": {
            "collectors_enabled": True,
            "sources": ["linkedin", "manual"],
            "titles": ["Software Engineer"],
            "locations": ["Remote"],
            "result_limit_per_source": 5,
            "manual_jobs": [_single_job("manual")],
        },
    }

    result = jobs_collect_v1.execute(_task(payload), db=None)
    artifact = result["content_json"]
    debug_payload = result["debug_json"]

    assert artifact["source_results"]["linkedin"]["status"] == status
    assert ("linkedin" in artifact["successful_sources"]) is expect_successful
    assert ("linkedin" in artifact["healthy_sources"]) is expect_healthy
    assert ("linkedin" in artifact["under_target_sources"]) is expect_under_target
    assert ("linkedin" in artifact["failed_sources"]) is expect_failed
    assert ("linkedin" in debug_payload["sources_succeeded"]) is expect_successful
    assert ("linkedin" in debug_payload["sources_healthy"]) is expect_healthy
    assert ("linkedin" in debug_payload["sources_under_target"]) is expect_under_target
    assert ("linkedin" in debug_payload["sources_failed"]) is expect_failed


def test_jobs_collect_v1_surfaces_source_metadata_quality(monkeypatch) -> None:
    class _MetadataCollector:
        SUPPORTED_FIELDS = {"source": "indeed"}

        @staticmethod
        def collect_jobs(request: dict, *, url_override: str | None = None) -> dict:
            del request, url_override
            return {
                "status": "success",
                "jobs": [
                    {
                        "source": "indeed",
                        "source_url": "https://www.indeed.com/viewjob?jk=123",
                        "title": "Senior Software Engineer",
                        "company": None,
                        "location": "Remote",
                        "url": "https://www.indeed.com/viewjob?jk=123",
                        "metadata_diagnostics": {
                            "missing_company": True,
                            "missing_posted_at": True,
                            "missing_source_url": False,
                            "missing_location": False,
                        },
                        "source_metadata": {"search_url": "https://www.indeed.com/jobs?q=senior+software+engineer"},
                    }
                ],
                "warnings": [],
                "errors": [],
                "meta": {
                    "returned_count": 1,
                    "metadata_completeness_summary": {
                        "job_count": 1,
                        "missing_company": 1,
                        "missing_posted_at": 1,
                        "missing_source_url": 0,
                        "missing_location": 0,
                    },
                },
            }

    monkeypatch.setattr(jobs_collect_v1, "_load_collector_module", lambda source: _MetadataCollector)

    payload = {
        "pipeline_id": "pipe-metadata-quality",
        "request": {
            "collectors_enabled": True,
            "sources": ["indeed"],
            "titles": ["Senior Software Engineer"],
            "result_limit_per_source": 5,
        },
    }
    result = jobs_collect_v1.execute(_task(payload), db=None)
    artifact = result["content_json"]

    assert artifact["source_metadata_quality"]["indeed"]["missing_company"] == 1
    assert artifact["source_metadata_quality"]["indeed"]["missing_posted_at"] == 1
    assert artifact["metadata_completeness_summary"]["missing_company"] == 1
    assert artifact["collection_summary"]["missing_company"] == 1
    assert artifact["collection_summary"]["missing_posted_at"] == 1
    observability = artifact["collection_observability"]
    assert observability["waterfall"]["raw_jobs_discovered"] == 1
    assert observability["by_source"]["indeed"]["raw_jobs_discovered"] == 1
    assert observability["by_source"]["indeed"]["kept_after_basic_filter"] == 1
    assert observability["by_source"]["indeed"]["jobs_dropped"] == 0
    assert observability["by_source"]["indeed"]["missing_rates"]["missing_company_rate"] == 100.0
    assert observability["active_sources_label"] == "Indeed active"
    assert observability["by_source"]["indeed"]["source_label"] == "Indeed"
    assert observability["operator_questions"]["which_source_is_weak"] == "Lowest raw contribution came from Indeed."
    assert observability["operator_questions"]["are_we_missing_metadata"] == "Weakest metadata source: Indeed."
    assert "Indeed contributed 1 raw jobs" in observability["run_preview"]["messages"]


def test_jobs_collect_v1_aggregates_query_observability_and_run_cap(monkeypatch) -> None:
    requested_limits: dict[str, int] = {}

    def _collector_for(source: str):
        class _Collector:
            SUPPORTED_FIELDS = {"source": source}

            @staticmethod
            def collect_jobs(request: dict, *, url_override: str | None = None) -> dict:
                del url_override
                requested_limits[source] = int(request.get("result_limit_per_source") or 0)
                limit = requested_limits[source]
                jobs = [
                    {
                        "source": source,
                        "source_url": f"https://example.test/{source}/{index}",
                        "title": f"Software Engineer {source} {index}",
                        "company": f"{source.title()} Corp",
                        "location": "Remote",
                        "url": f"https://example.test/{source}/{index}",
                        "source_metadata": {"search_url": f"https://example.test/{source}/search"},
                    }
                    for index in range(limit)
                ]
                return {
                    "status": "success",
                    "jobs": jobs,
                    "warnings": [],
                    "errors": [],
                    "meta": {
                        "requested_limit": limit,
                        "returned_count": len(jobs),
                        "discovered_raw_count": len(jobs),
                        "kept_after_basic_filter_count": len(jobs),
                        "dropped_by_basic_filter_count": 0,
                        "deduped_count": 0,
                        "queries_attempted": [f"{source} base", f"{source} expansion"],
                        "queries_executed_count": 2,
                        "empty_queries_count": 1 if source == "indeed" else 0,
                        "query_examples": [f"{source} base", f"{source} expansion"],
                        "search_attempts": [
                            {
                                "query": f"{source} base",
                                "location": "Remote",
                                "expansion_type": "base_title",
                                "jobs_found": max(limit - 1, 0),
                                "new_unique_jobs": max(limit - 1, 0),
                                "returned_count": max(limit - 1, 0),
                                "stop_reason": "max_pages_reached",
                            },
                            {
                                "query": f"{source} expansion",
                                "location": "Remote",
                                "expansion_type": "title_synonym",
                                "jobs_found": 1 if limit else 0,
                                "new_unique_jobs": 1 if limit else 0,
                                "returned_count": 1 if limit else 0,
                                "stop_reason": "max_pages_reached",
                            },
                        ],
                    },
                }

        return _Collector

    monkeypatch.setattr(jobs_collect_v1, "_load_collector_module", _collector_for)

    payload = {
        "pipeline_id": "pipe-query-observability",
        "request": {
            "collectors_enabled": True,
            "sources": ["linkedin", "indeed", "glassdoor"],
            "titles": ["Software Engineer"],
            "locations": ["Remote"],
            "result_limit_per_source": 3,
            "max_total_jobs": 4,
            "max_queries_per_run": 6,
            "enable_query_expansion": True,
        },
    }
    result = jobs_collect_v1.execute(_task(payload), db=None)
    artifact = result["content_json"]

    assert requested_limits == {"linkedin": 3, "indeed": 1}
    assert artifact["skipped_sources"] == ["glassdoor"]
    assert len(artifact["raw_jobs"]) == 4
    assert artifact["collection_summary"]["max_total_jobs"] == 4
    assert artifact["collection_summary"]["truncated_by_run_limit_count"] == 0
    assert artifact["collection_summary"]["queries_executed_count"] == 4
    assert artifact["collection_summary"]["empty_queries_count"] == 1
    assert artifact["collection_summary"]["query_examples"] == [
        "linkedin base",
        "linkedin expansion",
        "indeed base",
        "indeed expansion",
    ]
    observability = artifact["collection_observability"]
    assert observability["query_summary"]["queries_executed"] == 4
    assert observability["query_summary"]["empty_queries_count"] == 1
    assert observability["query_summary"]["max_total_jobs"] == 4
    assert observability["query_summary"]["query_examples"] == [
        "linkedin base",
        "linkedin expansion",
        "indeed base",
        "indeed expansion",
    ]
    assert observability["query_summary"]["query_runs"][0]["source"] == "linkedin"
    assert observability["by_source"]["linkedin"]["queries_executed_count"] == 2
    assert observability["by_source"]["indeed"]["queries_executed_count"] == 2
    assert observability["by_source"]["indeed"]["jobs_found_per_source"] == 1
    assert artifact["source_results"]["glassdoor"]["meta"]["reason"] == "source_disabled"


def test_jobs_collect_v1_stops_when_minimum_target_is_reached(monkeypatch) -> None:
    requested_limits: dict[str, int] = {}

    def _collector_for(source: str):
        class _Collector:
            SUPPORTED_FIELDS = {"source": source}

            @staticmethod
            def collect_jobs(request: dict, *, url_override: str | None = None) -> dict:
                del url_override
                requested_limits[source] = int(request.get("result_limit_per_source") or 0)
                jobs = [_single_job(source) | {"url": f"https://example.test/{source}/{idx}", "source_url": f"https://example.test/{source}/{idx}", "title": f"{source} role {idx}"} for idx in range(3)]
                return {
                    "status": "success",
                    "jobs": jobs,
                    "warnings": [],
                    "errors": [],
                    "meta": {
                        "requested_limit": requested_limits[source],
                        "returned_count": len(jobs),
                        "discovered_raw_count": len(jobs),
                        "kept_after_basic_filter_count": len(jobs),
                        "collection_stop_reason": "minimum_reached",
                    },
                }

        return _Collector

    monkeypatch.setattr(jobs_collect_v1, "_load_collector_module", _collector_for)

    payload = {
        "pipeline_id": "pipe-minimum-stop",
        "request": {
            "collectors_enabled": True,
            "sources": ["linkedin", "indeed"],
            "titles": ["Software Engineer"],
            "result_limit_per_source": 2,
            "minimum_unique_jobs_total": 3,
            "stop_when_minimum_reached": True,
        },
    }
    result = jobs_collect_v1.execute(_task(payload), db=None)
    artifact = result["content_json"]

    assert requested_limits == {"linkedin": 2}
    assert artifact["collection_summary"]["minimum_reached"] is True
    assert artifact["collection_summary"]["reason_stopped"] == "minimum_reached"
    assert artifact["collection_counts"]["minimum_unique_jobs_total_requested"] == 3
    assert artifact["collection_counts"]["reason_stopped"] == "minimum_reached"
    assert artifact["collection_observability"]["query_summary"]["minimum_reached"] is True
    assert artifact["collection_observability"]["query_summary"]["reason_stopped"] == "minimum_reached"
    assert artifact["source_results"]["indeed"]["status"] == "skipped"
    assert artifact["source_results"]["indeed"]["meta"]["reason"] == "minimum_reached"
    assert "Minimum jobs target reached before ranking." in artifact["collection_observability"]["run_preview"]["messages"]


def test_jobs_collect_v1_surfaces_minimum_target_shortfall(monkeypatch) -> None:
    def _collector_for(source: str):
        class _Collector:
            SUPPORTED_FIELDS = {"source": source}

            @staticmethod
            def collect_jobs(request: dict, *, url_override: str | None = None) -> dict:
                del request, url_override
                jobs = [_single_job(source)]
                return {
                    "status": "success",
                    "jobs": jobs,
                    "warnings": [],
                    "errors": [],
                    "meta": {
                        "requested_limit": 5,
                        "returned_count": 1,
                        "discovered_raw_count": 1,
                        "kept_after_basic_filter_count": 1,
                    },
                }

        return _Collector

    monkeypatch.setattr(jobs_collect_v1, "_load_collector_module", _collector_for)

    payload = {
        "pipeline_id": "pipe-minimum-shortfall",
        "request": {
            "collectors_enabled": True,
            "sources": ["linkedin", "indeed"],
            "titles": ["Software Engineer"],
            "result_limit_per_source": 5,
            "minimum_raw_jobs_total": 5,
            "minimum_unique_jobs_total": 5,
            "minimum_jobs_per_source": 3,
        },
    }
    result = jobs_collect_v1.execute(_task(payload), db=None)
    artifact = result["content_json"]
    debug_payload = result["debug_json"]

    assert artifact["collection_status"] == "partial_success"
    assert artifact["collection_summary"]["minimum_reached"] is False
    assert artifact["collection_summary"]["reason_stopped"] == "exhausted"
    assert "LinkedIn needs 2 more" in (artifact["collection_summary"]["minimum_shortfall_summary"] or "")
    assert debug_payload["minimum_reached"] is False
    assert debug_payload["reason_stopped"] == "exhausted"
    assert "Minimum jobs target shortfall:" in " ".join(artifact["collection_observability"]["run_preview"]["messages"])


def test_jobs_collect_v1_stops_for_time_cap(monkeypatch) -> None:
    monotonic_values = iter([0.0, 0.5, 2.0, 2.0])
    monkeypatch.setattr(jobs_collect_v1.time, "monotonic", lambda: next(monotonic_values))

    def _collector_for(source: str):
        class _Collector:
            SUPPORTED_FIELDS = {"source": source}

            @staticmethod
            def collect_jobs(request: dict, *, url_override: str | None = None) -> dict:
                del request, url_override
                return {
                    "status": "success",
                    "jobs": [_single_job(source)],
                    "warnings": [],
                    "errors": [],
                    "meta": {"requested_limit": 5, "returned_count": 1, "discovered_raw_count": 1},
                }

        return _Collector

    monkeypatch.setattr(jobs_collect_v1, "_load_collector_module", _collector_for)

    payload = {
        "pipeline_id": "pipe-time-cap",
        "request": {
            "collectors_enabled": True,
            "sources": ["linkedin", "indeed"],
            "titles": ["Software Engineer"],
            "result_limit_per_source": 5,
            "minimum_unique_jobs_total": 10,
            "collection_time_cap_seconds": 1,
        },
    }
    result = jobs_collect_v1.execute(_task(payload), db=None)
    artifact = result["content_json"]

    assert artifact["collection_summary"]["reason_stopped"] == "time_cap"
    assert artifact["collection_summary"]["minimum_reached"] is False
    assert artifact["source_results"]["indeed"]["status"] == "skipped"
    assert artifact["source_results"]["indeed"]["meta"]["reason"] == "time_cap_reached"


def test_jobs_collect_v1_logs_per_source_execution_and_empty_results(monkeypatch, caplog) -> None:
    class _EmptyCollector:
        SUPPORTED_FIELDS = {"source": "active"}

        @staticmethod
        def collect_jobs(request: dict, *, url_override: str | None = None) -> dict:
            del request, url_override
            return {"status": "success", "jobs": [], "warnings": [], "errors": [], "meta": {"returned_count": 0}}

    def _fake_load(source: str):
        if source in {"linkedin", "indeed"}:
            return _EmptyCollector
        raise AssertionError(f"unexpected source requested: {source}")

    monkeypatch.setattr(jobs_collect_v1, "_load_collector_module", _fake_load)

    payload = {
        "pipeline_id": "pipe-source-logs",
        "request": {
            "collectors_enabled": True,
            "sources": ["glassdoor", "handshake"],
            "titles": ["Software Engineer"],
            "locations": ["Remote"],
            "result_limit_per_source": 5,
        },
    }

    with caplog.at_level(logging.INFO):
        result = jobs_collect_v1.execute(_task(payload), db=None)

    messages = [record.getMessage() for record in caplog.records]
    assert any("jobs_collect source=glassdoor skipped:" in message for message in messages)
    assert any("jobs_collect source=handshake skipped:" in message for message in messages)
    assert any("jobs_collect source=linkedin status=start" in message for message in messages)
    assert any("jobs_collect source=indeed status=start" in message for message in messages)
    assert result["debug_json"]["sources_succeeded"] == []
    assert result["debug_json"]["sources_healthy"] == ["linkedin", "indeed"]
    assert result["debug_json"]["sources_empty"] == ["linkedin", "indeed"]
    assert result["debug_json"]["sources_failed"] == []
    assert result["debug_json"]["sources_skipped"] == ["glassdoor", "handshake"]
    assert result["debug_json"]["per_source_job_counts"] == {"glassdoor": 0, "handshake": 0, "linkedin": 0, "indeed": 0}


def test_jobs_collect_v1_defaults_enabled_sources_when_payload_omits_them(monkeypatch) -> None:
    requested_sources: list[str] = []

    class _Collector:
        SUPPORTED_FIELDS = {"source": "generic"}

        @staticmethod
        def collect_jobs(request: dict, *, url_override: str | None = None) -> dict:
            del url_override
            requested_sources.append(str(request.get("enabled_sources")))
            return {"status": "success", "jobs": [], "warnings": [], "errors": [], "meta": {"returned_count": 0}}

    monkeypatch.setattr(jobs_collect_v1, "_load_collector_module", lambda source: _Collector)

    payload = {
        "pipeline_id": "pipe-default-sources",
        "request": {
            "collectors_enabled": True,
            "query": "software engineer",
            "location": "United States",
        },
    }
    result = jobs_collect_v1.execute(_task(payload), db=None)

    assert result["content_json"]["request"]["enabled_sources"] == ["linkedin", "indeed"]
    assert result["content_json"]["request"]["sources"] == ["linkedin", "indeed"]
    assert result["debug_json"]["sources_attempted"] == ["linkedin", "indeed"]
