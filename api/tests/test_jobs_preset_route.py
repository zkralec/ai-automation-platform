import json
import sys

import pytest

sys.path.insert(0, "/app")

import planner_control


@pytest.fixture(autouse=True)
def _isolated_planner_db(tmp_path, monkeypatch):
    monkeypatch.setenv("PLANNER_CONTROL_DB_PATH", str(tmp_path / "planner-control.sqlite3"))


def test_ensure_jobs_digest_template_accepts_expanded_configuration() -> None:
    row = planner_control.ensure_jobs_digest_template(
        interval_seconds=420,
        desired_titles=["Machine Learning Engineer", "AI Engineer"],
        keywords=["python", "llm"],
        excluded_keywords=["intern"],
        preferred_locations=["Remote", "New York, NY"],
        remote_preference=["remote", "on-site"],
        minimum_salary=140000,
        experience_level="senior",
        enabled_sources=["linkedin", "indeed"],
        result_limit_per_source=333,
        minimum_raw_jobs_total=180,
        minimum_unique_jobs_total=120,
        minimum_jobs_per_source=40,
        stop_when_minimum_reached=True,
        collection_time_cap_seconds=150,
        max_queries_per_run=14,
        shortlist_count=5,
        freshness_preference="prefer_recent",
        jobs_notification_cooldown_days=7,
        jobs_shortlist_repeat_penalty=5.5,
        resurface_seen_jobs=False,
        enabled=True,
    )
    payload = json.loads(row["payload_json"])
    request = payload["request"]

    assert request["titles"] == ["Machine Learning Engineer", "AI Engineer"]
    assert request["keywords"] == ["python", "llm"]
    assert request["excluded_keywords"] == ["intern"]
    assert request["locations"] == ["Remote", "New York, NY"]
    assert request["work_mode_preference"] == ["remote", "onsite"]
    assert request["minimum_salary"] == 140000.0
    assert request["experience_level"] == "senior"
    assert request["sources"] == ["linkedin", "indeed"]
    assert request["result_limit_per_source"] == 333
    assert request["max_jobs_per_source"] == 333
    assert request["minimum_raw_jobs_total"] == 180
    assert request["minimum_unique_jobs_total"] == 120
    assert request["minimum_jobs_per_source"] == 40
    assert request["stop_when_minimum_reached"] is True
    assert request["collection_time_cap_seconds"] == 150
    assert request["max_queries_per_run"] == 14
    assert request["shortlist_max_items"] == 5
    assert request["shortlist_freshness_preference"] == "prefer_recent"
    assert request["shortlist_freshness_weight_enabled"] is True
    assert request["shortlist_freshness_max_bonus"] == 6.0
    assert request["jobs_notification_cooldown_days"] == 7
    assert request["jobs_shortlist_repeat_penalty"] == 5.5
    assert request["resurface_seen_jobs"] is False


def test_ensure_jobs_digest_template_maps_legacy_fields_for_compatibility() -> None:
    row = planner_control.ensure_jobs_digest_template(
        interval_seconds=300,
        desired_title="Data Scientist",
        location="Austin, TX",
        boards=["glassdoor"],
        desired_salary_min=125000,
        enabled=True,
    )
    payload = json.loads(row["payload_json"])
    request = payload["request"]

    assert request["titles"][0] == "Data Scientist"
    assert request["locations"][0] == "Austin, TX"
    assert request["sources"] == ["linkedin", "indeed"]
    assert request["disabled_sources"] == ["glassdoor"]
    assert request["source_configuration_notes"]
    assert request["minimum_salary"] == 125000.0
    assert request["minimum_raw_jobs_total"] == 120
    assert request["minimum_unique_jobs_total"] == 80
    assert request["minimum_jobs_per_source"] == 25


def test_ensure_jobs_digest_template_falls_back_when_sources_invalid() -> None:
    row = planner_control.ensure_jobs_digest_template(
        interval_seconds=300,
        desired_titles=["Backend Engineer"],
        enabled_sources=["monster", "careerbuilder"],
    )
    payload = json.loads(row["payload_json"])
    request = payload["request"]

    assert request["sources"] == ["linkedin", "indeed"]
    assert request["minimum_raw_jobs_total"] == 120
    assert request["minimum_unique_jobs_total"] == 80
