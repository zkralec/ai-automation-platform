import importlib
import json
import os
import sys
import uuid
from types import SimpleNamespace

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


@pytest.fixture()
def worker_module(tmp_path, monkeypatch):
    db_path = tmp_path / "worker_jobs_v2_end_to_end.db"
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db_path}")
    monkeypatch.setenv("USE_LLM", "false")
    monkeypatch.setenv("DAILY_BUDGET_USD", "10.0")
    monkeypatch.setenv("BUDGET_BUFFER_USD", "0.0")
    monkeypatch.setenv("OPENAI_MIN_COST_USD", "0.000001")

    if "worker" in sys.modules:
        del sys.modules["worker"]

    module = importlib.import_module("worker")
    module.Base.metadata.create_all(bind=module.engine)
    return module


def _seed_task(worker_module, *, task_type: str, payload: dict[str, object], model: str = "gpt-5-mini") -> str:
    task_id = str(uuid.uuid4())
    with worker_module.SessionLocal() as db:
        now = worker_module.now_utc()
        task = worker_module.Task(
            id=task_id,
            created_at=now,
            updated_at=now,
            status=worker_module.TaskStatus.queued,
            task_type=task_type,
            payload_json=json.dumps(payload, separators=(",", ":"), ensure_ascii=True),
            model=model,
            max_attempts=3,
        )
        db.add(task)
        db.commit()
    return task_id


def _task_ids_by_type(worker_module, task_type: str) -> list[str]:
    with worker_module.SessionLocal() as db:
        rows = (
            db.query(worker_module.Task)
            .filter(worker_module.Task.task_type == task_type)
            .order_by(worker_module.Task.created_at.asc())
            .all()
        )
        return [row.id for row in rows]


def _new_task_id(worker_module, task_type: str, seen_ids: set[str]) -> str:
    for task_id in _task_ids_by_type(worker_module, task_type):
        if task_id not in seen_ids:
            return task_id
    raise AssertionError(f"expected a new {task_type} task")


def _latest_artifact(worker_module, task_id: str, artifact_type: str) -> dict[str, object]:
    with worker_module.SessionLocal() as db:
        artifact = (
            db.query(worker_module.Artifact)
            .filter(worker_module.Artifact.task_id == task_id)
            .filter(worker_module.Artifact.artifact_type == artifact_type)
            .order_by(worker_module.Artifact.created_at.desc())
            .first()
        )
        assert artifact is not None, f"missing {artifact_type} for {task_id}"
        assert isinstance(artifact.content_json, dict)
        return artifact.content_json


def _task_status(worker_module, task_id: str) -> str:
    with worker_module.SessionLocal() as db:
        task = db.get(worker_module.Task, task_id)
        assert task is not None
        status = task.status
        return str(getattr(status, "value", status))


def _run_and_fetch_result(worker_module, task_id: str) -> dict[str, object]:
    worker_module.run_task(task_id)
    assert _task_status(worker_module, task_id) == "success"
    return _latest_artifact(worker_module, task_id, "result.json")


def _profile_stub(_request: dict[str, object]) -> dict[str, object]:
    return {
        "enabled": True,
        "applied": False,
        "source": "stored_profile_missing",
        "resume_name": None,
        "updated_at": None,
        "resume_char_count": 0,
        "resume_sent_char_count": 0,
        "resume_truncated": False,
        "resume_text": None,
    }


def _phase_for_request(request: dict[str, object]) -> str:
    keywords = [str(item).strip().lower() for item in request.get("keywords") or [] if str(item).strip()]
    if "phase2" in keywords:
        return "phase2"
    if "phase-empty" in keywords:
        return "phase-empty"
    return "phase1"


def _collector_dataset() -> dict[str, dict[str, list[dict[str, object]]]]:
    return {
        "phase1": {
            "linkedin": [
                {
                    "source": "linkedin",
                    "source_url": "https://www.linkedin.com/jobs/search/?keywords=backend+engineer&location=remote",
                    "title": "Senior Backend Engineer",
                    "company": "Acme Cloud",
                    "location": "Remote",
                    "url": "https://www.linkedin.com/jobs/view/1001",
                    "posted_at": "1 day ago",
                    "scraped_at": "2026-03-23T10:00:00Z",
                    "salary_min": 185000,
                    "salary_max": 215000,
                    "work_mode": "remote",
                    "experience_level": "senior",
                    "description_snippet": "Remote backend engineer building distributed systems and APIs.",
                }
            ],
            "indeed": [
                {
                    "source": "indeed",
                    "source_url": "https://www.indeed.com/jobs?q=senior+backend+engineer&l=Remote",
                    "title": "Senior Backend Engineer",
                    "company": "Acme Cloud",
                    "location": "Remote",
                    "url": "https://www.indeed.com/viewjob?jk=2001",
                    "posted_at": "2 days ago",
                    "scraped_at": "2026-03-23T10:00:00Z",
                    "salary_min": 180000,
                    "salary_max": 210000,
                    "work_mode": "remote",
                    "experience_level": "senior",
                    "description_snippet": "Distributed systems and platform APIs.",
                },
                {
                    "source": "indeed",
                    "title": "Backend Engineer",
                    "company": None,
                    "location": "Remote",
                    "salary_text": "From $140k a year",
                    "work_mode": "remote",
                    "experience_level": "mid",
                    "description_snippet": "Remote backend engineer building APIs and services.",
                },
            ],
            "handshake": [
                {
                    "source": "handshake",
                    "source_url": "https://joinhandshake.com/stu/jobs/3001",
                    "title": "Platform Engineer I",
                    "company": "CampusWorks",
                    "location": "Austin, TX",
                    "url": "https://joinhandshake.com/stu/jobs/3001",
                    "posted_at": "4 days ago",
                    "scraped_at": "2026-03-23T10:00:00Z",
                    "salary_min": 95000,
                    "salary_max": 110000,
                    "work_mode": "onsite",
                    "experience_level": "entry",
                    "description_snippet": "Entry-level platform engineering role.",
                }
            ],
        },
        "phase2": {
            "linkedin": [
                {
                    "source": "linkedin",
                    "source_url": "https://www.linkedin.com/jobs/search/?keywords=backend+engineer&location=remote",
                    "title": "Senior Backend Engineer",
                    "company": "Acme Cloud",
                    "location": "Remote",
                    "url": "https://www.linkedin.com/jobs/view/1001",
                    "posted_at": "1 day ago",
                    "scraped_at": "2026-03-24T10:00:00Z",
                    "salary_min": 185000,
                    "salary_max": 215000,
                    "work_mode": "remote",
                    "experience_level": "senior",
                    "description_snippet": "Remote backend engineer building distributed systems and APIs.",
                }
            ],
            "indeed": [
                {
                    "source": "indeed",
                    "title": "Backend Engineer",
                    "company": None,
                    "location": "Remote",
                    "salary_text": "From $140k a year",
                    "work_mode": "remote",
                    "experience_level": "mid",
                    "description_snippet": "Remote backend engineer building APIs and services.",
                }
            ],
            "handshake": [
                {
                    "source": "handshake",
                    "source_url": "https://joinhandshake.com/stu/jobs/3002",
                    "title": "Platform Engineer I",
                    "company": "Orbit Labs",
                    "location": "Remote",
                    "url": "https://joinhandshake.com/stu/jobs/3002",
                    "posted_at": "5 days ago",
                    "scraped_at": "2026-03-24T10:00:00Z",
                    "salary_min": 105000,
                    "salary_max": 120000,
                    "work_mode": "remote",
                    "experience_level": "entry",
                    "description_snippet": "Entry backend role for developer platform tooling.",
                }
            ],
        },
        "phase-empty": {
            "linkedin": [
                {
                    "source": "linkedin",
                    "title": "Backend Engineer",
                    "company": None,
                    "location": "Remote",
                    "description_snippet": "Valid job, but metadata is weak and compensation is unknown.",
                    "work_mode": "remote",
                    "experience_level": "mid",
                }
            ],
            "indeed": [],
            "handshake": [
                {
                    "source": "handshake",
                    "title": "Platform Support Engineer",
                    "company": "Tiny Ops",
                    "location": "Austin, TX",
                    "description_snippet": "Support-oriented platform role.",
                    "work_mode": "onsite",
                    "experience_level": "entry",
                }
            ],
        },
    }


def _collector_for(source: str):
    dataset = _collector_dataset()

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
            "max_pages_per_source": True,
            "max_jobs_per_source": True,
            "max_queries_per_title_location_pair": True,
            "max_queries_per_run": True,
            "enable_query_expansion": True,
        }

        @staticmethod
        def collect_jobs(request: dict[str, object], *, url_override: str | None = None) -> dict[str, object]:
            phase = _phase_for_request(request)
            rows = [dict(item) for item in dataset[phase].get(source, [])]
            output: list[dict[str, object]] = []
            for row in rows:
                if url_override and not row.get("source_url"):
                    row["source_url"] = url_override
                output.append(row)

            metadata_completeness_summary = {
                "job_count": len(output),
                "missing_company": sum(1 for row in output if not str(row.get("company") or "").strip()),
                "missing_posted_at": sum(1 for row in output if not str(row.get("posted_at") or "").strip()),
                "missing_source_url": sum(
                    1
                    for row in output
                    if not str(row.get("source_url") or row.get("url") or "").strip()
                ),
                "missing_location": sum(1 for row in output if not str(row.get("location") or "").strip()),
            }

            queries_executed = 4 if phase != "phase-empty" else 3
            search_attempts = [
                {
                    "query": f"backend engineer {idx}",
                    "location": "remote",
                    "jobs_found": len(output) if idx == 0 else max(len(output) - idx, 0),
                    "new_unique_jobs": len(output) if idx == 0 else max(len(output) - idx, 0),
                    "returned_count": len(output),
                    "stop_reason": "no_new_results" if idx == queries_executed - 1 else None,
                }
                for idx in range(queries_executed)
            ]
            return {
                "status": "success",
                "jobs": output,
                "warnings": ["low_volume_source"] if len(output) <= 1 else [],
                "errors": [],
                "meta": {
                    "requested_limit": int(request.get("result_limit_per_source") or 25),
                    "returned_count": len(output),
                    "discovered_raw_count": len(output),
                    "kept_after_basic_filter_count": len(output),
                    "dropped_by_basic_filter_count": 0,
                    "deduped_count": 0,
                    "queries_executed_count": queries_executed,
                    "empty_queries_count": 1 if len(output) <= 1 else 0,
                    "queries_attempted": [row["query"] for row in search_attempts],
                    "query_examples": [row["query"] for row in search_attempts[:3]],
                    "search_attempts": search_attempts,
                    "pages_fetched": min(queries_executed, 2),
                    "metadata_completeness_summary": metadata_completeness_summary,
                },
            }

    return _Collector


def _patch_pipeline_runtime(monkeypatch, worker_module):
    jobs_collect_module = importlib.import_module("task_handlers.jobs_collect_v1")
    jobs_rank_module = importlib.import_module("task_handlers.jobs_rank_v1")

    monkeypatch.setattr(
        jobs_collect_module,
        "_load_collector_module",
        lambda source: _collector_for(str(source).strip().lower()),
    )
    monkeypatch.setattr(worker_module.queue, "enqueue", lambda _fn_name, _task_id: None)
    monkeypatch.setattr(jobs_rank_module, "resolve_profile_context", _profile_stub)


def test_jobs_v2_end_to_end_multi_run_resurfacing_and_notify_flow(worker_module, monkeypatch) -> None:
    monkeypatch.setenv("NOTIFY_DISCORD_ALLOWLIST", "jobs_digest_v2")
    _patch_pipeline_runtime(monkeypatch, worker_module)

    notify_module = importlib.import_module("task_handlers.notify_v1")
    delivered_messages: list[str] = []

    def _fake_send_notification(channels, message, metadata):
        del metadata
        delivered_messages.append(message)
        assert channels == ["discord"]
        return {
            "discord": {
                "provider": "discord",
                "status": "sent",
                "http_status": 204,
                "rate_limited": False,
            }
        }

    monkeypatch.setattr(notify_module, "send_notification", _fake_send_notification)

    root_payload = {
        "pipeline_id": "pipe-jobs-e2e-phase1",
        "request": {
            "collectors_enabled": True,
            "sources": ["linkedin", "indeed", "handshake"],
            "titles": ["Backend Engineer"],
            "desired_title_keywords": ["backend engineer"],
            "keywords": ["phase1", "distributed", "api"],
            "locations": ["Remote"],
            "result_limit_per_source": 250,
            "max_queries_per_run": 12,
            "max_pages_per_source": 5,
            "max_total_jobs": 500,
            "shortlist_count": 1,
            "jobs_notification_cooldown_days": 7,
            "resurface_seen_jobs": True,
        },
    }

    seen_task_ids: set[str] = set()
    collect_task_id = _seed_task(worker_module, task_type="jobs_collect_v1", payload=root_payload)
    seen_task_ids.add(collect_task_id)
    collect_artifact = _run_and_fetch_result(worker_module, collect_task_id)
    collect_followup = _latest_artifact(worker_module, collect_task_id, "followup.json")

    assert collect_artifact["artifact_type"] == "jobs.collect.v1"
    assert collect_artifact["collection_summary"]["discovered_raw_count"] == 4
    assert collect_artifact["collection_summary"]["deduped_count"] == 0
    assert collect_artifact["collection_observability"]["query_summary"]["queries_executed"] == 12
    assert collect_artifact["collection_observability"]["by_source"]["handshake"]["jobs_found_per_source"] == 1
    assert collect_followup["requested_count"] == 1

    normalize_task_id = _new_task_id(worker_module, "jobs_normalize_v1", seen_task_ids)
    seen_task_ids.add(normalize_task_id)
    normalize_artifact = _run_and_fetch_result(worker_module, normalize_task_id)

    assert normalize_artifact["artifact_type"] == "jobs.normalize.v1"
    assert normalize_artifact["counts"]["raw_count"] == 4
    assert normalize_artifact["counts"]["deduped_count"] == 3
    assert normalize_artifact["counts"]["duplicates_collapsed"] == 1
    normalized_jobs = normalize_artifact["normalized_jobs"]
    incomplete_job = next(job for job in normalized_jobs if job["title"] == "Backend Engineer")
    assert incomplete_job["missing_company"] is True
    assert incomplete_job["missing_source_url"] is True
    assert incomplete_job["missing_posted_at"] is True
    assert normalize_artifact["normalization_observability"]["by_source"]["indeed"]["missing_rates"]["missing_company_rate"] > 0

    rank_task_id = _new_task_id(worker_module, "jobs_rank_v1", seen_task_ids)
    seen_task_ids.add(rank_task_id)
    rank_artifact = _run_and_fetch_result(worker_module, rank_task_id)

    assert rank_artifact["artifact_type"] == "jobs.rank.v1"
    assert rank_artifact["pipeline_counts"]["scored_count"] == 2
    assert rank_artifact["model_usage"]["llm_runtime_enabled"] is False
    top_ranked = rank_artifact["ranked_jobs"][0]
    assert top_ranked["company"] == "Acme Cloud"
    assert top_ranked["metadata_quality_penalty"] < rank_artifact["ranked_jobs"][1]["metadata_quality_penalty"]

    shortlist_task_id = _new_task_id(worker_module, "jobs_shortlist_v1", seen_task_ids)
    seen_task_ids.add(shortlist_task_id)
    shortlist_artifact = _run_and_fetch_result(worker_module, shortlist_task_id)

    assert shortlist_artifact["artifact_type"] == "jobs.shortlist.v1"
    assert shortlist_artifact["shortlist_count"] == 1
    assert shortlist_artifact["shortlist"][0]["company"] == "Acme Cloud"
    assert shortlist_artifact["history_observability"]["selected_newly_discovered_count"] == 1

    digest_task_id = _new_task_id(worker_module, "jobs_digest_v2", seen_task_ids)
    seen_task_ids.add(digest_task_id)
    digest_artifact = _run_and_fetch_result(worker_module, digest_task_id)

    assert digest_artifact["artifact_type"] == "jobs.digest.v2"
    assert digest_artifact["notify_decision"]["should_notify"] is True
    assert digest_artifact["pipeline_counts"]["shortlisted_count"] == 1
    assert digest_artifact["digest_jobs"][0]["company"] == "Acme Cloud"

    notify_task_id = _new_task_id(worker_module, "notify_v1", seen_task_ids)
    seen_task_ids.add(notify_task_id)
    notify_artifact = _run_and_fetch_result(worker_module, notify_task_id)

    assert notify_artifact["sent"] is True
    assert notify_artifact["source_task_type"] == "jobs_digest_v2"
    assert delivered_messages, "expected first-run notification delivery"

    second_collect_payload = {
        "pipeline_id": "pipe-jobs-e2e-phase2",
        "request": {
            "collectors_enabled": True,
            "sources": ["linkedin", "indeed", "handshake"],
            "titles": ["Backend Engineer"],
            "desired_title_keywords": ["backend engineer"],
            "keywords": ["phase2", "distributed", "api"],
            "locations": ["Remote"],
            "result_limit_per_source": 250,
            "max_queries_per_run": 12,
            "max_pages_per_source": 5,
            "max_total_jobs": 500,
            "shortlist_count": 1,
            "jobs_notification_cooldown_days": 7,
            "resurface_seen_jobs": True,
        },
    }

    collect_task_id_2 = _seed_task(worker_module, task_type="jobs_collect_v1", payload=second_collect_payload)
    seen_task_ids.add(collect_task_id_2)
    _run_and_fetch_result(worker_module, collect_task_id_2)

    normalize_task_id_2 = _new_task_id(worker_module, "jobs_normalize_v1", seen_task_ids)
    seen_task_ids.add(normalize_task_id_2)
    _run_and_fetch_result(worker_module, normalize_task_id_2)

    rank_task_id_2 = _new_task_id(worker_module, "jobs_rank_v1", seen_task_ids)
    seen_task_ids.add(rank_task_id_2)
    _run_and_fetch_result(worker_module, rank_task_id_2)

    shortlist_task_id_2 = _new_task_id(worker_module, "jobs_shortlist_v1", seen_task_ids)
    seen_task_ids.add(shortlist_task_id_2)
    shortlist_artifact_2 = _run_and_fetch_result(worker_module, shortlist_task_id_2)

    assert shortlist_artifact_2["shortlist_count"] == 1
    resurfaced = shortlist_artifact_2["shortlist"][0]
    assert resurfaced["title"] == "Backend Engineer"
    assert resurfaced["newly_discovered"] is False
    assert resurfaced["resurfaced_from_prior_runs"] is True
    assert resurfaced["previously_shortlisted"] is False
    assert resurfaced["previously_notified"] is False
    assert shortlist_artifact_2["history_observability"]["cooldown_suppressed_count"] == 1
    assert shortlist_artifact_2["history_observability"]["selected_resurfaced_count"] == 1

    digest_task_id_2 = _new_task_id(worker_module, "jobs_digest_v2", seen_task_ids)
    seen_task_ids.add(digest_task_id_2)
    digest_artifact_2 = _run_and_fetch_result(worker_module, digest_task_id_2)

    assert digest_artifact_2["notify_decision"]["should_notify"] is True
    assert digest_artifact_2["digest_jobs"][0]["title"] == "Backend Engineer"
    assert digest_artifact_2["digest_jobs"][0]["company"] in (None, "")

    notify_task_id_2 = _new_task_id(worker_module, "notify_v1", seen_task_ids)
    notify_artifact_2 = _run_and_fetch_result(worker_module, notify_task_id_2)

    assert notify_artifact_2["sent"] is True
    assert len(delivered_messages) == 2


def test_jobs_v2_end_to_end_empty_shortlist_keeps_diagnosable_artifacts(worker_module, monkeypatch) -> None:
    _patch_pipeline_runtime(monkeypatch, worker_module)

    payload = {
        "pipeline_id": "pipe-jobs-e2e-empty",
        "request": {
            "collectors_enabled": True,
            "sources": ["linkedin", "indeed", "handshake"],
            "titles": ["Backend Engineer"],
            "desired_title_keywords": ["backend engineer"],
            "keywords": ["phase-empty"],
            "locations": ["Remote"],
            "result_limit_per_source": 100,
            "max_queries_per_run": 8,
            "max_pages_per_source": 4,
                "max_total_jobs": 200,
                "shortlist_count": 3,
                "shortlist_min_score": 99,
                "shortlist_fail_soft_enabled": False,
                "jobs_notification_cooldown_days": 7,
            },
        }

    seen_task_ids: set[str] = set()
    collect_task_id = _seed_task(worker_module, task_type="jobs_collect_v1", payload=payload)
    seen_task_ids.add(collect_task_id)
    collect_artifact = _run_and_fetch_result(worker_module, collect_task_id)

    assert collect_artifact["collection_observability"]["query_summary"]["queries_executed"] == 9
    assert collect_artifact["collection_summary"]["discovered_raw_count"] == 2
    assert collect_artifact["collection_observability"]["by_source"]["linkedin"]["missing_counts"]["company"] == 1

    normalize_task_id = _new_task_id(worker_module, "jobs_normalize_v1", seen_task_ids)
    seen_task_ids.add(normalize_task_id)
    normalize_artifact = _run_and_fetch_result(worker_module, normalize_task_id)

    assert normalize_artifact["counts"]["normalized_count"] == 2
    assert normalize_artifact["normalization_observability"]["operator_questions"]["are_we_missing_metadata"]

    rank_task_id = _new_task_id(worker_module, "jobs_rank_v1", seen_task_ids)
    seen_task_ids.add(rank_task_id)
    rank_artifact = _run_and_fetch_result(worker_module, rank_task_id)

    assert rank_artifact["pipeline_counts"]["scored_count"] == 1
    assert rank_artifact["ranked_jobs"], "ranking should keep incomplete but valid jobs"

    shortlist_task_id = _new_task_id(worker_module, "jobs_shortlist_v1", seen_task_ids)
    seen_task_ids.add(shortlist_task_id)
    shortlist_artifact = _run_and_fetch_result(worker_module, shortlist_task_id)

    assert shortlist_artifact["shortlist_count"] == 0
    assert shortlist_artifact["shortlist"] == []

    digest_task_id = _new_task_id(worker_module, "jobs_digest_v2", seen_task_ids)
    seen_task_ids.add(digest_task_id)
    digest_artifact = _run_and_fetch_result(worker_module, digest_task_id)
    digest_followup = _latest_artifact(worker_module, digest_task_id, "followup.json")

    assert digest_artifact["artifact_type"] == "jobs.digest.v2"
    assert digest_artifact["notify_decision"]["should_notify"] is False
    assert digest_artifact["notify_decision"]["reason"] == "skipped_empty_shortlist"
    assert digest_artifact["pipeline_counts"]["shortlisted_count"] == 0
    assert digest_followup["requested_count"] == 0
    assert digest_followup["notify_decision"]["reason"] == "skipped_empty_shortlist"
    assert digest_followup["counts"]["enqueued"] == 0
    assert _task_ids_by_type(worker_module, "notify_v1") == []
