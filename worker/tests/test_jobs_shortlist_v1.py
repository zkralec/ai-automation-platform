import json
import os
import sys
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "worker"))

from jobs_history_state import record_jobs_notified, record_jobs_seen
from task_handlers import jobs_shortlist_v1


def _task(payload: dict, *, task_id: str = "task-shortlist-1", run_id: str = "run-shortlist-1") -> SimpleNamespace:
    return SimpleNamespace(
        id=task_id,
        max_attempts=3,
        _run_id=run_id,
        payload_json=json.dumps(payload, separators=(",", ":"), ensure_ascii=True),
    )


def _sqlite_session(tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path / 'jobs_shortlist_history.db'}")
    return sessionmaker(bind=engine, future=True)()


def test_jobs_shortlist_v1_consumes_jobs_scored_from_rank_artifact(monkeypatch) -> None:
    monkeypatch.setattr(
        jobs_shortlist_v1,
        "fetch_upstream_result_content_json",
        lambda db, upstream: {
            "artifact_type": "jobs.rank.v1",
            "pipeline_counts": {
                "collected_count": 40,
                "normalized_count": 22,
                "deduped_count": 18,
                "duplicates_collapsed": 4,
                "scored_count": 12,
            },
            "jobs_scored_artifact": {
                "artifact_type": "jobs_scored.v1",
                "jobs_scored": [
                    {
                        "job_id": "j1",
                        "title": "Senior ML Engineer",
                        "company": "Acme",
                        "source": "linkedin",
                        "overall_score": 94,
                        "score": 1.88,
                        "scoring_mode": "llm_structured",
                        "duplicate_group_id": "dup-1",
                        "explanation_summary": "Top fit.",
                    },
                    {
                        "job_id": "j2",
                        "title": "Senior ML Engineer II",
                        "company": "Acme",
                        "source": "indeed",
                        "overall_score": 93,
                        "score": 1.86,
                        "scoring_mode": "llm_structured",
                        "duplicate_group_id": "dup-2",
                        "explanation_summary": "Strong fit.",
                    },
                    {
                        "job_id": "j3",
                        "title": "Machine Learning Engineer",
                        "company": "Beta Labs",
                        "source": "glassdoor",
                        "overall_score": 90,
                        "score": 1.8,
                        "scoring_mode": "llm_structured",
                        "duplicate_group_id": "dup-3",
                        "explanation_summary": "Good fit.",
                    },
                    {
                        "job_id": "j4",
                        "title": "Machine Learning Engineer",
                        "company": "Beta Labs",
                        "source": "glassdoor",
                        "overall_score": 89,
                        "score": 1.78,
                        "scoring_mode": "llm_structured",
                        "duplicate_group_id": "dup-4",
                        "explanation_summary": "Similar role.",
                    },
                ],
            },
            "ranked_jobs": [],
        },
    )

    payload = {
        "pipeline_id": "pipe-short-1",
        "upstream": {"task_id": "rank-task", "run_id": "rank-run", "task_type": "jobs_rank_v1"},
        "request": {"shortlist_max_items": 3, "shortlist_min_score": 0.1},
        "shortlist_policy": {"max_items": 3, "per_company_cap": 1, "per_source_cap": 3},
    }
    result = jobs_shortlist_v1.execute(_task(payload), db=object())
    artifact = result["content_json"]

    assert artifact["artifact_type"] == "jobs.shortlist.v1"
    assert artifact["jobs_top_artifact"]["artifact_type"] == "jobs_top.v1"
    assert artifact["shortlist_count"] == 2
    companies = [row.get("company") for row in artifact["shortlist"]]
    assert sorted(companies) == ["Acme", "Beta Labs"]
    assert artifact["shortlist_summary_metadata"]["upstream_artifact_type"] == "jobs.rank.v1"
    assert artifact["ranking_mode"] == "llm_structured"
    assert artifact["fallback_used"] is False
    assert artifact["fail_soft_applied"] is False
    assert artifact["shortlist_confidence"] == "normal"
    assert artifact["pipeline_counts"]["collected_count"] == 40
    assert artifact["jobs_top_artifact"]["pipeline_counts"]["deduped_count"] == 18
    assert isinstance(artifact["notification_candidates"], list)
    assert result["next_tasks"][0]["task_type"] == "jobs_digest_v2"


def test_jobs_shortlist_v1_accepts_direct_jobs_scored_artifact(monkeypatch) -> None:
    monkeypatch.setattr(
        jobs_shortlist_v1,
        "fetch_upstream_result_content_json",
        lambda db, upstream: {
            "artifact_type": "jobs_scored.v1",
            "jobs_scored": [
                {
                    "job_id": "j1",
                    "title": "AI Engineer",
                    "company": "Acme",
                    "source": "linkedin",
                    "overall_score": 92,
                    "score": 1.84,
                }
            ],
        },
    )

    payload = {
        "pipeline_id": "pipe-short-2",
        "upstream": {"task_id": "rank-task", "run_id": "rank-run", "task_type": "jobs_rank_v1"},
        "request": {"shortlist_max_items": 5, "shortlist_min_score": 0.1},
    }
    result = jobs_shortlist_v1.execute(_task(payload), db=object())
    artifact = result["content_json"]

    assert artifact["shortlist_count"] == 1
    assert artifact["shortlist_summary_metadata"]["upstream_artifact_type"] == "jobs_scored.v1"


def test_jobs_shortlist_v1_freshness_weighting_promotes_recent_jobs(monkeypatch) -> None:
    monkeypatch.setattr(
        jobs_shortlist_v1,
        "fetch_upstream_result_content_json",
        lambda db, upstream: {
            "artifact_type": "jobs_scored.v1",
            "jobs_scored": [
                {
                    "job_id": "old-high",
                    "title": "ML Engineer",
                    "company": "OldCorp",
                    "source": "linkedin",
                    "overall_score": 90,
                    "score": 1.8,
                    "posted_at": "2024-01-01T00:00:00Z",
                },
                {
                    "job_id": "new-slightly-lower",
                    "title": "ML Engineer",
                    "company": "NewCorp",
                    "source": "indeed",
                    "overall_score": 87,
                    "score": 1.74,
                    "posted_at": "2026-03-10T00:00:00Z",
                },
            ],
        },
    )

    payload = {
        "pipeline_id": "pipe-short-3",
        "upstream": {"task_id": "rank-task", "run_id": "rank-run", "task_type": "jobs_rank_v1"},
        "request": {"shortlist_max_items": 1, "shortlist_min_score": 0.1},
        "shortlist_policy": {"max_items": 1, "freshness_weight_enabled": True, "freshness_max_bonus": 20},
    }
    result = jobs_shortlist_v1.execute(_task(payload), db=object())
    top = result["content_json"]["shortlist"][0]

    assert top["job_id"] == "new-slightly-lower"


def test_jobs_shortlist_v1_prefers_more_complete_metadata_when_scores_are_close(monkeypatch) -> None:
    monkeypatch.setattr(
        jobs_shortlist_v1,
        "fetch_upstream_result_content_json",
        lambda db, upstream: {
            "artifact_type": "jobs_scored.v1",
            "jobs_scored": [
                {
                    "job_id": "weak-topline",
                    "title": "Senior ML Engineer",
                    "company": "",
                    "source": "linkedin",
                    "overall_score": 91,
                    "score": 1.82,
                    "location": "Remote",
                    "source_url": None,
                    "posted_at": None,
                    "metadata_quality_score": 30,
                    "missing_company": True,
                    "missing_source_url": True,
                    "missing_posted_at": True,
                },
                {
                    "job_id": "complete-close",
                    "title": "Senior ML Engineer",
                    "company": "Acme",
                    "source": "indeed",
                    "overall_score": 89,
                    "score": 1.78,
                    "location": "Remote",
                    "source_url": "https://www.indeed.com/viewjob?jk=123",
                    "posted_at": "2026-03-20T00:00:00Z",
                    "metadata_quality_score": 94,
                    "has_direct_source_url": True,
                },
            ],
        },
    )

    payload = {
        "pipeline_id": "pipe-short-quality",
        "upstream": {"task_id": "rank-task", "run_id": "rank-run", "task_type": "jobs_rank_v1"},
        "request": {"shortlist_max_items": 1, "shortlist_min_score": 0.1},
        "shortlist_policy": {"max_items": 1, "freshness_weight_enabled": False},
    }
    result = jobs_shortlist_v1.execute(_task(payload), db=object())
    top = result["content_json"]["shortlist"][0]

    assert top["job_id"] == "complete-close"
    assert top["metadata_quality_score"] > 90


def test_jobs_shortlist_v1_empty_input_keeps_artifact_shape(monkeypatch) -> None:
    monkeypatch.setattr(
        jobs_shortlist_v1,
        "fetch_upstream_result_content_json",
        lambda db, upstream: {
            "artifact_type": "jobs_scored.v1",
            "jobs_scored": [],
            "pipeline_counts": {"collected_count": 0, "normalized_count": 0, "deduped_count": 0, "scored_count": 0},
        },
    )

    payload = {
        "pipeline_id": "pipe-short-empty",
        "upstream": {"task_id": "rank-task", "run_id": "rank-run", "task_type": "jobs_rank_v1"},
        "request": {"shortlist_max_items": 5, "shortlist_min_score": 0.1},
    }
    result = jobs_shortlist_v1.execute(_task(payload), db=object())
    artifact = result["content_json"]

    assert artifact["artifact_type"] == "jobs.shortlist.v1"
    assert artifact["shortlist_count"] == 0
    assert artifact["shortlist"] == []
    assert artifact["jobs_top_artifact"]["top_jobs"] == []
    assert artifact["pipeline_counts"]["shortlisted_count"] == 0
    assert result["next_tasks"][0]["task_type"] == "jobs_digest_v2"


def test_jobs_shortlist_v1_fail_soft_selects_floor_when_rank_fallback_clears_standard_shortlist(monkeypatch) -> None:
    monkeypatch.setattr(
        jobs_shortlist_v1,
        "fetch_upstream_result_content_json",
        lambda db, upstream: {
            "artifact_type": "jobs.rank.v1",
            "warnings": [
                "llm_batch_1_failed: ValueError: empty_llm_output",
                "llm_scoring_disabled_after_repeated_batch_failures",
            ],
            "model_usage": {"llm_runtime_enabled": True},
            "jobs_scored_artifact": {
                "artifact_type": "jobs_scored.v1",
                "llm": {"fallback_used": True},
                "jobs_scored": [
                    {
                        "job_id": "f1",
                        "title": "Software Engineer, New Grad",
                        "company": "Acme",
                        "source": "linkedin",
                        "overall_score": 4.0,
                        "overall_score_adjusted": 0.0,
                        "score": 0.0,
                        "scoring_mode": "deterministic_fallback",
                        "explanation_summary": "Deterministic fallback score.",
                    },
                    {
                        "job_id": "f2",
                        "title": "Software Engineer I",
                        "company": "Beta",
                        "source": "linkedin",
                        "overall_score": 4.0,
                        "overall_score_adjusted": 0.0,
                        "score": 0.0,
                        "scoring_mode": "deterministic_fallback",
                        "explanation_summary": "Deterministic fallback score.",
                    },
                    {
                        "job_id": "f3",
                        "title": "Associate Software Engineer",
                        "company": "Gamma",
                        "source": "linkedin",
                        "overall_score": 4.0,
                        "overall_score_adjusted": 0.0,
                        "score": 0.0,
                        "scoring_mode": "deterministic_fallback",
                        "explanation_summary": "Deterministic fallback score.",
                    },
                    {
                        "job_id": "f4",
                        "title": "Backend Engineer",
                        "company": "Delta",
                        "source": "linkedin",
                        "overall_score": 4.0,
                        "overall_score_adjusted": 0.0,
                        "score": 0.0,
                        "scoring_mode": "deterministic_fallback",
                        "explanation_summary": "Deterministic fallback score.",
                    },
                ],
            },
            "ranked_jobs": [],
        },
    )

    payload = {
        "pipeline_id": "pipe-short-fail-soft",
        "upstream": {"task_id": "rank-task", "run_id": "rank-run", "task_type": "jobs_rank_v1"},
        "request": {
            "search_mode": "broad_discovery",
            "shortlist_max_items": 5,
            "shortlist_min_score": 0.75,
            "shortlist_fallback_min_items": 3,
        },
    }
    result = jobs_shortlist_v1.execute(_task(payload), db=object())
    artifact = result["content_json"]

    assert artifact["shortlist_count"] == 3
    assert artifact["ranking_mode"] == "deterministic_fallback"
    assert artifact["fallback_used"] is True
    assert artifact["fail_soft_applied"] is True
    assert artifact["shortlist_confidence"] == "low"
    assert artifact["shortlist_summary_metadata"]["fail_soft_target_items"] == 3
    assert all(row["fail_soft_selected"] is True for row in artifact["shortlist"])
    assert all(row["selection_basis"] == "fail_soft" for row in artifact["shortlist"])


def test_jobs_shortlist_v1_fail_soft_still_allows_empty_shortlist_for_zero_signal_pool(monkeypatch) -> None:
    monkeypatch.setattr(
        jobs_shortlist_v1,
        "fetch_upstream_result_content_json",
        lambda db, upstream: {
            "artifact_type": "jobs.rank.v1",
            "warnings": ["llm_batch_1_failed: ValueError: empty_llm_output"],
            "model_usage": {"llm_runtime_enabled": True},
            "jobs_scored_artifact": {
                "artifact_type": "jobs_scored.v1",
                "llm": {"fallback_used": True},
                "jobs_scored": [
                    {
                        "job_id": "z1",
                        "title": "Generic Role",
                        "company": "UnknownCo",
                        "source": "linkedin",
                        "overall_score": 0.0,
                        "overall_score_adjusted": 0.0,
                        "score": 0.0,
                        "scoring_mode": "deterministic_fallback",
                        "explanation_summary": "Deterministic fallback score.",
                    },
                    {
                        "job_id": "z2",
                        "title": "Another Generic Role",
                        "company": "UnknownCo2",
                        "source": "linkedin",
                        "overall_score": 0.0,
                        "overall_score_adjusted": 0.0,
                        "score": 0.0,
                        "scoring_mode": "deterministic_fallback",
                        "explanation_summary": "Deterministic fallback score.",
                    },
                ],
            },
            "ranked_jobs": [],
        },
    )

    payload = {
        "pipeline_id": "pipe-short-fail-soft-empty",
        "upstream": {"task_id": "rank-task", "run_id": "rank-run", "task_type": "jobs_rank_v1"},
        "request": {
            "shortlist_max_items": 5,
            "shortlist_min_score": 0.75,
        },
    }
    result = jobs_shortlist_v1.execute(_task(payload), db=object())
    artifact = result["content_json"]

    assert artifact["shortlist_count"] == 0
    assert artifact["ranking_mode"] == "deterministic_fallback"
    assert artifact["fallback_used"] is True
    assert artifact["fail_soft_applied"] is False
    assert artifact["shortlist_confidence"] == "low"


def test_jobs_shortlist_v1_recognizes_broad_discovery_ranking_mode(monkeypatch) -> None:
    monkeypatch.setattr(
        jobs_shortlist_v1,
        "fetch_upstream_result_content_json",
        lambda db, upstream: {
            "artifact_type": "jobs.rank.v1",
            "warnings": ["llm_batch_1_failed: ValueError: empty_llm_output"],
            "model_usage": {"llm_runtime_enabled": True},
            "jobs_scored_artifact": {
                "artifact_type": "jobs_scored.v1",
                "llm": {"fallback_used": True},
                "jobs_scored": [
                    {
                        "job_id": "bd1",
                        "title": "Backend Engineer",
                        "company": "Acme",
                        "source": "linkedin",
                        "overall_score": 61.0,
                        "overall_score_adjusted": 49.0,
                        "score": 0.98,
                        "scoring_mode": "deterministic_broad_discovery",
                        "explanation_summary": "Deterministic broad-discovery ranking used after LLM scoring was unavailable.",
                    }
                ],
            },
            "ranked_jobs": [],
        },
    )

    payload = {
        "pipeline_id": "pipe-short-broad-mode",
        "upstream": {"task_id": "rank-task", "run_id": "rank-run", "task_type": "jobs_rank_v1"},
        "request": {
            "shortlist_max_items": 5,
            "shortlist_min_score": 0.75,
        },
    }
    result = jobs_shortlist_v1.execute(_task(payload), db=object())
    artifact = result["content_json"]

    assert artifact["ranking_mode"] == "deterministic_broad_discovery"
    assert artifact["fallback_used"] is True
    assert artifact["shortlist_confidence"] == "low"


def test_jobs_shortlist_v1_duplicate_heavy_fixture_prefers_diversity(monkeypatch, jobs_v2_samples) -> None:
    scored_rows = [dict(row) for row in jobs_v2_samples["shortlist_duplicate_heavy_scored_jobs"]]
    monkeypatch.setattr(
        jobs_shortlist_v1,
        "fetch_upstream_result_content_json",
        lambda db, upstream: {
            "artifact_type": "jobs_scored.v1",
            "jobs_scored": scored_rows,
            "pipeline_counts": {"collected_count": 30, "normalized_count": 20, "deduped_count": 16, "scored_count": len(scored_rows)},
        },
    )

    payload = {
        "pipeline_id": "pipe-short-dup-heavy",
        "upstream": {"task_id": "rank-task", "run_id": "rank-run", "task_type": "jobs_rank_v1"},
        "request": {"shortlist_max_items": 3, "shortlist_min_score": 0.1},
        "shortlist_policy": {"max_items": 3, "per_company_cap": 1, "per_source_cap": 2},
    }
    result = jobs_shortlist_v1.execute(_task(payload), db=object())
    artifact = result["content_json"]

    assert artifact["shortlist_count"] == 2
    companies = sorted(str(row.get("company")) for row in artifact["shortlist"])
    assert companies == ["Acme AI", "Beta Labs"]
    assert artifact["anti_repetition_summary"]["rejected_summary"]["per_company_cap"] >= 1


def test_jobs_shortlist_v1_previously_seen_never_shortlisted_can_resurface(monkeypatch, tmp_path) -> None:
    db = _sqlite_session(tmp_path)

    def _upstream(score: int) -> dict:
        return {
            "artifact_type": "jobs_scored.v1",
            "jobs_scored": [
                {
                    "job_id": "j1",
                    "canonical_job_key": "job:acme|software engineer|remote",
                    "title": "Software Engineer",
                    "company": "Acme",
                    "source": "linkedin",
                    "location": "Remote",
                    "source_url": "https://example.test/jobs/1",
                    "overall_score": score,
                    "score": score / 50.0,
                }
            ],
        }

    monkeypatch.setattr(
        jobs_shortlist_v1,
        "fetch_upstream_result_content_json",
        lambda db, upstream: _upstream(70),
    )
    first = jobs_shortlist_v1.execute(
        _task(
            {
                "pipeline_id": "pipe-short-history-1",
                "upstream": {"task_id": "rank-task", "run_id": "rank-run", "task_type": "jobs_rank_v1"},
                "request": {"shortlist_max_items": 1, "shortlist_min_score": 99},
            },
            run_id="run-short-history-1",
        ),
        db=db,
    )
    assert first["content_json"]["shortlist_count"] == 0

    monkeypatch.setattr(
        jobs_shortlist_v1,
        "fetch_upstream_result_content_json",
        lambda db, upstream: _upstream(92),
    )
    second = jobs_shortlist_v1.execute(
        _task(
            {
                "pipeline_id": "pipe-short-history-2",
                "upstream": {"task_id": "rank-task", "run_id": "rank-run", "task_type": "jobs_rank_v1"},
                "request": {"shortlist_max_items": 1, "shortlist_min_score": 0.1},
            },
            run_id="run-short-history-2",
        ),
        db=db,
    )

    top = second["content_json"]["shortlist"][0]
    assert top["newly_discovered"] is False
    assert top["resurfaced_from_prior_runs"] is True
    assert top["previously_shortlisted"] is False
    assert top["suppressed_due_to_cooldown"] is False
    assert second["content_json"]["history_observability"]["seen_before_count"] == 1


def test_jobs_shortlist_v1_recently_notified_jobs_are_suppressed_during_cooldown(monkeypatch, tmp_path) -> None:
    db = _sqlite_session(tmp_path)
    seed_job = {
        "canonical_job_key": "job:acme|software engineer|remote",
        "title": "Software Engineer",
        "company": "Acme",
        "source": "linkedin",
        "source_url": "https://example.test/jobs/1",
    }
    now_utc = datetime.now(timezone.utc)
    record_jobs_seen(db, [seed_job], seen_at=now_utc - timedelta(days=1))
    record_jobs_notified(db, [seed_job], notified_at=now_utc)
    db.commit()

    monkeypatch.setattr(
        jobs_shortlist_v1,
        "fetch_upstream_result_content_json",
        lambda db, upstream: {
            "artifact_type": "jobs_scored.v1",
            "jobs_scored": [
                {
                    "job_id": "j1",
                    "canonical_job_key": seed_job["canonical_job_key"],
                    "title": "Software Engineer",
                    "company": "Acme",
                    "source": "linkedin",
                    "location": "Remote",
                    "source_url": seed_job["source_url"],
                    "overall_score": 94,
                    "score": 1.88,
                }
            ],
        },
    )

    result = jobs_shortlist_v1.execute(
        _task(
            {
                "pipeline_id": "pipe-short-cooldown",
                "upstream": {"task_id": "rank-task", "run_id": "rank-run", "task_type": "jobs_rank_v1"},
                "request": {
                    "shortlist_max_items": 1,
                    "shortlist_min_score": 0.1,
                    "jobs_notification_cooldown_days": 7,
                },
            },
            run_id="run-short-cooldown",
        ),
        db=db,
    )

    artifact = result["content_json"]
    assert artifact["shortlist_count"] == 0
    assert artifact["rejected_summary"]["notification_cooldown"] == 1
    assert artifact["history_observability"]["cooldown_suppressed_count"] == 1


def test_jobs_shortlist_v1_old_notified_jobs_can_reappear_after_cooldown(monkeypatch, tmp_path) -> None:
    db = _sqlite_session(tmp_path)
    seed_job = {
        "canonical_job_key": "job:beta|data engineer|remote",
        "title": "Data Engineer",
        "company": "Beta",
        "source": "indeed",
        "source_url": "https://example.test/jobs/2",
    }
    now_utc = datetime.now(timezone.utc)
    record_jobs_seen(db, [seed_job], seen_at=now_utc - timedelta(days=10))
    record_jobs_notified(db, [seed_job], notified_at=now_utc - timedelta(days=10))
    db.commit()

    monkeypatch.setattr(
        jobs_shortlist_v1,
        "fetch_upstream_result_content_json",
        lambda db, upstream: {
            "artifact_type": "jobs_scored.v1",
            "jobs_scored": [
                {
                    "job_id": "j2",
                    "canonical_job_key": seed_job["canonical_job_key"],
                    "title": "Data Engineer",
                    "company": "Beta",
                    "source": "indeed",
                    "location": "Remote",
                    "source_url": seed_job["source_url"],
                    "overall_score": 93,
                    "score": 1.86,
                }
            ],
        },
    )

    result = jobs_shortlist_v1.execute(
        _task(
            {
                "pipeline_id": "pipe-short-post-cooldown",
                "upstream": {"task_id": "rank-task", "run_id": "rank-run", "task_type": "jobs_rank_v1"},
                "request": {
                    "shortlist_max_items": 1,
                    "shortlist_min_score": 0.1,
                    "jobs_notification_cooldown_days": 3,
                },
            },
            run_id="run-short-post-cooldown",
        ),
        db=db,
    )

    top = result["content_json"]["shortlist"][0]
    assert top["previously_notified"] is True
    assert top["suppressed_due_to_cooldown"] is False
    assert top["resurfaced_from_prior_runs"] is True


def test_jobs_shortlist_v1_normalizes_history_datetimes_for_json(monkeypatch, tmp_path) -> None:
    db = _sqlite_session(tmp_path)
    now_utc = datetime(2026, 3, 24, 19, 29, 35, tzinfo=timezone.utc)

    monkeypatch.setattr(
        jobs_shortlist_v1,
        "fetch_upstream_result_content_json",
        lambda db, upstream: {
            "artifact_type": "jobs_scored.v1",
            "jobs_scored": [
                {
                    "job_id": "j1",
                    "canonical_job_key": "job:acme|software engineer|remote",
                    "title": "Software Engineer",
                    "company": "Acme",
                    "source": "linkedin",
                    "location": "Remote",
                    "source_url": "https://example.test/jobs/1",
                    "overall_score": 94,
                    "score": 1.88,
                }
            ],
        },
    )
    monkeypatch.setattr(
        jobs_shortlist_v1,
        "load_jobs_history",
        lambda db, keys: {
            "job:acme|software engineer|remote": {
                "canonical_job_key": "job:acme|software engineer|remote",
                "first_seen_at": now_utc - timedelta(days=7),
                "last_seen_at": now_utc - timedelta(days=1),
                "times_seen": 2,
                "times_shortlisted": 1,
                "times_notified": 0,
                "last_shortlisted_at": now_utc - timedelta(days=2),
                "last_notified_at": now_utc - timedelta(hours=12),
            }
        },
    )

    result = jobs_shortlist_v1.execute(
        _task(
            {
                "pipeline_id": "pipe-short-history-json",
                "upstream": {"task_id": "rank-task", "run_id": "rank-run", "task_type": "jobs_rank_v1"},
                "request": {
                    "shortlist_max_items": 1,
                    "shortlist_min_score": 0.1,
                    "jobs_notification_cooldown_days": 7,
                },
            },
            run_id="run-short-history-json",
        ),
        db=db,
    )

    artifact = result["content_json"]
    top = artifact["shortlist"][0]
    assert top["history_first_seen_at"] == "2026-03-17T19:29:35Z"
    assert top["history_last_seen_at"] == "2026-03-23T19:29:35Z"
    assert top["history_last_shortlisted_at"] == "2026-03-22T19:29:35Z"
    assert top["history_last_notified_at"] == "2026-03-24T07:29:35Z"
    assert top["historical_state"]["last_notified_at"] == "2026-03-24T07:29:35Z"
    json.dumps(artifact)
