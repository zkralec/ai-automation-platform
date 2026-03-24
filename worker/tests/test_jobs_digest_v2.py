import json
import os
import sys
from types import SimpleNamespace

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "worker"))

from task_handlers import jobs_digest_v2


def _task(payload: dict, *, task_id: str = "task-digest-1", run_id: str = "run-digest-1", model: str = "gpt-5") -> SimpleNamespace:
    return SimpleNamespace(
        id=task_id,
        model=model,
        max_attempts=3,
        _run_id=run_id,
        payload_json=json.dumps(payload, separators=(",", ":"), ensure_ascii=True),
    )


def test_jobs_digest_v2_accepts_jobs_top_and_generates_llm_artifacts(monkeypatch) -> None:
    monkeypatch.setenv("USE_LLM", "true")
    monkeypatch.setattr(
        jobs_digest_v2,
        "fetch_upstream_result_content_json",
        lambda db, upstream: {
            "artifact_type": "jobs_top.v1",
            "top_jobs": [
                {
                    "job_id": "j-1",
                    "title": "Senior ML Engineer",
                    "company": "Acme",
                    "location": "Remote",
                    "source": "linkedin",
                    "source_url": "https://linkedin.example/jobs/1",
                    "salary_min": 180000,
                    "salary_max": 220000,
                    "explanation_summary": "Direct match on production ML systems.",
                },
                {
                    "job_id": "j-2",
                    "title": "Machine Learning Engineer",
                    "company": "Beta Labs",
                    "location": "New York, NY",
                    "source": "indeed",
                    "source_url": "https://indeed.example/jobs/2",
                    "salary_text": "USD 170,000 - 200,000",
                    "explanation_summary": "Strong fit with applied ML background.",
                },
            ],
            "pipeline_counts": {"collected_count": 44, "deduped_count": 19, "scored_count": 12},
        },
    )

    def _fake_llm(**kwargs):
        del kwargs
        return {
            "output_text": json.dumps(
                {
                    "executive_summary": {
                        "summary_text": "Strong senior ML alignment with remote-friendly options.",
                        "strongest_patterns": [
                            "Most top roles focus on production ML systems.",
                            "Remote/hybrid flexibility appears in top-ranked jobs.",
                        ],
                        "best_fit_roles": ["Senior ML Engineer", "Machine Learning Engineer"],
                    },
                    "jobs": [
                        {
                            "job_id": "j-1",
                            "rank": 1,
                            "title": "Senior ML Engineer",
                            "company": "Acme",
                            "location": "Remote",
                            "salary": "USD 180,000 - 220,000",
                            "source": "linkedin",
                            "source_url": "https://linkedin.example/jobs/1",
                            "why_it_fits": "Excellent title and domain alignment with profile strengths.",
                            "tradeoffs": "Scope may be heavily platform-focused versus research-oriented.",
                        },
                        {
                            "job_id": "j-2",
                            "rank": 2,
                            "title": "Machine Learning Engineer",
                            "company": "Beta Labs",
                            "location": "New York, NY",
                            "salary": "USD 170,000 - 200,000",
                            "source": "indeed",
                            "source_url": "https://indeed.example/jobs/2",
                            "why_it_fits": "Strong applied ML overlap and competitive compensation.",
                            "tradeoffs": "Location flexibility may be lower than fully remote roles.",
                        },
                    ],
                    "notification_excerpt": "Top picks: Senior ML Engineer @ Acme, Machine Learning Engineer @ Beta Labs.",
                },
                separators=(",", ":"),
                ensure_ascii=True,
            ),
            "tokens_in": 900,
            "tokens_out": 500,
            "cost_usd": "0.00420000",
            "openai_request_id": "req-digest-1",
        }

    monkeypatch.setattr(jobs_digest_v2, "run_chat_completion", _fake_llm)

    payload = {
        "pipeline_id": "pipe-digest-llm",
        "upstream": {"task_id": "short-task", "run_id": "short-run", "task_type": "jobs_shortlist_v1"},
        "request": {"notify_on_empty": False},
        "digest_policy": {"llm_enabled": True, "max_items": 5, "artifact_base_url": "http://localhost:8000"},
    }
    result = jobs_digest_v2.execute(_task(payload), db=object())
    artifact = result["content_json"]

    assert artifact["artifact_type"] == "jobs.digest.v2"
    assert artifact["generation_mode"] == "llm_structured"
    assert artifact["jobs_digest_json_artifact"]["artifact_type"] == "jobs_digest.json.v1"
    assert artifact["jobs_digest_md_artifact"]["artifact_type"] == "jobs_digest.md.v1"
    assert artifact["jobs_digest_json_artifact"]["executive_summary"]["collected_count"] == 44
    assert artifact["jobs_digest_json_artifact"]["executive_summary"]["deduped_count"] == 19
    assert len(artifact["jobs_digest_json_artifact"]["jobs"]) == 2
    assert artifact["model_usage"]["runtime_enabled"] is True
    assert result["usage"]["tokens_in"] == 900
    assert result["usage"]["tokens_out"] == 500
    assert result["usage"]["cost_usd"] == "0.00420000"
    assert result["usage"]["openai_request_ids"] == ["req-digest-1"]
    assert result["usage"]["ai_usage_task_run_ids"] == ["task-digest-1:run-digest-1:jobs_digest_v2_1"]
    debug_payload = result["debug_json"]
    assert debug_payload["notify_decision"]["should_notify"] is True
    assert debug_payload["notify_followup_requested"] is True
    assert debug_payload["notify_followup_spec"]["task_type"] == "notify_v1"
    assert result["next_tasks"][0]["task_type"] == "notify_v1"
    notify_payload = result["next_tasks"][0]["payload_json"]
    assert "Title: Senior ML Engineer" in notify_payload["message"]
    assert "Company: Acme" in notify_payload["message"]
    assert "Salary: USD 180,000 - 220,000" in notify_payload["message"]
    assert "Link: <https://linkedin.example/jobs/1>" in notify_payload["message"]
    assert "Title: Machine Learning Engineer" in notify_payload["message"]
    assert "task=task-digest-1" not in notify_payload["message"]
    refs = notify_payload["metadata"]["artifact_references"]
    assert refs["task_id"] == "task-digest-1"
    assert refs["run_id"] == "run-digest-1"
    assert refs["result_url"] == "http://localhost:8000/tasks/task-digest-1/result"


def test_jobs_digest_v2_retries_and_falls_back_by_default_when_strict_unspecified(monkeypatch) -> None:
    monkeypatch.setenv("USE_LLM", "true")
    monkeypatch.setattr(
        jobs_digest_v2,
        "fetch_upstream_result_content_json",
        lambda db, upstream: {
            "artifact_type": "jobs.shortlist.v1",
            "jobs_top_artifact": {
                "artifact_type": "jobs_top.v1",
                "top_jobs": [
                    {
                        "job_id": "j-1",
                        "title": "AI Engineer",
                        "company": "Acme",
                        "location": "Remote",
                        "source": "linkedin",
                        "source_url": "https://linkedin.example/jobs/1",
                        "explanation_summary": "Strong fit.",
                    }
                ],
                "pipeline_counts": {"collected_count": 30, "deduped_count": 12, "scored_count": 8},
            },
        },
    )

    calls = {"count": 0}

    def _bad_llm(**kwargs):
        del kwargs
        calls["count"] += 1
        return {
            "output_text": "not-json",
            "tokens_in": 100,
            "tokens_out": 20,
            "cost_usd": "0.00040000",
            "openai_request_id": f"req-bad-{calls['count']}",
        }

    monkeypatch.setattr(jobs_digest_v2, "run_chat_completion", _bad_llm)

    payload = {
        "pipeline_id": "pipe-digest-fallback",
        "upstream": {"task_id": "short-task", "run_id": "short-run", "task_type": "jobs_shortlist_v1"},
        "request": {"notify_on_empty": False},
        "digest_policy": {
            "llm_enabled": True,
            "llm_max_retries": 2,
        },
    }
    result = jobs_digest_v2.execute(_task(payload), db=object())
    artifact = result["content_json"]

    assert calls["count"] == 2
    assert artifact["generation_mode"] == "deterministic_fallback"
    assert artifact["jobs_digest_json_artifact"]["executive_summary"]["collected_count"] == 30
    assert artifact["notify_decision"]["should_notify"] is True
    assert any("llm_digest_failed" in row for row in artifact["warnings"])
    assert result["usage"]["tokens_in"] == 200
    assert result["usage"]["tokens_out"] == 40
    assert result["usage"]["cost_usd"] == "0.00080000"
    assert result["usage"]["ai_usage_task_run_ids"] == [
        "task-digest-1:run-digest-1:jobs_digest_v2_1",
        "task-digest-1:run-digest-1:jobs_digest_v2_2",
    ]
    assert result["debug_json"]["fallback_used"] is True
    assert result["debug_json"]["strict_llm_output"] is False
    notify_payload = result["next_tasks"][0]["payload_json"]
    assert "Title: AI Engineer" in notify_payload["message"]
    assert "Company: Acme" in notify_payload["message"]
    assert "Salary: Not listed" in notify_payload["message"]
    assert "Link: <https://linkedin.example/jobs/1>" in notify_payload["message"]


def test_jobs_digest_v2_defaults_to_single_retry_for_faster_fallback(monkeypatch) -> None:
    monkeypatch.setenv("USE_LLM", "true")
    monkeypatch.delenv("JOBS_DIGEST_LLM_MAX_RETRIES_DEFAULT", raising=False)
    monkeypatch.setattr(
        jobs_digest_v2,
        "fetch_upstream_result_content_json",
        lambda db, upstream: {
            "artifact_type": "jobs.shortlist.v1",
            "jobs_top_artifact": {
                "artifact_type": "jobs_top.v1",
                "top_jobs": [
                    {
                        "job_id": "j-1",
                        "title": "AI Engineer",
                        "company": "Acme",
                        "location": "Remote",
                        "source": "linkedin",
                        "source_url": "https://linkedin.example/jobs/1",
                        "explanation_summary": "Strong fit.",
                    }
                ],
                "pipeline_counts": {"collected_count": 30, "deduped_count": 12, "scored_count": 8},
            },
        },
    )

    calls = {"count": 0}

    def _bad_llm(**kwargs):
        del kwargs
        calls["count"] += 1
        return {
            "output_text": "not-json",
            "tokens_in": 100,
            "tokens_out": 20,
            "cost_usd": "0.00040000",
            "openai_request_id": f"req-bad-{calls['count']}",
        }

    monkeypatch.setattr(jobs_digest_v2, "run_chat_completion", _bad_llm)

    payload = {
        "pipeline_id": "pipe-digest-default-fast-fallback",
        "upstream": {"task_id": "short-task", "run_id": "short-run", "task_type": "jobs_shortlist_v1"},
        "request": {"notify_on_empty": False},
        "digest_policy": {"llm_enabled": True},
    }
    result = jobs_digest_v2.execute(_task(payload), db=object())
    artifact = result["content_json"]

    assert calls["count"] == 1
    assert artifact["generation_mode"] == "deterministic_fallback"
    assert artifact["digest_policy"]["llm_max_retries"] == 1
    assert any("llm_digest_failed" in row for row in artifact["warnings"])


def test_jobs_digest_v2_raises_when_strict_llm_output_and_malformed(monkeypatch) -> None:
    monkeypatch.setenv("USE_LLM", "true")
    monkeypatch.setattr(
        jobs_digest_v2,
        "fetch_upstream_result_content_json",
        lambda db, upstream: {
            "artifact_type": "jobs_top.v1",
            "top_jobs": [
                {
                    "job_id": "j-1",
                    "title": "AI Engineer",
                    "company": "Acme",
                    "location": "Remote",
                    "source": "linkedin",
                    "source_url": "https://linkedin.example/jobs/1",
                }
            ],
            "pipeline_counts": {"collected_count": 20, "deduped_count": 8},
        },
    )

    calls = {"count": 0}

    def _bad_llm(**kwargs):
        del kwargs
        calls["count"] += 1
        return {
            "output_text": "{}",
            "tokens_in": 50,
            "tokens_out": 20,
            "cost_usd": "0.00030000",
            "openai_request_id": f"req-strict-{calls['count']}",
        }

    monkeypatch.setattr(jobs_digest_v2, "run_chat_completion", _bad_llm)

    payload = {
        "pipeline_id": "pipe-digest-strict",
        "upstream": {"task_id": "short-task", "run_id": "short-run", "task_type": "jobs_shortlist_v1"},
        "request": {"notify_on_empty": False},
        "digest_policy": {
            "llm_enabled": True,
            "llm_max_retries": 2,
            "strict_llm_output": True,
        },
    }

    with pytest.raises(RuntimeError) as exc_info:
        jobs_digest_v2.execute(_task(payload), db=object())
    assert calls["count"] == 2
    assert "strict_llm_output=true" in str(exc_info.value)
    usage = getattr(exc_info.value, "usage", {})
    assert usage.get("tokens_in") == 100
    assert usage.get("tokens_out") == 40
    assert usage.get("cost_usd") == "0.00060000"
    assert usage.get("ai_usage_task_run_ids") == [
        "task-digest-1:run-digest-1:jobs_digest_v2_1",
        "task-digest-1:run-digest-1:jobs_digest_v2_2",
    ]


def test_jobs_digest_v2_malformed_then_valid_retry_succeeds(monkeypatch) -> None:
    monkeypatch.setenv("USE_LLM", "true")
    monkeypatch.setattr(
        jobs_digest_v2,
        "fetch_upstream_result_content_json",
        lambda db, upstream: {
            "artifact_type": "jobs_top.v1",
            "top_jobs": [
                {
                    "job_id": "j-1",
                    "title": "AI Engineer",
                    "company": "Acme",
                    "location": "Remote",
                    "source": "linkedin",
                    "source_url": "https://linkedin.example/jobs/1",
                    "explanation_summary": "Strong fit.",
                }
            ],
            "pipeline_counts": {"collected_count": 20, "deduped_count": 8},
        },
    )
    calls = {"count": 0}

    def _flaky_llm(**kwargs):
        del kwargs
        calls["count"] += 1
        if calls["count"] == 1:
            return {
                "output_text": "not-json",
                "tokens_in": 50,
                "tokens_out": 20,
                "cost_usd": "0.00030000",
                "openai_request_id": "req-digest-malformed",
            }
        return {
            "output_text": json.dumps(
                {
                    "executive_summary": {
                        "summary_text": "Good shortlist quality.",
                        "strongest_patterns": ["Strong title match."],
                        "best_fit_roles": ["AI Engineer"],
                    },
                    "jobs": [
                        {
                            "job_id": "j-1",
                            "rank": 1,
                            "title": "AI Engineer",
                            "company": "Acme",
                            "location": "Remote",
                            "salary": "Not listed",
                            "source": "linkedin",
                            "source_url": "https://linkedin.example/jobs/1",
                            "why_it_fits": "Strong title and location fit.",
                            "tradeoffs": "Salary transparency is limited.",
                        }
                    ],
                    "notification_excerpt": "Top pick: AI Engineer @ Acme.",
                },
                separators=(",", ":"),
                ensure_ascii=True,
            ),
            "tokens_in": 50,
            "tokens_out": 20,
            "cost_usd": "0.00030000",
            "openai_request_id": "req-digest-valid",
        }

    monkeypatch.setattr(jobs_digest_v2, "run_chat_completion", _flaky_llm)

    payload = {
        "pipeline_id": "pipe-digest-retry-success",
        "upstream": {"task_id": "short-task", "run_id": "short-run", "task_type": "jobs_shortlist_v1"},
        "request": {"notify_on_empty": False},
        "digest_policy": {"llm_enabled": True, "llm_max_retries": 3, "strict_llm_output": True},
    }
    result = jobs_digest_v2.execute(_task(payload), db=object())

    assert calls["count"] == 2
    assert result["content_json"]["generation_mode"] == "llm_structured"
    assert result["usage"]["tokens_in"] == 100
    assert result["usage"]["tokens_out"] == 40
    assert result["usage"]["cost_usd"] == "0.00060000"
    assert result["debug_json"]["fallback_used"] is False


def test_jobs_digest_v2_schema_missing_field_then_valid_retry_succeeds(monkeypatch) -> None:
    monkeypatch.setenv("USE_LLM", "true")
    monkeypatch.setattr(
        jobs_digest_v2,
        "fetch_upstream_result_content_json",
        lambda db, upstream: {
            "artifact_type": "jobs_top.v1",
            "top_jobs": [
                {
                    "job_id": "j-1",
                    "title": "AI Engineer",
                    "company": "Acme",
                    "location": "Remote",
                    "source": "linkedin",
                    "source_url": "https://linkedin.example/jobs/1",
                    "explanation_summary": "Strong fit.",
                }
            ],
            "pipeline_counts": {"collected_count": 20, "deduped_count": 8},
        },
    )
    calls = {"count": 0}

    def _schema_flaky_llm(**kwargs):
        del kwargs
        calls["count"] += 1
        if calls["count"] == 1:
            return {
                "output_text": json.dumps(
                    {
                        "executive_summary": {
                            "summary_text": "Good shortlist quality.",
                            "strongest_patterns": ["Strong title match."],
                            "best_fit_roles": ["AI Engineer"],
                        },
                        "jobs": [
                            {
                                "job_id": "j-1",
                                "rank": 1,
                                "title": "AI Engineer",
                                "company": "Acme",
                                "location": "Remote",
                                "salary": "Not listed",
                                "source": "linkedin",
                                "source_url": "https://linkedin.example/jobs/1",
                                "why_it_fits": "Strong title and location fit.",
                                "tradeoffs": "Salary transparency is limited.",
                            }
                        ],
                    },
                    separators=(",", ":"),
                    ensure_ascii=True,
                ),
                "tokens_in": 50,
                "tokens_out": 20,
                "cost_usd": "0.00030000",
                "openai_request_id": "req-digest-schema-missing",
            }
        return {
            "output_text": json.dumps(
                {
                    "executive_summary": {
                        "summary_text": "Good shortlist quality.",
                        "strongest_patterns": ["Strong title match."],
                        "best_fit_roles": ["AI Engineer"],
                    },
                    "jobs": [
                        {
                            "job_id": "j-1",
                            "rank": 1,
                            "title": "AI Engineer",
                            "company": "Acme",
                            "location": "Remote",
                            "salary": "Not listed",
                            "source": "linkedin",
                            "source_url": "https://linkedin.example/jobs/1",
                            "why_it_fits": "Strong title and location fit.",
                            "tradeoffs": "Salary transparency is limited.",
                        }
                    ],
                    "notification_excerpt": "Top pick: AI Engineer @ Acme.",
                },
                separators=(",", ":"),
                ensure_ascii=True,
            ),
            "tokens_in": 50,
            "tokens_out": 20,
            "cost_usd": "0.00030000",
            "openai_request_id": "req-digest-schema-valid",
        }

    monkeypatch.setattr(jobs_digest_v2, "run_chat_completion", _schema_flaky_llm)

    payload = {
        "pipeline_id": "pipe-digest-schema-retry-success",
        "upstream": {"task_id": "short-task", "run_id": "short-run", "task_type": "jobs_shortlist_v1"},
        "request": {"notify_on_empty": False},
        "digest_policy": {"llm_enabled": True, "llm_max_retries": 3, "strict_llm_output": True},
    }
    result = jobs_digest_v2.execute(_task(payload), db=object())

    assert calls["count"] == 2
    assert result["content_json"]["generation_mode"] == "llm_structured"


def test_jobs_digest_v2_cleans_placeholder_company_and_prefers_direct_job_link(monkeypatch) -> None:
    monkeypatch.setenv("USE_LLM", "false")
    monkeypatch.setattr(
        jobs_digest_v2,
        "fetch_upstream_result_content_json",
        lambda db, upstream: {
            "artifact_type": "jobs_top.v1",
            "top_jobs": [
                {
                    "job_id": "j-1",
                    "title": "Senior Software Engineer",
                    "company": "Unknown company",
                    "location": "",
                    "source": "indeed",
                    "source_url": "https://www.indeed.com/jobs?q=senior+software+engineer",
                    "url": "https://www.indeed.com/viewjob?jk=123",
                    "posted_at": "2026-03-20T00:00:00Z",
                    "explanation_summary": "Strong backend fit.",
                    "metadata_quality_score": 52,
                }
            ],
            "pipeline_counts": {"collected_count": 18, "deduped_count": 9, "scored_count": 4},
        },
    )

    payload = {
        "pipeline_id": "pipe-digest-clean-links",
        "upstream": {"task_id": "short-task", "run_id": "short-run", "task_type": "jobs_shortlist_v1"},
        "request": {"notify_on_empty": False},
        "digest_policy": {"llm_enabled": False},
    }
    result = jobs_digest_v2.execute(_task(payload), db=object())
    artifact = result["content_json"]
    digest_job = artifact["jobs_digest_json_artifact"]["jobs"][0]
    message = result["next_tasks"][0]["payload_json"]["message"]
    markdown = artifact["jobs_digest_md_artifact"]["content"]

    assert digest_job["company"] == ""
    assert digest_job["source_url"] == "https://www.indeed.com/viewjob?jk=123"
    assert str(digest_job["posted_display"]).startswith("Posted ")
    assert "Unknown company" not in message
    assert "Title: Senior Software Engineer" in message
    assert "Company: Not listed" in message
    assert "Salary: Not listed" in message
    assert "Link: <https://www.indeed.com/viewjob?jk=123>" in message
    assert "Unknown company" not in markdown


def test_jobs_digest_v2_fast_fallback_after_repeated_malformed_output(monkeypatch) -> None:
    monkeypatch.setenv("USE_LLM", "true")
    monkeypatch.setattr(
        jobs_digest_v2,
        "fetch_upstream_result_content_json",
        lambda db, upstream: {
            "artifact_type": "jobs_top.v1",
            "top_jobs": [
                {
                    "job_id": "j-1",
                    "title": "AI Engineer",
                    "company": "Acme",
                    "location": "Remote",
                    "source": "linkedin",
                    "source_url": "https://linkedin.example/jobs/1",
                    "explanation_summary": "Strong fit.",
                }
            ],
            "pipeline_counts": {"collected_count": 20, "deduped_count": 8},
        },
    )
    calls = {"count": 0}

    def _always_bad_llm(**kwargs):
        del kwargs
        calls["count"] += 1
        return {
            "output_text": "not-json",
            "tokens_in": 50,
            "tokens_out": 20,
            "cost_usd": "0.00030000",
            "openai_request_id": f"req-digest-fast-fallback-{calls['count']}",
        }

    monkeypatch.setattr(jobs_digest_v2, "run_chat_completion", _always_bad_llm)

    payload = {
        "pipeline_id": "pipe-digest-fast-fallback",
        "upstream": {"task_id": "short-task", "run_id": "short-run", "task_type": "jobs_shortlist_v1"},
        "request": {"notify_on_empty": False},
        "digest_policy": {"llm_enabled": True, "llm_max_retries": 4, "strict_llm_output": False},
    }
    result = jobs_digest_v2.execute(_task(payload), db=object())
    artifact = result["content_json"]

    assert calls["count"] == 2
    assert artifact["generation_mode"] == "deterministic_fallback"
    warnings = artifact.get("warnings") or []
    assert any("llm_digest_stop_reason:fast_fail_repeated_output_pattern" in row for row in warnings)
    assert result["debug_json"]["fallback_used"] is True


def test_jobs_digest_v2_empty_shortlist_skips_notify_and_keeps_artifacts(monkeypatch) -> None:
    monkeypatch.setenv("USE_LLM", "true")
    monkeypatch.setattr(
        jobs_digest_v2,
        "fetch_upstream_result_content_json",
        lambda db, upstream: {
            "artifact_type": "jobs.shortlist.v1",
            "jobs_top_artifact": {
                "artifact_type": "jobs_top.v1",
                "top_jobs": [],
                "pipeline_counts": {"collected_count": 18, "deduped_count": 7, "scored_count": 4},
            },
            "pipeline_counts": {"collected_count": 18, "deduped_count": 7, "scored_count": 4},
        },
    )

    payload = {
        "pipeline_id": "pipe-digest-empty",
        "upstream": {"task_id": "short-task", "run_id": "short-run", "task_type": "jobs_shortlist_v1"},
        "request": {"notify_on_empty": False},
        "digest_policy": {"llm_enabled": True, "strict_llm_output": True},
    }
    result = jobs_digest_v2.execute(_task(payload), db=object())
    artifact = result["content_json"]

    assert artifact["artifact_type"] == "jobs.digest.v2"
    assert artifact["generation_mode"] == "deterministic_fallback"
    assert artifact["top_jobs"] == []
    assert artifact["jobs_digest_json_artifact"]["jobs"] == []
    assert artifact["notify_decision"]["should_notify"] is False
    debug_payload = result["debug_json"]
    assert debug_payload["notify_decision"]["reason"] == "skipped_empty_shortlist"
    assert debug_payload["notify_followup_requested"] is False
    assert debug_payload["notify_followup_spec"] is None
    assert result["next_tasks"] == []


def test_jobs_digest_v2_preserves_fail_soft_shortlist_context_in_notify_metadata(monkeypatch) -> None:
    monkeypatch.setenv("USE_LLM", "false")
    monkeypatch.setattr(
        jobs_digest_v2,
        "fetch_upstream_result_content_json",
        lambda db, upstream: {
            "artifact_type": "jobs.shortlist.v1",
            "ranking_mode": "deterministic_fallback",
            "fallback_used": True,
            "llm_failed": True,
            "shortlist_confidence": "low",
            "fail_soft_applied": True,
            "shortlist": [
                {
                    "job_id": "j-1",
                    "canonical_job_key": "job:acme|software engineer|remote",
                    "title": "Software Engineer",
                    "company": "Acme",
                    "location": "Remote",
                    "source": "linkedin",
                    "source_url": "https://linkedin.example/jobs/1",
                    "explanation_summary": "Fallback shortlist candidate.",
                }
            ],
            "jobs_top_artifact": {
                "artifact_type": "jobs_top.v1",
                "top_jobs": [
                    {
                        "job_id": "j-1",
                        "canonical_job_key": "job:acme|software engineer|remote",
                        "title": "Software Engineer",
                        "company": "Acme",
                        "location": "Remote",
                        "source": "linkedin",
                        "source_url": "https://linkedin.example/jobs/1",
                        "explanation_summary": "Fallback shortlist candidate.",
                    }
                ],
                "pipeline_counts": {"collected_count": 30, "deduped_count": 12, "scored_count": 8},
                "ranking_mode": "deterministic_fallback",
                "fallback_used": True,
                "llm_failed": True,
                "shortlist_confidence": "low",
                "fail_soft_applied": True,
            },
            "pipeline_counts": {"collected_count": 30, "deduped_count": 12, "scored_count": 8},
        },
    )

    payload = {
        "pipeline_id": "pipe-digest-fail-soft",
        "upstream": {"task_id": "short-task", "run_id": "short-run", "task_type": "jobs_shortlist_v1"},
        "request": {"notify_on_empty": False},
        "digest_policy": {"llm_enabled": True, "max_items": 5},
    }
    result = jobs_digest_v2.execute(_task(payload), db=object())
    artifact = result["content_json"]

    assert artifact["notify_decision"]["should_notify"] is True
    assert artifact["shortlist_context"]["ranking_mode"] == "deterministic_fallback"
    assert artifact["shortlist_context"]["fallback_used"] is True
    assert artifact["shortlist_context"]["fail_soft_applied"] is True
    notify_payload = result["next_tasks"][0]["payload_json"]
    assert notify_payload["metadata"]["ranking_mode"] == "deterministic_fallback"
    assert notify_payload["metadata"]["fallback_used"] is True
    assert notify_payload["metadata"]["shortlist_confidence"] == "low"
    assert notify_payload["metadata"]["fail_soft_applied"] is True


def test_jobs_digest_v2_fallback_output_shape_is_ui_stable(monkeypatch, jobs_v2_samples) -> None:
    monkeypatch.setenv("USE_LLM", "false")
    monkeypatch.setattr(
        jobs_digest_v2,
        "fetch_upstream_result_content_json",
        lambda db, upstream: {
            "artifact_type": "jobs_top.v1",
            "top_jobs": [dict(row) for row in jobs_v2_samples["digest_top_jobs_sample"]],
            "pipeline_counts": {"collected_count": 40, "deduped_count": 19, "scored_count": 12},
        },
    )

    payload = {
        "pipeline_id": "pipe-digest-shape",
        "upstream": {"task_id": "short-task", "run_id": "short-run", "task_type": "jobs_shortlist_v1"},
        "request": {"notify_on_empty": False},
        "digest_policy": {"llm_enabled": True, "max_items": 5},
    }
    result = jobs_digest_v2.execute(_task(payload), db=object())
    jobs = result["content_json"]["jobs_digest_json_artifact"]["jobs"]

    assert len(jobs) == 2
    required_keys = {"rank", "title", "company", "location", "salary", "source", "source_url", "why_it_fits", "tradeoffs"}
    for row in jobs:
        assert required_keys.issubset(row.keys())
