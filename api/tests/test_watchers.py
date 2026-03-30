import json
import sys
import uuid

import pytest

sys.path.insert(0, "/app")

from fastapi.testclient import TestClient

from main import (
    app,
    Artifact,
    Base,
    Run,
    RunStatus,
    SessionLocal,
    Task,
    TaskStatus,
    engine,
    now_utc,
)

client = TestClient(app)


@pytest.fixture(autouse=True)
def _ensure_main_tables() -> None:
    Base.metadata.create_all(bind=engine)
    yield


def _cleanup_watcher_and_task(watcher_id: str, task_id: str | None = None) -> None:
    client.delete(f"/watchers/{watcher_id}")
    if not task_id:
        return
    with SessionLocal() as db:
        db.query(Artifact).filter(Artifact.task_id == task_id).delete()
        db.query(Run).filter(Run.task_id == task_id).delete()
        db.query(Task).filter(Task.id == task_id).delete()
        db.commit()


def test_create_watcher_normalizes_template_fields() -> None:
    watcher_id = f"watcher-test-{uuid.uuid4()}"
    try:
        response = client.post(
            "/watchers",
            json={
                "id": watcher_id,
                "name": "Deals Watcher",
                "task_type": "deals_scan_v1",
                "payload_json": '{"source":"watcher-test","collectors_enabled":true}',
                "interval_seconds": 180,
                "enabled": True,
                "priority": 15,
                "notification_behavior": {"mode": "notify_on_unicorn", "channel": "discord"},
                "metadata": {"owner": "ops", "watcher_category": "deals"},
            },
        )
        assert response.status_code == 200
        payload = response.json()
        assert payload["id"] == watcher_id
        assert payload["interval_seconds"] == 180
        assert payload["min_interval_seconds"] == 180
        assert payload["notification_behavior"]["mode"] == "notify_on_unicorn"
        assert payload["metadata"]["owner"] == "ops"
        assert "notification_behavior" not in payload["metadata"]

        get_response = client.get(f"/watchers/{watcher_id}")
        assert get_response.status_code == 200
        assert get_response.json()["name"] == "Deals Watcher"
    finally:
        _cleanup_watcher_and_task(watcher_id)


def test_patch_watcher_interval_and_notification_behavior() -> None:
    watcher_id = f"watcher-test-{uuid.uuid4()}"
    try:
        create_response = client.post(
            "/watchers",
            json={
                "id": watcher_id,
                "name": "Jobs Watcher",
                "task_type": "jobs_digest_v1",
                "payload_json": '{"source":"watcher-test-jobs"}',
                "interval_seconds": 300,
            },
        )
        assert create_response.status_code == 200

        patch_response = client.patch(
            f"/watchers/{watcher_id}",
            json={
                "min_interval_seconds": 420,
                "enabled": False,
                "notification_behavior": {"mode": "digest", "channel": "email"},
            },
        )
        assert patch_response.status_code == 200
        patched = patch_response.json()
        assert patched["enabled"] is False
        assert patched["interval_seconds"] == 420
        assert patched["min_interval_seconds"] == 420
        assert patched["notification_behavior"]["mode"] == "digest"
        assert patched["notification_behavior"]["channel"] == "email"
    finally:
        _cleanup_watcher_and_task(watcher_id)


def test_watcher_includes_last_run_and_outcome_summary() -> None:
    watcher_id = f"watcher-test-{uuid.uuid4()}"
    task_id = str(uuid.uuid4())
    run_id = str(uuid.uuid4())
    try:
        create_response = client.post(
            "/watchers",
            json={
                "id": watcher_id,
                "name": "Notify Watcher",
                "task_type": "notify_v1",
                "payload_json": '{"channel":"discord","message":"test"}',
                "interval_seconds": 300,
            },
        )
        assert create_response.status_code == 200

        with SessionLocal() as db:
            task = Task(
                id=task_id,
                created_at=now_utc(),
                updated_at=now_utc(),
                status=TaskStatus.failed,
                task_type="notify_v1",
                payload_json=json.dumps(
                    {
                        "channel": "discord",
                        "message": "watcher run",
                        "planner_template_id": watcher_id,
                    },
                    separators=(",", ":"),
                ),
                model="gpt-4o-mini",
                error="Notification delivery failed",
                max_attempts=3,
            )
            run = Run(
                id=run_id,
                task_id=task_id,
                attempt=1,
                status=RunStatus.failed,
                started_at=now_utc(),
                ended_at=now_utc(),
                wall_time_ms=321,
                error="Webhook timeout",
                created_at=now_utc(),
            )
            artifact = Artifact(
                id=str(uuid.uuid4()),
                task_id=task_id,
                run_id=run_id,
                artifact_type="result.json",
                content_json={"summary": "Delivery failed after timeout", "status": "failed"},
                created_at=now_utc(),
            )
            db.add(task)
            db.add(run)
            db.add(artifact)
            db.commit()

        watcher_response = client.get(f"/watchers/{watcher_id}")
        assert watcher_response.status_code == 200
        payload = watcher_response.json()
        assert payload["last_run_summary"] is not None
        assert payload["last_run_summary"]["task_id"] == task_id
        assert payload["last_run_summary"]["task_status"] == "failed"
        assert payload["last_run_summary"]["run_id"] == run_id
        assert payload["last_outcome_summary"] is not None
        assert payload["last_outcome_summary"]["artifact_type"] == "result.json"
        assert "Delivery failed" in (payload["last_outcome_summary"]["message"] or "")
    finally:
        _cleanup_watcher_and_task(watcher_id, task_id=task_id)


def test_jobs_watcher_includes_compact_workflow_summary() -> None:
    watcher_id = f"watcher-jobs-{uuid.uuid4()}"
    task_ids = [str(uuid.uuid4()) for _ in range(5)]
    run_ids = [str(uuid.uuid4()) for _ in range(5)]
    pipeline_id = f"pipe-{uuid.uuid4()}"
    notify_task_id: str | None = None
    try:
        create_response = client.post(
            "/watchers",
            json={
                "id": watcher_id,
                "name": "Jobs Digest Watcher",
                "task_type": "jobs_collect_v1",
                "payload_json": json.dumps(
                    {
                        "pipeline_id": pipeline_id,
                        "request": {
                            "search_mode": "broad_discovery",
                            "enabled_sources": ["linkedin", "indeed"],
                            "result_limit_per_source": 250,
                        },
                    },
                    separators=(",", ":"),
                ),
                "interval_seconds": 300,
            },
        )
        assert create_response.status_code == 200

        with SessionLocal() as db:
            task_specs = [
                    (
                        task_ids[0],
                        run_ids[0],
                        "jobs_collect_v1",
                        {
                            "pipeline_id": pipeline_id,
                            "planner_template_id": watcher_id,
                            "request": {"enabled_sources": ["linkedin", "indeed"], "search_mode": "broad_discovery"},
                        },
                        {
                            "collection_counts": {
                                "discovered_raw_count": 420,
                                "kept_after_basic_filter_count": 310,
                                "queries_executed_count": 12,
                                "minimum_reached": True,
                            },
                            "collection_observability": {
                                "minimum_targets": {
                                    "minimum_raw_jobs_total_requested": 120,
                                    "minimum_unique_jobs_total_requested": 80,
                                    "minimum_jobs_per_source_requested": 25,
                                    "minimum_reached": True,
                                    "reason_stopped": "minimum_reached",
                                },
                                "operator_questions": {
                                    "searched_enough": "LinkedIn + Indeed active. LinkedIn contributed 220 raw jobs; Indeed contributed 200 raw jobs. 12 queries executed.",
                                    "which_source_is_weak": "Lowest raw contribution came from Indeed.",
                                "why_did_raw_count_collapse": "Basic filtering removed 110 jobs.",
                                "are_we_missing_metadata": "Weakest metadata source: Indeed.",
                            },
                            "by_source": {
                                "linkedin": {
                                    "source_label": "LinkedIn",
                                    "raw_jobs_discovered": 220,
                                    "kept_after_basic_filter": 180,
                                    "jobs_dropped": 40,
                                    "pages_attempted": 6,
                                    "under_target": False,
                                    "suspected_blocking": False,
                                    "missing_rates": {
                                        "missing_company_rate": 2.0,
                                        "missing_posted_at_rate": 6.0,
                                        "missing_source_url_rate": 1.0,
                                        "missing_location_rate": 4.0,
                                    },
                                    "weakness_summary": "post date 6%, location 4%",
                                },
                                "indeed": {
                                    "source_label": "Indeed",
                                    "raw_jobs_discovered": 200,
                                    "kept_after_basic_filter": 130,
                                    "jobs_dropped": 70,
                                    "pages_attempted": 4,
                                    "under_target": True,
                                    "suspected_blocking": False,
                                    "missing_rates": {
                                        "missing_company_rate": 4.0,
                                        "missing_posted_at_rate": 18.0,
                                        "missing_source_url_rate": 12.0,
                                        "missing_location_rate": 7.0,
                                    },
                                    "weakness_summary": "post date 18%, link 12%",
                                },
                            },
                        },
                    },
                ),
                (
                    task_ids[1],
                    run_ids[1],
                    "jobs_normalize_v1",
                    {"pipeline_id": pipeline_id},
                    {
                        "counts": {
                            "deduped_count": 180,
                        }
                    },
                ),
                (
                    task_ids[2],
                    run_ids[2],
                    "jobs_rank_v1",
                    {"pipeline_id": pipeline_id},
                    None,
                ),
                (
                    task_ids[3],
                    run_ids[3],
                    "jobs_shortlist_v1",
                    {"pipeline_id": pipeline_id},
                    {
                        "shortlist_count": 6,
                        "pipeline_counts": {
                            "deduped_count": 180,
                            "shortlisted_count": 6,
                        },
                    },
                ),
                (
                    task_ids[4],
                    run_ids[4],
                    "jobs_digest_v2",
                    {"pipeline_id": pipeline_id},
                    {
                        "summary": "Strong backend and platform fit with several direct-apply options.",
                        "summary_for_ui": {"headline": "Solid senior backend batch with good source diversity."},
                        "top_jobs": [
                            {
                                "title": "Senior Software Engineer",
                                "company": "Acme",
                                "source": "indeed",
                                "source_url": "https://example.com/jobs/123",
                                "posted_display": "Posted 2d ago",
                                "why_it_fits": "Strong backend alignment and direct link.",
                            }
                        ],
                        "why_these": ["Clear backend match", "Strong metadata completeness"],
                        "notify_decision": {"should_notify": True, "reason": "shortlist_non_empty"},
                    },
                ),
            ]

            for task_id, run_id, task_type, payload, artifact_json in task_specs:
                task = Task(
                    id=task_id,
                    created_at=now_utc(),
                    updated_at=now_utc(),
                    status=TaskStatus.success,
                    task_type=task_type,
                    payload_json=json.dumps(payload, separators=(",", ":"), ensure_ascii=True),
                    model=None,
                    error=None,
                    max_attempts=3,
                )
                run = Run(
                    id=run_id,
                    task_id=task_id,
                    attempt=1,
                    status=RunStatus.success,
                    started_at=now_utc(),
                    ended_at=now_utc(),
                    wall_time_ms=120,
                    error=None,
                    created_at=now_utc(),
                )
                db.add(task)
                db.add(run)
                if artifact_json is not None:
                    db.add(
                        Artifact(
                            id=str(uuid.uuid4()),
                            task_id=task_id,
                            run_id=run_id,
                            artifact_type="result.json",
                            content_json=artifact_json,
                            created_at=now_utc(),
                        )
                    )

            notify_task_id = str(uuid.uuid4())
            notify_task = Task(
                id=notify_task_id,
                created_at=now_utc(),
                updated_at=now_utc(),
                status=TaskStatus.success,
                task_type="notify_v1",
                payload_json=json.dumps(
                    {
                        "source_task_type": "jobs_digest_v2",
                        "message": "digest sent",
                        "metadata": {"pipeline_id": pipeline_id},
                    },
                    separators=(",", ":"),
                    ensure_ascii=True,
                ),
                model=None,
                error=None,
                max_attempts=3,
            )
            db.add(notify_task)
            db.commit()

        watcher_response = client.get(f"/watchers/{watcher_id}")
        assert watcher_response.status_code == 200
        payload = watcher_response.json()
        workflow_summary = payload["workflow_summary"]
        assert workflow_summary["kind"] == "jobs_watcher"
        assert workflow_summary["search_mode"] == "broad_discovery"
        assert workflow_summary["active_sources_label"] == "LinkedIn + Indeed active"
        assert workflow_summary["source_contribution_summary"] == [
            "LinkedIn contributed 220 raw jobs",
            "Indeed contributed 200 raw jobs",
        ]
        assert workflow_summary["counts"]["raw_jobs_found"] == 420
        assert workflow_summary["counts"]["jobs_after_filtering"] == 310
        assert workflow_summary["counts"]["jobs_after_dedupe"] == 180
        assert workflow_summary["counts"]["shortlisted_count"] == 6
        assert workflow_summary["counts"]["minimum_reached"] is True
        assert workflow_summary["query_count_used"] == 12
        assert workflow_summary["notify"]["status"] == "sent"
        assert workflow_summary["digest_preview"]["headline"] == "Solid senior backend batch with good source diversity."
        assert workflow_summary["digest_preview"]["top_jobs"][0]["source_url"] == "https://example.com/jobs/123"
        assert workflow_summary["collection_quality"]["minimum_targets"]["reason_stopped"] == "minimum_reached"
        assert workflow_summary["collection_quality"]["by_source"][0]["source_label"] == "LinkedIn"
        assert workflow_summary["collection_quality"]["by_source"][1]["under_target"] is True
        assert workflow_summary["collection_quality"]["by_source"][1]["missing_posted_at_rate"] == 18.0
    finally:
        _cleanup_watcher_and_task(watcher_id, task_id=task_ids[0])
        extra_task_ids = task_ids[1:] + ([notify_task_id] if notify_task_id else [])
        with SessionLocal() as db:
            db.query(Artifact).filter(Artifact.task_id.in_(extra_task_ids)).delete(synchronize_session=False)
            db.query(Run).filter(Run.task_id.in_(extra_task_ids)).delete(synchronize_session=False)
            db.query(Task).filter(Task.id.in_(extra_task_ids)).delete(synchronize_session=False)
            db.commit()
