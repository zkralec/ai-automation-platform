"""
Tests for FastAPI main.py endpoints.
Tests cover model validation, budget enforcement, and request handling.
"""
import sys
import uuid
from datetime import timedelta

sys.path.insert(0, "/app")

import main as main_module
from fastapi.testclient import TestClient
from main import (
    app,
    Base,
    BUDGET_BUFFER_USD,
    DAILY_BUDGET_USD,
    engine,
    SessionLocal,
    Task,
    Run,
    Artifact,
    TaskStatus,
    RunStatus,
    now_utc,
)

client = TestClient(app)


class TestCreateTaskModelValidation:
    """Test POST /tasks endpoint model validation."""

    def test_rejects_invalid_model_override_with_400(self):
        """API rejects invalid model override with HTTP 400."""
        response = client.post(
            "/tasks",
            json={
                "task_type": "jobs_digest",
                "payload_json": '{"jobs": []}',
                "model": "invalid-model-xyz",  # Invalid model
            }
        )
        assert response.status_code == 400
        assert "Invalid model" in response.json()["detail"]
        assert "Available models" in response.json()["detail"]

    def test_accepts_valid_model_gpt4o_mini(self):
        """API accepts valid model 'gpt-4o-mini'."""
        response = client.post(
            "/tasks",
            json={
                "task_type": "jobs_digest",
                "payload_json": '{"jobs": []}',
                "model": "gpt-4o-mini",
            }
        )
        assert response.status_code == 200
        task = response.json()
        assert task["model"] == "gpt-4o-mini"

    def test_accepts_valid_model_gpt4o(self):
        """API accepts valid model 'gpt-4o'."""
        response = client.post(
            "/tasks",
            json={
                "task_type": "jobs_digest",
                "payload_json": '{"jobs": []}',
                "model": "gpt-4o",
            }
        )
        assert response.status_code == 200
        task = response.json()
        assert task["model"] == "gpt-4o"

    def test_accepts_null_model_override(self):
        """API accepts null/None model (uses routing)."""
        response = client.post(
            "/tasks",
            json={
                "task_type": "jobs_digest",
                "payload_json": '{"jobs": []}',
                "model": None,
            }
        )
        assert response.status_code == 200
        task = response.json()
        # Should get a routed model, not null
        assert task["model"] is not None
        assert task["model"] in [
            "gpt-5-nano",
            "gpt-5-mini",
            "gpt-5",
            "gpt-4o-mini",
            "gpt-4o",
            "gpt-4-turbo",
        ]

    def test_error_msg_lists_available_models(self):
        """API error message lists available models."""
        response = client.post(
            "/tasks",
            json={
                "task_type": "jobs_digest",
                "payload_json": '{}',
                "model": "fake-model",
            }
        )
        assert response.status_code == 400
        detail = response.json()["detail"]
        # Should mention each available model
        assert "gpt-4o-mini" in detail
        assert "gpt-4o" in detail
        assert "gpt-4-turbo" in detail

    def test_create_task_queue_failure_schedules_enqueue_recovery(self, monkeypatch):
        def fail_enqueue(*_args, **_kwargs):
            raise ConnectionError("redis unavailable")

        monkeypatch.setattr(main_module.queue, "enqueue", fail_enqueue)

        response = client.post(
            "/tasks",
            json={
                "task_type": "jobs_digest_v1",
                "payload_json": '{"jobs": []}',
            },
        )
        assert response.status_code == 200
        payload = response.json()
        assert payload["status"] == "queued"
        assert payload["error"].startswith("QUEUE_ENQUEUE_ERROR[api]:")
        assert payload["next_run_at"] is not None
        assert payload["diagnostics"]["is_queue_enqueue_failure"] is True
        assert payload["diagnostics"]["upstream_service"] == "redis"


def _seed_task_and_run(task_id: str, run_id: str) -> None:
    Base.metadata.create_all(bind=engine)
    with SessionLocal() as db:
        task = Task(
            id=task_id,
            created_at=now_utc(),
            updated_at=now_utc(),
            status=TaskStatus.success,
            task_type="jobs_digest",
            payload_json='{"jobs":[]}',
            model="gpt-4o-mini",
        )
        run = Run(
            id=run_id,
            task_id=task_id,
            attempt=1,
            status=RunStatus.success,
            started_at=now_utc(),
            ended_at=now_utc(),
            created_at=now_utc(),
        )
        db.add(task)
        db.add(run)
        db.commit()


class TestGetTaskResult:
    """Test GET /tasks/{task_id}/result endpoint."""

    def test_returns_404_when_no_artifact_exists(self):
        task_id = str(uuid.uuid4())
        run_id = str(uuid.uuid4())
        _seed_task_and_run(task_id, run_id)

        response = client.get(f"/tasks/{task_id}/result")
        assert response.status_code == 404
        assert response.json()["detail"] == f"No result found for task '{task_id}'"

    def test_returns_latest_text_artifact(self):
        task_id = str(uuid.uuid4())
        old_run_id = str(uuid.uuid4())
        latest_run_id = str(uuid.uuid4())
        with SessionLocal() as db:
            task = Task(
                id=task_id,
                created_at=now_utc(),
                updated_at=now_utc(),
                status=TaskStatus.success,
                task_type="jobs_digest",
                payload_json='{"jobs":[]}',
                model="gpt-4o-mini",
            )
            old_run = Run(
                id=old_run_id,
                task_id=task_id,
                attempt=1,
                status=RunStatus.success,
                started_at=now_utc(),
                ended_at=now_utc(),
                created_at=now_utc(),
            )
            latest_run = Run(
                id=latest_run_id,
                task_id=task_id,
                attempt=2,
                status=RunStatus.success,
                started_at=now_utc(),
                ended_at=now_utc(),
                created_at=now_utc(),
            )
            db.add(task)
            db.add(old_run)
            db.add(latest_run)
            db.flush()

            old_artifact = Artifact(
                id=str(uuid.uuid4()),
                task_id=task_id,
                run_id=old_run_id,
                artifact_type="llm_output",
                content_text="old output",
                content_json=None,
                created_at=now_utc() - timedelta(seconds=5),
            )
            latest_artifact = Artifact(
                id=str(uuid.uuid4()),
                task_id=task_id,
                run_id=latest_run_id,
                artifact_type="llm_output",
                content_text="latest output",
                content_json=None,
                created_at=now_utc(),
            )
            db.add(old_artifact)
            db.add(latest_artifact)
            db.commit()

        response = client.get(f"/tasks/{task_id}/result")
        assert response.status_code == 200
        payload = response.json()
        assert payload["task_id"] == task_id
        assert payload["artifact_type"] == "llm_output"
        assert payload["content_text"] == "latest output"
        assert payload["content_json"] is None
        assert "created_at" in payload

    def test_returns_json_artifact(self):
        task_id = str(uuid.uuid4())
        run_id = str(uuid.uuid4())
        _seed_task_and_run(task_id, run_id)

        with SessionLocal() as db:
            artifact = Artifact(
                id=str(uuid.uuid4()),
                task_id=task_id,
                run_id=run_id,
                artifact_type="structured_output",
                content_text=None,
                content_json={"summary": "ok", "score": 0.9},
                created_at=now_utc(),
            )
            db.add(artifact)
            db.commit()

        response = client.get(f"/tasks/{task_id}/result")
        assert response.status_code == 200
        payload = response.json()
        assert payload["task_id"] == task_id
        assert payload["artifact_type"] == "structured_output"
        assert payload["content_text"] is None
        assert payload["content_json"] == {"summary": "ok", "score": 0.9}

    def test_task_and_run_endpoints_surface_debug_diagnostics(self):
        task_id = str(uuid.uuid4())
        run_id = str(uuid.uuid4())
        with SessionLocal() as db:
            task = Task(
                id=task_id,
                created_at=now_utc(),
                updated_at=now_utc(),
                status=TaskStatus.failed,
                task_type="deals_scan_v1",
                payload_json='{"collectors_enabled": true}',
                model="gpt-5-mini",
                error="Connection error.",
            )
            run = Run(
                id=run_id,
                task_id=task_id,
                attempt=1,
                status=RunStatus.failed,
                started_at=now_utc(),
                ended_at=now_utc(),
                error="Connection error.",
                created_at=now_utc(),
            )
            db.add(task)
            db.add(run)
            db.flush()
            db.add(
                Artifact(
                    id=str(uuid.uuid4()),
                    task_id=task_id,
                    run_id=run_id,
                    artifact_type="debug.json",
                    content_json={
                        "error_type": "APIConnectionError",
                        "error": "Connection error.",
                        "retry_scheduled": False,
                    },
                    created_at=now_utc(),
                )
            )
            db.commit()

        task_response = client.get(f"/tasks/{task_id}")
        assert task_response.status_code == 200
        task_payload = task_response.json()
        assert task_payload["diagnostics"]["category"] == "upstream_connection_failure"
        assert task_payload["diagnostics"]["upstream_service"] == "openai"

        runs_response = client.get(f"/tasks/{task_id}/runs")
        assert runs_response.status_code == 200
        runs_payload = runs_response.json()
        assert runs_payload[0]["diagnostics"]["error_type"] == "APIConnectionError"
        assert runs_payload[0]["diagnostics"]["summary"].startswith("Worker failed while calling the OpenAI API")


class TestBudgetBlockHistory:
    """Budget blocks should be visible in task run history."""

    def test_blocked_task_creation_creates_failed_run_history(self):
        spend_run_id = str(uuid.uuid4())
        with SessionLocal() as db:
            spend_run = Run(
                id=spend_run_id,
                task_id=str(uuid.uuid4()),
                attempt=1,
                status=RunStatus.success,
                started_at=now_utc(),
                ended_at=now_utc(),
                cost_usd=DAILY_BUDGET_USD + BUDGET_BUFFER_USD,
                created_at=now_utc(),
            )
            db.add(spend_run)
            db.commit()

        task_id: str | None = None
        try:
            response = client.post(
                "/tasks",
                json={
                    "task_type": "jobs_digest",
                    "payload_json": '{"jobs":[]}',
                    "model": "gpt-4o-mini",
                },
            )
            assert response.status_code == 200
            payload = response.json()
            task_id = payload["id"]
            assert payload["status"] == "blocked_budget"

            runs_response = client.get(f"/tasks/{task_id}/runs")
            assert runs_response.status_code == 200
            runs = runs_response.json()
            assert len(runs) >= 1
            assert runs[0]["status"] == "failed"
            assert "Daily budget blocked" in (runs[0]["error"] or "")
        finally:
            with SessionLocal() as db:
                if task_id is not None:
                    db.query(Run).filter(Run.task_id == task_id).delete()
                    db.query(Task).filter(Task.id == task_id).delete()
                db.query(Run).filter(Run.id == spend_run_id).delete()
                db.commit()


class TestIdempotencyAndHealth:
    """Tests for idempotent task creation and health/readiness endpoints."""

    def test_idempotency_key_returns_existing_task(self):
        idem_key = f"idem-{uuid.uuid4()}"
        payload = {
            "task_type": "jobs_digest",
            "payload_json": '{"jobs":[]}',
            "idempotency_key": idem_key,
        }

        first = client.post("/tasks", json=payload)
        assert first.status_code == 200
        first_task = first.json()

        second = client.post("/tasks", json=payload)
        assert second.status_code == 200
        second_task = second.json()

        assert first_task["id"] == second_task["id"]
        assert first_task["idempotency_key"] == idem_key

        with SessionLocal() as db:
            count = (
                db.query(Task)
                .filter(Task.task_type == "jobs_digest", Task.idempotency_key == idem_key)
                .count()
            )
            assert count == 1

    def test_health_endpoint(self):
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json()["status"] == "ok"

    def test_ready_endpoint(self):
        response = client.get("/ready")
        assert response.status_code == 200
        assert response.json()["status"] == "ready"
