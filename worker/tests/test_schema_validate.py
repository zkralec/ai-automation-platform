"""Regression tests for payload schema resolution and notify_v1 compatibility."""

import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from core.schema_validate import _schema_dirs, validate_payload


def test_schema_dirs_prioritize_worker_task_payloads() -> None:
    dirs = _schema_dirs()
    assert dirs, "schema directory list must not be empty"
    first = dirs[0]
    assert str(first).endswith("/worker/schemas/task_payloads")


def test_notify_v1_accepts_disable_dedupe_flag() -> None:
    validate_payload(
        "notify_v1",
        {
            "channels": ["discord"],
            "message": "schema acceptance",
            "source_task_type": "ops_report_v1",
            "disable_dedupe": True,
        },
    )


def test_jobs_collect_v1_accepts_breadth_controls() -> None:
    validate_payload(
        "jobs_collect_v1",
        {
            "request": {
                "query": "machine learning engineer",
                "locations": ["Remote", "New York, NY"],
                "sources": ["linkedin", "indeed"],
                "result_limit_per_source": 600,
                "max_pages_per_source": 8,
                "max_queries_per_title_location_pair": 5,
                "early_stop_when_no_new_results": False,
            }
        },
    )
