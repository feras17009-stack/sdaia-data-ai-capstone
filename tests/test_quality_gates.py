"""
Unit tests for Great Expectations Quality Gates and Lineage Event Tracking
"""

import pytest
from src.quality.ge_suite import run_intentional_failing_quality_gate
from src.quality.lineage_tracker import PipelineLineageTracker


def test_quality_gate_failure_proof():
    res = run_intentional_failing_quality_gate()
    assert res["quality_gate_passed"] is False
    assert res["status"] == "PASSED_FAILURE_PROOF"


def test_openlineage_event_emission():
    tracker = PipelineLineageTracker()
    evt = tracker.emit_event(
        job_name="test_job",
        state="START",
        run_id="test-run-123",
        inputs=["test_input"],
        outputs=["test_output"]
    )
    assert evt["eventType"] == "START"
    assert evt["job"]["name"] == "test_job"
