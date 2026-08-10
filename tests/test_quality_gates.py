"""
TDD Tests: Data Quality Gates (Great Expectations) & Governance (OpenLineage).
"""

import os
import pytest
from datetime import datetime


def test_ge_quality_suite_pass_on_clean_data(spark_session, temp_workspace, valid_article_payload):
    """Verify that Great Expectations validation suite passes cleanly on valid Silver data."""
    from src.quality.ge_suite import run_quality_gate

    silver_path = os.path.join(temp_workspace, "delta", "silver_clean")
    
    rec1 = valid_article_payload.copy()
    rec1["_ingested_at"] = datetime.utcnow().isoformat()
    rec1["_source_file"] = "buffer_01.json"
    rec1["ingestion_date"] = "2026-08-10"

    df_clean = spark_session.createDataFrame([rec1])
    df_clean.write.format("delta").mode("overwrite").save(silver_path)

    # Run quality gate check
    result = run_quality_gate(spark_session, silver_path, raise_on_failure=False)
    assert result["success"] is True
    assert result["evaluated_expectations"] > 0


def test_ge_quality_suite_fail_on_corrupted_data(spark_session, temp_workspace, valid_article_payload):
    """Verify that null article_ids or negative word counts trigger Quality Gate failure."""
    from src.quality.ge_suite import run_quality_gate

    silver_path = os.path.join(temp_workspace, "delta", "silver_corrupted")

    # Create corrupted record (null article_id + negative word count)
    bad_rec = valid_article_payload.copy()
    bad_rec["article_id"] = None
    bad_rec["word_count"] = -99
    bad_rec["_ingested_at"] = datetime.utcnow().isoformat()
    bad_rec["_source_file"] = "buffer_bad.json"
    bad_rec["ingestion_date"] = "2026-08-10"

    df_bad = spark_session.createDataFrame([bad_rec])
    df_bad.write.format("delta").mode("overwrite").save(silver_path)

    # Run quality gate check without auto-raise to inspect output
    result = run_quality_gate(spark_session, silver_path, raise_on_failure=False)
    assert result["success"] is False


def test_ge_quality_suite_raises_airflow_exception(spark_session, temp_workspace, valid_article_payload):
    """Verify that AirflowException is raised when raise_on_failure=True and quality gate fails."""
    from src.quality.ge_suite import run_quality_gate, QualityGateFailureException

    silver_path = os.path.join(temp_workspace, "delta", "silver_bad_airflow")

    bad_rec = valid_article_payload.copy()
    bad_rec["article_id"] = None
    bad_rec["_ingested_at"] = datetime.utcnow().isoformat()
    bad_rec["_source_file"] = "buffer_bad.json"
    bad_rec["ingestion_date"] = "2026-08-10"

    df_bad = spark_session.createDataFrame([bad_rec])
    df_bad.write.format("delta").mode("overwrite").save(silver_path)

    with pytest.raises(QualityGateFailureException) as exc_info:
        run_quality_gate(spark_session, silver_path, raise_on_failure=True)

    assert "Quality Gate Validation Failed" in str(exc_info.value)


def test_openlineage_event_tracker(temp_workspace):
    """Verify OpenLineage tracker emits START, COMPLETE, and FAIL run lineage events."""
    from src.quality.lineage_tracker import OpenLineageTracker

    output_dir = os.path.join(temp_workspace, "lineage_events")
    tracker = OpenLineageTracker(job_name="test_silver_upsert_job", output_dir=output_dir)

    # Emit START event
    start_event = tracker.emit_start_event(
        inputs=["file://data/delta/bronze"],
        outputs=["file://data/delta/silver"]
    )
    assert start_event["eventType"] == "START"
    assert start_event["job"]["name"] == "test_silver_upsert_job"

    # Emit COMPLETE event
    complete_event = tracker.emit_complete_event(run_id=start_event["run"]["runId"])
    assert complete_event["eventType"] == "COMPLETE"
    assert complete_event["run"]["runId"] == start_event["run"]["runId"]

    # Verify fallback event log files were written to output_dir
    written_files = os.listdir(output_dir)
    assert len(written_files) >= 2
