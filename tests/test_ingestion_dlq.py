"""
TDD Tests: Real-time Ingestion & Dead-Letter Queue (DLQ) Quarantine Flow.
"""

import os
import json
import pytest


def test_ingestion_consumer_validation_routing(temp_workspace, valid_article_payload, malformed_article_payloads):
    """
    Verify that consumer validates payloads using Pydantic:
    - Valid payloads -> saved to valid_buffer.json
    - Invalid payloads -> isolated to quarantine DLQ directory with error metadata.
    """
    from src.ingestion.consumer import process_incoming_payloads

    valid_buffer_path = os.path.join(temp_workspace, "raw_sample", "valid_buffer.json")
    dlq_dir = os.path.join(temp_workspace, "quarantine_dlq")
    os.makedirs(os.path.dirname(valid_buffer_path), exist_ok=True)
    os.makedirs(dlq_dir, exist_ok=True)

    # Mix 1 valid payload with malformed payloads
    incoming_batch = [valid_article_payload] + malformed_article_payloads

    # Execute processing loop logic
    stats = process_incoming_payloads(
        batch=incoming_batch,
        valid_buffer_file=valid_buffer_path,
        dlq_dir=dlq_dir
    )

    # Assert valid count & quarantined count
    assert stats["processed"] == len(incoming_batch)
    assert stats["valid_count"] == 1
    assert stats["quarantined_count"] == len(malformed_article_payloads)

    # Verify valid_buffer.json contains valid record
    with open(valid_buffer_path, "r", encoding="utf-8") as f:
        valid_records = json.load(f)
    assert len(valid_records) == 1
    assert valid_records[0]["article_id"] == valid_article_payload["article_id"]

    # Verify quarantine DLQ directory contains error metadata files
    dlq_files = [f for f in os.listdir(dlq_dir) if f.endswith(".json")]
    assert len(dlq_files) == len(malformed_article_payloads)

    # Inspect one DLQ payload
    sample_dlq_file = os.path.join(dlq_dir, dlq_files[0])
    with open(sample_dlq_file, "r", encoding="utf-8") as f:
        dlq_data = json.load(f)
    
    assert "error_type" in dlq_data
    assert dlq_data["error_type"] == "ValidationError"
    assert "field_failed" in dlq_data
    assert "raw_payload" in dlq_data
    assert "quarantine_id" in dlq_data
