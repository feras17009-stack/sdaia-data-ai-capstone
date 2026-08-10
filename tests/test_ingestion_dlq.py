"""
Unit tests for Ingestion Schema Validation & Dead-Letter Topic (DLQ) Quarantine
"""

import os
import json
import pytest
from src.ingestion.schemas import ArticleEvent, QuarantineRecord
from src.ingestion.producer import publish_events
from src.ingestion.consumer import process_ingestion


def test_valid_article_event_schema():
    valid_payload = {
        "article_id": "ART-TEST-001",
        "title": "Testing Kafka Schema Validation Ingestion Boundary",
        "author": "SDAIA Trainee",
        "category": "Data Engineering",
        "content": "Comprehensive test payload enforcing Pydantic data contracts at the ingestion layer.",
        "views": 500,
        "rating": 4.5,
        "published_timestamp": "2026-08-10T12:00:00Z"
    }
    event = ArticleEvent(**valid_payload)
    assert event.article_id == "ART-TEST-001"
    assert event.views == 500


def test_malformed_article_event_schema():
    malformed_payload = {
        "article_id": "ART-BAD-TEST",
        "title": "X",  # Too short
        "author": "Tester",
        "category": "InvalidCategory",  # Not in allowed list
        "content": "Short",
        "views": -10,  # Negative
        "rating": 10.0,  # > 5.0
        "published_timestamp": "BAD-DATE"
    }
    with pytest.raises(ValueError):
        ArticleEvent(**malformed_payload)


def test_end_to_end_ingestion_dlq_routing():
    publish_res = publish_events()
    assert publish_res["status"] == "SUCCESS"

    process_res = process_ingestion()
    assert process_res["total_processed"] > 0
    assert process_res["valid_count"] == 5
    assert process_res["quarantine_count"] == 3
    assert os.path.exists(process_res["validated_path"])
    assert os.path.exists(process_res["quarantine_path"])
