"""
Unit tests for Ingestion Boundary Data Contracts (Pydantic schemas)
"""

import pytest
from pydantic import ValidationError
from src.ingestion.schemas import ArticleEvent, QuarantineRecord


def test_article_event_valid():
    payload = {
        "article_id": "ART-001",
        "title": "Scalable Data Pipelines with Delta Lake",
        "author": "Dr. Sarah",
        "category": "Data Engineering",
        "content": "Comprehensive guide on Delta Lake architecture and PySpark integration.",
        "views": 1200,
        "rating": 4.8,
        "published_timestamp": "2026-08-10T12:00:00Z"
    }
    event = ArticleEvent(**payload)
    assert event.article_id == "ART-001"
    assert event.rating == 4.8


def test_article_event_invalid_title():
    payload = {
        "article_id": "ART-BAD-01",
        "title": "AB",  # < 3 chars
        "author": "Tester",
        "category": "AI",
        "content": "Test content body.",
        "views": 10,
        "rating": 4.0,
        "published_timestamp": "2026-08-10T12:00:00Z"
    }
    with pytest.raises(ValidationError):
        ArticleEvent(**payload)


def test_article_event_invalid_rating_and_views():
    payload = {
        "article_id": "ART-BAD-02",
        "title": "Invalid View Rating Event",
        "author": "Tester",
        "category": "Cloud",
        "content": "Test content body.",
        "views": -5,  # negative
        "rating": 6.5,  # > 5.0
        "published_timestamp": "2026-08-10T12:00:00Z"
    }
    with pytest.raises(ValidationError):
        ArticleEvent(**payload)


def test_quarantine_record_formatting():
    q = QuarantineRecord(
        quarantine_id="QLOG-001",
        rejection_timestamp="2026-08-10T12:00:00Z",
        rejection_reason="Validation Failed: rating > 5.0",
        failed_field="rating",
        raw_payload={"article_id": "BAD-01", "rating": 6.5}
    )
    assert q.quarantine_id == "QLOG-001"
    assert q.failed_field == "rating"
