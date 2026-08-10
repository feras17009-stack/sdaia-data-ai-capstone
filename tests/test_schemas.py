"""
TDD Tests: Ingestion Data Contract & Dead-Letter Queue (DLQ) Quarantine Schema.
"""

import pytest
import json
from datetime import datetime


def test_article_contract_valid_payload(valid_article_payload):
    """Verify that valid article payloads successfully validate against ArticleContract."""
    from src.ingestion.schemas import ArticleContract

    contract = ArticleContract(**valid_article_payload)
    assert contract.article_id == valid_article_payload["article_id"]
    assert contract.title == valid_article_payload["title"]
    assert contract.category == valid_article_payload["category"]
    assert contract.word_count == valid_article_payload["word_count"]


def test_article_contract_invalid_title_length(valid_article_payload):
    """Verify that titles shorter than 5 characters trigger Pydantic ValidationError."""
    from pydantic import ValidationError
    from src.ingestion.schemas import ArticleContract

    payload = valid_article_payload.copy()
    payload["title"] = "Tiny"  # 4 chars < min_length=5

    with pytest.raises(ValidationError) as exc_info:
        ArticleContract(**payload)
    
    errors = exc_info.value.errors()
    assert any(err["loc"] == ("title",) for err in errors)


def test_article_contract_invalid_category(valid_article_payload):
    """Verify that unsupported categories trigger Pydantic ValidationError."""
    from pydantic import ValidationError
    from src.ingestion.schemas import ArticleContract

    payload = valid_article_payload.copy()
    payload["category"] = "INVALID_CATEGORY_NAME"

    with pytest.raises(ValidationError) as exc_info:
        ArticleContract(**payload)
    
    errors = exc_info.value.errors()
    assert any(err["loc"] == ("category",) for err in errors)


def test_article_contract_negative_word_count(valid_article_payload):
    """Verify that non-positive word counts trigger Pydantic ValidationError."""
    from pydantic import ValidationError
    from src.ingestion.schemas import ArticleContract

    payload = valid_article_payload.copy()
    payload["word_count"] = -10

    with pytest.raises(ValidationError) as exc_info:
        ArticleContract(**payload)
    
    errors = exc_info.value.errors()
    assert any(err["loc"] == ("word_count",) for err in errors)


def test_article_contract_missing_mandatory_field(valid_article_payload):
    """Verify that omitting mandatory fields triggers ValidationError."""
    from pydantic import ValidationError
    from src.ingestion.schemas import ArticleContract

    payload = valid_article_payload.copy()
    del payload["article_id"]

    with pytest.raises(ValidationError) as exc_info:
        ArticleContract(**payload)
    
    errors = exc_info.value.errors()
    assert any(err["loc"] == ("article_id",) for err in errors)


def test_dlq_quarantine_payload_formatter():
    """Verify that format_dlq_payload generates compliant DLQ JSON structure."""
    from src.ingestion.schemas import format_dlq_payload

    raw_data = {"article_id": "123", "word_count": -5}
    error_msg = "word_count must be greater than 0"
    field = "word_count"

    dlq_record = format_dlq_payload(
        raw_payload=raw_data,
        error_type="ValidationError",
        field_failed=field,
        error_message=error_msg
    )

    assert dlq_record["error_type"] == "ValidationError"
    assert dlq_record["field_failed"] == "word_count"
    assert dlq_record["error_message"] == error_msg
    assert dlq_record["raw_payload"] == raw_data
    assert "timestamp" in dlq_record
    assert "quarantine_id" in dlq_record
