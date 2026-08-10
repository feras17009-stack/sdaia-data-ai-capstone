"""
Pydantic Data Contracts & DLQ Quarantine Payload Formatting.
"""

import uuid
from datetime import datetime, timezone
from typing import Literal, Optional, Dict, Any
from pydantic import BaseModel, Field, field_validator


class ArticleContract(BaseModel):
    """Data Contract governing incoming raw article payloads."""
    article_id: str = Field(..., description="Unique UUID identifier for the article")
    title: str = Field(..., min_length=5, description="Article title (minimum 5 characters)")
    content: str = Field(..., min_length=10, description="Article content (minimum 10 characters)")
    category: Literal["AI_ML", "Data_Engineering", "Cloud_Computing", "Cybersecurity"] = Field(
        ..., description="Standardized category taxonomy"
    )
    author: Optional[str] = "Anonymous"
    published_at: str = Field(..., description="ISO 8601 publication timestamp")
    word_count: int = Field(..., gt=0, description="Positive word count value")

    @field_validator("article_id")
    @classmethod
    def validate_uuid(cls, value: str) -> str:
        """Enforces that article_id is a valid UUID string format."""
        if not value or not isinstance(value, str):
            raise ValueError("article_id must be a non-empty string")
        try:
            uuid.UUID(value)
        except ValueError:
            raise ValueError(f"article_id '{value}' is not a valid UUID format")
        return value


def format_dlq_payload(
    raw_payload: Dict[str, Any],
    error_type: str,
    field_failed: str,
    error_message: str
) -> Dict[str, Any]:
    """Formats an unprocessable or malformed record into a standardized DLQ quarantine structure."""
    return {
        "quarantine_id": f"dlq_{uuid.uuid4().hex[:8]}",
        "error_type": error_type,
        "field_failed": field_failed,
        "error_message": error_message,
        "raw_payload": raw_payload,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }
