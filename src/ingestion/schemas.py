"""
Pydantic Data Contracts for Ingestion Validation & Quarantine DLQ
"""

from typing import Optional, Dict, Any
from datetime import datetime
from pydantic import BaseModel, Field, field_validator


class ArticleEvent(BaseModel):
    """Data contract for incoming tech article events at the ingestion boundary."""
    article_id: str = Field(..., description="Unique business key for the article")
    title: str = Field(..., min_length=3, description="Article title (at least 3 characters)")
    author: str = Field(..., min_length=2, description="Author full name")
    category: str = Field(..., description="Category (e.g., AI, Data Engineering, Cloud, Security)")
    content: str = Field(..., min_length=10, description="Article body text")
    views: int = Field(ge=0, description="Article view count (must be non-negative)")
    rating: float = Field(ge=0.0, le=5.0, description="User rating between 0.0 and 5.0")
    published_timestamp: str = Field(..., description="ISO 8601 publication timestamp")

    @field_validator("published_timestamp")
    @classmethod
    def validate_timestamp_format(cls, v: str) -> str:
        try:
            datetime.fromisoformat(v.replace("Z", "+00:00"))
            return v
        except Exception:
            raise ValueError(f"Invalid ISO 8601 timestamp format: '{v}'")

    @field_validator("category")
    @classmethod
    def validate_category(cls, v: str) -> str:
        allowed = {"AI", "Data Engineering", "Cloud", "Cybersecurity", "Software Architecture"}
        if v not in allowed:
            raise ValueError(f"Category '{v}' is not in allowed list: {allowed}")
        return v


class QuarantineRecord(BaseModel):
    """Schema for malformed records routed to the Dead-Letter Topic / Quarantine zone."""
    quarantine_id: str
    rejection_timestamp: str
    rejection_reason: str
    failed_field: Optional[str] = None
    raw_payload: Dict[str, Any]
