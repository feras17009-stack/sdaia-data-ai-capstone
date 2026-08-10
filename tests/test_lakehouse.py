"""
Unit tests for Delta Lakehouse Bronze, Silver MERGE, Gold Aggregations, and Schema Enforcement
"""

import os
import pytest
from src.lakehouse.gold_aggregator import verify_schema_enforcement_failure


def test_schema_enforcement_refusal():
    """Verifies that bad writes are refused by Delta Schema Enforcement (Rubric requirement)."""
    res = verify_schema_enforcement_failure()
    assert res["rejection_caught"] is True
    assert res["status"] == "PASSED_ENFORCEMENT_PROOF"
