"""
Unit tests for Delta Lake MERGE (Upsert) Operation
"""

import pytest
from src.lakehouse.gold_aggregator import verify_schema_enforcement_failure


def test_delta_schema_enforcement():
    res = verify_schema_enforcement_failure()
    assert res["status"] == "PASSED_ENFORCEMENT_PROOF"
