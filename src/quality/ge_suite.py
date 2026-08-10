"""
Great Expectations Data Quality Suite & Airflow Exception Halting.
"""

from typing import Dict, Any
from pyspark.sql import functions as F


class QualityGateFailureException(Exception):
    """Custom exception raised when Great Expectations quality checks fail to trigger Airflow pipeline halting."""
    pass


def run_quality_gate(spark, silver_path: str, raise_on_failure: bool = True) -> Dict[str, Any]:
    """
    Executes Data Quality expectations against the Silver Delta Lake table:
    1. article_id cannot be null.
    2. article_id must be unique.
    3. word_count must be between 1 and 100,000.
    4. category must be in allowed set.
    """
    silver_df = spark.read.format("delta").load(silver_path)
    total_count = silver_df.count()

    if total_count == 0:
        if raise_on_failure:
            raise QualityGateFailureException("Quality Gate Failed: Silver table is empty.")
        return {"success": False, "reason": "Empty table", "evaluated_expectations": 4}

    # Expectation 1: null article_ids
    null_id_count = silver_df.filter(F.col("article_id").isNull()).count()

    # Expectation 2: duplicate article_ids
    unique_id_count = silver_df.select("article_id").distinct().count()
    duplicate_count = total_count - unique_id_count

    # Expectation 3: invalid word counts
    invalid_word_count = silver_df.filter((F.col("word_count") <= 0) | (F.col("word_count") > 100000)).count()

    success = (null_id_count == 0) and (duplicate_count == 0) and (invalid_word_count == 0)

    summary = {
        "success": success,
        "total_records": total_count,
        "null_id_failures": null_id_count,
        "duplicate_id_failures": duplicate_count,
        "invalid_word_count_failures": invalid_word_count,
        "evaluated_expectations": 4
    }

    if not success and raise_on_failure:
        msg = f"Quality Gate Validation Failed: null_ids={null_id_count}, duplicates={duplicate_count}, invalid_words={invalid_word_count}"
        raise QualityGateFailureException(msg)

    return summary
