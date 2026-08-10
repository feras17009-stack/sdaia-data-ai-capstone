"""
TDD Tests: PySpark Delta Lakehouse Architecture (Bronze, Silver MERGE, Gold, Schema Enforcement).
"""

import os
import pytest
from datetime import datetime


def test_spark_session_initialization(spark_session):
    """Verify that PySpark session initializes cleanly with Delta Lake support enabled."""
    assert spark_session is not None
    assert spark_session.version.startswith("3.")
    # Check Delta extensions
    ext = spark_session.conf.get("spark.sql.extensions", "")
    assert "DeltaSparkSessionExtension" in ext


def test_bronze_loader_ingestion(spark_session, temp_workspace, valid_article_payload):
    """Verify Bronze loader writes raw records with metadata fields to Delta format."""
    from src.lakehouse.bronze_loader import write_to_bronze

    bronze_path = os.path.join(temp_workspace, "delta", "bronze")
    
    # Create sample DataFrame
    df = spark_session.createDataFrame([valid_article_payload])
    
    # Execute Bronze load
    write_to_bronze(spark_session, df, bronze_path)
    
    # Read back from Delta Bronze
    bronze_df = spark_session.read.format("delta").load(bronze_path)
    assert bronze_df.count() == 1
    assert "_ingested_at" in bronze_df.columns
    assert "_source_file" in bronze_df.columns
    assert "ingestion_date" in bronze_df.columns


def test_silver_merge_upsert(spark_session, temp_workspace, valid_article_payload):
    """Verify Silver loader executes real Delta MERGE, updating existing records and inserting new ones."""
    from src.lakehouse.silver_merge import upsert_to_silver

    silver_path = os.path.join(temp_workspace, "delta", "silver")
    
    # Initial load (Insert record 1)
    record_1 = valid_article_payload.copy()
    record_1["_ingested_at"] = datetime.utcnow().isoformat()
    record_1["_source_file"] = "buffer_01.json"
    record_1["ingestion_date"] = "2026-08-10"

    df_initial = spark_session.createDataFrame([record_1])
    upsert_to_silver(spark_session, df_initial, silver_path)

    silver_df = spark_session.read.format("delta").load(silver_path)
    assert silver_df.count() == 1
    assert silver_df.filter(silver_df.article_id == record_1["article_id"]).select("title").collect()[0][0] == record_1["title"]

    # Secondary load: Update record 1 (new title) + Insert record 2
    updated_record_1 = record_1.copy()
    updated_record_1["title"] = "Updated Scalable RAG Systems Title"

    record_2 = record_1.copy()
    record_2["article_id"] = "b9999999-9c0b-4ef8-bb6d-6bb9bd380a99"
    record_2["title"] = "New Concurrent Article on Cloud Engineering"
    record_2["category"] = "Cloud_Computing"

    df_secondary = spark_session.createDataFrame([updated_record_1, record_2])
    upsert_to_silver(spark_session, df_secondary, silver_path)

    # Validate MERGE results
    merged_df = spark_session.read.format("delta").load(silver_path)
    assert merged_df.count() == 2
    
    # Verify title was updated for record 1
    rec1_title = merged_df.filter(merged_df.article_id == record_1["article_id"]).select("title").collect()[0][0]
    assert rec1_title == "Updated Scalable RAG Systems Title"


def test_gold_aggregate_computation(spark_session, temp_workspace, valid_article_payload):
    """Verify Gold loader generates genuine business metrics aggregated by category."""
    from src.lakehouse.gold_aggregator import build_gold_aggregates

    silver_path = os.path.join(temp_workspace, "delta", "silver")
    gold_path = os.path.join(temp_workspace, "delta", "gold")

    # Create populated Silver table
    rec1 = valid_article_payload.copy()
    rec1["_ingested_at"] = datetime.utcnow().isoformat()
    rec1["_source_file"] = "buffer_01.json"
    rec1["ingestion_date"] = "2026-08-10"

    rec2 = rec1.copy()
    rec2["article_id"] = "e0eebc99-9c0b-4ef8-bb6d-6bb9bd380a55"
    rec2["category"] = "AI_ML"
    rec2["word_count"] = 100

    df_silver = spark_session.createDataFrame([rec1, rec2])
    df_silver.write.format("delta").mode("overwrite").save(silver_path)

    # Compute Gold metrics
    build_gold_aggregates(spark_session, silver_path, gold_path)

    # Inspect Gold table
    gold_df = spark_session.read.format("delta").load(gold_path)
    assert "category" in gold_df.columns
    assert "total_articles" in gold_df.columns
    assert "avg_word_count" in gold_df.columns

    ai_row = gold_df.filter(gold_df.category == "AI_ML").collect()[0]
    assert ai_row["total_articles"] == 2


def test_gold_schema_enforcement_rejection(spark_session, temp_workspace):
    """Verify that writing a DataFrame with extra non-matching columns to Gold triggers Schema Enforcement failure."""
    from pyspark.sql.utils import AnalysisException
    from src.lakehouse.gold_aggregator import write_gold_table_strict

    gold_path = os.path.join(temp_workspace, "delta", "gold_strict")

    # Write initial compliant Gold DataFrame
    data_initial = [{"category": "AI_ML", "total_articles": 5}]
    df_initial = spark_session.createDataFrame(data_initial)
    df_initial.write.format("delta").mode("overwrite").save(gold_path)

    # Attempt to write DataFrame with unexpected extra column without schema merge
    data_mutated = [{"category": "AI_ML", "total_articles": 5, "UNAUTHORIZED_EXTRA_COLUMN": "FAIL"}]
    df_mutated = spark_session.createDataFrame(data_mutated)

    with pytest.raises(Exception) as exc_info:
        write_gold_table_strict(df_mutated, gold_path)

    # Confirm schema enforcement caught the incompatible write
    assert "schema" in str(exc_info.value).lower() or "analysisexception" in str(type(exc_info.value)).lower()
