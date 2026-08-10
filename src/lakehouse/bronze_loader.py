"""
Bronze Lakehouse Loader: Appends Raw Events with Audit Metadata.
"""

from datetime import datetime, timezone
from pyspark.sql import functions as F


def write_to_bronze(spark, df, bronze_path: str):
    """
    Appends raw validated events DataFrame to Delta Bronze storage.
    Enriches with metadata columns: _ingested_at, _source_file, ingestion_date.
    """
    now_iso = datetime.now(timezone.utc).isoformat()
    today_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    enriched_df = df
    if "_ingested_at" not in enriched_df.columns:
        enriched_df = enriched_df.withColumn("_ingested_at", F.lit(now_iso))
    if "_source_file" not in enriched_df.columns:
        enriched_df = enriched_df.withColumn("_source_file", F.lit("valid_buffer.json"))
    if "ingestion_date" not in enriched_df.columns:
        enriched_df = enriched_df.withColumn("ingestion_date", F.lit(today_date))

    enriched_df.write.format("delta") \
        .mode("append") \
        .partitionBy("ingestion_date") \
        .save(bronze_path)
