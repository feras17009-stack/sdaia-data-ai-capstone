"""
Silver Lakehouse Loader: Delta Lake MERGE Upsert Keyed on Article ID.
"""

from delta.tables import DeltaTable


def upsert_to_silver(spark, df, silver_path: str):
    """
    Upserts Bronze incremental DataFrame into Silver Delta Lake table using MERGE.
    - If article_id matches: UPDATE SET *
    - If article_id not matched: INSERT *
    - Includes first-run table initialization guard (CREATE TABLE IF NOT EXISTS).
    """
    if not DeltaTable.isDeltaTable(spark, silver_path):
        # Initial table creation on first run
        df.write.format("delta").mode("overwrite").save(silver_path)
        return

    silver_table = DeltaTable.forPath(spark, silver_path)
    
    (
        silver_table.alias("target")
        .merge(
            df.alias("source"),
            "target.article_id = source.article_id"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
