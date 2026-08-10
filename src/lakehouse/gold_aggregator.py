"""
Gold Lakehouse Loader: Business Aggregates & Strict Schema Enforcement Demonstration.
"""

from pyspark.sql import functions as F


def build_gold_aggregates(spark, silver_path: str, gold_path: str):
    """
    Reads Silver Delta Lake table, computes category-level genuine business metrics,
    and writes to Gold Delta Lake storage.
    """
    silver_df = spark.read.format("delta").load(silver_path)

    gold_df = (
        silver_df.groupBy("category")
        .agg(
            F.count("article_id").alias("total_articles"),
            F.avg("word_count").alias("avg_word_count"),
            F.max("published_at").alias("latest_publication")
        )
    )

    gold_df.write.format("delta").mode("overwrite").save(gold_path)
    return gold_df


def write_gold_table_strict(df, gold_path: str):
    """
    Writes DataFrame to Gold table with mergeSchema=False to enforce strict schema adherence.
    Will raise an AnalysisException if schema incompatible.
    """
    df.write.format("delta") \
        .option("mergeSchema", "false") \
        .mode("append") \
        .save(gold_path)
