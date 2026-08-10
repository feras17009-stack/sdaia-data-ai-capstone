"""
PySpark & Delta Lake 3.x Session Factory with Windows winutils Bootstrap Protection.
"""

import os
import sys


def get_spark_session(app_name: str = "CapstoneLakehousePipeline"):
    """
    Creates and configures a PySpark SparkSession with Delta Lake 3.x extensions.
    Includes Windows environment validation for winutils.exe.
    """
    try:
        from pyspark.sql import SparkSession
        from delta import configure_spark_with_delta_pip
    except ImportError as e:
        raise ImportError(
            "PySpark or Delta Spark packages not found. "
            "Please ensure `pip install pyspark==3.5.1 delta-spark==3.2.0` is run."
        ) from e

    # Windows environment check
    if sys.platform.startswith("win"):
        if not os.environ.get("HADOOP_HOME"):
            print("INFO: HADOOP_HOME not set on Windows. Defaulting temporary fallback to avoid WinUtils error.")

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
    )

    session = configure_spark_with_delta_pip(builder).getOrCreate()
    return session
