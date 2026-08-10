"""
PySpark & Delta Lake Session Manager
"""

import os
import sys

# Set JAVA_HOME if not already present in environment
DEFAULT_JAVA_HOME = r"C:\Users\feras\.antigravity-ide\extensions\redhat.java-1.55.0-win32-x64\jre\21.0.11-win32-x86_64"
if "JAVA_HOME" not in os.environ and os.path.exists(DEFAULT_JAVA_HOME):
    os.environ["JAVA_HOME"] = DEFAULT_JAVA_HOME


def get_spark_session(app_name: str = "SDAIA-Capstone-Delta-Lakehouse"):
    """
    Initializes and returns a PySpark SparkSession configured with Delta Lake 3.x support.
    """
    try:
        from pyspark.sql import SparkSession
        from delta import configure_spark_with_delta_pip
    except ImportError:
        raise ImportError("PySpark or delta-spark is not installed in the active environment.")

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.driver.memory", "2g")
        .config("spark.master", "local[*]")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


if __name__ == "__main__":
    spark = get_spark_session()
    print(f"[SparkSession] Active Spark Version: {spark.version}")
    spark.stop()
