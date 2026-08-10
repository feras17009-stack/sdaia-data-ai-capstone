"""
Pytest configuration and shared fixtures for the Capstone Data & AI Pipeline test suite.
"""

import os
import sys
import shutil
import tempfile
import pytest
from datetime import datetime, timezone

# Add src/ to sys.path so test files can import modules cleanly
SYS_SRC_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))
if SYS_SRC_DIR not in sys.path:
    sys.path.insert(0, SYS_SRC_DIR)


@pytest.fixture(scope="session")
def temp_workspace():
    """Provides a temporary directory workspace for testing file & lakehouse operations."""
    temp_dir = tempfile.mkdtemp(prefix="capstone_test_")
    yield temp_dir
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture(scope="session")
def spark_session(temp_workspace):
    """
    Creates a PySpark session configured with Delta Lake support.
    Falls back gracefully if PySpark/Delta packages are missing during unit test runs.
    """
    try:
        from pyspark.sql import SparkSession
        from delta import configure_spark_with_delta_pip

        builder = (
            SparkSession.builder.appName("CapstoneTDDTestSession")
            .master("local[2]")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.warehouse.dir", os.path.join(temp_workspace, "warehouse"))
            .config("spark.driver.host", "localhost")
        )
        session = configure_spark_with_delta_pip(builder).getOrCreate()
        yield session
        session.stop()
    except ImportError:
        pytest.skip("PySpark or Delta Lake package not installed; skipping Spark integration fixture.")


@pytest.fixture
def valid_article_payload():
    """Returns a valid dictionary representing an article payload matching ArticleContract."""
    return {
        "article_id": "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
        "title": "Building Scalable RAG Systems with Delta Lake",
        "content": "Retrieval-Augmented Generation (RAG) combines semantic vector search with structured data pipelines to deliver accurate AI responses with verified citations.",
        "category": "AI_ML",
        "author": "Dr. Sarah Norris",
        "published_at": "2026-08-10T12:00:00Z",
        "word_count": 22
    }


@pytest.fixture
def malformed_article_payloads():
    """Returns a list of invalid article payloads designed to trigger schema validation failures."""
    return [
        {
            # Missing mandatory article_id
            "title": "Invalid Article Without ID",
            "content": "Content is sufficient but ID is missing completely.",
            "category": "Data_Engineering",
            "published_at": "2026-08-10T12:00:00Z",
            "word_count": 10
        },
        {
            # Title too short (< 5 chars)
            "article_id": "b0eebc99-9c0b-4ef8-bb6d-6bb9bd380a22",
            "title": "Tiny",
            "content": "Content length is sufficient for contract.",
            "category": "Cloud_Computing",
            "published_at": "2026-08-10T12:00:00Z",
            "word_count": 6
        },
        {
            # Invalid category enum
            "article_id": "c0eebc99-9c0b-4ef8-bb6d-6bb9bd380a33",
            "title": "Invalid Category Payload",
            "content": "Testing category restriction against schema contract.",
            "category": "UNSUPPORTED_CATEGORY",
            "published_at": "2026-08-10T12:00:00Z",
            "word_count": 7
        },
        {
            # Non-positive word count (-5)
            "article_id": "d0eebc99-9c0b-4ef8-bb6d-6bb9bd380a44",
            "title": "Negative Word Count Record",
            "content": "This record has a negative word count value.",
            "category": "Cybersecurity",
            "published_at": "2026-08-10T12:00:00Z",
            "word_count": -5
        }
    ]
