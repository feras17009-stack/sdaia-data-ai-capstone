"""
Apache Airflow DAG: Modern Data Engineering End-to-End Orchestrated Pipeline
Wires Kafka Ingestion, Delta Lakehouse (Bronze/Silver/Gold), Great Expectations Quality Gate,
OpenLineage Tracking, and RAG Vector Store Indexing.
"""

from datetime import datetime, timedelta
import sys
import os

from airflow import DAG
from airflow.operators.python import PythonOperator

# Ensure project src directory is on Python path
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

default_args = {
    "owner": "SDAIA_Capstone",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


def task_emit_start_lineage(**kwargs):
    from src.quality.lineage_tracker import PipelineLineageTracker
    tracker = PipelineLineageTracker()
    run_id = kwargs.get("run_id", "air-run-001")
    tracker.emit_event(
        job_name="capstone_end_to_end_pipeline",
        state="START",
        run_id=run_id,
        inputs=["kafka_raw_topic"],
        outputs=["gold_delta_metrics", "rag_vector_index"]
    )


def task_ingest_raw_events():
    from src.ingestion.producer import publish_events
    from src.ingestion.consumer import process_ingestion
    publish_events()
    res = process_ingestion()
    print(f"[Airflow Task: Ingestion] Processed {res['total_processed']} events ({res['quarantine_count']} routed to DLQ).")


def task_load_bronze():
    from src.lakehouse.bronze_loader import load_bronze
    res = load_bronze()
    print(f"[Airflow Task: Bronze] Loaded {res['bronze_count']} rows into Delta Bronze.")


def task_upsert_silver():
    from src.lakehouse.silver_merge import upsert_silver
    res = upsert_silver()
    print(f"[Airflow Task: Silver] Upserted Delta Silver table. Total Silver rows: {res['silver_count']}.")


def task_run_quality_gate():
    from src.quality.ge_suite import run_silver_quality_gate
    res = run_silver_quality_gate()
    if not res["quality_gate_passed"]:
        raise ValueError("[Airflow Quality Gate HALT] Great Expectations quality suite failed! Halting downstream pipeline.")
    print("[Airflow Task: Quality Gate] Passed successfully.")


def task_aggregate_gold():
    from src.lakehouse.gold_aggregator import build_gold_aggregates
    res = build_gold_aggregates()
    print(f"[Airflow Task: Gold] Created genuine aggregates: {res['category_metrics_count']} categories, {res['author_metrics_count']} authors.")


def task_build_rag_index():
    from src.rag.rag_engine import GroundedRAGEngine
    engine = GroundedRAGEngine()
    engine.initialize_index()
    print(f"[Airflow Task: RAG Index] Hybrid Vector & BM25 index built successfully from Silver Delta documents.")


def task_emit_complete_lineage(**kwargs):
    from src.quality.lineage_tracker import PipelineLineageTracker
    tracker = PipelineLineageTracker()
    run_id = kwargs.get("run_id", "air-run-001")
    tracker.emit_event(
        job_name="capstone_end_to_end_pipeline",
        state="COMPLETE",
        run_id=run_id,
        inputs=["kafka_raw_topic"],
        outputs=["gold_delta_metrics", "rag_vector_index"]
    )


with DAG(
    "sdaia_modern_data_engineering_capstone",
    default_args=default_args,
    description="End-to-End SDAIA Capstone Pipeline: Ingestion DLQ -> Delta Lakehouse -> GE Quality Gate -> RAG Index",
    schedule_interval="@daily",
    catchup=False,
) as dag:

    t1_start_lineage = PythonOperator(
        task_id="start_lineage_tracking",
        python_callable=task_emit_start_lineage,
    )

    t2_ingest_kafka = PythonOperator(
        task_id="kafka_ingestion_and_dlq_quarantine",
        python_callable=task_ingest_raw_events,
    )

    t3_load_bronze = PythonOperator(
        task_id="load_delta_bronze_layer",
        python_callable=task_load_bronze,
    )

    t4_upsert_silver = PythonOperator(
        task_id="upsert_delta_silver_layer",
        python_callable=task_upsert_silver,
    )

    t5_quality_gate = PythonOperator(
        task_id="great_expectations_quality_gate",
        python_callable=task_run_quality_gate,
    )

    t6_aggregate_gold = PythonOperator(
        task_id="aggregate_delta_gold_layer",
        python_callable=task_aggregate_gold,
    )

    t7_build_rag = PythonOperator(
        task_id="build_rag_hybrid_index",
        python_callable=task_build_rag_index,
    )

    t8_complete_lineage = PythonOperator(
        task_id="complete_lineage_tracking",
        python_callable=task_emit_complete_lineage,
    )

    # Define task dependencies
    t1_start_lineage >> t2_ingest_kafka >> t3_load_bronze >> t4_upsert_silver >> t5_quality_gate
    t5_quality_gate >> t6_aggregate_gold >> t7_build_rag >> t8_complete_lineage
