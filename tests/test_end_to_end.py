"""
End-to-End Integration Test for Capstone Data & AI Pipeline
"""

import os
import pytest
from src.ingestion.producer import publish_events
from src.ingestion.consumer import process_ingestion
from src.quality.ge_suite import run_intentional_failing_quality_gate
from src.rag.chunker import chunk_text
from src.rag.hybrid_search import HybridSearchEngine
from src.rag.reranker import CandidateReranker


def test_full_pipeline_flow():
    # 1. Ingestion & DLQ
    pub_res = publish_events()
    assert pub_res["status"] == "SUCCESS"

    ingest_res = process_ingestion()
    assert ingest_res["valid_count"] == 5
    assert ingest_res["quarantine_count"] == 3

    # 2. Intentional Quality Gate Failure Proof
    fail_res = run_intentional_failing_quality_gate()
    assert fail_res["quality_gate_passed"] is False

    # 3. Hybrid RAG Search
    sample_text = "Delta Lake MERGE upsert updates matching records and inserts new ones."
    chunks = chunk_text(sample_text, chunk_size=100)
    assert len(chunks) >= 1

    sample_doc_chunk = {
        "chunk_id": "TEST_C1",
        "doc_id": "TEST_DOC",
        "title": "Delta Lake Test",
        "author": "Tester",
        "category": "Data Engineering",
        "text": sample_text,
        "citation_label": "[Doc ID: TEST_DOC, Chunk: 0]"
    }

    engine = HybridSearchEngine()
    engine.index_chunks([sample_doc_chunk])

    dense = engine.dense_search("Delta Lake MERGE", top_k=1)
    bm25 = engine.bm25_search("Delta Lake MERGE", top_k=1)
    fused = engine.reciprocal_rank_fusion(dense, bm25, top_k=1)

    assert len(fused) == 1

    reranker = CandidateReranker()
    reranked = reranker.rerank("Delta Lake MERGE", fused, top_k=1)
    assert len(reranked) == 1
