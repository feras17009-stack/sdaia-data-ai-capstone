"""
Unit tests for Hybrid Search (Dense + Lexical BM25 + RRF) and Grounded RAG Engine
"""

import pytest
from src.rag.chunker import chunk_text, prepare_document_chunks
from src.rag.hybrid_search import HybridSearchEngine
from src.rag.reranker import CandidateReranker
from src.rag.rag_engine import GroundedRAGEngine


def test_chunking_and_metadata_enrichment():
    text = "Delta Lake provides ACID transactions and time travel capability for Spark data lakes. " * 5
    chunks = chunk_text(text, chunk_size=150, overlap=20)
    assert len(chunks) > 1


def test_hybrid_search_rrf_and_reranking():
    sample_chunks = [
        {
            "chunk_id": "C1",
            "doc_id": "ART-101",
            "title": "Scaling Delta Lakes",
            "author": "Dr. Sarah",
            "category": "Data Engineering",
            "text": "Delta Lake MERGE provides ACID transactions and upserts keyed on a business key.",
            "citation_label": "[Doc ID: ART-101, Chunk: 0]"
        },
        {
            "chunk_id": "C2",
            "doc_id": "ART-102",
            "title": "Hybrid RAG Engine",
            "author": "Mohammed Al-Beladi",
            "category": "AI",
            "text": "Dense vector retrieval combines with BM25 keyword search using Reciprocal Rank Fusion.",
            "citation_label": "[Doc ID: ART-102, Chunk: 0]"
        }
    ]

    engine = HybridSearchEngine()
    engine.index_chunks(sample_chunks)

    dense = engine.dense_search("Delta Lake MERGE", top_k=2)
    bm25 = engine.bm25_search("Delta Lake MERGE", top_k=2)
    fused = engine.reciprocal_rank_fusion(dense, bm25, top_k=2)

    assert len(fused) > 0
    assert "rrf_score" in fused[0]

    reranker = CandidateReranker()
    reranked = reranker.rerank("Delta Lake MERGE", fused, top_k=1)
    assert len(reranked) == 1
    assert "rerank_score" in reranked[0]


def test_grounded_rag_engine_citations():
    rag = GroundedRAGEngine()
    rag.initialize_index()
    response = rag.query("How does Delta Lake MERGE work?")

    assert "answer" in response
    assert "citations" in response
    assert response["search_evaluation"]["final_reranked_count"] > 0
