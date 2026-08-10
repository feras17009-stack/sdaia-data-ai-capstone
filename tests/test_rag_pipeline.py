"""
Unit tests for RAG Chunking, Hybrid Search (Dense + BM25 + RRF), and Cross-Encoder Reranking
"""

import pytest
from src.rag.chunker import chunk_text
from src.rag.hybrid_search import HybridSearchEngine
from src.rag.reranker import CandidateReranker


def test_chunk_text_overlap():
    text = "Word " * 200
    chunks = chunk_text(text, chunk_size=200, overlap=30)
    assert len(chunks) > 1
    assert isinstance(chunks[0], str)


def test_hybrid_search_rrf_fusion():
    sample_chunks = [
        {
            "chunk_id": "C1",
            "doc_id": "ART-101",
            "title": "Delta Lake Upserts",
            "author": "Dr. Sarah",
            "category": "Data Engineering",
            "text": "Delta Lake MERGE provides ACID transactions and upserts keyed on a business key.",
            "citation_label": "[Doc ID: ART-101, Chunk: 0]"
        },
        {
            "chunk_id": "C2",
            "doc_id": "ART-102",
            "title": "Hybrid RAG Search",
            "author": "Mohammed Al-Beladi",
            "category": "AI",
            "text": "Dense vector retrieval combines with BM25 keyword search using Reciprocal Rank Fusion.",
            "citation_label": "[Doc ID: ART-102, Chunk: 0]"
        }
    ]

    engine = HybridSearchEngine()
    engine.index_chunks(sample_chunks)

    dense_hits = engine.dense_search("Delta Lake MERGE", top_k=2)
    bm25_hits = engine.bm25_search("Delta Lake MERGE", top_k=2)
    fused_hits = engine.reciprocal_rank_fusion(dense_hits, bm25_hits, top_k=2)

    assert len(fused_hits) > 0
    assert "rrf_score" in fused_hits[0]


def test_reranker_scoring():
    reranker = CandidateReranker()
    candidate_chunks = [
        {"chunk_id": "C1", "text": "Delta Lake schema enforcement blocks invalid columns.", "rrf_score": 0.02},
        {"chunk_id": "C2", "text": "Kafka producer writes to raw topics.", "rrf_score": 0.01}
    ]
    reranked = reranker.rerank("schema enforcement", candidate_chunks, top_k=2)
    assert len(reranked) == 2
    assert "rerank_score" in reranked[0]
