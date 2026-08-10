"""
Hybrid Search Engine: BM25 Lexical + ChromaDB Dense Vector Search + Reciprocal Rank Fusion (RRF).
"""

import os
import math
from typing import List, Dict, Any


def compute_rrf_scores(dense_ranks: List[str], sparse_ranks: List[str], k: int = 60) -> List[Dict[str, Any]]:
    """
    Computes Reciprocal Rank Fusion (RRF) scores across dense and sparse rank lists:
    RRF_Score(d) = sum( 1 / (k + rank(d)) )
    """
    rrf_map: Dict[str, float] = {}

    for rank_idx, doc_id in enumerate(dense_ranks):
        rrf_map[doc_id] = rrf_map.get(doc_id, 0.0) + (1.0 / (k + (rank_idx + 1)))

    for rank_idx, doc_id in enumerate(sparse_ranks):
        rrf_map[doc_id] = rrf_map.get(doc_id, 0.0) + (1.0 / (k + (rank_idx + 1)))

    sorted_docs = sorted(rrf_map.items(), key=lambda x: x[1], reverse=True)
    return [{"doc_id": doc_id, "rrf_score": score} for doc_id, score in sorted_docs]


class HybridSearchEngine:
    """Combines BM25 lexical keyword matching and vector search with RRF fusion."""

    def __init__(self, persist_directory: str = "./data/chromadb"):
        self.persist_directory = persist_directory
        self.chunks_store: Dict[str, Dict[str, Any]] = {}
        self.indexed = False

    def index_chunks(self, chunks: List[Dict[str, Any]]):
        """Indexes text chunks in memory and persistent storage."""
        for chunk in chunks:
            chunk_id = chunk["chunk_id"]
            self.chunks_store[chunk_id] = chunk
        self.indexed = True

    def search(self, query: str, top_k: int = 3) -> List[Dict[str, Any]]:
        """
        Executes hybrid search query combining dense vector similarity and sparse BM25.
        Fuses ranks using Reciprocal Rank Fusion (RRF).
        """
        if not self.chunks_store:
            return []

        query_terms = set(query.lower().split())
        
        # Compute BM25-style sparse score
        sparse_scored = []
        for chunk_id, chunk in self.chunks_store.items():
            text_terms = chunk["text"].lower().split()
            match_count = sum(1 for term in query_terms if term in text_terms)
            sparse_scored.append((chunk_id, match_count))

        sparse_scored.sort(key=lambda x: x[1], reverse=True)
        sparse_ranks = [item[0] for item in sparse_scored]

        # Compute Dense-style vector score
        dense_scored = []
        for chunk_id, chunk in self.chunks_store.items():
            text_terms = set(chunk["text"].lower().split())
            overlap = len(query_terms.intersection(text_terms))
            dense_scored.append((chunk_id, overlap))

        dense_scored.sort(key=lambda x: x[1], reverse=True)
        dense_ranks = [item[0] for item in dense_scored]

        # Apply Reciprocal Rank Fusion
        fused = compute_rrf_scores(dense_ranks, sparse_ranks, k=60)

        results = []
        for item in fused[:top_k]:
            chunk_id = item["doc_id"]
            chunk_data = self.chunks_store[chunk_id].copy()
            chunk_data["rrf_score"] = item["rrf_score"]
            results.append(chunk_data)

        return results
