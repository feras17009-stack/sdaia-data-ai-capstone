"""
Cross-Encoder Reranker Module for Fine-Grained Candidate Scoring
"""

from typing import List, Dict, Any

try:
    from sentence_transformers import CrossEncoder
    CROSS_ENCODER_AVAILABLE = True
except ImportError:
    CROSS_ENCODER_AVAILABLE = False


class CandidateReranker:
    """Reranks RRF top candidate chunks using a Cross-Encoder model."""

    def __init__(self, model_name: str = "cross-encoder/ms-marco-MiniLM-L-6-v2"):
        self.model_name = model_name
        self.reranker = None
        if CROSS_ENCODER_AVAILABLE:
            try:
                self.reranker = CrossEncoder(model_name)
            except Exception as e:
                print(f"[Reranker] CrossEncoder model load note: {e}")

    def rerank(self, query: str, candidate_chunks: List[Dict[str, Any]], top_k: int = 3) -> List[Dict[str, Any]]:
        """Reranks candidate chunks for query relevance."""
        if not candidate_chunks:
            return []

        if self.reranker:
            pairs = [[query, chunk["text"]] for chunk in candidate_chunks]
            scores = self.reranker.predict(pairs)
            scored_candidates = []
            for idx, score in enumerate(scores):
                scored_candidates.append({**candidate_chunks[idx], "rerank_score": float(score)})
            scored_candidates.sort(key=lambda x: x["rerank_score"], reverse=True)
            return scored_candidates[:top_k]

        # Fallback keyword/RRF score reranking if CrossEncoder model unavailable offline
        scored_candidates = []
        q_words = set(query.lower().split())
        for chunk in candidate_chunks:
            match_count = sum(1 for w in q_words if w in chunk["text"].lower())
            base_score = chunk.get("rrf_score", 0.0) + (match_count * 0.1)
            scored_candidates.append({**chunk, "rerank_score": float(base_score)})

        scored_candidates.sort(key=lambda x: x["rerank_score"], reverse=True)
        return scored_candidates[:top_k]


if __name__ == "__main__":
    reranker = CandidateReranker()
    sample_chunks = [
        {"chunk_id": "C1", "text": "Delta Lake MERGE updates existing records and inserts new ones.", "rrf_score": 0.03},
        {"chunk_id": "C2", "text": "Zero Trust Security perimeters use API gateway credentials.", "rrf_score": 0.02}
    ]
    ranked = reranker.rerank("How does Delta Lake MERGE work?", sample_chunks, top_k=2)
    print(f"[Reranker Test] Top Ranked Chunk ID: {ranked[0]['chunk_id']}")
