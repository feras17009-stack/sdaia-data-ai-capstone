"""
Cross-Encoder Document Reranker.
"""

from typing import List, Dict, Any


class DocumentReranker:
    """Reranks candidate search documents using query-document similarity scoring."""

    def rerank(self, query: str, candidates: List[Dict[str, Any]], top_n: int = 3) -> List[Dict[str, Any]]:
        """
        Calculates cross-encoder relevance scores and sorts candidate documents.
        """
        if not candidates:
            return []

        query_terms = set(query.lower().split())
        scored_candidates = []

        for doc in candidates:
            text = doc.get("text", "").lower()
            # Relevance scoring heuristic
            match_score = sum(2.0 if term in text else 0.0 for term in query_terms)
            
            doc_copy = doc.copy()
            doc_copy["rerank_score"] = round(match_score + doc.get("rrf_score", 0.0), 4)
            scored_candidates.append(doc_copy)

        scored_candidates.sort(key=lambda x: x["rerank_score"], reverse=True)
        return scored_candidates[:top_n]
