"""
Grounded RAG Answer Generation Engine with Inline Citations
"""

from typing import Dict, Any, List

from src.rag.chunker import prepare_document_chunks
from src.rag.hybrid_search import HybridSearchEngine
from src.rag.reranker import CandidateReranker


class GroundedRAGEngine:
    """
    End-to-End Grounded Retrieval-Augmented Generation (RAG) System.
    Retrieves context via Hybrid Search (Vector + BM25 + RRF) and Cross-Encoder Reranking,
    and synthesizes answers grounded in retrieved context with explicit citations.
    """

    def __init__(self, silver_delta_path: str = "./data/delta/silver"):
        self.silver_delta_path = silver_delta_path
        self.chunker = prepare_document_chunks
        self.search_engine = HybridSearchEngine()
        self.reranker = CandidateReranker()
        self.chunks: List[Dict[str, Any]] = []
        self._is_indexed = False

    def initialize_index(self):
        """Prepares chunks and builds hybrid vector + lexical index."""
        self.chunks = self.chunker(self.silver_delta_path)
        self.search_engine.index_chunks(self.chunks)
        self._is_indexed = True

    def query(self, user_query: str, top_k_search: int = 5, top_k_rerank: int = 3) -> Dict[str, Any]:
        """
        Executes query retrieval, reranking, context grounding, and citation generation.
        """
        if not self._is_indexed:
            self.initialize_index()

        # 1. Dense Vector Search
        dense_hits = self.search_engine.dense_search(user_query, top_k=top_k_search)

        # 2. Lexical BM25 Search
        bm25_hits = self.search_engine.bm25_search(user_query, top_k=top_k_search)

        # 3. Reciprocal Rank Fusion (RRF)
        fused_hits = self.search_engine.reciprocal_rank_fusion(dense_hits, bm25_hits, top_k=top_k_search)

        # 4. Cross-Encoder Reranking
        final_reranked_chunks = self.reranker.rerank(user_query, fused_hits, top_k=top_k_rerank)

        # 5. Synthesize Grounded Response with Inline Citations
        if not final_reranked_chunks:
            return {
                "query": user_query,
                "answer": "No relevant context documents were found in the knowledge base to answer this query.",
                "citations": [],
                "retrieved_chunks": []
            }

        # Build grounded synthesis text
        context_snippets = []
        citations_list = []

        for idx, chunk in enumerate(final_reranked_chunks, start=1):
            doc_id = chunk.get("doc_id", "DOC-UNKNOWN")
            chunk_id = chunk.get("chunk_id", "C0")
            title = chunk.get("title", "Untitled Document")
            author = chunk.get("author", "Unknown Author")
            text = chunk.get("text", "")

            citation_ref = f"[{idx}] (Doc ID: {doc_id}, Chunk ID: {chunk_id})"
            context_snippets.append(f"{text} {citation_ref}")
            citations_list.append({
                "citation_num": idx,
                "doc_id": doc_id,
                "chunk_id": chunk_id,
                "title": title,
                "author": author,
                "rerank_score": chunk.get("rerank_score", 0.0)
            })

        # Generate structured answer grounded in context
        grounded_answer_body = (
            f"Based strictly on the retrieved knowledge base documents:\n\n"
            + "\n\n".join([f"Key Excerpt {i+1}: \"{c['text']}\" [{i+1}]" for i, c in enumerate(final_reranked_chunks)])
        )

        formatted_citations = "\n".join([
            f"  - [{c['citation_num']}] Document '{c['title']}' by {c['author']} (ID: {c['doc_id']}, Chunk: {c['chunk_id']})"
            for c in citations_list
        ])

        final_answer = (
            f"### Query Response\n{grounded_answer_body}\n\n"
            f"### Referenced Citations\n{formatted_citations}"
        )

        return {
            "query": user_query,
            "answer": final_answer,
            "citations": citations_list,
            "retrieved_chunks": final_reranked_chunks,
            "search_evaluation": {
                "dense_hits_count": len(dense_hits),
                "bm25_hits_count": len(bm25_hits),
                "rrf_fused_count": len(fused_hits),
                "final_reranked_count": len(final_reranked_chunks)
            }
        }


if __name__ == "__main__":
    engine = GroundedRAGEngine()
    result = engine.query("How does Delta Lake MERGE and schema enforcement work?")
    print(result["answer"])
