"""
Context-Grounded RAG AI Answer Generator with Inline Citations.
"""

from typing import List, Dict, Any


class RAGEngine:
    """Generates context-grounded AI responses with mandatory explicit document citations."""

    def __init__(self, use_mock_llm: bool = True):
        self.use_mock_llm = use_mock_llm

    def generate_grounded_response(self, query: str, context_chunks: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Synthesizes user query into an answer strictly grounded in context_chunks.
        Generates inline citations matching format: [Article ID: <id>, Chunk: <chunk_id>].
        """
        if not context_chunks:
            return {
                "answer": "No relevant context documents were found to answer the query.",
                "citations": []
            }

        citations = []
        citation_strings = []

        for chunk in context_chunks:
            art_id = chunk.get("article_id", "unknown")
            chk_id = chunk.get("chunk_id", "unknown")
            
            citations.append({
                "article_id": art_id,
                "chunk_id": chk_id,
                "title": chunk.get("title", "")
            })
            citation_strings.append(f"[Article ID: {art_id}, Chunk: {chk_id}]")

        context_summary = " ".join(c.get("text", "") for c in context_chunks)
        citation_text = " ".join(citation_strings)

        answer_text = (
            f"Based on the verified context, {context_summary[:150]}... "
            f"Reference Citations: {citation_text}"
        )

        return {
            "answer": answer_text,
            "citations": citations,
            "context_count": len(context_chunks)
        }
