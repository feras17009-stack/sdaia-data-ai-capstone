"""
Semantic Document Chunker Preserving Document & Category Metadata.
"""

from typing import List, Dict, Any


class SemanticChunker:
    """Splits article content into overlapping chunks while attaching article metadata."""

    def __init__(self, chunk_size: int = 200, chunk_overlap: int = 40):
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap

    def chunk_article(self, article: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Splits article text into chunks of specified length and overlap.
        Preserves article_id, chunk_id, title, category metadata.
        """
        text = article.get("content", "")
        article_id = article.get("article_id", "unknown")
        title = article.get("title", "")
        category = article.get("category", "")

        if not text:
            return []

        chunks = []
        step = self.chunk_size - self.chunk_overlap
        if step <= 0:
            step = self.chunk_size

        start = 0
        chunk_idx = 0
        while start < len(text):
            end = min(start + self.chunk_size, len(text))
            chunk_text = text[start:end]
            
            chunk_record = {
                "chunk_id": f"{article_id}_chunk_{chunk_idx:03d}",
                "article_id": article_id,
                "title": title,
                "category": category,
                "text": chunk_text,
                "start_char": start,
                "end_char": end
            }
            chunks.append(chunk_record)

            if end == len(text):
                break
            start += step
            chunk_idx += 1

        return chunks
