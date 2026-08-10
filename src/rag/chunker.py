"""
Document Chunker & Citation Metadata Enrichment Engine
"""

import os
from typing import List, Dict, Any


def chunk_text(text: str, chunk_size: int = 500, overlap: int = 50) -> List[str]:
    """Splits a document text body into overlapping text chunks."""
    if not text:
        return []
    chunks = []
    start = 0
    while start < len(text):
        end = start + chunk_size
        chunk = text[start:end]
        chunks.append(chunk.strip())
        if end >= len(text):
            break
        start += (chunk_size - overlap)
    return chunks


def prepare_document_chunks(
    silver_delta_path: str = "./data/delta/silver",
    chunk_size: int = 500,
    overlap: int = 50
) -> List[Dict[str, Any]]:
    """
    Reads documents from Silver Delta layer, chunks content, and attaches rich metadata
    for downstream citation tracking.
    """
    documents = []

    # Read records from Silver Delta table
    if os.path.exists(silver_delta_path):
        try:
            from src.lakehouse.spark_session import get_spark_session
            spark = get_spark_session("RAG-Chunker")
            df_silver = spark.read.format("delta").load(silver_delta_path)
            documents = [row.asDict() for row in df_silver.collect()]
        except Exception as e:
            print(f"[Chunker] Spark load fallback: {e}")

    # Fallback to direct raw sample if Delta not yet built
    if not documents:
        validated_path = "./data/raw_sample/validated_records.json"
        if os.path.exists(validated_path):
            import json
            with open(validated_path, "r", encoding="utf-8") as f:
                documents = json.load(f)

    chunk_records = []
    for doc in documents:
        doc_id = doc.get("article_id", "DOC-UNKNOWN")
        title = doc.get("title", "")
        author = doc.get("author", "")
        category = doc.get("category", "")
        content = doc.get("content", "")

        raw_chunks = chunk_text(content, chunk_size=chunk_size, overlap=overlap)
        for idx, c_text in enumerate(raw_chunks):
            chunk_records.append({
                "chunk_id": f"{doc_id}_C{idx}",
                "doc_id": doc_id,
                "chunk_index": idx,
                "title": title,
                "author": author,
                "category": category,
                "text": c_text,
                "citation_label": f"[Doc ID: {doc_id}, Chunk: {idx} | '{title[:35]}...' by {author}]"
            })

    print(f"[RAG Chunker] Prepared {len(chunk_records)} text chunks from {len(documents)} source documents.")
    return chunk_records


if __name__ == "__main__":
    chunks = prepare_document_chunks()
    if chunks:
        print(f"Sample Chunk 0 Citation Label: {chunks[0]['citation_label']}")
