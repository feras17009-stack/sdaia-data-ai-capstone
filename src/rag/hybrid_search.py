"""
Hybrid Search Engine: Dense Vector Search + BM25 Lexical Search + Reciprocal Rank Fusion (RRF)
"""

import math
import os
from typing import List, Dict, Any

try:
    from sentence_transformers import SentenceTransformer
    SENTENCE_TRANSFORMERS_AVAILABLE = True
except ImportError:
    SENTENCE_TRANSFORMERS_AVAILABLE = False

try:
    from rank_bm25 import BM25Okapi
    BM25_AVAILABLE = True
except ImportError:
    BM25_AVAILABLE = False

try:
    import chromadb
    CHROMADB_AVAILABLE = True
except ImportError:
    CHROMADB_AVAILABLE = False


class HybridSearchEngine:
    """
    Combines Dense Vector Retrieval and BM25 Lexical Keyword Search
    fused using Reciprocal Rank Fusion (RRF).
    """

    def __init__(self, embedding_model_name: str = "sentence-transformers/all-MiniLM-L6-v2"):
        self.embedding_model_name = embedding_model_name
        self.model = None
        if SENTENCE_TRANSFORMERS_AVAILABLE:
            try:
                self.model = SentenceTransformer(embedding_model_name)
            except Exception as e:
                print(f"[HybridSearch] Embedding model load notice: {e}")

        self.bm25 = None
        self.chunks: List[Dict[str, Any]] = []
        self.tokenized_corpus = []
        self.chroma_client = None
        self.collection = None

    def index_chunks(self, chunks: List[Dict[str, Any]], vector_db_path: str = "./data/vector_store"):
        """Indexes chunks into ChromaDB Vector Store and BM25 Lexical Index."""
        self.chunks = chunks
        if not chunks:
            print("[HybridSearch] Warning: No chunks provided to index.")
            return

        # 1. Initialize BM25 Lexical Index
        if BM25_AVAILABLE:
            self.tokenized_corpus = [chunk["text"].lower().split() for chunk in chunks]
            self.bm25 = BM25Okapi(self.tokenized_corpus)

        # 2. Initialize ChromaDB Vector Store
        if CHROMADB_AVAILABLE and self.model:
            try:
                os.makedirs(vector_db_path, exist_ok=True)
                self.chroma_client = chromadb.PersistentClient(path=vector_db_path)
                # Create or get collection
                self.collection = self.chroma_client.get_or_create_collection("sdaia_tech_chunks")

                # Compute dense embeddings
                texts = [c["text"] for c in chunks]
                embeddings = self.model.encode(texts).tolist()
                ids = [c["chunk_id"] for c in chunks]
                metadatas = [
                    {
                        "doc_id": c["doc_id"],
                        "title": c["title"],
                        "author": c["author"],
                        "category": c["category"],
                        "citation_label": c["citation_label"]
                    }
                    for c in chunks
                ]

                # Upsert into ChromaDB
                self.collection.upsert(ids=ids, embeddings=embeddings, documents=texts, metadatas=metadatas)
                print(f"[HybridSearch] ChromaDB vector store indexed {len(ids)} embeddings.")
            except Exception as e:
                print(f"[HybridSearch] ChromaDB indexing note: {e}")

    def dense_search(self, query: str, top_k: int = 5) -> List[Dict[str, Any]]:
        """Dense Vector Retrieval."""
        if not self.chunks:
            return []

        # If ChromaDB collection active
        if self.collection and self.model:
            q_emb = self.model.encode([query]).tolist()
            res = self.collection.query(query_embeddings=q_emb, n_results=min(top_k, len(self.chunks)))
            results = []
            if res and res["ids"]:
                for idx in range(len(res["ids"][0])):
                    chunk_id = res["ids"][0][idx]
                    doc_chunk = next((c for c in self.chunks if c["chunk_id"] == chunk_id), None)
                    if doc_chunk:
                        results.append({**doc_chunk, "score": 1.0 - (res["distances"][0][idx] if res["distances"] else 0.5)})
            return results

        # Simple cosine fallback if ChromaDB persistent store loading
        if self.model:
            q_emb = self.model.encode(query)
            c_embs = self.model.encode([c["text"] for c in self.chunks])
            scores = []
            for idx, c_emb in enumerate(c_embs):
                # Cosine similarity
                dot = sum(a * b for a, b in zip(q_emb, c_emb))
                norm_a = math.sqrt(sum(a * a for a in q_emb))
                norm_b = math.sqrt(sum(b * b for b in c_emb))
                sim = dot / (norm_a * norm_b + 1e-9)
                scores.append((sim, self.chunks[idx]))
            scores.sort(key=lambda x: x[0], reverse=True)
            return [{**item[1], "score": float(item[0])} for item in scores[:top_k]]

        # Keyword match fallback
        return self.chunks[:top_k]

    def bm25_search(self, query: str, top_k: int = 5) -> List[Dict[str, Any]]:
        """Lexical BM25 Keyword Retrieval."""
        if not self.bm25 or not self.chunks:
            # Fallback string matching
            tokenized_query = query.lower().split()
            scored = []
            for c in self.chunks:
                score = sum(1 for tok in tokenized_query if tok in c["text"].lower())
                scored.append((score, c))
            scored.sort(key=lambda x: x[0], reverse=True)
            return [{**item[1], "score": float(item[0])} for item in scored[:top_k]]

        tokenized_query = query.lower().split()
        scores = self.bm25.get_scores(tokenized_query)
        scored_chunks = list(zip(scores, self.chunks))
        scored_chunks.sort(key=lambda x: x[0], reverse=True)
        return [{**item[1], "score": float(item[0])} for item in scored_chunks[:top_k]]

    def reciprocal_rank_fusion(
        self,
        dense_results: List[Dict[str, Any]],
        bm25_results: List[Dict[str, Any]],
        rrf_k: int = 60,
        top_k: int = 5
    ) -> List[Dict[str, Any]]:
        """
        Reciprocal Rank Fusion (RRF) algorithm:
        RRF_Score(d) = sum(1 / (k + rank(d))) across dense and BM25 lists.
        """
        rrf_scores: Dict[str, float] = {}
        chunk_map: Dict[str, Dict[str, Any]] = {}

        # 1. Process Dense Ranks
        for rank, item in enumerate(dense_results, start=1):
            cid = item["chunk_id"]
            chunk_map[cid] = item
            rrf_scores[cid] = rrf_scores.get(cid, 0.0) + (1.0 / (rrf_k + rank))

        # 2. Process BM25 Ranks
        for rank, item in enumerate(bm25_results, start=1):
            cid = item["chunk_id"]
            chunk_map[cid] = item
            rrf_scores[cid] = rrf_scores.get(cid, 0.0) + (1.0 / (rrf_k + rank))

        # Sort by RRF score descending
        fused = [
            {**chunk_map[cid], "rrf_score": score}
            for cid, score in sorted(rrf_scores.items(), key=lambda x: x[1], reverse=True)
        ]
        return fused[:top_k]


if __name__ == "__main__":
    from src.rag.chunker import prepare_document_chunks
    chunks = prepare_document_chunks()
    engine = HybridSearchEngine()
    engine.index_chunks(chunks)
    dense_hits = engine.dense_search("Delta Lake MERGE upsert schema enforcement", top_k=3)
    bm25_hits = engine.bm25_search("Delta Lake MERGE upsert schema enforcement", top_k=3)
    fused_hits = engine.reciprocal_rank_fusion(dense_hits, bm25_hits, top_k=3)
    print(f"[HybridSearch Test] Top RRF Fused Result ID: {fused_hits[0]['chunk_id'] if fused_hits else 'None'}")
