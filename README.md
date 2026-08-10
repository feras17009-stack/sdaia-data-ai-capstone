# Modern Data Engineering for AI Systems Capstone Platform

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

An enterprise-grade data and AI pipeline platform bridging real-time data engineering, lakehouse storage architecture, automated data quality governance, and Retrieval-Augmented Generation (RAG) AI systems.

---

## 🌟 Key Features

- **Real-Time Validated Ingestion:** Streams raw event payloads via Apache Kafka, validating contracts using Pydantic. Malformed payloads are isolated to a Dead-Letter Queue (DLQ) without breaking pipeline execution.
- **ACID Lakehouse Storage:** Manages raw data (Bronze), clean MERGE upserts (Silver), and aggregated metrics (Gold) using PySpark and Delta Lake 3.x with strict schema enforcement.
- **Automated Data Quality & Lineage:** Integrates Great Expectations failure gates to halt downstream DAG runs upon data quality degradation, tracking job telemetry with OpenLineage.
- **Hybrid RAG AI Search Engine:** Combines dense vector retrieval (ChromaDB), sparse lexical retrieval (BM25), Reciprocal Rank Fusion (RRF), and Cross-Encoder reranking to produce context-grounded AI responses with explicit document citations.
- **End-to-End Orchestration:** Multi-stage pipeline coordination via Apache Airflow.

---

## 📁 Repository Structure

```
.
├── PRD.md                  # Comprehensive Product Requirements Document
├── README.md               # Repository Overview & Quickstart Guide
├── src/                    # Core Source Code
│   ├── ingestion/          # Kafka Producer/Consumer & Pydantic Validation Schemas
│   ├── lakehouse/          # PySpark Delta Lakehouse (Bronze/Silver/Gold)
│   ├── quality/            # Great Expectations Suite & OpenLineage Telemetry
│   ├── rag/                # Hybrid Search, Chunking, Vector Storage & Reranking
│   └── utils/              # Helper functions & logger utilities
└── tests/                  # Automated Test Suites (pytest)
```

---

## 🚀 Quickstart Guide

### Prerequisites
- Python 3.10+
- Apache Spark 3.5+ & Delta Lake 3.x
- Apache Kafka (or local mock broker)

### Installation

1. **Clone the repository:**
   ```bash
   git clone https://github.com/feras17009-stack/sdaia-data-ai-capstone.git
   cd sdaia-data-ai-capstone
   ```

2. **Set up virtual environment:**
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

4. **Run automated test suite:**
   ```bash
   pytest tests/
   ```

---

## 📖 Product Requirements Document (PRD)

For detailed specifications, architectural diagrams, data model schemas, and evaluation benchmarks, refer to [PRD.md](PRD.md).

---

## 📜 License

Distributed under the MIT License. See `LICENSE` for more information.
