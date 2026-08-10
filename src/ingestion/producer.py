"""
Kafka Producer module for publishing valid & malformed events to Kafka raw topic.
"""

import json
import os
import time
from datetime import datetime
from typing import List, Dict, Any

try:
    from kafka import KafkaProducer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False


def generate_sample_dataset() -> List[Dict[str, Any]]:
    """Generates a mixture of valid records and intentional malformed records."""
    return [
        # --- VALID RECORDS (Happy Path) ---
        {
            "article_id": "ART-101",
            "title": "Scaling Distributed Delta Lakes with PySpark and Airflow",
            "author": "Dr. Sarah Al-Otaibi",
            "category": "Data Engineering",
            "content": "Delta Lake provides ACID transactions and time travel capability for Spark data lakes. In this comprehensive guide, we explore how schema enforcement and merge upserts keep data clean at enterprise scale.",
            "views": 1420,
            "rating": 4.8,
            "published_timestamp": "2026-08-01T10:00:00Z"
        },
        {
            "article_id": "ART-102",
            "title": "Building Production RAG Systems with Hybrid Vector & BM25 Search",
            "author": "Mohammed Al-Beladi",
            "category": "AI",
            "content": "Dense vector retrieval excels at semantic context matching, while BM25 keyword search catches exact technical terms. Fusing both using Reciprocal Rank Fusion (RRF) delivers superior grounded citations.",
            "views": 2890,
            "rating": 4.9,
            "published_timestamp": "2026-08-02T14:30:00Z"
        },
        {
            "article_id": "ART-103",
            "title": "Zero Trust Cloud Infrastructure & Security Controls",
            "author": "Fahad Al-Ghamdi",
            "category": "Cybersecurity",
            "content": "Securing multi-tenant cloud workloads requires identity-centric network perimeters and real-time anomaly detection pipelines built on streaming telemetry.",
            "views": 950,
            "rating": 4.6,
            "published_timestamp": "2026-08-03T09:15:00Z"
        },
        {
            "article_id": "ART-104",
            "title": "Microservices Integration Patterns with Apache Kafka",
            "author": "Laila Al-Hassan",
            "category": "Software Architecture",
            "content": "Event-driven architectures leverage Kafka topic partitions, dead-letter queues, and schema registries to achieve asynchronous decoupling and strict ingestion guarantees.",
            "views": 1820,
            "rating": 4.7,
            "published_timestamp": "2026-08-04T11:45:00Z"
        },
        {
            "article_id": "ART-105",
            "title": "Large Language Model Fine-Tuning on Saudi Sovereign Cloud",
            "author": "Dr. Sarah Al-Otaibi",
            "category": "AI",
            "content": "Fine-tuning open-weights Arabic models requires high-throughput GPU clusters, data deduplication, and strict data governance policies aligned with national standards.",
            "views": 3100,
            "rating": 5.0,
            "published_timestamp": "2026-08-05T16:00:00Z"
        },
        # --- MALFORMED RECORDS (Failure Path - Schema Violation) ---
        {
            "article_id": "ART-BAD-01",
            "title": "A",  # Title too short (< 3 chars)
            "author": "Anonymous",
            "category": "AI",
            "content": "Invalid title test payload for quarantine testing.",
            "views": 10,
            "rating": 4.0,
            "published_timestamp": "2026-08-06T12:00:00Z"
        },
        {
            "article_id": "ART-BAD-02",
            "title": "Malformed Views and Rating Event",
            "author": "Bad Data Generator",
            "category": "Cloud",
            "content": "This payload contains negative views and an out-of-range rating score.",
            "views": -500,  # Invalid: negative views
            "rating": 9.9,   # Invalid: rating > 5.0
            "published_timestamp": "2026-08-06T13:00:00Z"
        },
        {
            "article_id": "ART-BAD-03",
            "title": "Unapproved Category Event",
            "author": "Unknown User",
            "category": "CryptoSpeculation",  # Invalid category
            "content": "Payload using category outside allowed domain contracts.",
            "views": 50,
            "rating": 2.5,
            "published_timestamp": "INVALID-DATE-FORMAT"  # Invalid timestamp format
        }
    ]


def publish_events(bootstrap_servers: str = "localhost:9092", topic: str = "sdaia-raw-tech-events", output_dir: str = "./data/raw_sample") -> Dict[str, Any]:
    """Publishes sample records to Kafka topic (or writes raw batch file if Kafka offline)."""
    os.makedirs(output_dir, exist_ok=True)
    events = generate_sample_dataset()

    raw_file = os.path.join(output_dir, "raw_batch.json")
    with open(raw_file, "w", encoding="utf-8") as f:
        json.dump(events, f, indent=2)

    sent_count = 0
    kafka_connected = False

    if KAFKA_AVAILABLE:
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                request_timeout_ms=3000
            )
            for evt in events:
                producer.send(topic, value=evt)
                sent_count += 1
            producer.flush()
            kafka_connected = True
            print(f"[Producer] Published {sent_count} events to Kafka topic '{topic}'")
        except Exception as e:
            print(f"[Producer] Kafka connection attempt failed ({e}). Fallback to file stream buffer.")
    
    return {
        "status": "SUCCESS",
        "total_events": len(events),
        "kafka_connected": kafka_connected,
        "raw_file_path": raw_file
    }


if __name__ == "__main__":
    publish_events()
