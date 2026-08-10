"""
Kafka Consumer module for schema validation, Dead-Letter Topic (DLQ) routing, & Quarantine storage.
"""

import json
import os
import uuid
from datetime import datetime
from typing import Dict, Any, List

from pydantic import ValidationError

from src.ingestion.schemas import ArticleEvent, QuarantineRecord

try:
    from kafka import KafkaConsumer, KafkaProducer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False


def process_ingestion(
    bootstrap_servers: str = "localhost:9092",
    raw_topic: str = "sdaia-raw-tech-events",
    dlq_topic: str = "sdaia-quarantine-dlq",
    raw_dir: str = "./data/raw_sample",
    quarantine_dir: str = "./data/quarantine_dlq"
) -> Dict[str, Any]:
    """
    Consumes raw events, enforces Pydantic schema validation contract,
    routes valid events to Bronze buffer, and routes malformed events to DLQ Quarantine.
    """
    os.makedirs(raw_dir, exist_ok=True)
    os.makedirs(quarantine_dir, exist_ok=True)

    raw_payloads = []

    # 1. Try reading from Kafka, fallback to raw_batch.json buffer if Kafka unreachable
    if KAFKA_AVAILABLE:
        try:
            consumer = KafkaConsumer(
                raw_topic,
                bootstrap_servers=bootstrap_servers,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                group_id="sdaia-ingestion-group",
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                consumer_timeout_ms=3000
            )
            for msg in consumer:
                raw_payloads.append(msg.value)
            consumer.close()
        except Exception as e:
            print(f"[Consumer] Kafka consume notice: {e}. Reading from raw storage buffer...")

    if not raw_payloads:
        raw_file = os.path.join(raw_dir, "raw_batch.json")
        if os.path.exists(raw_file):
            with open(raw_file, "r", encoding="utf-8") as f:
                raw_payloads = json.load(f)

    validated_events: List[Dict[str, Any]] = []
    quarantined_records: List[Dict[str, Any]] = []

    dlq_producer = None
    if KAFKA_AVAILABLE:
        try:
            dlq_producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                request_timeout_ms=2000
            )
        except Exception:
            dlq_producer = None

    # 2. Schema Validation Loop
    for payload in raw_payloads:
        try:
            # Enforce data contract
            event_obj = ArticleEvent(**payload)
            validated_events.append(event_obj.model_dump())
        except ValidationError as val_err:
            # Extract specific rejection rationale
            errors = val_err.errors()
            failed_field = errors[0]["loc"][0] if errors and "loc" in errors[0] else "unknown"
            msg = errors[0]["msg"] if errors else str(val_err)

            q_record = QuarantineRecord(
                quarantine_id=f"QLOG-{uuid.uuid4().hex[:8].upper()}",
                rejection_timestamp=datetime.utcnow().isoformat() + "Z",
                rejection_reason=f"Validation Failed: {msg}",
                failed_field=str(failed_field),
                raw_payload=payload
            ).model_dump()

            quarantined_records.append(q_record)

            # Publish to Dead-Letter Topic
            if dlq_producer:
                try:
                    dlq_producer.send(dlq_topic, value=q_record)
                except Exception as p_err:
                    print(f"[Consumer] Could not send to DLQ topic: {p_err}")

    if dlq_producer:
        dlq_producer.flush()

    # 3. Persist Validated & Quarantined records
    validated_path = os.path.join(raw_dir, "validated_records.json")
    with open(validated_path, "w", encoding="utf-8") as f:
        json.dump(validated_events, f, indent=2)

    quarantine_path = os.path.join(quarantine_dir, "quarantine_records.json")
    with open(quarantine_path, "w", encoding="utf-8") as f:
        json.dump(quarantined_records, f, indent=2)

    print(f"[Ingestion Boundary] Total Received: {len(raw_payloads)}")
    print(f"  |-- Validated Events: {len(validated_events)} -> {validated_path}")
    print(f"  |-- Quarantined DLQ Events: {len(quarantined_records)} -> {quarantine_path}")

    return {
        "total_processed": len(raw_payloads),
        "valid_count": len(validated_events),
        "quarantine_count": len(quarantined_records),
        "validated_path": validated_path,
        "quarantine_path": quarantine_path
    }


if __name__ == "__main__":
    process_ingestion()
