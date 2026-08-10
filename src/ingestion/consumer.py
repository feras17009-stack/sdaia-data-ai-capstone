"""
Ingestion Consumer Loop & Pydantic Validation with DLQ Quarantine Routing.
"""

import os
import json
import uuid
from typing import List, Dict, Any
from pydantic import ValidationError
from src.ingestion.schemas import ArticleContract, format_dlq_payload


def process_incoming_payloads(
    batch: List[Dict[str, Any]],
    valid_buffer_file: str,
    dlq_dir: str
) -> Dict[str, int]:
    """
    Processes a batch of raw article dicts:
    - Validates against ArticleContract.
    - Saves valid records to valid_buffer_file JSON array.
    - Routes invalid payloads to dlq_dir as individual quarantine JSON files.
    """
    valid_records = []
    quarantine_count = 0

    os.makedirs(os.path.dirname(valid_buffer_file), exist_ok=True)
    os.makedirs(dlq_dir, exist_ok=True)

    for item in batch:
        try:
            contract = ArticleContract(**item)
            valid_records.append(contract.model_dump())
        except ValidationError as ve:
            quarantine_count += 1
            # Extract first failing field
            first_err = ve.errors()[0]
            field_name = str(first_err["loc"][0]) if first_err["loc"] else "unknown"
            err_msg = first_err["msg"]

            dlq_record = format_dlq_payload(
                raw_payload=item,
                error_type="ValidationError",
                field_failed=field_name,
                error_message=err_msg
            )

            # Save quarantine file
            q_filename = f"quarantine_{dlq_record['quarantine_id']}.json"
            with open(os.path.join(dlq_dir, q_filename), "w", encoding="utf-8") as f_dlq:
                json.dump(dlq_record, f_dlq, indent=2)
        except Exception as ex:
            quarantine_count += 1
            dlq_record = format_dlq_payload(
                raw_payload=item,
                error_type=type(ex).__name__,
                field_failed="general",
                error_message=str(ex)
            )
            q_filename = f"quarantine_{dlq_record['quarantine_id']}.json"
            with open(os.path.join(dlq_dir, q_filename), "w", encoding="utf-8") as f_dlq:
                json.dump(dlq_record, f_dlq, indent=2)

    # Append/write valid records
    if os.path.exists(valid_buffer_file):
        try:
            with open(valid_buffer_file, "r", encoding="utf-8") as f_exist:
                existing = json.load(f_exist)
        except Exception:
            existing = []
        existing.extend(valid_records)
        valid_records = existing

    with open(valid_buffer_file, "w", encoding="utf-8") as f_out:
        json.dump(valid_records, f_out, indent=2)

    return {
        "processed": len(batch),
        "valid_count": len(valid_records),
        "quarantined_count": quarantine_count
    }
