"""
OpenLineage Event Tracker with Resilient File Log Fallback.
"""

import os
import json
import uuid
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional


class OpenLineageTracker:
    """Emits OpenLineage-compliant run events (START, COMPLETE, FAIL) for job governance."""

    def __init__(self, job_name: str, namespace: str = "sdaia.capstone.pipeline", output_dir: Optional[str] = None):
        self.job_name = job_name
        self.namespace = namespace
        self.output_dir = output_dir or os.path.join(".", "data", "openlineage_events")
        os.makedirs(self.output_dir, exist_ok=True)

    def _create_base_event(self, event_type: str, run_id: Optional[str] = None) -> Dict[str, Any]:
        return {
            "eventType": event_type,
            "eventTime": datetime.now(timezone.utc).isoformat(),
            "run": {
                "runId": run_id or str(uuid.uuid4())
            },
            "job": {
                "namespace": self.namespace,
                "name": self.job_name
            },
            "producer": "https://github.com/sdaia-capstone/lineage-tracker"
        }

    def _persist_event(self, event: Dict[str, Any]):
        file_path = os.path.join(self.output_dir, f"lineage_{event['eventType'].lower()}_{event['run']['runId'][:8]}.json")
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(event, f, indent=2)

    def emit_start_event(self, inputs: List[str], outputs: List[str]) -> Dict[str, Any]:
        event = self._create_base_event("START")
        event["inputs"] = [{"namespace": self.namespace, "name": inp} for inp in inputs]
        event["outputs"] = [{"namespace": self.namespace, "name": outp} for outp in outputs]
        self._persist_event(event)
        return event

    def emit_complete_event(self, run_id: str) -> Dict[str, Any]:
        event = self._create_base_event("COMPLETE", run_id=run_id)
        event["inputs"] = []
        event["outputs"] = []
        self._persist_event(event)
        return event

    def emit_fail_event(self, run_id: str, error_message: str) -> Dict[str, Any]:
        event = self._create_base_event("FAIL", run_id=run_id)
        event["error"] = {"message": error_message}
        self._persist_event(event)
        return event
