"""Lineage before the engine's catalog (X7, LIN-01, B9): the engine's catalog records lineage from bronze on; the hops
before it (source system -> mover -> landing -> bronze) are only in our file log. This turns file-log records into
run events in the OpenLineage shape (an open standard), with the platform's run id as the run, so they join the
engine's lineage and the run log on one id (connect the dots). Pure: the emitter port sends them wherever the binding
says (a lineage service, the ops tables).

    events_from_files   one COMPLETE event per loaded batch (inputs: the source dataset; outputs: the bronze table),
                        one FAIL event per held or rejected batch (no output: nothing reached bronze)
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from dataplatform.meta.port import FileRecord

PRODUCER = "https://github.com/dataagentsai/data-platform-suite"
SCHEMA_URL = "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent"
LOADED = ("loaded",)
HELD = ("rejected_contract", "rejected_volume", "held_mass_delete")


def events_from_files(files: Iterable[FileRecord], system_of: dict[str, str], env: str, at: str,
                      bronze_namespace: str = "lakehouse") -> list[dict[str, Any]]:
    """Run events for the batches in `files`. `system_of` maps a source name to its system (erp -> erp_db), which
    names the input dataset's namespace; `at` is the event time (ISO, from the clock port). Duplicates skipped by the
    lander produce no event: nothing moved."""
    out = []
    for f in files:
        if f.status not in LOADED + HELD:
            continue
        loaded = f.status in LOADED
        event: dict[str, Any] = {
            "eventType": "COMPLETE" if loaded else "FAIL",
            "eventTime": at,
            "producer": PRODUCER,
            "schemaURL": SCHEMA_URL,
            "run": {"runId": f.run_id, "facets": {"batch": {"batch_key": f.batch_key, "window": f.window,
                                                            "file": f.file_name, "rows": f.rows,
                                                            "declared_rows": f.declared_rows,
                                                            "status": f.status, "reason": f.reason}}},
            "job": {"namespace": f"dataplatform.{env}", "name": f"land.{f.source}.{f.dataset}"},
            "inputs": [{"namespace": system_of.get(f.source, f.source), "name": f.dataset,
                        "facets": {"delivery": {"file": f.file_name, "checksum": f.checksum}}}],
            "outputs": [{"namespace": bronze_namespace, "name": f"{env}_bronze.{f.source}.{f.dataset}"}] if loaded
            else [],
        }
        out.append(event)
    return out
