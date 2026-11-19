"""The run-log reader port (dataplatform.obs.runlog) with no run log connected: level 1 of ADR-024 rule 6.

A customer with only the platform's telemetry gets the same screens and tools; every column that needs a run log
stays empty until one is connected (obs/ops.py fills job-level rows from the telemetry, origin `telemetry`).
"""

from __future__ import annotations

from datetime import datetime

from dataplatform.obs.runlog import RunLogSnapshot


class NullRunLog:
    def snapshot(self, domain: str, since: datetime, until: datetime) -> RunLogSnapshot:
        return RunLogSnapshot()
