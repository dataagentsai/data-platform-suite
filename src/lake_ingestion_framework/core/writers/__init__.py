"""Writers — sinks the framework produces to.

In cloud, ADF (or other realm engines) own the write — the framework
only generates the JSON. In local mode, the ADF Mimic invokes a writer
directly. ``LocalDeltaWriter`` lives here because writing Delta is
realm-agnostic — any future local executor (e.g., a databricks-native
local mimic) writes Delta the same way.

See ``docs/adf_mimic_design.md`` §3.2.
"""

from lake_ingestion_framework.core.writers.local_delta import (
    LocalDeltaWriter,
    WriteStats,
)

__all__ = ["LocalDeltaWriter", "WriteStats"]
