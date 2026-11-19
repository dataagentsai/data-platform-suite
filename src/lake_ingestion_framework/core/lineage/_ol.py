"""Shared OpenLineage plumbing — lazy import + serializer.

Centralised so each emitter (dbt manifest, Silver, future Source /
Activation) doesn't re-implement the import dance or guess at which
submodule houses ``Serde``. OL has churned the public surface across
versions; one place to update.
"""

from __future__ import annotations

import types
from typing import Any, cast

from lake_ingestion_framework.core.exceptions import LakeIngestError


class LineageEmitError(LakeIngestError):
    """The lineage emitter couldn't proceed — bad input or missing dep."""


def lazy_import_openlineage() -> Any:
    """Import the OL client on first use; surface a clean error if missing.

    Returns a ``SimpleNamespace`` aggregating just the bits emitters need.
    """
    try:
        from openlineage.client import serde
        from openlineage.client.event_v2 import (
            InputDataset,
            Job,
            OutputDataset,
            Run,
            RunEvent,
            RunState,
        )
        from openlineage.client.facet_v2 import (
            documentation_job,
            schema_dataset,
        )
    except ImportError as exc:
        raise LineageEmitError(
            "openlineage-python not installed. Install with: pip install lake-ingest[lineage]"
        ) from exc

    return types.SimpleNamespace(
        Run=Run,
        Job=Job,
        RunEvent=RunEvent,
        RunState=RunState,
        InputDataset=InputDataset,
        OutputDataset=OutputDataset,
        Serde=serde.Serde,
        SchemaDatasetFacet=schema_dataset.SchemaDatasetFacet,
        SchemaDatasetFacetFields=schema_dataset.SchemaDatasetFacetFields,
        DocumentationJobFacet=documentation_job.DocumentationJobFacet,
    )


def serialize(ol: Any, event: Any) -> str:
    """Serialize an OL event to JSON using the client's serde."""
    return cast("str", ol.Serde.to_json(event))


# Producer URI is required by the OL spec — identifies *us* as the
# emitter on every event so consumers (Marquez, UC, Atlan) know which
# tool wrote the lineage.
PRODUCER = "https://github.com/anthropics/lake-ingest"
