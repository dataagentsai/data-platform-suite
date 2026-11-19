"""ADF JSON artifact generator — reads metadata, emits resource JSON.

Public entry point: ``plan_artifacts(repo, env)`` returns a list of
``Artifact`` dataclasses, each carrying a target file path + a
deterministic JSON-serialised content blob.

Determinism is load-bearing: re-running the generator against the
same metadata produces byte-identical output (per CLAUDE.md golden-
diff invariant). The serializer enforces this via sorted keys + fixed
indentation; assembly enforces it by sorting iteration sources.
"""

from __future__ import annotations

from lake_ingestion_framework.realms.adf_adls.generator.assemble import (
    Artifact,
    plan_artifacts,
)

__all__ = ["Artifact", "plan_artifacts"]
