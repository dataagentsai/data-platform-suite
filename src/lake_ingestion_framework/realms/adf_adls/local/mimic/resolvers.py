"""Resolvers for LinkedServices, Datasets, and the local-mode metadata DB.

The mimic doesn't ship with a registry of LinkedService/Dataset
definitions — those live in the customer's deployed ADF artifacts. The
resolvers here translate a referenced name + parameter block into a
concrete config (connection params, dataset path) at runtime.

Two pieces:

- ``ArtifactRegistry`` — a thin in-memory map from artifact name to
  parsed JSON, populated when the runner loads pipelines from disk.
- ``resolve_linked_service`` / ``resolve_dataset`` — pure functions that
  take an artifact reference (with parameter overrides) plus the
  registry, evaluate parameter expressions, and return the resolved
  config the activity executors use.

See ``docs/adf_mimic_design.md`` §8.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from lake_ingestion_framework.core.exceptions import LakeIngestError
from lake_ingestion_framework.realms.adf_adls.local.mimic.expressions import evaluate
from lake_ingestion_framework.realms.adf_adls.local.mimic.run_state import RunState


class ArtifactNotFoundError(LakeIngestError):
    """A referenced LinkedService / Dataset / Pipeline isn't in the registry."""


@dataclass
class ArtifactRegistry:
    """In-memory lookup for ADF artifacts the mimic loads from disk.

    Populated once at runner startup by walking the deploy output
    directory (``<customer>-ingestion/adf/``). Activity executors look
    up referenced artifacts by name.
    """

    linked_services: dict[str, dict[str, Any]] = field(default_factory=dict)
    datasets: dict[str, dict[str, Any]] = field(default_factory=dict)
    pipelines: dict[str, dict[str, Any]] = field(default_factory=dict)

    @classmethod
    def from_directory(cls, root: Path) -> ArtifactRegistry:
        """Load every ``*.json`` under ``root/{linkedServices,datasets,pipelines}``.

        ADF deploy artifacts come in this canonical layout. Other layouts
        (single bundle, flat directory) are not v1 supported — the
        generator emits the canonical shape.
        """
        registry = cls()
        for kind, target in (
            ("linkedServices", registry.linked_services),
            ("datasets", registry.datasets),
            ("pipelines", registry.pipelines),
        ):
            sub = root / kind
            if not sub.exists():
                continue
            for path in sorted(sub.glob("*.json")):
                doc = json.loads(path.read_text())
                name = doc.get("name") or path.stem
                target[name] = doc
        return registry

    def linked_service(self, name: str) -> dict[str, Any]:
        if name not in self.linked_services:
            raise ArtifactNotFoundError(f"LinkedService {name!r} not in registry.")
        return self.linked_services[name]

    def dataset(self, name: str) -> dict[str, Any]:
        if name not in self.datasets:
            raise ArtifactNotFoundError(f"Dataset {name!r} not in registry.")
        return self.datasets[name]

    def pipeline(self, name: str) -> dict[str, Any]:
        if name not in self.pipelines:
            raise ArtifactNotFoundError(f"Pipeline {name!r} not in registry.")
        return self.pipelines[name]


# ──────────────────────────────────────────────────────────────────────
# LinkedService resolution
# ──────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class ResolvedLinkedService:
    """Concrete config a connector can be built from."""

    name: str
    type: str
    properties: dict[str, Any]


def resolve_linked_service(
    ls_ref: dict[str, Any],
    registry: ArtifactRegistry,
    run_state: RunState,
) -> ResolvedLinkedService:
    """Resolve a ``linkedServiceName`` reference (with parameters) to a config.

    The reference shape is::

        {
          "referenceName": "LS_POSTGRES",
          "type": "LinkedServiceReference",
          "parameters": {
            "p_server": "@pipeline().parameters.p_server",
            ...
          }
        }

    We look up the LinkedService definition, evaluate the parameters
    against the run state, and substitute them into the LS connection
    config. The final dict is what the activity executor passes to the
    connector factory.
    """
    name = ls_ref["referenceName"]
    raw_definition = registry.linked_service(name)
    parameter_overrides = evaluate(ls_ref.get("parameters", {}), run_state)
    resolved_props = _substitute_parameters(
        raw_definition.get("properties", {}), parameter_overrides
    )
    return ResolvedLinkedService(
        name=name,
        type=str(raw_definition.get("properties", {}).get("type", "")),
        properties=resolved_props,
    )


def _substitute_parameters(
    properties: dict[str, Any], parameter_values: dict[str, Any]
) -> dict[str, Any]:
    """Walk LS properties replacing ``@{linkedService().parameters.x}`` refs."""

    def _walk(node: Any) -> Any:
        if isinstance(node, str):
            return _replace_ls_param_template(node, parameter_values)
        if isinstance(node, dict):
            return {k: _walk(v) for k, v in node.items()}
        if isinstance(node, list):
            return [_walk(v) for v in node]
        return node

    walked = _walk(properties)
    return walked if isinstance(walked, dict) else {}


def _replace_ls_param_template(value: str, params: dict[str, Any]) -> str:
    """Substitute ``@{linkedService().parameters.<name>}`` segments."""
    if "@{linkedService().parameters." not in value:
        return value
    out = value
    for key, val in params.items():
        token = "@{linkedService().parameters." + key + "}"
        if token in out:
            out = out.replace(token, str(val))
    return out


# ──────────────────────────────────────────────────────────────────────
# Dataset resolution
# ──────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class ResolvedDataset:
    """Resolved dataset reference — used by Copy / Lookup activities."""

    name: str
    type: str
    linked_service_ref: dict[str, Any]
    type_properties: dict[str, Any]


def resolve_dataset(
    ds_ref: dict[str, Any],
    registry: ArtifactRegistry,
    run_state: RunState,
) -> ResolvedDataset:
    """Resolve a Dataset reference to its (LinkedService, typeProperties) pair.

    Dataset parameters from ``ds_ref["parameters"]`` are evaluated
    against the run state and substituted into the dataset definition.
    """
    name = ds_ref["referenceName"]
    raw_definition = registry.dataset(name)
    properties = raw_definition.get("properties", {})
    parameter_overrides = evaluate(ds_ref.get("parameters", {}), run_state)
    type_props = _substitute_dataset_params(
        properties.get("typeProperties", {}), parameter_overrides
    )
    return ResolvedDataset(
        name=name,
        type=str(properties.get("type", "")),
        linked_service_ref=properties.get("linkedServiceName", {}),
        type_properties=type_props,
    )


def _substitute_dataset_params(
    type_props: dict[str, Any], parameter_values: dict[str, Any]
) -> dict[str, Any]:
    """Walk dataset typeProperties replacing dataset-parameter templates."""

    def _walk(node: Any) -> Any:
        if isinstance(node, dict):
            if node.get("type") == "Expression" and "value" in node:
                return _resolve_dataset_expression(node["value"], parameter_values)
            return {k: _walk(v) for k, v in node.items()}
        if isinstance(node, list):
            return [_walk(v) for v in node]
        if isinstance(node, str):
            return _replace_ds_param_template(node, parameter_values)
        return node

    walked = _walk(type_props)
    return walked if isinstance(walked, dict) else {}


def _resolve_dataset_expression(value: str, params: dict[str, Any]) -> Any:
    """Resolve ``@dataset().<name>`` whole-expression."""
    if value.startswith("@dataset()."):
        key = value[len("@dataset().") :]
        if key in params:
            return params[key]
    return _replace_ds_param_template(value, params)


def _replace_ds_param_template(value: str, params: dict[str, Any]) -> str:
    """Substitute ``@{dataset().<name>}`` segments."""
    if "@{dataset()." not in value:
        return value
    out = value
    for key, val in params.items():
        token = "@{dataset()." + key + "}"
        if token in out:
            out = out.replace(token, str(val))
    return out


__all__ = [
    "ArtifactNotFoundError",
    "ArtifactRegistry",
    "ResolvedDataset",
    "ResolvedLinkedService",
    "resolve_dataset",
    "resolve_linked_service",
]
