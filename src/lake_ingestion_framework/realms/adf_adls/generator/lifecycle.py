"""ADLS Gen2 lifecycle policy generator.

Emits a single Azure Storage management policy JSON (the shape that
``az storage account management-policy create`` consumes) implementing
the framework's Hot → Cool → Archive → Delete tiering rule.

Design choice — single platform-level rule, not per-dataset:

The framework can't know rendered ADLS prefixes at deploy time
(``target_path_template`` tokens like ``{yyyy}/{mm}/{dd}`` are
runtime-only), so a per-dataset ``prefixMatch`` policy would be either
brittle (substituting placeholders) or wrong (mid-template prefixes).

Instead the policy targets the entire **target container**: every
blob written by the framework lives under one container per env, and
the platform-level lifecycle defaults are good enough for ~95% of
datasets. Datasets with bespoke retention requirements (legal hold,
compliance archival) get a deploy-time warning and a manual edit.

See ``docs/cost.md`` for the rule (Hot → Cool at 30 days → Archive at
180 days; per-dataset overrides via ``dataset_config.lifecycle_*``).
"""

from __future__ import annotations

LIFECYCLE_RULE_NAME = "lake-ingest-tiering"


def build_adls_lifecycle_policy(
    *,
    container: str,
    cool_days: int,
    archive_days: int,
    delete_days: int = 0,
) -> dict[str, object]:
    """Build an Azure Storage management policy with one Lifecycle rule.

    The rule targets ``<container>/`` (every blob the framework writes
    in this env). Tiering: Hot → Cool at ``cool_days``, Cool → Archive
    at ``archive_days``. ``delete_days > 0`` adds a delete action;
    ``= 0`` (default) keeps blobs forever once archived.

    The returned dict serialises to the shape ``az storage
    account management-policy create --policy @policy.json`` accepts.
    """
    if cool_days <= 0:
        raise ValueError(f"cool_days must be > 0; got {cool_days}")
    if archive_days <= cool_days:
        raise ValueError(f"archive_days ({archive_days}) must be > cool_days ({cool_days})")
    if delete_days < 0:
        raise ValueError(f"delete_days must be >= 0; got {delete_days}")
    if delete_days and delete_days <= archive_days:
        raise ValueError(
            f"delete_days ({delete_days}) must be > archive_days ({archive_days}) " "when set"
        )

    base_blob_actions: dict[str, object] = {
        "tierToCool": {"daysAfterModificationGreaterThan": cool_days},
        "tierToArchive": {"daysAfterModificationGreaterThan": archive_days},
    }
    if delete_days > 0:
        base_blob_actions["delete"] = {"daysAfterModificationGreaterThan": delete_days}

    return {
        "policy": {
            "rules": [
                {
                    "name": LIFECYCLE_RULE_NAME,
                    "enabled": True,
                    "type": "Lifecycle",
                    "definition": {
                        "filters": {
                            "blobTypes": ["blockBlob"],
                            "prefixMatch": [f"{container}/"],
                        },
                        "actions": {
                            "baseBlob": base_blob_actions,
                        },
                    },
                },
            ],
        },
    }


__all__ = ["LIFECYCLE_RULE_NAME", "build_adls_lifecycle_policy"]
