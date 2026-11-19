"""ADF Trigger builders.

MVP ships a single scheduled trigger fired by ``platform_config.default_schedule_cron``.
As of Phase 5c the trigger is wired to ``PL_MASTER_DRIVER`` whenever
the master driver is being emitted; otherwise the trigger emits with
an empty ``pipelines`` array (still deploys cleanly).

Per CLAUDE.md "Driver pipelines, not per-job triggers": this trigger
is intentionally **one trigger per env**, not per dataset. Cadence
fan-out is a runtime concern of the driver pipeline.
"""

from __future__ import annotations

from lake_ingestion_framework.realms.adf_adls.generator import naming

# Sentinel cron used when platform_config has no default_schedule_cron
# row. Maps to "every day at 02:00 UTC", matching our docs.
_DEFAULT_CRON = "0 2 * * *"


def build_daily_trigger(
    *,
    schedule_cron: str | None = None,
    invoke_master_driver: bool = False,
) -> dict[str, object]:
    """Schedule trigger that fires the master driver once a day.

    ``schedule_cron`` is a 5-field cron string. We translate the
    minute + hour into ADF's schedule shape; days/months/weekdays
    default to "every" for the MVP. Operators wanting more exotic
    cadences hand-edit the emitted JSON or override
    ``default_schedule_cron`` in platform_config.

    ``invoke_master_driver`` wires the trigger's ``pipelines`` array
    to ``PL_MASTER_DRIVER``. Pass ``False`` (default) when the driver
    isn't being emitted (e.g. no enabled sources) — the trigger then
    deploys with an empty pipelines list.
    """
    name = naming.trigger_daily()
    cron = schedule_cron or _DEFAULT_CRON
    minute, hour = _parse_minute_hour(cron)

    pipelines: list[dict[str, object]] = []
    if invoke_master_driver:
        pipelines.append(
            {
                "pipelineReference": {
                    "referenceName": naming.pipeline_master_driver(),
                    "type": "PipelineReference",
                },
                "parameters": {},
            }
        )

    return {
        "name": name,
        "properties": {
            "annotations": [],
            "type": "ScheduleTrigger",
            "typeProperties": {
                "recurrence": {
                    "frequency": "Day",
                    "interval": 1,
                    "startTime": "2024-01-01T00:00:00Z",
                    "timeZone": "UTC",
                    "schedule": {
                        "minutes": [minute],
                        "hours": [hour],
                    },
                },
            },
            "pipelines": pipelines,
        },
    }


def _parse_minute_hour(cron: str) -> tuple[int, int]:
    """Extract (minute, hour) from a 5-field cron like ``0 2 * * *``.

    Falls back to (0, 2) for cron values we can't statically resolve
    (ranges, lists, ``*`` in the minute or hour slot). Operators with
    complex cadences should hand-edit the emitted JSON or wire a
    custom trigger downstream.
    """
    fallback_minute = 0
    fallback_hour = 2
    parts = cron.split()
    cron_field_count = 5
    if len(parts) != cron_field_count:
        return (fallback_minute, fallback_hour)
    try:
        minute = int(parts[0])
        hour = int(parts[1])
    except ValueError:
        return (fallback_minute, fallback_hour)
    return (minute, hour)


__all__ = ["build_daily_trigger"]
