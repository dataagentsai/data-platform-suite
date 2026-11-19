"""Observability (W3, goals G8 and G10; ADR-024): runs and promises. One run id everywhere, the ops tables and their
metric views, freshness, pushed alerts, five dashboards. Owners judge (DES in dataplatform/des), observability reports.

    clock.py       the clock port (simulated days run on simulated time)
    trace.py       the telemetry port: run id = trace id (W3C form); LocalTracer
    recorder.py    the run recorder: wraps the metadata store port (W1a) and the telemetry port
    runlog.py      the run log read back for operations (a time window, with timestamps): the reader port
    system.py      the platform's own records (jobs, lineage, access), copied once a day: the system-records port
    freshness.py   freshness against the manifest's targets: per dataset and per bookmark (pure)
    alerts.py      alert rules as config, change-of-state, one per rule per day, routed by tier, snoozes (pure)
    rules/         the default alert rules (alerts.yaml)
    ops.py         the ops builder: run log + system records + freshness + alerts -> ops tables (pure)
    steps.py       the one step format every owner writes (detect / quality / recon / profile, with a severity)
    pipeline_events.py  the declarative pipeline event log port (pipelines report themselves)
    native.py      platform-native screens and notifications, adopted, not rebuilt
    views.py       the ops metric views (certified definitions the dashboards read)
    dashboards.py  observability's dashboards as product-free definitions, with an in-memory evaluator
    monitor.py     pushed by runs: alerts as runs end, lateness when due, the daily copy; the notifier port
    fixture.py     FIXTURE: a fake runner that fills the run log until W1b-W1e produce real runs

Adapters (only they name a product) live in dataplatform/adapters/: the run-log reader (memory, relational store),
the system records (memory, the engine's system tables), the tracer (the standard tracing API) and the dashboard
file compiler. Decisions: README.md beside this file.
"""
