"""Realm packages — generator + runner code per (compute + orchestrator + ingest + target) tuple.

Per CLAUDE.md, realms are the swappable unit. Each realm package has
its own extras and is imported lazily so the framework stays usable
when a realm's extras aren't installed.

Cross-references: ``docs/realms_strategy.md`` (catalog),
``docs/realms/`` (per-realm specs).
"""
