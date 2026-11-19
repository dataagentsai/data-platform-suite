"""The metadata store (ADR-010): the port, the settings registry and the loader that fills the store from a manifest.

    port.py                  the MetadataStore protocol, its records and refusals, the shared rules (redact, run_status)
    registry.py              the settings registry (settings_registry.json, exported from the old key registry)
    loader.py                manifest folder -> validated documents -> the store (M3: metadata is data)

Adapters: adapters/memory_meta.py (tests) and the relational database adapter beside it
(one adapter for every server of that kind; the binding picks only the connection string).
"""
