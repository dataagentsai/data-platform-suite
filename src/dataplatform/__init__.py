"""dataplatform: the realm-agnostic kernel for the Clean Data Engineering specs.

Layout (code to interface):
    ports.py        the plugs (Protocols). The kernel talks only to these.
    model.py        plain records passed between the kernel and the adapters.
    keys.py         batch keys and config fingerprints.
    contract.py     reads an ODCS contract; checks a delivery's columns; turns quality rules into row predicates.
    lander.py       landing -> bronze: our own code (decision D11), whatever mover filled landing.
    reconcile.py    counts and totals compared between hops, with a tolerance.
    manifest.py     DPM manifest -> compiled config for the metadata DB (local, needs PyYAML).
    meta/           the metadata store port (ADR-010), the settings registry and the validating loader.
    world/          the pretend shop (DWD) and its oracle.
    adapters/       one package per product family. Only adapters import a product.

Standard library only, so it runs unchanged locally, in tests and on any cloud's Python.
"""
__version__ = "0.1.0"
