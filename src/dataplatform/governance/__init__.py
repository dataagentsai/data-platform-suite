"""Governance (goal G6, concern X13): what the catalog must say, read from the contracts and the manifest.

    tables.py   the published tables and views as plain records (from ODCS contracts and metric views), the data
                products and which datasets feed them, and the tag standard (DPC-0075, data_catalog kernel_owns).
    catalog.py  the desired catalog entries and a pure plan that diffs them against what the catalog holds (CAT-01).
    port.py     the catalog-state port the plan reads "current" through; adapters in dataplatform.adapters.

Product-free (R1): realms/databricks/semantic_governance renders the plan as its own statements.
"""
