"""Local-mode support for the adf-adls realm.

Contains the ADF Mimic — a minimum-viable executor that runs the ADF JSON
the generator emits, against local sources (Docker containers), writing
to local Delta. The gateway that lets every layer above ingestion be
exercised end-to-end on a laptop without Azure.

See ``docs/adf_mimic_design.md``.
"""
