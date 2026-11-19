"""The semantic layer and what reads it (goal G6, ADR-022): metric definitions and the experiences built on them.

    metrics.py      metric views read from the manifest's metric files, linted (naming, certification, known answers),
                    split into the certified view and a preview of measures not yet certified.
    experiences.py  dashboard and conversation-space definitions generated from the certified views.

Product-free (R1): a realm turns these definitions into its own files (realms/databricks/semantic_governance).
"""
