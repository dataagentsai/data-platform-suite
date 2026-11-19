"""Access model as code (DPC-0066; SEC-02, ENV-02; dpc/ports.yaml access_control).

    model.py   groups and classification tags -> a plan of grants, column masks and row filters (pure), and the
               port's `check`: what a group sees of a table under that plan.

Product-free (R1): realms/databricks/semantic_governance renders the plan as its own statements.
"""
