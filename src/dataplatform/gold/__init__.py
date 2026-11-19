"""Gold (W1d): the gold harness (ADR-003, -004, -005, -006, -017, -018; D18), product-free.

    plan.py      a gold table's plan from its DPM gold manifest and ODCS contract (one-to-one or modelled)
    keys.py      deterministic hash surrogate keys with reserved members -1 (unknown) and -2 (not applicable)
    windows.py   windows of a fact, the recompute range, completeness from input watermarks, restatements
    ports.py     the gold read and write edges and the engine operations the harness asks for
    harness.py   one gold run: dimensions, then facts by window recompute with replace, checks before publish
    daybyday.py  gold per world day against the oracle; LATE-01, LATE-02, HIST-05 (and DEL-01, DUP-01) judged

The per-fact business logic is one pure build() (a SQL model or a dataframe function) the harness calls; the harness
owns every read, key lookup, check and write.
"""
