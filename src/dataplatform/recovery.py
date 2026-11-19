"""Withdrawing a bad batch after it was published (VOID-01, ADR-025). Product-free.

    withdraw    void the batch (every read skips it) and record a backfill plan that names every window, so the next
                runs rebuild silver from bronze and recompute every gold window of the facts that read the dataset;
                gold replaces only the windows whose numbers changed and logs each as a restatement (RSTM-01)

Order of a withdrawal: withdraw, then silver with rebuild={dataset}, then gold as usual. No row is deleted by hand.
"""

from __future__ import annotations

from datetime import date

from dataplatform.gold.harness import EVERY_WINDOW
from dataplatform.meta.port import BackfillPlan, MetadataStore


def withdraw(store: MetadataStore, source: str, dataset: str, batch_key: str, *, reason: str, approved_by: str,
             today: date) -> BackfillPlan:
    store.void_batch(source, dataset, batch_key, reason, approved_by)
    plan = BackfillPlan(f"void-{batch_key[:40]}", source, dataset, (EVERY_WINDOW,), today.isoformat(),
                        f"VOID-01: batch {batch_key} withdrawn: {reason}", approved_by)
    store.record_corrections(plan, [])
    return plan
