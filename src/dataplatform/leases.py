"""Writer leases (ORCH-04): two runs never write the same table at once. Product-free; the metadata store keeps the
lease (runtime.table_lease), so it holds across jobs, clusters and laptops.

A run takes the lease before it writes a table and gives it back when done. A second run that finds the table busy
does not write: its dataset run ends `skipped` with a failed `orch.lease` step that names the holder, so the alert
and the ops screen say why. A lease outlives a crashed run only until it expires; then the next run takes it over.

Lease names are the layer and the logical table: bronze:<source.dataset> (the lander and housekeeping share it),
silver:<source.dataset>, gold:<schema.table>.
"""

from __future__ import annotations

from dataplatform.meta.port import MetadataStore, StepRecord

LEASE_SECONDS = 4 * 3600  # longer than any one table's write; a crashed run's lease frees itself after this
STEP = "orch.lease"


def busy(store: MetadataStore, run_id: str, source: str, dataset: str, lease: str, holder: str) -> str:
    """Record that this run did not write because another holds the lease; the dataset run must be started."""
    msg = f"{lease} is being written by run {holder}; this run does not write it (ORCH-04)"
    store.log_step(StepRecord(run_id, source, dataset, STEP, "failed", {}, msg))
    store.finish_dataset_run(run_id, source, dataset, "skipped", message=msg)
    return msg
