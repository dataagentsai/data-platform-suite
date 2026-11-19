"""The one step format (ADR-024 rule 2): every owner writes one result row per check to the run log.

    detect.<DAC id>     the engine's detectors (ingestion today; silver and gold harness as they record them)
    quality.<rule>      data expectation services (DES): a quality rule's verdict
    recon.<entry>       DES: a reconciliation entry
    profile.<name>      DES: a profile

A row is the run log's step record: run id, source, dataset, the step name above, a status, integer counts and a
message. The status is the severity observability reads, and the only thing it reads to alert and to mark a run:

    succeeded -> passed     warned -> warned (handled, or flagged for a later layer)     failed -> blocked

Details stay with the owner; an alert and the ops screens link to the owner's screen (`screen`). Owners record
severities and never send alerts (rule 3). `violations` is the contract; tests/dataplatform/test_obs_steps.py runs it
over every owner's rows.
"""

from __future__ import annotations

import re
from collections.abc import Mapping

DETECT = "detect"
CHECK_KINDS = ("quality", "recon", "profile")
KINDS = (DETECT, *CHECK_KINDS)
OWNER = {DETECT: "engine", "quality": "des", "recon": "des", "profile": "des"}
SCREEN = {"engine": "sources_ingestion", "des": "des_data_expectations"}  # where the detail lives
SEVERITY = {"succeeded": "passed", "warned": "warned", "failed": "blocked"}
MAX_MESSAGE = 4000
_NAME = re.compile(r"^(detect|quality|recon|profile)\.[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")


def kind_of(step: str) -> str | None:
    """The owner kind of a step name, or None for a pipeline step (land, bronze, silver.<stage>, ingest.*)."""
    head, dot, rest = step.partition(".")
    return head if dot and rest and head in KINDS else None


def owner_of(step: str) -> str:
    kind = kind_of(step)
    return OWNER[kind] if kind else "pipeline"


def severity(status: str) -> str:
    return SEVERITY.get(status, status)


def violations(step: str, status: str, counts: Mapping[str, object], message: str) -> list[str]:
    """What breaks the one step format in this row (empty when it keeps it)."""
    out = []
    if not _NAME.match(step):
        out.append(f"step {step!r} is not <detect|quality|recon|profile>.<id>")
    if status not in SEVERITY:
        out.append(f"status {status!r} is not one of {sorted(SEVERITY)}")
    bad = [k for k, v in counts.items() if not isinstance(k, str) or isinstance(v, bool) or not isinstance(v, int)]
    if bad:
        out.append(f"counts {bad} are not name -> integer")
    if not isinstance(message, str) or len(message) > MAX_MESSAGE:
        out.append(f"message is not text of at most {MAX_MESSAGE} characters")
    if status == "failed" and not message:
        out.append("a blocked step says why")
    return out
