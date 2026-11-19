"""The bronze-to-silver engines and what each can honour (ADR-035, DAC ENG-02).

Two engines run the same thirteen stages from the same metadata: `job` (our runner, our history write, the default,
ADR-026) and `declarative` (the stages inside a declarative pipeline, history by its managed change capture). A
dataset picks one with the setting `silver_engine` (dataset first, then the domain's platform settings, then `job`).
A dataset that asks an engine for something it cannot do is refused when the manifests load, with the reason, never
run another way. What an engine supports is data here, so a binding can narrow or widen it (EngineCaps).
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from dataplatform.silver.plan import HISTORY_BY_KIND

ENGINES = ("job", "declarative")
DEFAULT_ENGINE = "job"


@dataclass(frozen=True)
class EngineCaps:
    history: frozenset[str]  # history types it can write (silver/plan.py HISTORY_TYPES)
    formats: frozenset[str] | None  # table formats it writes natively; None = any (through the table port)


CAPS: dict[str, EngineCaps] = {
    "job": EngineCaps(frozenset({"scd1", "scd2", "append", "snapshot", "source_versioned"}), None),
    # Spike 13-14 Nov: managed change capture keeps SCD Type 1 and 2; the bound declarative product writes its own
    # format natively and the open format only as added metadata, so a native open-format contract stays on `job`.
    "declarative": EngineCaps(frozenset({"scd1", "scd2", "append"}), frozenset({"delta"})),
}


def engine_of(settings: Mapping[str, str], platform: Mapping[str, str]) -> str:
    return str(settings.get("silver_engine") or platform.get("silver_engine") or DEFAULT_ENGINE)


def history_type(manifest: Mapping[str, object]) -> str:
    raw = manifest.get("history")
    hist: Mapping[str, object] = raw if isinstance(raw, Mapping) else {}
    kind = str(hist.get("kind") or "")
    return str(hist.get("type") or HISTORY_BY_KIND.get(kind, ""))


def engine_problems(name: str, manifest: Mapping[str, object], settings: Mapping[str, str],
                    platform: Mapping[str, str], caps: Mapping[str, EngineCaps] | None = None) -> list[str]:
    """Why this dataset cannot run on its chosen engine; empty when it can."""
    caps = CAPS if caps is None else caps
    engine = engine_of(settings, platform)
    if engine not in caps:
        return [f"{name}: silver_engine '{engine}' is not one of {sorted(caps)}"]
    c, out = caps[engine], []
    hist = history_type(manifest)
    if hist and hist not in c.history:
        out.append(f"{name}: history '{hist}' is not supported by silver_engine '{engine}' "
                   f"(supports {sorted(c.history)}); use silver_engine 'job'")
    load = manifest.get("load")
    deletes = load.get("deletes") if isinstance(load, Mapping) else None
    if engine == "declarative" and isinstance(deletes, Mapping) and deletes.get("detect") == "key_compare":
        out.append(f"{name}: deletes found by comparing full copies are not supported by silver_engine "
                   f"'{engine}'; use silver_engine 'job'")
    fmt = str(settings.get("table_format") or platform.get("table_format") or "")
    if c.formats is not None and fmt and fmt not in c.formats:
        out.append(f"{name}: table_format '{fmt}' is not written natively by silver_engine '{engine}' "
                   f"(writes {sorted(c.formats)}); a contract that needs native '{fmt}' stays on silver_engine 'job'")
    return out
