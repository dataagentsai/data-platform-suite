"""Domain models: how one simulated company's source systems behave. The DWD's `name` picks the model.

A domain model is the only place that knows a business (orders, presses, dealers). The engine, the clock, the
ports and the adapters know none of it (M3).
"""

from __future__ import annotations

from typing import Any

from dataplatform.world.dwd import Dwd


def model_for(dwd: Dwd, scale: float = 1.0) -> Any:
    name = dwd.world["name"]
    if name == "tyre-company":
        from dataplatform.world.domains.tyre import TyreCompany

        return TyreCompany(dwd, scale)
    if name == "clinical-trial":
        from dataplatform.world.domains.clinical import ClinicalTrial

        return ClinicalTrial(dwd, scale)
    raise ValueError(f"no domain model for world {name!r}")


def gold_judge_for(dwd: Dwd, tz: str) -> Any:
    """The domain's gold judge: published gold against the oracle's gold for the day, and the incidents gold owns."""
    name = dwd.world["name"]
    if name == "tyre-company":
        from dataplatform.world.domains.tyre_gold import TyreGoldJudge

        return TyreGoldJudge(tz)
    if name == "clinical-trial":
        from dataplatform.world.domains.clinical_gold import ClinicalGoldJudge

        return ClinicalGoldJudge(tz)
    raise ValueError(f"no gold judge for world {name!r}")


def models_folder(dwd: Dwd) -> str:
    """The folder of the domain's gold build models, under the repo's models/ (named by the platform domain)."""
    return f"models/{dwd.platform['domain']}"
