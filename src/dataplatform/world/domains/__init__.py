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
    raise ValueError(f"no domain model for world {name!r}")
