"""ADR-035 step 2: the SCD Type 2 contract (silver/history_contract.py) on the declarative engine.

One streaming table per contract case, fed by its own folder of change files, history by AUTO CDC (value tracked,
note untracked). The runner (run_contract.py) feeds the batches round by round and compares meaning, not columns.
"""
import sys
from pathlib import Path

from pyspark import pipelines as dp
from pyspark.sql import functions as F

_root = Path(spark.conf.get("spike.root"))  # noqa: F821
sys.path.insert(0, str(next(d / "src" for d in [_root, *_root.parents] if (d / "src" / "dataplatform").exists())))
from dataplatform.silver.history_contract import CASES, slug  # noqa: E402

BASE = spark.conf.get("contract.base")  # noqa: F821 - /Volumes/.../<run tag>
SCHEMA = "k STRING, v BIGINT, note STRING, seq BIGINT, op STRING, minute INT, pos BIGINT"


def declare(s: str) -> None:
    @dp.temporary_view(name=f"changes_{s}")
    def changes():
        return spark.readStream.schema(SCHEMA).json(f"{BASE}/{s}")  # noqa: F821

    dp.create_streaming_table(f"c_{s}")
    dp.create_auto_cdc_flow(
        target=f"c_{s}",
        source=f"changes_{s}",
        keys=["k"],
        sequence_by=F.col("seq"),
        apply_as_deletes=F.expr("op = 'D'"),
        except_column_list=["op", "minute", "pos"],
        stored_as_scd_type=2,
        track_history_column_list=["v"],
    )


for case, _, _ in CASES:
    declare(slug(case))
