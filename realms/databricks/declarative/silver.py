"""The declarative silver engine on Databricks (ADR-035): one generic definition for every dataset whose
silver_engine is `declarative` (or the datasets named in dp.only, for proofs).

Per dataset, from the metadata store (never from this file): the view -> the silver plan -> the declarative recipe
(dataplatform.silver.declarative). A streaming read of bronze runs the eleven in-flow stages, unchanged; quarantined
rows go to the dataset's quarantine table through their own flow; history datasets get AUTO CDC (keys, change order,
SCD 1 or 2, deletes, tracked columns) from the recipe; append datasets get streaming de-duplication.
"""
import os
import sys
from dataclasses import replace
from pathlib import Path

from pyspark import pipelines as dp
from pyspark.sql import functions as F

conf = spark.conf.get  # noqa: F821 - provided by the pipeline runtime
_root = Path(conf("dp.root"))
sys.path.insert(0, str(next(d / "src" for d in [_root, *_root.parents] if (d / "src" / "dataplatform").exists())))
os.environ.update(DP_META_INSTANCE=conf("dp.meta_instance"), DP_META_DATABASE=conf("dp.meta_database"))

from dataplatform.adapters.keyed_tokens import KeyedTokenizer  # noqa: E402 - the tokenizer the binding names
from dataplatform.adapters.lakebase import meta_store_from_env  # noqa: E402
from dataplatform.meta.port import GOLD_KINDS  # noqa: E402
from dataplatform.silver.contract_rules import contract_rules  # noqa: E402
from dataplatform.silver.declarative import recipe  # noqa: E402
from dataplatform.silver.engines import engine_of  # noqa: E402
from dataplatform.silver.plan import plan_from_view  # noqa: E402
from dataplatform.stages import run_stages  # noqa: E402

ENV, DOMAIN = conf("dp.env"), conf("dp.domain")
ONLY = {x for x in conf("dp.only", "").replace("|", ",").split(",") if x}
RUN_ID, AS_OF = conf("dp.run_id", "declarative"), conf("dp.as_of", "1970-01-01")
BRONZE = conf("dp.bronze_catalog", "") or f"{ENV}_bronze"  # proof runs may point at a scratch copy of bronze
TOKEN_PATTERN = KeyedTokenizer.PATTERN  # personal-data stage: restricted columns must hold these tokens


def declare(name: str, bronze: str, r) -> None:
    table = name.replace(".", "__")

    def staged():
        return run_stages(r.flow_plan, spark.readStream.table(bronze), None, RUN_ID, AS_OF)  # noqa: F821

    dp.temporary_view(name=f"{table}__valid")(lambda: staged()[0])
    _, quarantine, _ = staged()
    if quarantine is not None:
        dp.create_streaming_table(f"{table}__quarantine")
        dp.append_flow(target=f"{table}__quarantine", name=f"{table}__quarantine_flow")(lambda: staged()[1])
    if r.capture is not None:
        c = r.capture
        dp.create_streaming_table(table)
        dp.create_auto_cdc_flow(
            target=table, source=f"{table}__valid", keys=list(c.keys), sequence_by=F.col(c.sequence_by),
            apply_as_deletes=F.expr(c.delete_when) if c.delete_when else None,
            except_column_list=list(c.exclude), stored_as_scd_type=c.scd_type,
            track_history_column_list=list(c.track) or None,
        )
    else:
        def appended():
            rows = spark.readStream.table(f"{table}__valid")  # noqa: F821
            if not r.dedupe_keys:
                return rows
            if r.dedupe_within:  # keys kept only as long as late rows may still come (the dataset's lookback)
                col, days = r.dedupe_within
                return rows.withWatermark(col, f"{days} days").dropDuplicatesWithinWatermark(list(r.dedupe_keys))
            return rows.dropDuplicates(list(r.dedupe_keys))  # unbounded; the recipe's note says so

        dp.table(name=table)(appended)


store = meta_store_from_env()
platform = (store.domain_view(DOMAIN) or {}).get("settings", {}) if hasattr(store, "domain_view") else {}
for ref in store.datasets(DOMAIN):
    name = f"{ref.source}.{ref.dataset}"
    if not ref.is_enabled or ref.kind in GOLD_KINDS:
        continue
    view = store.dataset_view(ref.source, ref.dataset)
    if (name not in ONLY) if ONLY else engine_of(view.get("settings", {}), platform) != "declarative":
        continue
    plan = plan_from_view(view, TOKEN_PATTERN)
    contract, untranslated = contract_rules(store.dq_rules(ref.source, ref.dataset))  # DQ-01, same as the job engine
    r = recipe(replace(plan, rules=plan.rules + contract))
    for note in untranslated:
        print(f"declarative silver: {note}")
    for note in r.notes:
        print(f"declarative silver: {note}")
    declare(name, f"{BRONZE}.{ref.source}.{ref.dataset}", r)
