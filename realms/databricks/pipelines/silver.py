"""Silver, as a Lakeflow Declarative Pipeline: maximum use of what Databricks offers, driven by the metadata DB.

What Databricks does here:  AUTO CDC (ordering by the source's change sequence, deletes, SCD type 2 history),
                            expectations (row rules), streaming reads of bronze, lineage in Unity Catalog.
What we bring:              the rules and table names, read from the metadata DB (never written in this file),
                            and a quarantine table that keeps each failed row with the names of the rules it broke.
"""
import json

import dlt
from pyspark.sql import functions as F

META = spark.conf.get("dp.meta")  # noqa: F821 - `spark` is provided by the pipeline runtime
SOURCE, DATASET = spark.conf.get("dp.source"), spark.conf.get("dp.dataset")  # noqa: F821

_row = (spark.table(f"{META}.dataset_config")  # noqa: F821
        .where((F.col("source_name") == SOURCE) & (F.col("dataset_name") == DATASET))
        .orderBy(F.col("registered_at").desc()).first())
CFG = json.loads(_row["config_json"])
RULES = {r["rule"]: f"coalesce({r['predicate']}, false)" for r in
         spark.table(f"{META}.quality_rules")  # noqa: F821
         .where((F.col("source_name") == SOURCE) & (F.col("dataset_name") == DATASET)).collect()}
LOAD_COLS = ["op", "_batch_key", "_run_id", "_source_file", "_window"]
SILVER = CFG["silver_table"].rsplit(".", 1)[-1]
QUARANTINE = CFG["quarantine_table"].rsplit(".", 1)[-1]


@dlt.view(name=f"{DATASET}_changes_valid", comment="Bronze changes that pass every contract row rule.")
@dlt.expect_all_or_drop(RULES)
def valid():
    return spark.readStream.table(CFG["bronze_table"])  # noqa: F821


@dlt.table(name=QUARANTINE, comment="Rows that broke a contract rule, kept with the rule names (DPC-0011).")
def quarantine():
    failed = F.array_compact(F.array(*[F.when(~F.expr(p), F.lit(n)) for n, p in RULES.items()]))
    return (spark.readStream.table(CFG["bronze_table"])  # noqa: F821
            .withColumn("_failed_rules", failed).where(F.size("_failed_rules") > 0))


dlt.create_streaming_table(name=SILVER, comment=f"{DATASET}: history kept (SCD type 2), ordered by "
                                                f"{CFG['sequence_by']}, deletes applied. Contract {CFG['contract_id']} "
                                                f"{CFG['contract_version']}.")
dlt.create_auto_cdc_flow(
    target=SILVER,
    source=f"{DATASET}_changes_valid",
    keys=CFG["keys"],
    sequence_by=F.col(CFG["sequence_by"]),
    apply_as_deletes=F.expr("op = 'D'"),
    except_column_list=LOAD_COLS,
    stored_as_scd_type=2,
)
