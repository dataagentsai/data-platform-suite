"""ADR-035 spike, step 1b: the declarative engine on Databricks Lakeflow (serverless).

Same flow as the open-source spike: the change feed read as a stream, one of our own stage functions (clean_text)
applied inside the flow, AUTO CDC writing SCD Type 2. Two targets: the default table format, and the same with the
properties that let other engines read it as Iceberg (the open-format question for the declarative path).
"""
import sys

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from pathlib import Path  # noqa: E402

_root = Path(spark.conf.get("spike.root"))  # noqa: F821 - ${workspace.file_path}; our src/ is synced above or beside it
sys.path.insert(0, str(next(d / "src" for d in [_root, *_root.parents] if (d / "src" / "dataplatform").exists())))
from dataplatform.stages.clean import clean_text  # noqa: E402 - our stage code, unchanged

SOURCE = spark.conf.get("spike.source")  # noqa: F821
SCHEMA = "order_id STRING, line_no INT, dealer_code STRING, promised_date STRING, qty INT, op STRING, change_seq BIGINT"
ICEBERG_READS = {"delta.enableIcebergCompatV3": "true", "delta.universalFormat.enabledFormats": "iceberg",
                 "delta.columnMapping.mode": "name"}


@dp.temporary_view
def order_line_changes():
    raw = spark.readStream.schema(SCHEMA).json(SOURCE)  # noqa: F821
    return raw.withColumn("dealer_code", clean_text(F.col("dealer_code")))


for name, props in (("order_line", {}), ("order_line_ice3", ICEBERG_READS)):
    dp.create_streaming_table(name, table_properties=props or None)
    dp.create_auto_cdc_flow(
        target=name,
        source="order_line_changes",
        keys=["order_id", "line_no"],
        sequence_by=F.col("change_seq"),
        apply_as_deletes=F.expr("op = 'D'"),
        except_column_list=["op"],
        stored_as_scd_type=2,
    )
