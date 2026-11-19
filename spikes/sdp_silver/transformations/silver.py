"""ADR-035 spike, step 1: open-source Spark Declarative Pipelines builds SCD Type 2 silver with AUTO CDC.

One change feed (order lines: insert, update, a change that arrives late with an older sequence, a delete) is read as a
stream, passed through one of our own stage functions (clean_text, the hygiene stage's column rule), and applied to a
streaming table as SCD Type 2 by AUTO CDC. The test of a declarative engine for silver: our code runs inside the flow,
history is right, the late change does not become current, the delete closes the key.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from dataplatform.stages.clean import clean_text  # our stage code, unchanged

SOURCE = spark.conf.get("spike.source")  # noqa: F821 - SDP provides `spark`
SCHEMA = "order_id STRING, line_no INT, dealer_code STRING, promised_date STRING, qty INT, op STRING, change_seq BIGINT"


@dp.temporary_view
def order_line_changes():
    raw = spark.readStream.schema(SCHEMA).json(SOURCE)  # noqa: F821
    return raw.withColumn("dealer_code", clean_text(F.col("dealer_code")))


dp.create_streaming_table("order_line")

dp.create_auto_cdc_flow(
    target="order_line",
    source="order_line_changes",
    keys=["order_id", "line_no"],
    sequence_by=F.col("change_seq"),
    apply_as_deletes=F.expr("op = 'D'"),
    except_column_list=["op"],
    stored_as_scd_type=2,
)
