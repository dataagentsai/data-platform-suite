"""ADR-035 step 4b probe: can a declarative pipeline stream-read our bronze (Unity Catalog managed Iceberg)?"""
from pyspark import pipelines as dp


@dp.table(name="bronze_order_line_probe")
def probe():
    return spark.readStream.table("dev_bronze.erp.sales_order_line")  # noqa: F821
