"""Publish the ops tables (obs/ops_export.py file) as catalog tables, then the ops metric views over them (B13).

The ops tables are rebuilt from the whole run log on every run, so each publish replaces them. The dashboards read
the metric views (obs/views.py), so the views are created after the tables. Runs on any Spark session with a
catalog that takes `CREATE ... WITH METRICS` (a Unity Catalog workspace); a view the session refuses is reported,
not raised, so the run's own result stands.
"""

from __future__ import annotations

import re
from typing import Any

NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,127}$")  # catalog and schema names go into DDL, which takes no binds
SPARK_TYPES = {"text": "StringType", "integer": "LongType", "decimal": "DoubleType", "flag": "BooleanType",
               "time": "TimestampType", "date": "DateType"}


def publish(spark: Any, doc: dict[str, Any], catalog: str, schema: str) -> list[str]:
    from pyspark.sql import types as T  # noqa: PLC0415 - Spark only where it runs

    from dataplatform.adapters.databricks_ops_views import view_sql  # noqa: PLC0415
    from dataplatform.obs.ops_export import restore  # noqa: PLC0415
    from dataplatform.obs.views import VIEWS  # noqa: PLC0415

    bad = [n for n in (catalog, schema) if not NAME.match(n)]
    if bad:
        raise ValueError(f"not a plain catalog or schema name: {bad}")
    notes = []
    # shiftleft: allow SL-SEC-01 -- DDL takes no bind parameters; both names are checked against NAME above
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
    for name, table in sorted(doc.items()):
        cols, rows = restore(table)
        if not cols:
            notes.append(f"{name}: no rows and no columns, left as it was")
            continue
        struct = T.StructType([T.StructField(c, getattr(T, SPARK_TYPES[k])(), True) for c, k in cols])
        (spark.createDataFrame(rows, struct).write.mode("overwrite").option("overwriteSchema", "true")
         .saveAsTable(f"{catalog}.{schema}.{name}"))
        notes.append(f"{name}: {len(rows)} rows")
    for view in VIEWS:
        try:
            spark.sql(view_sql(view, catalog, schema).rstrip().rstrip(";"))
            notes.append(f"{view.name}: view created")
        except Exception as exc:
            notes.append(f"{view.name}: view NOT created: {str(exc).splitlines()[0][:200]}")
    return notes
