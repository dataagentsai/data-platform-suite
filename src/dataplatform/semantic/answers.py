"""Ask a metric view's question of published gold (ADR-022 known answers, one number everywhere).

The question is written the way the metric view defines it, from the metric file: the measure's expression over the
fact joined to each dimension on its surrogate key, filtered on the view's dimension expressions, in the business
time zone. The same text the semantic generator renders into `known_answers.sql` (which runs against the engine's
own metric views); here it runs through the GoldReader port, so a local run checks the same numbers before any
workspace exists. Product-free: the SQL is the engine-neutral subset (date_format, DATE_TRUNC) both engines take.
"""

from __future__ import annotations

from collections.abc import Iterable

from dataplatform.gold.ports import GoldReader
from dataplatform.semantic.metrics import Filter, KnownAnswerCase, MetricView

FORMATS = {"month": "yyyy-MM", "day": "yyyy-MM-dd"}


def _where(view: MetricView, f: Filter) -> str:
    expr = view.dimension(f.dimension).expr
    value = f.value.replace("'", "''")
    if f.grain in FORMATS:
        return f"date_format({expr}, '{FORMATS[f.grain]}') = '{value}'"
    return f"{expr} = '{value}'"


def question_sql(view: MetricView, case: KnownAnswerCase) -> str:
    measure = next(m for m in view.measures if m.name == case.measure)
    joins = " ".join(f"LEFT JOIN {j.name} AS {j.name} ON source.{j.key} = {j.name}.{j.key}" for j in view.joins)
    where = " AND ".join(_where(view, f) for f in case.answer.filters)
    # shiftleft: allow SL-SEC-01 -- the text comes from the reviewed metric file (as known_answers.sql), values are quoted
    return f"SELECT CAST({measure.expr} AS DOUBLE) AS got FROM {view.source} AS source {joins} WHERE {where}"


def answers(reader: GoldReader, views: Iterable[MetricView], cases: Iterable[KnownAnswerCase],
            time_zone: str) -> dict[str, float | None]:
    """{"<view>|<measure>|<oracle key>": the view's number} (the DP_GOLD_METRICS file's shape)."""
    by_name = {v.name: v for v in views}
    out: dict[str, float | None] = {}
    for c in cases:
        rows = reader.rows(question_sql(by_name[c.view], c), time_zone=time_zone)
        got = rows[0]["got"] if rows else None
        out[f"{c.view}|{c.measure}|{c.oracle_key}"] = None if got is None else float(got)
    return out
