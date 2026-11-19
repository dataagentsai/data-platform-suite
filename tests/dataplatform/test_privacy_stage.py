"""The privacy scan (stages/privacy.py, B20 C15) on Spark and DuckDB. Table-driven."""

import pytest

pytest.importorskip("pyspark")

from dataplatform.stages.privacy import clear_counts

pytestmark = pytest.mark.usefixtures("spark")
PATTERN = r"^tok_[0-9a-f]{24}$"
TOK = "tok_" + "a" * 24


@pytest.mark.parametrize(("case", "rows", "want"), [
    ("tokens and empty values pass", [(TOK, None), (None, TOK)], {"email": 0, "phone": 0}),
    ("a clear email and a clear phone", [("ann@example.com", TOK), (TOK, "+91 98450 00000")], {"email": 1, "phone": 1}),
    ("a token with the wrong length is clear", [("tok_abc", None)], {"email": 1, "phone": 0}),
])
def test_clear_counts(spark, case, rows, want):
    df = spark.createDataFrame(rows, "email string, phone string")
    assert clear_counts(df, ["email", "phone", "absent"], PATTERN) == want, case
