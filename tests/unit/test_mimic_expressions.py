"""Unit tests for the ADF Mimic expression evaluator.

Validates each whitelist pattern + that out-of-whitelist expressions
fail loudly (per docs/adf_mimic_design.md §4.2 fail-loud rule).
"""

from __future__ import annotations

import pytest
from lake_ingestion_framework.realms.adf_adls.local.mimic.expressions import (
    UnsupportedExpressionError,
    evaluate,
)
from lake_ingestion_framework.realms.adf_adls.local.mimic.run_state import RunState


def _state(**kwargs: object) -> RunState:
    base = {
        "run_id": "run-123",
        "pipeline_name": "PL_TEST",
        "parameters": {"src": "postgres", "rows": 100},
        "variables": {},
        "activity_outputs": {},
    }
    base.update(kwargs)
    return RunState(**base)  # type: ignore[arg-type]


# ──────────────────────────────────────────────────────────────────────
# Whole-string expressions
# ──────────────────────────────────────────────────────────────────────


def test_pipeline_parameter_returns_native_value() -> None:
    state = _state()
    assert evaluate("@pipeline().parameters.src", state) == "postgres"
    assert evaluate("@pipeline().parameters.rows", state) == 100  # int preserved


def test_pipeline_runid() -> None:
    state = _state(run_id="abc-xyz")
    assert evaluate("@pipeline().RunId", state) == "abc-xyz"


def test_variables_lookup() -> None:
    state = _state(variables={"current_run_id": "r-1"})
    assert evaluate("@variables('current_run_id')", state) == "r-1"


def test_variables_unknown_raises() -> None:
    state = _state(variables={})
    with pytest.raises(KeyError, match="not declared"):
        evaluate("@variables('nope')", state)


def test_activity_output_first_row_field() -> None:
    state = _state(
        activity_outputs={"RunLogStarted": {"firstRow": {"run_id": 42, "status": "STARTED"}}}
    )
    assert evaluate("@activity('RunLogStarted').output.firstRow.run_id", state) == 42


def test_activity_output_value_array() -> None:
    state = _state(activity_outputs={"Lookup": {"value": [1, 2, 3], "count": 3}})
    assert evaluate("@activity('Lookup').output.value", state) == [1, 2, 3]
    assert evaluate("@activity('Lookup').output.count", state) == 3


def test_activity_missing_raises() -> None:
    state = _state(activity_outputs={})
    with pytest.raises(UnsupportedExpressionError, match="no recorded output"):
        evaluate("@activity('Ghost').output.x", state)


def test_activity_path_missing_raises() -> None:
    state = _state(activity_outputs={"A": {"firstRow": {"x": 1}}})
    with pytest.raises(UnsupportedExpressionError, match="no path segment"):
        evaluate("@activity('A').output.firstRow.zzz", state)


# ──────────────────────────────────────────────────────────────────────
# ForEach @item()
# ──────────────────────────────────────────────────────────────────────


def test_item_returns_full_item() -> None:
    state = _state().fork_for_iteration(0, {"id": 1, "name": "alice"})
    assert evaluate("@item()", state) == {"id": 1, "name": "alice"}


def test_item_field_access() -> None:
    state = _state().fork_for_iteration(0, {"id": 7, "name": "bob"})
    assert evaluate("@item().id", state) == 7
    assert evaluate("@item().name", state) == "bob"


def test_item_outside_foreach_raises() -> None:
    state = _state()
    with pytest.raises(UnsupportedExpressionError, match="outside ForEach"):
        evaluate("@item()", state)


# ──────────────────────────────────────────────────────────────────────
# Functions
# ──────────────────────────────────────────────────────────────────────


def test_concat_simple() -> None:
    state = _state()
    assert evaluate("@concat('hello-', pipeline().parameters.src)", state) == "hello-postgres"


def test_concat_with_int_coerces_to_string() -> None:
    state = _state()
    assert evaluate("@concat('count=', pipeline().parameters.rows)", state) == "count=100"


def test_string_function() -> None:
    state = _state(activity_outputs={"A": {"firstRow": {"id": 42}}})
    assert evaluate("@string(activity('A').output.firstRow.id)", state) == "42"


def test_int_function() -> None:
    state = _state(parameters={"src": "5", "rows": 0})
    assert evaluate("@int(pipeline().parameters.src)", state) == 5


def test_equals_function() -> None:
    state = _state()
    assert evaluate("@equals(pipeline().parameters.src, 'postgres')", state) is True
    assert evaluate("@equals(pipeline().parameters.src, 'mysql')", state) is False


def test_utcnow_returns_iso_string() -> None:
    state = _state()
    result = evaluate("@utcnow()", state)
    assert isinstance(result, str)
    assert "T" in result  # ISO 8601 separator


def test_guid_returns_uuid() -> None:
    import uuid

    state = _state()
    result = evaluate("@guid()", state)
    # Should parse as a valid UUID
    assert uuid.UUID(result)


def test_unsupported_function_raises() -> None:
    state = _state()
    with pytest.raises(UnsupportedExpressionError, match="not in the ADF Mimic v1 whitelist"):
        evaluate("@startsWith(pipeline().parameters.src, 'pg')", state)


# ──────────────────────────────────────────────────────────────────────
# Inline @{...} string templates
# ──────────────────────────────────────────────────────────────────────


def test_inline_template_substitutes() -> None:
    state = _state()
    assert evaluate("prefix-@{pipeline().parameters.src}-suffix", state) == "prefix-postgres-suffix"


def test_inline_template_multiple_segments() -> None:
    state = _state()
    result = evaluate(
        "EXEC sp_run_log_started @dataset_id = @{pipeline().parameters.rows}, "
        "@correlation_id = @{pipeline().RunId}",
        state,
    )
    assert result == "EXEC sp_run_log_started @dataset_id = 100, @correlation_id = run-123"


def test_string_without_expression_is_returned_unchanged() -> None:
    state = _state()
    assert evaluate("plain text with no @", state) == "plain text with no @"


def test_expression_dict_node() -> None:
    state = _state()
    node = {"type": "Expression", "value": "@pipeline().parameters.src"}
    assert evaluate(node, state) == "postgres"


def test_nested_expression_substitution() -> None:
    state = _state()
    node = {
        "p_server": {"type": "Expression", "value": "@pipeline().parameters.src"},
        "static": "hello",
    }
    assert evaluate(node, state) == {"p_server": "postgres", "static": "hello"}


# ──────────────────────────────────────────────────────────────────────
# Literals
# ──────────────────────────────────────────────────────────────────────


def test_quoted_literals() -> None:
    state = _state()
    # When the whole string is a function call referencing a literal:
    assert evaluate("@concat('a', 'b')", state) == "ab"


def test_int_literal() -> None:
    state = _state()
    assert evaluate("@int('42')", state) == 42
