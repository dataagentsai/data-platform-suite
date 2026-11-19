"""ADF expression evaluator — whitelist-only.

ADF expressions are a real DSL (``@concat()``, ``@activity().output``,
``@pipeline().parameters``, etc.). We don't parse the full grammar — we
implement only the patterns the generator emits today, and fail loud on
anything outside the whitelist. Per ``docs/adf_mimic_design.md`` §7.

Two layers:

1. **String-template substitution** — ``"prefix-@{...}-suffix"`` strings
   contain inline expressions that resolve to substrings. The most
   common shape (e.g., ``"@{pipeline().parameters.x}"``).
2. **Whole-value expression** — when the value of a JSON property is
   ``{"type": "Expression", "value": "@..."}`` the entire result is an
   evaluated expression (may be non-string).

Supported operators (verified against generator output):

- ``@pipeline().parameters.<name>``
- ``@pipeline().RunId``
- ``@variables('<name>')``
- ``@activity('<name>').output.firstRow.<col>``
- ``@activity('<name>').output.value``
- ``@activity('<name>').output.count``
- ``@item()`` and ``@item().<col>`` (in ForEach)
- ``@concat(<a>, <b>, ...)``
- ``@string(<x>)`` / ``@int(<x>)``
- ``@equals(<a>, <b>)``
- ``@utcnow()``
- ``@guid()``
- Literal strings, ints, bools, ``null``

Anything else raises ``UnsupportedExpressionError``.
"""

from __future__ import annotations

import re
import uuid
from datetime import UTC, datetime
from typing import Any

from lake_ingestion_framework.core.exceptions import LakeIngestError
from lake_ingestion_framework.realms.adf_adls.local.mimic.run_state import RunState


class UnsupportedExpressionError(LakeIngestError):
    """An expression uses an operator/function not in the v1 whitelist."""


# ──────────────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────────────


def evaluate(node: Any, run_state: RunState) -> Any:
    """Evaluate an arbitrary JSON node against the run state.

    Recognises:

    - ``str``: if it contains ``@{...}`` segments, substitute each;
      otherwise return as-is.
    - ``{"type": "Expression", "value": "@..."}``: evaluate the inner
      expression and return the result (may be non-string).
    - ``dict`` / ``list``: recurse for nested expression substitution
      (used to resolve LinkedService / Dataset parameter blocks).
    - other types: return unchanged.
    """
    if isinstance(node, str):
        return _evaluate_string(node, run_state)
    if isinstance(node, dict):
        if node.get("type") == "Expression" and "value" in node:
            value = node["value"]
            if not isinstance(value, str):
                return value
            return _evaluate_string(value, run_state)
        return {k: evaluate(v, run_state) for k, v in node.items()}
    if isinstance(node, list):
        return [evaluate(v, run_state) for v in node]
    return node


# ──────────────────────────────────────────────────────────────────────
# Internals
# ──────────────────────────────────────────────────────────────────────


# Match either a whole-string expression "@..." or an embedded "@{...}"
# segment. Whole-string expressions are resolved as their native type;
# embedded segments are stringified into the surrounding text.
_TEMPLATE_RE = re.compile(r"@\{([^{}]+)\}")
_WHOLE_EXPR_RE = re.compile(r"^@(?!\{)(.+)$")


def _evaluate_string(value: str, run_state: RunState) -> Any:
    # Whole-string expression like "@pipeline().parameters.x" returns the
    # native value (could be non-string).
    whole = _WHOLE_EXPR_RE.match(value)
    if whole is not None:
        return _evaluate_expression(whole.group(1), run_state)

    # Otherwise substitute @{...} segments inside the string.
    if "@{" not in value:
        return value

    def _substitute(match: re.Match[str]) -> str:
        result = _evaluate_expression(match.group(1), run_state)
        return str(result) if result is not None else ""

    return _TEMPLATE_RE.sub(_substitute, value)


# Pattern matchers for each whitelist case. Tried in order; first match
# wins. A small set keeps the evaluator legible.

_PIPELINE_PARAM_RE = re.compile(r"^pipeline\(\)\.parameters\.([A-Za-z_][A-Za-z0-9_]*)$")
_PIPELINE_RUNID_RE = re.compile(r"^pipeline\(\)\.RunId$")
_VARIABLES_RE = re.compile(r"^variables\('([A-Za-z_][A-Za-z0-9_]*)'\)$")
_ACTIVITY_OUTPUT_RE = re.compile(r"^activity\('([^']+)'\)\.output(?:\.([A-Za-z_][A-Za-z0-9_.]*))?$")
_ITEM_RE = re.compile(r"^item\(\)(?:\.([A-Za-z_][A-Za-z0-9_]*))?$")
_FUNC_RE = re.compile(r"^([A-Za-z_]+)\((.*)\)$", re.DOTALL)


def _evaluate_expression(expr: str, run_state: RunState) -> Any:  # noqa: PLR0911 — pattern dispatch
    """Resolve a stripped-of-leading-``@`` expression to its native value."""
    expr = expr.strip()

    if (m := _PIPELINE_PARAM_RE.match(expr)) is not None:
        return run_state.get_parameter(m.group(1))

    if _PIPELINE_RUNID_RE.match(expr):
        return run_state.run_id

    if (m := _VARIABLES_RE.match(expr)) is not None:
        return run_state.get_variable(m.group(1))

    if (m := _ACTIVITY_OUTPUT_RE.match(expr)) is not None:
        return _resolve_activity_output(m.group(1), m.group(2), run_state)

    if (m := _ITEM_RE.match(expr)) is not None:
        return _resolve_item(m.group(1), run_state)

    if (m := _FUNC_RE.match(expr)) is not None:
        return _call_function(m.group(1), m.group(2), run_state)

    # Literal — quoted string or numeric/boolean
    return _parse_literal(expr)


def _resolve_activity_output(name: str, path: str | None, run_state: RunState) -> Any:
    if name not in run_state.activity_outputs:
        raise UnsupportedExpressionError(
            f"Activity {name!r} has no recorded output. "
            "Did the activity run before this expression?"
        )
    cursor: Any = run_state.activity_outputs[name]
    if path is None:
        return cursor
    for segment in path.split("."):
        if isinstance(cursor, dict) and segment in cursor:
            cursor = cursor[segment]
        else:
            raise UnsupportedExpressionError(
                f"Activity {name!r} output has no path segment {segment!r} "
                f"(full path: output.{path})."
            )
    return cursor


def _resolve_item(path: str | None, run_state: RunState) -> Any:
    if run_state.iteration is None:
        raise UnsupportedExpressionError(
            "@item() referenced outside ForEach (no iteration context)."
        )
    _, item = run_state.iteration
    if path is None:
        return item
    if not isinstance(item, dict) or path not in item:
        raise UnsupportedExpressionError(
            f"@item() has no key {path!r} (item is {type(item).__name__})."
        )
    return item[path]


# ──────────────────────────────────────────────────────────────────────
# Function whitelist
# ──────────────────────────────────────────────────────────────────────


def _call_function(fname: str, args_str: str, run_state: RunState) -> Any:
    fname_lower = fname.lower()
    args = _split_function_args(args_str)
    evaluated = [_evaluate_expression(a.strip(), run_state) for a in args]

    if fname_lower == "concat":
        return "".join("" if v is None else str(v) for v in evaluated)
    if fname_lower == "string":
        if len(evaluated) != 1:
            raise UnsupportedExpressionError(f"@string() expects 1 arg, got {len(evaluated)}")
        return "" if evaluated[0] is None else str(evaluated[0])
    if fname_lower == "int":
        if len(evaluated) != 1:
            raise UnsupportedExpressionError(f"@int() expects 1 arg, got {len(evaluated)}")
        return int(evaluated[0])
    if fname_lower == "equals":
        if len(evaluated) != 2:  # noqa: PLR2004 — fixed binary operator arity
            raise UnsupportedExpressionError(f"@equals() expects 2 args, got {len(evaluated)}")
        return evaluated[0] == evaluated[1]
    if fname_lower == "utcnow":
        return datetime.now(UTC).isoformat()
    if fname_lower == "guid":
        return str(uuid.uuid4())

    raise UnsupportedExpressionError(
        f"Function @{fname}() is not in the ADF Mimic v1 whitelist. "
        "Either extend the evaluator's function table or check why the "
        "generator emits this expression."
    )


def _split_function_args(args_str: str) -> list[str]:
    """Split a comma-separated argument list, respecting parens + quotes."""
    if not args_str.strip():
        return []
    out: list[str] = []
    depth = 0
    in_quote = False
    quote_char = ""
    current: list[str] = []
    for ch in args_str:
        if in_quote:
            current.append(ch)
            if ch == quote_char:
                in_quote = False
            continue
        if ch in {"'", '"'}:
            in_quote = True
            quote_char = ch
            current.append(ch)
            continue
        if ch == "(":
            depth += 1
            current.append(ch)
            continue
        if ch == ")":
            depth -= 1
            current.append(ch)
            continue
        if ch == "," and depth == 0:
            out.append("".join(current))
            current = []
            continue
        current.append(ch)
    if current:
        out.append("".join(current))
    return out


def _parse_literal(value: str) -> Any:  # noqa: PLR0911 — flat type-dispatch reads cleaner
    if value.startswith("'") and value.endswith("'"):
        return value[1:-1]
    if value.startswith('"') and value.endswith('"'):
        return value[1:-1]
    if value == "true":
        return True
    if value == "false":
        return False
    if value == "null":
        return None
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        pass
    raise UnsupportedExpressionError(
        f"Cannot evaluate {value!r} as any supported pattern. "
        "Either it's a literal in an unrecognised form or the operator is "
        "outside the v1 whitelist."
    )


__all__ = ["UnsupportedExpressionError", "evaluate"]
