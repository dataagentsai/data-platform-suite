"""The benchmark mechanics (benchmarking/bench): the scenario plan, the maintenance probes and the Power BI query runner.
No Spark, no network, no Power BI: stand-ins record what would have been sent."""

import sys
from pathlib import Path
from typing import Any

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2] / "benchmarking"
sys.path.insert(0, str(ROOT))

from bench import dax, maintenance, plan  # noqa: E402

PB = {"cases": [{"id": "PB-01", "name": "n", "hard_because": "h", "data_shape": {"a": 1}, "modes": ["import"],
                 "pass_if": "p"}]}
MT = {"operations": [{"id": "M-01", "expect": dict.fromkeys(plan.FORMAT_KEY.values(), {}), "measure": ["x"]}]}


def _pb(**over: Any) -> dict[str, Any]:
    return {"cases": [{**PB["cases"][0], **over}]}


@pytest.mark.parametrize(
    ("case", "powerbi", "maint", "expect"),
    [
        ("valid", PB, MT, []),
        ("repeated case id", {"cases": PB["cases"] * 2}, MT, ["repeated"]),
        ("missing pass_if", _pb(pass_if=""), MT, ["pass_if is missing"]),
        ("unknown mode", _pb(modes=["excel"]), MT, ["unknown mode 'excel'"]),
        ("id clash across files", PB, {"operations": [{**MT["operations"][0], "id": "PB-01"}]}, ["repeated"]),
        ("a format has no expectation", PB,
         {"operations": [{"id": "M-01", "expect": {"uc_managed_iceberg": {}}, "measure": ["x"]}]},
         ["no expectation for uc_managed_delta_uniform", "no expectation for aws_s3_tables"]),
        ("no measure", PB, {"operations": [{**MT["operations"][0], "measure": []}]}, ["measure is missing"]),
    ],
)
def test_problems(case, powerbi, maint, expect):
    got = plan.problems(powerbi, maint)
    assert len(got) == len(expect), got
    assert all(any(e in g for g in got) for e in expect)


def test_shipped_scenarios_are_valid_and_cover_every_format():
    pb = plan.load(ROOT / "scenarios/powerbi_worst_cases.yaml")
    mt = plan.load(ROOT / "scenarios/maintenance.yaml")
    assert plan.problems(pb, mt) == []
    runs = plan.expand(pb, mt)
    pb_modes = sum(len(c["modes"]) for c in pb["cases"])
    assert len(runs) == len(plan.FORMATS) * (pb_modes + len(mt["operations"]))
    assert len({r["id"] for r in runs}) == len(runs)
    assert {r["format"] for r in runs} == set(plan.FORMATS)


def test_every_maintenance_cell_starts_unverified():
    mt = plan.load(ROOT / "scenarios/maintenance.yaml")
    cells = [c for op in mt["operations"] for c in op["expect"].values()]
    assert cells and all(c["status"] == "unverified" for c in cells)


# ------------------------------------------------------------------------------------------------ maintenance
class _Row(dict):
    def __getitem__(self, k: Any) -> Any:
        return super().__getitem__(k) if isinstance(k, str) else list(self.values())[k]


class FakeSpark:
    """Records every statement; raises for the ones whose text contains a refused fragment."""

    def __init__(self, refuse: tuple[str, ...] = ()):
        self.refuse, self.sent = refuse, []

    def sql(self, text: str) -> "FakeSpark":
        self.sent.append(text)
        if any(r in text for r in self.refuse):
            raise RuntimeError(f"refused: {text}\nsecond line")
        self._last = text
        return self

    def first(self) -> _Row:
        return _Row(numFiles=7, n=7)

    def collect(self) -> list[Any]:
        return []


@pytest.mark.parametrize(
    ("case", "fmt", "statements"),
    [
        ("managed iceberg uses databricks commands", "iceberg-uc",
         ["OPTIMIZE `c`.`s`.`t`", "VACUUM `c`.`s`.`t`", "ALTER TABLE `c`.`s`.`t` CLUSTER BY (k)"]),
        ("s3 tables uses the iceberg procedures", "s3tables",
         ["CALL cat.system.rewrite_data_files(table => 'ns.t')", "CALL cat.system.expire_snapshots(table => 'ns.t', retain_last => 1)"]),
    ],
)
def test_probes_choose_statements_by_format(case, fmt, statements):
    spark = FakeSpark()
    maintenance.run_probes(spark, fmt, "`c`.`s`.`t`" if fmt != "s3tables" else "`cat`.`ns`.`t`", "ns.t", "cat")
    for s in statements:
        assert s in spark.sent
    if fmt == "s3tables":
        assert not any(s.startswith(("OPTIMIZE", "VACUUM")) for s in spark.sent)


@pytest.mark.parametrize(
    ("case", "refuse", "expect"),
    [
        ("everything works", (), {"M-01": "supported", "M-02": "supported", "M-03": "supported"}),
        ("vacuum refused", ("VACUUM",), {"M-01": "supported", "M-02": "refused", "M-03": "supported"}),
        ("only the second statement refused", ("CLUSTER BY",), {"M-01": "supported", "M-02": "supported", "M-03": "refused"}),
    ],
)
def test_a_refusal_is_a_result_not_a_crash(case, refuse, expect):
    res = maintenance.run_probes(FakeSpark(refuse), "iceberg-uc", "`c`.`s`.`t`", "s.t", "c")
    got = maintenance.verdicts(res)
    assert {k: got[k] for k in expect} == expect
    refused = [r for r in res if not r["ok"]]
    assert all(r["error"] and "\n" not in r["error"] and len(r["error"]) <= 300 for r in refused)


def test_a_probe_with_two_statements_is_partial_when_one_fails():
    res = maintenance.run_probes(FakeSpark(("OPTIMIZE",)), "delta-uc-uniform", "`c`.`s`.`t`", "s.t", "c")
    assert maintenance.verdicts(res)["M-06"] == "partial"


@pytest.mark.parametrize("bad", ["a b", "t;drop table x", "`t`; --", ""])
def test_render_refuses_unsafe_names(bad):
    with pytest.raises(ValueError, match="not a usable name"):
        maintenance.render("OPTIMIZE {t}", bad, "ns.t", "cat")


def test_file_counts_are_recorded_before_and_after():
    res = maintenance.run_probes(FakeSpark(), "iceberg-uc", "`c`.`s`.`t`", "s.t", "c")
    last = next(r for r in res if r["op"] == "M-01")
    assert last["files_before"] == 7 and last["files_after"] == 7


# ------------------------------------------------------------------------------------------------------ power bi
@pytest.mark.parametrize(
    ("case", "template", "names", "expect"),
    [
        ("filled", "COUNTROWS('{{fact}}')", {"fact": "Sales"}, "COUNTROWS('Sales')"),
        ("spaces inside the braces", "{{ fact }}", {"fact": "Sales"}, "Sales"),
        ("same name twice", "{{a}}{{a}}", {"a": "x"}, "xx"),
    ],
)
def test_fill(case, template, names, expect):
    assert dax.fill(template, names) == expect


def test_fill_refuses_a_half_filled_query():
    with pytest.raises(KeyError, match="dim, fact"):
        dax.fill("{{fact}} {{dim}}", {})


WS, DS = "11111111-1111-1111-1111-111111111111", "22222222-2222-2222-2222-222222222222"


def test_request_shape_and_ids_checked():
    r = dax.request(WS, DS, "EVALUATE ROW(\"a\", 1)", "tok")
    assert r.full_url == f"https://api.powerbi.com/v1.0/myorg/groups/{WS}/datasets/{DS}/executeQueries"
    assert r.get_header("Authorization") == "Bearer tok" and r.get_method() == "POST"
    with pytest.raises(ValueError, match="must be ids"):
        dax.request("../x", DS, "q", "tok")


def _clock() -> Any:
    ticks = iter(x * 0.5 for x in range(1000))
    return lambda: next(ticks)


@pytest.mark.parametrize(
    ("case", "answers", "cold_s", "failed"),
    [
        ("all fine", [(200, {"results": [{"tables": [{"rows": [{"a": 1}]}]}]})] * 3, 0.5, 0),
        ("one failure", [(200, {}), (500, {"error": "x"}), (200, {})], 0.5, 1),
    ],
)
def test_run_pack_and_summary(case, answers, cold_s, failed):
    it = iter(answers)
    res = dax.run_pack([{"id": "Q", "case": "PB-02", "dax": "{{t}}"}], {"t": "x"}, WS, DS, "tok", 3,
                       send=lambda req: next(it), clock=_clock())
    assert [r["cold"] for r in res] == [True, False, False]
    s = dax.summary(res)["Q"]
    assert s["cold_s"] == cold_s and s["failed"] == failed


def test_the_shipped_query_pack_needs_only_the_listed_names():
    pack = yaml.safe_load((ROOT / "powerbi/queries.yaml").read_text())
    needed = set(pack["names_needed"])
    used = {n for q in pack["queries"] for n in dax._HOLE.findall(q["dax"])}  # noqa: SLF001
    assert used <= needed
    assert all(q["case"] in {c["id"] for c in plan.load(ROOT / "scenarios/powerbi_worst_cases.yaml")["cases"]}
               for q in pack["queries"])


@pytest.mark.parametrize("bad", ["a b", "t;drop table x", "`t`; --", ""])
def test_file_count_and_timing_refuse_unsafe_names(bad):
    with pytest.raises(ValueError, match="not a usable name"):
        maintenance.count_files(FakeSpark(), bad, "iceberg-uc")
    with pytest.raises(ValueError, match="not a usable name"):
        maintenance.timed_query(FakeSpark(), bad)


@pytest.mark.parametrize(
    ("case", "values", "expect"),
    [
        ("the whole word", [10, 50, 95], [10, 50, 95]),
        ("one value", [7], [7, 7, 7]),
        ("none", [], [0.0, 0.0, 0.0]),
    ],
)
def test_percentile(case, values, expect):
    assert [dax.percentile(values, p) for p in (0, 50, 100)][0] == expect[0]
    assert dax.percentile(values, 100) == (max(values) if values else 0.0)


def test_applies_and_expand_pure_helpers():
    p = maintenance.Probe("M-99", "x", ("SELECT 1",), ("s3tables",))
    assert maintenance.applies(p, "s3tables") and not maintenance.applies(p, "iceberg-uc")
    runs = plan.expand(PB, MT, ("iceberg-uc",), days=2, scale=0.5, case_profiles={"PB-01": "scd2_deep"})
    assert {r["world"]["shape"] for r in runs if r["kind"] == "powerbi"} == {"scd2_deep"}
    assert {r["world"]["days"] for r in runs} == {2}
