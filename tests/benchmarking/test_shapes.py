"""Worst-case data shapes: the profiles file, the slice writer and the tyre world's `stress:` block.

The rule that matters most: a world with no stress block (or one that restates the defaults) is the same world as before,
so the oracle and every earlier test still hold. Needs ~/data-platform-catalog for the world tests.
"""

import copy
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2] / "benchmarking"
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT.parent / "src"))

from bench import plan, shapes  # noqa: E402

KNOWN = {"dealers_multiplier", "materials_multiplier", "dealer_skew_alpha", "dealer_weight_cap",
         "material_revisions_per_day"}
SLICE = Path.home() / "data-platform-catalog/slices/02-tyre"


@pytest.mark.parametrize(
    ("case", "profile", "expect"),
    [
        ("empty profile adds nothing", {}, ""),
        ("one setting", {"dealers_multiplier": 20}, "  dealers_multiplier: 20"),
        ("sorted, floats kept", {"dealer_skew_alpha": 0.8, "dealer_weight_cap": 200.0},
         "  dealer_skew_alpha: 0.8\n  dealer_weight_cap: 200.0"),
    ],
)
def test_stress_block(case, profile, expect):
    assert expect in shapes.stress_block(profile, KNOWN)


@pytest.mark.parametrize(
    ("case", "profile", "match"),
    [
        ("unknown setting", {"colour": 1}, "unknown stress setting"),
        ("not a number", {"dealers_multiplier": "many"}, "must be a number"),
        ("a boolean is not a number", {"dealers_multiplier": True}, "must be a number"),
    ],
)
def test_stress_block_refuses(case, profile, match):
    with pytest.raises(ValueError, match=match):
        shapes.stress_block(profile, KNOWN)


def test_every_shipped_profile_uses_only_settings_the_world_knows():
    for name, prof in shapes.profiles().items():
        shapes.stress_block(prof, shapes.known_settings())  # raises on an unknown or non-numeric setting
        assert isinstance(name, str)


def test_case_profiles_name_real_cases_and_profiles():
    sh = plan.load(ROOT / "scenarios/shapes.yaml")
    cases = {c["id"] for c in plan.load(ROOT / "scenarios/powerbi_worst_cases.yaml")["cases"]}
    assert set(sh["case_profiles"]) <= cases
    assert set(sh["case_profiles"].values()) <= set(sh["profiles"])


def test_write_slice_appends_the_block_and_refuses_a_second(tmp_path):
    src = tmp_path / "slice"
    src.mkdir()
    (src / "world.yaml").write_text("name: w\nseed: 1\n")
    (src / "contracts.txt").write_text("x")
    out = shapes.write_slice(src, tmp_path / "out", {"dealers_multiplier": 3}, KNOWN)
    assert yaml.safe_load((out / "world.yaml").read_text())["stress"] == {"dealers_multiplier": 3}
    assert (out / "contracts.txt").exists()
    assert "stress" not in yaml.safe_load((src / "world.yaml").read_text())
    with pytest.raises(ValueError, match="already has a stress block"):
        shapes.write_slice(out, tmp_path / "out2", {"dealers_multiplier": 3}, KNOWN)


# ------------------------------------------------------------------------------------------------- the tyre world
@pytest.fixture(scope="module")
def dwd():
    if not SLICE.exists():
        pytest.skip("needs ~/data-platform-catalog")
    pytest.importorskip("pyspark")
    from dataplatform.world import dwd as loader

    return loader.load(SLICE, None)


def _company(dwd, **stress):
    from dataplatform.world.domains.tyre import TyreCompany

    d = copy.deepcopy(dwd)
    if stress:
        d.world["stress"] = stress
    return TyreCompany(d, 0.02)


def test_defaults_restated_are_the_same_world(dwd):
    from dataplatform.world.domains.tyre import STRESS_DEFAULTS

    a, b = _company(dwd), _company(dwd, **STRESS_DEFAULTS)
    assert a.dealer_weight == b.dealer_weight
    assert a.materials == b.materials
    assert a.price == b.price


@pytest.mark.parametrize(
    ("case", "stress", "check"),
    [
        ("more dealers", {"dealers_multiplier": 3}, lambda base, w: len(w.dealers) == 3 * len(base.dealers)),
        ("more materials", {"materials_multiplier": 2}, lambda base, w: len(w.materials) == 2 * len(base.materials)),
        ("heavier skew", {"dealer_skew_alpha": 0.5, "dealer_weight_cap": 1000.0},
         lambda base, w: max(w.dealer_weight.values()) > max(base.dealer_weight.values())),
    ],
)
def test_stress_changes_the_population(dwd, case, stress, check):
    base = _company(dwd)
    assert check(base, _company(dwd, **stress))


def test_more_material_revisions_make_more_history(dwd):
    from dataplatform.world.domains import tyre

    def revised(company):
        company.day(1)
        e = company.day(2)
        return sum(len(ch) for _, ds, ch in e.changes if ds == tyre.MAT)

    assert revised(_company(dwd, material_revisions_per_day=10)) == revised(_company(dwd)) + 8


def test_an_unknown_stress_setting_is_refused(dwd):
    with pytest.raises(ValueError, match="unknown stress setting"):
        _company(dwd, colour=1)
