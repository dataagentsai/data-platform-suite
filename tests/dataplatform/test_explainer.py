"""The explainer ships with the platform (explainer/): it builds from this repository alone, offline, and every link in
it
leads somewhere real. Table-driven.

The build needs the specification catalog beside the repository (or DP_CATALOG); without it these tests skip.
"""

import re
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "explainer"))

import callgraph  # noqa: E402
import lib  # noqa: E402

CATALOG = lib.ROOTS["C"][0]
pytestmark = pytest.mark.skipif(not CATALOG.exists(), reason="needs the specification catalog (DP_CATALOG)")


@pytest.fixture(scope="module")
def built(tmp_path_factory):
    out = tmp_path_factory.mktemp("explainer")
    done = subprocess.run(  # noqa: S603 -- this repository's own generator, a fixed argument list
        [sys.executable, str(ROOT / "explainer/build.py"), "--out", str(out)],
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )
    assert done.returncode == 0, f"the explainer did not build:\n{done.stdout[-1500:]}\n{done.stderr[-1500:]}"
    return out


@pytest.mark.parametrize(
    "page",
    [
        "index",
        "map",
        "map-orchestration",
        "map-index",
        "map-callers",
        "flow",
        "meta",
        "ingest",
        "silver",
        "silver-scd2",
        "silver-deletes",
        "silver-declarative",
        "gold",
        "semantic",
        "obs",
        "tests",
        "tests-end-to-end",
    ],
)
def test_the_page_is_built(built, page):
    assert (built / f"{page}.html").exists(), page


@pytest.mark.parametrize(
    ("case", "pattern"),
    [
        ("no outside stylesheet or font", r'<link[^>]+href="https?://'),
        ("no outside script", r'<script[^>]+src="https?://'),
        ("no outside image", r'<img[^>]+src="https?://'),
        ("no style import", r"@import\s+url\(\s*['\"]?https?://"),
    ],
)
def test_the_output_makes_no_outside_request(built, case, pattern):
    bad = [p.name for p in built.glob("*.html") if re.search(pattern, p.read_text())]
    css = built / "style.css"
    if css.exists() and re.search(pattern, css.read_text()):
        bad.append("style.css")
    assert bad == [], case


def test_every_link_leads_to_a_page_or_a_file(built):
    missing = []
    for page in sorted(built.glob("*.html")):
        for href in re.findall(r'href="([^"#]+)', page.read_text()):
            if href.startswith(("http://", "https://", "mailto:")):
                continue
            if not (built / href.split("#")[0]).resolve().exists():
                missing.append(f"{page.name} -> {href}")
    assert missing == [], f"{len(missing)} link(s) lead nowhere, first: {missing[:5]}"


@pytest.mark.parametrize(
    ("definition", "called_by"),
    [
        ("src/dataplatform/silver/runner.py::build_silver", "scripts/run_hop.py::run_silver"),
        ("src/dataplatform/ingest/runner.py::ingest", "src/dataplatform/ingest/daybyday.py::run_days"),
        ("src/dataplatform/gold/harness.py::run_gold", "scripts/run_hop.py::run_gold"),
        ("src/dataplatform/ingest/daybyday.py::run_days", "scripts/run_platform.py::main"),
    ],
)
def test_the_call_graph_finds_the_known_callers(definition, called_by):
    g = callgraph.build(ROOT)
    assert definition in g.defs, definition
    assert called_by in {u.by for u in g.callers.get(definition, [])}


@pytest.mark.parametrize(
    ("source", "alias", "name", "want"),
    [
        ("def a():\n    pass\n", "", "a", True),
        ("class C:\n    def m(self):\n        pass\n", "", "C.m", True),
        ("x = 1\n", "", "nothing", False),
    ],
)
def test_definitions_are_found(tmp_path, source, alias, name, want):
    (tmp_path / "src/dataplatform").mkdir(parents=True)
    (tmp_path / "src/dataplatform/mod.py").write_text(source)
    (tmp_path / "tests").mkdir()
    g = callgraph.build(tmp_path)
    assert (f"src/dataplatform/mod.py::{name}" in g.defs) is want
