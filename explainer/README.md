# The explainer

A guided tour of this platform, from one picture down to the line of code, built from this repository. It ships with the
platform: it needs nothing from the internet, and it describes the code you actually have.

    make explainer            # writes build/explainer/ ; open build/explainer/index.html

What it reads: this repository's code, tests and run reports, and the specification catalog (set `DP_CATALOG` to its
checkout; by default a sibling folder). What it writes: static pages, with every code box cut from the real file and
linked to its line by a relative path. The build refuses a missing code anchor or a dead link, so the pages cannot drift
from the code without the build saying so.

Also available as plain text, with no model and no network, from `scripts/explain.py`: what a run did (`run`), what a
dataset's manifest makes the platform do (`metadata`), what checks a change passes (`cicd`), and where a function is and who
calls it (`code`).

    explainer/build.py        the generator       explainer/lib.py       helpers every page uses
    explainer/pages_*.py      the pages           explainer/callgraph.py who calls what, found by reading the code
    explainer/kit.py          the drawing kit     explainer/data/        rule counts per concern (make_coverage.py)
