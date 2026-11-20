"""The concerns: one page per cross-cutting concern (X1..X24), all with the same shape. Pilot: X4 data quality.

Page shape (every concern page follows it, so a reader learns it once):
  1. one plain paragraph: why the concern exists
  2. a picture of where it happens in the platform; every box opens the page that explains that part
  3. a table of the places it happens: what is checked, what a failure does
  4. request and response: for every major function, what goes in and what comes out
  5. one real run: inputs and outputs printed by the code, not written by hand
  6. the code, the settings, the tests, and what is not built yet
"""
import json

from lib import *  # noqa: F401,F403

CLEAN = "P/src/dataplatform/stages/clean.py"
COMMON = "P/src/dataplatform/stages/common.py"
RULES = "P/src/dataplatform/silver/contract_rules.py"
RUNNER = "P/src/dataplatform/silver/runner.py"
LANDER = "P/src/dataplatform/lander.py"
T_STAGES = "P/tests/dataplatform/test_silver_stages.py"
T_RULES = "P/tests/dataplatform/test_contract_rules.py"
DATA = HERE / "data"

FAMILIES = [
    ("Run", [("X1", "Metadata"), ("X2", "Orchestration"), ("X15", "Replay"), ("X16", "CI/CD"), ("X17", "Testing"),
             ("X18", "Resilience"), ("X23", "Layout"), ("X24", "Streaming")]),
    ("Trust", [("X3", "Contracts"), ("X4", "Data quality"), ("X5", "Profiling"), ("X6", "Reconciliation")]),
    ("See", [("X7", "Lineage"), ("X8", "Observability"), ("X9", "Freshness"), ("X10", "Cost"), ("X19", "Performance")]),
    ("Guard", [("X11", "Access"), ("X12", "Privacy"), ("X13", "Governance"), ("X14", "Audit")]),
    ("Self", [("X20", "Self-healing"), ("X21", "Agents"), ("X22", "Simulation")]),
]
WRITTEN = {"X4": "concern-x4"}     # concern id -> page id; grows as concern pages are written


def _coverage(cid):
    try:
        rules = json.loads((DATA / "coverage.json").read_text())["rules"].get(cid, [])
    except OSError:
        return []
    return rules


@page("concerns", "Every concern, one page each", short="Concerns", parent=None,
      blurb="The 24 cross-cutting concerns as a map. Each opens a page with a picture, the functions, what goes in and what comes out.")
def concerns():
    out = ("<p class='lead'>A platform is more than its layers. Some jobs cut across all of them: checking quality, "
           "keeping history, controlling access, recording what happened. We call these <b>concerns</b> and number "
           "them X1 to X24. Each concern gets one page, and every page has the same shape, so you learn the shape once.</p>")
    d, y0 = "", 20
    col_w = 196
    for i, (fam, items) in enumerate(FAMILIES):
        x = 10 + i * (col_w + 8)
        d += box(x, y0, col_w, 46, fam, "", "solid")
        for j, (cid, name) in enumerate(items):
            pid = WRITTEN.get(cid)
            d += box(x, y0 + 66 + j * 54, col_w, 44, f"{cid} {name}", "", "bx" if pid else "bxm",
                     href=f"{pid}.html" if pid else None, tag="open")
    out += "<h2>The map</h2>"
    out += fig(1010, 20 + 66 + 8 * 54 + 10, d, "The 24 concerns in five families",
               "Solid boxes are the five families. Dashed boxes have no page yet. Boxes with a tab open their page.")
    out += ("<h2>How to read a concern page</h2>" + ladder([
        ("Why", "One plain paragraph on what goes wrong without this concern."),
        ("The picture", "Where the concern happens along the data's path. Every box opens the page for that part."),
        ("The places", "A table: each place the concern is checked, what is checked, and what a failure does."),
        ("Request and response", "For each major function: what goes in, what comes out, and who calls it."),
        ("One real run", "Inputs and outputs printed by the code on a small example. Nothing here is typed by hand."),
        ("Code and tests", "The code cut from the real file, the settings that drive it, the tests, and what is not built."),
    ]))
    rows = []
    for fam, items in FAMILIES:
        for cid, name in items:
            pid = WRITTEN.get(cid)
            rules = _coverage(cid)
            tested = sum(r["state"] == "tested" for r in rules)
            rows.append([fam, cid, name, f"{tested} of {len(rules)} rules tested" if rules else "no rules in the catalog",
                         link(pid, "Open the page") if pid else "Page to come"])
    out += "<h2>All 24</h2>" + table(["Family", "#", "Concern", "Rules proven by a test", "Page"], rows)
    return out


def x4_picture():
    d = text(10, 24, "A delivery moves left to right. Each box is a place where quality is decided.", "sm")
    y = 50
    d += box(10, y, 150, 86, "A delivery", ["one file from", "a source"], "bxm")
    d += box(200, y, 170, 86, "Lander", ["count and column", "checks"], "bx", href="ingest-lander.html")
    d += box(410, y, 150, 86, "Bronze", ["as received,", "nothing lost"], "bx", href="ingest.html")
    d += box(600, y, 190, 86, "Silver quality stage", ["row rules, grade", "by severity"], "solid", href="silver-quality.html")
    d += box(830, y, 170, 86, "Silver", ["good rows, with", "flags"], "bx", href="silver.html")
    for a, b in ((160, 200), (370, 410), (560, 600), (790, 830)):
        d += arrow(a + 2, y + 43, b - 4, y + 43)
    d += box(200, 190, 170, 70, "Rejected", ["whole delivery,", "not loaded"], "bxs", href="ingest-lander.html")
    d += arrow(285, y + 88, 285, 188, "wrong count", anchor="start", lx=292, ly=165)
    d += box(600, 190, 190, 70, "Quarantine table", ["the row and why"], "bxs", href="silver-quality.html")
    d += arrow(695, y + 88, 695, 188, "major or critical", anchor="start", lx=702, ly=165)
    d += box(830, 190, 170, 70, "Whole batch held", ["bookmark stays"], "bxs", href="silver-quality.html")
    d += path(f"M 755 {y + 88} C 760 150 860 140 900 188", "ln")
    d += text(852, 150, "critical", "sm")
    d += box(410, 300, 290, 70, "Run log and alerts", ["every verdict is written down"], "bxm", href="obs-runlog.html")
    d += box(730, 300, 270, 70, "Duplicates stage", ["same key twice in a batch"], "bx", href="silver-duplicates.html")
    return d


@page("concern-x4", "Data quality: what happens to a bad row", parent="concerns", short="X4 Data quality",
      blurb="Rules on rows and deliveries. A bad row is set aside with its reason, a critical failure holds the batch, and nothing is dropped silently.")
def concern_x4():
    out = ("<p class='lead'>Data from a source is never perfect. A quantity is negative, a plant code does not exist, a "
           "required value is empty. Data quality is the job of noticing this and deciding what happens next, without "
           "stopping the good data and without letting the bad data into a report.</p>"
           "<p>Two simple designs both fail. If every problem stopped the load, one bad record would hold back a day of "
           "good ones. If every problem were only logged, bad rows would end up in dashboards. This platform grades "
           "each rule by <b>severity</b>, and the severity decides the outcome.</p>")
    out += "<h2>The picture</h2>"
    out += fig(1030, 390, x4_picture(), "Where quality is decided on the path from delivery to silver",
               "Follow the top row. Under it, the three places a delivery or a row can be set aside. Click any box to zoom in.")
    out += "<h2>The places quality is decided</h2>"
    out += table(["Place", "What is checked", "If it fails", "Page"], [
        ["Lander", "The file has the contract's columns. The row count equals the count the delivery announced, and sits inside the dataset's expected band.",
         "The whole delivery is rejected (<code>rejected_contract</code> or <code>rejected_volume</code>), not loaded, and the reason is logged.", link("ingest-lander", "Lander")],
        ["Silver: duplicates", "The same business key twice inside the source.", "Handled by the dataset's dedup setting.", link("silver-duplicates", "Duplicates")],
        ["Silver: quality stage, minor rule", "A row rule graded minor.", "The row is kept. The rule's name is added to <code>_dq_flags</code>.", link("silver-quality", "Quality")],
        ["Silver: quality stage, major rule", "A row rule graded major.", "The row is quarantined with the rule's name as the reason. The rest of the batch is written.", link("silver-quality", "Quality")],
        ["Silver: quality stage, critical rule", "A row rule graded critical, which includes every required column being filled.",
         "The row is quarantined <i>and</i> the whole batch is held. Nothing is written for the dataset and the bookmark stays.", link("silver-quality", "Quality")],
    ])
    rules = _coverage("X4")
    if rules:
        mark = {"tested": "a test names it", "built": "code only", "none": "nothing yet"}
        out += "<h2>The catalog's rules for this concern</h2>"
        out += table(["Rule", "Must or should", "What it asks", "State"],
                     [[r["id"], r["level"], r["title"], mark.get(r["state"], r["state"])] for r in rules])
        out += ("<p>The state is computed from the repository: a rule is tested when a test names it, built when only "
                "code names it. Three MUST rules have nothing yet, and they are listed again under "
                "<i>Not built yet</i> at the end.</p>")
    out += "<h2>Request and response, function by function</h2>"
    out += ("<p>Each row is one function a reader will meet. <b>Goes in</b> is what it is handed. <b>Comes out</b> is "
            "what it gives back. Where a function reports instead of returning, the last column says where.</p>")
    out += table(["Function", "Goes in", "Comes out", "Called by"], [
        [fn(LANDER, "_decide"), "One delivery, its rows, the column verdict, the dataset's expected-rows band.",
         "A <code>LandOutcome</code> with status <code>loaded</code>, <code>rejected_contract</code> or <code>rejected_volume</code>, and a reason sentence.", "<code>land</code> in the same file"],
        [fn(RULES, "translate"), "One contract rule from the metadata store: its check, column, severity, action and parameters.",
         "A stage <code>Rule</code> (name, SQL condition, severity, row action), <i>or</i> a sentence saying why it is not a row rule.", "<code>contract_rules</code>"],
        [fn(RULES, "contract_rules"), "All the contract's rules for the dataset.",
         "Two things: the row rules, and one reason sentence for every rule left out. Nothing is dropped silently.", "<code>build_silver</code>, the silver runner"],
        [fn(RULES, "with_overrides"), "The row rules and the manifest's severity-to-action choices.",
         "The same rules with the action replaced by the dataset's choice. A dataset may flag what the contract would quarantine.", "<code>build_silver</code>"],
        [fn(CLEAN, "quality"), "A frame of rows and the plan with all its rules.",
         "An <code>Out</code> of three parts: the good rows (with <code>_dq_flags</code>), the quarantine records, and two counts, <code>flagged</code> and <code>critical</code>.", "the stage runner"],
        [fn(COMMON, "quarantine_rows"), "A frame and one column holding the failure reason (empty means good).",
         "The good rows, and quarantine records: stage name, reason, batch key, source file, and the whole original row as JSON.", "<code>quality</code> and other stages"],
        [fn(COMMON, "add_flags"), "A frame and the pairs (condition, flag name).", "The frame with the names of failed flag rules appended to <code>_dq_flags</code>.", "<code>quality</code>"],
        [fn(RUNNER, "build_silver"), "A dataset's settings and its new bronze batches.",
         "A <code>SilverDataset</code> with status <code>succeeded</code> or <code>skipped</code>. If <code>quality.critical</code> is above zero it writes nothing and logs the step <code>silver.quality</code> as failed.", "<code>run_silver</code>, the day loop"],
    ])
    out += "<h2>One real run</h2>"
    out += ("<p>Four sales order lines go through the quality stage with five rules in the plan: the order id and the "
            "dealer code must be filled (critical), the quantity is never negative (critical), the plant code is one of "
            "two (major), and the press cycle state is one of four known states (minor, flag only). The block below was "
            "<b>printed by the code</b> on a local Spark session, not typed. The code that produced it is the same "
            "stage function the daily run calls.</p>")
    ex = DATA / "x4_example.txt"
    out += raw(ex.read_text() if ex.exists() else "(example not generated yet)", "Inputs, then the stage's three outputs", "Printed by the stage")
    out += ("<p>Read it like this. SO-1001 is clean and has no flags. SO-1002 has a plant that does not exist: "
            "major, so it is quarantined. SO-1003 has a negative quantity and SO-1004 has no dealer code: both are "
            "critical, so the critical count is 2. The silver runner then holds the whole batch, which means even "
            "SO-1001 is not written until the batch is voided or resent. SO-1004's cycle state, <code>resting</code>, "
            "is not a known state, but the row was quarantined for the missing dealer, so no flag was added and "
            "<code>flagged</code> is 0. Had it been the only problem, the row would have stayed in silver with "
            "<code>cycle_state-invalidValues-1</code> in <code>_dq_flags</code>.</p>")
    out += "<h2>The code</h2>"
    out += code(CLEAN, r"^def quality\(", caption="The whole stage: build a failure condition per rule, sort the rules by action, quarantine, flag, count.")
    out += code(RULES, r"^def translate\(", caption="One contract rule in; one stage rule or one reason out.")
    out += code(RUNNER, r"    critical = int\(result\.counts\.get", r'return SilverDataset\(source, dataset, "skipped", dict\(result\.counts\)',
                "The runner reads the critical count before it writes anything.")
    out += ("<p>The full walk-through of the stage, the translation and the hold is on " + link("silver-quality") +
            ". The lander side is on " + link("ingest-lander") + ".</p>")
    out += "<h2>The settings that drive it</h2>"
    out += table(["Setting", "What it does", "Allowed values", "Default"], [
        ["<code>required</code> on a contract column", "Makes a critical rule that the column is filled.", "true or false", "false"],
        ["<code>quality</code> entries on a contract column", "The row rules, each with a severity.", "critical, major, minor", "major"],
        ["<code>quality.actions.&lt;severity&gt;.rows</code> in the dataset manifest", "The action for that severity, laid over what the contract says.", "quarantine, flag, none", "quarantine for critical and major, flag for minor"],
        ["<code>volume.expected_rows_per_delivery</code> in the dataset manifest", "The row-count band a delivery must fall in.", "min and max", "no limit"],
    ])
    out += "<h2>How it is tested</h2>"
    out += (f"<p>{src(T_STAGES)} has a table of cases for the stage and a test that one poison record does not stop its "
            f"batch. {src(T_RULES)} has a table of rules that must translate and a table of rules that must not.</p>")
    out += code(T_STAGES, r'"a good row passes"', 4, "The three stage cases.")
    out += not_built([
        "<b>QUAR-01:</b> a released row should go through the same checks again. Nothing in the code does this yet.",
        "<b>LATE-04:</b> time zones and business dates have no rule or test yet.",
        "<b>SCD-04:</b> facts joined to the right version of history is not tested yet.",
        "The data quality results table is never written in a normal run. Rule verdicts are in the run log, not in a table of their own.",
        "Declarative-engine expectations are described on " + link("silver-declarative") + ", and are separate from the stage above.",
    ])
    return out
