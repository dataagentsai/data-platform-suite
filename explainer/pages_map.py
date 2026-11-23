"""The code map: one page that shows the whole platform (upstream, platform, downstream), the starting point of every
layer, how a day is orchestrated, every numbered concern with who provides it and where it starts in the code, and the
way into every file and every function (index, and who calls what)."""
import json
from pathlib import Path

import lib
from lib import *  # noqa: F401,F403

COVERAGE = json.loads((Path(__file__).parent / "data/coverage.json").read_text())
P = "P/src/dataplatform/"

# --------------------------------------------------------------------------------- where each layer starts
lib.STARTS.update({
    "meta": [("Load a domain's manifests into the store", P + "meta/loader.py", "load"),
             ("The store's contract (every call the runners make)", P + "meta/port.py", "MetadataStore")],
    "ingest": [("One ingestion run (landing to bronze, pull to bronze)", P + "ingest/runner.py", "ingest"),
               ("The day loop that plays the world and judges bronze", P + "ingest/daybyday.py", "run_days")],
    "silver": [("One silver run for a domain", P + "silver/runner.py", "build_silver"),
               ("The thirteen stages for one dataset", P + "stages/__init__.py", "run_stages")],
    "gold": [("One gold run for a business date", P + "gold/harness.py", "run_gold")],
    "semantic": [("Read a metric definition", P + "semantic/metrics.py", "metric_view"),
                 ("Ask the known questions of built gold", P + "semantic/answers.py", "answers")],
    "obs": [("The monitor that alerts, pushed by every run", P + "obs/monitor.py", "Monitor"),
            ("Render the operations page", P + "obs/page.py", "render")],
    "tests": [("The whole platform and the simulated company, day by day", "P/scripts/run_platform.py", "main")],
})


def picture():
    d = ""
    for x, y, w, h in ((6, 36, 236, 850), (262, 36, 590, 850), (872, 36, 244, 850)):
        d += box(x, y, w, h, "", "", "bxm")
    d += text(124, 24, "UPSTREAM", "tt", "middle") + text(557, 24, "DATA PLATFORM", "tt", "middle")
    d += text(994, 24, "DOWNSTREAM", "tt", "middle")
    # upstream
    d += box(18, 66, 212, 92, "Source-side extraction", ["change capture, exports,", "file drops (push)"], "bxm",
             href="ingest-sources.html")
    d += box(18, 236, 212, 92, "Source systems", ["databases, files, APIs,", "streams"], "bxm", href="ingest-sources.html")
    d += box(18, 410, 212, 100, "Simulated company", ["plays a source on every day", "shown here, knows the truth"], "bxm",
             href="tests-world.html")
    d += arrow(124, 236, 124, 160) + arrow(124, 410, 124, 330)
    # the two ways in
    d += box(280, 76, 130, 72, "Landing", ["files as delivered"], "bx", href="ingest-lander.html")
    d += box(450, 76, 170, 72, "Lander", ["landing to bronze,", "entry steps, ingest()"], "bx", href="ingest-steps.html")
    d += box(450, 210, 170, 72, "Puller", ["source to bronze: full copy,", "pages, stream, ingest()"], "bx",
             href="ingest-pipelines.html")
    d += box(670, 76, 160, 206, "Bronze", ["every batch, as received", "nothing lost"], "bx", href="ingest-lander.html")
    d += arrow(232, 112, 278, 112) + arrow(412, 112, 448, 112) + arrow(622, 112, 668, 112)
    d += text(255, 100, "push", "sm", "middle")
    d += arrow(232, 276, 448, 246) + arrow(622, 246, 668, 246)
    d += text(340, 252, "pull", "sm", "middle")
    # the spine
    d += box(280, 344, 150, 82, "Silver", ["build_silver", "clean, keyed, history"], "bx", href="silver.html")
    d += box(480, 344, 150, 82, "Gold", ["run_gold", "dimensions, facts"], "bx", href="gold.html")
    d += box(680, 344, 150, 82, "Semantic", ["metric_view", "one meaning"], "bx", href="semantic.html")
    d += path("M750,284 L750,314 L355,314 L355,342", "ln") + arrow(432, 385, 478, 385) + arrow(632, 385, 678, 385)
    # the metadata store
    d += box(280, 480, 550, 86, "Metadata store (Lakebase, a managed PostgreSQL)",
             ["settings, columns, keys, file log, bookmarks, run log", "every step reads its settings here and writes what it did here"],
             "solid", href="meta.html")
    d += arrow(355, 480, 355, 428) + arrow(555, 480, 555, 428) + arrow(755, 480, 755, 428)
    d += path("M830,523 L846,523 L846,200 L832,200", "ln")
    # the concerns bands
    d += text(557, 602, "Across every layer: the concerns, numbered in the table below", "tx", "middle")
    d += box(280, 632, 270, 72, "Databricks Jobs", ["X2 schedules, tasks, retries"], "bxs", href="map-orchestration.html")
    d += box(560, 632, 270, 72, "Day loop, leases, bookmarks", ["X2 X15, ours"], "bx", href="map-orchestration.html")
    d += box(280, 730, 270, 72, "Data expectation services", ["X4 X5 X6, ours", "quality, profiling, reconciliation"], "bx",
             href="obs-reconcile.html")
    d += box(560, 730, 270, 72, "Lakeflow expectations", ["X4 X5, on the", "declarative engine"], "bxs",
             href="silver-declarative.html")
    d += box(280, 828, 270, 72, "Run log, tracing, alerts", ["X8 X9 X14, ours"], "bx", href="obs.html")
    d += box(560, 828, 270, 72, "Monitor, lineage, audit", ["X7 X8 X14, adopted", "from the platform"], "bxs",
             href="obs-lineage.html")
    # downstream
    d += box(886, 76, 216, 86, "Metric views", ["certified definitions"], "bx", href="semantic.html")
    d += box(886, 196, 216, 86, "Dashboards", ["operations (ours), job", "health (Databricks)"], "bx", href="obs-page.html")
    d += box(886, 316, 216, 86, "Conversation space", ["generated from the metrics"], "bx", href="semantic.html")
    d += box(886, 436, 216, 86, "Access and governance", ["X11 X13, grants and masks", "(Unity Catalog)"], "bxs",
             href="semantic.html")
    d += box(886, 566, 216, 120, "Not in this repository", ["ML, reverse ETL,", "BI tools"], "bxm")
    d += arrow(832, 385, 858, 385) + path("M858,119 L858,387", "ln", end=False)
    d += arrow(858, 119, 884, 119) + arrow(858, 239, 884, 239) + arrow(858, 359, 884, 359)
    # legend
    d += box(280, 928, 22, 16, "", "", "bx") + text(310, 941, "built here", "sm")
    d += box(420, 928, 22, 16, "", "", "bxs") + text(450, 941, "provided by Databricks, adopted", "sm")
    d += box(690, 928, 22, 16, "", "", "bxm") + text(720, 941, "outside this repository", "sm")
    return d


# --------------------------------------------------------------------------------- the numbered concerns
# (id, name, group, who provides, what it means, [starting points], [explained on page ids], note)
# starting point: ("fn", file, name) a function or class; ("file", file) a whole file
CONCERNS = [
    ("X1", "Metadata and control plane", "Run", "Ours, stored on Lakebase", "Everything is declared in one manifest and validated.",
     [("fn", P + "meta/loader.py", "load"), ("fn", P + "meta/port.py", "MetadataStore")], ["meta", "meta-loader"], ""),
    ("X2", "Orchestration", "Run", "Both", "What runs when, and in what order. Databricks Jobs start the work. Our day loop, leases and bookmarks keep the order safe.",
     [("fn", "P/scripts/run_platform.py", "main"), ("fn", P + "ingest/daybyday.py", "run_days"), ("fn", P + "leases.py", "busy")],
     ["map-orchestration"], "No schedule or file-arrival trigger is defined in this snapshot."),
    ("X3", "Contracts and schema evolution", "Trust", "Ours", "What a table promises, and what happens when a source changes shape.",
     [("fn", P + "stages/clean.py", "contract_check"), ("fn", P + "contract.py", "check_columns")], ["silver-contract-check", "ingest-steps"], ""),
    ("X4", "Data quality", "Trust", "Both", "Rules on rows and tables; bad rows set aside with a reason.",
     [("fn", P + "stages/clean.py", "quality"), ("fn", P + "silver/contract_rules.py", "contract_rules")], ["silver-quality"],
     "The results table is never written in a normal run."),
    ("X5", "Profiling", "Trust", "Both", "Knowing what the data looks like, so change is visible.",
     [("fn", P + "profiling.py", "profile_rows"), ("fn", P + "profiling.py", "drift")], ["obs-reconcile"], ""),
    ("X6", "Reconciliation", "Trust", "Ours", "Counts and sums agree from source to gold.",
     [("fn", P + "reconcile.py", "compare"), ("file", P + "silver/live_recon.py")], ["obs-reconcile"], ""),
    ("X7", "Lineage", "See", "Databricks, read by us", "Where each table came from and what it feeds.",
     [("file", P + "obs/lineage.py"), ("file", P + "adapters/databricks_system.py")], ["obs-lineage"],
     "Our own lineage events are built only in a test."),
    ("X8", "Observability", "See", "Both", "One run is followable end to end.",
     [("fn", P + "obs/recorder.py", "RunRecorder"), ("file", P + "obs/trace.py"), ("file", P + "adapters/otel_tracer.py")], ["obs-runlog"], ""),
    ("X9", "Freshness and SLOs", "See", "Ours", "Is the data as new as promised.",
     [("fn", P + "obs/freshness.py", "check"), ("file", P + "obs/success.py")], ["obs-freshness"], ""),
    ("X10", "Cost and FinOps", "See", "Not here", "Who spent what, against which budget.",
     [("fn", P + "governance/tables.py", "compute_tags")], [], "The platform only tags its jobs. Cost is read by a separate tool."),
    ("X11", "Security and access", "Guard", "Both", "Who may read or change what. We plan it, Unity Catalog enforces it.",
     [("file", P + "access/model.py"), ("file", "P/realms/databricks/semantic_governance/render.py")], ["semantic"],
     "Generated and tested locally. Applying it to a workspace is not shown in this snapshot."),
    ("X12", "Privacy and personal data", "Guard", "Ours", "Find, mask, keep only as long as allowed.",
     [("fn", P + "stages/clean.py", "personal_data"), ("file", P + "adapters/keyed_tokens.py")], ["silver-personal-data"], ""),
    ("X13", "Governance, catalog and semantics", "Guard", "Both", "Owners, glossary, metric definitions.",
     [("fn", P + "semantic/metrics.py", "metric_view"), ("file", P + "governance/catalog.py")], ["semantic"], ""),
    ("X14", "Audit and run log", "Guard", "Both", "A record of every run and every change.",
     [("file", P + "obs/runlog.py"), ("file", P + "adapters/postgres_runlog.py")], ["obs-runlog", "meta-runtime"], ""),
    ("X15", "Replay, backfill and idempotency", "Run", "Ours", "Re-running never doubles a number.",
     [("fn", P + "recovery.py", "withdraw"), ("fn", P + "housekeeping.py", "run_housekeeping"),
      ("file", P + "silver/baseline_writer.py")], ["obs-recovery", "silver-writer"], ""),
    ("X16", "Environments, CI/CD and release", "Run", "Both", "Development to test to production, infrastructure as code.",
     [("file", "P/realms/databricks/engine/databricks.yml"), ("file", "P/realms/aws-databricks/generate_bundle.py")],
     ["map-orchestration"], "The build workflows are not in the public snapshot."),
    ("X17", "Testing", "Run", "Ours", "Unit, contract, data and simulation tests.",
     [("file", "P/tests/dataplatform/conftest.py"), ("file", P + "twin/runner.py")], ["tests"], ""),
    ("X18", "Resilience and recovery", "Run", "Both", "Retries, retention, time travel, recovery.",
     [("fn", P + "resilience.py", "SourceGuard"), ("fn", P + "retention.py", "apply_landing_retention")], ["obs-resilience", "obs-recovery"], ""),
    ("X19", "Performance", "See", "Both", "File sizes, layout, compaction, right-sized compute.",
     [("fn", P + "timing.py", "hop_delay"), ("file", P + "adapters/table_store.py")], ["obs-page"], ""),
    ("X20", "Self-healing", "Self", "Not built", "Detect, fix in development, deploy, verify, roll back.", [], [],
     "No code for it exists in this repository. Only the release gate's logic is written (twin/gate.py)."),
    ("X21", "Agent assistance", "Self", "Not built", "Agents draft manifests, rules and docs.", [], [],
     "No agent code exists in this repository."),
    ("X22", "Simulation", "Self", "Ours", "A fake world that breaks the platform on purpose.",
     [("fn", P + "world/engine.py", "World"), ("file", P + "twin/runner.py")], ["tests-world", "tests-incidents"], ""),
    ("X23", "Layout: catalogs and schemas", "Run", "Both", "From a table's name you can tell its environment, layer and domain.",
     [("file", P + "adapters/uc_catalog.py"), ("file", "P/tests/dataplatform/test_uc_table_names.py")], [],
     "Added on the site's platform page, not yet in the catalog's list of 22."),
    ("X24", "Streaming", "Run", "Ours, laptop only", "A second speed on the same line, used only where a decision needs it.",
     [("fn", P + "ingest/pipelines.py", "stream_events"), ("file", P + "adapters/local_stream.py"), ("file", P + "adapters/kinesis.py")],
     ["ingest-pipelines"], "Added on the site's platform page. Trigger-once only, never continuous."),
]


def start_link(s):
    if s[0] == "fn":
        return fn(s[1], s[2])
    rel = s[1].split("/", 1)[1]
    short = rel.replace("src/dataplatform/", "")
    if not (lib.ROOTS["P"][0] / rel).exists():
        lib.MISSING.append(s[1])
    return f'<a href="{url(s[1])}"><code>{escape(short)}</code></a>'


def rules_cell(cid):
    rs = COVERAGE["rules"].get(cid, [])
    if not rs:
        return "<small>no rules in the catalog</small>"
    n = {k: sum(1 for r in rs if r["state"] == k) for k in ("tested", "built", "none")}
    return f"{len(rs)} rules<br><small>{n['tested']} tested, {n['built']} built only, {n['none']} none</small>"


def concerns_table():
    groups = ["Run", "Trust", "See", "Guard", "Self"]
    out = ""
    for g in groups:
        rows = []
        for cid, name, grp, who, means, starts, pages, note in CONCERNS:
            if grp != g:
                continue
            starts_html = "<br>".join(start_link(s) for s in starts) or "<small>no code</small>"
            pages_html = " · ".join(link(p) for p in pages) or "<small>no page yet</small>"
            note_html = f"<br><small>{note}</small>" if note else ""
            rows.append([f"<b>{cid}</b>", f"<b>{name}</b><br>{means}{note_html}", who, starts_html, pages_html, rules_cell(cid)])
        out += f"<h3>{g}</h3>" + table(["#", "Concern", "Who provides it", "Where it starts in the code", "Explained on", "Rules (DAC)"], rows)
    return out


def entry_table():
    rows = [
        ("The whole day loop", "P/scripts/run_platform.py", "main",
         "A person, or the Databricks job <code>dp-&lt;env&gt;-engine-days</code> through <code>tasks/run_days.py</code>",
         "Plays the world, moves files, then runs ingestion, silver, gold and the checks for each day", "map-orchestration"),
        ("One hop as its own process", "P/scripts/run_hop.py", "main",
         "The day loop with <code>--hops process</code>, or a deployed job task",
         "Runs one of ingestion, silver or gold for one business date and saves its report", "map-orchestration"),
        ("Upstream: the simulated company", P + "world/engine.py", "World",
         "The day loop", "Writes each day into the mock sources and records the true totals", "tests-world"),
        ("Push to landing", P + "adapters/local_movers.py", "ChangeFeedMover",
         "The day loop", "Takes change-capture output and file drops to the landing area", "ingest-sources"),
        ("Ingestion: landing or source to bronze", P + "ingest/runner.py", "ingest", "", "Picks one of five pipelines per dataset", "ingest-runner"),
        ("Bronze to silver", P + "silver/runner.py", "build_silver", "", "Reader, plan, thirteen stages, one write", "silver"),
        ("Silver to gold", P + "gold/harness.py", "run_gold", "", "Dimensions first, then facts, window by window", "gold"),
        ("Semantic", P + "semantic/answers.py", "answers", "", "Asks each metric its known questions", "semantic"),
        ("Operations page", "P/scripts/run_platform.py", "write_ops_page", "", "Writes the page and the ops tables from the run log", "obs-page"),
        ("Load the metadata store", P + "meta/loader.py", "load", "A person or a deployment step", "Validates manifests, then writes them", "meta-loader"),
    ]
    out = []
    for part, file, name, by, what, page_id in rows:
        links, more = caller_links(file, name, 4)
        called = links or by or "started from outside the code"
        if links and more:
            called += f"<br><small>and {more} more</small>"
        out.append([f"<b>{part}</b>", fn(file, name), called, what, link(page_id)])
    return table(["Part", "Starting point", "Called by (found by reading the code)", "What it does", "Explained on"], out)


@page("map", "Code map: the whole platform on one page, and the way into every line of code", parent="index",
      short="Code map",
      blurb="Upstream, platform and downstream in one picture. Every part, every concern and every layer's starting point, linked to the code.")
def code_map():
    out = ("<p class='lead'>This is the page to start from when you want to find code. The picture shows the whole "
           "system. Every box opens the page that explains it, and below the picture every starting point and every "
           "concern is linked to a line of code. From any function you can see who calls it.</p>")
    out += fig(1130, 960, picture(), "The whole system: upstream, the data platform and downstream",
               "Read left to right. Data comes in by two ways, pushed to landing or pulled by the platform, and both end in bronze. "
               "The metadata store drives every step. Blue boxes are built here, yellow boxes are provided by Databricks.")
    out += "<h2>How to find your way</h2>"
    out += ladder([
        ("Know the part", "Click its box above, or find its row in one of the two tables below."),
        ("Go to the starting function", "Each row names the function where that part begins, with its file and line. The link opens that line on GitHub."),
        ("See who starts it", f"The same row lists the callers. {link('map-callers')} lists callers for every function and method in the code."),
        ("Find any file", f"{link('map-index')} lists every module with what it is for and which page explains it."),
    ])
    out += "<h2>Where each part starts, and who starts it</h2>" + entry_table()
    out += ("<p>The callers are found by reading the code, not by running it. A call written as <code>object.method()</code> "
            "is matched by the method's name, so a method of the same name in another class can appear. Those are marked "
            "<i>by name</i> on the caller pages.</p>")
    out += "<h2>How a day is orchestrated, in one paragraph</h2>"
    out += ("<p>One script, <code>run_platform.py</code>, owns the day loop. For each day it asks the simulated company to act, "
            "moves the day's deliveries to landing, runs ingestion, then silver, then gold, and compares each layer with the "
            "true answers. The same script is what the deployed Databricks job runs, as a single task. Underneath, three "
            "mechanisms keep the order safe: a lease per dataset so two runs never write one table, bookmarks that move only "
            f"when a dataset run succeeds, and the run log. The detail, with the job definitions, is on {link('map-orchestration')}.</p>")
    out += "<h2>Every concern, numbered, and who provides it</h2>"
    out += ("<p>The platform's cross-cutting concerns are numbered X1 to X22 in the catalog. X23 and X24 were added on the site's "
            "platform page and are not in the catalog's list yet. <b>Who provides it</b> says whether we built the code, "
            "Databricks provides the capability and we read or configure it, or both. It is read from the code and from the "
            f"adopt-first table in the observability notes, not from a claim. The rule counts are computed on {COVERAGE['date']} "
            "from the catalog's obligations and the tests of this repository: a rule is <i>tested</i> when a test names it, "
            "<i>built only</i> when only code names it, and <i>none</i> when nothing does.</p>")
    out += concerns_table()
    out += not_built([
        "X20 self-healing and X21 agent assistance have no code in this repository.",
        "X10 cost is not in the platform. Jobs are tagged so a separate tool can attribute spend.",
        "The three-task platform job that the day loop's <code>--hops job</code> option expects is not defined in this snapshot.",
        "The picture is hand-drawn from the code. The tables are checked against it at build time: a function that is renamed or removed breaks the build.",
    ])
    return out


# --------------------------------------------------------------------------------------- orchestration
@page("map-orchestration", "Orchestration: how a day runs, from the job to the last check", parent="map",
      short="Orchestration, end to end",
      blurb="Three ways the same day loop runs, the job definitions, and what keeps the order safe.")
def orchestration():
    out = ("<p class='lead'>A platform needs something that decides what runs when. Here that is two things working "
           "together. Databricks Jobs start the work and retry it. Our own code decides what each run does and makes "
           "sure runs never collide or skip data.</p>")
    d = box(10, 20, 200, 80, "Databricks job", ["dp-env-engine-days", "one task"], "bxs")
    d += box(260, 20, 200, 80, "run_days.py", ["sets the options", "calls the script"], "bx")
    d += box(510, 20, 200, 80, "run_platform.py", ["the day loop", "main()"], "solid")
    d += box(760, 20, 200, 80, "run_days()", ["world, move, ingest", "then the day's judges"], "bx", href="ingest-runner.html")
    d += arrow(212, 60, 258, 60) + arrow(462, 60, 508, 60) + arrow(712, 60, 758, 60)
    d += box(510, 150, 200, 70, "silver_day()", ["build_silver"], "bx", href="silver.html")
    d += box(760, 150, 200, 70, "gold_day()", ["run_gold"], "bx", href="gold.html")
    d += box(260, 150, 200, 70, "Operations page", ["ops tables, checks"], "bx", href="obs-page.html")
    d += path("M860,102 L860,126 L610,126 L610,148", "ln") + arrow(712, 185, 758, 185)
    d += arrow(508, 185, 462, 185)
    out += fig(980, 250, d, "The deployed day loop",
               "One Databricks task runs the whole loop. Inside, each day runs ingestion, then silver, then gold, then the checks.")
    out += "<h2>Three ways the same day runs</h2>"
    out += table(["Way", "What starts it", "Where each hop runs", "Used for"], [
        ["Inline", "<code>python scripts/run_platform.py</code>", "All in one process", "The 3-day loop on a laptop, and every test"],
        ["One process per hop", "<code>run_platform.py --hops process</code>", f"Each hop is its own process: {fn('P/scripts/run_hop.py', 'main')}", "Proving that each hop works as a deployed job task would, alone"],
        ["Deployed job", "The Databricks job <code>dp-&lt;env&gt;-engine-days</code>", "One task runs the inline loop on the job's own Spark", "The 3-day and 14-day proofs on Databricks"],
    ])
    out += "<h2>The way into each layer</h2>"
    out += table(["Layer", "The function that starts it", "Started inline by", "Started alone by"], [
        ["Landing to bronze (ingestion)", fn(P + "ingest/runner.py", "ingest"), "the day loop, once per day", "<code>run_hop.py --hop ingest</code>"],
        ["Bronze to silver", fn(P + "silver/runner.py", "build_silver"), "the day loop's silver hook", "<code>run_hop.py --hop silver</code>"],
        ["Silver to gold", fn(P + "gold/harness.py", "run_gold"), "the day loop's gold hook", "<code>run_hop.py --hop gold</code>"],
        ["Gold to semantic", fn(P + "semantic/answers.py", "answers"), "the checks at the end of each day", "no entry of its own yet"],
    ])
    out += (f"<p>The same list, with who calls each one found by reading the code, is on {link('map')}. Every layer reads and "
            "writes its tables through the table port, so the table format (Unity Catalog managed Iceberg, managed Delta with "
            "UniForm, or AWS S3 Tables) is a job parameter and not a change to any layer's code.</p>")
    out += "<h2>The job definition</h2>"
    out += code("P/realms/databricks/engine/databricks.yml", r"^    engine_days:", r"\"\$\{var.meta_instance\}\", --run-id", "The deployed job: a few parameters and one task.", cap=40)
    out += ("<p>There is one task. The job's parameters are the slice (which company's world), the number of days, the world's "
            "size (<code>scale</code>), the name of the world calendar, how long bronze is kept before housekeeping, and the "
            "table format of every layer. The task passes them to <code>run_days.py</code>, which sets its environment and runs "
            "<code>run_platform.py</code> inside the job. When the loop ends it copies the operations page and the report to a "
            "volume and publishes the operations tables.</p>")
    out += code("P/realms/databricks/engine/tasks/run_days.py", r"^sys.argv = \[", r"^code = 0", "How the task turns the job's options into the day loop's options.", cap=12)
    out += "<h2>The loop itself</h2>"
    out += code(P + "ingest/daybyday.py", r"^def run_days\(", r"^    world.finish\(\)", "The loop: world acts, deliveries move, ingestion runs, the day is judged.", cap=50)
    out += ladder([
        ("world.step", "The simulated company acts for the day and returns the true totals (the oracle)."),
        ("move", "The day's change-capture output and file drops are moved to landing."),
        ("ingest", f"One ingestion run takes everything new to bronze: {fn(P + 'ingest/runner.py', 'ingest')}."),
        ("judge", "Bronze row counts are compared with the truth, and each planted incident is judged."),
        ("on_day", "The caller's hook runs silver, gold, housekeeping and upkeep for the same day, then the next day starts."),
    ])
    out += "<h2>What keeps the order safe</h2>"
    out += table(["Mechanism", "What it prevents", "Where"], [
        ["A lease per dataset", "Two runs writing one table at the same time. The second finds it busy and writes nothing.", fn(P + "leases.py", "busy")],
        ["Bookmarks that move only on success", "Skipping data after a failure, or reading it twice. A failed run is taken again whole.", f"{link('silver-reader')}, {link('meta-runtime')}"],
        ["The file log", "Losing track of what was delivered, held, voided or loaded.", link("meta-runtime")],
        ["The run log", "Not knowing what a run did. Every step of every run is a row.", link("obs-runlog")],
    ])
    out += "<h2>The first slice's job</h2>"
    out += code("P/realms/databricks/databricks.yml", r"^      name: dp-\$\{var.env\}-slice1-day", r"^            python_file: ./tasks/main.py$", "The earlier four-task job for one slice: prepare, land, silver, finish, each waiting for the one before.", cap=30)
    out += not_built([
        "No schedule or file-arrival trigger is defined. The jobs are started by a person or a command.",
        "The platform job with three tasks (ingestion, silver, gold) that <code>--hops job</code> expects is not defined in this snapshot, so that way has not been shown here.",
        "Retries are Databricks' job setting plus our source guard for pulls. There is no retry of a failed hop by our code.",
        "A rule that jobs wait for their data and not for the clock (ORCH-01) is written in code comments but has no test.",
        "Datasets are not grouped. One daily job loads every dataset of a domain. Many datasets per source would want a "
        "data group, with one job per group and dependencies between groups. That is planned, not built.",
        "Whether a job runs on serverless or on a cluster is set in the bundle file, not in the manifest. A per-group "
        "setting is planned.",
    ])
    out += ("<p>The benchmark has its own jobs and a deploy script (the <code>benchmarking</code> folder): one that probes table "
            "maintenance in each format, and the same <code>engine_days</code> job run per format with a larger "
            "<code>scale</code>.</p>")
    return out


# --------------------------------------------------------------------------------------- the index of every module
PAGE_OF = [  # (path fragment, page id): the first match wins
    ("src/dataplatform/meta/", "meta"), ("adapters/sql/", "meta-functions"), ("memory_meta", "meta"), ("postgres_meta", "meta"),
    ("lakebase", "meta"), ("src/dataplatform/ingest/", "ingest"), ("lander.py", "ingest-lander"), ("keys.py", "ingest-lander"),
    ("source_ports", "ingest-sources"), ("local_movers", "ingest-sources"), ("sqlite_source", "ingest-sources"),
    ("postgres_source", "ingest-sources"), ("http_api", "ingest-sources"), ("file_drop", "ingest-sources"),
    ("local_stream", "ingest-sources"), ("kinesis", "ingest-sources"), ("s3.py", "ingest-sources"), ("delta_bronze", "ingest-lander"),
    ("src/dataplatform/silver/", "silver"), ("src/dataplatform/stages/", "silver-stages"), ("spark_silver", "silver-writer"),
    ("duckdb_silver", "silver-writer"), ("src/dataplatform/gold/", "gold"), ("spark_gold", "gold-harness"), ("duckdb_gold", "gold-harness"),
    ("src/dataplatform/semantic/", "semantic"), ("src/dataplatform/governance/", "semantic"), ("src/dataplatform/access/", "semantic"),
    ("src/dataplatform/obs/", "obs"), ("src/dataplatform/des/", "obs-reconcile"), ("reconcile.py", "obs-reconcile"),
    ("profiling.py", "obs-reconcile"), ("recovery.py", "obs-recovery"), ("retention.py", "obs-recovery"),
    ("housekeeping.py", "obs-recovery"), ("resilience.py", "obs-resilience"), ("src/dataplatform/world/", "tests-world"),
    ("src/dataplatform/twin/", "tests"), ("scripts/run_platform.py", "map-orchestration"), ("scripts/run_hop.py", "map-orchestration"),
    ("realms/databricks/engine", "map-orchestration"), ("realms/databricks/semantic_governance", "semantic"),
    ("realms/databricks/dashboards", "obs-page"), ("realms/databricks/declarative", "silver-declarative"),
]


def group_of(file):
    if file.startswith("src/dataplatform/"):
        parts = file.split("/")
        return parts[2] if len(parts) > 3 else "core"
    return file.split("/")[0] if file.startswith("scripts") else "realms"


G = lib.graph()
GROUP_LABEL = {"core": "Top-level modules", "scripts": "Scripts", "realms": "Deployable bundles (realms)"}


def explained_on(file):
    for frag, pid in PAGE_OF:
        if frag in file:
            return link(pid)
    return "<small>none yet</small>"


@page("map-index", "Code index: every module, what it is for, and where it is explained", parent="map",
      short="Code index", blurb="All modules of the platform, grouped by package.")
def code_index():
    out = ("<p class='lead'>Every Python module of the platform, grouped by package. Each row gives the module's own one-line "
           "description, its size, a link to the file, and the page that explains it. The list is generated from the code when "
           "the site is built, so it cannot drift.</p>")
    groups = {}
    for f, m in sorted(G.modules.items()):
        groups.setdefault(group_of(f), []).append((f, m))
    total = sum(len(v) for v in groups.values())
    out += f"<p>{total} modules in {len(groups)} groups.</p>"
    out += table(["Group", "Modules", "Lines", "Caller pages"],
                 [[GROUP_LABEL.get(g, g), str(len(v)), f"{sum(m['lines'] for _, m in v):,}", link("map-callers-" + slug(g))]
                  for g, v in sorted(groups.items())])
    for g, mods in sorted(groups.items()):
        out += f"<h2 id='{slug(g)}'>{GROUP_LABEL.get(g, g)}</h2>"
        rows = []
        for f, m in mods:
            rows.append([f'<a href="{url("P/" + f)}"><code>{escape(f.replace("src/dataplatform/", ""))}</code></a>',
                         escape(m["doc"]) or "<small>no description</small>", str(m["lines"]), explained_on(f)])
        out += table(["Module", "What it is for (its own first sentence)", "Lines", "Explained on"], rows)
    return out


# --------------------------------------------------------------------------------------- who calls what
def callers_cell(d, limit=6):
    order = {"exact": 0, "value": 1, "by name": 2}
    uses = sorted(G.callers.get(d.id, []), key=lambda u: (order.get(u.how, 3), u.file, u.line))
    out = []
    for u in uses[:limit]:
        who = G.defs.get(u.by)
        label = (who.qual if who else "module level") + f" · {u.file.replace('src/dataplatform/', '')}:{u.line}"
        mark = "" if u.how == "exact" else f" <small>({u.how})</small>"
        a = f'<a href="#{slug(u.by)}">' if who and group_of(who.file) == group_of(d.file) else f'<a href="{url("P/" + u.file, u.line)}">'
        out.append(f"{a}<code>{escape(label)}</code></a>{mark}")
    if len(uses) > limit:
        out.append(f"<small>and {len(uses) - limit} more</small>")
    return "<br>".join(out) or "<small>none found in the code</small>"


def callers_page(g):
    def render():
        defs = [d for d in G.defs.values() if group_of(d.file) == g]
        by_file = {}
        for d in sorted(defs, key=lambda x: (x.file, x.line)):
            by_file.setdefault(d.file, []).append(d)
        out = (f"<p class='lead'>Every class, function and method in {GROUP_LABEL.get(g, 'the ' + g + ' package')}, with who "
               "calls it. Found by reading the code. A call written as <code>object.method()</code> is matched by the "
               "method's name, so a same-named method elsewhere can appear: those rows say <i>by name</i>.</p>")
        out += f"<p>{len(defs)} definitions in {len(by_file)} modules. Jump to a module: " + " · ".join(
            f'<a href="#{slug(f)}">{escape(f.split("/")[-1])}</a>' for f in by_file) + "</p>"
        for f, ds in by_file.items():
            m = G.modules[f]
            out += (f"<h2 id='{slug(f)}'>{escape(f.replace('src/dataplatform/', ''))}</h2>"
                    f"<p>{escape(m['doc'])} {explained_on(f)}</p>")
            rows = []
            for d in ds:
                tests = sorted(G.tested_by.get(d.id, ()))
                tcell = ", ".join(f'<a href="{url("P/" + t)}"><code>{escape(t.split("/")[-1])}</code></a>' for t in tests[:2])
                if len(tests) > 2:
                    tcell += f" <small>and {len(tests) - 2} more</small>"
                rows.append([f'<span id="{slug(d.id)}"></span><a href="{url("P/" + d.file, d.line)}"><code>{escape(d.qual)}</code></a>'
                             f"<br><small>{d.kind}, line {d.line}</small>", escape(d.doc) or "", callers_cell(d), tcell or "<small>no test names it</small>"])
            out += table(["Definition", "What it does (its own first sentence)", "Called by", "Named in tests"], rows)
        return out
    return render


@page("map-callers", "Who calls what: every function and method", parent="map", short="Who calls what",
      blurb="For every definition, who calls it. One page per group of modules.")
def callers_hub():
    groups = {group_of(f): [] for f in G.modules}
    for d in G.defs.values():
        groups[group_of(d.file)].append(d)
    uses = sum(len(v) for v in G.callers.values())
    out = ("<p class='lead'>When you find a function and wonder who starts it, look it up here. Each page below covers one "
           "group of modules and lists every class, function and method in it, with the places that use it and the tests that "
           "name it.</p>")
    out += (f"<p>{len(G.defs):,} definitions and {uses:,} uses were found by reading {len(G.modules)} modules. "
            "The callers are not run-time facts: a function passed through a variable, a callback registered at run time, or "
            "a call made through a dictionary of names may be missing. A function the day loop starts through "
            "<code>runpy</code> or a job file shows as started from outside the code.</p>")
    out += table(["Group", "Definitions", "Page"], [[GROUP_LABEL.get(g, g), str(len(v)), link("map-callers-" + slug(g))]
                                                      for g, v in sorted(groups.items())])
    return out


for _g in sorted({group_of(f) for f in G.modules}):
    page("map-callers-" + slug(_g), f"Who calls what: {GROUP_LABEL.get(_g, _g)}", parent="map-callers",
         short=GROUP_LABEL.get(_g, _g), blurb=f"Every definition in {GROUP_LABEL.get(_g, _g)} and its callers.")(callers_page(_g))
