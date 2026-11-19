"""Tests and proof: table-driven tests, the simulated company with its oracle, planted incidents, what each layer has."""
from lib import *  # noqa: F401,F403

W = "P/src/dataplatform/world/"
ENGINE = W + "engine.py"
DWD = W + "dwd.py"
CLOCK = W + "clock.py"
TYRE = W + "domains/tyre.py"
ORACLE = W + "domains/tyre_oracle.py"
JUDGE = W + "domains/tyre_gold.py"
WORLD = "C/slices/02-tyre/world.yaml"
ING_DAY = "P/src/dataplatform/ingest/daybyday.py"
SIL_DAY = "P/src/dataplatform/silver/daybyday.py"
GOLD_DAY = "P/src/dataplatform/gold/daybyday.py"
HIST = "P/src/dataplatform/silver/history_contract.py"
T = "P/tests/dataplatform/"
W1C = "P/docs/W1C-SILVER.md"
W1D = "P/docs/W1D-GOLD.md"
DUCK = "P/docs/DUCKDB-SPIKE.md"


def n_tests(rel):
    """Test functions in a file of the platform repository, counted when the page is built."""
    p = ROOTS["P"][0] / rel
    if not p.exists():
        MISSING.append("P/" + rel)
        return 0
    return len(re.findall(r"(?m)^\s*def test_", p.read_text()))


def tf(name, folder="tests/dataplatform/"):
    """A test file as a link, with its number of test functions."""
    return f"{src('P/' + folder + name, name)} ({n_tests(folder + name)})"


def files(*names):
    return "<br>".join(tf(n) for n in names)


def obs_test_files():
    folder = ROOTS["P"][0] / "tests/dataplatform"
    names = sorted(p.name for p in folder.glob("test_obs_*.py")) + ["test_ops_export.py", "test_ops_restatements.py",
                                                                   "test_hop_reports.py", "test_twin_scenarios.py"]
    return names, sum(n_tests("tests/dataplatform/" + n) for n in names)


# ------------------------------------------------------------------------------------------------------ the hub
def hub_picture():
    d = box(10, 50, 290, 110, "1 Tables of cases", ["one function, many rows:", "input and expected output"], "bx",
            href="tests-tables.html")
    d += box(350, 50, 290, 110, "2 A company with known truth", ["generated data, and the true", "answer for every day"],
             "solid", href="tests-oracle.html")
    d += box(690, 50, 290, 110, "3 Faults planted on purpose", ["eleven incidents on known", "days, each must be caught"],
             "bx", href="tests-incidents.html")
    d += text(20, 24, "proves each small part", "tx")
    d += text(360, 24, "proves each layer, every day", "tx")
    d += text(700, 24, "proves trouble is noticed", "tx")
    d += box(350, 240, 290, 70, "The simulated tyre company", "feeds kinds 2 and 3", "bxs", href="tests-world.html")
    d += arrow(495, 225, 495, 164) + arrow(640, 260, 800, 164)
    return d


@page("tests", "Tests and proof: how we know every layer is right", parent="index", short="Tests and proof",
      blurb="Three kinds of proof: tables of cases, a simulated company whose true answers are known, and faults planted on purpose.")
def tests():
    out = ("<p class='lead'>A data platform can run without an error and still be wrong. A row can be counted "
           "twice. A delete can be missed. So the platform is proven in three different ways, and each catches "
           "what the others cannot.</p>")
    out += fig(990, 330, hub_picture(), "The three kinds of proof and the simulated company behind two of them",
               "Read the three boxes left to right, from the smallest thing proven to the largest. Click a box to zoom in.")
    out += "<h2>The three kinds of proof</h2>"
    out += table(["Kind", "What it is", "What it proves", "What it cannot prove"], [
        [f"1. {link('tests-tables', 'Table-driven unit and contract tests')}",
         "One test function and a table of cases. Each case is a row: a name, an input and the expected output. "
         "A contract table is the same idea for a port: every adapter of the port must pass the same rows.",
         "That one function, or one adapter, does what each row says.",
         "That the parts add up to a correct table at the end."],
        [f"2. {link('tests-oracle', 'The simulated company with an oracle')}",
         "A generated tyre company runs for simulated days and writes into mock source systems. The generator "
         "also records the true answer for every day. That record is the oracle, and it is the golden data.",
         "That each layer's output equals the truth on every simulated day.",
         "Anything the simulated company does not do."],
        [f"3. {link('tests-incidents', 'Planted incidents')}",
         "Eleven faults put into the simulated company's data on purpose, on known days: a late file, a file "
         "sent twice, a renamed column, deleted rows and more.",
         "That the platform catches or handles each fault at the right layer, and raises nothing on a clean day.",
         "Faults nobody thought to plant."],
    ])
    out += ("<p>An <b>oracle</b> in testing is a source of true answers that does not depend on the thing being "
            "tested. Here it is computed from the simulated company's own record of what it did, never from the "
            "platform's logs. <b>Golden data</b> means data whose correct result is known in advance.</p>")
    out += "<h2>The direct answer: is every layer tested against golden data?</h2>"
    out += ("<p>Not every layer, and not all in the same way. The table is the short version. "
            f"{link('tests-by-layer')} has the test files, the counts and what is missing.</p>")
    out += table(["Layer", "Compared with the oracle?", "How"], [
        ["Metadata store", "No", "It holds settings and logs, not business data. It is proven by a contract "
                                 "table run against two adapters."],
        ["Ingestion and bronze", "Yes, by row count", "Rows appended to bronze per dataset per day equal the "
                                                      "oracle's count, in a slow test and in the recorded runs."],
        ["Bronze to silver", "Yes", "Current rows, versions, deletes marked, each dataset's reconciliation "
                                    "measures and readings per hour, in a slow test that needs Spark and in the "
                                    "recorded runs."],
        ["Silver to gold", "Yes in the recorded runs, partly in tests",
         "The run script compares gold with the oracle every day. One slow test runs gold on the simulated "
         "company and checks the days its scenarios name. There is no test that asserts gold equals the oracle "
         "on all fourteen days."],
        ["Semantic", "Yes, five known answers", "Five certified metrics each carry one known answer. The oracle "
                                                "reproduces all five. Asking the same five of built gold is a "
                                                "separate command, not part of the default test run."],
        ["Operations", "No", "Alerts and screens are checked against the incident calendar on a stand-in run "
                             "log: one alert per incident, none on a clean day."],
    ])
    out += note("One thing to keep in mind",
                "<p>The golden-data tests for silver and gold need Spark, and the one for bronze needs the Delta "
                "package. A test run without them skips those tests. A check of the public snapshot on 19 "
                "November 2026 ran the two main test folders without Spark: 1316 passed and 219 skipped, and the "
                "skipped ones need Spark. So a green run without Spark proves the first kind of proof and not "
                "the second.</p>")
    out += "<h2>The pages under this one</h2>"
    out += table(["Page", "What it explains"], [
        [link("tests-world"), "The simulated tyre company: its sources, its clock, its volumes and how it is run."],
        [link("tests-oracle"), "What truth the oracle records for each day and layer, and how each layer is compared with it."],
        [link("tests-incidents"), "Every planted incident: its day, the rule that detects it, the layer that handles it, and the recorded result."],
        [link("tests-tables"), "How a case table looks, the contract tables every adapter must pass, the history contract, and the same tests on two engines."],
        [link("tests-by-layer"), "One table with a row per layer, and the check of how many catalog obligations a test cites."],
    ])
    out += "<h2>The design decisions behind the proof</h2>"
    out += table(["Decision", "Why"], [
        ["The truth never comes from the platform", "A platform checked against its own logs agrees with itself "
                                                    "even when it is wrong."],
        ["The simulated company is deterministic", "The same seed gives the same data on any machine, so a "
                                                   "failure can be repeated."],
        ["Incidents live in a file, not in code", "Anyone can move an incident to another day or change its "
                                                  "size by editing one line."],
        ["A check on a layer that does not exist is pending, never passing", "Nothing counts as proven by "
                                                                             "default."],
        ["Tests are tables", "A new case is a new row. The reader sees every case at a glance."],
    ])
    out += not_built([
        "No test asserts that gold equals the oracle on every one of the fourteen days. The run script does "
        "that comparison, and its result is recorded in a design note.",
        "The fourteen-day run at full volume through gold has not been completed. The recorded attempt ran out "
        "of memory on day 12 on a laptop.",
        "The golden-data tests are slow and need Spark, so a run without Spark does not include them.",
        "The declarative silver engine has not been run against the oracle in any test in this repository.",
        "Operations has no oracle of its own. Its checks use a stand-in run log, not the one the engine writes.",
    ])
    return out


# ------------------------------------------------------------------------------------------------------ the world
def world_picture():
    d = box(10, 20, 200, 60, "World file", "sources, volumes, incidents", "bx")
    d += box(10, 110, 200, 60, "Manifests, contracts", "columns, keys, how read", "bx")
    d += box(290, 60, 190, 80, "Domain model", ["the only code that", "knows tyres"], "solid")
    d += box(560, 10, 200, 50, "2 databases", "with change feeds", "bxm")
    d += box(560, 70, 200, 50, "2 file drops", "lab and dealer files", "bxm")
    d += box(560, 130, 200, 50, "1 interface", "dealer records", "bxm")
    d += box(560, 190, 200, 50, "1 stream", "press readings", "bxm")
    d += box(290, 190, 190, 60, "Oracle file", "one per day", "bxs", href="tests-oracle.html")
    d += box(840, 90, 140, 80, "The platform", "ingests them", "bx", href="ingest.html")
    d += arrow(210, 50, 288, 90) + arrow(210, 140, 288, 110)
    d += arrow(480, 85, 558, 35) + arrow(480, 95, 558, 95) + arrow(480, 105, 558, 155) + arrow(480, 115, 558, 215)
    d += arrow(385, 140, 385, 188)
    d += arrow(760, 35, 838, 105) + arrow(760, 95, 838, 120) + arrow(760, 155, 838, 140) + arrow(760, 215, 838, 155)
    return d


@page("tests-world", "The simulated tyre company", parent="tests", short="The simulated company",
      blurb="A generated company that runs day by day, writes into six mock source systems, and knows its own truth.")
def tests_world():
    out = ("<p class='lead'>The simulated company is a program that pretends to be a tyre maker. It runs for "
           "simulated days. Each day it takes orders, cures tyres, inspects them, receives dealer files and "
           "emits sensor readings, and writes all of it into mock source systems.</p>"
           "<p>It exists because real data cannot be used to prove a platform. With real data nobody knows the "
           "right answer. With generated data the generator knows exactly what it produced, so every number "
           "the platform shows can be checked.</p>")
    out += fig(990, 270, world_picture(), "From the world file to six mock sources and one oracle file per day",
               "The domain model in the middle is the only code that knows the business. The four dashed boxes are the sources the platform reads.")
    out += "<h2>The world file</h2>"
    out += (f"<p>The company is described in one file, {src(WORLD)}. The file says what the sources are, how "
            "much they produce, when the clock starts, and which incidents happen on which day. It does not "
            "declare columns or keys. Those come from the dataset manifests and contracts that it cites, so "
            "the simulated company and the platform read the same specification.</p>")
    out += code(WORLD, r"^clock:", r"    P-HUN:", "The clock: fourteen days from 9 November 2026, in the business time zone, with two plants.")
    out += code(WORLD, r"^population:", r"  scrap_share:", "The volumes and shares the generator aims for.")
    out += "<h2>The six source systems</h2>"
    out += ("<p>The table lists the six systems in the world file. The second column is the kind of source, "
            "the third the datasets it holds, and the fourth how it delivers.</p>")
    out += table(["System", "Kind", "Datasets", "How it delivers"], [
        ["<code>erp_db</code>", "Database with a change feed", "Sales order lines, deliveries, materials",
         "Order lines and deliveries every 15 minutes through the change feed. Materials as a full copy daily at 01:00."],
        ["<code>mes_db</code>", "Database with a change feed", "Production batches, presses",
         "Production every 15 minutes. Presses as a full copy daily at 01:00."],
        ["<code>lims_drop</code>", "File drop", "Inspection results", "One CSV file per plant per hour."],
        ["<code>partner_drop</code>", "File drop", "Dealer sell-out", "One CSV file per dealer per month, in the first 5 days of the month."],
        ["<code>dealer_api</code>", "Request and response interface", "Dealers", "Pages of 200 records since a cursor, read every 6 hours."],
        ["<code>press_stream</code>", "Event stream", "Press readings", "One reading per press every 10 seconds."],
    ])
    out += ("<p>A <b>change feed</b> is a list of every insert, update and delete in the order the database "
            "committed them. A <b>full copy</b> is the whole table sent again. Between them the six systems "
            "cover every way the platform can read a source.</p>")
    out += "<h2>How one simulated day runs</h2>"
    out += ladder([
        ("The domain model produces the day", "It works out everything the sources did that day and returns "
                                              "it as one object."),
        ("The engine writes it through ports", "Each output goes to the port that matches its system's kind. "
                                               "A write to a system the world file does not declare is refused."),
        ("The oracle is saved", "The model computes what a correct platform should show at the end of the day, "
                                "and the engine writes it as one file for that day."),
        ("The platform runs", "The caller runs ingestion, then silver, then gold, and compares each with the "
                              "oracle file."),
    ])
    out += code(ENGINE, r"    def step\(self, day: int\)", caption="One world day: write the sources, then save the oracle.")
    out += code(TYRE, r"    def day\(self, d: int\) -> Emission:", caption="What the tyre model does in one day, system by system.")
    out += ("<p>The engine knows nothing about tyres. It only checks that the model writes to declared systems "
            "through the right port, and that the model can plant every kind of incident on the calendar. If "
            "the calendar names a kind the model has no handler for, the engine refuses to start and names it.</p>")
    out += code(ENGINE, r"^class World:", r"            raise WorldError\(f\"the domain model cannot plant", "The refusal at start-up.")
    out += "<h2>A small worked example</h2>"
    out += ("<p>One order line through three days of the simulated company. The values are an example typed "
            "for this page, in the shape the generator writes to the order line's change feed.</p>")
    out += raw("""day  op  order_id   line_no  ordered_qty  promised_date  line_status  change_seq
 1   I   SO-000017     1         40      2026-11-14     open            17
 2   U   SO-000017     1         40      2026-11-15     open           6204
 3   U   SO-000017     1         40      2026-11-15     cancelled     11873""",
               "An insert, a promised-date change, and a cancellation. The change sequence only ever grows.")
    out += ("<p>The model keeps the truth beside this: the day the line was placed, the date first promised, "
            "the day it was cancelled. The oracle is built from that record, not from the feed.</p>")
    out += "<h2>Scale and repeatability</h2>"
    out += table(["Property", "How", "Where"], [
        ["Deterministic", "The seed in the world file is 11. Each concern has its own random stream, so "
                          "changing how readings are made never changes an order.", src(TYRE)],
        ["Scalable", "A scale below 1.0 shrinks the number of orders and the frequency of readings. Dealers "
                     "and presses do not shrink.", "The <code>--scale</code> option"],
        ["Movable incidents", "A named calendar moves every incident to other days. The compressed calendar "
                              "puts all eleven on days 2 and 3 of a 3-day run.", src(WORLD)],
    ])
    out += code(DWD, r"^def apply_calendar\(", r"    return \{\*\*changed", "Applying a named calendar. Every incident must be moved, and the order of days must hold.", cap=32)
    out += ("<p>The three rules at the bottom are the order of days. A day the incident reaches back to must be "
            "before the incident. A later effect must be after it. A last day must not be before it. A calendar "
            "that breaks one of these is refused, never quietly adjusted.</p>")
    out += "<h2>The settings that drive it</h2>"
    out += table(["Setting in the world file", "Meaning", "Value for the tyre company"], [
        ["<code>seed</code>", "The seed of the random generator.", "11"],
        ["<code>clock.start</code>, <code>clock.days</code>", "The first instant and the number of days.", "9 November 2026, 14 days"],
        ["<code>clock.business_timezone</code>", "The zone a business day is counted in.", "Asia/Kolkata"],
        ["<code>population.dealers</code>, <code>population.materials</code>", "Master data sizes.", "600 and 1800"],
        ["<code>population.orders_per_day</code>", "The range of orders placed in a day.", "1500 to 3000"],
        ["<code>population.on_time_share</code>, <code>in_full_share</code>, <code>cancel_share</code>", "Shares the generator aims for.", "0.93, 0.96, 0.03"],
        ["<code>calendar</code>", "The planted incidents with their days and exact values.", "Eleven entries"],
        ["<code>calendars</code>", "Named variants of the calendar.", "One: <code>compressed</code>"],
    ])
    out += "<h2>How it is run</h2>"
    out += table(["Script", "What it does"], [
        [src("P/scripts/run_world.py"), "Runs the company alone for a number of days and writes the sources "
                                        "and the oracle files. It is the binding for the local stack: the "
                                        "only place that picks adapters for the sources."],
        [src("P/scripts/run_platform.py"), "Runs the company and the platform together, day by day. After each "
                                           "world day it runs ingestion, silver and gold, compares each with "
                                           "the oracle, judges the day's incidents, and at the end writes a "
                                           "report and the operations page."],
    ])
    out += code(ING_DAY, r"^def run_days\(", caption="The day loop: world step, movers, ingestion, then bronze against the oracle.", cap=46)
    out += "<h2>How the simulated company itself is tested</h2>"
    out += (f"<p>The golden data must be right before anything is judged against it. {tf('test_world_tyre.py')} "
            "runs fourteen days at scale 0.05 once and checks many things on that one run. Each incident is "
            "found in the sources on its day. The oracle's silver numbers equal what the mock sources actually "
            "hold. Bronze never exceeds what the sources sent. The same seed gives the same world. The clean "
            f"days are days 1, 2 and 14. {tf('test_world_calendar.py')} covers the named calendars.</p>")
    out += code(T + "test_world_tyre.py", r"^INCIDENTS = \[", r"^\]", "One row per incident: its id, its day, its detector and the function that checks the sources.")
    out += not_built([
        "Volume is not realistic. The world file says so itself: thousands of rows a day, not millions.",
        "Prices and margins are plausible but not modelled, and the shop floor beyond the presses is not simulated.",
        "The database and the dealer interface have only local adapters. The file drop and the stream also have "
        "cloud adapters.",
        "There is one domain model, the tyre company. A second company would need its own model.",
        "A new kind of incident needs a handler in the model's code. The world file alone cannot add one.",
    ])
    return out


# ------------------------------------------------------------------------------------------------------ the oracle
def oracle_picture():
    d = box(10, 30, 180, 80, "World day N", "the sources' output", "bx", href="tests-world.html")
    d += box(10, 170, 180, 80, "Oracle, day N", "the true answer", "solid")
    d += box(280, 30, 150, 80, "Bronze", "", "bx")
    d += box(520, 30, 150, 80, "Silver", "", "bx")
    d += box(760, 30, 150, 80, "Gold", "", "bx")
    d += arrow(190, 70, 278, 70) + arrow(430, 70, 518, 70) + arrow(670, 70, 758, 70)
    d += arrow(100, 112, 100, 168)
    d += arrow(190, 200, 355, 114, "rows") + arrow(190, 215, 595, 114) + arrow(190, 230, 835, 114)
    d += text(600, 200, "measures, versions, hours", "sm", "middle")
    d += text(800, 235, "windows, completeness", "sm", "middle")
    d += text(560, 285, "The oracle is computed from the world's own record. It never reads the platform.", "tx", "middle")
    return d


@page("tests-oracle", "The oracle: the true answer for every day and every layer", parent="tests", short="The oracle",
      blurb="One file per simulated day with what bronze, silver, gold and the metrics must show. Each layer is compared with it.")
def tests_oracle():
    out = ("<p class='lead'>The oracle is the simulated company's own account of what a correct platform must "
           "show at the end of each day. It gives numbers for bronze, for silver, for gold and for the "
           "certified metrics.</p>"
           "<p>Without it, a test can only check that the platform did not crash, or that it agrees with "
           "itself. With it, every layer is checked against an independent truth, every simulated day.</p>")
    out += fig(930, 305, oracle_picture(), "The oracle beside the platform's layers",
               "The top row is the platform. The oracle box below is the truth for the same day. Each arrow from it to a layer is one comparison.")
    out += "<h2>What the oracle records for one day</h2>"
    out += ("<p>The oracle is one file per simulated day. The table lists its parts. The first column is the "
            "key in the file, and the second is what it holds.</p>")
    out += table(["Key", "What it says"], [
        ["<code>sources</code>", "Rows each source produced that day."],
        ["<code>bronze</code>", "Rows a correct platform appends to bronze that day, per dataset. A file sent "
                                "twice adds 0. A short full copy adds 0, because it is held. Files with a "
                                "renamed column are stopped."],
        ["<code>silver</code>", "Per dataset at the end of the day: current rows, the dataset's own "
                                "reconciliation measures, and for datasets with history the number of versions "
                                "and of rows marked deleted. For readings, rows per event hour."],
        ["<code>gold</code>", "The on-time-in-full fact per promised date, the scrap fact per day and plant, "
                              "the sales fact per month and region, each with a completeness flag."],
        ["<code>metrics</code>", "The certified metrics as the metric views define them."],
        ["<code>known_answers</code>", "Every known answer in the metric files, beside what the world produced "
                                       "and whether they match."],
        ["<code>findings</code>", "The incidents planted that day and the detector each must fire."],
        ["<code>clean_day</code>", "True when no incident was planted that day."],
    ])
    out += code(ORACLE, r"^def day_oracle\(", caption="The oracle for one day, assembled from the model's own record.")
    out += "<h2>Expected and truth: two numbers where they can differ</h2>"
    out += ("<p>Usually what a correct platform shows is what really happened. Two incidents break that. When "
            "the change feed skips seven delivery rows, a correct platform cannot show them, because they "
            "never reached it. When lab files arrive with a renamed column, a correct platform rightly stops "
            "them. For these the oracle gives two numbers.</p>")
    out += table(["Number", "Meaning", "Used for"], [
        ["<code>expected</code>", "What a correct platform shows, given what reached it.", "Comparing the layer's tables."],
        ["<code>truth</code>", "What really happened at the source.", "The gap between the two is what reconciliation must find."],
    ])
    out += code(ORACLE, r"^def _silver\(", caption="The silver part of the oracle. Deliveries and inspections carry a truth beside the expected numbers.", cap=40)
    out += ("<p>Notice the first helper it calls, <code>measures</code>. It reads the measure list from the "
            "dataset's own manifest. The oracle and the platform therefore count exactly the same things, and "
            "a measure added to a manifest is checked without touching the oracle.</p>")
    out += code(ORACLE, r"^def measures\(", caption="The oracle counts what the manifest says to count.")
    out += "<h2>A worked example</h2>"
    out += ("<p>These are numbers from the recorded full-volume run of 4 November 2026, for the sales order "
            "lines. The first column is the day, and the next three are what the oracle expected and silver "
            "held. Five lines were deleted at the source on day 4.</p>")
    out += table(["Day", "Current order lines", "Versions", "Marked deleted", "Silver equals the oracle"], [
        ["3", "23,879", "24,711", "0", "yes"],
        ["4", "32,710", "34,274", "5", "yes"],
        ["5", "41,273", "43,497", "5", "yes"],
    ])
    out += "<h2>How each layer is compared</h2>"
    out += table(["Layer", "Code", "What is compared", "Tolerance"], [
        ["Bronze", src(ING_DAY), "Rows the day's run appended per dataset against the oracle's bronze count.", "Exact"],
        ["Silver", src(SIL_DAY), "Every number the oracle gives for the dataset that silver can count: current "
                                 "rows, measures, versions, deletes marked. For readings, the number of event "
                                 "hours whose count differs.", "0.01, because sums are rounded to cents"],
        ["Gold", f"{src(GOLD_DAY)} and {src(JUDGE)}", "Totals per fact, and every window: lines, quantity, "
                                                      "on-time-in-full lines, lines on the unknown member and "
                                                      "the completeness flag.", "0.01"],
        ["Semantic", src("P/src/dataplatform/semantic/answers.py"), "The five known answers, asked of gold the "
                                                                    "way each metric view defines them.", "The tolerance each known answer states"],
    ])
    out += code(ING_DAY, r"    def bronze_ok\(self\)", caption="Bronze equals the oracle when every dataset's count is equal.")
    out += code(SIL_DAY, r"^def compare\(", caption="Silver against the oracle: one pair of numbers per measure.")
    out += ("<p>Walk through it. For each dataset in the oracle's silver part, silver's measures are read "
            "through the tables port. Every numeric key the oracle gives that silver also produced becomes a "
            "pair of silver's number and the oracle's. For a dataset with event times, silver's rows per hour "
            "are compared with the oracle's, and the number of hours that differ becomes one more pair whose "
            "expected value is zero. A day is equal when no pair differs by more than the tolerance and the "
            "run succeeded.</p>")
    out += code(JUDGE, r"    def _otif\(self, want", r"                \"windows_differing\"", "Gold against the oracle for the on-time-in-full fact, window by window.", cap=24)
    out += ("<p>The gold comparison is the domain's own, because only the domain knows what a promised date "
            "is. It reads published gold with a query through a reader port, never the platform's logs.</p>")
    out += "<h2>The known answers at the semantic layer</h2>"
    out += ("<p>A <b>known answer</b> is one question with its correct result, written beside a certified "
            "metric in its file. It says, for example, that the on-time-in-full share for the south region in "
            "November is 0.8961. The values are the oracle's numbers for seed 11 at full volume after fourteen "
            "days.</p>")
    out += code("C/slices/02-tyre/manifest/metrics/mv_scrap.yaml", r"known_answer:", 1, "A known answer as the metric file states it.")
    out += ("<p>The table is from the gold design note. The second column is the value in the metric file. The "
            "third is what gold gave in the recorded run of 5 November 2026 at one tenth of full volume, which "
            "equalled that run's oracle. Three answers depend on volume, so at one tenth they differ from the "
            "file and agree with the oracle.</p>")
    out += table(["Known answer", "Value in the file", "Gold at scale 0.1", "Depends on scale"], [
        ["On-time-in-full share, south, November", "0.8961", "0.8851", "yes"],
        ["Order lines, November", "107,188", "10,216", "yes"],
        ["Sell-in units, north, November", "1,279,642", "115,637", "yes"],
        ["Sell-out units, north, October", "150,803", "150,803", "no, dealer files do not scale"],
        ["Scrap rate, plant P-CHN, 12 November", "0.0180", "0.017981", "no, presses do not scale"],
    ])
    out += code(ORACLE, r"^def known_answers\(", caption="The oracle reads each known answer from the metric file and puts its own number beside it.")
    out += "<h2>The recorded runs</h2>"
    out += ("<p>Three runs are written down in the design notes. Every number on this site that describes "
            "silver or gold against the oracle comes from one of them.</p>")
    out += table(["Run", "Where recorded", "Result"], [
        ["14 days at full volume, local Spark, 4 November 2026", src(W1C),
         "294 numbers compared between silver and the oracle over 14 days, all equal. Bronze equal every day. "
         "Run time 2,539 seconds, of which silver 1,711."],
        ["14 days at scale 0.1, a fresh Spark session each day, 5 November 2026", src(W1D),
         "Bronze, silver and gold equal the oracle on every one of the 14 days. 1,493 seconds in all."],
        ["Full volume through gold, 4 November 2026", src(W1D),
         "Gold equalled the oracle on days 1 to 7. On days 8 to 11 one completeness flag differed, and that "
         "was fixed afterwards. On day 12 the run ran out of memory and wrote no report."],
        ["14 days at scale 0.1 on DuckDB and on Spark, 5 November 2026", src(DUCK),
         "Bronze, silver and gold equal the oracle every day on both engines, with identical known answers."],
    ])
    out += "<h2>How the oracle is tested</h2>"
    out += (f"<p>{tf('test_world_tyre.py')} checks the oracle against the mock sources directly: the oracle's "
            "row counts and totals for silver must equal what the source tables and the stream actually hold. "
            f"{tf('test_semantic_metrics.py')} checks that every known answer names an address the oracle "
            "fills. A further test in that file reproduces the five known answers from a full-volume world. "
            "It runs only when an environment variable is set, because it takes about three minutes, and its "
            "note says it passed on 4 November 2026.</p>")
    out += code(T + "test_world_tyre.py", r'    "ds,measure,source",', r"^\)", "The oracle's silver numbers against what the sources hold.")
    out += not_built([
        "Bronze is compared by row count only. Sums and keys are first compared at silver.",
        "The full-volume run through gold for all fourteen days is owed. The design note says so.",
        "The oracle covers the three facts and the certified metrics. It gives no expected value for the "
        "dimension tables' surrogate keys, which are proven by table-driven tests instead.",
        "Press readings feed no gold fact today, so the stream is proven up to silver only.",
        "The known answers hold at full volume. At any other scale the test compares gold with the oracle of "
        "the same run, and three of the five differ from the file by design.",
    ])
    return out


# ------------------------------------------------------------------------------------------------------ incidents
def incidents_picture():
    days = [(3, "late readings"), (4, "deletes"), (5, "late file"), (6, "file resent"), (7, "column renamed"),
            (8, "late change"), (9, "short copy"), (10, "correction"), (11, "new dealer"), (12, "feed skips"),
            (13, "replay")]
    d = text(10, 25, "The world's own calendar: one incident a day from day 3 to day 13", "tx")
    for i in range(14):
        x = 10 + i * 69
        kind = "bxs" if i + 1 in (1, 2, 14) else "bx"
        label = dict(days).get(i + 1, "clean")
        d += box(x, 40, 63, 70, str(i + 1), [], kind)
        d += text(x + 31, 130 + (i % 2) * 20, label, "sm", "middle")
    d += text(10, 195, "The compressed calendar: the same eleven on days 2 and 3 of a 3-day run", "tx")
    d += box(10, 210, 120, 60, "1", "clean", "bxs") + box(140, 210, 400, 60, "2", "six incidents", "bx")
    d += box(550, 210, 400, 60, "3", "five incidents", "bx")
    return d


@page("tests-incidents", "Planted incidents: eleven faults the platform must catch", parent="tests",
      short="Planted incidents",
      blurb="Each incident's day, the rule that detects it, the layer that handles it, and the result in the recorded runs.")
def tests_incidents():
    out = ("<p class='lead'>A planted incident is a fault put into the simulated company's data on purpose, on "
           "a known day. A dealer's file arrives four days late. A file is sent twice. The lab renames a "
           "column. Each one is something real sources do.</p>"
           "<p>Clean data proves that the platform works when nothing goes wrong. Incidents prove the part "
           "that matters more: that it notices when something does, handles it at the right layer, and stays "
           "quiet on a clean day.</p>")
    out += fig(990, 290, incidents_picture(), "The two calendars",
               "Days 1, 2 and 14 are clean. The top row is the 14-day calendar. The bottom row is the same incidents packed into 3 days.")
    out += "<h2>How an incident is declared</h2>"
    out += ("<p>Every incident is one entry in the world file's calendar. The entry names the day, the dataset, "
            "the exact values, the detector expected to fire, and the oracle: one sentence that says what a "
            "correct platform must hold afterwards. A <b>detector</b> here is the id of a rule in the "
            "specification catalog, such as DEL-01 for deletes at the source.</p>")
    out += code(WORLD, r"  - id: sellout-file-late", r"      oracle:", "One calendar entry in full.")
    out += ("<p>The generator reads the values from the <code>params</code> block and keeps no table of its "
            "own. When the plain sentence and the block disagree, the block wins.</p>")
    out += "<h2>The four verdicts</h2>"
    out += table(["Verdict", "Meaning"], [
        ["handled", "The layer did what the rule asks."],
        ["flagged", "The layer noticed the condition and recorded it. A later layer applies it."],
        ["deferred", "Not this layer's rule. The layer only loads the rows."],
        ["missed", "The layer should have caught it and did not. This fails the test."],
    ])
    out += "<h2>Every incident</h2>"
    out += ("<p>The table has one row per incident. Day is the day in the world's own 14-day calendar. "
            "Compressed is the day in the compressed 3-day calendar. Detector is the rule id the world file "
            "names. The last two columns say what each layer does with it.</p>")
    out += table(["Incident", "Day", "Compressed", "What happens", "Detector", "Ingestion", "Where it is handled"], [
        ["<code>readings-out-of-order</code>", "3", "2", "For two hours, 4% of readings from 6 presses arrive up to 90 seconds late.",
         "LATE-03", "flagged: late readings counted", "Silver places each reading in the hour of its event time."],
        ["<code>order-lines-deleted</code>", "4", "3", "5 cancelled order lines from day 1 are hard-deleted at the source.",
         "DEL-01", "flagged: delete rows counted", "Silver keeps the 5 keys and marks them deleted. Gold no longer counts them."],
        ["<code>sellout-file-late</code>", "5", "2", "Dealer D-0042's October file, due on day 1, arrives on day 5.",
         "LATE-01", "deferred", "Gold publishes October flagged incomplete, then rebuilds it and logs a "
                               "restatement. Freshness shows the dealer's bookmark lagging."],
        ["<code>sellout-file-resent</code>", "6", "3", "The same file is sent again, byte for byte, under a new name.",
         "DUP-01", "handled: the batch is skipped as a duplicate", "Ingestion. Gold confirms nothing changed."],
        ["<code>lab-column-renamed</code>", "7", "2", "The lab renames <code>defect_code</code> to <code>defect</code> in one plant's files from 10:00.",
         "SCH-02", "handled: those files are stopped, naming the column", "Ingestion. The other plant's files keep loading."],
        ["<code>promised-date-out-of-order</code>", "8", "3", "A promised-date change arrives with a change sequence older than one already loaded.",
         "SCD-02", "flagged: an older change counted", "Silver puts the late change into history and keeps the newer one current."],
        ["<code>material-copy-short</code>", "9", "2", "The daily full copy of materials comes back with 40% of rows missing.",
         "DEL-04", "handled: the copy is held as a mass delete", "Ingestion. Nothing reaches bronze, and silver keeps every material."],
        ["<code>promised-date-corrected</code>", "10", "2", "12 corrections to promised dates are recorded in the metadata store, to take effect a day later.",
         "HIST-05", "deferred", "Gold applies the correction plan on its date and rebuilds only the plan's windows. Silver is unchanged."],
        ["<code>new-dealer-late</code>", "11", "2", "Orders for a new dealer start on day 11. The dealer's record appears in the interface on day 13.",
         "LATE-02", "deferred", "Gold keeps the lines on the unknown member, then gives them the right key the day the dealer arrives."],
        ["<code>change-feed-drops-rows</code>", "12", "3", "The change feed silently skips 7 delivery rows.",
         "REC-05", "deferred", "The live source compared with silver after the silver run must name the 7 keys."],
        ["<code>stream-replayed</code>", "13", "3", "After a restart, the stream replays the last 45 minutes of readings.",
         "DUP-04", "flagged: replayed ids counted", "Silver drops the replayed readings, so each is held once."],
    ])
    out += code(WORLD, r"^calendars:", r"      stream-replayed: \{day: 3\}", "The compressed calendar: the moves, and the one world setting it changes.", cap=24)
    out += "<h2>The results in the recorded runs</h2>"
    out += ("<p>The verdicts below are copied from the two design notes. The silver rows are from the "
            f"14-day full-volume run of 4 November 2026 in {src(W1C)}. The gold rows are from the 14-day run at "
            f"scale 0.1 of 5 November 2026 in {src(W1D)}. The one ingestion row is from the first of the two notes. "
            "Both runs used the world's own calendar, so the day column is the 14-day day.</p>")
    out += table(["Day", "Incident", "Detector", "Layer", "Verdict", "Evidence recorded"], [
        ["3", "readings-out-of-order", "LATE-03", "Silver", "handled", "188 late readings. Silver per event hour equals the oracle in all 24 hours."],
        ["4", "order-lines-deleted", "DEL-01", "Silver", "handled", "5 of 5 deleted keys kept in silver and marked deleted."],
        ["4", "order-lines-deleted", "DEL-01", "Gold", "handled", "0 of 5 deleted lines in the on-time-in-full fact."],
        ["5", "sellout-file-late", "LATE-01", "Gold", "handled", "October flagged incomplete on days 1 to 4, complete and restated on day 5. Rows went from 7,392 to 9,404."],
        ["6", "sellout-file-resent", "DUP-01", "Gold", "handled", "Sell-out per month unchanged, no restatement."],
        ["7", "lab-column-renamed", "SCH-02", "Gold", "equal to the oracle", "The affected plant's 12 hours flagged incomplete. The other plant's rows complete."],
        ["8", "promised-date-out-of-order", "SCD-02", "Silver", "handled", "3 versions. The late change sits in history, not current."],
        ["9", "material-copy-short", "DEL-04", "Ingestion", "handled", "720 of 1,800 keys missing, which is 40%. The copy was held."],
        ["11", "promised-date-corrected", "HIST-05", "Gold", "handled", "The plan applied on its date. Restated windows 13 to 18 November, none outside the plan."],
        ["11, 12", "new-dealer-late", "LATE-02", "Gold", "flagged", "6 then 10 of the dealer's lines on the unknown member, equal to the oracle. No row dropped."],
        ["13", "new-dealer-late", "LATE-02", "Gold", "handled", "The dealer arrived. 0 lines on the unknown member."],
        ["13", "stream-replayed", "DUP-04", "Silver", "handled", "19,440 replayed rows dropped. Silver readings equal the oracle."],
    ])
    out += note("Two things the recorded runs do not show",
                "<p>First, neither design note records a verdict for <code>change-feed-drops-rows</code>. The "
                "code that judges it exists in the silver day-by-day module, and the comparison it relies on "
                "has its own unit test, but I found no recorded run that shows the seven keys being named.</p>"
                "<p>Second, neither note records a run on the compressed calendar. The compressed days in the "
                "table above come from the world file. A test proves that the compressed calendar loads, moves "
                "every incident into three days and keeps day 1 clean. It does not run the platform on it.</p>")
    out += "<h2>How a verdict is reached</h2>"
    out += code(ING_DAY, r"^def judge\(", caption="Ingestion's judge: one verdict per incident, from the dataset's run report.", cap=32)
    out += ("<p>Walk through it. For a duplicate, the verdict is handled when at least one batch was skipped as "
            "a duplicate. For a renamed column, it is handled when a file was rejected and the reason names the "
            "missing column. For a short copy, it is handled when a batch was held as a mass delete. For the "
            "four detectors ingestion only notices, the verdict is flagged when the count it noticed is above "
            "zero. For the four that belong to later layers, the verdict is deferred, with a sentence saying "
            "who owns it. Anything else is missed.</p>")
    out += code(SIL_DAY, r"^def judge\(", r"        return v\(marked == len\(rows\)", "Silver's judge reads what the silver table holds. The delete case is shown.", cap=22)
    out += ("<p>Silver's judge does not read counts from a log. For the deleted lines it asks silver for the "
            "versions of each deleted key and checks that a current version is marked deleted. The gold judge "
            "works the same way, with queries on published gold.</p>")
    out += "<h2>No alarm on a clean day</h2>"
    out += ("<p>Catching every incident is half the promise. The other half is that nothing fires when "
            "nothing happened. One test walks every day's findings and fails if an incident shows up on a day "
            "the calendar did not give it. It also checks that days 1, 2 and 14 are clean.</p>")
    out += code(T + "test_world_tyre.py", r"^def test_detectors_fire_only_on_their_days\(", caption="Each incident on its own day and no other.")
    out += "<h2>How the incidents are tested</h2>"
    out += table(["Test file", "What it proves about incidents", "Needs"], [
        [tf("test_world_tyre.py"), "Each of the eleven is really in the sources on its day, with the detector the world file names.", "Python only"],
        [tf("test_ingest_world.py"), "Ingestion gives each of the eleven the verdict it owes, over 14 days at scale 0.05.", "The Delta package. Marked slow."],
        [tf("test_silver_world.py"), "Silver handles DEL-01, SCD-02, LATE-03 and DUP-04, and the short copy is still held.", "Spark. Marked slow."],
        [tf("test_twin_engine.py"), "The scenario checks for ingestion, silver and gold pass on the engine at scale 0.02.", "Spark for silver and gold. Marked slow."],
        [tf("test_twin_scenarios.py"), "Every calendar incident has a scenario file, and on a stand-in run log each raises one alert and no clean day raises any.", "Python only"],
    ])
    out += code(T + "test_ingest_world.py", r"^INCIDENTS = \[", r"^\]", "The verdict ingestion owes each incident, as a table.")
    out += not_built([
        "The silent loss of seven delivery rows has no recorded verdict, and neither slow test passes the live "
        "source reader that the check needs. Only the run script does.",
        "No recorded run uses the compressed calendar.",
        "Silver does not write detector rows to the run log for the incidents it handles, so operations cannot "
        "alert on them from the engine's run log yet.",
        "The late dealer file is listed by the world file as a finding only on day 5, when the file arrives. "
        "The lateness is visible from day 1. The operations notes record this as a gap in the specification.",
        "Checks for the data expectation services layer are pending in every scenario.",
    ])
    return out


# ------------------------------------------------------------------------------------------------------ tables
def tables_picture():
    d = box(10, 30, 220, 90, "One table of cases", ["name, input,", "expected output"], "solid")
    d += box(320, 10, 200, 60, "Adapter in memory", "", "bx")
    d += box(320, 85, 200, 60, "Adapter on Delta", "", "bx")
    d += box(320, 160, 200, 60, "Adapter on Spark", "", "bx")
    d += box(620, 85, 200, 60, "Same rows,", "same expected results", "bxs")
    d += arrow(230, 60, 318, 40) + arrow(230, 75, 318, 115) + arrow(230, 90, 318, 190)
    d += arrow(520, 40, 618, 100) + arrow(520, 115, 618, 115) + arrow(520, 190, 618, 130)
    d += text(420, 255, "A contract table: every adapter of a port passes the same cases.", "tx", "middle")
    return d


@page("tests-tables", "Table-driven tests and port contract tables", parent="tests", short="Tables of cases",
      blurb="How a case table looks, the tables every adapter of a port must pass, the history contract, and two engines on one set of tests.")
def tests_tables():
    out = ("<p class='lead'>Almost every test in this platform is a table. One test function holds the logic of "
           "the check. A table beside it holds the cases, one per row: a name, an input and the expected "
           "result.</p>"
           "<p>The reason is readability. A reader sees every case a function is proven on at a glance, and "
           "adding a case means adding one row. A failing case is reported by its name.</p>")
    out += "<h2>What a case table looks like</h2>"
    out += ("<p>This is the table for the hygiene stage of silver. Each row has a name, the raw text, the text "
            "expected after the stage, and any settings for that case.</p>")
    out += code(T + "test_silver_stages.py", r"^# -+ 3 hygiene", r"^def test_hygiene\(",
                "Nine cases of one stage, then the function that runs them.")
    out += ("<p>The names are sentences. When the row named empty becomes null fails, the report says exactly "
            "that, and nobody has to read the test's code to learn what broke.</p>")
    out += "<h2>A case table where the input is a timeline</h2>"
    out += ("<p>A table row can hold more than two values. The retention rules are tested with rows of a "
            "delivery, whether the lander took it, and the action expected.</p>")
    out += code(T + "test_retention.py", r"^ACTION_CASES = \[", r"^def test_landing_actions", "The rows, and the three-line function that runs them.", cap=20)
    out += "<h2>Contract tables for ports</h2>"
    out += fig(830, 270, tables_picture(), "One table of cases run against every adapter of a port",
               "The table on the left is written once. Each adapter in the middle must give the results on the right.")
    out += ("<p>A <b>port</b> is an interface the platform's core calls, such as the bronze writer. An "
            "<b>adapter</b> is one implementation of it for one product. The core is only safe to move between "
            "products if every adapter behaves the same. A contract table states that behaviour once, and the "
            "test runs every row against every adapter.</p>")
    out += code(T + "test_ingest_ports.py", r"^BRONZE_CASES = \[", r"        assert act\(w\) == expect",
                "The bronze writer's contract, and the function that runs each row against each adapter.", cap=34)
    out += ("<p>The two decorators above the function are the whole mechanism. One lists the adapters and the "
            "other the cases, and the test framework runs every combination. An adapter that is not available "
            "on the machine, such as PostgreSQL with no local server, is skipped with the reason.</p>")
    out += table(["Port", "Test file", "Adapters the table runs against"], [
        ["Metadata store", tf("test_meta_store.py"), "In memory, PostgreSQL"],
        ["Source database", tf("test_source_ports.py"), "In memory, SQLite, PostgreSQL"],
        ["File drop", "the same file", "In memory, local folder, object storage against a local stand-in"],
        ["Event stream", "the same file", "In memory, local files, a cloud stream against a local stand-in"],
        ["Bronze writer", tf("test_ingest_ports.py"), "In memory, Delta through a native library, Delta through Spark"],
        ["Landing", "the same file", "Local files, object storage against a local stand-in"],
        ["Silver tables and the stage runner", tf("test_silver_ports.py"), "Delta on Spark"],
        ["Gold tables and the gold reader", tf("test_gold_ports.py"), "Delta folders"],
        ["Table store", tf("test_table_store.py"), "Delta, Iceberg"],
        ["Run log reader", tf("test_obs_runlog.py"), "In memory, PostgreSQL, and a customer adapter that is skipped"],
        ["Notifier", tf("test_obs_notify.py"), "In memory, workspace destinations with an injected transport"],
    ])
    out += code(T + "test_source_ports.py", r"^DB_CASES = \{", r"^\}", "The source database contract: changes applied, the live rows expected, and the feed expected.")
    out += "<h3>A worked example: one row of that contract</h3>"
    out += ("<p>Take the third case, an older change arriving late. Order SO1, line 10, is inserted with a "
            "promised date of 12 November. Two updates follow. The table shows the three changes in the order "
            "they are applied, and what every adapter must then hold.</p>")
    out += table(["Applied", "Operation", "Promised date", "Change sequence"], [
        ["first", "insert", "2026-11-12", "1"],
        ["second", "update", "2026-11-14", "3"],
        ["third", "update", "2026-11-13", "2"],
    ])
    out += table(["What is checked", "Expected"], [
        ["The live row", "Promised date 2026-11-14 at change sequence 3. The older change did not overwrite the newer."],
        ["The change feed, in arrival order", "Insert 1, update 3, update 2. The feed keeps arrival order, late change included."],
    ])
    out += ("<p>The last case is also worth reading. It says a row can be in the table and not in the change "
            "feed. That is the silent loss the simulated company plants, and the contract makes sure every "
            "source adapter can express it.</p>")
    out += "<h2>The history contract</h2>"
    out += ("<p>Silver can run on two engines, the job engine and the declarative engine. They store history "
            "differently. Type 2 history keeps one row per version of a key. To make sure both engines mean "
            "the same thing, the cases are held in a file of pure data that names no engine.</p>")
    out += code(HIST, r"^CASES: list", r"^\]", "The eight cases every bronze-to-silver engine must pass.", cap=36)
    out += ("<p>Read one case. The fifth gives three batches for key <code>a</code>: an insert at sequence 1, "
            "an update to value 3 at sequence 5, and then an update to value 2 at sequence 4 that arrives last. "
            "The expected result is three versions in sequence order, with value 3 current. The late change "
            "went into history and did not become current.</p>")
    out += table(["Field of a change row", "Meaning"], [
        ["key", "The business key."],
        ["value", "A tracked column. A change to it opens a new version."],
        ["note", "An untracked column. A change to it updates the version in place."],
        ["sequence", "The source's order of changes."],
        ["op", "I for insert, U for update, D for delete."],
        ["committed minute, feed position", "When the source committed the change, and its place in the feed."],
    ])
    out += code(T + "test_silver_stages.py", r"^def test_history_type2\(", caption="The job engine run against the contract.")
    out += ("<p>The engines also differ in how they store a deleted key. The job engine keeps the last version "
            "current and flags it deleted. A managed change-capture engine closes the version. So each "
            "engine's output is first brought to one common shape, and then compared.</p>")
    out += code(HIST, r"^def normalise_closed\(", caption="Bringing an engine that closes deleted versions to the contract's shape.")
    out += "<h2>The same tests on two engines</h2>"
    out += ("<p>The silver stages and the gold builds are written against one dataframe interface. They run on "
            "Spark, and they also run on DuckDB through a library that offers the same interface. "
            "<b>Conformance</b> means the same test tables pass on both.</p>")
    out += code(T + "conftest.py", r"^def pytest_addoption\(", r"        activate\(\)", "The engine is chosen once per test process, before any test module is imported.")
    out += code(T + "conftest.py", r"^def spark\(engine_name", caption="The fixture keeps the name spark. The tests only see a session and its frames.")
    out += ("<p>The option is <code>--engine spark</code>, which is the default, or <code>--engine duckdb</code>. "
            "The engine cannot be chosen per test, because the DuckDB library takes the place of the Spark "
            "imports for the whole process. So conformance is two test processes over the same files.</p>")
    out += code("P/Makefile", r"^CONFORMANCE = ", r"--engine duckdb", "The four test files that make up conformance, and the two runs.")
    out += ("<p>The recorded result is in the DuckDB design note. On 5 November 2026, 143 stage and gold-build "
            "cases passed on Spark and the same 143 passed on DuckDB. Spark took 74 seconds and DuckDB 133 "
            "seconds. Getting there took 13 rewrites into forms both engines read the same way, and the note "
            "lists each. Four cases pin known differences between the engines, such as how a record hash "
            "treats empty fields, which is why one table is always written by one engine.</p>")
    out += "<h2>How the tests themselves are kept honest</h2>"
    out += table(["Rule", "Where it shows"], [
        ["A check with nothing to compare is never a pass", "The reconciliation function fails a hop with no value."],
        ["Pending is said, not hidden", "An adapter or layer that does not exist yet is a skipped row with its reason."],
        ["The core names no product", "Several test files scan the core's source for product names and fail on any."],
        ["Generated files are checked for drift", "A test regenerates the dashboard and view files and compares them."],
    ])
    out += not_built([
        "The history contract runs against the job engine in the test suite. The declarative engine's run of "
        "the contract is in the spikes folder and needs a workspace. It is not part of the tests.",
        "The gold tables contract has one adapter, so it proves that adapter and not yet that two agree.",
        "The silver tables contract runs on Spark. The DuckDB silver adapter is exercised by the end to end "
        "run, not by this contract table.",
        "Conformance covers four test files. The day-by-day golden tests run on Spark only in the test suite.",
        "The design note says continuous integration runs the DuckDB conformance step. The public snapshot "
        "contains no workflow folder, so I could not verify that from the repository. The Makefile target "
        "exists.",
    ])
    return out


# ------------------------------------------------------------------------------------------------------ by layer
def layers_picture():
    names = [("Metadata", "store"), ("Ingestion", ""), ("Bronze", ""), ("Bronze to", "silver"), ("Silver to", "gold"),
             ("Semantic", ""), ("Operations", "")]
    marks = ["contract table", "tables, world", "row counts", "full compare", "run script", "5 answers", "fixture"]
    kinds = ["bxs", "bx", "bx", "solid", "bx", "bx", "bxs"]
    d = ""
    for i, ((a, b), m, k) in enumerate(zip(names, marks, kinds)):
        x = 10 + i * 139
        d += box(x, 40, 129, 80, [a, b] if b else a, [], k)
        d += text(x + 64, 150, m, "sm", "middle")
    d += text(495, 195, "Under each layer: the strongest proof it has against known truth.", "tx", "middle")
    return d


@page("tests-by-layer", "What each layer has: tests, golden data, incidents, and what is missing", parent="tests",
      short="Proof by layer",
      blurb="One row per layer with its test files and counts, whether the oracle checks it, and the gaps. Plus the obligation coverage check.")
def tests_by_layer():
    obs_names, obs_total = obs_test_files()
    out = ("<p class='lead'>This page answers one question exactly: for each layer of the platform, what proves "
           "it? It lists the test files, says whether the layer is compared with the oracle's golden data, "
           "names the planted incidents it is judged on, and says what is missing.</p>")
    out += fig(990, 210, layers_picture(), "The seven layers and the strongest proof each has",
               "Bronze to silver has the most complete comparison in a test. The metadata store and operations have no golden data.")
    out += "<h2>How to read the counts</h2>"
    out += ("<p>The number after each file name is the number of test functions in the file. It was counted "
            "when this page was built, by searching each file for lines that start a test function. It is a "
            "count of functions, not of cases. A table-driven function with twenty rows counts as one, so the "
            "number of cases that actually run is larger.</p>")
    out += "<h2>One row per layer</h2>"
    out += table(["Layer", "Unit and contract test files", "Golden data from the oracle", "Incidents it is judged on", "What is missing"], [
        ["Metadata store",
         files("test_meta_store.py", "test_meta_loader.py", "test_lakebase.py"),
         "No. It holds settings and logs, not business data. Every test that runs the simulated company loads "
         "the tyre manifests through it.",
         "One, indirectly. The 12 promised-date corrections must be found in the store's corrections table "
         "and not in a file.",
         "The PostgreSQL rows of the contract table are skipped when no local server answers."],
        ["Ingestion: sources to landing",
         files("test_source_ports.py", "test_ingest_steps.py", "test_ingest_pipelines.py", "test_ingest_formats.py",
               "test_resilience.py", "test_cloud_adapters.py"),
         f"Yes, through bronze. {tf('test_ingest_world.py')} runs 14 days at scale 0.05.",
         "All eleven get a verdict. Handled: DUP-01, SCH-02, DEL-04. Flagged: LATE-03, DEL-01, SCD-02, DUP-04. "
         "Deferred: LATE-01, HIST-05, LATE-02, REC-05.",
         "The world test needs the Delta package and is skipped without it. It is marked slow."],
        ["Bronze",
         files("test_ingest_ports.py", "test_profiling.py", "test_retention.py", "test_housekeeping.py",
               "test_baseline_stage.py"),
         "Yes, by row count only. Rows appended per dataset by each day's run equal the oracle's count on each "
         "of the 14 days. The same test also reloads a past day and checks that no bronze table changes.",
         "The file sent twice adds 0 rows. The short copy adds 0 rows. The renamed-column files are stopped.",
         "Sums and keys are not compared at bronze. Content is first compared at silver."],
        ["Bronze to silver",
         files("test_silver_plan.py", "test_silver_stages.py", "test_silver_ports.py", "test_silver_runner.py",
               "test_silver_engines.py", "test_silver_declarative.py", "test_privacy_stage.py", "test_live_recon.py",
               "test_reconcile_buckets.py", "test_reconcile_engine.py", "test_table_store.py"),
         f"Yes. {tf('test_silver_world.py')} runs 14 days at scale 0.05 and requires that no number differs "
         "from the oracle on any day. Recorded at full volume on 4 November 2026: 294 numbers, all equal.",
         "DEL-01, SCD-02, LATE-03 and DUP-04 must be handled. DEL-04 must still be held.",
         "The test needs Spark and is marked slow. The declarative engine is not run against the oracle. "
         "REC-05 is not judged in this test."],
        ["Silver to gold",
         files("test_gold_plan.py", "test_gold_engine.py", "test_gold_harness.py", "test_gold_ports.py",
               "test_gold_calendar.py"),
         f"Partly in tests, fully in a recorded run. {tf('test_twin_engine.py')} runs gold each day at scale "
         "0.02 and checks the verdicts and the days its scenario files name. The run script compares gold with "
         "the oracle every day. Recorded on 5 November 2026 at scale 0.1: equal on all 14 days.",
         "DEL-01, LATE-01, DUP-01, HIST-05 and LATE-02.",
         "No test asserts gold equals the oracle on all 14 days. The full-volume 14-day run is owed. The "
         "scenario test needs Spark and is marked slow."],
        ["Semantic",
         files("test_semantic_metrics.py", "test_semantic_experiences.py")
         + "<br>" + tf("test_semantic_governance.py", "tests/databricks_realm/"),
         "Yes, five known answers. One test checks that the oracle fills every known answer's address. One "
         "reproduces all five from a full-volume world, and runs only when asked. Two more compare the metric "
         "views' numbers from built gold with the file and with the oracle, and skip unless a gold run's "
         "output is named.",
         "None directly.",
         "The gold comparison is a separate command after a run. Recorded at scale 0.1: 12 passed and 4 "
         "failed, the 4 being answers that depend on volume. At full volume it is owed for the 14-day run."],
        ["Operations",
         f"{len(obs_names)} files, {obs_total} test functions in all: the files named <code>test_obs_</code>, "
         "then " + ", ".join(tf(n) for n in obs_names if not n.startswith("test_obs_")),
         "No. There is no oracle for alerts or screens. They are checked against the incident calendar on a "
         "stand-in run log.",
         "All eleven: each must raise one alert, and no clean day may raise any.",
         "The checks do not run on the run log the engine writes. Delivery to a real workspace is untested."],
    ])
    out += (f"<p>The simulated company has its own tests, which every golden comparison rests on: "
            f"{tf('test_world_tyre.py')} and {tf('test_world_calendar.py')}. The first needs only Python.</p>")
    out += "<h2>Which of these run without Spark</h2>"
    out += ("<p>A check of the public snapshot on 19 November 2026 ran the folders "
            "<code>tests/dataplatform</code> and <code>tests/aws_databricks</code> without Spark. 1316 tests "
            "passed and 219 were skipped. The skipped ones need Spark. That figure counts cases, so it is "
            "much larger than the function counts in the table.</p>")
    out += table(["Golden-data test", "Needs", "In a run without Spark"], [
        ["<code>test_world_tyre.py</code>", "Python and the catalog beside the repository", "Runs"],
        ["<code>test_ingest_world.py</code>", "The Delta package", "Runs only if that package is installed"],
        ["<code>test_silver_world.py</code>", "Spark and the Delta package", "Skipped"],
        ["<code>test_twin_engine.py</code>, silver and gold part", "Spark", "Skipped"],
        ["Known answers against built gold", "A finished run of the platform", "Skipped"],
    ])
    out += ("<p>I did not see the list of which tests were skipped in that check, so I cannot say from it "
            "whether the bronze world test ran. The table above is from the skip conditions written in each "
            "test file.</p>")
    out += code(T + "test_silver_world.py", r"^def test_silver_equals_the_oracle\(", caption="The silver golden test: one case per day, and no difference allowed.")
    out += code(T + "test_ingest_world.py", r"^def test_bronze_equals_the_oracle\(", caption="The bronze golden test: one case per day.")
    out += "<h2>The obligation coverage check</h2>"
    out += ("<p>The specification catalog holds a list of obligations: rules the platform must keep, each "
            "with an id such as DEL-01. The coverage check asks a plain question of each one. Does any test "
            "file cite its id?</p>")
    out += table(["State", "Meaning"], [
        ["tested", "A test file contains the id."],
        ["waived", "A waiver names the id, with a reason of at least 10 characters, an approver and an end date that has not passed."],
        ["built", "Only code contains the id. The rule is kept somewhere but no test says so."],
        ["none", "Nothing cites it."],
    ])
    out += code("P/src/dataplatform/governance/coverage.py", r"^def coverage\(", caption="The whole check: a test beats a waiver, a waiver beats code.")
    out += code("P/src/dataplatform/governance/coverage.py", r"^def _cited\(", caption="What cited means: the id as a whole word in the file's text.")
    out += "<h3>A worked example</h3>"
    out += ("<p>The rule about deletes at the source has the id DEL-01. The table shows what three pieces of "
            "text do to its state. The cases are the check's own test cases.</p>")
    out += table(["Text found", "Where", "State of DEL-01"], [
        ["<code># DEL-01 proven here</code>", "A test file", "tested"],
        ["<code>DEL-011 and XDEL-01</code>", "A test file", "none. A longer id does not count for a shorter one."],
        ["<code>DEL-04 guard</code>", "A code file", "DEL-04 is built. DEL-01 is untouched."],
    ])
    out += ("<p>I ran the script on 19 November 2026 against the public snapshot and the catalog beside it. "
            "The catalog holds 105 obligations. 59 are cited by a test, which is 56%. None is waived. 14 are "
            "cited only in code. 32 are cited nowhere.</p>")
    out += table(["State", "Obligations", "Share of 105"], [
        ["tested", "59", "56%"], ["waived", "0", "0%"], ["built", "14", "13%"], ["none", "32", "30%"],
    ])
    out += ("<p>The waiver file lists six proposed waivers. Each has an empty approver, so by the rule above "
            "none of them counts yet.</p>")
    out += code("P/docs/dac-waivers.yaml", r"^- id: HEAL-01", 4, "One proposed waiver. The empty approver is why it does not count.")
    out += code(T + "test_dac_coverage.py", r'    \("cited in a test is tested"', r"^\]\)", "The coverage check's own cases.")
    out += note("What this check does and does not say",
                "<p>Cited is a weak word. A test that mentions an id in a comment counts as much as a test that "
                "proves the rule. The check finds obligations nobody has written a test for. It does not "
                "measure how well the tested ones are tested, and it does not run the tests.</p>")
    out += not_built([
        "The success criterion for coverage says 89 of 89 rules. The catalog now holds 105, and 59 are cited "
        "by a test.",
        "The six waivers are proposed and unsigned, so the count of waived obligations is zero.",
        "Coverage counts a citation, not a passing test. A cited test that is skipped still counts.",
        "No golden-data test exists for the metadata store or for operations. Both are proven by contract "
        "tables and, for operations, by a stand-in run log.",
        "Gold has no test file of its own that runs the 14 days and asserts equality with the oracle each day, "
        "the way silver and bronze have.",
        "The script reads workflow files as tests too. The public snapshot has no workflow folder, so any "
        "obligation that only a workflow proves shows as untested here.",
    ])
    return out


E2E = "P/tests/dataplatform/test_end_to_end.py"


@page("tests-end-to-end", "The daily end-to-end test: every layer against the truth, day by day", parent="tests",
      short="Daily end-to-end test",
      blurb="Three simulated days through every layer on a laptop, judged day by day. The same tables judge the 14-day proof on Databricks.")
def end_to_end():
    out = ("<p class='lead'>This is the test to run before every change is trusted. It plays the simulated company for three "
           "days, sends each day through ingestion, silver and gold, and then asks one question per layer: did it equal the "
           "true answer on every day? A failure names the layer and the day.</p>")
    d = box(10, 20, 170, 80, "Simulated company", ["three days, scale 0.1"], "bxm", href="tests-world.html")
    d += box(220, 20, 150, 80, "Ingestion", ["bronze"], "bx", href="ingest.html")
    d += box(410, 20, 150, 80, "Silver", [""], "bx", href="silver.html")
    d += box(600, 20, 150, 80, "Gold", [""], "bx", href="gold.html")
    d += box(790, 20, 180, 80, "report.json", ["one file for the run"], "solid")
    d += arrow(182, 60, 218, 60) + arrow(372, 60, 408, 60) + arrow(562, 60, 598, 60) + arrow(752, 60, 788, 60)
    d += box(790, 140, 180, 80, "The test's tables", ["one question each"], "bx")
    d += arrow(880, 102, 880, 138)
    out += fig(990, 250, d, "The end-to-end test", "The run writes one report. The test reads it and judges it with a few small tables.")
    out += "<h2>What it does</h2>"
    out += ladder([
        ("Run the loop", f"A fixture starts {src('P/scripts/run_platform.py')} with DuckDB, SQLite sources, the in-memory metadata store and the compressed calendar, which puts every planted fault in days 2 and 3. It takes about two minutes on a laptop."),
        ("Read the report", "The run writes <code>report.json</code>: per day, each layer's measures next to the oracle's, every incident's verdict, and every run's status."),
        ("Judge it with tables", "One table says which part of the report holds each layer. One says which layers' incidents must all be handled. One says which runs must have succeeded."),
        ("Name the failure", "A failing test says which layer and which day, and lists each measure that differs."),
    ])
    out += code(E2E, r"^LAYERS = \[", r"^\]", "The layers and where their verdicts are in the report.", cap=12)
    out += code(E2E, r"^def test_the_layer_equals_the_oracle_every_day", caption="The check that answers the question: a layer's measures equal the oracle's on every day.")
    out += code(E2E, r"^def test_incidents_were_actually_planted", caption="A green run that planted nothing proves nothing, so the test counts the kinds of fault judged.")
    out += "<h2>What it checks</h2>"
    out += table(["Test", "What it asserts"], [
        ["The layer equals the oracle every day (bronze, silver, gold)", "Every measure of every dataset or table equals the oracle's value on every day"],
        ["Every incident the layer judges is handled", "No incident's verdict is <i>missed</i> at ingestion, silver or gold"],
        ["Incidents were actually planted", "At least eleven kinds of fault were judged in the run"],
        ["Every run succeeded", "Ingestion, silver and gold runs ended succeeded, or partial where a batch was held on purpose"],
        ["The metrics were asked of built gold", "The known questions were asked and answered by the gold tables the run built"],
        ["The known answers match the specification (full scale only)", "Skipped at three days and scale 0.1: the specification's values hold at scale 1.0 after 14 days"],
    ])
    out += "<h2>Run it on another run's report</h2>"
    out += ("<p>The test does not need to start its own run. Set <code>DP_E2E_REPORT</code> to the path of a "
            "<code>report.json</code> from any run, and the same tables judge that run. The 14-day proof on Databricks writes the same "
            "report, so one set of checks judges both the laptop loop and the cluster. The full-scale known-answer check runs only "
            "when the report is for 14 days at scale 1.0.</p>")
    out += "<h2>A recorded result</h2>"
    out += ("<p>On 19 November 2026 the loop ran for three days at scale 0.1 on a laptop in 119 seconds. Bronze, silver and gold "
            "equalled the oracle on all three days, 11 of 11 planted faults were handled at their layer, and the test passed "
            "(11 passed, 1 skipped). Silver took 87 seconds of the 119 and gold 19. These numbers come from that one run.</p>")
    out += not_built([
        "Below scale 0.1 the compressed calendar cannot find enough cancelled order lines to delete, and the world stops with an error. The test uses 0.1.",
        "Three days is the daily loop. Gold has not been judged on all fourteen days by a test in this repository, only by the run script. Point the test at the Databricks report to judge fourteen.",
        "On Spark the same test would take longer. It uses DuckDB so that it runs without a cluster.",
    ])
    return out
