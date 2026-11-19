"""Start here, and one day end to end."""
from lib import *  # noqa: F401,F403

RUN = "P/scripts/run_platform.py"


def spine(y=60, links=True):
    names = [("Sources", ["databases, files,", "an API, a stream"], "bxm", "ingest-sources"),
             ("Landing", ["files as", "they arrived"], "bx", "ingest"),
             ("Bronze", ["every batch,", "nothing lost"], "bx", "ingest-lander"),
             ("Silver", ["clean, keyed,", "with history"], "bx", "silver"),
             ("Gold", ["dimensions", "and facts"], "bx", "gold"),
             ("Semantic", ["metrics with", "one meaning"], "bx", "semantic")]
    d, w, gap = "", 140, 34
    for i, (t, s, k, pid) in enumerate(names):
        x = 10 + i * (w + gap)
        d += box(x, y, w, 90, t, s, k, href=f"{pid}.html" if links else None)
        if i:
            d += arrow(x - gap + 2, y + 45, x - 4, y + 45)
    return d


@page("index", "A data platform, explained from the big picture down to the code", short="Start here",
      blurb="The questions this explainer answers, the one picture to keep in mind, and how to read the pages.")
def index():
    out = ("<p class='lead'>This is a guided tour of a working data platform. It starts with one picture and "
           "ends at the line of code that does the work. You can stop at any depth and still have a true picture.</p>")
    out += note("Looking for the code?", "<p>Start from the " + link("map", "code map") + ": the whole system on one page, upstream, "
                "data platform and downstream, with every part's starting function, every numbered concern, and for every "
                "function a list of who calls it.</p>")
    out += "<h2>The questions these pages answer</h2>"
    out += table(["The question", "Where the answer is"], [
        ["What happens to one day of data, from the source system to a number on a dashboard?", link("flow")],
        ["How can one piece of code load hundreds of different tables without a job written for each?", link("meta")],
        ["How does data get in, whether it is pushed as files, pulled from a database, or streamed?", link("ingest")],
        ["How is raw data turned into data you can trust: cleaned, typed, with no duplicates?", link("silver-stages")],
        ["How is history kept? What are type 1 and type 2, and what happens when a change arrives late?", link("silver-history")],
        ["What happens when a row is deleted at the source?", link("silver-deletes")],
        ["How do the tables that answer business questions get built?", link("gold")],
        ["How does a metric keep one meaning for everyone?", link("semantic")],
        ["How do we know when something went wrong, and where?", link("obs")],
        ["How do we know the numbers are right?", link("tests")],
    ])
    d = spine()
    d += box(10, 200, 836, 60, "Metadata store: what to load, how to treat it, and what every run did", "", "solid",
             href="meta.html")
    d += box(10, 290, 400, 60, "Operations: run log, freshness, alerts, lineage", "", "bxs", href="obs.html")
    d += box(446, 290, 400, 60, "Tests and proof: a simulated company with known answers", "", "bxs", href="tests.html")
    d += text(428, 30, "Data moves one way, left to right", "tx", "middle")
    out += "<h2>The one picture</h2>"
    out += fig(1060, 370, d, "The data spine with the metadata store under it",
               "The top row is the path the data takes. The highlighted bar is what drives every step. Click any box to zoom in.")
    out += ("<p>Five words carry the whole design. <b>Landing</b> is where files arrive, untouched. <b>Bronze</b> "
            "keeps every batch exactly as received, so nothing is ever lost. <b>Silver</b> is the cleaned, "
            "trusted copy, one row per business key or per version. <b>Gold</b> is shaped for questions: "
            "dimensions and facts. The <b>semantic layer</b> gives each metric one definition.</p>"
            "<p>Under all of it sits the <b>metadata store</b>, a small database. It holds the settings for "
            "every dataset and a record of everything every run did. The code is the same for every dataset. "
            "Only the settings differ. That is what metadata-driven means.</p>")
    out += "<h2>How to read these pages</h2>"
    out += table(["Zoom level", "What you get", "Time"], [
        ["0", "This page: the questions and the one picture", "2 minutes"],
        ["1", f"One page per layer, and {link('flow', 'one day end to end')}: what the layer is for, its parts, its design decisions", "10 minutes each"],
        ["2", "One page per part: how it works step by step, a worked example, the settings that drive it, the real code", "10 to 20 minutes each"],
        ["3", "One page per stage or per kind of history: the function line by line, and its tests", "As deep as you want"],
    ])
    out += ("<p>Every page shows its zoom level at the top. Boxes in pictures are links: click one to zoom in. "
            "Every code box is cut from the real file when the site is built, names the file and line, and links "
            "to that line in the public repository. Every layer page ends with an honest list of what is not "
            "built yet.</p>")
    out += "<h2>The example used everywhere</h2>"
    out += ("<p>The pages use one made-up company: a tyre maker with a sales system, a factory system, a "
            "laboratory, dealers, and presses that send readings. It has nine datasets, chosen so that between "
            "them they need every way of loading and every kind of history.</p>")
    out += table(["Dataset", "What it is", "How it arrives", "History kept in silver", "How deletes are seen"], [
        ["Sales order line", "One line of a customer order", "Pushed as a change feed", "Type 2: every version", "The feed says so"],
        ["Delivery", "A shipment against an order", "Pushed as a change feed", "Type 1: latest state", "The feed says so"],
        ["Production", "Tyres made, per press and shift", "Pushed as a change feed", "Appended events", "Not expected"],
        ["Material", "The catalogue of tyres", "Pulled as a full copy", "Type 2: every version", "A key missing from the copy"],
        ["Press", "The machines that cure tyres", "Pulled as a full copy", "Type 2: every version", "A key missing from the copy"],
        ["Dealer", "Who sells the tyres", "Pulled page by page from an API", "Type 2: every version", "Not detected"],
        ["Inspection result", "Laboratory test results", "Pushed as files", "Appended events", "Not expected"],
        ["Dealer sell-out", "Monthly sales reported by dealers", "Pushed as files", "Snapshot: the newest file for a month wins", "Not expected"],
        ["Press reading", "Sensor readings from the presses", "Streamed", "Appended events", "Not expected"],
    ])
    out += (f"<p>These settings are not typed here from memory. They are in the dataset manifests, for example "
            f"{src('C/slices/02-tyre/manifest/datasets/erp.sales_order_line.yaml')}.</p>")
    out += "<h2>Where the code is</h2>"
    out += table(["Folder", "What it holds"], [
        ["<code>src/dataplatform</code>, <code>tests</code>, <code>realms</code>, <code>scripts</code>",
         "The platform code these pages explain, its tests, and the deployable bundles. This explainer is built from them."],
        ["<code>explainer</code>", "The generator that wrote these pages. Run <code>make explainer</code> to rebuild them after a change."],
        ["The specification catalog, a separate checkout",
         "The specifications the code follows, and the tyre example's manifests and contracts. Set <code>DP_CATALOG</code> to its folder."],
    ])
    out += ("<p>You can also ask the platform to explain things without opening a browser. "
            "<code>python scripts/explain.py run</code>, <code>metadata</code>, <code>cicd</code> and <code>code</code> print "
            "plain-language text from a run's files, a dataset's manifest, the repository's checks, and the code itself. They "
            "use no model and no network, so the same inputs always give the same text.</p>")
    out += note("What is real and what is not",
                "<p>The code is real and runs. The tyre company is simulated: a program plays the company day by "
                "day, sends its data through the platform, and knows the true answer for every day. That is how "
                "each layer is checked. The source connectors used in these runs are local stand-ins, not "
                "connections to real business systems. Each layer page says what is not built yet.</p>")
    return out


@page("flow", "One day, end to end: what happens to a day of data", parent="index", short="One day, end to end",
      blurb="Follow one business day from the source systems to the known answers, step by step.")
def flow():
    out = ("<p class='lead'>The fastest way to understand the platform is to follow one day through it. This page "
           "walks the day in order. Each step links to the page that explains it in full.</p>")
    d = spine(40)
    for i in range(6):
        d += num(22 + i * 174, 36, i + 1)
    d += box(10, 190, 314, 70, "Each layer is compared with the truth", "the oracle of the simulated company", "bxs",
             href="tests-oracle.html")
    d += box(358, 190, 314, 70, "Every step is written to the run log", "in the metadata store", "solid",
             href="obs-runlog.html")
    d += box(706, 190, 314, 70, "After the last day: known answers", "asked of the metrics", "bxs", href="semantic.html")
    out += fig(1060, 280, d, "One day through the six stops", "The numbers match the steps below. Click a box to zoom in.")
    out += "<h2>The day, in order</h2>"
    out += ladder([
        ("Before the first day", f"The dataset manifests are checked and loaded into the {link('meta', 'metadata store')}. "
                                 f"From this moment the store is the only place the code reads settings from. See {link('meta-loader')}."),
        ("1 The sources change", "Orders are placed, tyres are made, a dealer changes region. In the runs shown on "
                                 f"these pages a simulated company does this, so the true totals for the day are known. See {link('tests-world')}."),
        ("2 Data reaches landing", "Sources that push send change-feed files and plain files to the landing zone. "
                                   f"Sources that are pulled are read by the platform. See {link('ingest-sources')}."),
        ("3 One ingestion run takes everything new to bronze",
         f"The {link('ingest-runner', 'runner')} loops over the enabled datasets. For each it picks one of "
         f"{link('ingest-pipelines', 'five pipelines')}, passes the batch through the {link('ingest-steps', 'entry steps')}, "
         f"and the {link('ingest-lander', 'lander')} writes it to bronze. Bronze row counts are compared with the truth."),
        ("4 One silver run",
         f"For each dataset the {link('silver-reader', 'reader')} picks the bronze batches loaded since silver last ran. "
         f"They go through the {link('silver-stages', 'thirteen stages')}, the {link('silver-history', 'history stage')} "
         f"decides versions and deletes, and the {link('silver-writer', 'writer')} applies it in one merge. Silver is compared with the truth."),
        ("5 One gold run for the business date",
         f"The {link('gold-harness', 'harness')} builds the dimensions and the facts for the day's {link('gold-windows', 'window')}. "
         "Gold is compared with the truth."),
        ("6 The semantic layer", f"After the last day the metrics are asked questions whose answers are known. See {link('semantic')}."),
        ("Throughout", f"Every run, dataset and step is recorded. Freshness, alerts and the operations page are read "
                       f"from that record. See {link('obs')}."),
    ])
    out += "<h2>The same thing in the code</h2>"
    out += ("<p>One script runs the simulated company and the platform together, day by day, on a laptop. Its "
            "opening comment is the most compact description of a day that exists.</p>")
    out += code(RUN, r'^"""Run the tyre world', r'^This file is the local binding', "The script's own description of a day.", cap=30)
    out += code(RUN, r"    def on_day\(day: Day\)", caption="What runs after each day's ingestion: silver, then gold, each judged against the truth.", cap=45)
    out += "<h2>What is checked on a day</h2>"
    out += table(["Layer", "Compared with the truth", "Incidents judged here"], [
        ["Bronze", "Rows per dataset for the day", "A file sent twice, a column renamed, a full copy that is far too short"],
        ["Silver", "Current rows, versions, deletes marked, and each dataset's own measures", "Deletes, a change arriving out of order, late readings, a replayed stream"],
        ["Gold", "Each fact's measures", "Late facts, history, deletes and duplicates seen from gold"],
        ["Semantic", "Known answers", "None"],
    ])
    out += (f"<p>An incident is a fault planted in the data on purpose on a known day. {link('tests-incidents')} "
            "lists them all, with the rule that detects each one and the layer that handles it.</p>")
    out += "<h2>Where it has run</h2>"
    out += table(["Where", "What", "Result"], [
        ["A laptop, local Spark", "14 simulated days at full scale, 4 November 2026",
         f"Silver equal to the truth on all 14 days, 294 numbers compared. Recorded in {src('P/docs/W1C-SILVER.md')}."],
        ["A laptop, no Spark", "The unit and contract tests of the public snapshot, 19 November 2026",
         "1,316 passed and 219 skipped. The skipped tests need Spark."],
        ["Databricks", "Three simulated days as a job", f"Described on {link('tests-by-layer')}."],
    ])
    return out
