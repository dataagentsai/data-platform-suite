"""Silver to gold: the hub, the plan, the harness, dimensions, keys, facts and windows."""
from lib import *  # noqa: F401,F403

HARNESS = "P/src/dataplatform/gold/harness.py"
GPLAN = "P/src/dataplatform/gold/plan.py"
GPORTS = "P/src/dataplatform/gold/ports.py"
KEYS = "P/src/dataplatform/gold/keys.py"
WINDOWS = "P/src/dataplatform/gold/windows.py"
CALENDAR = "P/src/dataplatform/gold/calendar.py"
INSPECT = "P/src/dataplatform/gold/inspect.py"
SPARK = "P/src/dataplatform/adapters/spark_gold.py"
DUCK = "P/src/dataplatform/adapters/duckdb_gold.py"
MODELS = "P/models/tyre/gold/"
MAN = "C/slices/02-tyre/manifest/gold/"
NOTE = "P/docs/W1D-GOLD.md"
T_PLAN = "P/tests/dataplatform/test_gold_plan.py"
T_ENGINE = "P/tests/dataplatform/test_gold_engine.py"
T_HARNESS = "P/tests/dataplatform/test_gold_harness.py"
T_PORTS = "P/tests/dataplatform/test_gold_ports.py"


# ------------------------------------------------------------------------------------------------------- hub
def gold_shape():
    d = box(10, 60, 150, 80, "Metadata store", ["gold manifests,", "run log"], "bxs", href="meta.html")
    d += box(10, 230, 150, 80, "Silver", ["every version,", "deletes marked"], "bx", href="silver.html")
    d += box(215, 60, 160, 80, "Plan", ["one per gold", "table"], "bx", href="gold-plan.html")
    d += box(215, 230, 160, 80, "Dimensions", ["one member per", "silver version"], "bx", href="gold-dimensions.html")
    d += box(430, 60, 160, 80, "Harness", ["reads, checks,", "writes"], "solid", href="gold-harness.html")
    d += box(430, 230, 160, 80, "Fact build", ["one model", "per fact"], "bx", href="gold-facts.html")
    d += box(645, 230, 150, 80, "Key lookup", ["as of the", "event time"], "bx", href="gold-keys.html")
    d += box(645, 60, 150, 80, "Windows", ["what to rebuild,", "what is complete"], "bx", href="gold-windows.html")
    d += box(850, 145, 130, 80, "Gold", ["dimensions", "and facts"], "bx")
    d += arrow(160, 100, 213, 100) + arrow(375, 100, 428, 100)
    d += arrow(160, 270, 213, 270) + arrow(375, 270, 428, 270) + arrow(590, 270, 643, 270)
    d += arrow(720, 228, 720, 144) + arrow(795, 100, 848, 165)
    d += arrow(510, 142, 510, 228)
    d += text(495, 30, "The harness drives every box and writes each step to the run log", "tx", "middle")
    return d


@page("gold", "Silver to gold: tables shaped for questions", parent="index", short="Silver to gold",
      blurb="One harness builds every dimension and fact from a manifest. Only a fact's own logic is written by hand.")
def gold():
    out = ("<p class='lead'>Silver is clean and complete, but it is still shaped like the sources: one table per "
           "source table, with every version of every row. Gold is shaped for questions. It holds dimensions, which "
           "describe things such as dealers, materials and presses, and facts, which hold the numbers people add up, "
           "such as order lines delivered on time or tyres scrapped per hour.</p>"
           "<p>Nobody writes a job per gold table. One piece of code, the gold harness, builds all of them. A "
           "harness is a fixed frame that does the reading, the checking and the writing, and calls a small piece "
           "of table-specific logic in the middle. What changes from one table to the next is a manifest, which is "
           "a short file of settings, and for a fact one model, which is a single query or function.</p>")
    out += fig(990, 330, gold_shape(), "The parts of silver to gold",
               "Read it left to right along the bottom row, then up. The highlighted box is the one piece of code that "
               "runs everything. Click any box to zoom in.")
    out += "<h2>Reader, processor and writer</h2>"
    out += ("<p>The harness has the same three parts as the silver runner, and the code keeps them apart on "
            "purpose.</p>")
    out += table(["Part", "What it is in the code", "What it may do"], [
        ["Reader", "The read edge, <code>GoldTables.silver</code> and <code>GoldTables.gold</code>",
         "Reads every row silver holds for an input, and the gold table as it is published now. Nothing else reads."],
        ["Processor", "The fact's model, called through <code>GoldEngine.build</code>, and the engine operations "
                      "around it (dimension shaping, key lookup, window numbers, checks)",
         "Takes rows and gives rows or small numbers back. It never reads a table and never writes one."],
        ["Writer", "The write edge, <code>GoldTables.write_dimension</code> and <code>GoldTables.replace_windows</code>",
         "Replaces a whole dimension, or exactly the named windows of a fact, in one transaction."],
    ])
    out += code(GPORTS, r"^class GoldTables\(Protocol\):", r"rows twice changes nothing\. Returns the rows written",
                "The read and write edges as the harness sees them. An adapter for Spark and one for DuckDB fill them in.",
                cap=30)
    out += "<h2>The order of one gold run</h2>"
    out += ladder([
        ("Build the plans", f"Every enabled gold table of the domain is read from the metadata store and turned into "
                            f"a {link('gold-plan', 'plan')}. A manifest the harness cannot run is refused here."),
        ("Put dimensions first", "The plans are sorted so that every dimension is built before any fact. A fact "
                                 "that names a dimension with no manifest is refused."),
        ("Take a lease per table", "Before a table is built the run takes a lease on it. A lease is a short lock "
                                   "with a time limit, kept in the metadata store. If another run holds it, this "
                                   "table is recorded as skipped and the run moves on."),
        ("Build each dimension", f"Every silver version becomes one member with a stable integer key. The whole "
                                 f"dimension is replaced. See {link('gold-dimensions')} and {link('gold-keys')}."),
        ("Build each fact", f"The harness reads the inputs, calls the fact's model, looks up the dimension keys as "
                            f"of the event, decides which windows to rebuild and which are complete, runs the "
                            f"checks, and replaces only the windows that changed. See {link('gold-harness')}, "
                            f"{link('gold-facts')} and {link('gold-windows')}."),
        ("Record", "Each step is one row in the run log. The table's bookmarks move only when its build succeeded."),
    ])
    out += code(HARNESS, r"^def run_gold\(", caption="One gold run. The loop is the whole orchestration: plan, lease, build, release.",
                cap=40)
    out += ("<p>Walk through it. <code>plans</code> returns the plans already sorted, dimensions first. For each "
            "one the run opens a dataset run in the run log and asks for the lease. A dimension goes to "
            "<code>_dimension</code> and a fact goes to <code>_fact</code>. A failed check or a refused plan is "
            "caught and recorded against the table, and the loop carries on with the next table. The lease is "
            "released in every case.</p>")
    out += "<h2>What is inside</h2>"
    out += table(["Page", "What it explains"], [
        [link("gold-plan"), "What a gold manifest declares, how it becomes a plan, and what a model looks like."],
        [link("gold-harness"), "The harness step by step: what it reads, the row ledger, the checks, the write and the run log."],
        [link("gold-dimensions"), "How silver's type 2 history becomes dimension members with a validity span."],
        [link("gold-keys"), "The stable integer keys, and why gold needs them on top of silver's hashes."],
        [link("gold-facts"), "The three tyre facts, what a row of each means and how each is rebuilt."],
        [link("gold-windows"), "Which windows a run rebuilds, when a window is complete, late data, restatements and withdrawn batches."],
    ])
    out += "<h2>The design decisions that shape this layer</h2>"
    out += table(["Decision", "What it means", "Why"], [
        ["One harness, one model per fact",
         "The harness owns every read, key lookup, check and write. A person writes only the fact's own query or function.",
         "The parts that are easy to get wrong, such as late data and reruns, are written and tested once."],
        ["Dimensions need no code",
         "A dimension is one silver table reshaped by a common pipeline. Its manifest may not declare a model.",
         "Every dimension gets keys, validity and reserved members in exactly the same way."],
        ["Facts are rebuilt by window and replaced, never appended",
         "A fact names a window column. Each run recomputes a range of windows and replaces the ones whose content changed.",
         "A rerun writes nothing, and a late or corrected row lands in the right place without special handling."],
        ["No fact row is ever dropped",
         "A fact row whose dimension member has not arrived yet points at a reserved member with key -1 and is fixed when the member arrives.",
         "Totals in gold always equal totals in the model's output. A missing dealer must not hide an order."],
        ["Checks come before the write",
         "If a check fails, nothing is written for that table and the last good windows stay published.",
         "Readers keep a correct older answer instead of getting a wrong newer one."],
        ["A window says whether it is complete",
         "Every fact row carries <code>_is_complete</code>. A window is complete only when every input has loaded past its end.",
         "A reader can tell a final number from one that is still filling."],
    ])
    out += "<h2>What a gold table looks like</h2>"
    out += ("<p>The table below lists the columns the harness adds. The business columns of each gold table come "
            "from its contract.</p>")
    out += table(["Table", "Column", "What it holds"], [
        ["Dimension", "the key column, for example <code>dealer_sk</code>", "The stable integer key of one version of one member."],
        ["Dimension", "<code>valid_from</code>, <code>valid_to</code>, <code>is_current</code>", "The time span in which this version was true."],
        ["Dimension", "<code>_source_valid_from</code>, <code>_is_deleted</code>, <code>_run_id</code>", "The real start of the version in the source, silver's delete mark, and the run that wrote the row."],
        ["Fact", "one key column per dimension, for example <code>dealer_sk</code>", "The member the row belongs to, as of the event. It is -1 when the member is unknown and -2 when the row has no business key."],
        ["Fact", "<code>_is_complete</code>", "True when every input of the row's window has arrived."],
        ["Fact", "<code>_run_id</code>", "The run that last changed the row's window."],
    ])
    out += "<h2>How we know it is right</h2>"
    out += (f"<p>Gold is compared with the oracle every day of a simulated run. The oracle is the simulated tyre "
            f"company's own record of what is true, explained in {link('tests')}. In the recorded run of 5 November "
            f"2026 (14 days at one tenth of full scale, local Spark), bronze, silver and gold equalled the oracle on "
            f"every one of the 14 days. A full-scale attempt on 4 November 2026 equalled the oracle in every number "
            f"through day 11 and then ran out of memory on the laptop in day 12's silver run, so the full-scale "
            f"14-day check is still owed. Both runs are written up in {src(NOTE)}.</p>")
    out += not_built([
        "The full-scale 14-day gold run against the oracle has not completed. The recorded complete run is at one tenth of full scale.",
        "Gold has no settings in the settings registry. The lookback, the orphan limit and the completeness choice live only in the manifest.",
        "A manifest can say <code>materialize</code> is something other than a table, and the plan checks that such "
        "a table has one input and no corrections, but nothing builds a materialised view. Every tyre table is a table.",
        "The press readings stream feeds no gold table yet.",
    ])
    return out


# ------------------------------------------------------------------------------------------------------- plan
@page("gold-plan", "Gold manifests and models: what a gold table declares", parent="gold", short="Manifests and models",
      blurb="Grain, inputs, dimensions, window, model and checks in one file. The plan refuses what it cannot run.")
def gold_plan():
    out = ("<p class='lead'>A gold table is described by two small things. The manifest is a file of settings that "
           "says what the table is. The model is the one query or function that holds the table's own business "
           "logic. A dimension has only a manifest. A fact has both.</p>"
           "<p>Before anything is read, the harness turns each manifest into a plan, which is one frozen object "
           "holding every answer the build needs. If the manifest asks for something the harness cannot do, the "
           "plan refuses with a message that names the table, and no data is touched.</p>")
    d = box(10, 20, 200, 70, "Gold manifest", "grain, inputs, window", "bx")
    d += box(10, 110, 200, 70, "Contract", "columns and types", "bx")
    d += box(10, 200, 200, 70, "Input's primary key", "from the store", "bxs", href="meta.html")
    d += box(300, 110, 200, 70, "plan_from_view", "checks, then builds", "solid")
    d += box(590, 110, 150, 70, "GoldPlan", "frozen", "bx")
    d += box(590, 200, 150, 70, "Model", "SQL or function", "bxm", href="gold-facts.html")
    d += box(830, 110, 150, 70, "Harness", "runs the plan", "bx", href="gold-harness.html")
    d += arrow(210, 55, 298, 130) + arrow(210, 145, 298, 145) + arrow(210, 235, 298, 162)
    d += arrow(500, 145, 588, 145) + arrow(740, 145, 828, 145) + arrow(740, 235, 828, 165)
    out += fig(990, 290, d, "From a manifest and a contract to a plan",
               "Three things go in on the left. The plan names the model but does not open it. The harness calls the model later.")
    out += "<h2>A fact's manifest</h2>"
    out += code(MAN + "fact_otif.yaml", r"^kind: gold_fact", r"^    tolerance:",
                "The manifest of the on-time-in-full fact of the tyre company.")
    out += ("<p>Read it top to bottom. The <b>grain</b> is what one row stands for: here, one order line. The "
            "<b>inputs</b> are the silver tables the model reads. Each entry under <b>dimensions</b> names a gold "
            "dimension, the key column the fact will carry, the business key column the fact has, and the fact "
            "column whose time decides which version of the member applies. The <b>window</b> names the column the "
            "fact is rebuilt by, the size of one window, and how many windows back each run recomputes. The "
            "<b>build</b> names the model. The <b>checks</b> are run before anything is published.</p>")
    out += "<h2>A dimension's manifest</h2>"
    out += code(MAN + "dim_dealer.yaml", r"^kind: gold_dimension", r"^materialize:",
                "The dealer dimension. There is no build: the common pipeline does all of it.")
    out += "<h2>The keys a gold manifest may hold</h2>"
    out += ("<p>The table lists each key, what it means, its allowed values and its default, as "
            "<code>plan_from_manifest</code> reads them. Gold keys are not in the settings registry, so the "
            "manifest is the only place they are set.</p>")
    out += table(["Key", "What it means", "Allowed values and default"], [
        ["<code>kind</code>", "Whether the table is a dimension or a fact.", "<code>gold_dimension</code> or <code>gold_fact</code>. Required."],
        ["<code>pipeline</code>", "How the table is built.", "<code>one_to_one</code> for a dimension, <code>modelled</code> for a fact. Defaults from the kind."],
        ["<code>domain</code>", "The domain that owns the table. It names the gold schema, <code>gold_&lt;domain&gt;</code>.", "Required."],
        ["<code>contract</code>, <code>contract_version</code>", "The contract that lists the table's columns and types. The loader refuses a version that does not match the contract file.", "Required."],
        ["<code>grain</code>", "The columns that identify one row. For a dimension it is the key column.", "A list of contract columns."],
        ["<code>inputs[].dataset</code>", "A silver table the build reads, written as source and dataset.", "A dimension uses its first input only."],
        ["<code>dimensions[].name</code>, <code>.key</code>", "A gold dimension the fact points at and the key column it fills.", "The key must be a contract column. The dimension must have a manifest."],
        ["<code>dimensions[].business_key</code>", "The fact's columns that hold the member's business key.", "A list of columns the model returns."],
        ["<code>dimensions[].as_of</code>", "The fact column whose time picks the member's version.", "A column the model returns."],
        ["<code>window.column</code>, <code>.unit</code>, <code>.lookback</code>", "The column a fact is rebuilt by, the size of a window, and how many windows back each run recomputes.", "Unit is <code>hour</code>, <code>day</code> or <code>month</code>. Lookback defaults to 0. A fact must have a window."],
        ["<code>build.kind</code>, <code>build.ref</code>", "The model: a SQL file, or a module and function.", "<code>sql</code> or <code>dataframe</code>. A fact must have one. A dimension must not."],
        ["<code>corrections</code>", "Whether recorded corrections are laid over silver before the model runs.", "True or false. Default false."],
        ["<code>completeness.on_incomplete</code>", "What to do with a window whose inputs have not all arrived.", "<code>flag</code> publishes it marked incomplete. <code>hold</code> keeps it out. Default <code>flag</code>."],
        ["<code>checks.grain_unique</code>", "Refuse to publish if two rows share the grain.", "Default true."],
        ["<code>checks.orphan_rate_max</code>", "The largest share of rows allowed on the unknown member.", "A share, such as 0.01 for one row in a hundred. No default: without it orphans only warn."],
        ["<code>checks.reconcile.measures</code>, <code>.tolerance</code>", "Columns whose totals must equal the model's totals, and how far each may differ.", "Tolerance defaults to 0."],
        ["<code>materialize</code>", "The physical form.", "Anything other than <code>table</code> must have one input and no corrections."],
    ])
    out += "<h2>The plan object</h2>"
    out += code(GPLAN, r"^class GoldPlan:", r"business_key: tuple\[str",
                "Every field of a gold plan. It is frozen: nothing downstream can change it.")
    out += ("<p>Most fields are the manifest's keys under a typed name. Three are worked out. "
            "<code>pipeline</code> defaults from the kind. <code>columns</code> are the contract's columns with "
            "each logical type turned into an engine type. <code>business_key</code> is filled only for a "
            "dimension, and it does not come from the manifest at all. It is the primary key of the dimension's "
            "input, read from the metadata store, so the key of a dealer is stated in one place only.</p>")
    out += "<h2>The checks, in the order the code makes them</h2>"
    out += code(GPLAN, r"^def plan_from_manifest\(", r"completeness\.on_incomplete \{on_inc!r\}",
                "The first half of plan_from_manifest: every refusal.", cap=45)
    out += ladder([
        ("Unknown kind", "The kind is not a gold dimension or a gold fact. Refused."),
        ("A model on a dimension", "A one-to-one table declares a build. Refused, because the common pipeline is the only way a dimension is made."),
        ("A fact without a model", "A modelled table has no build reference. Refused."),
        ("Unknown model kind", "The build kind is not <code>sql</code> or <code>dataframe</code>. Refused."),
        ("A materialised view that needs more", "The table is not a plain table and has two inputs or corrections. Refused."),
        ("Window problems", "The unit is not hour, day or month, a fact has no window, or the window column is not a contract column. Refused."),
        ("Dimension keys not in the contract", "A fact names a key column its contract does not have. Refused."),
        ("Unknown completeness choice", "<code>on_incomplete</code> is not <code>flag</code> or <code>hold</code>. Refused."),
        ("A dimension whose key is missing", "The input's primary key is unknown, or is not among the dimension's contract columns. Refused."),
    ])
    out += code(GPLAN, r"^def _dim_key\(", caption="A dimension's business key is its input's primary key, and it must be in the dimension's contract.")
    out += "<h2>From the manifest file to the harness</h2>"
    out += ("<p>The loader writes each gold manifest into the metadata store as a dataset, beside the source "
            "datasets. Its source name is the gold schema, <code>gold_sales</code> for the sales domain, and its "
            "primary key is the grain. Because gold tables are datasets in the store, their runs, steps and "
            "bookmarks sit in the same run log as ingestion and silver.</p>")
    out += code("P/src/dataplatform/meta/loader.py", r"^def gold_doc\(", caption="A gold manifest as a dataset of the store. Settings stay empty.")
    out += code(HARNESS, r"^def plans\(", caption="The harness reads every enabled gold dataset, fetches each input's primary key, and builds the plans.")
    out += code(GPLAN, r"^def order\(", caption="Dimensions first, then facts, each group by name.")
    out += "<h2>A model</h2>"
    out += ("<p>A model is the only code a person writes for a fact. It is pure, which means it reads only the "
            "inputs it is handed and writes nothing. A SQL model reads its inputs as views named by source and "
            "dataset joined with two underscores, such as <code>erp__sales_order_line</code>. It may use the named "
            "parameter <code>:business_tz</code>, the business time zone.</p>")
    out += code(MODELS + "sales/fact_otif.sql", r"^WITH first_promise", r"^FROM judged",
                "The model of the on-time-in-full fact. Its manifest is the first box on this page.")
    out += ("<p>Four things to notice. First, the model receives every silver row, including old versions and "
            "deleted rows, so it filters for itself with <code>_is_current AND NOT _is_deleted</code>. Second, it "
            "uses the old versions on purpose: <code>first_promise</code> takes the promised date of the line's "
            "first version, so a later reschedule does not move the goalpost. Third, it returns business keys "
            "such as <code>dealer_code</code>, not gold keys. The harness adds <code>dealer_sk</code>. Fourth, it "
            "returns <code>order_ts</code>, which is not in the fact's contract. The harness needs it to pick the "
            "dealer's version, and drops it when it puts the columns in contract order.</p>")
    out += (f"<p>A dataframe model is a function with a fixed signature. {link('gold-facts')} shows the one the "
            f"tyre company has, {src(MODELS + 'manufacturing/scrap.py')}.</p>")
    out += "<h2>A worked example: the plan of the on-time-in-full fact</h2>"
    out += ("<p>The left column is a field of the plan. The right column is its value for "
            "<code>fact_otif</code>, read from the manifest above.</p>")
    out += facts([
        ("table", "<code>gold_sales.fact_otif</code>"),
        ("pipeline", "<code>modelled</code>"),
        ("grain", "<code>order_id</code>, <code>line_no</code>"),
        ("inputs", "<code>erp.sales_order_line</code>, <code>erp.delivery</code>"),
        ("dimensions", "<code>dim_dealer</code> by <code>dealer_code</code> as of <code>order_ts</code>, <code>dim_material</code> by <code>material_code</code> as of <code>order_ts</code>"),
        ("window", "<code>promised_date</code>, one day, 35 days back"),
        ("build", "SQL, <code>gold/sales/fact_otif.sql</code>"),
        ("corrections", "true"),
        ("on_incomplete", "<code>flag</code>"),
        ("orphan_rate_max", "0.01, which is one row in a hundred"),
        ("reconcile", "<code>ordered_qty</code>, tolerance 0"),
    ])
    out += "<h2>How it is tested</h2>"
    out += (f"<p>{src(T_PLAN)} tests the plan without any engine. One table of cases holds every refusal with "
            "a piece of its message. Another test builds the plan of every real tyre manifest.</p>")
    out += code(T_PLAN, r"\(\"one-to-one with a build\"", r"plan_from_manifest\(doc, cols, \{\}\)",
                "The refusals as a table: a name, a manifest with one thing wrong, and the words the message must contain.")
    out += code(T_PLAN, r"^def test_every_tyre_gold_manifest_is_a_plan", caption="Every tyre gold manifest becomes a plan, and the three dimensions sort first.")
    out += not_built([
        "Some manifest keys are read into the plan, or written in the manifest, and then used by nothing. "
        "<code>history</code> on a dimension, the <code>role</code> and <code>bookmark</code> of an input, "
        "<code>keys.surrogate</code>, and <code>checks.reconcile.against</code> are in this group. The values "
        "<code>keys.unknown_member</code> and <code>keys.not_applicable</code> reach the plan, but the engine uses "
        "the constants -1 and -2 from the keys module whatever the manifest says.",
        "Gold has no keys in the settings registry, so a gold value cannot be set per domain or per environment the way a silver setting can.",
        "The manifest cannot say that an input arrives as one file per member, such as one file per dealer. The harness works it out from ingestion's bookmarks.",
        "The reconcile check says <code>against: silver</code> in the manifest, but the harness compares with the model's own output, not with silver.",
    ])
    return out


# ------------------------------------------------------------------------------------------------------- harness
@page("gold-harness", "The gold harness, step by step", parent="gold", short="The harness",
      blurb="What one fact build reads, the row ledger that proves no row was lost, the checks, the write and the run log.")
def gold_harness():
    out = ("<p class='lead'>The harness is the code that builds a gold table from its plan. For a fact it does "
           "everything except the business logic: it reads the inputs, calls the model, attaches dimension keys, "
           "decides which windows to rebuild, checks the result, writes it, and records what happened.</p>"
           "<p>It exists so that the hard parts are written once. Without it, every fact would have its own "
           "handling of reruns, late rows and missing dimension members, and each would be wrong in its own way.</p>")
    d = box(10, 90, 130, 80, "Read", ["silver inputs,", "watermarks"], "bx")
    d += box(175, 90, 130, 80, "Model", ["the fact's", "own logic"], "bxm", href="gold-facts.html")
    d += box(340, 90, 130, 80, "Key lookup", ["and the", "row ledger"], "bx", href="gold-keys.html")
    d += box(505, 90, 130, 80, "Windows", ["range and", "completeness"], "bx", href="gold-windows.html")
    d += box(670, 90, 130, 80, "Checks", ["before any", "write"], "solid")
    d += box(835, 90, 145, 80, "Replace", ["only changed", "windows"], "bx")
    for x in (140, 305, 470, 635, 800):
        d += arrow(x, 130, x + 33, 130)
    d += text(495, 40, "One fact, one run. Every box writes a row to the run log.", "tx", "middle")
    d += text(735, 205, "a failure here writes nothing", "sm", "middle")
    out += fig(990, 225, d, "The steps of one fact build",
               "Left to right. The dashed box is the only part a person writes. If the highlighted box fails, nothing after it happens.")
    out += "<h2>One fact build, in the order the code runs</h2>"
    out += ladder([
        ("Read the table's bookmarks", "The fact's own bookmarks say how far it has published and which correction plans it has already applied."),
        ("Read each input", "For every input the harness notes the silver table's version, reads every row silver holds, lays active corrections over the rows if the fact allows corrections, and works out how far that input has loaded in source time."),
        ("Record what was read", "Two rows go to the run log: the version of each input, and the text of the model with its parameter values. A past window can be rebuilt later from these two rows alone."),
        ("Call the model", "The model returns one row per grain with business keys and measures."),
        ("Look up the keys", "Each dimension's key is attached by a left join as of the event time. The row count before and after must be equal. The row ledger is written."),
        ("Choose the windows", "The range is the lookback from the business date, plus windows a correction plan names, plus published windows that still hold rows on the unknown member."),
        ("Decide completeness", "Each window in the range is complete only if every input has loaded past the window's end."),
        ("Finish the rows", "The harness adds <code>_is_complete</code> and <code>_run_id</code>, and puts the columns in contract order."),
        ("Check", "The grain must be unique, the share of rows on the unknown member must be under the limit, and the totals must equal the model's totals. A failure stops here and nothing is written."),
        ("Replace what changed", "A digest of each window's content is compared with the published window. Only windows whose digest differs are replaced, in one transaction."),
        ("Log restatements", "A changed window that readers had been entitled to treat as final is logged as a restatement, with its numbers before and after."),
        ("Move the bookmarks", "The input watermarks, how far gold has published, and the plans applied are saved together with the success status."),
    ])
    out += "<h2>What it reads from silver, and how</h2>"
    out += ("<p>The read edge hands over <b>every row</b> silver holds for an input: every version of every key, "
            "with deleted rows marked. It does not filter to the current rows and it does not read only what "
            "changed since last time. The model decides which rows it wants. An input that silver has never "
            "written comes back as an empty table with the right columns, so a fact can be built before all of "
            "its inputs have delivered.</p>")
    out += code(HARNESS, r"^def _inputs\(", caption="Reading the inputs of one fact: version, rows, corrections, and how far each input has loaded.", cap=30)
    out += ("<p>Walk through the loop. <code>silver_version</code> records the table version that was read. "
            "<code>silver</code> reads the rows. The lines about plans deal with corrections and withdrawn "
            f"batches, which {link('gold-windows')} explains. The frame is stored under the name the model will "
            "see, the source and dataset joined by two underscores. The last lines pick one of two ways to say "
            "how far the input has loaded. If ingestion keeps one bookmark per feed, for example one per dealer "
            "file, those are used. Otherwise the input has one watermark.</p>")
    out += code(HARNESS, r"^def silver_watermark\(", caption="How far an input has loaded: the end of the newest business day whose delivery silver has taken.")
    out += ("<p>A watermark here is the latest point in source time up to which an input is known to be loaded. "
            "It is not read from the data. It comes from the metadata store: silver's bookmark says how many "
            "file log records it has taken, the harness looks at the delivery window of each loaded record among "
            "those, and takes the end of the newest day. An empty delivery still moves it, which is right, "
            "because a day with no orders is still a day that has been loaded.</p>")
    out += "<h2>Where the as-of join on version validity happens</h2>"
    out += ("<p>Silver keeps type 2 history: each version of a dealer has a span during which it was true. Gold "
            "uses that span in one place, the key lookup. The dimension build copies silver's span into "
            "<code>valid_from</code> and <code>valid_to</code> on each member. The key lookup then joins each "
            "fact row to the member whose span contains the fact's event time. This is called an as-of join: "
            "the row gets the dealer as the dealer was at that moment.</p>")
    out += code(SPARK, r"    def add_keys\(self, plan", caption="The key lookup on Spark: one left join per dimension, on the business key and the validity span.")
    out += ("<p>The lines that matter, in order. Reserved members are taken out of the dimension first, so a "
            "fact can never match them by accident. The dimension's columns are renamed with a prefix so they "
            "cannot clash with the fact's. The join condition is the business key being equal, plus "
            "<code>valid_from &lt;= t</code> and <code>t &lt; valid_to</code>. The span is closed at the start "
            "and open at the end, so an event exactly at a change belongs to the new version and can never match "
            "two. The join is a left join, so a fact row with no match is kept. The last expression sets the "
            "key: -2 if the fact has no business key at all, the member's key if one matched, and -1 if none "
            "did.</p>")
    out += "<h2>The row ledger</h2>"
    out += ("<p>A join is the easiest place to lose or multiply rows without noticing. The harness therefore "
            "counts. It counts the rows read from each input, the rows the model returned, the rows after the "
            "key lookup, and for each dimension the rows that matched a member, the rows on the unknown member "
            "and the rows marked not applicable. These counts are the row ledger, and they are written to the "
            "run log as the step <code>gold.ledger</code>.</p>")
    out += code(HARNESS, r"^def _key_lookup\(", caption="Rows in must equal rows out, or the table is not published.")
    out += code(HARNESS, r"^def _ledger\(", caption="The ledger. Each join's three counts must add up to the rows after the lookup.")
    out += ("<p>Two rules make the ledger a check and not only a report. The rows built must equal the rows "
            "keyed. And for every dimension, matched plus unknown plus not applicable must equal the rows keyed. "
            "If the total moved, <code>_which_join</code> reruns the lookup one dimension at a time and names the "
            "first join that changed the count, so the message says which dimension has overlapping versions.</p>")
    out += "<h3>A worked ledger</h3>"
    out += ("<p>Four order lines go in. One is for dealer D-9, who has no record yet. One has no dealer code. "
            "The left table is the model's output. The right table is the ledger the run log receives.</p>")
    out += table(["order_id", "dealer_code", "dealer_sk after the lookup"], [
        ["O0", "D-1", "the key of D-1's version"], ["O1", "D-9", "-1"], ["O2", "(none)", "-2"],
        ["O3", "D-2", "the key of D-2's version"]])
    out += table(["Ledger count", "Value"], [
        ["<code>built</code>", "4"], ["<code>after_keys</code>", "4"], ["<code>dealer_sk.matched</code>", "2"],
        ["<code>dealer_sk.unknown</code>", "1"], ["<code>dealer_sk.not_applicable</code>", "1"]])
    out += "<h2>The checks before publishing</h2>"
    out += code(HARNESS, r"^def _checks\(", caption="Three checks. Any failure raises, and the caller writes nothing.", cap=40)
    out += table(["Check", "Run log step", "What fails it"], [
        ["Grain unique", "<code>detect.KEY-01</code>", "Two rows share the grain columns."],
        ["Orphan rate", "<code>detect.LATE-02</code>", "Rows on the unknown member are a warning. They fail the build only when their share is above <code>orphan_rate_max</code>."],
        ["Reconcile", "<code>gold.reconcile</code>", "A measure's total in the rows about to be written differs from its total in the model's output for the same windows by more than the tolerance."],
    ])
    out += ("<p>The reconcile check compares the harness with the model, not gold with silver. It proves that "
            "the key lookup, the window selection and the column ordering lost no quantity on the way.</p>")
    out += "<h2>The write: replace changed windows</h2>"
    out += code(HARNESS, r"    published = ctx\.tables\.gold\(p\)", r"^    ctx\.engine\.release\(built, keyed, rows\)",
                "The middle of _fact: the range, completeness, the checks, and the write.", cap=30)
    out += ("<p>The decision to write is made from digests. <code>window_numbers</code> returns, for each "
            "window, its row count, the sums of its measures and a digest, which is one number computed from "
            "every column of every row except <code>_run_id</code>. The harness computes this for the published "
            "table and for the new rows. A window is in <code>changed</code> only if the two digests differ. If "
            "the fact's manifest says <code>hold</code>, incomplete windows are taken out of the list. Then one "
            "call replaces exactly those windows.</p>")
    out += code(SPARK, r"    def replace_windows\(self, plan: GoldPlan, rows: DataFrame, windows",
                caption="The write edge on Delta: one overwrite limited to the named windows.")
    out += ("<p>The write is an overwrite with a condition on the window column, so rows of the named windows "
            "are removed and the new rows put in their place in one commit. Rows of other windows are not "
            "touched. A window that is named but has no new rows becomes empty, which is how an order that was "
            "deleted at the source leaves gold. The window names are checked against a strict pattern before "
            "they are placed in the condition, so no data value can ever become part of the statement.</p>")
    out += note("Why a rerun writes nothing",
                "<p>The digest leaves out <code>_run_id</code>. A second run over the same silver produces the same "
                "digest for every window, so <code>changed</code> is empty and nothing is written. A side effect is "
                "useful: the <code>_run_id</code> on a row tells you which run last changed its window.</p>")
    out += "<h2>What goes into the run log</h2>"
    out += ("<p>Each row below is one step the harness records for a table, in the order they are written. "
            "The step names are the ones in the code.</p>")
    out += table(["Step", "Written when", "What it carries"], [
        ["<code>gold.inputs</code>", "After the inputs are read", "The silver version of each input, and the business date of the run."],
        ["<code>gold.query</code>", "After the inputs are read", "The model's SQL with the parameter values shown, or the name of the dataframe function."],
        ["<code>gold.keys</code>", "After the key lookup", "Rows in and rows out. Failed if they differ, with the join that caused it."],
        ["<code>gold.ledger</code>", "After the key lookup", "Rows read per input, rows built, rows keyed, and three counts per dimension."],
        ["<code>detect.KEY-01</code>", "In the checks, and once per dimension", "Duplicate grain rows, or duplicate dimension keys."],
        ["<code>detect.LATE-02</code>", "In the checks, only when there are orphans", "Rows on the unknown member, per dimension key."],
        ["<code>gold.reconcile</code>", "In the checks", "The totals of the reconcile measures."],
        ["<code>detect.HIST-05</code>", "When a correction plan is applied for the first time", "The plan, its windows, its reason and who recorded it."],
        ["<code>gold.restatement</code>", "Once per restated window", "Each number before and after, with the window and the reason."],
        ["<code>detect.LATE-01</code>", "When late data restated a window", "The windows restated."],
        ["<code>gold.completeness</code>", "Always", "How many windows are incomplete and what each waits for."],
        ["<code>gold.write</code>", "Last", "All the counts of the build."],
    ])
    out += code(HARNESS, r"    last = _time\(done\.get\(\"published_through\"", r"return GoldTable\(p\.name, \"succeeded\"",
                "The end of _fact: restatements, the counts, and the bookmarks saved with the success status.", cap=30)
    out += "<p>The bookmarks of a fact are saved in the same call that marks the build succeeded. The table lists them.</p>"
    out += table(["Bookmark", "What it holds"], [
        ["<code>input.&lt;dataset&gt;</code>", "The watermark of each input in source time, as this run saw it."],
        ["<code>published_through</code>", "The end of the business date this run was for."],
        ["<code>feeds_waiting</code>", "The windows that are incomplete because a member's file has not arrived."],
        ["<code>plan.&lt;id&gt;</code>", "The business date on which a correction plan was first applied, so it is logged once."],
    ])
    out += "<h2>When something fails</h2>"
    out += code(HARNESS, r"^def _fail\(", caption="A failure is recorded against the table and the run carries on with the next one.")
    out += ("<p>A failed check is recorded under the step <code>gold.checks</code>, a refused plan under "
            "<code>gold.plan</code>, and any other error under <code>gold.build</code>. In all three cases "
            "nothing was written and no bookmark moved, so the next run tries the same windows again.</p>")
    out += "<h2>Rebuilding a past window from the run log</h2>"
    out += (f"<p>Because every fact run records its input versions and its query, {src(INSPECT)} can rebuild "
            "one window of a past run. It reads the two run log rows, reads each silver input as it was at that "
            "version, runs the same model and key lookup, and returns the window's numbers beside what gold "
            "publishes now. It writes nothing.</p>")
    out += code(HARNESS, r"^def _trace\(", caption="The two rows that make a past run reproducible.")
    out += "<h2>How it is tested</h2>"
    out += (f"<p>{src(T_HARNESS)} runs the real harness on local Spark with a metadata store held in memory and "
            f"a small made-up shop: dealers, orders by day, and a monthly file per dealer. {src(T_PORTS)} is the "
            "contract table for the read and write edges, which every adapter must pass.</p>")
    out += code(T_HARNESS, r"^def test_replace_never_append", caption="Three orders on three days. A rerun writes nothing. A delete and a change replace only their own two windows.")
    out += ("<p>The last two lines are the sharpest. Order A's window was not replaced, so its row still "
            "carries the first run's id. Order C's window was replaced, so its row carries the third run's id.</p>")
    out += code(T_HARNESS, r"^def test_a_failed_check_holds_the_last_good_windows", caption="Two dealer versions that overlap would multiply a fact row. The key step fails and gold is unchanged.")
    out += not_built([
        "Every run reads every silver row of every input and runs the model over all of it. Only the write is "
        "limited to changed windows. The manifest's comment about finding only the changed days describes an "
        "intent the code does not have yet.",
        "The inspect tool reads dimensions as they are now and does not replay corrections, so a window that a correction plan rebuilt shows its uncorrected numbers there.",
        "The window digest is computed with a hash function of the engine, so digests can be compared between two runs of one engine but not between Spark and DuckDB.",
        "The tolerance of the reconcile check is looked up by column name, while the tyre manifests write it by measure name. For those manifests the tolerance is 0 either way, so nothing shows today.",
    ])
    return out


# ------------------------------------------------------------------------------------------------------- dimensions
@page("gold-dimensions", "Dimensions: from silver's history to members with a validity span", parent="gold",
      short="Dimensions",
      blurb="Every silver version becomes one member. No code per dimension: the common pipeline does it all.")
def gold_dimensions():
    out = ("<p class='lead'>A dimension describes the things a fact is about: dealers, materials, presses. In "
           "gold a dimension keeps history, so a question about last March can use the dealer's region as it "
           "was last March.</p>"
           "<p>Silver already keeps that history as type 2: one row per version of a business key, each with "
           "the time span in which it was true. The gold dimension is that silver table reshaped. One function "
           "does this for every dimension, and a dimension's manifest is not allowed to bring its own code.</p>")
    d = box(10, 40, 230, 70, "Silver version 1", ["D-1, north, from 9 Nov"], "bx")
    d += box(10, 130, 230, 70, "Silver version 2", ["D-1, south, from 11 Nov"], "bx")
    d += box(330, 85, 190, 70, "Common pipeline", ["key, span, columns"], "solid")
    d += box(610, 10, 370, 60, "Member 1: north", ["from the beginning to 11 Nov"], "bxs")
    d += box(610, 85, 370, 60, "Member 2: south", ["from 11 Nov to the end of time"], "bxs")
    d += box(610, 160, 370, 60, "Reserved: -1 unknown, -2 not applicable", ["always present"], "bxm")
    d += arrow(240, 75, 328, 110) + arrow(240, 165, 328, 130)
    d += arrow(520, 110, 608, 45) + arrow(520, 120, 608, 115) + arrow(520, 130, 608, 185)
    out += fig(990, 235, d, "Two silver versions of one dealer become two members",
               "Each silver version becomes one member. The first member's span is stretched back to the beginning of time. Two reserved members are added.")
    out += flow3("Every row silver holds for the dimension's one input, with silver's own columns "
                 "<code>_valid_from</code>, <code>_valid_to</code>, <code>_is_current</code> and <code>_is_deleted</code>.",
                 "Each version gets a stable integer key and a validity span. The contract's columns are kept, in "
                 "contract order. Two reserved members are added. The keys are checked to be unique.",
                 "The whole dimension, written in one transaction in place of the old one.")
    out += "<h2>A worked example</h2>"
    out += ("<p>Dealer D-1 moved from the north region to the south on 11 November. Dealer D-2 has one version. "
            "The first table is silver. Each row is one version.</p>")
    out += table(["dealer_code", "region", "_valid_from", "_valid_to", "_is_current"], [
        ["D-1", "north", "2026-11-09 00:00", "2026-11-11 00:00", "false"],
        ["D-1", "south", "2026-11-11 00:00", "(open)", "true"],
        ["D-2", "west", "2026-11-09 10:00", "(open)", "true"]])
    out += ("<p>The second table is the gold dimension. Each row is one member. The keys are the real numbers "
            "the recipe gives for these inputs.</p>")
    out += table(["dealer_sk", "dealer_code", "region", "valid_from", "valid_to", "is_current"], [
        ["80631743438072549", "D-1", "north", "1900-01-01 00:00", "2026-11-11 00:00", "false"],
        ["66182239882639827", "D-1", "south", "2026-11-11 00:00", "9999-12-31 00:00", "true"],
        ["1003977605158419583", "D-2", "west", "1900-01-01 00:00", "9999-12-31 00:00", "true"],
        ["-1", "unknown", "unknown", "1900-01-01 00:00", "9999-12-31 00:00", "true"],
        ["-2", "not applicable", "not applicable", "1900-01-01 00:00", "9999-12-31 00:00", "true"]])
    out += ("<p>Three things changed between the two tables. Each version has a key. The first version of each "
            "dealer now starts in 1900, not on the day its record arrived. An open end became the year 9999, so "
            "a join never has to deal with a missing value.</p>")
    out += "<h2>The settings that drive it</h2>"
    out += table(["Where", "Key", "What the pipeline takes from it"], [
        ["Gold manifest", "<code>grain</code>", "The name of the key column, for example <code>dealer_sk</code>."],
        ["Gold manifest", "<code>inputs[0].dataset</code>", "The silver table to reshape. The key recipe also uses this name."],
        ["Contract", "the columns", "Which columns the dimension has, their types and their order."],
        ["Metadata store", "the input's primary key", "The business key, for example <code>dealer_code</code>."],
        ["Silver", "<code>history</code> setting of the input", "Whether silver kept versions at all. A silver table with type 2 history gives several members per key. One with type 1 history gives one."],
    ])
    out += "<h2>The code</h2>"
    out += code(SPARK, r"    def dimension\(self, plan", caption="The common pipeline: the whole of how a dimension is shaped.")
    out += ladder([
        ("Number the versions", "Within each business key the versions are numbered by their start. The number is only used to find the first one."),
        ("Compute the key", f"The key is computed from the input's name, the business key and the version's start. {link('gold-keys')} explains the recipe."),
        ("Set the span", "<code>valid_from</code> is silver's start, except for version number 1, which gets the beginning of time. <code>valid_to</code> is silver's end, or the end of time when silver's end is open."),
        ("Keep the contract's columns", "Each contract column is taken from silver and cast to the contract's type. The four columns the pipeline computes itself replace silver's."),
        ("Keep three extra columns", "<code>_source_valid_from</code> keeps the real start, <code>_is_deleted</code> keeps silver's delete mark, and <code>_run_id</code> names the run."),
        ("Add the reserved members", "Two rows are appended with keys -1 and -2."),
    ])
    out += note("Why the first version reaches back to 1900",
                "<p>A dealer's first record can arrive after the dealer's first order. If the first member only "
                "started when its record arrived, that early order would find no member whose span contains its "
                "time and would stay on the unknown member for ever. Stretching the first version back means an "
                "event before a member's first version joins that first version. The real start is not lost. It "
                "is kept in <code>_source_valid_from</code>, and the key is still computed from it.</p>")
    out += code(SPARK, r"    def _reserved\(self, plan", caption="The two reserved members. Text columns hold the words unknown or not applicable, whole numbers hold 0.")
    out += code(HARNESS, r"^def _dimension\(", caption="The harness side: shape, check the keys are unique, replace the whole table, record.")
    out += ("<p>The harness reads silver, calls the pipeline, and counts duplicate keys. A duplicate means two "
            "versions got the same key, which happens only if silver's history is broken or two different texts "
            "hashed to the same number. Either way the step <code>detect.KEY-01</code> is recorded as failed "
            "and the dimension is not written. Otherwise the whole table is replaced.</p>")
    out += code(SPARK, r"    def write_dimension\(self, plan: GoldPlan, rows: DataFrame",
                caption="The write edge for a dimension on Delta: one overwrite of the whole table.")
    out += ("<p>Replacing the whole table every run is safe because the keys are computed from the data. A "
            "version that did not change gets the same key it had yesterday, so every fact row that points at "
            "it still points at it.</p>")
    out += "<h2>The date dimension is generated, not read</h2>"
    out += (f"<p>No source sends a calendar. The platform makes one row per day itself, with calendar and fiscal "
            f"columns, in {src(CALENDAR)}. It is not built by the harness and has no manifest. "
            f"{link('gold-windows')} shows the code.</p>")
    out += "<h2>How it is tested</h2>"
    out += (f"<p>{src(T_ENGINE)} runs the pipeline on the three silver versions of the worked example above and "
            "checks each member against the Python key recipe.</p>")
    out += code(T_ENGINE, r"^VERSIONS = \[", 3, "The silver rows the test starts from. T0 is 9 November 2026 at midnight UTC, and ts(48) is 48 hours later.")
    out += code(T_ENGINE, r"\(\"the first version reaches back to the beginning\"", r"assert \(row\[\"dealer_code\"\]",
                "One case per member: its key from the Python recipe, and the span it must have.")
    out += code(T_ENGINE, r"^def test_a_rebuild_gives_the_same_keys", caption="The same versions in another order, in another run, give the same keys.")
    out += not_built([
        "The pipeline always makes one member per silver version. The manifest's <code>history</code> key is not "
        "read by the engine, so a dimension cannot ask for only the current version of each key.",
        "A member that was deleted at the source stays in the dimension with <code>_is_deleted</code> set. The key lookup does not look at that flag.",
        "The three extra columns the pipeline keeps are not in the dimension contracts.",
        "A dimension is rebuilt whole on every run, even when nothing in its silver input changed.",
        "A dimension has exactly one input. A dimension that combines two silver tables would need a model, and the plan refuses a model on a dimension.",
        "The date dimension is written by the run script, not by the harness. It is not in the metadata store and has no run log rows.",
    ])
    return out


# ------------------------------------------------------------------------------------------------------- keys
@page("gold-keys", "Stable integer keys, and why gold needs them", parent="gold", short="Keys",
      blurb="A key is computed from the data, so every rebuild gives the same number. Two reserved keys mean unknown and not applicable.")
def gold_keys():
    out = ("<p class='lead'>Every dimension member has one integer key, and every fact row carries the key of "
           "the member it belongs to. The key is not a counter. It is computed from the member's own data, so "
           "rebuilding a table from nothing gives every member the same key it had before.</p>"
           "<p>Without this, a rebuild would renumber the members, and every fact row and every saved report "
           "that holds a key would point at the wrong dealer.</p>")
    d = box(10, 70, 190, 70, "Input name", "dealer_api.dealer", "bx")
    d += box(10, 160, 190, 70, "Business key", "D-1", "bx")
    d += box(10, 250, 190, 70, "Version start", "9 Nov 2026, UTC", "bx")
    d += box(290, 160, 200, 70, "One line of text", "joined with a bar", "bxs")
    d += box(580, 160, 160, 70, "SHA-256", "first 15 hex digits", "solid")
    d += box(830, 160, 150, 70, "Whole number", "under 2 to the 60", "bx")
    d += arrow(200, 105, 288, 180) + arrow(200, 195, 288, 195) + arrow(200, 285, 288, 212)
    d += arrow(490, 195, 578, 195) + arrow(740, 195, 828, 195)
    d += text(495, 30, "The same three inputs always give the same key, on any engine", "tx", "middle")
    out += fig(990, 335, d, "The key recipe", "Three values become one line of text. The text is hashed. The first 15 hex digits of the hash are the key.")
    out += "<h2>Why not use silver's hashes</h2>"
    out += ("<p>Silver already has two hashes. <code>_key_hash</code> is a hash of the business key, and "
            "<code>_row_key</code> identifies one version. Both are SHA-256 values kept as 64 characters of "
            "text. Gold computes its own key for four reasons.</p>")
    out += table(["Reason", "What silver's hash gives", "What gold needs"], [
        ["One key per version", "<code>_key_hash</code> is the same for every version of a dealer.",
         "A fact row must point at one version, the one that was true at the event."],
        ["A whole number", "64 characters of text.",
         "A single whole-number column. It is smaller in every fact row, and joins and group-bys on it are cheaper."],
        ["Reserved members", "A hash has no value that means nothing was found.",
         "Two keys that are never produced by the recipe: -1 for a member that has not arrived, -2 for a row that has no business key."],
        ["A recipe anyone can repeat", "The hash is computed inside the silver stages.",
         "A recipe written as plain text, so Python, Spark and DuckDB all compute the same number and a test can prove it."],
    ])
    out += "<h2>The recipe</h2>"
    out += code(KEYS, r"^def canonical_time\(", caption="A version's start is always written the same way: UTC, microseconds, a Z at the end.")
    out += code(KEYS, r"^def key_text\(", r"return int\(digest", "The text, then the key.")
    out += ("<p>Walk through it. <code>key_text</code> makes a list that starts with the input's name, then "
            "each business key value as text with a missing value written as empty, then the version's start. "
            "The parts are joined with a bar. <code>surrogate_key</code> hashes that text with SHA-256, keeps "
            "the first 15 hex digits, and reads them as a number in base 16.</p>"
            "<p>Fifteen hex digits are 60 bits. That is why 15 was chosen: the number is always positive and "
            "always fits in a 64-bit integer, whatever engine stores it. The input's name is part of the text "
            "so that a material M1 from one system and a material M1 from another never share a key.</p>")
    out += "<h3>A worked example</h3>"
    out += ("<p>Each row is one member of the dealer dimension. The middle column is the text that is hashed. "
            "The right column is the key that results. These are computed with the function above.</p>")
    out += table(["Member", "Text that is hashed", "Key"], [
        ["D-1, first version", "<code>dealer_api.dealer|D-1|2026-11-09T00:00:00.000000Z</code>", "80631743438072549"],
        ["D-1, second version", "<code>dealer_api.dealer|D-1|2026-11-11T00:00:00.000000Z</code>", "66182239882639827"],
        ["D-2, only version", "<code>dealer_api.dealer|D-2|2026-11-09T10:00:00.000000Z</code>", "1003977605158419583"]])
    out += "<h2>The same recipe inside each engine</h2>"
    out += ("<p>The dimension has millions of rows on a real system, so the key is computed by the engine, not "
            "row by row in Python. Each engine adapter repeats the recipe in its own functions.</p>")
    out += code(SPARK, r"^def key_text\(dataset: str, key: list\[Column\]", r"\.cast\(\"long\"\)",
                "On Spark: the same text, the same hash, the same 15 digits, converted from base 16 to base 10.")
    out += code(DUCK, r"^def surrogate\(", caption="On DuckDB there is no base conversion function, so the 15 digits are added up one by one.")
    out += ("<p>The DuckDB version looks different but does the same sum. It takes each of the 15 characters, "
            "turns the character into its value from 0 to 15 using its character code, multiplies by the right "
            "power of 16, and adds the 15 results.</p>")
    out += "<h2>The reserved members</h2>"
    out += code(KEYS, r"^def member\(", caption="The rule for the key a fact row points at.")
    out += table(["Situation", "Key", "What happens next"], [
        ["The fact row's dealer matched a member", "The member's key", "Nothing. This is the normal case."],
        ["The fact row has a dealer code, but no member matched", "-1, unknown",
         "The row is published and counted as an orphan. Its window is rebuilt on every later run until the member arrives, whatever the lookback."],
        ["The fact row has no dealer code at all", "-2, not applicable", "The row is published. There is nothing to wait for."],
    ])
    out += (f"<p>In the engines this rule is written as an expression inside the key lookup, which "
            f"{link('gold-harness')} shows. The published windows that still hold a -1 are found by "
            "<code>windows_with_unknown</code> and added to the range of every run.</p>")
    out += code(SPARK, r"    def windows_with_unknown\(self, plan", caption="Published windows that still hold a row on the unknown member.")
    out += "<h2>Collisions</h2>"
    out += ("<p>Two different texts can in principle give the same 15 digits. The recipe does not prevent "
            "that. The harness detects it: after shaping a dimension it counts duplicate keys, and if there is "
            "even one, the dimension is not written and the step <code>detect.KEY-01</code> is recorded as "
            "failed.</p>")
    out += "<h2>The settings that drive it</h2>"
    out += table(["Where", "Key", "Effect"], [
        ["Fact manifest", "<code>dimensions[].business_key</code>", "The fact columns compared with the member's business key."],
        ["Fact manifest", "<code>dimensions[].as_of</code>", "The fact column whose time picks the version."],
        ["Fact manifest", "<code>checks.orphan_rate_max</code>", "The share of rows on -1 above which the build fails."],
        ["Keys module", "<code>HEX_DIGITS</code>, <code>SEP</code>, <code>TS_FORMAT</code>", "15, a bar, and the time format. These are constants, not settings."],
    ])
    out += "<h2>How it is tested</h2>"
    out += (f"<p>{src(T_PLAN)} tests the Python recipe with a table of pairs that must or must not give the "
            f"same key. {src(T_ENGINE)} then tests the as-of lookup on the engine, and the same table of cases "
            "runs on Spark and on DuckDB.</p>")
    out += code(T_PLAN, r"\(\"same inputs, same key\"", r"assert \(K\.surrogate_key\(\*a\)",
                "Pairs of inputs, and whether the two keys must be equal.")
    out += code(T_ENGINE, r"\(\"an event in the first version\"", r"assert row\[\"dealer_sk\"\] == key",
                "One fact row per case: a dealer, a time, and the key it must get. V1 and V2 are the two versions of D-1.")
    out += ("<p>The third and fourth cases are the two edges. An event exactly at the change gets the new "
            "version, because the span is open at its end. An event 100 hours before the dealer's first record "
            "still gets the first version, because that version reaches back to 1900.</p>")
    out += code(T_HARNESS, r"^def test_a_late_member_is_rekeyed_outside_the_lookback",
                caption="Dealer D-3 arrives days after its order. The order's window is rebuilt although it is outside the 5-day lookback.")
    out += ("<p>In the recorded 14-day run at one tenth of full scale (5 November 2026), a new dealer's record "
            "arrived late on purpose. Six of its order lines sat on the unknown member on day 11 and ten on day "
            f"12, and none on day 13 after the record arrived. No row was dropped on any day. The table is in {src(NOTE)}.</p>")
    out += not_built([
        "A collision is detected, not resolved. If one ever happens the dimension stops being written until someone changes the recipe.",
        "The Python function <code>member</code> states the rule and is tested, but the engines do not call it. They repeat the rule as an engine expression.",
        "The manifest's <code>keys.unknown_member</code> and <code>keys.not_applicable</code> values are not used by the engine. The reserved keys are always -1 and -2.",
        "The specification does not say how an event before a member's first version should join. Joining the first version is a decision taken in the code.",
    ])
    return out


# ------------------------------------------------------------------------------------------------------- facts
@page("gold-facts", "The facts the harness builds", parent="gold", short="Facts",
      blurb="Three tyre facts with three shapes: one row per order line, one per month, one per hour. Each is one model.")
def gold_facts():
    out = ("<p class='lead'>A fact holds the numbers people add up. The harness builds every fact the same "
           "way: it calls the fact's model over the silver inputs, and replaces the windows whose content "
           "changed.</p>"
           "<p>A reader coming from other platforms may expect a setting that names the kind of fact, such as "
           "transaction, periodic snapshot or accumulating snapshot. This harness has no such setting. There is "
           "one kind of fact build. What makes facts differ is the model, the grain and the window. The tyre "
           "company has three facts, and between them they cover three common shapes.</p>")
    d = box(10, 30, 300, 90, "fact_otif", ["one row per open order line", "window: promised date, day"], "bx")
    d += box(345, 30, 300, 90, "fact_sales", ["one row per dealer, material, month", "window: sale month"], "bx")
    d += box(680, 30, 300, 90, "fact_scrap", ["one row per press, material, hour", "window: cure hour"], "bx")
    d += box(345, 175, 300, 70, "The same harness", "keys, windows, checks, replace", "solid", href="gold-harness.html")
    d += arrow(160, 122, 400, 173) + arrow(495, 122, 495, 173) + arrow(830, 122, 590, 173)
    out += fig(990, 260, d, "Three facts, one harness", "Each top box is one model written by a person. All three go through the same highlighted box.")
    out += "<h2>The three facts side by side</h2>"
    out += ("<p>Each row is one fact. The columns say what one row of the fact means, which silver tables it "
            "reads, the window it is rebuilt by with the lookback in brackets, and what kind of model it has.</p>")
    out += table(["Fact", "One row means", "Inputs", "Window (lookback)", "Model", "Shape"], [
        ["<code>fact_otif</code>", "One open order line, judged on time and in full",
         "order lines, deliveries", "promised date, by day (35)", "SQL",
         "A row that is updated as its story unfolds: ordered, then delivered"],
        ["<code>fact_sales</code>", "Units sold in and sold out for one dealer, material and month",
         "order lines, dealer sell-out files", "sale month (3)", "SQL", "A total per period"],
        ["<code>fact_scrap</code>", "Tyres cured and scrapped on one press, for one material, in one hour",
         "production, inspection results", "cure hour (48)", "dataframe function", "A total per period, with rows that can still be waiting"],
    ])
    out += "<h2>How a model is called</h2>"
    out += code(SPARK, r"    def build\(self, plan: GoldPlan, inputs", caption="The harness calls the model through one method, whatever kind it is.")
    out += ("<p>For a SQL model, each input frame is registered as a view under its name, the file is read, "
            "and the query is run with the parameters the text actually uses. The parameters are bound by the "
            "engine, never pasted into the text. For a dataframe model, the function is loaded from the models "
            "folder and called with the frames and the parameters. Both kinds return one frame and neither may "
            "read or write anything.</p>")

    out += "<h2>fact_otif: a row that changes as deliveries arrive</h2>"
    out += ("<p>OTIF stands for on time in full. One row is one open order line. It says whether the line was "
            "delivered by the date first promised, and whether the full quantity arrived.</p>")
    out += code(MODELS + "sales/fact_otif.sql", r"^judged AS \(", r"^FROM judged", "The judging step and the final select of the model.")
    out += ("<p>The row for an order line exists as soon as the order does, with a delivered quantity of 0 and "
            "both flags false. When deliveries arrive, the same row is rebuilt with the new quantity and "
            "flags. A delivery counts on its local date: the delivery time is moved to the business time zone "
            "before it is compared with the promised date. Cancelled and rejected lines are left out, because "
            "the earlier step keeps only lines whose status is open.</p>")
    out += "<p>An example. The order line and its deliveries are on the left. The fact row is on the right.</p>"
    out += table(["Order line", "Deliveries", "delivered_qty", "on_time", "in_full", "on_time_in_full"], [
        ["A, 10 tyres, promised 12 Nov", "10 on 10 Nov", "10", "true", "true", "1"],
        ["A, 10 tyres, promised 12 Nov", "10 on 13 Nov", "10", "false", "true", "0"],
        ["A, 10 tyres, promised 12 Nov", "6 on 10 Nov", "6", "true", "false", "0"],
        ["A, 10 tyres, promised 12 Nov", "6 and then 4, both before 12 Nov", "10", "true", "true", "1"],
        ["A, 10 tyres, promised 12 Nov", "none yet", "0", "false", "false", "0"],
        ["A, first promised 12 Nov, later moved to 14 Nov", "10 on 13 Nov", "10", "false", "true", "0"]])
    out += ("<p><b>How it is rebuilt.</b> The window is the promised date, one day at a time, and each run "
            "recomputes the last 35 days of promised dates. A delivery that arrives three weeks after its "
            "promised date still lands in the right day. A promised date in the future is published but flagged "
            "incomplete, because the day is not over. The fact allows corrections, so a recorded correction to "
            "a promised date is laid over silver before the model runs.</p>")

    out += "<h2>fact_sales: a total per month from two sources</h2>"
    out += ("<p>One row is one dealer, one material and one month. It holds what the company sold to the "
            "dealer, called sell-in, and what the dealer reported selling on, called sell-out.</p>")
    out += code(MODELS + "sales/fact_sales.sql", r"^WITH sell_in AS", r"ON i\.dealer_code = o\.dealer_code",
                "Two totals per dealer, material and month, joined so that a row exists if either side has one.")
    out += ("<p>Sell-in comes from order lines, counted in the local month of the order time. Sell-out comes "
            "from the file each dealer sends once a month. A dealer who resends a month replaces that month in "
            "silver, so the model reads only the current rows. The two sides are joined with a full outer join. "
            "A dealer with orders but no file yet still gets a row, with sell-out 0, and the other way round.</p>")
    out += "<p>An example. The inputs are on the left and the fact row on the right.</p>"
    out += table(["Order lines", "Dealer file", "units_sold_in", "units_sold_out"], [
        ["D-1, M-1, 7 tyres and 5 tyres in November", "none", "12", "0"],
        ["D-1, M-1, 7 tyres in November", "D-1, M-1, November, 3 sold", "7", "3"],
        ["none", "D-2, M-2, October: an older file with 50, then a newer one with 60", "0", "60"]])
    out += ("<p><b>How it is rebuilt.</b> The window is the sale month and each run recomputes the last 3 "
            "months. A month stays flagged incomplete until every dealer who owes a file for it has sent one. "
            f"When the last file arrives, the month is replaced and logged as a restatement. {link('gold-windows')} "
            "follows that case through.</p>")

    out += "<h2>fact_scrap: a total per hour, where single rows can wait</h2>"
    out += ("<p>One row is one press, one material and one hour. It holds the tyres cured and the tyres "
            "scrapped at inspection. Reworked tyres are not scrap.</p>")
    out += code(MODELS + "manufacturing/scrap.py", r"^def build\(", caption="The one dataframe model of the tyre company.")
    out += ("<p>Production rows are joined to the inspection results by a left join, so an hour of production "
            "appears even before its inspection is filed. The last aggregate is the interesting one. It "
            "computes <code>_input_complete</code>, which is true only if every production row of that press "
            "and hour has an inspection result. The harness reads that column. A row that says it is still "
            "waiting is flagged incomplete on its own, even if its window is otherwise complete. The column "
            "is not in the contract and is dropped before the write.</p>")
    out += "<p>An example. The inputs are on the left and the fact row on the right.</p>"
    out += table(["Production in the hour from 10:00", "Inspection results", "tyres_cured", "tyres_scrapped", "_input_complete"], [
        ["Press X: 30 tyres", "2 scrapped", "30", "2", "true"],
        ["Press X: 30 tyres", "not filed yet", "30", "0", "false"],
        ["Press X: 30 tyres", "0 scrapped, result rework", "30", "0", "true"],
        ["Press X: 30 tyres and 20 tyres", "1 scrapped for the first batch only", "50", "1", "false"]])
    out += ("<p><b>How it is rebuilt.</b> The window is the cure hour, an instant in UTC, and each run "
            "recomputes the 48 hours up to the end of the business date. This fact does not allow corrections.</p>")
    out += note("Why the row-level flag exists",
                "<p>The full-scale attempt of 4 November 2026 found the reason. One plant's lab files were held "
                "back by a planted incident, and the whole hour was flagged incomplete, including the rows of "
                "another plant whose files had arrived. The numbers were equal to the oracle and only the flag "
                "differed. The fix was to let a model say per row whether its own input has arrived. The run "
                f"of 5 November at one tenth of full scale confirmed it. Both are recorded in {src(NOTE)}.</p>")

    out += "<h2>The settings that drive a fact</h2>"
    out += (f"<p>All of them are in the fact's manifest. {link('gold-plan')} lists every key. The ones that "
            "decide how a fact behaves are these.</p>")
    out += table(["Key", "fact_otif", "fact_sales", "fact_scrap"], [
        ["<code>grain</code>", "order_id, line_no", "dealer_code, material_code, sale_month", "press_id, material_code, cure_hour"],
        ["<code>window</code>", "promised_date, day, 35", "sale_month, month, 3", "cure_hour, hour, 48"],
        ["<code>dimensions[].as_of</code>", "order_ts", "sale_month", "cure_hour"],
        ["<code>build.kind</code>", "sql", "sql", "dataframe"],
        ["<code>corrections</code>", "true", "true", "false"],
        ["<code>checks.orphan_rate_max</code>", "0.01", "0.01", "0.005"],
        ["<code>checks.reconcile</code>", "ordered_qty", "units_sold_out", "tyres_cured, tyres_scrapped"],
    ])
    out += "<h2>How it is tested</h2>"
    out += (f"<p>Each model has its own table of cases in {src(T_ENGINE)}: 11 for on time in full, 5 for sales "
            "and 6 for scrap. A case is a few silver rows going in and the fact rows expected out. The same "
            "tables run on Spark and on DuckDB.</p>")
    out += code(T_ENGINE, r"^OTIF_CASES = \{", r"^\}", "The cases of the on-time-in-full model. Each is order lines, deliveries, and the expected row.")
    out += code(T_ENGINE, r"^def test_fact_otif_build", caption="The test hands the model two silver frames and compares what comes back.")
    out += code(T_ENGINE, r"^SCRAP_CASES = \{", r"^\}", "The cases of the scrap model, including an hour that waits because one of its rows waits.")
    out += not_built([
        "There is no setting for the kind of fact, and no build that appends new rows only. Every fact is recomputed by window and replaced. A very large fact of pure events would pay for that.",
        "No fact reads the press readings stream. A press-hour fact from readings would be a fourth model with an hour window.",
        "The cure hour is the UTC hour of the curing time. The plants' time zones are declared nowhere the platform reads, so plant-local hours and shifts cannot be built.",
        "Net sales value is built into <code>fact_sales</code>, but its measure in the semantic layer is still a draft.",
        "A model's purity is not enforced at run time. It is checked by a static scan of the models folder before a change is merged.",
    ])
    return out


# ------------------------------------------------------------------------------------------------------- windows
@page("gold-windows", "Windows: what is rebuilt, what is complete, and what counts as a restatement", parent="gold",
      short="Windows and completeness",
      blurb="The lookback, completeness from input watermarks, late data, corrections, withdrawn batches, and the date dimension.")
def gold_windows():
    out = ("<p class='lead'>A window is one slice of a fact along its time column: one day of promised dates, "
           "one month of sales, one hour of curing. The harness thinks about a fact one window at a time. This "
           "page answers four questions about a window. Is it rebuilt in this run? Is it complete? Did its "
           "numbers change after readers were told they were final? What happens when a batch is withdrawn?</p>"
           "<p>Without these rules, a dashboard would show a half-loaded day as if it were final, and a number "
           "that changed overnight would change without any record that it did.</p>")
    d = ""
    days = [("18 Oct", "bxs"), ("...", "bxm"), ("20 Nov", "bxs"), ("21 Nov", "bxs"), ("22 Nov", "solid"),
            ("23 Nov", "bxm"), ("24 Nov", "bxm")]
    for i, (n, k) in enumerate(days):
        d += box(20 + i * 137, 80, 120, 60, n, "", k)
    d += arrow(80, 36, 80, 76, "first window in range", lx=90, ly=50, anchor="start")
    d += arrow(628, 36, 628, 76, "business date of the run", lx=638, ly=50, anchor="start")
    d += text(350, 175, "Rebuilt and can be complete: inputs have loaded past these days", "sm", "middle")
    d += text(830, 175, "Rebuilt, flagged incomplete", "sm", "middle")
    out += fig(990, 195, d, "The range of one run of a fact with a 35-day lookback",
               "A run for 22 November starts 35 days back. Windows after the business date are built too, but they cannot be complete yet.")
    out += "<h2>Windows are named by text</h2>"
    out += ("<p>The harness never handles the engine's date types. It names a window by a piece of text. A day "
            "is <code>2026-11-12</code>. A month is its first day, <code>2026-10-01</code>. An hour is its start "
            "in UTC, <code>2026-11-12T05:00:00Z</code>. A day or a month starts at midnight in the business "
            "time zone. An hour is an instant and has no zone.</p>")
    out += code(WINDOWS, r"^def end\(", caption="The first instant after a window. Completeness is decided against this.")
    out += code(WINDOWS, r"^def of\(", caption="The window an instant falls in.")
    out += "<h2>Which windows a run rebuilds</h2>"
    out += code(WINDOWS, r"^def range_start\(", caption="The first window of the range: the lookback counted back from the business date.")
    out += ("<p>For days and months the lookback is counted back from the business date itself. For hours it "
            "is counted back from the end of the business date, so a 48-hour lookback covers the business date "
            "and the day before it. The table shows the three tyre facts for a run on 22 November 2026, with the "
            "business time zone of India. The values are the ones the test table checks.</p>")
    out += table(["Fact", "Unit and lookback", "First window rebuilt"], [
        ["<code>fact_otif</code>", "day, 35", "<code>2026-10-18</code>"],
        ["<code>fact_sales</code>", "month, 3", "<code>2026-08-01</code>"],
        ["<code>fact_scrap</code>", "hour, 48", "<code>2026-11-20T18:00:00Z</code>"]])
    out += code(HARNESS, r"^def _range\(", caption="The full range: from the first window on, plus named windows outside it.")
    out += ("<p>The range has no upper end. Every window from the first one on is recomputed, including "
            "future ones. On top of that, three kinds of window are added even when they are older than the "
            "lookback.</p>")
    out += table(["Extra window", "Why it is added"], [
        ["A window a correction plan names", "A correction to an old order must reach its window however old it is."],
        ["A published window with a row on the unknown member", "When the missing dealer arrives, the row must be given its real key."],
        ["Every window", "When a batch was withdrawn, any window may have held its rows."],
    ])
    out += ("<p>Recomputing a window does not mean writing it. The write replaces only the windows whose "
            f"content changed, as {link('gold-harness')} explains.</p>")

    out += "<h2>Completeness: is every input in?</h2>"
    out += ("<p>A window is complete when every input of the fact has loaded past the window's end. Until "
            "then its rows carry <code>_is_complete = false</code>. The harness does not look at the data to "
            "decide this. It compares the window's end with each input's watermark, which is the point in "
            "source time up to which silver has taken that input's deliveries.</p>")
    out += code(WINDOWS, r"^def caught_up\(", caption="One input has caught up with a window when its watermark is at or after the window's end.")
    out += code(HARNESS, r"^def _complete\(", caption="A window is complete only if no input is still waited for.")
    out += "<h3>A worked example</h3>"
    out += ("<p>Three orders have promised dates of 9, 10 and 12 November. Silver has taken the order "
            "deliveries for 9 and 10 November. The watermark is therefore the end of 10 November. Each row "
            "below is one window of the fact.</p>")
    out += table(["Window", "Window ends", "Watermark at or after the end?", "_is_complete"], [
        ["2026-11-09", "end of 9 Nov", "yes", "true"], ["2026-11-10", "end of 10 Nov", "yes", "true"],
        ["2026-11-12", "end of 12 Nov", "no", "false"]])
    out += "<h3>Inputs that arrive one file per member</h3>"
    out += ("<p>The dealer sell-out input is different. It is not one delivery a day. It is one file per "
            "dealer per month, and the dealers send them on different days. A single watermark cannot say "
            "whether October is in. So ingestion keeps one bookmark per dealer, holding the newest month that "
            "dealer has sent, and a month is complete when every dealer who owes a file has sent it.</p>")
    out += code(WINDOWS, r"^def feeds_caught_up\(", caption="Complete when every expected feed has reached this window. The feeds still missing are returned.")
    out += code(HARNESS, r"^def _expected_feeds\(", caption="Who owes a file: the members of the fact's dimension, each from its first version on.")
    out += ("<p>The second function answers who owes a file. It looks for a dimension of the fact whose "
            "business keys include every feed name. For the sales fact that is the dealer dimension. Each "
            "dealer owes files from the start of its first version, so a dealer who joined in November owes "
            "nothing for October.</p>")
    out += "<h3>Rows that wait on their own</h3>"
    out += ("<p>A model may return a column named <code>_input_complete</code>. A row where it is false is "
            "flagged incomplete even when its window is complete. The scrap model uses this for production "
            "rows whose inspection result has not been filed.</p>")
    out += code(SPARK, r"    def finish\(self, plan", caption="The flag on each row: the window's flag, and the row's own flag when the model gave one.")
    out += "<h3>Flag or hold</h3>"
    out += ("<p>The manifest key <code>completeness.on_incomplete</code> decides what happens to an incomplete "
            "window. With <code>flag</code> it is published with the flag false, and a reader or a metric "
            "chooses whether to count it. With <code>hold</code> it is not written until it is complete. All "
            "three tyre facts use <code>flag</code>.</p>")

    out += "<h2>Late data and restatements</h2>"
    out += ("<p>A restatement is a change to a number that readers were entitled to treat as final. Not every "
            "change is one. A day that fills up during the day is only filling. The rule is in one function.</p>")
    out += code(WINDOWS, r"^def is_restatement\(", caption="When a rebuild counts as a restatement.")
    out += table(["Before the run", "After the run", "Restatement?", "Why"], [
        ["Not published", "Anything", "No", "Nobody saw an earlier number."],
        ["Complete", "Complete, different numbers", "Yes", "A final number changed."],
        ["Complete", "No rows left", "Yes", "What readers saw is gone."],
        ["Incomplete, the period was over, a member's file was missing", "Complete, different numbers", "Yes", "The period had ended, so readers took the number as the month's number."],
        ["Incomplete, waiting for an input's ordinary delay", "Complete, different numbers", "No", "The number was provisional and said so."],
        ["Incomplete", "Still incomplete", "No", "Still provisional."],
        ["Incomplete, the period was still open", "Complete", "No", "An open period completing is normal."],
        ["Incomplete", "Complete, same numbers", "No", "Only the flag moved."],
    ])
    out += "<h3>The late dealer file, followed through</h3>"
    out += ("<p>In the simulated tyre company, the dealers' October sell-out files arrive over the first five "
            "days of the run, and one is late on purpose. In the recorded run of 5 November 2026 at one tenth "
            "of full scale, October was flagged incomplete on days 1 to 4. On day 5 the last file arrived, the "
            "month became complete, and it was replaced and logged as a restatement: 7,392 rows became 9,404 "
            "and 602,101 units sold out became 765,689. On day 6 a dealer resent a file with the same content. "
            f"Nothing was replaced and nothing was logged. The table is in {src(NOTE)}.</p>")
    out += code(HARNESS, r"^def _record\(", caption="What is written to the run log about corrections, restatements and completeness.", cap=25)
    out += ("<p>Each restated window gets one <code>gold.restatement</code> row with every number before and "
            "after. The operations layer reads those rows into a table of restatements with one row per window "
            f"and measure. {link('obs')} covers that table.</p>")

    out += "<h2>Corrections</h2>"
    out += ("<p>Sometimes a value in the source was simply wrong and someone corrects it after the fact, for "
            "example a promised date that was keyed in a day early. The platform does not edit silver. The "
            "correction is recorded in the metadata store, and the gold harness lays it over the silver rows in "
            "memory before the model runs.</p>"
            "<p>Corrections come in a plan. A plan names the dataset, the date from which its corrections are "
            "active, the windows it expects to change, a reason, and who recorded it.</p>")
    out += code("P/src/dataplatform/meta/port.py", r"^class BackfillPlan:", r"status: str = \"recorded\"",
                "A plan and one correction, as the metadata store hands them to the harness.", cap=30)
    out += code(HARNESS, r"^def _corrections\(", caption="Which corrections are applied to one input in this run.")
    out += ("<p>The rule in the middle is the important one. If a correction changes the fact's own window "
            "column, it moves a row from one window to another. It is applied only if the plan names both the "
            "old window and the new one. Otherwise it is skipped and counted in "
            "<code>corrections_not_applied</code>. A correction can therefore never change a window that the "
            "person who approved the plan did not expect to change.</p>")
    out += code(SPARK, r"    def overlay\(self, frame", caption="Laying corrections over silver rows: a small table of fixes joined on the business key.")
    out += ("<p>In the recorded run, a planted correction to promised dates became active on day 11. The plan "
            "was applied on that day, the windows from 13 to 18 November were restated, and no window outside "
            "the plan changed.</p>")

    out += "<h2>A withdrawn batch</h2>"
    out += ("<p>A batch that turns out to be bad after it was published can be withdrawn. Withdrawing is also "
            "called voiding. It marks the batch void, so no reader takes it again, and records a plan whose "
            "window list is a star, meaning every window.</p>")
    out += code("P/src/dataplatform/recovery.py", r"^def withdraw\(", caption="Withdrawing a batch: void it, and record a plan that names every window.")
    out += ladder([
        ("Withdraw", "The batch is voided with a reason and the name of who approved it."),
        ("Rebuild silver", "Silver for that dataset is rebuilt from bronze. The reader skips the void batch, so its rows are gone."),
        ("Run gold as usual", "Every fact that reads the dataset sees the plan, sets its range to every window, recomputes all of them, and replaces only the ones whose content changed."),
        ("Read the log", "Each changed window that had been complete is logged as a restatement whose reason names the plan."),
    ])
    out += "<p>No row is deleted by hand at any point. The withdrawn rows leave gold because the rebuild no longer produces them.</p>"

    out += "<h2>The settings that drive it</h2>"
    out += table(["Where", "Key", "What it means", "Values in the tyre example"], [
        ["Fact manifest", "<code>window.column</code>", "The column a fact is sliced by.", "promised_date, sale_month, cure_hour"],
        ["Fact manifest", "<code>window.unit</code>", "The size of one window.", "day, month, hour"],
        ["Fact manifest", "<code>window.lookback</code>", "How many windows back each run recomputes. Default 0.", "35, 3, 48"],
        ["Fact manifest", "<code>completeness.on_incomplete</code>", "Publish an incomplete window flagged, or hold it back. Default flag.", "flag"],
        ["Fact manifest", "<code>corrections</code>", "Whether corrections are laid over the inputs. Default false.", "true, true, false"],
        ["Domain", "<code>business_timezone</code>", "Where a day and a month start.", "Asia/Kolkata"],
        ["Run", "the business date", "The date the run is for. The range is counted back from it.", "one per day"],
        ["Metadata store", "silver's bookmark and the file log", "The watermark of each input.", ""],
        ["Metadata store", "per-feed bookmarks", "The newest month each dealer has sent.", ""],
    ])

    out += "<h2>The date dimension</h2>"
    out += ("<p>Questions about windows usually need a calendar: which fiscal quarter a day is in, whether it "
            "was a working day. No source sends one, so the platform generates it. It is one row per day, keyed "
            "by the date and by a number such as 20261113. A fact can join it on any of its date columns "
            "without any change to the fact.</p>")
    out += code(CALENDAR, r"^def fiscal\(", caption="The fiscal year, quarter and month of a day, for a fiscal year that starts in a given month.")
    out += ("<p>The fiscal year is named by the calendar year it ends in. With a start in April, 1 April 2026 "
            "is the first day of fiscal year 2027, and 14 November 2026 is in fiscal quarter 3, fiscal month "
            "8.</p>")
    out += code(CALENDAR, r"^def calendar_rows\(", caption="One row per day. Holidays are a list the caller passes in. Nothing is guessed.")
    out += ("<p>The run script writes the calendar whole on every run, for five years either side of the "
            "simulated company's start date, with the fiscal year starting in April.</p>")

    out += "<h2>How it is tested</h2>"
    out += (f"<p>The window arithmetic, completeness and the restatement rule are pure functions, so "
            f"{src(T_PLAN)} tests them as tables with no engine. {src(T_HARNESS)} then runs the late file, the "
            f"correction and the withdrawn batch through the real harness. "
            f"{src('P/tests/dataplatform/test_gold_calendar.py')} covers the calendar, and "
            f"{src('P/tests/dataplatform/test_ops_restatements.py')} covers the table of restatements.</p>")
    out += code(T_PLAN, r"\(\"first publication\", None", r"assert W\.is_restatement\(",
                "The restatement rule as a table: numbers before, numbers after, when it was last published, whether a feed was waited for, and the verdict.")
    out += code(T_HARNESS, r"^def test_a_late_member_file_completes_and_restates_the_month",
                caption="Dealer D-1 sends October on day 1 and D-2 on day 5. The month is incomplete, then complete and restated from 5 units to 12. A resend changes nothing.")
    out += code("P/tests/dataplatform/test_ops_restatements.py", r"^@pytest\.mark\.parametrize\(\(\"case\", \"steps\", \"want\"\)", r"^\]\)",
                "From run log rows to the table of restatements: one row per window and measure, with the change.")
    out += not_built([
        "Completeness only knows deliveries by business day. An input that silver loads several times a day still counts as loaded only through the end of the newest whole day it has taken.",
        "Whether an input arrives one file per member is inferred from ingestion's bookmarks. No manifest key declares it.",
        "A day on which each dealer's file is due is not known to the platform. October is flagged until the last dealer has sent, with no notion of a file being overdue.",
        "The specification says restatements are logged but not when a rebuild is one. The rule on this page is a decision taken in the code.",
        "A restatement caused by a withdrawn batch is recorded under the same detector name as a correction plan. Only the reason text tells them apart.",
        "Restatements live in the run log. There is no separate restatement table in the metadata store.",
        "The date dimension has no holidays for the tyre company. The run script passes none.",
        "Plant time zones are not declared anywhere the platform reads, so an hour window is always a UTC hour.",
    ])
    return out
