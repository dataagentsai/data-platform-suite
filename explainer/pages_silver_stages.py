"""Bronze to silver: the thirteen stages hub, the stage runner, and one page per stage (stage 12, history, is
owned by pages_silver_history.py and only linked from here)."""
from lib import *  # noqa: F401,F403

CLEAN = "P/src/dataplatform/stages/clean.py"
ROWS = "P/src/dataplatform/stages/rows.py"
COMMON = "P/src/dataplatform/stages/common.py"
INIT = "P/src/dataplatform/stages/__init__.py"
PRIV = "P/src/dataplatform/stages/privacy.py"
PLAN = "P/src/dataplatform/silver/plan.py"
RULES = "P/src/dataplatform/silver/contract_rules.py"
RUNNER = "P/src/dataplatform/silver/runner.py"
EDGE = "P/src/dataplatform/adapters/spark_silver.py"
DUCK = "P/src/dataplatform/adapters/duckdb_silver.py"
KEYED = "P/src/dataplatform/adapters/keyed_tokens.py"
PORT = "P/src/dataplatform/meta/port.py"
LOADER = "P/src/dataplatform/meta/loader.py"
REGISTRY = "P/src/dataplatform/meta/settings_registry.json"
NOTE = "P/docs/W1C-SILVER.md"
SPIKE = "P/docs/DUCKDB-SPIKE.md"
T_STAGES = "P/tests/dataplatform/test_silver_stages.py"
T_RULES = "P/tests/dataplatform/test_contract_rules.py"
T_PRIV = "P/tests/dataplatform/test_privacy_stage.py"
T_RUNNER = "P/tests/dataplatform/test_silver_runner.py"
T_PORTS = "P/tests/dataplatform/test_silver_ports.py"
TYRE_M = "C/slices/02-tyre/manifest/datasets/"
TYRE_C = "C/slices/02-tyre/contracts/"
PLATFORM = "C/slices/02-tyre/manifest/platform.yaml"

NO_COLUMN_SETTINGS = ("<p>One honest remark about the tyre example. None of the nine tyre dataset manifests has a "
                      "<code>columns</code> list today, so every tyre column runs with the defaults. The settings "
                      "below are read by the code and covered by the tests, and the example manifest lines on this "
                      "page are typed for this page, not copied from the tyre manifests.</p>")


def pic(goes_in, stage, outs):
    """The standard stage picture: what goes in, the stage (dark), what comes out. Returns (drawing, height)."""
    n = len(outs)
    h = max(150, 20 + n * 90)
    mid = h / 2
    d = box(10, mid - 40, 240, 80, goes_in[0], goes_in[1], "bx")
    d += box(350, mid - 45, 260, 90, stage[0], stage[1], "solid")
    d += arrow(250, mid, 348, mid)
    for i, (title, sub, kind) in enumerate(outs):
        y = mid - 35 if n == 1 else 15 + i * 90
        d += box(710, y, 270, 70, title, sub, kind)
        d += arrow(610, mid, 708, y + 35)
    return d, h


def switch_row(group):
    return [f"<code>processor_groups.{group}</code>",
            "A dataset setting in the metadata store. The value <code>disabled</code> makes the stage runner skip "
            "this stage for the dataset.",
            "The settings registry allows <code>enabled</code>, <code>disabled</code>, <code>opt_in</code> and "
            "<code>dataset_decides</code>. The plan acts only on <code>disabled</code>.",
            "Not set, so the stage runs"]


# ------------------------------------------------------------------------------------------------------------ hub
def thirteen():
    top = [("1 Contract", "check", "columns present", "bx", "silver-contract-check"),
           ("2 Project", "", "pick and rename", "bx", "silver-project"),
           ("3 Hygiene", "", "clean up text", "bxm", "silver-hygiene"),
           ("4 Parse", "and type", "cast to the type", "bxm", "silver-parse-type"),
           ("5 Time", "", "local time beside", "bxm", "silver-time"),
           ("6 Standardise", "", "maps and case", "bxm", "silver-standardise"),
           ("7 Personal", "data", "tokens only", "bxm", "silver-personal-data")]
    low = [("8 Row filter", "", "drop and count", "bx", "silver-row-filter"),
           ("9 Quality", "", "rules by severity", "bx", "silver-quality"),
           ("10 Duplicates", "", "repeats, replays", "bxm", "silver-duplicates"),
           ("11 Keys", "", "key hash", "bx", "silver-keys"),
           ("12 History", "", "by dataset kind", "bx", "silver-history"),
           ("13 Provenance", "", "where it came from", "bx", "silver-provenance")]
    d = text(10, 24, "Solid outline: always runs. Dashed outline: the dataset may switch it off.", "tx")
    for row, y in ((top, 60), (low, 220)):
        for i, (a, b, sub, kind, pid) in enumerate(row):
            x = 10 + i * 142
            d += box(x, y, 118, 86, [a, b] if b else a, sub, kind, href=f"{pid}.html")
            if i:
                d += arrow(x - 24, y + 43, x - 2, y + 43)
    d += path("M 921 146 L 921 178 L 26 178 L 26 204")
    d += text(480, 172, "then", "sm", "middle")
    return d


@page("silver-stages", "The thirteen stages and the stage runner", parent="silver", short="The thirteen stages",
      blurb="Thirteen plain functions in one fixed order. Six can be switched off per dataset. One runner chains them.")
def silver_stages():
    out = ("<p class='lead'>Between bronze and silver every dataset passes the same thirteen stages in the same "
           "order. A stage is a plain function: it receives rows and the dataset's plan, and it returns rows. It "
           "never reads a table, never writes one and never looks at the clock.</p>"
           "<p>Without a fixed list each table would grow its own cleaning job, and nobody could say what "
           "silver guarantees. With the list, one table of test cases per stage proves the behaviour once, and "
           "the only thing that differs between datasets is their settings.</p>")
    out += fig(1000, 320, thirteen(), "The thirteen stages in order",
               "Read the top row left to right, then the bottom row. Click a box to open that stage's page.")
    out += "<h2>The stages at a glance</h2>"
    out += ("<p>Each line of this table is one stage. The fourth column says whether a dataset may switch the "
            "stage off. The last column says which rows the stage can set aside in the quarantine table. To "
            "quarantine a row means to take it out of the batch and store it with the reason, so that the good "
            "rows go on.</p>")
    out += table(["No.", "Stage", "What it does", "Can be switched off", "What it can quarantine"], [
        ["1", link("silver-contract-check", "Contract check"),
         "Checks that every contract column the plan keeps is present. A missing column stops the dataset run.",
         "No", "Nothing. It stops the whole run instead."],
        ["2", link("silver-project", "Project"),
         "Picks the columns, renames them to their contract names, extracts columns held in <code>_extra</code>, "
         "leaves out excluded columns.", "No", "Nothing."],
        ["3", link("silver-hygiene", "Hygiene"),
         "Cleans text: byte-order mark, control characters, line endings, spaces at the ends, runs of spaces, "
         "empty text to a missing value.", "Yes", "Nothing."],
        ["4", link("silver-parse-type", "Parse and type"),
         "Casts each column to the contract's type. A value that does not convert is never silently lost.",
         "Yes", "Rows with a value that does not convert, unless the column says to empty the value instead."],
        ["5", link("silver-time", "Time"),
         "Adds the local wall-clock time beside a timestamp when the column asks for it.", "Yes", "Nothing."],
        ["6", link("silver-standardise", "Standardise"),
         "Applies declared value maps and upper or lower case.", "Yes", "Nothing."],
        ["7", link("silver-personal-data", "Personal data"),
         "Checks that every restricted column holds tokens, never the plain value.", "Yes",
         "Rows with a plain value in a restricted column."],
        ["8", link("silver-row-filter", "Row filter"),
         "Drops rows that a declared filter does not want, and counts them.", "No",
         "Nothing. Dropped rows are counted, not stored."],
        ["9", link("silver-quality", "Quality"),
         "Runs the row rules. The rule's severity decides whether a failing row is quarantined or kept with a flag.",
         "No", "Rows that break a rule whose action is quarantine. A critical failure also holds the batch."],
        ["10", link("silver-duplicates", "Duplicates"),
         "Removes repeats inside the batch and, for appended events, rows already in silver.", "Yes",
         "Nothing. Dropped and replayed rows are counted."],
        ["11", link("silver-keys", "Keys"),
         "Checks that every part of the business key is present and adds the key hash.", "No",
         "Rows with an empty key part."],
        ["12", link("silver-history", "History"),
         "Lays the rows out as versions, latest state, appended events or snapshots, by the kind of dataset, and "
         "marks deletes.", "No",
         "Only for datasets whose source keeps the versions itself: keys whose versions overlap or leave a gap."],
        ["13", link("silver-provenance", "Provenance"),
         "Stamps the silver run, the load time and a record hash, and puts the columns in a fixed order.", "No",
         "Nothing."],
    ])

    out += "<h2>What a stage is</h2>"
    out += ("<p>Three small definitions in one file describe every stage. <code>StageCtx</code> is what a stage "
            "is told besides the rows. <code>Out</code> is what it gives back. <code>Stage</code> is the shape of "
            "the function itself.</p>")
    out += code(COMMON, r"^class StageCtx:", caption="What every stage receives besides the rows.")
    out += table(["Field", "What it is"], [
        ["<code>plan</code>", f"The dataset's {link('silver-plan', 'plan')}: columns, keys, history kind, rules. "
                              "It is frozen, so no stage can change it."],
        ["<code>current</code>", "The silver table as it stands, handed over by the read edge. It is "
                                 "<code>None</code> before the first write. A stage may join with it. Only the "
                                 "duplicates and history stages do."],
        ["<code>run_id</code>", "The id of this silver run. The provenance stage stamps it on every row."],
        ["<code>as_of</code>", "The business date of the run, as text. The duplicates stage counts its lookback "
                               "back from this date, not from today's date."],
        ["<code>loaded_at</code>", "The load time, read from the clock once by the adapter and passed in. A stage "
                                   "that read the clock itself would give different rows on every run and could "
                                   "not be tested with fixed answers."],
    ])
    out += code(COMMON, r"^class Out:", r"^Stage = ", caption="What every stage returns, and the type of a stage.")
    out += ("<p><code>df</code> is the rows that go on to the next stage. <code>quarantine</code> is the rows "
            "this stage set aside, or nothing. <code>tally</code> is a dictionary of named frames whose rows are "
            "to be counted, for example the rows a filter dropped. A stage hands over frames to count, not "
            "numbers, because counting runs a job, and a pure function should not run jobs. The adapter counts "
            "them all at once at the end.</p>")

    out += "<h2>The fixed order, and why this order</h2>"
    out += code(PLAN, r"^STAGES = \(", r"^\)", "The order is one tuple in the plan module. Nothing else decides it.")
    out += code(INIT, r"^FUNCTIONS: dict", r"^check_stage_order",
                "The engine's functions by stage name. The last line refuses to import if the names or the order differ.")
    out += code(PLAN, r"^def check_stage_order\(", caption="The check that runs at import time.")
    out += ("<p>The order is checked when the module is imported, so a build with a stage missing, added or "
            "moved fails before any data is read. The order itself follows from what each stage needs from the "
            "ones before it. The table gives the reason for each position.</p>")
    out += table(["Position", "What the position gives"], [
        ["Contract check first", "Every later stage addresses columns by name. A missing column must stop the run "
                                 "with a clear message, not fail somewhere in the middle with an engine error."],
        ["Project second", "After it, every column carries its contract name, so all later stages and all rules "
                           "can use contract names only. It also creates the <code>_dq_flags</code> column that "
                           "later stages add to."],
        ["Hygiene before parse and type", "The text <code>' 42 '</code> is trimmed before it is cast to a number, "
                                          "and empty text becomes a missing value before the cast, so neither "
                                          "counts as a failed cast."],
        ["Parse and type before time, standardise and quality", "The time stage needs real timestamps. The rules "
                                                                "in the quality stage compare typed values, for "
                                                                "example a quantity with zero."],
        ["Standardise before quality", "A rule that lists the valid values of a column sees the values after the "
                                       "value map and the letter case were applied."],
        ["Personal data, row filter and quality before duplicates", "A row that is set aside or dropped takes no "
                                                                    "part in choosing which of two duplicates "
                                                                    "is kept."],
        ["Keys before history", "History groups and joins on the key hash that the keys stage adds."],
        ["Provenance last", "It stamps the run and fixes the column order after history has added its own columns."],
    ])

    out += "<h2>Which stages can be switched off, and how</h2>"
    out += code(PLAN, r"^SWITCHES = \{", r"^\}",
                "Six stages have a switch. The left side is the stage, the right side is the name of its setting group.")
    out += ("<p>A dataset switches a stage off with the setting <code>processor_groups.&lt;group&gt;</code> set to "
            "<code>disabled</code>. The group names differ from the stage names because the setting keys were "
            "kept from an earlier framework. The other seven stages have no switch and always run.</p>")
    out += code(PLAN, r"    disabled = frozenset", 1, "How the plan turns the settings into the set of switched-off stages.")
    out += code(PLAN, r"    def runs\(self", 2, "The one question the stage runner asks the plan.")
    out += ("<p>The comparison is with the exact word <code>disabled</code>. The settings registry also allows "
            "<code>enabled</code>, <code>opt_in</code> and <code>dataset_decides</code> for these keys, with no "
            "default. The plan treats all three, and a missing setting, the same way: the stage runs. How a "
            f"setting's value is chosen between the default, the domain and the dataset is on {link('meta-settings', 'the settings page')}.</p>")
    out += table(["Stage", "Setting that switches it off", "What switching it off means"], [
        ["Hygiene", "<code>processor_groups.hygiene</code>", "Text reaches silver exactly as bronze has it."],
        ["Parse and type", "<code>processor_groups.type_safety</code>", "Columns keep the types bronze has. No cast failure is detected."],
        ["Time", "<code>processor_groups.timestamp_normalize</code>", "No local time column is added."],
        ["Standardise", "<code>processor_groups.standardization</code>", "Value maps and letter case are not applied."],
        ["Personal data", "<code>processor_groups.pii</code>", "Restricted columns are not checked for plain values."],
        ["Duplicates", "<code>processor_groups.dedup</code>", "No repeat is removed, and replays are not checked against silver."],
    ])

    out += "<h2>How the stages are chained</h2>"
    out += code(INIT, r"^def run_stages\(", caption="The whole stage runner. It is a loop over the fixed tuple.")
    out += ladder([
        ("Build the context", "One <code>StageCtx</code> is made from the plan, silver as it stands, the run id, "
                              "the business date and the load time. Every stage receives the same one."),
        ("Start from bronze", "<code>df</code> starts as the new bronze rows."),
        ("Walk the tuple", "For each stage name in <code>STAGES</code>, the runner asks <code>plan.runs(name)</code>. "
                           "A switched-off stage is skipped and <code>df</code> passes through untouched."),
        ("Call the stage", "The function is looked up by name and called with the rows and the context. Its "
                           "<code>df</code> becomes the input of the next stage."),
        ("Collect the quarantine", "If the stage set rows aside, that frame is added to a list."),
        ("Collect the counts", "Each frame in the stage's <code>tally</code> is kept under the name "
                               "<code>&lt;stage&gt;.&lt;what&gt;</code>, for example <code>duplicates.dropped</code>."),
        ("Return three things", "The rows to write, one quarantine frame made by stacking the list, and the "
                                "dictionary of frames to count."),
    ])
    out += ("<p>Nothing has been computed when <code>run_stages</code> returns. On Spark a frame is a recipe, "
            "and the thirteen stages have only extended the recipe. The work happens later, when the adapter "
            "asks for the rows.</p>")

    out += "<h2>What a quarantine row holds</h2>"
    out += ("<p>Four stages can set rows aside: parse and type, personal data, quality and keys. History can do "
            "it for one kind of dataset. They all use the same helper, so every quarantine row has the same "
            "five columns whatever stage made it. That is what lets the runner stack them with "
            "<code>unionByName</code>.</p>")
    out += code(COMMON, r"^def quarantine_rows\(", caption="The helper that splits a frame into good rows and quarantine records.")
    out += ("<p>Walk through it. The stage passes a column expression called <code>reason</code>. It is text "
            "for a bad row and null for a good row. The helper adds it as a working column, keeps the rows where "
            "it is null as the good rows, and turns the others into records. Each record carries the stage name, "
            "the reason, the batch and the file the row came from, and the row itself as one JSON text. The "
            "<code>record</code> argument says which columns go into that JSON. By default it is all of them.</p>")
    out += table(["Column", "What it holds"], [
        ["<code>_stage</code>", "The stage that set the row aside, for example <code>parse_type</code>."],
        ["<code>_reason</code>", "Every check the row failed, in plain text."],
        ["<code>_batch_key</code>", "The bronze batch the row came from, or null if the frame has no such column."],
        ["<code>_source_file</code>", "The file the row came from, or null."],
        ["<code>_record</code>", "The whole row as JSON text, with the values as they were when the stage looked at them."],
    ])
    out += code(COMMON, r"^def reasons\(", caption="How several failed checks on one row become one reason text.")
    out += ("<p>A stage builds a list of pairs: a condition that is true when the check failed, and the text "
            "that names the failure. <code>reasons</code> turns each pair into a column that is the text when "
            "the condition holds and null otherwise. <code>concat_ws</code> joins the texts and skips the nulls, "
            "so a row that failed two checks gets both texts, and a row that failed none gets an empty text. The "
            "last line turns that empty text into null, which is the sign of a good row.</p>")
    out += raw('_stage       parse_type\n'
               '_reason      ordered_qty is not a integer\n'
               '_batch_key   2026-11-09T10:15|3f9a...\n'
               '_source_file sales_order_line_1015.json\n'
               '_record      {"order_id":"SO-1002","line_no":"10","ordered_qty":"4.5", ... }',
               "One quarantine record for an order line whose quantity was sent as 4.5. The values are made up.")
    out += (f"<p>The write edge appends these records to a table beside the silver table. {link('silver-writer', 'The writer page')} "
            "shows that step.</p>")

    out += "<h2>How the counts are collected</h2>"
    out += ("<p>The stage runner returns frames, not numbers. The adapter turns them into the counts that go to "
            "the run log. This table lists every count name the code can produce.</p>")
    out += table(["Count", "Where it comes from"], [
        ["<code>rows_in</code>", "The adapter counts the bronze rows it was given."],
        ["<code>rows_out</code>, <code>rows_removed</code>", "The adapter groups the final rows by their <code>_action</code>: <code>upsert</code> and <code>delete</code>."],
        ["<code>&lt;stage&gt;.quarantined</code>", "The adapter groups the quarantine records by their <code>_stage</code> column."],
        ["<code>parse_type.emptied</code>", "Rows where a value was emptied instead of quarantined."],
        ["<code>row_filter.dropped</code>", "Rows a declared filter dropped."],
        ["<code>quality.flagged</code>, <code>quality.critical</code>", "Rows kept with a flag, and rows that broke a critical rule."],
        ["<code>duplicates.dropped</code>, <code>duplicates.replayed</code>", "Repeats inside the batch, and rows already in silver."],
    ])
    out += code(RUNNER, r"    for stage in STAGES:", 5,
                "After a successful write the runner logs one step per stage that has counts.")
    out += ("<p>The runner cuts the stage name off each count and writes one step record per stage. The step's "
            "status is <code>warned</code> when the stage quarantined or emptied anything, and "
            "<code>succeeded</code> otherwise.</p>")

    out += "<h2>The <code>hold</code> argument</h2>"
    out += ("<p><code>run_stages</code> takes an optional function called <code>hold</code>. When it is given, "
            "the runner passes each stage's output through it before the next stage reads it. Its job is to fix "
            "the rows in place, so that the next stage starts from stored rows and not from a recipe.</p>"
            "<p>Spark does not need it and passes nothing. The DuckDB adapter does need it. There, every frame "
            "is one SQL query, and each stage would make the query of all earlier stages longer. The adapter "
            "passes a function that runs the query once into a table.</p>")
    out += code(DUCK, r"            rows, quarantine, tally = run_stages", 2, "The DuckDB adapter passes its checkpoint function as hold.")
    out += (f"<p>The design note {src(SPIKE)} records the effect: holding each stage's output halved the time of "
            "the stage tests on DuckDB, from 194 seconds to 92 seconds, and without any hold a history test "
            "over three batches did not finish in five minutes.</p>")

    out += "<h2>How the Spark adapter calls the stage runner</h2>"
    out += code(EDGE, r"^class SparkStageRunner:", r"return StageResult\(rows, quarantine, counts\)",
                "The adapter between the silver runner and run_stages on Spark.", cap=40)
    out += ladder([
        ("Cache bronze", "The new bronze rows are cached, because they are read several times below. The cache "
                         "can be turned off, because serverless compute refuses it."),
        ("Read the clock once", "<code>datetime.now(UTC)</code> is passed as the load time. This is the only "
                                "place the clock is read."),
        ("Run the recipe once", "<code>localCheckpoint(eager=True)</code> computes the final rows now and keeps "
                                "them. The counts and the write then read the same rows. It also matters for the "
                                "merge: the stages read the silver table, and a merge into that same table must "
                                "see a fixed input."),
        ("Turn engine errors into a stage error", "If Spark fails on the data, the adapter raises "
                                                  "<code>StageError</code> with the first 500 characters of the "
                                                  "message. The silver runner knows what to do with that error."),
        ("Count the rows", "Bronze rows in, then the final rows grouped by action."),
        ("Count everything else in one job", "Each tally frame is reduced to one column holding its name. The "
                                             "quarantine frame is reduced to its stage name with "
                                             "<code>.quarantined</code> added. All of them are stacked and "
                                             "grouped once. Every tally name starts at zero, so a count that "
                                             "found nothing is still reported."),
        ("Return", "A <code>StageResult</code> with the rows, the quarantine frame and the counts."),
    ])
    out += code(RUNNER, r"    except StageError as exc:", 2, "What the silver runner does with a StageError.")
    out += ("<p>A <code>StageError</code> from any stage fails the dataset run at the step "
            "<code>silver.stages</code>. Nothing is written and the bookmark does not move, so the next run takes "
            f"the same batches again. {link('silver-reader', 'The reader page')} explains the bookmark.</p>")

    out += "<h2>How it is tested</h2>"
    out += (f"<p>{src(T_STAGES)} has one table of cases per stage, on one shared local Spark session. Each stage "
            f"page shows cases from it. The adapter's own behaviour is in {src(T_PORTS)}: its cases check the "
            "counts and that a batch which breaks its contract raises the stage error.</p>")
    out += code(T_PORTS, r'"rows in and out", APPEND', 6, "Three cases for the adapter's counts.")
    out += "<h2>The design decisions behind this page</h2>"
    out += table(["Decision", "What it means", "Why"], [
        ["Stages are pure functions", "Frames in, frames out. No reads, no writes, no clock.",
         "The same functions run on Spark and on DuckDB, and a test needs only a few rows in memory."],
        ["The order lives in one tuple", "The kernel owns the list. An engine must supply exactly these names in this order.",
         "An engine cannot quietly leave a stage out."],
        ["Switches are settings, not code", "Six stages can be skipped per dataset by a registered setting.",
         "A dataset never needs its own version of the pipeline."],
        ["Bad rows are set aside with a reason", "A row that fails a check goes to quarantine. The good rows of the same batch go on.",
         "One bad record must not stop a load, and nothing may disappear without a trace."],
    ])
    out += not_built([
        "On Spark only the final rows are checkpointed. The quarantine frame is computed again when it is "
        "counted and again when it is written. The DuckDB adapter checkpoints both.",
        "There is no setting that adds a stage or changes the order. That is deliberate, but it means a "
        "dataset with a special need has to be handled before bronze or after silver.",
        "The settings registry also has the keys <code>processor_groups.provenance</code> and "
        "<code>processor_groups.case_normalize</code>. No code reads them, so setting them changes nothing.",
        "Rows dropped by the row filter and the duplicates stage are counted but not stored anywhere.",
    ])
    return out


# ------------------------------------------------------------------------------------------------------------ 1
@page("silver-contract-check", "Stage 1: contract check, are all the columns there", parent="silver-stages",
      short="1 Contract check",
      blurb="Every contract column the plan keeps must be in the bronze frame. A missing one stops the dataset run, by name.")
def silver_contract_check():
    out = ("<p class='lead'>The first stage looks at the column names of the new bronze rows and compares them "
           "with the dataset's contract. The contract is the agreed list of columns, types and keys of the "
           "dataset. If a contract column is missing, the stage stops the dataset run and names the column.</p>"
           "<p>Without this stage a missing column would surface later as an engine error in whichever stage "
           "first touched it, with a message about an unresolved column and no hint that the source changed.</p>")
    d, h = pic(("New bronze rows", "column names only"), ("1 Contract check", "is every kept column here?"),
               [("The same rows, untouched", "all columns present", "bx"),
                ("StageError", ["the run fails, nothing is written"], "bxm")])
    out += fig(990, h, d, "The contract check either passes the rows on or stops the run",
               "The stage never changes a row. It has only two outcomes.")
    out += flow3("The new bronze rows and the plan's list of kept columns.",
                 "The frame's column names are compared with the names the plan expects. No row is read.",
                 "The same frame, or an error that names the missing columns.")
    out += "<h2>A worked example</h2>"
    out += ("<p>The sales order line contract has thirteen columns. Suppose the bronze frame arrives without "
            "<code>promised_date</code>. The table shows three situations and what the stage does in each.</p>")
    out += table(["Bronze frame has", "Column settings", "Result"], [
        ["All thirteen contract columns and an extra column <code>sales_office</code>", "None",
         "Passes. An extra column is not a fault. The next stage decides what happens to it."],
        ["Everything except <code>promised_date</code>", "None",
         "Stops with: <code>erp.sales_order_line: contract column(s) promised_date missing from bronze (DPC-0008)</code>"],
        ["<code>qty</code> but no <code>ordered_qty</code>", "<code>ordered_qty</code> has <code>from: qty</code>",
         "Passes. A renamed column is looked up by its source name."],
    ])
    out += "<h2>The settings that drive it</h2>"
    out += NO_COLUMN_SETTINGS
    out += table(["Setting", "What it means", "Allowed values", "Default"], [
        ["The contract's column list", "The columns the stage expects.", "Set by the contract file", "None"],
        ["<code>columns[].from</code>", "The column's name in bronze when it differs from the contract name. The stage looks for this name.", "A column name", "The contract name"],
        ["<code>columns[].path</code>", "The column is extracted from the held columns in <code>_extra</code>. The stage then expects <code>_extra</code>, not the column.", "A JSON path such as <code>$.reason</code>", "Empty"],
        ["<code>columns[].exclude</code>", "The column is left out of silver, so the stage does not expect it.", "true or false", "false"],
    ])
    out += "<p>This stage has no switch. It always runs.</p>"
    out += "<h2>The code</h2>"
    out += code(CLEAN, r"^def contract_check\(", caption="The whole stage.")
    out += ladder([
        ("The names bronze has", "<code>have</code> is the set of column names of the frame."),
        ("The names that are missing", "<code>ctx.plan.kept</code> is the contract's columns without the excluded "
                                       "ones. For each kept column that is not extracted by a path, the stage "
                                       "takes its source name and checks whether bronze has it. The helper "
                                       "<code>_source_name</code> returns the <code>from</code> name if one is "
                                       "set and the contract name otherwise."),
        ("The held columns", "If any kept column is extracted by a path, the frame must have the "
                             "<code>_extra</code> column to extract from. If it does not, <code>_extra</code> is "
                             "added to the missing list."),
        ("Stop or pass", "If anything is missing, the stage raises <code>StageError</code> with the table name "
                         "and the sorted list. Otherwise it returns the frame as it came."),
    ])
    out += code(CLEAN, r"^def _source_name\(", caption="The name a contract column has in bronze.")
    out += ("<p>The error ends the dataset run. The silver runner records it at the step "
            "<code>silver.stages</code>, marks the dataset run failed, writes nothing and leaves the bookmark "
            f"where it was. {link('silver-stages', 'The hub page')} shows that part of the runner.</p>")
    out += "<h2>How it is tested</h2>"
    out += (f"<p>{src(T_STAGES)} has five cases for this stage. Each case gives the frame's columns, one row, the "
            "column settings and the expected error, or none.</p>")
    out += code(T_STAGES, r'"every column present"', 8, "The five cases. The third expects an error that names column v.")
    out += not_built([
        "The stage checks names only. A column that is present with the wrong type passes here and is handled "
        "by stage 4.",
        "The check is on the frame as a whole, not on each batch. Bronze is one table, so a batch that lacks a "
        "column which the table already has shows up as missing values, not as a missing column. Those missing "
        "values are caught by the required-column rules of the quality stage. This is my reading of the code. "
        "I did not verify what ingestion checks per batch before bronze.",
        "The stage does not look at the contract's version.",
    ])
    return out


# ------------------------------------------------------------------------------------------------------------ 2
@page("silver-project", "Stage 2: project, choosing and naming the columns", parent="silver-stages",
      short="2 Project",
      blurb="Contract columns under their contract names, every other source column too, excluded columns gone.")
def silver_project():
    out = ("<p class='lead'>The second stage decides which columns go on and what they are called. After it, "
           "every contract column has its contract name, whatever the source called it, and every later stage "
           "can work with contract names only.</p>"
           "<p>By default no column is thrown away. A column the source added that the contract does not know "
           "still reaches silver. Without that rule a new source column would be lost until someone changed "
           "the contract and reloaded history.</p>")
    d, h = pic(("Bronze rows", ["source names, load columns"]), ("2 Project", "select, rename, extract"),
               [("Rows with contract names", ["plus other source columns,", "load columns, _dq_flags"], "bx")])
    out += fig(990, h, d, "Project selects and renames columns", "Rows are not added or removed. Only the columns change.")
    out += flow3("Bronze rows with the source's column names and the columns the lander stamped.",
                 "One <code>select</code> builds the new column list in a fixed order.",
                 "The same rows. Contract columns first, then other source columns, then the underscore columns, then <code>_dq_flags</code>.")
    out += "<h2>A worked example</h2>"
    out += ("<p>Take a dealer row. Suppose the source calls the dealer's name <code>name</code>, sends a new "
            "column <code>segment</code> that the contract does not have, and the contract's "
            "<code>contact_phone</code> is not wanted in silver. The settings for that would look like this.</p>")
    out += raw("columns:\n"
               "  - {name: dealer_name, from: name}\n"
               "  - {name: contact_phone, exclude: true}",
               "Per-column settings in a dataset manifest. Typed for this page.")
    out += "<p>The first table is the bronze row. The second is the row after the stage.</p>"
    out += table(["dealer_code", "name", "region", "contact_email", "contact_phone", "updated_at", "segment", "_batch_key"],
                 [["D-1001", "Sharma Tyres", "north", "tok_5c1e...", "tok_9a40...", "2026-11-09 06:00:00", "retail", "b-17"]])
    out += table(["dealer_code", "dealer_name", "region", "contact_email", "updated_at", "segment", "_batch_key", "_dq_flags"],
                 [["D-1001", "Sharma Tyres", "north", "tok_5c1e...", "2026-11-09 06:00:00", "retail", "b-17", "[]"]])
    out += ("<p>Three things changed. <code>name</code> became <code>dealer_name</code>. "
            "<code>contact_phone</code> is gone. A new column <code>_dq_flags</code> holds an empty list. The "
            "column <code>segment</code> passed through although the contract does not list it.</p>")
    out += "<h2>The settings that drive it</h2>"
    out += NO_COLUMN_SETTINGS
    out += table(["Setting", "What it means", "Allowed values", "Default"], [
        ["<code>columns[].from</code>", "The column's name in bronze.", "A safe column name", "The contract name"],
        ["<code>columns[].path</code>", "Take the column out of the JSON text in <code>_extra</code>. <code>_extra</code> is where ingestion puts columns it was told to hold.", "A JSON path", "Empty"],
        ["<code>columns[].exclude</code>", "Leave the column out of silver.", "true or false", "false"],
        ["Projection mode", "<code>all</code> keeps other source columns. <code>declared</code> keeps only the contract's columns.", "<code>all</code>, <code>declared</code>", "<code>all</code>"],
    ])
    out += ("<p>The projection mode is an argument of <code>plan_from_view</code>. No manifest key sets it yet. "
            "This stage has no switch.</p>")
    out += code(PLAN, r"^class ColumnPlan:", r"values: tuple", "Everything the plan knows about one column. Stages 1 to 7 read these fields.")
    out += "<h2>The code</h2>"
    out += code(CLEAN, r"^def project\(", caption="The whole stage. It ends in one select.")
    out += code(CLEAN, r"^_NAME = ", 1, "The pattern a column name must match.")
    out += ladder([
        ("Refuse unsafe names", "Every contract name and every source name must start with a letter or an "
                                "underscore and continue with letters, digits or underscores, at most 128 "
                                "characters in all. Later stages put column names into SQL expressions, so a "
                                "name with a space or a quote is refused here, with a stage error."),
        ("Know which names are spoken for", "<code>sources</code> is the set of every contract column's source "
                                            "name and contract name, including the excluded columns. It is used "
                                            "below to decide what counts as another source column."),
        ("Pick the contract columns", "For each kept column the stage takes the bronze column under its source "
                                      "name, or, when the column has a path, the value at that path inside "
                                      "<code>_extra</code>. Either way it is given the contract name with "
                                      "<code>alias</code>. This is where the rename happens."),
        ("Collect the other source columns", "<code>others</code> is every bronze column that is not spoken for "
                                             "and does not start with an underscore. An excluded column is spoken "
                                             "for, so it is neither picked nor counted among the others. That is "
                                             "how it disappears."),
        ("Carry the underscore columns", "<code>meta</code> is every column that starts with an underscore, such "
                                         "as <code>_batch_key</code>, <code>_source_file</code> and "
                                         "<code>_extra</code>. They always go on."),
        ("Start the flags", "If the frame already has <code>_dq_flags</code> it is kept. Otherwise an empty list "
                            "of text is created. Later stages append names to it."),
        ("Select", "The final column list is the picked columns, then the others when the projection is "
                   "<code>all</code>, then the underscore columns, then <code>_dq_flags</code>."),
    ])
    out += ("<p>A value extracted from <code>_extra</code> comes out as text, because "
            "<code>get_json_object</code> returns text. Stage 4 then casts it to the contract's type like any "
            "other column.</p>")
    out += code(COMMON, r"^def no_flags\(", caption="Why the empty list is written as SQL text.")
    out += "<h2>How it is tested</h2>"
    out += (f"<p>{src(T_STAGES)} has four cases in a table and two further tests. Each case lists the columns "
            "that must be in the result and the columns that must not.</p>")
    out += code(T_STAGES, r'"all columns by default", "id string', 8, "The four cases: all columns, a rename, an exclusion, an extraction.")
    out += code(T_STAGES, r"^def test_project_declared_keeps_only_the_contract", caption="The declared mode drops the column the contract does not have.")
    out += not_built([
        "Keeping only the contract's columns exists in the code but no manifest key selects it. The design "
        f"note {src(NOTE)} lists this as a gap in the manifest specification.",
        "The manifest's <code>columns</code> list has no typed schema of its own. The plan refuses a key it "
        "does not know, which is the only check today.",
        "A held column in <code>_extra</code> does not become a real silver column by itself. It needs a "
        "contract column with a <code>path</code>.",
    ])
    return out


# ------------------------------------------------------------------------------------------------------------ 3
@page("silver-hygiene", "Stage 3: hygiene, cleaning up text", parent="silver-stages", short="3 Hygiene",
      blurb="Strips invisible characters, trims spaces, makes line endings the same, turns empty text into a missing value.")
def silver_hygiene():
    out = ("<p class='lead'>The third stage cleans every text column of the contract. It removes characters "
           "nobody can see, trims spaces, and turns text that is empty into a missing value.</p>"
           "<p>Without it, <code>'D-1001'</code> and <code>'D-1001 '</code> would be two different dealers, a "
           "number sent as <code>' 42'</code> might fail to convert, and an empty text would count as a value "
           "in every rule that asks whether a column is filled.</p>")
    d, h = pic(("Rows with raw text", "as the source typed it"), ("3 Hygiene", "five replacements, then empty to null"),
               [("Rows with clean text", "same rows, same columns", "bx")])
    out += fig(990, h, d, "Hygiene rewrites text values in place", "No row is added, removed or set aside.")
    out += flow3("Rows after projection.",
                 "For every kept contract column that is text and has not opted out, the value is passed through "
                 "five replacements in a fixed order.",
                 "The same rows with cleaned text. Other columns are untouched.")
    out += "<h2>A worked example</h2>"
    out += ("<p>Four dealer names as a source might send them, and what silver gets. The marks "
            "<code>[BOM]</code>, <code>[CR]</code>, <code>[LF]</code> and <code>[BEL]</code> stand for single "
            "invisible characters. A dot stands for a space.</p>")
    out += table(["dealer_name in bronze", "dealer_name after hygiene", "Which step acted"], [
        ["<code>[BOM]Sharma·Tyres</code>", "<code>Sharma·Tyres</code>", "Step 1 removed the byte-order mark."],
        ["<code>··Coastal···Wheels·</code>", "<code>Coastal·Wheels</code>", "Step 4 trimmed the ends. Step 5 made three spaces one."],
        ["<code>Hill[BEL]top[CR][LF]Depot</code>", "<code>Hilltop[LF]Depot</code>", "Step 2 removed the bell character. Step 3 made the line ending a single line feed."],
        ["<code>···</code>", "missing value", "Step 4 left an empty text. The last line turned it into null."],
    ])
    out += "<h2>The settings that drive it</h2>"
    out += NO_COLUMN_SETTINGS
    out += table(["Setting", "What it means", "Allowed values", "Default"], [
        ["<code>columns[].hygiene</code>", "Whether this column is cleaned. A column whose exact text matters can opt out.", "true or false", "true"],
        switch_row("hygiene"),
    ])
    out += raw("columns:\n  - {name: defect_code, hygiene: false}", "A column that opts out. Typed for this page.")
    out += "<h2>The code</h2>"
    out += code(CLEAN, r"^def hygiene\(", caption="The stage: which columns are cleaned.")
    out += ("<p>The stage reads each column's actual type from the frame. A column is cleaned when three things "
            "are true: it is a kept contract column, its <code>hygiene</code> setting is on, and it is text "
            "right now. The third condition matters. A contract column that will become a number in stage 4 "
            "but arrives as text is cleaned too, so its spaces are gone before the cast. A column that already "
            "arrives as a number is left alone.</p>")
    out += code(CLEAN, r"^def clean_text\(", caption="The cleaning itself: one expression made of five replacements.")
    out += code(CLEAN, r"^_CONTROL = ", 1, "The set of control characters that step 2 removes.")
    out += ("<p>Each line of <code>clean_text</code> is one regular expression replacement applied to the result "
            "of the line before. A regular expression is a pattern that describes which characters to find. "
            "The table takes them one at a time.</p>")
    out += table(["Step", "Pattern", "What it finds", "Replaced with", "Why"], [
        ["1", "<code>^</code> followed by the byte-order mark",
         "The byte-order mark, but only as the very first character. <code>^</code> means the start of the value. "
         "The mark is the invisible character U+FEFF that some programs write at the start of a file. It is "
         "typed directly in the source, so the code box above shows nothing between <code>^</code> and the quote.",
         "Nothing", "A file's first value would otherwise start with an invisible character and match nothing."],
        ["2", r"<code>[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]</code>",
         "Any one control character. The square brackets list ranges of character codes. The ranges leave out "
         r"three codes on purpose: <code>\x09</code> is the tab, <code>\x0a</code> is the line feed and "
         r"<code>\x0d</code> is the carriage return.",
         "Nothing", "Control characters such as the bell or the null character carry no meaning in business text and break displays and exports."],
        ["3", r"<code>\r\n?</code>",
         r"A carriage return, followed by a line feed if there is one. The <code>?</code> makes the line feed optional. "
         "So it finds the Windows line ending and the old Mac line ending.",
         "One line feed", "The same text typed on two systems becomes equal."],
        ["4", r"<code>^\s+|\s+$</code>",
         r"White space at the start, or white space at the end. <code>\s+</code> is one or more spaces, tabs or "
         r"line breaks. <code>$</code> means the end of the value. The bar means or.",
         "Nothing", "Spaces at the ends are almost never intended and make equal keys unequal."],
        ["5", r"<code>[ \t]{2,}</code>",
         r"Two or more spaces or tabs in a row. <code>{2,}</code> means at least two. Line feeds are not in the "
         "brackets, so a break between lines inside the text is kept.",
         "One space", "Double spaces inside names are typing noise."],
        ["Last", "<code>x == ''</code>", "A value that is now empty.", "Null",
         "An empty text and a missing value should not be two different things downstream."],
    ])
    out += ("<p>The order of the steps is part of the design. Control characters are removed before the line "
            "endings are unified, and that is safe because the carriage return and the line feed are not in "
            "the control set. The ends are trimmed before runs of spaces are collapsed. The test for empty "
            "comes last, so a value made only of spaces, tabs or control characters ends as null.</p>"
            "<p>Two small consequences follow from the patterns. A single tab inside a text stays a tab, "
            "because step 5 needs at least two characters. A run of tabs becomes one space, not one tab.</p>"
            "<p>The key stage later joins key parts with the character U+001F. That character is inside the "
            "control set of step 2, so a cleaned text can never contain it. "
            f"{link('silver-keys', 'The keys page')} uses this fact.</p>")
    out += "<h2>How it is tested</h2>"
    out += (f"<p>{src(T_STAGES)} has nine cases. Each gives one raw value, the expected value and the settings. "
            "The test runs the whole chain of stages, not the hygiene function alone, so the last case also "
            "proves that the switch really skips the stage.</p>")
    out += code(T_STAGES, r'\("trimmed", ', 9, "The nine cases. The last two are the opt-out and the switch.")
    out += code(T_STAGES, r"^def test_hygiene\(", caption="The test body: one row through run_stages.")
    out += not_built([
        f"Unicode normalisation is not applied. The design note {src(NOTE)} says there is no built-in "
        "function for it on Spark. Two texts that look the same but are built from different code points stay different.",
        "Only the six kinds of white space of the regular expression class are trimmed. A non-breaking space "
        "(U+00A0) at the end of a value is, by my reading of the pattern, not removed. There is no test case for it.",
        "The byte-order mark is removed only at the very start of a value.",
        "Only contract columns are cleaned. Other source columns that pass through under the all-columns rule keep their raw text.",
        "The stage does not count how many values it changed, so the run log cannot say how dirty a batch was.",
    ])
    return out


# ------------------------------------------------------------------------------------------------------------ 4
@page("silver-parse-type", "Stage 4: parse and type, casting without losing the original", parent="silver-stages",
      short="4 Parse and type",
      blurb="Each column is cast to its contract type. A value that does not convert is quarantined or emptied and counted, never silently lost.")
def silver_parse_type():
    out = ("<p class='lead'>The fourth stage gives every contract column the type its contract states. Text "
           "that holds a number becomes a number, text that holds a date becomes a date.</p>"
           "<p>The danger in casting is silence. A plain cast turns a value it cannot read into a missing value "
           "and says nothing. A quantity typed as <code>4O</code> with the letter O would become an empty "
           "quantity, and the order's total would be wrong with no trace. This stage makes every such value "
           "visible: the row goes to quarantine with the original text, or the value is emptied, flagged and "
           "counted, as the column's setting says.</p>")
    d, h = pic(("Rows, some columns as text", "after hygiene"), ("4 Parse and type", "cast beside, compare, replace"),
               [("Typed rows", "values converted", "bx"),
                ("Quarantine", ["rows with a value that", "did not convert"], "bxm"),
                ("Emptied values", ["kept, flagged, counted"], "bxm")])
    out += fig(990, h, d, "Parse and type has three outcomes for a row",
               "Most rows take the top path. The two dashed boxes are the two ways a failed cast is made visible.")
    out += ladder([
        ("Skip what is already right", "A column that already has the target type and no declared pattern is not touched."),
        ("Cast beside the original", "For every other column a new working column <code>_cast_&lt;name&gt;</code> "
                                     "holds the converted value. The original column is still there."),
        ("Detect a failure", "A cast failed when the original has a value and the converted column has none."),
        ("Split", "Rows with a failure in a column that says quarantine are set aside. Their record holds the "
                  "original values."),
        ("Flag the emptied", "Rows with a failure in a column that says empty stay, and get a flag."),
        ("Replace", "In the rows that stay, each original column is replaced by its converted column."),
    ])
    out += "<h2>A worked example</h2>"
    out += ("<p>Four sales order lines arrive with the quantity and the promised date as text. Suppose "
            "<code>promised_date</code> has the pattern <code>dd.MM.yyyy</code>. The first table is what goes "
            "in.</p>")
    out += table(["order_id", "line_no", "ordered_qty", "promised_date"], [
        ["SO-1001", "10", "<code>'40'</code>", "<code>'20.11.2026'</code>"],
        ["SO-1002", "10", "<code>'4.5'</code>", "<code>'21.11.2026'</code>"],
        ["SO-1003", "10", "<code>'24.0'</code>", "<code>'31.02.2026'</code>"],
        ["SO-1004", "10", "missing", "<code>'22.11.2026'</code>"],
    ])
    out += "<p>Two rows go on, now typed.</p>"
    out += table(["order_id", "line_no", "ordered_qty", "promised_date"], [
        ["SO-1001", "10", "40 (whole number)", "2026-11-20 (date)"],
        ["SO-1004", "10", "missing", "2026-11-22 (date)"],
    ])
    out += "<p>Two rows go to quarantine. The record keeps the text exactly as it arrived.</p>"
    out += table(["_stage", "_reason", "_record (shortened)"], [
        ["parse_type", "ordered_qty is not a integer", '<code>{"order_id":"SO-1002", "ordered_qty":"4.5", ...}</code>'],
        ["parse_type", "promised_date is not a date", '<code>{"order_id":"SO-1003", "promised_date":"31.02.2026", ...}</code>'],
    ])
    out += ("<p>Notice four things. A quantity of 4.5 is not rounded to 4. The text <code>24.0</code> would have "
            "converted to 24, but its row left because of the date. The thirty-first of February is not a date. "
            "A missing quantity is not a failed cast, because there was nothing to convert. Whether a missing "
            "quantity is acceptable is a question for the quality stage. The wording <em>is not a integer</em> "
            "is the code's own text.</p>")
    out += "<h2>The settings that drive it</h2>"
    out += NO_COLUMN_SETTINGS
    out += table(["Setting", "What it means", "Allowed values", "Default"], [
        ["The contract's <code>logicalType</code>", "The type the column must have in silver.",
         "<code>string</code>, <code>integer</code>, <code>number</code>, <code>boolean</code>, <code>timestamp</code>, <code>date</code>", "None. The contract must state it."],
        ["<code>columns[].format</code>", "The pattern of a date or timestamp sent as text.", "A date pattern such as <code>dd.MM.yyyy</code>", "Empty. The engine's standard form is expected."],
        ["<code>columns[].on_cast_failure</code>", "What happens to a value that does not convert.",
         "<code>quarantine</code>: the row is set aside. <code>empty</code>: the value becomes missing, the row is flagged and counted.", "<code>quarantine</code>"],
        switch_row("type_safety"),
    ])
    out += code(PLAN, r"    failure = str\(override.get", 5, "The plan refuses to empty a required column or a key column.")
    out += ("<p>The second refusal protects the data. Emptying a required column would only move the failure to "
            "the quality stage, and emptying a key part would make the row impossible to identify.</p>")
    out += "<h2>The code: the target types</h2>"
    out += code(CLEAN, r"^SPARK_TYPES", r"^\}", "The six contract types and the engine type each one becomes.")
    out += ("<p>An <code>integer</code> in the contract is a 64-bit whole number in silver. A <code>number</code> "
            "is a double, which is a floating point number. There is no decimal type in this list.</p>")
    out += "<h2>The code: one value</h2>"
    out += code(CLEAN, r"^def cast_to\(", caption="How one column is converted. Every branch gives null for a value it cannot read.")
    out += code(CLEAN, r"^def _try_cast\(", r'"timestamp": "timestamp", "date": "date"', "The cast that gives null instead of an error, on every engine, and the SQL name of each type.", cap=12)
    out += table(["Contract type", "How the value is converted", "What that means"], [
        ["date or timestamp with a pattern", "The value is read as text with <code>try_to_timestamp</code> and the pattern. For a date the result is then cut to the day.",
         "A text that does not fit the pattern, or names a day that does not exist, gives null."],
        ["integer", "The value is first cast to a double. It is kept only when the double equals its own floor, which is the number without its fraction. Then it is cast to a whole number.",
         "<code>'42'</code> and <code>'42.0'</code> become 42. <code>'4.5'</code> gives null and is a failure. A plain cast to a whole number would have cut it to 4."],
        ["boolean", "The value is made lower-case text. <code>true</code> and <code>1</code> become true. <code>false</code> and <code>0</code> become false. There is no other branch.",
         "Anything else, such as <code>yes</code>, gives null and is a failure."],
        ["string", "A plain cast to text.", "This cannot fail."],
        ["number, and date or timestamp without a pattern", "<code>try_cast</code> to the engine type.", "The engine's own reading of the text. A value it cannot read gives null."],
    ])
    out += ("<p><code>_try_cast</code> builds a small SQL expression, because <code>try_cast</code> is the one "
            "form that returns null for a bad value on Spark with strict mode on, on Spark with it off, and on "
            "DuckDB. The column name goes into that expression, so it is checked against the safe-name pattern "
            "first and wrapped in backticks. The type comes from a fixed dictionary, never from data.</p>")
    out += "<h2>The code: the stage</h2>"
    out += code(CLEAN, r"^def parse_type\(", caption="The stage. The original column stays until the split is done.")
    out += ladder([
        ("Read the actual types", "<code>types</code> maps each column of the frame to the type it has now."),
        ("Decide per column", "For each kept column the target type is looked up. If the column already has it "
                              "and no pattern is declared, the loop moves on."),
        ("Add the converted column", "<code>work</code> gets a new column <code>_cast_&lt;name&gt;</code>. The "
                                     "original column is not overwritten. This is the line that makes everything "
                                     "else possible."),
        ("Define failed", "<code>failed</code> is true when the original is not null and the converted value is "
                          "null. A null original is not a failure."),
        ("Sort the failure into one of two lists", "If the column says <code>empty</code>, the pair goes to "
                                                   "<code>emptied</code> with the flag name "
                                                   "<code>emptied:&lt;name&gt;</code>. Otherwise it goes to "
                                                   "<code>checks</code> with the reason text."),
        ("Split", "<code>quarantine_rows</code> is called with <code>df.columns</code> as the record columns. "
                  "<code>df</code> is the frame as it came in, so the record holds the original values and none "
                  "of the working columns. This is how a failed cast keeps the value the source sent."),
        ("Flag", "<code>add_flags</code> appends <code>emptied:&lt;name&gt;</code> to <code>_dq_flags</code> on "
                 "the good rows where that cast failed."),
        ("Count", "The tally <code>emptied</code> is the good rows that carry at least one flag starting with "
                  "<code>emptied:</code>."),
        ("Replace and tidy", "For each column that was cast, the original is overwritten with the converted "
                             "column and the working column is dropped. For an emptied value the converted "
                             "column is null, so the value in silver is missing."),
    ])
    out += code(COMMON, r"^def add_flags\(", caption="How a flag name is appended to a row's list of flags.")
    out += ("<p><code>add_flags</code> builds a list with one entry per flag: the name where the condition "
            "holds and null where it does not. <code>array_compact</code> removes the nulls. The result is "
            "joined to the flags the row already has.</p>")
    out += "<h2>How it is tested</h2>"
    out += (f"<p>{src(T_STAGES)} has nine cases. Each gives a contract type, one raw text, the column's "
            "settings, the expected value, the expected quarantine reasons and the expected count of emptied "
            "values.</p>")
    out += code(T_STAGES, r'"text to whole number"', 11, "The nine cases. The second is the fraction that is never cut. The eighth is the emptied value.")
    out += code(T_STAGES, r"^def test_parse_type\(", caption="The test body. An emptied value must also carry its flag.")
    out += not_built([
        "A whole number goes through a double on its way. A double holds whole numbers exactly only up to "
        "about nine thousand million million (two to the power 53). By my reading of the code, a larger whole "
        "number sent as text could change in its last digits. There is no test case for it. A column that "
        "already arrives as a whole number is not affected, because it is skipped.",
        "There is no decimal type. Money is a double in silver.",
        "A boolean accepts only true, false, 1 and 0.",
        "When the stage is switched off, nothing checks that bronze types match the contract.",
        "Only contract columns are cast. Other source columns keep the type bronze gave them.",
    ])
    return out


# ------------------------------------------------------------------------------------------------------------ 5
@page("silver-time", "Stage 5: time, the local time beside the instant", parent="silver-stages", short="5 Time",
      blurb="Timestamps stay instants in UTC. A column may ask for its wall-clock time in a named zone beside it.")
def silver_time():
    out = ("<p class='lead'>A timestamp in silver is an instant: one point in time, the same everywhere, held in "
           "UTC. The fifth stage leaves it that way. When a column asks for it, the stage adds a second column "
           "with the time a clock on the wall showed in a named time zone.</p>"
           "<p>Both are needed. The instant is what makes two events comparable across plants. The wall-clock "
           "time is what a shift report needs, because a shift starts at six in the morning local time, not at "
           "a fixed UTC hour.</p>")
    d, h = pic(("Typed rows", "order_ts as a timestamp"), ("5 Time", "convert_timezone, format as text"),
               [("The same rows", ["order_ts unchanged,", "order_ts_local added"], "bx")])
    out += fig(990, h, d, "The time stage adds a column beside a timestamp", "The timestamp itself is not changed.")
    out += flow3("Rows whose timestamp columns have been typed by stage 4.",
                 "For each timestamp column with a <code>local_tz</code>, the wall-clock time in that zone is worked out and written as text.",
                 "The same rows with one more column per such timestamp, named <code>&lt;name&gt;_local</code>.")
    out += "<h2>A worked example</h2>"
    out += ("<p>Suppose <code>order_ts</code> of the sales order line has <code>local_tz: Asia/Kolkata</code>. "
            "The two rows show the instant and the column the stage adds. India is five and a half hours ahead "
            "of UTC.</p>")
    out += table(["order_id", "order_ts (instant, UTC)", "order_ts_local (text)"], [
        ["SO-1001", "2026-11-09 00:00:00", "<code>2026-11-09T05:30:00</code>"],
        ["SO-1002", "2026-11-09 20:15:00", "<code>2026-11-10T01:45:00</code>"],
    ])
    out += ("<p>The second row shows why the local column matters: the order was placed on the tenth by the "
            "plant's calendar and on the ninth by UTC.</p>")
    out += "<h2>The settings that drive it</h2>"
    out += NO_COLUMN_SETTINGS
    out += table(["Setting", "What it means", "Allowed values", "Default"], [
        ["<code>columns[].local_tz</code>", "The time zone whose wall-clock time is added beside this timestamp.", "A zone name such as <code>Asia/Kolkata</code> or <code>Europe/Budapest</code>", "Empty. Nothing is added."],
        switch_row("timestamp_normalize"),
    ])
    out += "<h2>The code</h2>"
    out += code(CLEAN, r"^def time\(", caption="The whole stage.")
    out += ladder([
        ("Which columns", "Only kept contract columns whose type is timestamp and whose <code>local_tz</code> is set."),
        ("Shift to the zone", "<code>convert_timezone</code> takes the instant, read as UTC, and gives the "
                              "wall-clock time in the named zone. The zone's rules for summer time are applied "
                              "by the engine."),
        ("Write it as text", "The result is formatted in two pieces, the date and the time, joined around a "
                             "literal letter T. The outcome is a text such as <code>2026-11-09T05:30:00</code>, "
                             "with no offset at the end."),
        ("Add the column", "The text goes into a new column named after the timestamp with <code>_local</code> added."),
    ])
    out += ("<p>The local time is stored as text on purpose. If it were stored as a timestamp, every tool would "
            "treat it as an instant in UTC and shift it again when showing it. As text it means exactly what it "
            "says.</p>"
            "<p>The comment in the code explains the two odd-looking choices. Another function for the same job "
            "and a pattern with a quoted letter do not behave the same on Spark and DuckDB, so the stage uses "
            "the forms that do.</p>"
            "<p>The stage itself converts nothing to UTC. It relies on the session's time zone being UTC, which "
            "the local session factory sets, and on stage 4 having read the timestamps under that setting.</p>")
    out += code(EDGE, r"spark\.sql\.session\.timeZone", 1, "The session's time zone is fixed to UTC where the local session is built.")
    out += "<h2>How it is tested</h2>"
    out += (f"<p>{src(T_STAGES)} has three cases. The input instant is always midnight UTC on 9 November 2026.</p>")
    out += code(T_STAGES, r'"UTC kept, nothing added"', 3, "No setting, India, and Hungary.")
    out += code(T_STAGES, r"^def test_time\(", caption="The test also checks that the original timestamp is still there.")
    out += not_built([
        "No tyre dataset declares a <code>local_tz</code>. The sales order line contract says the plant's local "
        "time is added beside <code>order_ts</code>, but the two plants are in different zones, and the plant's "
        f"zone is a fact of another dataset. The design note {src(NOTE)} records that a per-row zone belongs in gold.",
        "One column can have one fixed zone only.",
        "The new column is not a contract column. It reaches silver among the other source columns.",
        "The stage does not check that the session's time zone really is UTC.",
    ])
    return out


# ------------------------------------------------------------------------------------------------------------ 6
@page("silver-standardise", "Stage 6: standardise, one spelling for each value", parent="silver-stages",
      short="6 Standardise",
      blurb="Declared value maps and upper or lower case, per text column. A value not in the map is kept as it is.")
def silver_standardise():
    out = ("<p class='lead'>The sixth stage makes the same thing look the same. A column can declare a value "
           "map, which replaces known spellings with the agreed one, and a letter case.</p>"
           "<p>Without it, a region sent as <code>N</code>, <code>North</code> and <code>north</code> would be "
           "three regions in every report, and each report would grow its own fix.</p>")
    d, h = pic(("Typed rows", "region as the source spells it"), ("6 Standardise", "value map, then letter case"),
               [("The same rows", "region in the agreed spelling", "bx")])
    out += fig(990, h, d, "Standardise rewrites text values by a declared map and case", "No row is added, removed or set aside.")
    out += flow3("Rows after typing.",
                 "For each text column with a map or a case, the value is looked up in the map, then its case is changed.",
                 "The same rows with the declared spellings.")
    out += "<h2>A worked example</h2>"
    out += ("<p>The dealer contract allows five regions: north, south, east, west and central_europe. Suppose "
            "the dealer interface sends short codes for some. These settings would bring them in line.</p>")
    out += raw("columns:\n"
               "  - name: region\n"
               "    values: {N: north, S: south, CE: central_europe}\n"
               "    case: lower", "A value map and a case for one column. Typed for this page.")
    out += table(["region in", "After the map", "After the case", "Remark"], [
        ["<code>N</code>", "<code>north</code>", "<code>north</code>", "Found in the map."],
        ["<code>CE</code>", "<code>central_europe</code>", "<code>central_europe</code>", "Found in the map."],
        ["<code>West</code>", "<code>West</code>", "<code>west</code>", "Not in the map, so kept. The case then fixes it."],
        ["<code>n</code>", "<code>n</code>", "<code>n</code>", "The map is exact. A small n is not the key <code>N</code>."],
        ["missing", "missing", "missing", "A missing value stays missing."],
    ])
    out += ("<p>The fourth row is the one to remember. The map is applied before the case and compares exactly, "
            "so <code>n</code> passes through unmapped. The quality stage then rejects it, because the dealer "
            "contract's rule on <code>region</code> lists the valid values. That is the reason standardise comes "
            "before quality in the fixed order.</p>")
    out += "<h2>The settings that drive it</h2>"
    out += NO_COLUMN_SETTINGS
    out += table(["Setting", "What it means", "Allowed values", "Default"], [
        ["<code>columns[].values</code>", "A map from a value as sent to the value wanted.", "Any pairs of text", "Empty. No value is replaced."],
        ["<code>columns[].case</code>", "The letter case of the whole value.", "<code>upper</code>, <code>lower</code>. Anything else is refused by the plan.", "Empty. The case is left alone."],
        switch_row("standardization"),
    ])
    out += "<h2>The code</h2>"
    out += code(CLEAN, r"^def standardise\(", caption="The whole stage.")
    out += ladder([
        ("Text columns only", "A column whose contract type is not <code>string</code> is skipped."),
        ("Build the map", "The plan holds the map as a sorted tuple of pairs. <code>create_map</code> wants a "
                          "flat list: key, value, key, value. The nested loop in the comprehension flattens the "
                          "pairs into that list."),
        ("Look up, and fall back", "<code>mapping[col]</code> gives the mapped value, or null when the value is "
                                   "not a key. <code>coalesce</code> takes the first of its arguments that is "
                                   "not null, so a value that is not in the map falls back to itself."),
        ("Change the case", "<code>upper</code> or <code>lower</code> is applied to the result of the map, not to the raw value."),
        ("Write only when asked", "The column is rewritten only if it has a map or a case. Other columns are not touched at all."),
    ])
    out += "<h2>How it is tested</h2>"
    out += f"<p>{src(T_STAGES)} has six cases. Each gives the settings, one raw value and the expected value.</p>"
    out += code(T_STAGES, r'"nothing declared"', 6, "The six cases. The last proves the map runs before the case.")
    out += not_built([
        "A value that is not in the map is kept without a count or a flag. Whether it is acceptable is left to "
        "a rule in the quality stage.",
        "The map compares exactly, including letter case. A map that should ignore case needs every spelling listed.",
        "The settings registry has a separate key <code>processor_groups.case_normalize</code>. No code reads "
        "it. Letter case is switched together with the value maps by <code>processor_groups.standardization</code>.",
        "Only text columns of the contract are handled.",
    ])
    return out


# ------------------------------------------------------------------------------------------------------------ 7
@page("silver-personal-data", "Stage 7: personal data, tokens only", parent="silver-stages",
      short="7 Personal data",
      blurb="A restricted column must hold tokens. A row with a plain value is quarantined before it can reach silver.")
def silver_personal_data():
    out = ("<p class='lead'>Some columns hold personal data, such as a dealer's contact address or an "
           "inspector's id. The contract marks them <code>restricted</code>. Ingestion replaces their values "
           "with tokens before bronze. A token is a stand-in text that is the same for the same value but "
           "cannot be turned back into it. The seventh stage is the guard behind that promise: it checks that "
           "every restricted column really holds tokens.</p>"
           "<p>Without the guard, one misconfigured source would put plain personal data into silver, and from "
           "there into every table and report built on it, and nobody would notice.</p>")
    d, h = pic(("Rows with restricted columns", "contact_email, contact_phone"), ("7 Personal data", "does each value look like a token?"),
               [("Rows that hold tokens", "or nothing in the column", "bx"),
                ("Quarantine", ["rows with a plain value"], "bxm")])
    out += fig(990, h, d, "The personal data stage lets tokens pass and sets plain values aside",
               "The stage does not create tokens. It only checks them.")
    out += flow3("Rows after standardising, and the token format the deployment uses.",
                 "Each restricted column's value is matched against the token pattern.",
                 "Rows whose restricted values are tokens or missing go on. The others go to quarantine.")
    out += "<h2>A worked example</h2>"
    out += ("<p>Three dealer rows. The dealer contract marks <code>contact_email</code> and "
            "<code>contact_phone</code> as restricted. The phone is not required, so it may be missing.</p>")
    out += table(["dealer_code", "contact_email", "contact_phone", "Result"], [
        ["D-1001", "<code>tok_5c1e09b7a2d44f6e8a013c7d</code>", "<code>tok_9a40e1f2c3b4a5968778695a</code>", "Goes on."],
        ["D-1002", "<code>tok_0d3b6f18c9e24a7fb1a2c3d4</code>", "missing", "Goes on. A missing value is not a plain value."],
        ["D-1003", "a plain e-mail address", "<code>tok_77aa01bc23de45f678901234</code>", "Quarantined with the reason <code>contact_email is not tokenised</code>."],
    ])
    out += "<h2>The settings that drive it</h2>"
    out += table(["Setting", "What it means", "Allowed values", "Default"], [
        ["The contract's <code>classification</code>", "Marks a column as holding personal data.", "The stage acts on the value <code>restricted</code> only.", "Empty. The column is not checked."],
        ["The token pattern", "A regular expression that a token matches. It is not a dataset setting. The deployment's binding passes it to the silver run.", "Any regular expression", "Empty. With restricted columns and no pattern the stage stops the run."],
        switch_row("pii"),
    ])
    out += code(TYRE_C + "dealer.odcs.yaml", r'name: "contact_email"', 2, "The two restricted columns in the dealer contract.")
    out += code(KEYED, r"    PATTERN = ", 1, "The token format of the keyed tokenizer.")
    out += code(KEYED, r"    def token\(self", 3, "How that tokenizer makes a token: the first 24 hex digits of a keyed hash, after a fixed prefix.")
    out += ("<p>Read the pattern piece by piece. <code>^</code> is the start of the value. <code>tok_</code> is "
            "the literal prefix. <code>[0-9a-f]{24}</code> is exactly twenty-four characters, each a digit or a "
            "letter from a to f. <code>$</code> is the end. So a value is a token only if it is the prefix "
            "followed by exactly twenty-four hex digits and nothing else.</p>")
    out += "<h2>The code</h2>"
    out += code(CLEAN, r"^def personal_data\(", caption="The whole stage.")
    out += ladder([
        ("Find the restricted columns", "The kept columns whose classification is <code>restricted</code>. If "
                                        "there are none, the frame is returned as it came."),
        ("Refuse to guess", "If there are restricted columns but no token pattern, the stage raises a stage "
                            "error. Passing the rows unchecked would be the silent failure the stage exists to prevent."),
        ("Build one check per column", "The check is true when the value is present and does not match the "
                                       "pattern. <code>rlike</code> is a match against a regular expression. The "
                                       "sign <code>~</code> means not."),
        ("Split", "<code>quarantine_rows</code> sets aside every row with at least one failed check. A row with "
                  "two plain values gets both reasons."),
    ])
    out += "<h2>The privacy scan beside the stage</h2>"
    out += ("<p>The same folder holds a second, smaller function. It is not one of the thirteen stages. It is "
            "a scan that can be run over bronze and silver at any time and counts, for each restricted column, "
            "the values that are present and do not look like a token.</p>")
    out += code(PRIV, r"^def clear_counts\(", caption="The scan: one pass over the frame, one count per column.")
    out += ("<p>For each column it adds up 1 for a value that is present and does not match, and 0 otherwise. "
            "All columns are counted in a single aggregation, so the frame is read once. A column the frame "
            "does not have is left out of the answer. The stage stops bad rows on the way in. The scan proves "
            "afterwards that none is there.</p>")
    out += "<h2>How it is tested</h2>"
    out += (f"<p>{src(T_STAGES)} has three cases for the stage: a token passes, a plain value is quarantined "
            "with its reason, and a missing value passes. A fourth test checks the refusal when no token "
            f"format is bound. {src(T_PRIV)} has three cases for the scan, including a token of the wrong length.</p>")
    out += code(T_STAGES, r"^def test_personal_data\(", caption="The test body for the three cases.")
    out += code(T_STAGES, r"^def test_personal_data_needs_a_token_format", caption="Restricted columns and no pattern: the stage refuses.")
    out += code(T_PRIV, r"^PATTERN = ", 2, "The pattern and the sample token the scan's cases use.")
    out += not_built([
        "The quarantine record keeps every column of the row, so the plain value that caused the quarantine is "
        "stored in the quarantine table as it arrived. The quarantine table therefore needs the same protection "
        "as the source. I found no code that masks it.",
        "The stage checks the shape of a value, not where it came from. A text that happens to look like a "
        "token passes.",
        "Only the classification <code>restricted</code> is acted on.",
        "The stage does not create tokens. If ingestion did not tokenise, every row of the batch is quarantined.",
        "Switching the stage off with <code>processor_groups.pii</code> removes the check entirely. Nothing "
        "else in silver looks at restricted columns during a run.",
    ])
    return out


# ------------------------------------------------------------------------------------------------------------ 8
@page("silver-row-filter", "Stage 8: row filter, rows that are not wanted", parent="silver-stages",
      short="8 Row filter",
      blurb="Declared filters drop rows and count them. A dropped row is not wrong, so it is not quarantined.")
def silver_row_filter():
    out = ("<p class='lead'>The eighth stage removes rows that silver is not meant to hold, by a declared "
           "condition. An example is test orders that a source system mixes in with real ones.</p>"
           "<p>The difference from quarantine is the point of the stage. A quarantined row is a fault that "
           "someone should look at. A filtered row is fine, just not wanted. So the stage does not store it. "
           "It only counts it, so that the numbers of rows in and rows out can still be explained.</p>")
    d, h = pic(("Rows", "after the personal data check"), ("8 Row filter", "keep rows that satisfy every filter"),
               [("Rows kept", "every filter is true", "bx"),
                ("Dropped", ["counted, not stored"], "bxm")])
    out += fig(990, h, d, "The row filter keeps the rows that satisfy every declared filter",
               "The dashed box is a count only. Nothing is written for a dropped row.")
    out += flow3("Rows, and the plan's list of filters. Each filter is a name and a condition.",
                 "All conditions are joined with AND. A row is kept only if the result is true.",
                 "The kept rows, and the dropped rows as a frame to count.")
    out += "<h2>A worked example</h2>"
    out += ("<p>Suppose the sales order line has one filter named <code>real_orders</code> with the condition "
            "<code>dealer_code &lt;&gt; 'D-TEST'</code>.</p>")
    out += table(["order_id", "dealer_code", "Condition gives", "Result"], [
        ["SO-1001", "D-1001", "true", "Kept."],
        ["SO-9001", "D-TEST", "false", "Dropped and counted."],
        ["SO-1002", "missing", "unknown", "Dropped and counted. A condition that cannot be decided counts as not satisfied."],
    ])
    out += "<p>The run log would show <code>row_filter.dropped = 2</code> for this batch.</p>"
    out += "<h2>The settings that drive it</h2>"
    out += ("<p>There is no manifest key for a row filter today. Filters are passed in code, as an argument of "
            "the silver run: a dictionary from the dataset name to its filters.</p>")
    out += table(["Setting", "What it means", "Allowed values", "Default"], [
        ["<code>row_filters</code> argument of <code>build_silver</code>", "For each dataset name, a tuple of rules. Only each rule's name and condition are used here.", "A condition in the engine's SQL expression language", "No filters. The stage passes every row."],
    ])
    out += code(RUNNER, r"^def build_silver\(", r"\) -> SilverReport:", "The silver run's arguments. row_filters is the second from last.")
    out += "<p>This stage has no switch. With no filters it does nothing.</p>"
    out += "<h2>The code</h2>"
    out += code(CLEAN, r"^def row_filter\(", caption="The whole stage.")
    out += ladder([
        ("Nothing declared", "If the plan has no filters the frame is returned untouched and nothing is counted."),
        ("Start from true", "<code>keep</code> starts as the constant true."),
        ("Add each filter", "Each filter's condition is turned into a column expression with <code>F.expr</code> "
                            "and joined to <code>keep</code> with AND. A row must satisfy every filter."),
        ("Decide the undecided", "A condition on a missing value gives neither true nor false. "
                                 "<code>coalesce(..., False)</code> turns that into false. Without it, the "
                                 "outcome for such a row would depend on how the engine treats an unknown value "
                                 "in a filter."),
        ("Mark, then split", "The result is stored in a working column <code>_keep</code>. The kept rows are the "
                             "ones where it is true. The dropped rows are the others, returned in the tally "
                             "under the name <code>dropped</code>."),
    ])
    out += ("<p>The condition is SQL text that the engine runs as written. It is written by whoever sets up the "
            "run, never taken from data.</p>")
    out += "<h2>How it is tested</h2>"
    out += (f"<p>{src(T_STAGES)} has four cases over the same three rows: no filter, one filter, two filters, "
            "and a condition that cannot be decided for two of the rows.</p>")
    out += code(T_STAGES, r'\("no filter", \(\)', 4, "The four cases: the filters, the ids kept and the number dropped.")
    out += code(T_STAGES, r"^def test_row_filter\(", caption="The test body and its three rows.")
    out += not_built([
        "No manifest key declares a row filter, so a filter cannot be set as metadata.",
        "The two places in the code that start a silver run, the day-by-day run and the baseline writer, do "
        "not pass any filters. As the code stands, the stage is exercised only by its tests.",
        "Dropped rows are counted in total. The count is not split by filter, and the rows are not kept.",
    ])
    return out


# ------------------------------------------------------------------------------------------------------------ 9
def quality_pic():
    d = box(10, 20, 230, 70, "Required columns", "from the contract", "bx")
    d += box(10, 120, 230, 70, "Contract row rules", "translated by the runner", "bx")
    d += box(340, 60, 230, 90, "9 Quality", "severity sets the action", "solid")
    d += box(680, 5, 290, 60, "Good rows", "go on", "bx")
    d += box(680, 78, 290, 60, "Flagged rows", "kept, rule name in _dq_flags", "bx")
    d += box(680, 151, 290, 60, "Quarantine", "set aside with the rule names", "bxm")
    d += box(680, 224, 290, 60, "Critical count", "above zero: the runner holds the batch", "bxm")
    d += arrow(240, 55, 338, 95) + arrow(240, 155, 338, 115)
    d += arrow(570, 90, 678, 35) + arrow(570, 100, 678, 108) + arrow(570, 112, 678, 181) + arrow(570, 124, 678, 254)
    return d


@page("silver-quality", "Stage 9: quality, row rules and what their severity does", parent="silver-stages",
      short="9 Quality",
      blurb="Each rule has a severity. Minor flags the row and keeps it, major quarantines it, critical also holds the whole batch.")
def silver_quality():
    out = ("<p class='lead'>The ninth stage tests every row against the dataset's row rules. A row rule is a "
           "condition a single row must satisfy, such as a quantity that is never negative. What happens to a "
           "row that fails depends on how serious the rule is.</p>"
           "<p>Two simpler designs both go wrong. If every failure stopped the load, one bad record would hold "
           "back a whole day of good ones. If every failure were only logged, bad rows would flow into reports. "
           "Grading the rules avoids both.</p>")
    out += fig(990, 300, quality_pic(), "Rules from two places, four outcomes",
               "The rules come from two places on the left. A row ends in one of the first three boxes on the right. The fourth box is a count that the runner reads.")
    out += "<h2>Severity to action</h2>"
    out += ("<p>Every rule has one of three severities. The severity decides the action for a failing row. The "
            "table shows the defaults as the code applies them.</p>")
    out += table(["Severity", "What happens to a failing row", "What happens to the batch", "Meant for"], [
        ["critical", "Quarantined.", "Held. Nothing of this run is written for the dataset and the bookmark stays.", "A failure that makes the data wrong to use at all, such as a negative order quantity."],
        ["major", "Quarantined.", "The rest of the batch is written.", "A row that cannot be trusted, such as a plant code that does not exist."],
        ["minor", "Kept. The rule's name is added to <code>_dq_flags</code>.", "Written.", "A blemish worth knowing about, such as an unknown cycle state on a sensor reading."],
    ])
    out += ("<p>A flag and a quarantine differ in where the row ends up. A flagged row is in silver, and anyone "
            "who reads silver can filter on the <code>_dq_flags</code> column. A quarantined row is not in "
            "silver. It sits in the quarantine table with its reason until someone deals with it.</p>")
    out += "<h2>Where the rules come from</h2>"
    out += "<p>The plan that reaches the stage holds rules from two places.</p>"
    out += "<h3>One: every required column must be filled</h3>"
    out += code(PLAN, r"^def _rules\(", caption="The plan makes one critical rule per required column.")
    out += code(PLAN, r"^QUALITY_DEFAULTS", 2, "The default action for each severity, and the three actions a row can get.")
    out += ("<p>For each contract column that is required and not excluded, the plan makes a rule named "
            "<code>&lt;column&gt;_present</code> with the condition <code>&lt;column&gt; IS NOT NULL</code> and "
            "the severity critical. Its action is the dataset's action for critical, which is "
            "<code>quarantine</code> unless the manifest says otherwise.</p>")
    out += "<h3>Two: the row rules written in the contract</h3>"
    out += ("<p>A contract can attach quality rules to a column. The loader stores them in the metadata store "
            "with a severity and an action that follows from the severity.</p>")
    out += code(TYRE_C + "sales_order_line.odcs.yaml", r'name: "ordered_qty"', 1, "A critical rule in the sales order line contract: the quantity is never negative.")
    out += code(PORT, r"^DQ_ACTION_OF", 1, "The action the store gives a contract rule, by its severity.")
    out += code(RUNNER, r"    contract, untranslated = contract_rules", 5,
                "Just after the plan is built, the runner translates the contract's rules and adds them to the plan.")
    out += ("<p>The plan is frozen, so the runner makes a copy with the translated rules added after the "
            "required-column rules. Every rule that could not be translated is written to the run log under "
            "the step <code>silver.contract_rules</code>, with the reason. A rule is never dropped silently.</p>")
    out += "<h2>How a contract rule is translated</h2>"
    out += code(RULES, r"^_COUNT_WHERE", r"^ROW_ACTION", "The patterns the translation uses, and the map from the store's actions to row actions.")
    out += code(RULES, r"^def translate\(", caption="One contract rule in, one stage rule or one reason out.")
    out += ladder([
        ("Map the action", "The store knows three actions: <code>stop</code>, <code>quarantine</code> and "
                           "<code>warn</code>. For a row, <code>stop</code> and <code>quarantine</code> both "
                           "mean quarantine, and <code>warn</code> means flag. Holding the batch for a critical "
                           "rule is not the stage's job. The runner does it."),
        ("Leave table rules to their owners", "A rule on the whole table with no column, such as no duplicate "
                                              "keys or a row count above zero, is not a row rule. The function "
                                              "returns a sentence saying which part of the platform keeps it."),
        ("Valid values", "A rule with the check <code>invalidValues</code> and a list of valid values becomes "
                         "<code>column IS NULL OR column IN ('a', 'b')</code>. Each value is quoted, and a "
                         "quote inside a value is doubled. A missing value passes on purpose, because the "
                         "required-column rule is the one that judges missing values."),
        ("A counting query", "A rule of type <code>sql</code> is accepted only in one shape: count the rows of "
                             "the table where a condition holds, and the count must be zero. The pattern "
                             "<code>_COUNT_WHERE</code> captures the condition. The row rule is then "
                             "<code>coalesce(NOT (condition), true)</code>: a row passes when the bad condition "
                             "is false, and also when it cannot be decided."),
        ("Refuse anything that is not a plain condition", "The pattern <code>_UNSAFE</code> looks for a "
                                                          "statement separator, a comment mark, or one of the "
                                                          "words select, from, join, drop, insert, update and "
                                                          "delete. A condition that contains one is not translated."),
        ("Say why", "Anything else returns a sentence naming the rule and the reason it is not a row rule."),
    ])
    out += table(["Contract rule", "Becomes this row rule"], [
        ["<code>SELECT COUNT(*) FROM ${object} WHERE ordered_qty &lt; 0</code>, must be 0, critical", "<code>coalesce(NOT (ordered_qty &lt; 0), true)</code>, critical, quarantine"],
        ["valid values of <code>plant_code</code>: P-CHN, P-HUN, major", "<code>plant_code IS NULL OR plant_code IN ('P-CHN', 'P-HUN')</code>, major, quarantine"],
        ["valid values of <code>cycle_state</code> on press readings, minor", "the same shape of condition, minor, flag"],
        ["no duplicate values of order_id, line_no, change_seq", "Not translated. Kept by the duplicates stage."],
        ["row count above zero", "Not translated. Kept by the lander's volume check."],
    ])
    out += "<h2>The stage itself</h2>"
    out += code(CLEAN, r"^def quality\(", caption="The whole stage.")
    out += ladder([
        ("Refuse an unknown action", "If any rule has an action other than <code>quarantine</code>, "
                                     "<code>flag</code> or <code>none</code>, the stage stops with an error. "
                                     "Skipping a rule it does not understand would hide a problem."),
        ("Build a failure condition per rule", "For every rule whose action is not <code>none</code>, the "
                                               "rule's condition is run as an expression. "
                                               "<code>coalesce(..., False)</code> makes an undecided condition "
                                               "count as not satisfied. The sign <code>~</code> in front turns "
                                               "satisfied into failed."),
        ("Sort the rules into three lists", "<code>held</code> is the rules that quarantine. "
                                            "<code>flagged</code> is the rules that flag. <code>critical</code> "
                                            "is the rules that quarantine and are critical."),
        ("Quarantine", "<code>quarantine_rows</code> sets aside every row that fails a rule in <code>held</code>. "
                       "The reason is the names of the failed rules."),
        ("Flag", "<code>add_flags</code> appends the names of failed flag rules to <code>_dq_flags</code> on the rows that stay."),
        ("Count the flagged", "If there are flag rules, the tally <code>flagged</code> is the kept rows whose list of flags is not empty."),
        ("Count the critical", "If there are critical rules, the tally <code>critical</code> is the rows of the "
                               "incoming frame that fail any of them. The conditions are joined with OR. This "
                               "count is what the runner looks at."),
    ])
    out += ("<p>There are two layers of handling for a condition that cannot be decided. A translated contract "
            "rule wraps itself so that undecided means pass. The stage's own wrapper makes undecided mean fail "
            "for any rule that does not do that. The required-column rules are never undecided.</p>")
    out += "<h2>A critical failure holds the whole batch</h2>"
    out += code(RUNNER, r"    critical = int\(result\.counts\.get", r'return SilverDataset\(source, dataset, "skipped", dict\(result\.counts\)',
                "The runner reads the count before it writes anything.")
    out += ladder([
        ("Read the count", "<code>quality.critical</code> is the number of rows that broke a critical rule."),
        ("Release", "The stage result is released. The write edge is never called."),
        ("Record", "The step <code>silver.quality</code> is logged as failed, with all the counts and a message "
                   "that names the critical rules. The dataset run is finished with the status "
                   "<code>skipped</code> and the same message."),
        ("Leave the bookmark", "No bookmark is passed, so it stays where it was. The next run takes the same "
                               "batches again and is held again, until someone acts."),
    ])
    out += ("<p>Three consequences are worth spelling out. The good rows of the run are not written either. "
            "The quarantine records of the run are not written, because writing them is part of the write edge. "
            "And the hold covers every new batch of the dataset that this run picked up, because the stages ran "
            "once over all of them together.</p>")
    out += "<h2>A worked example</h2>"
    out += ("<p>Four sales order lines. The contract's rules in play are the critical rule on "
            "<code>ordered_qty</code>, the major rule on <code>plant_code</code> and the required-column rule on "
            "<code>dealer_code</code>.</p>")
    out += table(["order_id", "dealer_code", "plant_code", "ordered_qty", "What the stage does"], [
        ["SO-1001", "D-1001", "P-CHN", "40", "Goes on."],
        ["SO-1002", "D-1002", "P-XXX", "12", "Quarantined. Reason: <code>plant_code-invalidValues-1</code>."],
        ["SO-1003", "D-1003", "P-HUN", "-5", "Quarantined. Reason: <code>ordered_qty-sql-1</code>. Counted as critical."],
        ["SO-1004", "missing", "P-HUN", "8", "Quarantined. Reason: <code>dealer_code_present</code>. Counted as critical."],
    ])
    out += ("<p>The stage returns one good row, three quarantine records and a critical count of two. Because "
            "that count is above zero, the runner writes nothing, not even SO-1001. Had only SO-1002 been bad, "
            "SO-1001 would have been written and SO-1002 would have gone to the quarantine table.</p>"
            "<p>For a flag, take a press reading whose <code>cycle_state</code> is <code>resting</code>, which "
            "is not one of the four valid states. That rule is minor. The reading is kept in silver and its "
            "<code>_dq_flags</code> holds <code>[cycle_state-invalidValues-1]</code>.</p>")
    out += "<h2>The settings that drive it</h2>"
    out += table(["Setting", "What it means", "Allowed values", "Default"], [
        ["The contract's <code>required</code> on a column", "Makes a critical rule that the column is filled.", "true or false", "false"],
        ["The contract's <code>quality</code> entries on a column", "The row rules. Each has a severity.", "<code>critical</code>, <code>major</code>, <code>minor</code>. An unknown or missing severity counts as major.", "major"],
        ["<code>quality.actions.critical.rows</code> in the dataset manifest", "The action for the required-column rules.", "<code>quarantine</code>, <code>flag</code>, <code>none</code>", "<code>quarantine</code>"],
        ["<code>quality.actions.major.rows</code> and <code>quality.actions.minor.rows</code> in the dataset manifest", "The action for the contract's rules of that severity. It lays over what the store says: a dataset may flag what the contract would quarantine.", "<code>quarantine</code>, <code>flag</code>, <code>none</code>", "<code>quarantine</code> for major, <code>flag</code> for minor"],
    ])
    out += code(PLATFORM, r"^quality_defaults:", 4, "The tyre platform manifest states the same three defaults for the whole domain.")
    out += "<p>This stage has no switch.</p>"
    out += "<h2>How it is tested</h2>"
    out += (f"<p>{src(T_STAGES)} has three cases for the stage and a separate test that one bad record does not "
            f"stop the good rows of its batch. {src(T_RULES)} has a table of rules that must translate and a "
            f"table of rules that must not, each with the sentence expected. {src(T_RUNNER)} has the test for "
            "the hold.</p>")
    out += code(T_STAGES, r'"a good row passes"', 4, "The three stage cases. The third changes the critical action to flag in the manifest.")
    out += code(T_STAGES, r"^def test_one_poison_record_does_not_stop_the_load", caption="One bad row in three: the other two go on.")
    out += code(T_RULES, r'"sql count-where becomes a row condition"', 20, "Six rules that translate, each with the exact row rule expected.", cap=22)
    out += code(T_RULES, r'"table duplicates: kept by the duplicates stage"', 13, "Six rules that do not translate, each with the words the reason must contain.")
    out += code(T_RUNNER, r"^def test_a_critical_rule_broken_holds_the_batch", caption="The hold: nothing written, bookmark unmoved, the quality step failed.")
    out += not_built([
        "The dataset view does not carry the domain's quality defaults, so the defaults are written in the plan code.",
        "The other parts of an action in the manifest, alerting the owner and paging, are not read by this stage.",
        "Only two shapes of contract rule are translated: a list of valid values, and a counting query that must "
        "give zero. Every other check is reported in the run log and not run here.",
        "A list of valid values is written as quoted text even when the values are whole numbers, as for the "
        "material's rim size. The engine converts when it compares. I did not find a test for that on both engines.",
        "If a dataset changes the critical action to flag, the required-column rules no longer count as "
        "critical and no batch is held for them.",
        "The count <code>quality.flagged</code> counts kept rows with any flag at all, so it includes rows "
        "whose only flag is an emptied value from stage 4.",
        "The hold message tells the operator to void or resend the batch. I did not verify how a resent batch "
        "replaces the held one. That is on the ingestion side.",
        f"The hold exists on the job engine only. See {link('silver-engines', 'the two engines compared')}.",
    ])
    return out


# ------------------------------------------------------------------------------------------------------------ 10
@page("silver-duplicates", "Stage 10: duplicates, inside the batch and against silver", parent="silver-stages",
      short="10 Duplicates",
      blurb="Repeats inside the batch are removed by the dataset's rule. For appended events, a key already in silver is a replay and is dropped.")
def silver_duplicates():
    out = ("<p class='lead'>The tenth stage removes rows that say the same thing twice. It works in two steps. "
           "First it removes repeats among the new rows. Then, for datasets that only append, it removes new "
           "rows whose key is already in silver.</p>"
           "<p>Sources repeat themselves all the time: a file is sent twice, a stream replays an hour after a "
           "restart, a change feed delivers the same change in two files. Without this stage every repeat "
           "would be counted twice in every total built on silver.</p>")
    d, h = pic(("Good rows of the batch", "plus silver as it stands"), ("10 Duplicates", "rank within a group, keep rank 1"),
               [("Rows kept", "one per group", "bx"),
                ("Dropped", ["repeats inside the batch"], "bxm"),
                ("Replayed", ["key already in silver"], "bxm")])
    out += fig(990, h, d, "The duplicates stage keeps one row per group and counts the rest",
               "The two dashed boxes are counts. Dropped and replayed rows are not stored.")
    out += "<h2>The five ways rows are grouped</h2>"
    out += ("<p>The stage always does the same thing: it puts rows into groups, numbers the rows inside each "
            "group, and keeps number 1. What differs is how the groups are formed and which row gets number 1. "
            "The code checks the five cases in this order and takes the first that applies.</p>")
    out += table(["When", "A group is", "The row kept", "Why"], [
        ["<code>dedup.mode</code> is <code>none</code>", "Every row by itself", "Every row", "The dataset asked for no removal."],
        ["<code>dedup.mode</code> is <code>full_row</code>", "Rows equal in every contract column", "The first to arrive", "Only exact copies are repeats."],
        ["The history type is type 2", "Rows with the same key and the same order value", "The first to arrive",
         "A type 2 dataset must keep every distinct change. Two rows with the same key and the same change sequence are one change sent twice."],
        ["<code>dedup.mode</code> is <code>business_key_latest</code>", "Rows with the same key", "The one with the highest order value. On a tie, the last to arrive.", "Only the latest state matters."],
        ["Otherwise, which means <code>business_key_first</code>", "Rows with the same key", "The first to arrive", "An event happens once. Anything after the first is a repeat."],
    ])
    out += ("<p>The third line is easy to miss. Six of the nine tyre datasets say "
            "<code>business_key_latest</code>, and four of those keep type 2 history. If the stage took the "
            "mode literally for them, a batch holding three changes to one order line would be cut down to the "
            "last change, and the history stage would never see the other two. So for type 2 the stage ignores "
            "the word latest and keeps every distinct change.</p>")
    out += "<h2>A worked example: press readings</h2>"
    out += ("<p>Press readings are events. Their manifest says to keep the first row per "
            "<code>reading_id</code> and to look back two days in silver.</p>")
    out += code(TYRE_M + "press_stream.press_reading.yaml", r"^dedup:", 1, "The press reading manifest: first arrival per reading id.")
    out += code(TYRE_M + "press_stream.press_reading.yaml", r"late_lookback_days", 1, "The same manifest: how far back silver is checked for replays.")
    out += ("<p>The run's business date is 10 November. Silver already holds reading R-501, loaded in the "
            "window of 9 November. Four rows arrive, in this order.</p>")
    out += table(["reading_id", "press_id", "temperature_c", "Arrival order", "Result"], [
        ["R-502", "PR-07", "171.2", "1", "Kept."],
        ["R-502", "PR-07", "171.2", "2", "Dropped. It is the second row of its group."],
        ["R-501", "PR-07", "170.9", "3", "Replayed. The key is in silver within the last two days."],
        ["R-503", "PR-08", "168.4", "4", "Kept."],
    ])
    out += ("<p>Two rows go on. The counts are <code>duplicates.dropped = 1</code> and "
            "<code>duplicates.replayed = 1</code>. In the recorded 14-day run of the tyre company at full scale "
            f"(4 November 2026, local Spark, in {src(NOTE)}), day 13 replayed part of the stream, and this "
            "stage dropped 19,440 replayed readings, leaving silver equal to the oracle.</p>")
    out += "<h2>The settings that drive it</h2>"
    out += table(["Setting", "What it means", "Allowed values", "Default"], [
        ["<code>dedup.mode</code>", "How repeats inside the batch are recognised.", "<code>business_key_latest</code>, <code>business_key_first</code>, <code>full_row</code>, <code>none</code>", "<code>none</code>"],
        ["<code>dedup.business_keys</code>", "The columns that identify a row for this stage.", "A list of contract columns", "The contract's primary key"],
        ["<code>dedup.order_by</code>, or <code>load.history_order_by</code> when set", "The column that says which row is later.", "A contract column", "Empty"],
        ["<code>load.late_lookback_days</code>", "How many days of silver are checked for a replay, counted back from the run's business date.", "A whole number of days", "Not set. All of silver is checked."],
        switch_row("dedup"),
    ])
    out += "<p>This table shows what each tyre dataset declares and which of the five cases it therefore takes.</p>"
    out += table(["Dataset", "Mode", "History", "Case taken", "Checked against silver"], [
        ["Dealers", "business_key_latest", "type 2", "Same key and order value", "No"],
        ["Materials", "business_key_latest", "type 2", "Same key and order value", "No"],
        ["Presses", "business_key_latest", "type 2", "Same key and order value", "No"],
        ["Sales order lines", "business_key_latest", "type 2", "Same key and order value", "No"],
        ["Deliveries", "business_key_latest", "type 1", "Latest per key", "No"],
        ["Dealer sell-out", "business_key_latest, ordered by file name", "snapshot", "Latest per key", "No"],
        ["Production records", "business_key_first", "append", "First per key", "Yes, all of silver"],
        ["Inspection results", "business_key_first", "append", "First per key", "Yes, all of silver"],
        ["Press readings", "business_key_first", "append", "First per key", "Yes, the last 2 days"],
    ])
    out += "<h2>The code</h2>"
    out += code(ROWS, r"^def duplicates\(", caption="The whole stage: five ways to rank, then the check against silver.")
    out += code(COMMON, r"^ARRIVAL = ", r"return \[F\.col\(c\)\.asc_nulls_last",
                "What first to arrive means: the transport columns the frame has, oldest first.")
    out += ladder([
        ("Arrival order", "<code>arrival_order</code> builds a sort order from whichever of six columns the frame "
                          "has: the position in the change feed, the stream sequence, the arrival time, the "
                          "commit time, the file name and the batch key. Missing values sort last. If the frame "
                          "has none of them, a constant is used and the choice among equal rows is left to the engine."),
        ("Rank", "Each of the five cases adds a working column <code>_rn</code> with "
                 "<code>row_number()</code> over a window. A window is a grouping with an order inside each "
                 "group. <code>partitionBy</code> names the group and <code>orderBy</code> the order."),
        ("The latest case", "The order is the order column from highest to lowest, with missing values last, "
                            "and then arrival from newest to oldest. So if the same change is delivered twice, "
                            "the later delivery wins the tie."),
        ("Keep rank 1", "<code>kept</code> is the rows with <code>_rn = 1</code>. <code>dropped</code> is the rest, returned in the tally."),
        ("Decide whether to look at silver", "Three things must hold: the history type is append, silver "
                                             "exists, and the mode is not <code>none</code>."),
        ("Take the recent keys", "<code>recent</code> narrows silver to the lookback. The key columns are "
                                 "selected, made distinct, and given a marker column <code>_seen</code>."),
        ("Left join", "The kept rows are joined to those keys. A left join keeps every new row and fills "
                      "<code>_seen</code> only where the key was found."),
        ("Split on the marker", "Rows where <code>_seen</code> is null are new and go on. Rows where it is set "
                                "are replays and go into the tally <code>replayed</code>."),
    ])
    out += code(ROWS, r"^def recent\(", caption="Which part of silver counts as already loaded.")
    out += ("<p>If the dataset has no lookback, all of silver is used. Otherwise the function takes the run's "
            "business date, subtracts the lookback days, and keeps the silver rows whose <code>_window</code> "
            "is on or after that date. <code>_window</code> is the day of the bronze batch that carried the "
            "row. It is not the time of the event. Both sides are dates written as text in the same form, so "
            "comparing the texts compares the dates.</p>"
            "<p>The lookback is a trade. Checking every new reading against the whole readings table would "
            "grow more expensive every day. In the recorded run that table grew by 622,080 rows a day. Two days "
            "is enough for a stream that replays recent data, and the join stays the same size from day to "
            "day. The price is that a replay of older data is not recognised.</p>"
            "<p>Why only append datasets are checked against silver follows from the next stage. For history "
            "kinds, the history stage lays the new rows out together with what silver holds, and a change seen "
            "twice produces the same row key and changes nothing. An appended row has no such second chance.</p>")
    out += "<h2>How it is tested</h2>"
    out += (f"<p>{src(T_STAGES)} has five cases for repeats inside a batch and three for the check against "
            "silver. In the first table each row is an id, a value, an order value and a feed position.</p>")
    out += code(T_STAGES, r'\("no dedup", \{\}', 13, "The five cases inside a batch. The last is the type 2 case: three rows in, two distinct changes out.")
    out += code(T_STAGES, r'"a key already in silver is a replay \(DUP-04\)", None', 3, "The three cases against silver: no lookback, inside it, outside it.")
    out += code(T_STAGES, r"^def test_duplicates_against_silver\(", caption="Silver holds key a. The batch brings a and b.")
    out += not_built([
        "A replay older than the lookback is not seen and is appended again. The third test case states this behaviour.",
        "The replay check compares keys only. A row with a known key and different values is dropped as a "
        "replay, and nothing records that the values differed.",
        "Dropped and replayed rows are counted but not kept.",
        "The settings registry lists the modes <code>none</code>, <code>exact</code>, "
        "<code>business_key_first</code>, <code>business_key_latest</code> and <code>flag</code> for "
        "<code>dedup.mode</code>. The plan accepts <code>full_row</code> and refuses <code>exact</code> and "
        "<code>flag</code>. The two lists do not agree.",
        "With the stage switched off, the check against silver is switched off too.",
        f"On the declarative engine this stage cannot run inside the flow. See {link('silver-declarative', 'the declarative engine')}.",
    ])
    return out


# ------------------------------------------------------------------------------------------------------------ 11
@page("silver-keys", "Stage 11: keys, the business key and its hash", parent="silver-stages", short="11 Keys",
      blurb="Every part of the business key must be present. The parts are joined with a separator and hashed into _key_hash.")
def silver_keys():
    out = ("<p class='lead'>The eleventh stage makes sure every row can be identified, and gives it one column "
           "that stands for its identity. The business key is the set of contract columns that identify a row, "
           "such as the order id and the line number. The stage checks that none of them is empty and adds "
           "<code>_key_hash</code>, a hash of the key.</p>"
           "<p>A hash is a fixed-length fingerprint worked out from a text: the same text always gives the "
           "same fingerprint. With it, the history stage and the writer can group and join on one column of "
           "one type, whether a dataset's key is one text column or three columns of mixed types.</p>")
    d, h = pic(("Rows", "after duplicates"), ("11 Keys", "check the parts, hash the key"),
               [("Rows with _key_hash", "64 hex characters", "bx"),
                ("Quarantine", ["rows with an empty key part"], "bxm")])
    out += fig(990, h, d, "The keys stage adds the key hash or sets the row aside", "A row without a full key cannot be merged, so it does not go on.")
    out += flow3("Rows, and the contract's primary key columns from the plan.",
                 "Each key column is checked for a missing value. The parts are turned to text, joined with a separator and hashed.",
                 "Rows with a new column <code>_key_hash</code>. Rows with an empty key part go to quarantine.")
    out += "<h2>A worked example</h2>"
    out += ("<p>The sales order line's key is <code>order_id</code> and <code>line_no</code>. The symbol ␟ in "
            "the third column stands for the separator character, which is invisible.</p>")
    out += table(["order_id", "line_no", "Text that is hashed", "_key_hash"], [
        ["SO-1001", "10", "<code>SO-1001␟10</code>", "<code>d5474bf7d0a59c6d583e20daf204634aaf0a92fdfe3427b7af73ea0fc656a9eb</code>"],
        ["SO-1001", "missing", "none", "Quarantined: <code>business key line_no is empty</code>"],
    ])
    out += ("<p>The hash in the first row was worked out for this page with Python's SHA-256 over that text. "
            "The tests below prove that the stage gives the same result as Python for the same bytes.</p>")
    out += "<h2>The settings that drive it</h2>"
    out += table(["Setting", "What it means", "Allowed values", "Default"], [
        ["The contract's primary key", "The columns marked <code>primaryKey</code>, in the order of <code>primaryKeyPosition</code>.", "One or more contract columns", "None. The plan refuses a dataset without one."],
    ])
    out += code(TYRE_C + "sales_order_line.odcs.yaml", r'name: "order_id"', 2, "The two key columns of the sales order line contract.")
    out += ("<p>The stage has no switch and no other setting. It uses the contract's primary key, not the "
            "<code>dedup.business_keys</code> of the stage before.</p>")
    out += "<h2>The code</h2>"
    out += code(ROWS, r"^def keys\(", caption="The whole stage: three lines.")
    out += ladder([
        ("One check per key column", "The check is true when the column is null. Its text is "
                                     "<code>business key &lt;name&gt; is empty</code>."),
        ("Split", "<code>quarantine_rows</code> sets aside rows with any empty key part."),
        ("Hash", "The good rows get <code>_key_hash</code> from <code>key_hash</code>, which is the next thing to read."),
    ])
    out += code(COMMON, r"^SEP = ", 1, "The separator between key parts.")
    out += code(COMMON, r"^def text_parts\(", r"return F\.sha2\(text_parts", "How the parts become one text, and the text a hash.")
    out += ladder([
        ("Each part to text", "Every key column is cast to text. A whole number 10 becomes <code>10</code>. A "
                              "date becomes <code>2026-11-09</code>."),
        ("A stand-in for a missing part", "<code>coalesce</code> replaces a null part with the null mark, "
                                          "explained below."),
        ("Join with the separator", "<code>concat_ws</code> joins the parts with <code>SEP</code> between them."),
        ("Hash", "<code>sha2(..., 256)</code> is SHA-256. The result is 64 hex characters."),
    ])
    out += "<h3>Why a separator is needed</h3>"
    out += ("<p>Take two rows with a two-part key. One has the parts <code>ab</code> and <code>c</code>. The "
            "other has <code>a</code> and <code>bc</code>. Joined with nothing between them, both give "
            "<code>abc</code> and so the same hash. Two different rows would look like one, and the merge would "
            "overwrite one with the other. With a separator the texts are <code>ab␟c</code> and "
            "<code>a␟bc</code>, which differ.</p>"
            "<p>The separator must be a character that cannot appear in a key value. If it were a dash, the "
            "parts <code>a-b</code> and <code>c</code> would collide with <code>a</code> and <code>b-c</code>. "
            "The code uses U+001F, a control character whose name is unit separator and whose purpose is "
            "exactly this. Stage 3 removes control characters from text, and U+001F is in the set it removes, "
            "so a cleaned key value cannot contain it.</p>")
    out += "<h3>The null mark</h3>"
    out += code(COMMON, r"^def null_mark\(", caption="The text that stands for a missing part.")
    out += ("<p>In this stage a missing key part never reaches the hash, because the row is quarantined first. "
            "But <code>text_parts</code> is shared. The history stage uses it to fingerprint the tracked "
            "columns of a version, and those may be missing. There a null must not look like an empty text, so "
            "it is replaced by a single null character. The comment explains why it is written as "
            "<code>chr(0)</code>: a null character typed inside a SQL text does not survive on every engine.</p>")
    out += "<h3>What usually catches an empty key first</h3>"
    out += ("<p>In the tyre contracts every key column is also marked required. The quality stage, two stages "
            "earlier, has a critical rule for every required column. So an empty key part is normally caught "
            "there, and the batch is held. The quarantine in this stage acts when a key column is not marked "
            "required, or when the dataset has changed the critical action to flag.</p>")
    out += "<h2>How it is tested</h2>"
    out += (f"<p>{src(T_STAGES)} has two cases for the stage, a test that the hash tells the parts apart, and "
            "a table of exact values that must be the same on every engine.</p>")
    out += code(T_STAGES, r'\("keys present"', 2, "The two stage cases: a full key, and an empty part.")
    out += code(T_STAGES, r"^def test_keys_hash_is_stable_and_distinguishes_parts", caption="ab and c, a and bc, and ab and c again: two different hashes, not one and not three.")
    out += code(T_STAGES, r"^SAME = \[", 9, "Exact values: the hash must equal Python's SHA-256 of the parts joined by the separator.")
    out += code(T_STAGES, r"# a timestamp as text", 3, "One known difference between engines, kept in a table of its own.")
    out += not_built([
        "A key part that is a timestamp hashes differently on Spark and on DuckDB, because the two write a "
        "timestamp as text differently. The test file records this. A dataset with a timestamp in its key "
        "must stay on one engine. Text, whole number and date parts hash the same on both.",
        "The hash is of the text form. The whole number 10 and the text <code>10</code> give the same hash.",
        "The stage does not check that a key is unique. That is the work of the duplicates and history stages.",
        "A key part that is an empty text after hygiene is already null, so it is caught. With hygiene "
        "switched off, an empty text would count as present.",
    ])
    return out


# ------------------------------------------------------------------------------------------------------------ 13
@page("silver-provenance", "Stage 13: provenance, where each row came from", parent="silver-stages",
      short="13 Provenance",
      blurb="Stamps the silver run and load time, keeps the batch and file, adds a record hash, and fixes the column order.")
def silver_provenance():
    out = ("<p class='lead'>The last stage finishes the row. It records which silver run handled it and when, "
           "keeps the facts about the bronze batch it came from, adds a hash of its business values, and puts "
           "the columns in one fixed order.</p>"
           "<p>Provenance means the record of where something came from. When a number in a report looks "
           f"wrong, these columns are how you walk back from the silver row to the run, the batch and the file. "
           f"The stage before this one is {link('silver-history', 'stage 12, history')}, which has its own pages.</p>")
    d, h = pic(("Rows from history", "with _row_key and _action"), ("13 Provenance", "rename, hash, stamp, reorder"),
               [("Finished silver rows", ["business columns first,", "silver's columns last"], "bx")])
    out += fig(990, h, d, "Provenance stamps the rows and fixes the column order", "No row is added, removed or set aside.")
    out += flow3("The rows history produced, the run id and the load time from the context.",
                 "One column is renamed, three are added, and the columns are put in order.",
                 "The rows exactly as the write edge will write them.")
    out += "<h2>A worked example</h2>"
    out += ("<p>One dealer row, shown as a list because it is wide. The left column is the column name in the "
            "order the stage produces. The values are made up, and long values are cut.</p>")
    out += table(["Column", "Value", "Where it comes from"], [
        ["<code>dealer_code</code>", "D-1001", "Business column, in contract order"],
        ["<code>dealer_name</code>", "Sharma Tyres", "Business column"],
        ["<code>region</code>", "north", "Business column"],
        ["<code>contact_email</code>", "tok_5c1e...", "Business column"],
        ["<code>contact_phone</code>", "tok_9a40...", "Business column"],
        ["<code>updated_at</code>", "2026-11-09 06:00:00", "Business column"],
        ["<code>segment</code>", "retail", "Another source column, not in the contract. These come next, in alphabetical order."],
        ["<code>_batch_key</code>", "b-17", "Stamped by the lander, carried through"],
        ["<code>_source_file</code>", "dealers_page_3.json", "Stamped by the lander"],
        ["<code>_window</code>", "2026-11-09", "Stamped by the lander"],
        ["<code>_key_hash</code>", "9b1c...", "Stage 11"],
        ["<code>_record_hash</code>", "e07a...", "This stage"],
        ["<code>_valid_from</code>, <code>_is_current</code>, <code>_row_key</code> and the other history columns", "...", "Stage 12"],
        ["<code>_dq_flags</code>", "[]", "Stages 2, 4 and 9"],
        ["<code>_bronze_run_id</code>", "ingest-2026-11-09", "The lander's <code>_run_id</code>, renamed by this stage"],
        ["<code>_silver_run_id</code>", "silver-2026-11-09", "This stage"],
        ["<code>_silver_loaded_at</code>", "2026-11-09 06:20:11", "This stage"],
        ["<code>_action</code>", "upsert", "Stage 12. The write edge uses it and does not store it."],
    ])
    out += "<h2>The settings that drive it</h2>"
    out += ("<p>The stage reads no dataset setting. It uses the plan's list of kept columns, the run id and the "
            "load time from the context, and three fixed lists of column names in the plan module. It has no switch.</p>")
    out += code(PLAN, r"^LOAD_COLUMNS = ", 2, "The columns the lander stamps on every bronze row, and the transport columns some sources add.")
    out += "<h2>The code</h2>"
    out += code(ROWS, r"^def provenance\(", caption="The whole stage.")
    out += ladder([
        ("The business columns", "<code>business</code> is the names of the kept contract columns, in contract order."),
        ("Rename the bronze run", "Bronze calls the run that loaded the row <code>_run_id</code>. In silver there "
                                  "are two runs to tell apart, so the column becomes <code>_bronze_run_id</code>."),
        ("The record hash", "The business columns are packed into one structure, written as JSON text, and "
                            "hashed with SHA-256. More on this below."),
        ("The silver run", "<code>_silver_run_id</code> is the run id from the context, the same on every row of the run."),
        ("The load time", "<code>_silver_loaded_at</code> is the load time from the context, as a timestamp. "
                          "The stage does not read the clock. If no load time was passed, the column is null."),
        ("The fixed columns", "<code>fixed</code> is the load columns, then the transport columns, then silver's own columns, then <code>_action</code>."),
        ("Everything else", "<code>extra</code> is every column that is neither a business column nor a fixed "
                            "one, sorted by name. These are the source columns the contract does not list, and "
                            "columns added on the way such as a local time."),
        ("Reorder", "The final order is business, then extra, then the fixed columns that the frame actually "
                    "has. <code>_run_id</code> is left out because it was renamed."),
    ])
    out += ("<p>The fixed order matters more than it looks. Rows from two batches are stacked by column name "
            "elsewhere, but a table whose columns come in a stable, predictable order is easier to read, to "
            "compare and to test. The test for this stage checks that the first three columns are the "
            "contract's.</p>")
    out += "<h3>How the record hash is computed</h3>"
    out += ("<p>Take the test's row: <code>id</code> is a, <code>v</code> is 1, <code>seq</code> is 2. The "
            "three steps are these.</p>")
    out += table(["Step", "Result"], [
        ["<code>F.struct(...)</code> packs the business columns, in contract order", "one value with three named fields"],
        ["<code>F.to_json(...)</code> writes it as text", '<code>{"id":"a","v":1,"seq":2}</code>'],
        ["<code>F.sha2(..., 256)</code> hashes the text", "<code>a54293011ea10149452d63d0aecefc4d8b15607ca6493bc30910599c25022f31</code>"],
    ])
    out += ("<p>JSON is used and not a plain join of the values, because JSON carries the column names and "
            "the types. The number 1 and the text <code>\"1\"</code> are written differently, and a value "
            "cannot slide from one column into the next. So the key hash needs a separator and the record "
            "hash does not.</p>"
            "<p>The hash covers the kept contract columns only. It does not cover other source columns, "
            "<code>_extra</code>, or any underscore column. Two rows with the same business values from two "
            "different batches have the same record hash.</p>"
            "<p>One difference between engines is recorded in the tests. Spark leaves a missing field out of "
            "the JSON. DuckDB writes it as null. The texts differ, so the hashes differ. A record hash must "
            "therefore be compared only with hashes made by the same engine.</p>")
    out += "<h2>How it is tested</h2>"
    out += (f"<p>{src(T_STAGES)} has six cases, one per column the stage is responsible for, a test that the "
            "record hash changes with a business value, and exact values in the table of engine checks.</p>")
    out += code(T_STAGES, r'"the bronze run kept under its own name"', 6, "The six cases. Each names a column and a check on its value.")
    out += code(T_STAGES, r"^def test_provenance\(", caption="The test body. It also checks the column order and that _run_id is gone.")
    out += code(T_STAGES, r"^def test_record_hash_changes_with_a_business_value", caption="Rows a and b differ in the id, c in the value: three different hashes.")
    out += code(T_STAGES, r"record hash: business values as JSON", 2, "The exact value: the hash equals SHA-256 of the JSON text.")
    out += code(T_STAGES, r"# SQLFrame's to_json keeps null fields", 3, "The known difference when a business value is missing.")
    out += not_built([
        "Nothing in the stages reads <code>_record_hash</code> back. The history stage drops it when it takes "
        "silver's rows into its working shape, and this stage computes it again. Type 2 history decides whether "
        "a version changed from its own fingerprint of the tracked columns. The record hash is there for people "
        "and for later checks, not for the engine's decisions.",
        "The record hash differs between Spark and DuckDB for a row with a missing business value.",
        "A rerun of the same batch gives the same business values and the same row key, but a new silver run id "
        "and a new load time. So a rerun's merge rewrites these two columns on the rows it touches. This is my "
        "reading of the merge, which sets every column of a matched row.",
        "The settings registry has a key <code>processor_groups.provenance</code>. No code reads it. The stage cannot be switched off.",
        "The load time is one value for the whole run, not the moment each row was written.",
    ])
    return out
