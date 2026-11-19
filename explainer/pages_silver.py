"""Bronze to silver: the hub, how the runner picks what to read, the plan, and the write edge."""
from lib import *  # noqa: F401,F403

RUNNER = "P/src/dataplatform/silver/runner.py"
PLAN = "P/src/dataplatform/silver/plan.py"
EDGE = "P/src/dataplatform/adapters/spark_silver.py"
PORTS = "P/src/dataplatform/silver/ports.py"
TYRE = "C/slices/02-tyre/manifest/datasets/"


def shape():
    d = box(10, 70, 150, 90, "Metadata store", ["settings, columns,", "keys, file log"], "bxs", href="meta.html")
    d += box(10, 230, 150, 90, "Bronze", ["every batch,", "as received"], "bx", href="ingest-lander.html")
    d += box(230, 70, 170, 90, "Plan", ["one object per", "dataset"], "bx", href="silver-plan.html")
    d += box(230, 230, 170, 90, "Reader", ["only the new", "batches"], "bx", href="silver-reader.html")
    d += box(470, 150, 200, 90, "Thirteen stages", ["plain functions:", "rows in, rows out"], "solid",
             href="silver-stages.html")
    d += box(740, 70, 150, 90, "Writer", ["one merge", "per dataset"], "bx", href="silver-writer.html")
    d += box(740, 230, 150, 90, "Quarantine", ["bad rows, with", "the reason"], "bxm", href="silver-quality.html")
    d += box(950, 70, 130, 90, "Silver", ["clean, keyed,", "with history"], "bx")
    d += arrow(160, 115, 228, 115) + arrow(160, 275, 228, 275)
    d += arrow(400, 115, 468, 180) + arrow(400, 275, 468, 215)
    d += arrow(670, 180, 738, 115) + arrow(670, 215, 738, 275)
    d += arrow(890, 115, 948, 115)
    d += text(545, 30, "The runner drives all of it and writes every step to the run log", "tx", "middle")
    return d


@page("silver", "Bronze to silver: from data as received to data you can trust", parent="index",
      short="Bronze to silver",
      blurb="One plan per dataset, thirteen stages in a fixed order, one write. History and deletes are decided here.")
def silver():
    out = ("<p class='lead'>Bronze holds every batch exactly as the source sent it, mistakes and repeats included. "
           "Silver is the first layer people and gold can trust: one row per business key or per version, typed, "
           "cleaned, with deletes marked and history kept.</p>"
           "<p>The work between the two is the same for every dataset. Nobody writes a job per table. One piece of "
           "code reads the dataset's settings from the metadata store and runs the same thirteen stages in the "
           "same order. What changes from one dataset to the next is only the settings.</p>")
    out += fig(1090, 340, shape(), "The parts of bronze to silver",
               "Read it left to right. The highlighted box is where the rows are changed. Click any box to zoom in.")
    out += "<h2>One run for one dataset, in order</h2>"
    out += ladder([
        ("Take the lease", "The runner takes a lease on the dataset. A lease is a short lock with a time limit, "
                           "kept in the metadata store. A second run that starts at the same time finds the dataset "
                           "busy and writes nothing."),
        ("Build the plan", f"The dataset's merged settings, contract columns and keys become one {link('silver-plan', 'plan')} "
                           "object. A setting the engine does not know stops the dataset here, before any data is read."),
        ("Pick the batches", f"The {link('silver-reader', 'reader')} asks the file log which bronze batches were loaded since "
                             "silver last ran, and reads only those."),
        ("Run the stages", f"The new rows and the silver table as it stands go through the {link('silver-stages', 'thirteen stages')}. "
                           "The stages never read or write. They return the rows to write, the rows to set aside, and counts."),
        ("Hold on a critical failure", "If any row broke a rule marked critical, nothing is written and the bookmark "
                                       "stays where it was. The batch stays blocked until someone voids it or the source resends it."),
        ("Write once", f"The {link('silver-writer', 'writer')} applies all rows in one merge and appends the set-aside rows to the "
                       "quarantine table."),
        ("Record and move the bookmark", "Each stage's counts go to the step log. The bookmark moves only when the "
                                         "dataset run succeeded, so a failed run is taken again whole next time."),
    ])
    out += "<h2>The four design decisions that shape this layer</h2>"
    out += table(["Decision", "What it means", "Why"], [
        ["The stages are fixed and ordered",
         "Every dataset passes the same thirteen stages in the same order. A dataset can switch six of them off. "
         "It cannot add a stage or reorder them.",
         "A fixed list can be tested once and trusted everywhere. A setting outside the list is refused, so nothing "
         "is silently ignored."],
        ["The stages are pure functions",
         "A stage receives rows and returns rows. Reading bronze and writing silver happen only at the two edges.",
         "Pure functions run the same on Spark and on DuckDB, so the same tests run on a laptop in seconds and on "
         "the cluster."],
        ["History is chosen by the kind of dataset",
         "Master data keeps every version (type 2). Transactions keep the latest state (type 1). Events are "
         "appended. Snapshots replace their window.",
         "The kind is a fact about the data. The dataset states its kind once, and the right history follows."],
        ["Deletes are soft",
         "A row deleted at the source stays in silver, marked deleted, with the time. Nothing is removed unless "
         "the dataset asks for that.",
         "Gold and auditors need to know that a row existed and when it went away."],
    ])
    out += "<h2>What a silver table looks like</h2>"
    out += ("<p>Every silver table has the business columns first, then the columns silver adds. The added columns "
            "all start with an underscore. A row is live when <code>_is_current</code> is true and "
            "<code>_is_deleted</code> is false.</p>")
    out += table(["Column", "What it holds"], [
        ["<code>_key_hash</code>", "A hash of the business key. Every later step joins and groups on it."],
        ["<code>_row_key</code>", "The identity of the row in silver. The writer merges on it. For type 2 it is the key plus the version's start, so each version is its own row."],
        ["<code>_seq_from</code>", "For type 2: the source order value at which the version starts."],
        ["<code>_valid_from</code>, <code>_valid_to</code>", "The time span during which the version was true. An open end means it is still true."],
        ["<code>_is_current</code>", "True on the latest version of a key."],
        ["<code>_is_deleted</code>, <code>_deleted_at</code>", "Set when the source deleted the row, with the time of the delete."],
        ["<code>_dq_flags</code>", "The names of the minor quality rules the row broke. The row is kept."],
        ["<code>_record_hash</code>", "A hash of the business values of the row. Nothing in the platform reads it yet."],
        ["<code>_batch_key</code>, <code>_source_file</code>, <code>_bronze_run_id</code>, <code>_silver_run_id</code>, <code>_silver_loaded_at</code>",
         "Where the row came from and which runs carried it. This is the row's provenance."],
    ])
    out += code(PLAN, r"^SILVER_COLUMNS = \(", r"^\)", "The columns silver adds, as the code lists them.")
    out += "<h2>Two engines, one set of stages</h2>"
    out += (f"<p>The stages can run in two ways. The <b>job engine</b> is the default: our own runner calls the "
            f"stages and writes with one merge. The <b>declarative engine</b> runs the same stage functions inside "
            f"a declarative pipeline and lets the pipeline's change capture keep the history. The setting "
            f"<code>silver_engine</code> picks one per dataset. Both must pass the same table of history cases. "
            f"See {link('silver-declarative')} and {link('silver-engines')}.</p>")
    out += "<h2>How we know it is right</h2>"
    out += ("<p>In the recorded 14-day run of the tyre company at full scale (4 November 2026, local Spark), "
            "294 numbers were compared between silver and the oracle over the 14 days, and all were equal. The "
            f"oracle is the simulated company's own record of what is true. {link('tests')} explains it. "
            f"The run's table is in {src('P/docs/W1C-SILVER.md')}.</p>")
    out += not_built([
        "The plan does not yet receive the domain's quality defaults from the metadata store, so the severity "
        "actions fall back to defaults written in the plan code.",
        "On the declarative engine, three of the nine tyre datasets are refused (two full copies that detect deletes "
        "by key compare, and the snapshot). They run on the job engine.",
        "A critical rule holds the whole batch on the job engine. The declarative engine quarantines the rows but "
        "cannot hold the batch.",
    ])
    return out


@page("silver-reader", "The reader: how the runner picks what to read from bronze", parent="silver",
      short="Reader: picking batches",
      blurb="A bookmark in the metadata store says how far silver has read. Only loaded batches after it are taken.")
def silver_reader():
    out = ("<p class='lead'>Silver must read each bronze batch exactly once, even after a crash, a rerun or a "
           "reload. It does not look at timestamps in the data to decide this. It asks the metadata store.</p>"
           "<p>Two things in the metadata store make this work. The <b>file log</b> has one record for every "
           "batch ingestion handled, oldest first, with a status. The <b>bookmark</b> named "
           "<code>silver.files</code> is a number: how many file log records of this dataset silver has already "
           "passed.</p>")
    d = ""
    stat = [("1", "loaded", "bxs"), ("2", "loaded", "bxs"), ("3", "held", "bxm"), ("4", "loaded", "solid"),
            ("5", "void", "bxm"), ("6", "loaded", "solid")]
    for i, (n, s, k) in enumerate(stat):
        d += box(20 + i * 150, 70, 130, 70, f"record {n}", s, k)
    d += arrow(320, 30, 320, 66, "bookmark = 2")
    d += text(470, 175, "Records 1 and 2 were taken by an earlier run. This run looks at records 3 to 6", "tx", "middle")
    d += text(470, 200, "and takes only the two that are loaded. The bookmark then moves to 6.", "tx", "middle")
    out += fig(930, 220, d, "The file log of one dataset and the silver bookmark",
               "Each box is one record in the file log. The highlighted boxes are the batches this run reads.")
    out += "<h2>The rules, one at a time</h2>"
    out += table(["Situation", "What the reader does", "Why"], [
        ["A batch has status <code>loaded</code>", "Takes it.", "It is in bronze and passed ingestion's checks."],
        ["A batch was held or skipped by ingestion", "Does not take it. The bookmark still moves past it.",
         "It never reached bronze, so there is nothing to read."],
        ["A batch was voided", "Never takes it.", "Someone declared the batch bad. Voiding is how a bad batch is taken out."],
        ["The same batch key appears twice", "Takes it once.", "A resent file has the same batch key. The stages would cope, but reading once is cheaper."],
        ["A batch has load type <code>baseline</code>", "Skips it in a normal run.",
         "A baseline batch is a compacted copy of what silver already holds, written by housekeeping before old bronze is removed. Only a rebuild reads it."],
        ["The dataset is being rebuilt", "Starts from record 0 and takes every loaded batch again.",
         "A rebuild drops the silver table and replays bronze, oldest first."],
        ["The dataset run fails", "The bookmark does not move.", "The next run takes the same batches again, whole."],
    ])
    out += code(RUNNER, r"^def new_batches\(", caption="The whole selection rule. It returns the batch keys to read and the new bookmark.")
    out += ("<p>Walk through it. The function reads the dataset's file log. It starts at record 0 for a rebuild, "
            "otherwise at the bookmark. It then walks the remaining records in order. A baseline record is "
            "skipped unless this is a rebuild. A record is taken when its status is <code>loaded</code> and its "
            "batch key has not been taken already in this pass. The second value returned is the length of the "
            "file log, which becomes the new bookmark if the run succeeds.</p>")
    out += "<h2>Reading the rows</h2>"
    out += ("<p>With the batch keys in hand, the read edge loads the bronze table and keeps the rows whose "
            "<code>_batch_key</code> is one of them. Ingestion stamped that column on every bronze row. The read "
            "edge also hands over the silver table as it stands, because the history stage needs to see what "
            "silver already holds.</p>")
    out += code(EDGE, r"    def bronze\(self, plan", r"return self._read\(self.silver_path\(plan.source, plan.dataset\)\)",
                "The read edge on Delta: new bronze rows by batch key, and silver as it stands.")
    out += "<h2>Where the bookmark moves</h2>"
    out += code(RUNNER, r"    keys, mark = new_batches", r"return SilverDataset\(source, dataset, \"succeeded\", \{\"batches\": 0\}",
                "Nothing new, or only held batches: the two early exits.")
    out += ("<p>There are two early exits. If the file log has not grown, the dataset run is recorded as skipped "
            "and nothing else happens. If the file log grew but none of the new records is loaded, the run "
            "succeeds with zero batches and the bookmark moves past them. In every other case the bookmark is "
            "passed to <code>finish_dataset_run</code> at the very end, together with the success status. The "
            "metadata store saves the two in one transaction, so the bookmark can never move for a run that did "
            "not finish.</p>")
    out += note("Why not read by a timestamp in bronze?",
                "<p>A timestamp bookmark misses a batch that is reloaded with an older time, and reads a batch "
                "twice when two runs overlap. The file log is written by ingestion in the same transaction that "
                "marks a batch loaded, so it is the one list that is always complete and in order.</p>")
    out += "<h2>How it is tested</h2>"
    out += (f"<p>{src('P/tests/dataplatform/test_silver_runner.py')} runs the runner with in-memory fakes behind "
            "the ports. Its cases cover which batches are taken, the bookmark after success and after failure, "
            "the hold on a critical rule, and a rebuild.</p>")
    out += not_built(["The reader does not yet read bronze through a table change log. It trusts the file log and "
                      "the batch key column."])
    return out


@page("silver-plan", "The plan: a dataset's settings turned into one object", parent="silver", short="The plan",
      blurb="Everything the stages need to know about a dataset, checked before any data is read.")
def silver_plan():
    out = ("<p class='lead'>The stages never read the metadata store. They receive one object, the plan, that "
           "already holds every answer: the columns, the key, the kind of history, how duplicates are removed, "
           "how deletes are handled, and which rules apply.</p>"
           "<p>Building the plan is also the gate. If a dataset's settings ask for something the engine cannot "
           "do, the plan refuses with a message that names the dataset and the setting, and no data is touched.</p>")
    d = box(10, 30, 190, 70, "Dataset manifest", "history, load, dedup", "bx")
    d += box(10, 120, 190, 70, "Contract", "columns, types, key", "bx")
    d += box(10, 210, 190, 70, "Merged settings", "default, domain, dataset", "bx", href="meta-settings.html")
    d += box(290, 120, 190, 70, "dataset_view", "one JSON document", "bxs", href="meta-functions.html")
    d += box(570, 120, 190, 70, "plan_from_view", "checks, then builds", "solid")
    d += box(850, 120, 130, 70, "SilverPlan", "frozen", "bx")
    d += arrow(200, 65, 288, 140) + arrow(200, 155, 288, 155) + arrow(200, 245, 288, 172)
    d += arrow(480, 155, 568, 155) + arrow(760, 155, 848, 155)
    out += fig(990, 300, d, "From three sources of settings to one plan",
               "The metadata store merges the three sources into one document. The plan is built from that document alone.")
    out += "<h2>What the plan holds</h2>"
    out += code(PLAN, r"^class SilverPlan:", r"measures: tuple\[Measure", "Every field of the plan. It is frozen: no stage can change it.")
    out += table(["Field", "Where it comes from", "Which stage uses it"], [
        ["<code>columns</code>", "The contract's columns, each with the manifest's per-column settings laid over it", "Stages 1 to 7"],
        ["<code>keys</code>", "The contract's primary key", "Keys, history"],
        ["<code>history</code>, <code>kind</code>", "<code>history.type</code> if given, else chosen from <code>history.kind</code>", "History"],
        ["<code>order_by</code>", "<code>load.history_order_by</code>, else <code>dedup.order_by</code>", "Duplicates, history"],
        ["<code>track</code>", "<code>history.track_columns</code>. Empty means every non-key column.", "History, type 2"],
        ["<code>dedup_mode</code>, <code>dedup_keys</code>", "<code>dedup.mode</code>, <code>dedup.business_keys</code>", "Duplicates"],
        ["<code>delete_detect</code>, <code>delete_apply</code>", "<code>load.deletes.detect</code>, <code>load.deletes.apply</code>", "History"],
        ["<code>lookback_days</code>", "<code>load.late_lookback_days</code>", "Duplicates, for appended events"],
        ["<code>rules</code>", "Required columns, the quality actions, and the contract's row rules", "Quality"],
        ["<code>disabled</code>", "The <code>processor_groups.*</code> settings that say <code>disabled</code>", "The stage runner"],
        ["<code>token_pattern</code>", "The deployment's token format, passed in by the binding", "Personal data"],
    ])
    out += "<h2>History follows the kind of dataset</h2>"
    out += code(PLAN, r"^HISTORY_BY_KIND", 2, "The default history for each kind, and the five history types the engine knows.")
    out += ("<p>A dataset states its kind once: master, transaction, event or snapshot. The plan picks the "
            "history type from the kind. A dataset can override it. In the tyre example the sales order line is "
            "a transaction, which would normally keep only the latest state, but its manifest asks for type 2 so "
            "that every change to the promised date is kept.</p>")
    out += code(TYRE + "erp.sales_order_line.yaml", r"^history:", r"^load:|^quality:|^reconciliation:", "The sales order line's manifest: a transaction that asks for type 2 history on three columns.", cap=22)
    out += "<h2>The checks, in the order the code makes them</h2>"
    out += code(PLAN, r"^def plan_from_view\(", r"if projection not in", "The first half of plan_from_view: every refusal.", cap=50)
    out += ladder([
        ("Stray column settings", "The manifest has settings for a column the contract does not have. Refused."),
        ("No primary key", "The contract names no primary key. Silver cannot identify a row without one. Refused."),
        ("Unknown kind or history type", "The kind is not one of the four, or the history type is not one of the five. Refused."),
        ("No order for keyed history", "Type 1 and type 2 need an order column, because changes must be laid out in the source's order, not in arrival order. Refused if neither order setting is present."),
        ("History columns not in the contract", "A tracked column or the order column is not a contract column. Refused."),
        ("Unknown delete or duplicate setting", "The value is not in the allowed list. Refused."),
        ("Stage switches", "For each of the six optional stages, the plan reads its <code>processor_groups</code> setting. The word <code>disabled</code> switches the stage off."),
    ])
    out += code(PLAN, r"^SWITCHES = \{", r"^\}", "The six stages a dataset may switch off, and the setting that does it. The other seven always run.")
    out += "<h2>Two times the plan works out for you</h2>"
    out += ("<p>The plan decides two time columns so that the stages do not have to guess. The <b>effective "
            "time</b> says when a change took effect, and gives versions their validity. For a change feed it is "
            "the commit time the feed carried. Otherwise it is the order column when that column is a timestamp. "
            "The <b>event time</b> is the time of an appended event. It is the watermark column when that is a "
            "timestamp, and otherwise the effective time.</p>")
    out += "<h2>How it is tested</h2>"
    out += (f"<p>{src('P/tests/dataplatform/test_silver_plan.py')} is one table of cases. It builds the plan for "
            "every tyre dataset, checks each refusal with its message, checks the stage order, and checks that "
            "the silver kernel imports no engine.</p>")
    out += not_built(["The dataset view does not carry the domain's quality defaults yet, so the plan falls back "
                      "to defaults written beside it in the code.",
                      "Keeping only the contract's columns (projection <code>declared</code>) exists in the plan "
                      "but has no setting in the manifest yet."])
    return out


@page("silver-writer", "The writer: one merge per dataset, and why a rerun changes nothing", parent="silver",
      short="Writer",
      blurb="The stages decide what each row becomes. The writer only applies the decision, in one transaction.")
def silver_writer():
    out = ("<p class='lead'>By the time rows reach the writer, every decision is made. The history stage has given "
           "each row a <code>_row_key</code> and an <code>_action</code>. The writer's only job is to apply them "
           "to the silver table in one transaction.</p>"
           "<p>This split is deliberate. All the logic about versions, late changes and deletes sits in pure "
           "functions that are easy to test. The writer has no logic to get wrong.</p>")
    d = box(10, 40, 200, 80, "Rows from the stages", ["_row_key, _action"], "bx")
    d += box(300, 10, 220, 60, "action = upsert", "key matches: update", "bxs")
    d += box(300, 80, 220, 60, "action = upsert", "no match: insert", "bxs")
    d += box(300, 150, 220, 60, "action = delete", "key matches: remove row", "bxs")
    d += box(620, 40, 160, 80, "MERGE", "on _row_key", "solid")
    d += box(860, 40, 120, 80, "Silver", "", "bx")
    d += arrow(210, 80, 298, 40) + arrow(210, 80, 298, 110) + arrow(210, 90, 298, 180)
    d += arrow(520, 40, 618, 70) + arrow(520, 110, 618, 85) + arrow(520, 180, 618, 100)
    d += arrow(780, 80, 858, 80)
    out += fig(990, 230, d, "The three things a merge can do with a row",
               "Every row carries its own instruction. The merge matches on the row key and follows it.")
    out += "<h2>What the two columns mean</h2>"
    out += table(["History type", "What <code>_row_key</code> is", "So the merge does this"], [
        ["Type 1", "The hash of the business key", "One row per key. A change overwrites it."],
        ["Type 2", "The hash of the business key and the version's start", "One row per version. A new version is an insert. Closing the old version is an update of that version's row."],
        ["Append", "The hash of the business key", "Rows are appended. No merge is needed."],
        ["Snapshot", "The hash of the business key and the batch", "A newer file for the same window adds its rows and updates the older rows to not current."],
    ])
    out += ("<p>The action is <code>upsert</code> for a row to write. It is <code>delete</code> in two cases. The "
            "first is a stale row: when a late change rebuilds a key's history, a version row that the new "
            "history no longer contains is removed. The second is a dataset whose delete setting is "
            "<code>remove</code>, where a deleted key's rows are taken out instead of marked.</p>")
    out += code(EDGE, r"    def write\(self, plan", r"        return written", "The write edge. Quarantine first, then append or merge.")
    out += ladder([
        ("Quarantine", "If any stage set rows aside, they are appended to the dataset's quarantine table, with the stage and the reason."),
        ("Nothing to write", "If the stages produced no rows to write or remove, the writer returns zero."),
        ("First load or an append dataset", "The rows are appended. A merge needs a table to merge into, and events only ever append."),
        ("Everything else", "One merge into the silver table."),
    ])
    out += code(EDGE, r"    def _merge\(self, rows", r"            \.execute\(\)", "The merge. New columns are added first, then one statement applies every row.", cap=40)
    out += ("<p>Three things to notice. First, the match is on <code>_row_key</code> only. Second, a new column "
            "in the source reaches silver without a release: the writer adds any column the table does not have "
            "before merging. Third, the three clauses map one to one to the picture above.</p>")
    out += "<h2>Why running the same batch twice changes nothing</h2>"
    out += ("<p>A write that can be repeated safely is called idempotent. Silver gets this from two places.</p>"
            "<p>For merges, the row key and the version boundaries are computed from the data, never from the "
            "run. The same batch produces the same row keys and the same values, so the merge finds every row "
            "already there and updates it to what it already is.</p>"
            "<p>For appends, the writer tags the write with an application name and a transaction number, which "
            "is the bookmark value. Delta remembers the last number per application and ignores a write that "
            "carries a number it has already seen.</p>")
    out += code(EDGE, r"    def _append\(self, df", 3, "The idempotent append: the same application and number is written once.")
    out += "<h2>How it is tested</h2>"
    out += (f"<p>{src('P/tests/dataplatform/test_silver_ports.py')} is the contract table for the silver tables "
            "port. Each adapter must pass the same cases: a write, the measures after it, the same append twice, "
            "removal of a stale row, a new column arriving, quarantine, and the bronze read.</p>")
    out += not_built(["Type 2 history is written as one merge of whole version rows today. A full copy that "
                      "changes many keys rewrites more files than a merge of only the differences would.",
                      "The transaction number guard covers appends. Merges rely on computed row keys instead."])
    return out
