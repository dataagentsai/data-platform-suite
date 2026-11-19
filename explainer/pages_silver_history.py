"""Stage 12, history: the hub, the five history types, deletes, and the two engines that keep history."""
from lib import *  # noqa: F401,F403

HIST = "P/src/dataplatform/stages/history.py"
PLAN = "P/src/dataplatform/silver/plan.py"
CONTRACT = "P/src/dataplatform/silver/history_contract.py"
ENGINES = "P/src/dataplatform/silver/engines.py"
DECL = "P/src/dataplatform/silver/declarative.py"
REALM = "P/realms/databricks/declarative/silver.py"
ROWS = "P/src/dataplatform/stages/rows.py"
STEPS = "P/src/dataplatform/ingest/steps.py"
PIPES = "P/src/dataplatform/ingest/pipelines.py"
EDGE = "P/src/dataplatform/adapters/spark_silver.py"
LOADER = "P/src/dataplatform/meta/loader.py"
T_STAGES = "P/tests/dataplatform/test_silver_stages.py"
T_ENGINES = "P/tests/dataplatform/test_silver_engines.py"
T_DECL = "P/tests/dataplatform/test_silver_declarative.py"
T_STEPS = "P/tests/dataplatform/test_ingest_steps.py"
T_PIPES = "P/tests/dataplatform/test_ingest_pipelines.py"
SPIKE = "P/spikes/sdp_silver/"
NOTES = "P/docs/W1C-SILVER.md"
TYRE = "C/slices/02-tyre/manifest/datasets/"
RUN = "the recorded 14-day run of the tyre company at full scale (4 November 2026, local Spark)"


def yes(flag):
    return "yes" if flag else "no"


def clock(minute):
    return "open" if minute is None else f"{minute // 60:02d}:{minute % 60:02d}"


def worked(n, title, proves, batches, after, extra=""):
    """One case of the history contract as two small tables: the changes in arrival order, and silver afterwards."""
    out = f"<h3>Case {n}: {title}</h3><p>{proves}</p>"
    rows = []
    for i, batch in enumerate(batches, 1):
        for key, value, note_, seq, op, minute in batch:
            rows.append([f"batch {i}", key, value, note_, seq, op, clock(minute)])
    out += table(["Arrives in", "Key", "Value (tracked)", "Note (not tracked)", "Sequence", "Operation",
                  "Committed at"], rows)
    out += table(["Key", "Value", "Note", "<code>_seq_from</code>", "<code>_valid_from</code>",
                  "<code>_valid_to</code>", "<code>_is_current</code>", "<code>_is_deleted</code>"],
                 [[k, v, n_, s, clock(vf), clock(vt), yes(cur), yes(dele)] for k, v, n_, s, vf, vt, cur, dele in after])
    return out + extra


# ------------------------------------------------------------------------------------------------ the hub
def hub_picture():
    d = box(10, 20, 200, 70, "Rows from stage 11", "each has a key hash", "bx", href="silver-keys.html")
    d += box(395, 20, 200, 70, "history()", "picks by plan.history", "solid")
    d += box(780, 20, 200, 70, "Silver as it stands", "handed in, never read", "bxs", href="silver-reader.html")
    d += arrow(210, 55, 393, 55) + arrow(780, 55, 597, 55)
    kinds = [("Type 2", "keep every version", "silver-scd2", "bx"), ("Type 1", "latest wins", "silver-scd1", "bx"),
             ("Append", "events, add only", "silver-append", "bx"), ("Snapshot", "replace the window", "silver-snapshot", "bx"),
             ("Source versioned", "source's own dates", "silver-source-versioned", "bx"),
             ("Deletes", "inside types 1, 2", "silver-deletes", "bxm")]
    for i, (name, sub, pid, kind) in enumerate(kinds):
        x = 10 + i * 164
        d += box(x, 160, 150, 80, name, sub, kind, href=f"{pid}.html")
        d += arrow(495, 90, x + 75, 158)
    d += text(495, 280, "Every row leaves with a _row_key and an _action for the writer", "tx", "middle")
    return d


@page("silver-history", "Stage 12: history, chosen by the kind of dataset", parent="silver-stages", short="12 History",
      blurb="One dispatcher, five ways to keep history. Each returns rows that already carry their row key and action.")
def silver_history():
    out = ("<p class='lead'>Stage 12 decides what a new row means for the silver table. It may be a new version of "
           "something silver already holds, a replacement, a new event, or a delete. The answer depends on the "
           "kind of dataset, so the stage has five ways of working and picks one from the plan.</p>"
           "<p>Without this stage silver would be a pile of changes. A reader could not tell which row of a "
           "material is the one that is true today, or what a promised date was last week.</p>")
    out += fig(990, 300, hub_picture(), "The history dispatcher and the five history types",
               "The highlighted box is the dispatcher. It hands the rows to one of the five functions below it. "
               "Click a box to zoom in.")
    out += flow3(
        "The rows that passed stages 1 to 11, each with a <code>_key_hash</code>. The silver table as it stands, "
        "handed in by the read edge. The plan.",
        "The dispatcher calls the function for the plan's history type. That function works out what each row "
        "becomes and which stored rows must change.",
        "Rows to write. Each has the history columns, a <code>_row_key</code> that names the row in silver, and an "
        "<code>_action</code> that tells the writer to write it or remove it.")
    out += "<h2>History follows the kind of dataset</h2>"
    out += ("<p>A dataset states its kind once in its manifest. The kind is one of master, transaction, event or "
            "snapshot. The plan turns the kind into a history type. A dataset may name a history type directly, "
            "and then that wins.</p>")
    out += code(PLAN, r"^HISTORY_BY_KIND", 2, "The default history type for each kind, and the five types the engine knows.")
    out += code(PLAN, r'    history = str\(hist.get\("type"\)', 3, "An explicit type wins over the kind. An unknown type is refused.")
    out += "<h2>The five types side by side</h2>"
    out += ("<p>Each row of this table is one history type. The columns say what one stored row stands for, how "
            "its row key is built, what a change to an existing key does, what a delete does, and which dataset "
            "of the tyre company uses it.</p>")
    out += table(["Type", "What one row is", "Row key", "A change does this", "A delete does this", "Tyre dataset"], [
        [link("silver-scd2", "Type 2") + " <code>scd2</code>", "One version of a key",
         "Hash of the key hash and the version's start in the source order",
         "A change to a tracked column opens a new version. Any other change updates the version it falls in.",
         "Marks the version it falls in as deleted. Nothing is removed.",
         "Materials, presses, dealers, and sales order lines (a transaction that asks for type 2)"],
        [link("silver-scd1", "Type 1") + " <code>scd1</code>", "The latest state of a key", "The key hash",
         "The change with the highest order value replaces the row.",
         "The row stays, marked deleted.", "Deliveries"],
        [link("silver-append", "Append") + " <code>append</code>", "One event", "The key hash",
         "Nothing. An event is never changed. A repeat was dropped by stage 10.",
         "Nothing. Events have no delete handling.",
         "Lab inspection results, production records, press readings"],
        [link("silver-snapshot", "Snapshot") + " <code>snapshot</code>", "One row of one delivered file",
         "Hash of the key hash and the batch key",
         "A newer file for the same window becomes current. The window's older rows stay, not current.",
         "A row missing from the newer file stops being current. There is no deleted flag.",
         "Dealer sell-out"],
        [link("silver-source-versioned", "Source versioned") + " <code>source_versioned</code>",
         "One version, with the source's own validity dates", "Hash of the key hash and the version's start date",
         "A newer delivery of the same key and start replaces the stored one.",
         "A last version that the source closed marks the key deleted.",
         "None of the nine tyre datasets"],
    ])
    out += "<h2>The dispatcher</h2>"
    out += code(HIST, r"^KINDS = \{", r"    return Out\(KINDS", "The whole dispatcher. Four types share one signature. The fifth also returns rows to set aside.")
    out += ("<p>The dispatcher first puts the stored silver rows into the working shape. Then it looks at the "
            "plan's history type. Source versioned history is called on its own because it can set bad keys "
            "aside, so it returns two things. The other four are looked up in <code>KINDS</code> and called "
            "the same way: new rows, silver as it stands, and the plan.</p>")
    out += "<h2>What <code>_row_key</code> and <code>_action</code> mean</h2>"
    out += ("<p>The history stage never writes. It labels each row so that the writer has nothing left to decide. "
            f"The writer is explained on {link('silver-writer', 'the writer page')}.</p>")
    out += table(["Column", "What it is", "Why it is built this way"], [
        ["<code>_row_key</code>", "The identity of one row in the silver table. The writer matches on it and on "
                                  "nothing else.",
         "It is computed from the data, never from the run. The same batch run twice gives the same row keys, so "
         "the second run finds every row already there."],
        ["<code>_action</code>", "<code>upsert</code> means write this row. <code>delete</code> means remove the "
                                 "stored row with this row key.",
         "A remove is needed in two cases. A late change can move the start of a version, which changes its row "
         "key and leaves the old row stale. And a dataset can ask for deleted rows to be removed instead of marked."],
    ])
    out += "<h2>The helpers every keyed type shares</h2>"
    out += ("<p>Type 2 and type 1 are built from the same six small functions. Read them once here. The pages "
            "for each type then only show what is different.</p>")
    out += code(HIST, r"^SILVER_ONLY = ", 3, "Three lists of column names the helpers use.")
    out += table(["List", "What it holds"], [
        ["<code>SILVER_ONLY</code>", "Columns stage 13 adds after history. They are dropped from the stored rows "
                                     "so that stored rows and new rows have the same columns."],
        ["<code>RECOMPUTED</code>", "The history columns. They are worked out again every time a key is touched."],
        ["<code>WORK</code>", "Scratch columns used while computing. They never reach silver."],
    ])
    out += code(HIST, r"^def working\(", caption="Stored rows in the working shape.")
    out += ("<p><code>working</code> makes the stored rows look like rows in flight. Silver calls the bronze run "
            "<code>_bronze_run_id</code>. In flight the same value is called <code>_run_id</code>, so the column "
            "is renamed back. The three columns that stage 13 adds are dropped.</p>")
    out += code(HIST, r"^def _incoming\(", caption="New rows become events.")
    out += ("<p><code>_incoming</code> adds three columns to the new rows. <code>_op</code> is the operation. "
            "A change feed sends I for insert, U for update and D for delete. A source that sends no operation "
            "gets U on every row. <code>_ts</code> is the effective time, which is the time the change took "
            "effect at the source. The plan names the column that holds it, and it is empty when the dataset has "
            "none. <code>_existing</code> is false, which marks the row as new. The last line is the first place "
            "deletes are decided: when the dataset's delete rule is <code>none</code>, rows with operation D are "
            "thrown away here.</p>")
    out += code(HIST, r"^def _existing\(", caption="Stored versions become events too.")
    out += ("<p><code>_existing</code> does the same for rows silver already holds. A stored row that is marked "
            "deleted gets operation D. Its time is its <code>_valid_from</code>. Its position in the source "
            "order, <code>_pos</code>, is its <code>_seq_from</code>, which is the order value at which the "
            "version started. Then the old history columns are dropped, because they will be computed again.</p>")
    out += code(HIST, r"^def _events\(", caption="Every event of the keys this batch touches.")
    out += ("<p><code>_events</code> builds the list the history is computed from. On the first load there is no "
            "silver, so the events are the new rows alone. Otherwise it adds any deletes found by comparing a "
            f"full copy with silver ({link('silver-deletes', 'the deletes page')} explains that). It then lists the keys "
            "this batch touches and takes from silver only the rows of those keys. A key the batch does not "
            "mention is never read and never rewritten. The function returns the events and the stored rows "
            "of the touched keys.</p>")
    out += code(HIST, r"^def _stale\(", caption="Stored rows the new history no longer contains.")
    out += code(HIST, r"^def _finish\(", caption="Scratch columns dropped, the action set.")
    out += ("<p><code>_stale</code> compares row keys. A stored row of a touched key whose row key is not in the "
            "new result is stale, and gets the action <code>delete</code>. <code>_finish</code> drops the scratch "
            "columns and sets the action to <code>upsert</code>. When the delete rule is <code>remove</code>, a "
            "row marked deleted gets the action <code>delete</code> instead. Last, the stale rows are added.</p>")
    out += "<h2>The settings that drive stage 12</h2>"
    out += ("<p>Each row is one key of the dataset manifest. The columns give its meaning, the values the code "
            "accepts, and what happens when it is absent.</p>")
    out += table(["Key", "What it means", "Allowed values", "When absent"], [
        ["<code>history.kind</code>", "The kind of dataset", "master, transaction, event, snapshot", "The plan refuses the dataset"],
        ["<code>history.type</code>", "The history type, when the kind's default is not wanted",
         "scd1, scd2, append, snapshot, source_versioned", "Chosen from the kind"],
        ["<code>history.track_columns</code>", "Type 2: the columns whose change opens a new version", "Contract columns",
         "Every kept contract column except the key and the order column"],
        ["<code>load.history_order_by</code>", "The column that gives the source's order of changes", "A contract column",
         "<code>dedup.order_by</code> is used. Types 1 and 2 are refused when neither is set."],
        ["<code>load.deletes.detect</code>", "How a delete is found", "change_feed, key_compare, none", "none"],
        ["<code>load.deletes.apply</code>", "What a delete does to silver", "mark_deleted, remove, none", "none"],
        ["<code>history.valid_from_column</code>, <code>history.valid_to_column</code>",
         "Source versioned: the source's own validity columns", "Contract columns", "Source versioned is refused"],
        ["<code>history.gaps_allowed</code>", "Source versioned: whether a gap between two versions is accepted", "true, false", "false"],
    ])
    out += "<h2>Key design decisions</h2>"
    out += table(["Decision", "Why"], [
        ["History is laid out in the source's order, never in arrival order.",
         "Changes can arrive late or out of order. Only the source knows the true order, and it says so in the "
         "order column."],
        ["A touched key's history is computed again from its stored versions and the new changes.",
         "One computation covers a first load, a normal change, a late change and a rerun. There is no separate "
         "code path for each."],
        ["Row keys come from the data.", "A rerun produces the same rows, so it changes nothing."],
        ["Deletes mark, they do not remove, unless the dataset asks.",
         "Gold and auditors need to know that a row existed and when it went away."],
    ])
    out += not_built([
        "Events have little delete handling. When deletes are not applied, an append dataset drops a delete row of an "
        "event feed, as type 1 and type 2 do. With <code>mark_deleted</code> or <code>remove</code> an append dataset "
        "does nothing special: a delete row is appended like any other row.",
        "A snapshot has no deleted flag. A row that the newer file leaves out is only marked not current.",
        "No tyre dataset uses source versioned history or the <code>remove</code> rule. Source versioned history "
        "has its own table of test cases. The <code>remove</code> rule has none.",
        "The plan checks the value of <code>load.deletes.apply</code> but not of <code>load.deletes.detect</code>. "
        "The manifest schema checks that one before the plan is built.",
    ])
    return out


# ------------------------------------------------------------------------------------------------ type 2
def scd2_steps():
    d = box(10, 20, 190, 60, "New changes", "this batch", "bx")
    d += box(10, 110, 190, 60, "Stored versions", "of the touched keys", "bxs")
    d += box(260, 60, 180, 70, "Events", "in source order", "solid")
    d += box(490, 60, 150, 70, "Versions", "by tracked hash", "bx")
    d += box(690, 60, 130, 70, "One row", "per version", "bx")
    d += box(870, 60, 110, 70, "Rows", "to merge", "bx", href="silver-writer.html")
    d += arrow(200, 50, 258, 85) + arrow(200, 140, 258, 105)
    d += arrow(440, 95, 488, 95) + arrow(640, 95, 688, 95) + arrow(820, 95, 868, 95)
    d += num(350, 45, 1) + num(565, 45, 2) + num(755, 45, 3) + num(925, 45, 4)
    return d


def late_timeline():
    d = text(470, 30, "What silver holds afterwards, laid out in the source's order", "tx", "middle")
    d += box(40, 50, 300, 70, "value 1", "starts at sequence 1", "bx")
    d += box(380, 50, 200, 70, "value 2", "starts at sequence 4", "bxs")
    d += box(620, 50, 280, 70, "value 3, current", "starts at sequence 5", "solid")
    d += box(40, 230, 220, 60, "arrived first", "sequence 1", "bx")
    d += box(360, 230, 220, 60, "arrived second", "sequence 5", "bx")
    d += box(680, 230, 220, 60, "arrived third", "sequence 4, late", "bxm")
    d += arrow(150, 230, 190, 124) + arrow(470, 230, 760, 124) + arrow(790, 230, 480, 124)
    d += text(470, 320, "The order the three changes arrived in, left to right", "tx", "middle")
    return d


@page("silver-scd2", "Type 2 history: keep every version of a key", parent="silver-history", short="Type 2: every version",
      blurb="How scd2() lays changes out in the source's order, opens versions, dates them and stays safe to rerun.")
def silver_scd2():
    out = ("<p class='lead'>Type 2 history keeps one row for every version of a business key. When a tracked "
           "value changes, the old version is closed and a new one is opened. Nothing is overwritten, so silver "
           "can answer what was true on any past day.</p>"
           "<p>The tyre company needs this for its sales order lines. On-time delivery is judged against the "
           "date that was first promised to the dealer. If silver kept only the latest promised date, a line "
           "whose date was moved would always look on time.</p>"
           "<p>The hard part is not opening a version. The hard part is that changes do not arrive in order. "
           "A change made at ten o'clock can reach silver after a change made at eleven. If silver trusted the "
           "order of arrival, the older value would become the current one. So the function on this page never "
           "looks at arrival order to decide history. It uses the order the source gave.</p>")
    out += fig(990, 190, scd2_steps(), "The four steps of the type 2 function",
               "Read left to right. The highlighted box is the idea everything else rests on: stored versions and new "
               "changes are put in one list, in the source's order.")
    out += ladder([
        ("Collect the events", "The new changes of this batch and the versions silver already holds for the same "
                               "keys become one list of events. Each event has a position in the source's order."),
        ("Open versions", "Walking each key's events in that order, an event opens a new version when its tracked "
                          "values differ from the tracked values before it."),
        ("Pick one row per version", "A version may contain several events. The one with the highest order value "
                                     "gives the version its values."),
        ("Date, flag and key the versions", "Each version gets its validity times, its current flag, its deleted "
                                            "flag and a row key built from the key and the version's start. "
                                            "Stored rows that the new history no longer contains are marked for removal."),
    ])
    out += "<h2>A worked example: a promised date that moves twice</h2>"
    out += ("<p>Order O1, line 1 was promised for 10 November. The planner moved it to 11 November, and later to "
            "12 November. The change feed gives every change a number, <code>change_seq</code>, that grows with "
            "each change at the source. The move to 12 November reached silver first. The move to 11 November "
            "reached silver after it.</p>")
    out += table(["Arrives", "Order", "Line", "Promised date", "<code>change_seq</code>"], [
        ["first", "O1", "1", "2026-11-10", "10"],
        ["second", "O1", "1", "2026-11-12", "30"],
        ["third, late", "O1", "1", "2026-11-11", "20"],
    ])
    out += ("<p>Silver ends with three versions in the source's order. The late change sits in the middle of the "
            "history. The version that started at 30 is still the current one.</p>")
    out += table(["Order", "Line", "Promised date", "<code>_seq_from</code>", "<code>_is_current</code>"], [
        ["O1", "1", "2026-11-10", "10", "no"],
        ["O1", "1", "2026-11-11", "20", "no"],
        ["O1", "1", "2026-11-12", "30", "yes"],
    ])
    out += ("<p>The declarative spike uses the same order line and the same three sequence numbers, with both "
            "moves in one batch and the older one listed second. The same pattern, with smaller numbers, is "
            "case 5 of the contract below.</p>")
    out += "<h2>The settings that drive it</h2>"
    out += code(TYRE + "erp.sales_order_line.yaml", r"^history:", r"^  late_lookback_days",
                "The sales order line: type 2 on three columns, ordered by the change sequence, deletes marked.")
    out += table(["Key", "What it means here", "Value for the sales order line"], [
        ["<code>history.type</code>", "Asks for type 2 although the kind is transaction", "scd2"],
        ["<code>history.track_columns</code>", "Only a change to one of these opens a version",
         "promised_date, ordered_qty, line_status"],
        ["<code>load.history_order_by</code>", "The source's order of changes", "change_seq"],
        ["<code>load.deletes</code>", "Deletes arrive in the change feed and are marked", "change_feed, mark_deleted"],
    ])
    out += ("<p>The plan adds one more thing the function needs: the <b>effective time</b>. It is the time a "
            "change took effect, and it becomes the validity dates of versions. For a change feed it is the "
            "commit time the feed carries, in the column <code>_committed_at</code>. For other sources it is the "
            "order column, when that column is a timestamp. Materials and presses are full copies ordered by "
            "<code>extracted_at</code>, a timestamp, so their versions are dated by the time the copy was taken.</p>")
    out += code(PLAN, r'    effective = "_committed_at"', 1, "How the plan picks the effective time.")

    out += "<h2>The code, step by step</h2>"
    out += "<h3>Step 1: put stored versions and new changes in one list, in the source's order</h3>"
    out += code(HIST, r"^def scd2\(", r'F.col\("_existing"\).desc_nulls_last\(\)\)',
                "The first lines of scd2(): the events, and the order they are walked in.")
    out += ("<p>Line by line. <code>o</code> is the name of the order column, <code>change_seq</code> for the "
            "order lines. <code>_incoming</code> turns the new rows into events, and each new event gets a "
            "position <code>_pos</code> equal to its order value. <code>_events</code> adds the stored versions "
            f"of the same keys. Both helpers are explained on {link('silver-history', 'the hub page')}. A stored version "
            "arrives with <code>_pos</code> equal to its <code>_seq_from</code>, the order value at which it "
            "started.</p>"
            "<p><code>by_pos</code> is the heart of the function. It is a window, which means a rule for walking "
            "rows group by group. The group is one business key. Inside a key the rows are walked by position, "
            "then by the order column, and at a tie the stored version comes before a new change. Arrival order "
            "is not in this rule at all. That one fact is why a late change lands in its proper place.</p>"
            "<p>The comment about nulls matters for portability. Spark and DuckDB disagree about where an empty "
            "value sorts unless the code says so. Every window on this page states it, so the same history comes "
            "out on both engines.</p>")
    out += "<h3>Step 2: decide which events open a version</h3>"
    out += code(HIST, r"^def _tracked\(", caption="Which columns are tracked.")
    out += ("<p>If the manifest lists tracked columns, those are used. If it lists none, every contract column "
            "that silver keeps is tracked, except the key columns and the order column. The order column must be "
            "left out, because it changes with every change and would open a version every time.</p>")
    out += code(HIST, r"    tracked = F.sha2\(text_parts", r"^    \)$",
                "The tracked hash, the value before, and the two columns that mark versions.")
    out += table(["Column", "What it holds", "Why"], [
        ["<code>_t</code>", "A hash of the event's tracked values. A hash is a short fingerprint: two rows with the "
                            "same tracked values have the same hash. A new delete event has no hash.",
         "Comparing one fingerprint is simpler than comparing every tracked column. A delete says the row is "
         "gone, not that a value changed, so it must not look like a change."],
        ["<code>before</code>", "The most recent hash among the earlier events of the same key, skipping events "
                                "that have none.",
         "Skipping empty hashes means a delete does not break the chain. The event after a delete is compared "
         "with the values before the delete."],
        ["<code>_new</code>", "True when this event opens a version.", "See the three rules below."],
        ["<code>_ver</code>", "A running count of the true values of <code>_new</code> within the key.",
         "Events with the same count belong to the same version."],
    ])
    out += ("<p>The three rules for <code>_new</code> are read in order. If there is no hash before this event, "
            "it is the first event of the key and it opens the first version. Otherwise, if this event has no "
            "hash, it is a delete and it opens nothing. Otherwise it opens a version exactly when its hash "
            "differs from the hash before it.</p>"
            "<p>Here is the worked example at this point, after the third batch. The first two rows are versions "
            "silver already holds. The third is the late change.</p>")
    out += table(["Event", "<code>_pos</code>", "Promised date", "<code>_t</code>", "<code>before</code>",
                  "<code>_new</code>", "<code>_ver</code>"], [
        ["stored version", "10", "2026-11-10", "hash A", "none", "true", "1"],
        ["new change, late", "20", "2026-11-11", "hash B", "hash A", "true", "2"],
        ["stored version", "30", "2026-11-12", "hash C", "hash B", "true", "3"],
    ])
    out += "<h3>Step 3: one row per version</h3>"
    out += code(HIST, r'    group = Window.partitionBy\("_key_hash", "_ver"\)', r'\}\)\.where\("_rn = 1"\)',
                "From events to versions: the start, the earliest time, and the row that represents the version.")
    out += ("<p>A version can hold more than one event. A change to a column that is not tracked falls into the "
            "version that is open at that point. Three values are computed over the events of a version.</p>")
    out += table(["Column", "What it is"], [
        ["<code>_start</code>", "The lowest position in the version. This is where the version begins in the source's order."],
        ["<code>_gts</code>", "The earliest effective time in the version."],
        ["<code>_rn</code>", "A rank. Rank 1 is the event with the highest order value. At a tie a new change beats "
                             "a stored version, and after that the newest arrival wins."],
    ])
    out += ("<p>Only rank 1 is kept. So a version takes its start from its earliest event and its values from "
            "its latest event. The tie rules make a resent change harmless: the same change delivered twice has "
            "the same order value, and the newest delivery simply replaces the older one.</p>")
    out += "<h3>Step 4: validity dates, flags and the row key</h3>"
    out += code(HIST, r'    per_key = Window.partitionBy\("_key_hash"\)\.orderBy\(F\.col\("_start"\)',
                r"    return _finish\(out, _stale\(rows, out\), plan\)",
                "The last lines of scd2(): the columns silver stores for each version.")
    out += table(["Column", "How it is computed", "What to notice"], [
        ["<code>_valid_from</code>", "The lowest effective time among this version and every later version of the key.",
         "Looking at later versions too keeps the dates in step with the source's order. A version can never start "
         "after a version that follows it."],
        ["<code>_valid_to</code>", "The <code>_valid_from</code> of the next version. Empty for the last version.",
         "The end of one version is exactly the start of the next. There are no gaps and no overlaps."],
        ["<code>_is_current</code>", "True when no version follows.", "At most one version per key is current."],
        ["<code>_is_deleted</code>", "True when the row that represents the version is a delete.",
         "A delete marks the version it falls in. It does not close it."],
        ["<code>_deleted_at</code>", "The effective time of that delete.", ""],
        ["<code>_seq_from</code>", "The version's start in the source's order.", "The next run reads it back as the stored version's position."],
        ["<code>_row_key</code>", "A hash of the key hash, a bar character and the version's start.",
         "This is what makes each version its own row in the merge."],
    ])
    out += ("<p>The rule for <code>_valid_from</code> deserves an example. In case 5 below, the late change was "
            "committed at 01:05, an hour after the newer change was committed at 00:05. Dated by its own commit "
            "time, the middle version would start after the version that follows it. The rule gives it the "
            "lowest time among itself and the later version, which is 00:05. Its validity then runs from 00:05 "
            "to 00:05. It has zero length. The version is in history and can be read, but a lookup for the value "
            "that was true at some moment never lands on it.</p>")
    out += ("<p>The last line calls <code>_stale</code> and <code>_finish</code>. A stored row of a touched key "
            "whose row key is not among the new rows gets the action <code>delete</code>. Case 6 below shows why "
            f"that is needed. Every other row gets <code>upsert</code>. {link('silver-writer', 'The writer')} then applies "
            "them in one merge on <code>_row_key</code>.</p>")

    out += "<h2>Why running the same batch again changes nothing</h2>"
    out += ("<p>Nothing in the function depends on when it runs or how often. Positions come from the order "
            "column. Versions come from the tracked hash. The row key comes from the key and the version's "
            "start. A second run of the same batch adds events that have the same order value as events already "
            "folded into the stored versions. They fall into the same versions, the same row wins rank 1 with "
            "the same values, and every row key is one silver already has. The merge updates each row to what it "
            f"already is. Case 8 proves it. One stage earlier, {link('silver-duplicates', 'stage 10')} has already "
            "dropped a change that appears twice inside one batch.</p>")
    out += code(ROWS, r'    elif plan.history == "scd2":', 3,
                "Stage 10 for a type 2 dataset: every distinct change is kept, and the same key and order value twice is one change.")

    out += "<h2>The eight cases every engine must pass</h2>"
    out += ("<p>The behaviour above is written down as data in one file. Each case lists the batches of changes "
            "in arrival order and what silver must mean afterwards. A change row is the key, a tracked value, a "
            "note that is not tracked, the sequence number, the operation, the minute it was committed, and its "
            "position in the feed. Think of the value as the promised date and the note as a free-text remark on "
            "the order line.</p>")
    out += code(CONTRACT, r"^CASES: list", r"^\]$", "The eight cases, exactly as the tests read them.", cap=40)
    out += ("<p>Each case is shown below as two tables. The first table lists the changes in the order they "
            "arrived. The second table shows silver afterwards. The test checks the key, the value, "
            "<code>_seq_from</code>, <code>_is_current</code> and <code>_is_deleted</code>. The note and the two "
            "validity times were read by running the cases through the stage functions on the DuckDB engine "
            "while this page was written. All times are on 9 November 2026.</p>")
    out += worked(1, "first load", "It proves that a first load gives each key one current version.",
                  [[("a", 1, "x", 1, "I", 0), ("b", 5, "x", 2, "I", 0)]],
                  [("a", 1, "x", 1, 0, None, True, False), ("b", 5, "x", 2, 0, None, True, False)])
    out += worked(2, "a tracked change opens a version",
                  "It proves that a change to a tracked value closes the old version and opens a new one. The old "
                  "version ends at the minute the new one starts.",
                  [[("a", 1, "x", 1, "I", 0)], [("a", 2, "x", 3, "U", 5)]],
                  [("a", 1, "x", 1, 0, 5, False, False), ("a", 2, "x", 3, 5, None, True, False)])
    out += worked(3, "a change that is not tracked updates the version in place",
                  "It proves that a change to a column outside the tracked list opens nothing. The version keeps "
                  "its start and takes the new note.",
                  [[("a", 1, "x", 1, "I", 0)], [("a", 1, "y", 3, "U", 5)]],
                  [("a", 1, "y", 1, 0, None, True, False)])
    out += worked(4, "several changes in one batch",
                  "It proves that order inside a batch does not matter. The change with sequence 5 is listed "
                  "before the change with sequence 3, and the versions still come out in sequence order.",
                  [[("a", 1, "x", 1, "I", 0), ("a", 3, "x", 5, "U", 9), ("a", 2, "x", 3, "U", 5)]],
                  [("a", 1, "x", 1, 0, 5, False, False), ("a", 2, "x", 3, 5, 9, False, False),
                   ("a", 3, "x", 5, 9, None, True, False)])
    out += worked(5, "an older change arrives after a newer one",
                  "This is the late change. It proves that a change with a lower sequence, arriving in a later "
                  "batch, goes into its place in history and does not become current. The late change was "
                  "committed 65 minutes after the first load, which is later than the newer change.",
                  [[("a", 1, "x", 1, "I", 0)], [("a", 3, "x", 5, "U", 5)], [("a", 2, "x", 4, "U", 65)]],
                  [("a", 1, "x", 1, 0, 5, False, False), ("a", 2, "x", 4, 5, 5, False, False),
                   ("a", 3, "x", 5, 5, None, True, False)])
    out += fig(940, 335, late_timeline(), "The late change of case 5 on a timeline",
               "The bottom row is the order of arrival. The top row is the history silver keeps. The dashed box "
               "arrived last and still lands in the middle, because its sequence is 4.")
    out += ("<p>Follow the dashed box. When batch 3 arrives, silver holds two versions of the key, starting at "
            "sequence 1 and at sequence 5. They become two stored events at positions 1 and 5. The late change "
            "is a new event at position 4. Walked by position, the late change is second. Its value differs "
            "from the value before it, so it opens a version. The stored version at position 5 differs from the "
            "late change, so it still opens a version, with the same start and therefore the same row key as "
            "before. It stays the last version, so it stays current. The middle version has zero length, as "
            "explained above.</p>")
    out += worked(6, "a late change that equals the version after it",
                  "It proves that a late change can move the start of a version. The late change has value 3 at "
                  "sequence 4. The stored version with value 3 starts at sequence 5. They have the same tracked "
                  "value, so they are one version, and that version really began at sequence 4.",
                  [[("a", 1, "x", 1, "I", 0)], [("a", 3, "x", 5, "U", 5)], [("a", 3, "x", 4, "U", 4)]],
                  [("a", 1, "x", 1, 0, 4, False, False), ("a", 3, "x", 4, 4, None, True, False)],
                  "<p>The version's start moved from 5 to 4, so its row key changed. The stage returns three rows "
                  "for this batch: the first version with a new end, the current version under its new row key, "
                  "and the old row key with the action <code>delete</code>. Without that removal silver would "
                  "hold the same version twice.</p>")
    out += worked(7, "a delete",
                  "It proves that a delete marks the key and removes nothing. The version stays current and "
                  "keeps its start. It is flagged deleted, and <code>_deleted_at</code> holds the delete's commit "
                  "time, 00:05.",
                  [[("a", 1, "x", 1, "I", 0)], [("a", 1, "x", 2, "D", 5)]],
                  [("a", 1, "x", 1, 0, None, True, True)],
                  "<p>The delete event has no tracked hash, so it opens no version. It falls into the version "
                  "that is open. It has the highest order value in that version, so it becomes the row that "
                  f"represents it, and its operation D sets the flag. {link('silver-deletes', 'The deletes page')} covers "
                  "what happens next, including a key that comes back.</p>")
    out += worked(8, "the same batch twice",
                  "It proves that a rerun changes nothing. The second batch repeats the first, row for row.",
                  [[("a", 1, "x", 1, "I", 0), ("a", 2, "x", 2, "U", 1)], [("a", 1, "x", 1, "I", 0), ("a", 2, "x", 2, "U", 1)]],
                  [("a", 1, "x", 1, 0, 1, False, False), ("a", 2, "x", 2, 1, None, True, False)])

    out += "<h2>How it is tested</h2>"
    out += (f"<p>The history section of {src(T_STAGES)} runs all thirteen stages for each batch and keeps silver "
            "in memory, applying the row keys and actions the way the merge would.</p>")
    out += code(T_STAGES, r"^def _apply\(", caption="The test's stand-in for the merge: rows whose key is not in the new output are kept, upserts are added.")
    out += code(T_STAGES, r"^def test_history_type2\(", caption="One test, run once for each of the eight cases.")
    out += ("<p>After each case the test also checks three properties of the whole table: every row key appears "
            "once, a key has at most one current version, and the versions of a key never overlap. A second test "
            "pins the zero-length validity of the late change.</p>")
    out += code(T_STAGES, r"^def test_history_type2_validity_never_overlaps\(",
                caption="The late change again, this time checking the validity times.")
    out += ("<p>The same file is run on Spark and on DuckDB, so the eight cases hold on both engines the stages "
            f"run on. {link('silver-engines', 'The engines page')} explains how the declarative engine is held to the same cases.</p>")
    out += "<h2>What the recorded run shows</h2>"
    out += (f"<p>In {RUN}, a promised date was changed out of order on day 8. The design note records the verdict: "
            "three versions, the late change with sequence 87216 in history and not current, and the current "
            "promised date 2026-11-20. On day 14 silver held 113,457 current order lines in 121,322 versions, "
            f"equal to the oracle. The numbers are quoted from {src(NOTES)}. The run was not repeated for this page.</p>")
    out += not_built([
        "A stored version remembers its start and its latest values, not every change that fell into it. If a "
        "tracked value changes and later changes back, and the middle change arrives after the last one was "
        "already folded into the stored version, the middle change is placed after that version and becomes "
        "current. Checked by running it: value 1 at sequence 1, a note change at sequence 3, then a late value 2 "
        "at sequence 2 leaves value 2 current. The source's true latest value is 1. A test case named "
        "<code>late-after-flip-back</code> in <code>test_silver_stages.py</code> states the right answer and is marked as a known "
        "failure, so the bug stays visible and the case turns red the day it is fixed.",
        "When the order column is not a timestamp and the source is not a change feed, there is no effective "
        "time, so <code>_valid_from</code> and <code>_valid_to</code> stay empty. The versions are still right. "
        "The full copy test cases run this way.",
        "Every version of a touched key is written again in the merge, not only the versions that changed.",
    ])
    return out


# ------------------------------------------------------------------------------------------------ type 1
@page("silver-scd1", "Type 1 history: the latest state of a key wins", parent="silver-history", short="Type 1: latest wins",
      blurb="One row per key. The change with the highest source order replaces it, whatever order changes arrive in.")
def silver_scd1():
    out = ("<p class='lead'>Type 1 history keeps one row per business key: its latest state. A change replaces "
           "the row. The past is not kept.</p>"
           "<p>It is the default for transactions, such as the tyre company's deliveries. A delivery is updated "
           "as it moves, and reports only need where it stands now. The risk is the same as for type 2. If the "
           "latest arrival won, a late older change would overwrite newer data. So the latest change by the "
           "source's order wins.</p>")
    d = box(10, 30, 210, 60, "New changes", "this batch", "bx")
    d += box(10, 120, 210, 60, "Stored row", "of each touched key", "bxs")
    d += box(320, 70, 260, 70, "Highest order value wins", "one row per key", "solid")
    d += box(690, 70, 250, 70, "Row key = key hash", "the merge overwrites", "bx", href="silver-writer.html")
    d += arrow(220, 60, 318, 95) + arrow(220, 150, 318, 115) + arrow(580, 105, 688, 105)
    out += fig(950, 200, d, "Type 1 in one picture",
               "The stored row competes with the new changes. Whichever has the highest order value is the row silver keeps.")
    out += flow3("The new changes, and the stored row of every key they touch.",
                 "The events of a key are ranked by the order column, highest first. Rank 1 is kept.",
                 "One row per touched key, with the key hash as its row key.")
    out += "<h2>A worked example</h2>"
    out += ("<p>Delivery D-7 was changed twice at the source. The second change, with sequence 2, reached silver "
            "first. The first change, with sequence 1, arrived in a later batch.</p>")
    out += table(["Arrives in", "Delivery", "Delivered quantity", "<code>change_seq</code>"], [
        ["batch 1", "D-7", "40", "2"], ["batch 2", "D-7", "36", "1"]])
    out += "<p>Silver keeps the row with sequence 2. The late older change is ignored.</p>"
    out += table(["Delivery", "Delivered quantity", "<code>_seq_from</code>", "<code>_is_current</code>", "<code>_is_deleted</code>"],
                 [["D-7", "40", "2", "yes", "no"]])
    out += "<h2>The settings that drive it</h2>"
    out += code(TYRE + "erp.delivery.yaml", r"^dedup:", 1, "Deliveries name their order column in the duplicate settings.")
    out += code(TYRE + "erp.delivery.yaml", r"^history:", r"^  deletes:", "The kind is transaction, so the history type is type 1.")
    out += table(["Key", "What it means", "Value for deliveries"], [
        ["<code>history.kind</code>", "A transaction gets type 1 unless it names another type", "transaction"],
        ["<code>dedup.order_by</code>", "The source's order. It is used because <code>load.history_order_by</code> is not set.", "change_seq"],
        ["<code>load.deletes</code>", "Deletes arrive in the change feed and are marked", "change_feed, mark_deleted"],
    ])
    out += "<h2>The code</h2>"
    out += code(HIST, r"^def scd1\(", caption="The whole of type 1.")
    out += (f"<p>The first lines are the same as type 2. The new rows become events, and the stored rows of the "
            f"touched keys are added. {link('silver-history', 'The hub page')} explains those helpers.</p>"
            "<p>The window <code>latest</code> ranks the events of a key. The highest order value comes first. "
            "At a tie a new change comes before the stored row, and after that the newest arrival wins. Rank 1 "
            "is kept and the rest are dropped.</p>"
            "<p>The history columns are simple. The row is always current. There is no end of validity. "
            "<code>_valid_from</code> is the effective time of the change that won. <code>_seq_from</code> is "
            "its order value. The row key is the key hash, so the merge finds the one stored row of the key and "
            "overwrites it.</p>"
            "<p>A delete is one more event. If the delete has the highest order value, the row that survives is "
            "the delete row, and it is flagged deleted with the delete's time. The row is not removed unless the "
            "dataset's rule is <code>remove</code>. If a newer change for the key arrives later, that change "
            "wins and the flag is gone.</p>")
    out += "<h2>How it is tested</h2>"
    out += code(T_STAGES, r'\("the latest change wins"', r"    assert_history_invariants\(silver\)",
                "Three cases: the latest wins, a late older change does not overwrite, a delete is soft.")
    out += ("<p>The second case is the worked example above. Its two batches arrive in the wrong order and "
            "silver must still hold sequence 2.</p>")
    out += not_built([
        "Type 1 keeps no past. If a report later needs the earlier values of a delivery, the dataset has to move "
        "to type 2 and be rebuilt from bronze.",
        "Stage 10 already keeps only the latest change per key inside a batch for this kind of dataset, so the "
        "ranking here mostly decides between the batch and the stored row.",
    ])
    return out


# ------------------------------------------------------------------------------------------------ append
@page("silver-append", "Append history: events are only ever added", parent="silver-history", short="Append: events",
      blurb="An event happened once and never changes. New rows are added, repeats were removed one stage earlier.")
def silver_append():
    out = ("<p class='lead'>An event is something that happened at a moment: a press reading, a lab inspection, "
           "a production record. It does not change afterwards. Append history adds new events to silver and "
           "touches nothing that is already there.</p>"
           "<p>This is the cheapest history, and the biggest tables use it. In the tyre company the press "
           "readings alone grow by 622,080 rows a day in the recorded run. Rewriting or comparing versions for "
           "such a table would be wasted work.</p>")
    d = box(10, 50, 220, 70, "New events", "after stage 10", "bx", href="silver-duplicates.html")
    d += box(340, 50, 260, 70, "Stamp the event time", "into _valid_from", "solid")
    d += box(710, 50, 230, 70, "Appended to silver", "nothing is merged", "bx", href="silver-writer.html")
    d += arrow(230, 85, 338, 85) + arrow(600, 85, 708, 85)
    out += fig(950, 160, d, "Append history in one picture",
               "The function in the middle adds columns and nothing else. Removing repeats is stage 10's job.")
    out += flow3("Rows that stage 10 has already cleared of repeats, both inside the batch and against silver.",
                 "Each row gets the history columns of a live row and its event time.",
                 "The same rows, with the key hash as row key and the action <code>upsert</code>.")
    out += "<h2>A worked example</h2>"
    out += ("<p>Three press readings arrive. The third one was measured at 00:01 but reached the platform an "
            "hour late, in the next batch.</p>")
    out += table(["Arrives in", "Reading", "Event time"], [
        ["batch 1", "r1", "00:00"], ["batch 1", "r2", "01:01"], ["batch 2", "r3", "00:01"]])
    out += ("<p>All three are appended. The late reading keeps its own event time, so it is counted in the hour "
            "it was measured, not the hour it arrived.</p>")
    out += table(["Reading", "<code>_valid_from</code>", "<code>_is_current</code>", "<code>_is_deleted</code>"], [
        ["r1", "00:00", "yes", "no"], ["r2", "01:01", "yes", "no"], ["r3", "00:01", "yes", "no"]])
    out += "<h2>The settings that drive it</h2>"
    out += code(TYRE + "press_stream.press_reading.yaml", r"^dedup:", 1, "Press readings: a repeat is found by the reading id.")
    out += code(TYRE + "press_stream.press_reading.yaml", r"^history:", r"^  late_lookback_days",
                "The kind is event, so the history type is append.")
    out += table(["Key", "What it means", "Value for press readings"], [
        ["<code>history.kind</code>", "An event dataset is appended", "event"],
        ["<code>config.watermark_column</code>", "When this column is a timestamp it is the event time", "event_ts"],
        ["<code>load.late_lookback_days</code>", "How far back stage 10 looks in silver for a repeat. Absent means all of silver.", "2"],
    ])
    out += "<h2>The code</h2>"
    out += code(HIST, r"^def _live\(", caption="The history columns of a row that has no history of its own.")
    out += code(HIST, r"^def append\(", caption="The whole of append history.")
    out += ("<p><code>_live</code> returns the column values of a live row: current, not deleted, no validity "
            "and no start in the source order. <code>append</code> lays those over the rows and then sets two "
            "columns of its own. <code>_valid_from</code> becomes the event time when the plan has one and the "
            "column is in the batch. Otherwise it is the effective time, which may be empty. The row key is the "
            "key hash.</p>"
            f"<p>The function never looks at silver. It does not need to. {link('silver-duplicates', 'Stage 10')} has "
            "already dropped any reading whose key is in silver within the lookback, and the writer appends with "
            f"a transaction number so that a repeated write is skipped. {link('silver-writer', 'The writer page')} "
            "explains the transaction number.</p>")
    out += "<h2>How it is tested</h2>"
    out += code(T_STAGES, r'\("new rows appended"', r'\] == want, case',
                "Three cases, run through all the stages: new rows, a repeat across batches, a repeat inside a batch.")
    out += code(T_STAGES, r"^def test_late_readings_keep_their_event_time\(",
                caption="The worked example above: three readings, two event hours.")
    out += (f"<p>In {RUN}, 188 readings arrived late on day 3 and silver's count per event hour equalled the "
            "oracle in all 24 hours. On day 13 the stream was replayed, 19,440 repeated rows were dropped, and "
            f"silver held 8,087,040 readings, equal to the oracle. Both are quoted from {src(NOTES)}.</p>")
    out += not_built([
        "Events cannot be deleted or corrected through this path. The function does not read the operation "
        "column, so a delete row in an event feed is not treated as a delete.",
        "An event dataset without an event time and without an effective time gets an empty <code>_valid_from</code>.",
    ])
    return out


# ------------------------------------------------------------------------------------------------ snapshot
@page("silver-snapshot", "Snapshot history: a newer file replaces its window", parent="silver-history", short="Snapshot: windows",
      blurb="Each file is the whole truth for one window. The newest file for a window is current, older rows stay.")
def silver_snapshot():
    out = ("<p class='lead'>A snapshot file states everything about one window, such as one dealer's sales for "
           "one month. When a corrected file for the same window arrives, it replaces what the earlier file said. "
           "The earlier rows are kept, marked not current.</p>"
           "<p>The tyre company's dealers send sell-out files this way. A dealer who finds a mistake sends the "
           "month again. If silver added the new file to the old one, the month's units would be counted twice.</p>")
    d = box(10, 30, 250, 70, "File f1, dealer d1", "materials m1 and m2", "bxs")
    d += box(10, 140, 250, 70, "File f2, dealer d1", "material m1 only", "bx")
    d += box(370, 85, 240, 70, "Newest file per window", "by order column, then batch", "solid")
    d += box(720, 30, 250, 70, "f2 rows: current", "", "bx")
    d += box(720, 140, 250, 70, "f1 rows: kept", "not current", "bxm")
    d += arrow(260, 65, 368, 110) + arrow(260, 175, 368, 130) + arrow(610, 110, 718, 65) + arrow(610, 130, 718, 175)
    out += fig(980, 230, d, "A newer file takes over a window",
               "Both files are for the same dealer. The newer one becomes current. Every row of the older one stays in silver, switched off.")
    out += flow3("The rows of the new files, and the current rows silver holds.",
                 "For each window the newest file is found. If it is at least as new as what silver holds for the "
                 "window, its rows become current and the window's other current rows are switched off.",
                 "The new rows, plus the older rows of the same windows with <code>_is_current</code> false.")
    out += "<h2>A worked example</h2>"
    out += ("<p>This is the second case of the test table. Dealer d1 sends file f1 with two materials. Later the "
            "dealer sends file f2 with a corrected count for material m1 and no row for m2.</p>")
    out += table(["Arrives in", "Dealer", "Material", "Units", "File"], [
        ["batch 1", "d1", "m1", "3", "f1"], ["batch 1", "d1", "m2", "4", "f1"], ["batch 2", "d1", "m1", "5", "f2"]])
    out += table(["Dealer", "Material", "Units", "File", "<code>_is_current</code>"], [
        ["d1", "m1", "3", "f1", "no"], ["d1", "m1", "5", "f2", "yes"], ["d1", "m2", "4", "f1", "no"]])
    out += ("<p>Material m2 is not in the newer file, so the dealer's window no longer has it. Its row is still "
            "in silver, but it is not current.</p>")
    out += "<h2>The settings that drive it</h2>"
    out += code(TYRE + "partner.dealer_sellout.yaml", r"^dedup:", 1, "The business key and the order column of dealer sell-out.")
    out += code(TYRE + "partner.dealer_sellout.yaml", r"^history:", r"^  late_lookback_days",
                "The kind is snapshot. The batch key is the file's checksum, so a renamed resend is the same batch.")
    out += code(PLAN, r"        snapshot_by=keys\[:-1\]", 1, "The window is not a setting. The plan takes the key without its last column.")
    out += ("<p>The contract's primary key is what the window is cut from. The manifest above lists the key as "
            "dealer code, sale month and material code, so the window is a dealer's month. Files are ordered by "
            "<code>file_name</code>.</p>")
    out += "<h2>The code</h2>"
    out += code(HIST, r"^def snapshot\(", caption="The whole of snapshot history.")
    out += ladder([
        ("Stamp each row", "<code>_stamp</code> is the order value and the batch key together. Comparing stamps "
                           "compares the order value first and the batch key second."),
        ("Name the window", "<code>_win</code> is a hash of the window columns."),
        ("Keep the newest file of each window in this batch", "Two files for the same window can arrive in one "
                                                              "batch. Only the rows with the highest stamp per window are kept."),
        ("Give the rows their columns", "They are live rows. The row key is a hash of the key hash and the batch "
                                        "key, so rows from different files never collide."),
        ("First load", "With no silver table the new rows are returned as they are."),
        ("Decide whether the new file applies", "<code>held</code> is the highest stamp among silver's current "
                                                "rows per window. A window applies when silver has nothing for it, or the incoming stamp is "
                                                "at least as high."),
        ("Switch off what was replaced", "For the windows that apply, silver's current rows that are not among "
                                         "the rows being written are returned with <code>_is_current</code> false."),
    ])
    out += ("<p>The comparison in step 6 is what stops an old file from winning. If a dealer's earlier file is "
            "sent again after the correction, its stamp is lower than what silver holds, so the window does not "
            "apply and nothing is written for it.</p>")
    out += "<h2>How it is tested</h2>"
    out += code(T_STAGES, r'\("a window\'s rows"', r'"_is_current"\) == want, case',
                "Four cases: a first file, a newer file, another window left alone, an older file sent again.")
    out += (f"<p>In {RUN}, silver's sell-out rows equalled the oracle on every one of the 14 days, ending at "
            f"9,404 current rows. The figure is quoted from {src(NOTES)}.</p>")
    out += not_built([
        "The window cannot be declared. It is always the business key without its last column. The design note "
        "lists this as a gap in the manifest format.",
        "A row dropped by a newer file has no deleted flag and no time of removal. It is only not current.",
        "The declarative engine refuses snapshot history. Snapshot datasets run on the job engine.",
    ])
    return out


# ------------------------------------------------------------------------------------------------ source versioned
@page("silver-source-versioned", "Source versioned history: the source already keeps the versions",
      parent="silver-history", short="Source versioned",
      blurb="The source delivers versions with its own validity dates. Silver checks them per key and never rebuilds them.")
def silver_source_versioned():
    out = ("<p class='lead'>Some sources keep history themselves. Every row they send is a version, with a "
           "valid-from date and a valid-to date. Silver should store those versions as delivered. It should not "
           "work out versions of its own.</p>"
           "<p>What silver must do is check them. Two versions of one key that overlap, or that leave a gap, "
           "would make a lookup by date return two answers or none. Such a key is set aside whole, with the "
           "reason, and its stored history is left as it was.</p>")
    d = box(10, 60, 210, 70, "Delivered versions", "with valid from, valid to", "bx")
    d += box(320, 60, 240, 70, "Check each key", "overlap, gap, end before start", "solid")
    d += box(680, 10, 280, 70, "Good keys", "stored with the source's dates", "bx")
    d += box(680, 120, 280, 70, "Bad keys, whole", "quarantine, with the reason", "bxm", href="silver-quality.html")
    d += arrow(220, 95, 318, 95) + arrow(560, 85, 678, 45) + arrow(560, 105, 678, 155)
    out += fig(970, 210, d, "Source versioned history in one picture",
               "A key passes or fails as a whole. One bad version sets aside every delivered version of that key.")
    out += flow3("Delivered versions, and the stored versions of the same keys.",
                 "One version is kept per key and start date, the newest delivery winning. Each key's versions "
                 "are walked in date order and checked.",
                 "The good keys' versions with the standard history columns. The bad keys' delivered rows as quarantine records.")
    out += "<h2>A worked example</h2>"
    out += ("<p>This is the overlap case of the test table. Key a has two versions, and the first ends at 01:30, "
            "after the second has started at 01:00. Key b has one clean version.</p>")
    out += table(["Key", "Value", "Valid from", "Valid to"], [
        ["a", "1", "00:00", "01:30"], ["a", "2", "01:00", "open"], ["b", "7", "00:00", "open"]])
    out += "<p>Key b is loaded. Key a is set aside with the reason <code>versions overlap</code>.</p>"
    out += table(["Key", "Value", "<code>_is_current</code>", "<code>_is_deleted</code>"], [["b", "7", "yes", "no"]])
    out += "<h2>The settings that drive it</h2>"
    out += code(PLAN, r"^def _validity\(", caption="Where the plan reads the two validity columns and the gap rule.")
    out += table(["Key", "What it means", "Allowed values", "When absent"], [
        ["<code>history.type</code>", "Must name this history type. No kind maps to it.", "source_versioned", "The kind's default is used"],
        ["<code>history.valid_from_column</code>", "The source column holding a version's start", "A contract column", "The plan refuses the dataset"],
        ["<code>history.valid_to_column</code>", "The source column holding a version's end", "A contract column", "The plan refuses the dataset"],
        ["<code>history.gaps_allowed</code>", "Whether a gap between two versions of a key is accepted", "true, false", "false"],
    ])
    out += "<p>The two column settings may also be given as dataset settings with the same names.</p>"
    out += "<h2>The code</h2>"
    out += code(HIST, r"^FAR = ", 2, "An end date in the year 9999 means the version is open.")
    out += code(HIST, r"^def source_versioned\(", r'    events = events.withColumn\("_sv_reason", reason\)',
                "The first half: collect the versions, keep one per start, and find the reason a key fails.", cap=40)
    out += ladder([
        ("Read the dates", "<code>stamp</code> casts the two validity columns to timestamps. An end at or after "
                           "the year 9999 becomes empty, which means open."),
        ("Add the stored versions", "The stored rows of the delivered keys are added, with their old history columns dropped."),
        ("One version per key and start", "If the same key and start date is present twice, a newly delivered row "
                                          "beats the stored row, and the newest arrival beats older ones."),
        ("Check in date order", "Each key's versions are walked by start date. A version with no start fails. A "
                                "version that ends before it starts fails. A version that starts before the previous one "
                                "ended fails as an overlap, and so does any version after an open one. When gaps are not "
                                "allowed, a version that starts after the previous one ended fails as a gap."),
    ])
    out += code(HIST, r'    events = events.withColumn\("_key_reason"', r"    return _finish\(out, stale, plan\), quarantine",
                "The second half: bad keys to quarantine, good keys get the standard columns.")
    out += ("<p>One failing version gives its reason to the whole key. The delivered rows of a failing key go to "
            "quarantine with the table name and the reason. Only the business columns are recorded.</p>"
            "<p>For the good keys the source's dates become <code>_valid_from</code> and <code>_valid_to</code>. "
            "The last version by start date is current. If the source closed that last version, the key is "
            "marked deleted and the end date is the time of deletion. The row key is a hash of the key hash and "
            "the start date. Stale rows are looked for only among the good keys, so a bad key's stored history "
            "is not touched.</p>")
    out += "<h2>How it is tested</h2>"
    out += code(T_STAGES, r"^SV_CASES = \[", r"^\]$", "Eight cases. Each lists whether gaps are allowed, the batches, and silver afterwards.", cap=30)
    out += ("<p>Three more tests check that no key is lost between silver and quarantine, that the quarantine "
            "reason names the table and the failure, and that the plan refuses the history type without its two columns.</p>")
    out += not_built([
        "None of the nine tyre datasets uses this history type, so it has no recorded run. It is covered by its test cases only.",
        "The declarative engine refuses it. Source versioned datasets run on the job engine.",
        "A key that fails stays out of silver until the source sends a clean set of versions. There is no "
        "partial load of a key.",
    ])
    return out


# ------------------------------------------------------------------------------------------------ deletes
def deletes_picture():
    d = box(10, 20, 200, 60, "Change feed", "a row with operation D", "bx")
    d += box(10, 100, 200, 60, "Full copy", "a key is missing", "bx")
    d += box(10, 180, 200, 60, "Any other source", "nothing is detected", "bxm")
    d += box(270, 100, 190, 60, "Mass-delete guard", "in ingestion", "bxs", href="ingest-steps.html")
    d += box(520, 60, 190, 80, "Delete event", "placed in source order", "solid")
    d += box(780, 20, 200, 60, "mark_deleted", "flag and time", "bx")
    d += box(780, 100, 200, 60, "remove", "row taken out", "bx")
    d += box(780, 180, 200, 60, "none", "delete ignored", "bxm")
    d += arrow(210, 50, 518, 85) + arrow(210, 130, 268, 130) + arrow(460, 130, 518, 115)
    d += arrow(710, 90, 778, 50) + arrow(710, 100, 778, 130) + arrow(710, 110, 778, 210)
    return d


@page("silver-deletes", "Deletes: how they are detected and how they are applied", parent="silver-history", short="Deletes",
      blurb="Two ways to learn that a row is gone, three things to do about it, and the guard that stops a broken copy.")
def silver_deletes():
    out = ("<p class='lead'>A row that is deleted at the source must not stay in silver looking alive. Silver "
           "also must not lose the fact that the row existed. This page follows a delete from the moment the "
           "platform learns of it to what the silver table holds afterwards.</p>"
           "<p>There are two separate questions, and the manifest answers each with its own setting. The first "
           "is detection: how does the platform learn that a row is gone? The second is application: what does "
           "silver do about it?</p>")
    out += fig(990, 260, deletes_picture(), "Detection on the left, application on the right",
               "The left column is how a delete is found. The right column is what it does to silver. In between, "
               "every delete becomes an ordinary event in the key's history.")
    out += "<h2>Detection: three settings</h2>"
    out += ("<p>The setting is <code>load.deletes.detect</code>. Each row below is one of its values.</p>")
    out += table(["Value", "How the delete is found", "Where in the code", "Tyre datasets"], [
        ["<code>change_feed</code>", "The change feed sends a row for the delete, with the operation D in the "
                                     "column <code>_op</code>. The row carries the key, the order value and the commit time.",
         "<code>_incoming</code> keeps the row as an event", "Sales order lines, deliveries"],
        ["<code>key_compare</code>", "A full copy is the whole source table. A key that is live in silver and "
                                     "missing from the newest copy was deleted at the source.",
         "<code>_key_compare_deletes</code> makes a delete event for it", "Materials, presses"],
        ["<code>none</code>", "Nothing is detected.", "", "Dealers, dealer sell-out, and the three event datasets"],
    ])
    out += "<h3>A delete in a change feed</h3>"
    out += code(HIST, r"^def _incoming\(", caption="A delete row is an event like any other, unless the dataset applies no deletes.")
    out += ("<p>Nothing special happens at detection. The delete row passes the earlier stages like any row, "
            "because it has a key and the business columns. Here it gets its operation and its time. If the "
            "dataset's apply rule is <code>none</code>, the row is dropped on the last line and the delete never "
            "reaches history.</p>")
    out += "<h3>A key missing from a full copy</h3>"
    out += code(HIST, r"^def _key_compare_deletes\(", caption="Making delete events for keys the newest copy no longer has.")
    out += ladder([
        ("Three conditions", "The function does nothing unless detection is <code>key_compare</code>, the apply "
                             "rule is not <code>none</code>, and the dataset is read as a full copy."),
        ("Find the missing keys", "<code>live</code> is silver's current rows that are not deleted. <code>gone</code> "
                                  "is the live rows whose key is not in the incoming copy."),
        ("Read the copy's stamp", "<code>copy</code> is one row holding the copy's highest order value, its "
                                  "latest time, and its batch, file and window."),
        ("Build the delete events", "Each missing key's stored row is turned into an event and stamped with the "
                                    "copy's values. The operation is set to D and the event is marked as new. "
                                    "So the delete takes the copy's order value and time."),
    ])
    out += ("<p>The result is a delete event that looks just like one from a change feed. From here on the two "
            "kinds of delete are handled by the same code.</p>")
    out += "<h2>The mass-delete guard: a short copy never reaches silver</h2>"
    out += ("<p>Finding deletes by comparison has a danger. If an extract breaks and delivers only part of the "
            "table, every key it left out looks deleted. The guard stops this in ingestion, before the copy is "
            f"written to bronze. Because silver reads only loaded batches, a held copy is never compared. "
            f"{link('ingest-steps', 'The ingestion steps page')} covers the other entry steps.</p>")
    out += code(STEPS, r"^def mass_delete_problem\(", caption="The guard: the share of known keys missing from the copy, against the declared limit.")
    out += ("<p>The function returns a reason when the copy must be held, and an empty text otherwise. It only "
            "acts on a full copy, only when the dataset declares a limit, and only when there are known keys to "
            "compare with. It counts the known keys that the copy lacks, divides by the number of known keys, "
            "and holds the copy when the share is above the limit. At exactly the limit the copy loads.</p>")
    out += code(PIPES, r"^def _guard_keys\(", caption="What the copy is compared with: silver's live keys, or the last accepted copy while silver is empty.")
    out += code(EDGE, r"    def current_keys\(self, source: str, dataset: str, keys", caption="Silver's answer: the keys of rows that are current and not deleted.")
    out += ("<p>The comparison is with silver's live keys. That choice matters. Keys silver has already marked "
            "deleted are not counted as missing again, so a table that slowly shrinks does not trip the guard. "
            "Before silver has ever been written, the guard compares with the last copy that was accepted into bronze.</p>")
    out += code(TYRE + "erp.material.yaml", r"^history:", r"^  mass_delete_max_share",
                "Materials: deletes by key compare, marked, and a copy missing more than 5 percent of the keys is held.")
    out += ("<p>A held copy is logged with the status <code>held_mass_delete</code>, nothing is appended, and "
            "the dataset run fails. The next good copy is compared with the last accepted one, not with the held one.</p>")
    out += "<h2>Application: three settings</h2>"
    out += code(PLAN, r"^DELETE_APPLY = ", 1, "The three values of load.deletes.apply.")
    out += table(["Value", "What happens to the key in silver", "Where in the code"], [
        ["<code>mark_deleted</code>", "The row stays. <code>_is_deleted</code> is true and <code>_deleted_at</code> "
                                      "holds the delete's time.", "<code>scd2</code> and <code>scd1</code> set the two columns"],
        ["<code>remove</code>", "Every row of a key whose current version is deleted gets the action <code>delete</code>, and the merge removes them all.",
         "<code>_finish</code>"],
        ["<code>none</code>", "The delete is ignored. The key stays as it was.", "<code>_incoming</code> drops the "
                                                                              "row, and <code>_key_compare_deletes</code> returns nothing"],
    ])
    out += code(HIST, r"^def _finish\(", caption="The remove rule: a key whose current version is deleted loses every row it has.")
    out += "<h2>What type 2 and type 1 do with a delete</h2>"
    out += ("<p>Both treat the delete as an event placed in the key's source order. They differ in what a row is.</p>")
    out += table(["", "Type 2", "Type 1"], [
        ["Where the delete lands", "In the version that is open at the delete's place in the order. It opens no version of its own.",
         "It competes with the other events of the key. It wins when it has the highest order value."],
        ["What silver holds afterwards", "The same version, still current, with the same start and row key, flagged deleted.",
         "The one row of the key, flagged deleted. Its <code>_seq_from</code> is the delete's order value."],
        ["Earlier history", "Every earlier version stays as it was.", "There is none."],
        ["A delete that arrives late", "It marks the older version it falls in. The current version stays live.",
         "It loses to the newer stored row and changes nothing."],
        ["With the rule <code>remove</code>", "Every version of the key is removed, because its current version is deleted.",
         "The row is removed."],
    ])
    out += ("<p>A live row is one where <code>_is_current</code> is true and <code>_is_deleted</code> is false. "
            "A deleted key therefore has a current row and no live row. Here is contract case 7 again. "
            "The delete was committed at 00:05.</p>")
    out += table(["Key", "Value", "<code>_seq_from</code>", "<code>_valid_to</code>", "<code>_is_current</code>",
                  "<code>_is_deleted</code>", "<code>_deleted_at</code>"], [["a", "1", "1", "open", "yes", "yes", "00:05"]])
    out += "<h2>A deleted key that comes back</h2>"
    out += ("<p>Sources do restore rows. A material is withdrawn and later reinstated. Nothing special is coded "
            "for this. It falls out of treating a delete as an event.</p>")
    out += table(["Situation", "What silver holds afterwards", "How this is known"], [
        ["Type 2, the key returns with the same tracked values",
         "The same version, live again. The returning row has a higher order value than the delete, so it becomes "
         "the row that represents the version, and the deleted flag is gone.",
         "The third case of the full copy test below"],
        ["Type 2, the key returns with a different tracked value",
         "A new current version. The old version stays in history, not current, and still flagged deleted.",
         "Run for this page on DuckDB. No test covers it."],
        ["Type 1, the key returns", "The newer row replaces the deleted one and the flag is gone.",
         "Run for this page on DuckDB. No test covers it."],
    ])
    out += ("<p>The first row works because of how versions are opened. The delete event has no tracked hash, "
            "so the returning row is compared with the values before the delete. They are equal, so no new "
            f"version opens. {link('silver-scd2', 'The type 2 page')} explains the hash.</p>")
    out += "<h2>What gold sees</h2>"
    out += ("<p>Gold reads silver's live rows, the ones that are current and not deleted, when it wants what "
            "exists today. A deleted member is still in silver, so a fact that refers to it can still find it. "
            "The dimension build carries <code>_is_deleted</code> into gold so that reports can leave deleted "
            f"members out of counts. {link('gold-dimensions', 'The gold dimensions page')} has the detail.</p>")
    out += "<h2>How it is tested</h2>"
    out += code(T_STAGES, r'\("case", "deletes", "copies", "want"\)', r'"_is_deleted"\) == want, case',
                "Full copies: a missing key is marked, no rule leaves it alone, and a key that comes back is live again.")
    out += code(T_STEPS, r'\("first copy", "full_copy", None', r"    assert bool\(reason\) is held",
                "The guard as a table: first copy, nothing missing, 40 percent missing, exactly at the limit, not a full copy, no limit.")
    out += code(T_PIPES, r"\(\"silver's keys are the comparison", r"\.status == status, case",
                "The guard against silver: keys silver already marked deleted do not count as missing.")
    out += (f"<p>Type 2's delete is case 7 of the contract, and type 1 has its own case, <i>a delete is soft</i>. "
            f"In {RUN}, five order lines were deleted at the source on day 4, and all five were kept in silver "
            "and marked deleted. On day 9 a material copy arrived with 720 of 1,800 keys missing, which is 40 "
            f"percent, and the guard held it. Both results are quoted from {src(NOTES)}.</p>")
    out += "<h2>What is not handled</h2>"
    out += not_built([
        "A hard delete at a source that has no change feed and sends no full copy is never seen. The dealer "
        "interface is read page by page since the last cursor, and its manifest says detect none. A dealer "
        "removed there stays live in silver for ever.",
        "A change feed that drops a delete, or a delete that is filtered out before the feed, is not found by "
        "any later comparison. A change feed dataset is never compared with a full copy.",
        "Events are never deleted. Append history does not read the operation column.",
        "A snapshot has no deletes. A row a newer file leaves out is switched to not current, with no flag and no time.",
        "The guard only exists for full copies with a declared limit. It compares with the keys silver holds at "
        "that moment, so the very first copy of a dataset has nothing to be compared with.",
        "No tyre dataset uses the <code>remove</code> rule. It is tested on a small table (every version of a deleted "
        "key goes, other keys stay) but has not been run on the simulated company.",
        "The declarative engine cannot detect deletes by comparing full copies, and it stores a delete "
        f"differently. {link('silver-declarative', 'The declarative engine page')} explains both.",
    ])
    return out


# ------------------------------------------------------------------------------------------------ declarative engine
def declarative_picture():
    d = box(10, 80, 150, 70, "Bronze", "read as a stream", "bx", href="ingest-lander.html")
    d += box(210, 80, 190, 70, "Eleven stages", "the same functions", "solid", href="silver-stages.html")
    d += box(450, 20, 180, 60, "Valid rows", "a temporary view", "bx")
    d += box(450, 150, 180, 60, "Set-aside rows", "an append flow", "bxm")
    d += box(680, 20, 170, 60, "AUTO CDC", "keeps the history", "bxs")
    d += box(890, 20, 95, 60, "Silver", "", "bx")
    d += box(680, 150, 170, 60, "Quarantine table", "", "bx", href="silver-quality.html")
    d += arrow(160, 115, 208, 115) + arrow(400, 100, 448, 55) + arrow(400, 130, 448, 175)
    d += arrow(630, 50, 678, 50) + arrow(850, 50, 888, 50) + arrow(630, 180, 678, 180)
    d += text(495, 250, "Stage 10 (duplicates) and stage 12 (history) are left out of the flow", "tx", "middle")
    return d


@page("silver-declarative", "The declarative engine: the same stages, history by AUTO CDC", parent="silver",
      short="Declarative engine",
      blurb="The stage functions run unchanged inside a declarative pipeline. Its change capture keeps the history.")
def silver_declarative():
    out = ("<p class='lead'>A declarative pipeline is a way of running data work where you describe the tables "
           "you want and the product works out how to build and update them. The declarative engine runs our "
           "stage functions inside such a pipeline. For history it does not use our history stage. It uses the "
           "pipeline's own change capture, called AUTO CDC and also known as apply changes.</p>"
           "<p>It exists to answer a fair question: why write history code at all when the product has a managed "
           "feature for it? The answer here is to support both, from the same settings, and to hold both to the "
           "same table of cases.</p>")
    out += fig(995, 270, declarative_picture(), "The declarative flow for one dataset",
               "The highlighted box is our own code, the same functions the job engine calls. The grey box is the product's change capture.")
    out += flow3("The dataset's plan from the metadata store, and its bronze table read as a stream.",
                 "Eleven of the thirteen stages run on the stream. Rows that pass go to a view. Rows set aside go "
                 "to the quarantine table. The change capture reads the view and keeps the silver table.",
                 "A streaming silver table per dataset, and a quarantine table when a stage can set rows aside.")
    out += "<h2>Why two stages cannot run in the flow</h2>"
    out += ("<p>A declarative flow reads a stream. A stream hands over new rows and never the whole table. Two "
            "stages need the whole table. Stage 10 ranks rows of the same key against each other and looks into "
            "silver for repeats. Stage 12 lays the new changes beside silver's stored versions. So the recipe "
            "switches those two off and hands their work to the pipeline.</p>")
    out += code(DECL, r"^OUTSIDE_THE_FLOW = ", 2, "The two stages left out, and the name of the operation column.")
    out += "<h2>The recipe: the plan translated for the pipeline</h2>"
    out += ("<p>The recipe is a small object built from the plan. It holds no product code. It says which stages "
            "run in the flow and what to ask the change capture for.</p>")
    out += code(DECL, r"^class ChangeCapture:", caption="What the change capture is asked to do, in product-free terms.")
    out += code(DECL, r"^class Recipe:", caption="The recipe: the plan for the flow, and either a change capture or keys for removing repeats.")
    out += code(DECL, r"^def recipe\(", caption="How a plan becomes a recipe, or is refused.")
    out += ladder([
        ("The flow plan", "A copy of the plan with stages 10 and 12 added to its switched-off stages. Anything the "
                          "dataset already switched off stays off."),
        ("Type 1 and type 2", "The plan must have business keys and an order column, and must not detect deletes "
                              "by comparing full copies. Then a <code>ChangeCapture</code> is filled in."),
        ("Append", "No change capture. If the dataset removes duplicates, the keys for that are passed on. When "
                   "the plan has an event time and a late lookback, they bound how long keys are remembered. "
                   "Otherwise a note says the keys are remembered for ever."),
        ("Anything else", "Snapshot and source versioned history are refused with a message that names the job engine."),
    ])
    out += "<h2>From our plan to the change capture call</h2>"
    out += ("<p>Each row is one thing the job engine's history stage decides. The columns show where it sits in "
            "the plan, what the recipe turns it into, and the argument of the pipeline call that receives it.</p>")
    out += table(["What", "In the plan", "In the recipe", "In the call"], [
        ["The business key", "<code>keys</code>", "<code>keys</code>", "<code>keys</code>"],
        ["The source's order of changes", "<code>order_by</code>", "<code>sequence_by</code>", "<code>sequence_by</code>"],
        ["Type 1 or type 2", "<code>history</code> is scd1 or scd2", "<code>scd_type</code> 1 or 2", "<code>stored_as_scd_type</code>"],
        ["Tracked columns", "<code>track</code>", "<code>track</code>", "<code>track_history_column_list</code>, or nothing when the list is empty"],
        ["Deletes", "<code>delete_detect</code> is change_feed and <code>delete_apply</code> is mark_deleted or remove",
         "<code>delete_when</code> is the condition <code>_op = 'D'</code>", "<code>apply_as_deletes</code>"],
        ["Columns not stored", "<code>ingest_read</code> is change_feed", "<code>exclude</code> holds <code>_op</code>", "<code>except_column_list</code>"],
    ])
    out += code(REALM, r"^def declare\(", caption="The generic pipeline definition for one dataset. It is the only place the product's functions are called.")
    out += ("<p>Walk through it. <code>staged</code> calls the same <code>run_stages</code> the job engine "
            "calls, with the flow plan, the bronze table as a stream, and no silver table. The rows that pass "
            "become a temporary view. If any stage can set rows aside, a quarantine table is created and an "
            "append flow feeds it.</p>"
            "<p>For a type 1 or type 2 dataset the silver table is created as a streaming table and "
            "<code>create_auto_cdc_flow</code> is called with the six values from the table above. For an append "
            "dataset the valid rows are written as they come. With bounded keys, repeats are dropped within a "
            "watermark, which is the pipeline's limit on how late a row may still arrive. With unbounded keys "
            "every key ever seen is remembered.</p>")
    out += code(REALM, r"^for ref in store.datasets\(DOMAIN\):", caption="One loop over the metadata store declares every dataset. There is no file per table.")
    out += ("<p>The loop reads the datasets of the domain from the metadata store. A dataset is declared when "
            "its <code>silver_engine</code> setting says declarative, or when it is named in the pipeline's "
            "<code>only</code> setting for a proof run. The plan is built the same way as for the job engine, "
            "the contract's row rules are added, and the recipe is made. The recipe's notes are printed.</p>")
    out += "<h2>How changes that arrive out of order are handled</h2>"
    out += ("<p>The job engine sorts the events itself. The declarative engine passes the order column as "
            "<code>sequence_by</code> and the change capture does the ordering. A change with a lower sequence "
            "than one already applied is placed in history and does not become current. Arrival order is not a "
            "sequence, so the recipe refuses a type 1 or type 2 dataset that has no order column.</p>"
            "<p>The spike that proved this feeds the pipeline a promised date moved twice, with the older change "
            "after the newer one.</p>")
    out += code(SPIKE + "make_changes.py", r"^BATCHES = \{", r"^\}$", "The spike's two batches. In batch 2 the change with sequence 30 comes before the change with sequence 20.")
    out += code(SPIKE + "show.py", r"^checks = \{", r"^\}$", "The five checks the spike makes on the table the pipeline built.")
    out += (f"<p>The spike's notes record that all five checks passed on the managed pipeline service on 13 and "
            f"14 November 2026. They also record that the open-source pipeline could not run the same flow on "
            f"that date, because no open table format supported the one Spark version that had type 2 change "
            f"capture. The notes are in {src(SPIKE + 'README.md')}.</p>")
    out += "<h2>How deletes are handled</h2>"
    out += ("<p>Only one kind of delete can be expressed: a row in a change feed with operation D. The recipe "
            "turns it into the condition <code>_op = 'D'</code>, and the change capture treats a matching row as "
            "a delete of that key at that sequence. The <code>_op</code> column is then left out of the stored table.</p>"
            "<p>Deletes found by comparing full copies cannot be expressed. Comparing needs the whole silver "
            "table, and the flow never has it. Such a dataset is refused.</p>"
            "<p>Both apply rules, <code>mark_deleted</code> and <code>remove</code>, become the same condition. "
            "The declarative engine has no way to tell them apart.</p>")
    out += "<h2>What differs in the stored table</h2>"
    out += ("<p>The two engines agree on what the history means. They do not store it the same way. Each row "
            "below is one difference.</p>")
    out += table(["", "Job engine", "Declarative engine"], [
        ["A version's span", "<code>_seq_from</code> holds the start in the source order. <code>_valid_from</code> "
                             "and <code>_valid_to</code> hold times.",
         "<code>__START_AT</code> and <code>__END_AT</code>, written by the change capture. In the spike they hold "
         "sequence values, 10, 20 and 30."],
        ["The current version", "<code>_is_current</code> is true", "<code>__END_AT</code> is empty"],
        ["A deleted key, type 2", "The last version stays current and is flagged: <code>_is_deleted</code> true, "
                                  "<code>_deleted_at</code> set.",
         "The last version is closed: <code>__END_AT</code> is set and no version follows."],
        ["Row identity", "<code>_row_key</code>", "The keys and <code>__START_AT</code>"],
        ["Columns our history stage adds", "Present", "Absent, because stage 12 does not run"],
        ["Columns the other stages add", "Present", "Present. The key hash, the quality flags and the provenance columns come from stages that run in the flow."],
    ])
    out += ("<p>Because of the third row, the two outputs cannot be compared column by column. The shared "
            "contract compares meaning instead. A helper reads the declarative table and says: the last version "
            "of a key is current, and it is deleted when it has an end.</p>")
    out += code(CONTRACT, r"^def normalise_closed\(", caption="Turning a table that closes deleted keys into the contract's five values per version.")
    out += "<h2>How it is tested</h2>"
    out += (f"<p>{src(T_DECL)} tests the recipe, which needs no pipeline. Its tables cover the change capture "
            "for five kinds of history dataset, the keys for append datasets, the bound on remembered keys, the "
            "two stages left out, and each refusal with its reason.</p>")
    out += code(T_DECL, r'\("SCD2 from a change feed with deletes"', r"    assert r.dedupe_keys == \(\)",
                "Five history cases. Each names the plan and the exact change capture it must produce.")
    out += code(T_DECL, r'\("snapshot history", \{"history": "snapshot"', r"        recipe\(plan\(\*\*kw\)\)",
                "Five refusals, each matched against the words of its message.")
    out += ("<p>The pipeline itself can only be tested where the product runs. The spike holds a pipeline that "
            "declares one table per contract case, and a runner that feeds the batches round by round and "
            "compares the result with the same eight cases the job engine passes.</p>")
    out += code(SPIKE + "lakeflow/transformations/contract.py", r"^def declare\(", caption="One streaming table per contract case, with the value tracked and the note not.")
    out += not_built([
        "Three of the nine tyre datasets cannot run here: materials and presses, which find deletes by comparing "
        "full copies, and dealer sell-out, which is a snapshot.",
        "A critical quality rule cannot hold the batch. The broken rows go to the quarantine table and the rest of the batch is written.",
        "The pipeline definition writes nothing to our run log. It prints the recipe's notes. The pipeline's own event log is the record.",
        "The stored table lacks <code>_is_current</code>, <code>_is_deleted</code>, <code>_valid_from</code> and "
        "<code>_row_key</code>. Nothing in the repository adds them back, so a reader written for the job "
        "engine's columns cannot read a declarative table as it is.",
        "When a type 2 dataset lists no tracked columns, the call passes no list and the change capture tracks "
        "every column of the view. The job engine tracks only the kept contract columns. All four type 2 tyre "
        "datasets list their tracked columns, so the difference does not arise there. It was not tested.",
        "Of the three append tyre datasets, only press readings has both an event time and a late lookback. Lab "
        "inspection results and production records get unbounded removal of repeats, with the recipe's note. "
        "This is read from the manifests and the code and was not run.",
        "What the change capture does with a delete on a type 1 table is not shown anywhere in the repository. "
        "The spike and the contract cover type 2 only.",
        "No recorded output of the eight contract cases on the pipeline is in the repository. The runner is "
        "there. Its result could not be checked for this page.",
    ])
    return out


# ------------------------------------------------------------------------------------------------ engines compared
def engines_picture():
    d = box(10, 110, 140, 70, "The plan", "same settings", "bxs", href="silver-plan.html")
    d += box(210, 20, 170, 60, "Our runner", "lease, bookmark, log", "bx")
    d += box(430, 20, 170, 60, "Thirteen stages", "", "bx", href="silver-stages.html")
    d += box(650, 20, 170, 60, "One merge", "on _row_key", "bx", href="silver-writer.html")
    d += box(210, 210, 170, 60, "The pipeline", "streams, checkpoints", "bxm")
    d += box(430, 210, 170, 60, "Eleven stages", "", "bx", href="silver-declarative.html")
    d += box(650, 210, 170, 60, "AUTO CDC", "", "bxm", href="silver-declarative.html")
    d += box(860, 110, 125, 70, "Contract", "eight cases", "solid")
    d += arrow(150, 130, 208, 55) + arrow(150, 160, 208, 235)
    d += arrow(380, 50, 428, 50) + arrow(600, 50, 648, 50) + arrow(380, 240, 428, 240) + arrow(600, 240, 648, 240)
    d += arrow(820, 55, 880, 108) + arrow(820, 235, 880, 182)
    d += text(515, 110, "Job engine, the default", "tx", "middle")
    d += text(515, 195, "Declarative engine", "tx", "middle")
    return d


@page("silver-engines", "Two engines for silver: the job engine and the declarative engine", parent="silver",
      short="Job engine and declarative engine",
      blurb="One setting picks the engine. The loader refuses what an engine cannot do. Both answer to one history contract.")
def silver_engines():
    out = ("<p class='lead'>Bronze to silver can be run by two engines. The <b>job engine</b> is our own runner: "
           "it calls the thirteen stages and writes with one merge. The <b>declarative engine</b> runs the "
           "stages inside a declarative pipeline and lets the pipeline's change capture keep the history. A "
           "dataset picks one with a single setting.</p>"
           "<p>Two engines are only safe if two things hold. A dataset must never be run by an engine that "
           "cannot do what its manifest asks. And both engines must mean the same thing by history. This page "
           "shows how each of the two is enforced, and how far each has been checked.</p>")
    out += fig(995, 290, engines_picture(), "Two engines between one plan and one contract",
               "Both lanes start from the same plan and are judged by the same eight cases. The dashed boxes belong to the product, not to our code.")
    out += "<h2>Picking the engine</h2>"
    out += code(ENGINES, r"^def engine_of\(", caption="The dataset's setting first, then the domain's, then the default.")
    out += table(["Key", "What it means", "Allowed values", "Default"], [
        ["<code>silver_engine</code>", "Which engine builds silver for this dataset. It can be set on the dataset "
                                       "or for the whole domain.", "job, declarative", "job"],
        ["<code>table_format</code>", "The open table format the dataset's tables are written in. It limits which engine may be chosen.",
         "iceberg, delta, hudi", "Set by the deployment"],
    ])
    out += "<h2>What each engine can do, written as data</h2>"
    out += code(ENGINES, r"^CAPS: dict", r"^\}$", "The capabilities of each engine: the history types it writes and the table formats it writes natively.")
    out += ("<p>The job engine writes all five history types, in any format, because it writes through the "
            "table port. The declarative engine writes type 1, type 2 and append, and only Delta natively. The "
            "comment records why. In the spike, the pipeline's tables were Delta tables that other engines can "
            "read as Iceberg through added metadata. They were not native Iceberg tables.</p>"
            "<p>Because the capabilities are data, a deployment can pass its own table to "
            "<code>engine_problems</code> and widen or narrow an engine without changing this file.</p>")
    out += "<h2>The refusals, and why each exists</h2>"
    out += code(ENGINES, r"^def engine_problems\(", caption="Every reason a dataset cannot run on its chosen engine. An empty list means it can.")
    out += table(["Combination", "Message says", "Why it is refused"], [
        ["An engine name that is not known", "<code>silver_engine</code> is not one of the known engines", "A typing mistake must not fall back to a default silently."],
        ["Declarative with snapshot history", "history is not supported, use the job engine",
         "Replacing a window means comparing a file with what silver holds. A streaming flow never has the whole table."],
        ["Declarative with source versioned history", "history is not supported, use the job engine",
         "Checking a key's versions for overlaps and gaps needs all of them together."],
        ["Declarative with deletes detected by key compare", "deletes found by comparing full copies are not supported",
         "Finding a missing key means comparing the copy with silver's live keys."],
        ["Declarative with a table format it does not write natively", "the format is not written natively",
         "A contract that promises a native open format must not be met with a different format plus metadata."],
    ])
    out += ("<p>The same list is checked in two places. The loader checks it when the manifests are loaded into "
            "the metadata store, so a bad combination is refused before anything runs. The recipe checks it "
            f"again when the pipeline is declared. {link('meta-loader', 'The loader page')} covers the loader.</p>")
    out += code(LOADER, r'        errors \+= \[f"\{path.name\}: \{p\}" for p in engine_problems\(', 2,
                "The loader adds the engine problems to the manifest's other errors. Any error stops the load.")
    out += "<h2>The history contract both engines must pass</h2>"
    out += ("<p>The contract is eight cases written as plain data. Each case gives batches of changes in arrival "
            "order, and what silver must mean afterwards as one line per version: key, value, the sequence the "
            "version starts at, whether it is current, whether it is deleted.</p>")
    out += code(CONTRACT, r"^CASES: list", r"^\]$", "The eight cases.", cap=40)
    out += table(["Case", "What it pins down"], [
        ["1", "A first load gives one current version per key."],
        ["2", "A tracked change opens a version."],
        ["3", "A change that is not tracked updates the version in place."],
        ["4", "Several changes in one batch become versions in sequence order."],
        ["5", "An older change that arrives after a newer one goes into history. The newer one stays current."],
        ["6", "A late change equal to the version after it moves that version's start."],
        ["7", "A delete marks the key deleted. Nothing is removed."],
        ["8", "The same batch twice changes nothing."],
    ])
    out += (f"<p>{link('silver-scd2', 'The type 2 page')} works through all eight with before and after tables.</p>"
            "<p>The engines store a deleted key differently, so their tables are first brought to the contract's "
            "shape. The job engine needs no help: its columns are the contract's values. For an engine that "
            "closes the deleted version there is a helper. The file's description calls this step "
            "<code>normalise</code>. The function it provides is <code>normalise_closed</code>.</p>")
    out += code(CONTRACT, r"^def normalise_closed\(", caption="For an engine that closes a deleted key: the last version is current, and it is deleted when it has an end.")
    out += "<h2>Side by side</h2>"
    out += ("<p>Each row is one job that bronze to silver has to do. The two columns say who does it on each engine.</p>")
    out += table(["", "Job engine", "Declarative engine"], [
        ["Who orders changes", "Our history stage, by the order column", "The change capture, by <code>sequence_by</code>"],
        ["Who writes", "Our writer, one merge on <code>_row_key</code> or an append with a transaction number", "The pipeline"],
        ["Deletes from a change feed", "Marked: the version stays current and is flagged. Removed when the rule is remove.",
         "The key's last version is closed. The two apply rules are not told apart."],
        ["Deletes by comparing full copies", "Yes, behind the mass-delete guard", "Refused"],
        ["Late changes", "Placed in history by the order column. A touched key's versions are computed again.", "Placed in history by the sequence"],
        ["Full copies", "Yes", "Only when deletes are not detected by key compare"],
        ["Snapshots", "Yes", "Refused"],
        ["Source versioned history", "Yes", "Refused"],
        ["Duplicates", "Stage 10, including a look into silver", "For append datasets, the pipeline drops repeats by key. For type 1 and type 2, the change capture's keys and sequence."],
        ["Quarantine", "Set-aside rows go to the quarantine table", "The same, through its own flow"],
        ["Hold on a critical rule", "The whole batch is held and the bookmark stays", "Not possible. The rows are quarantined and the rest is written."],
        ["Run log", "Every stage's counts go to the step log", "Not written. The pipeline's own event log is the record."],
        ["What was read", "The file log and the bookmark", "The pipeline's stream checkpoints"],
        ["Table format", "Any, through the table port", "Delta natively"],
        ["Where it runs", "Spark, and DuckDB for tests and small runs", "Only inside the pipeline product"],
    ])
    out += "<h2>When to choose which</h2>"
    out += table(["If the dataset", "Choose", "Because"], [
        ["Is a full copy that can lose rows", "Job", "Only the job engine finds deletes by comparison."],
        ["Is a snapshot or keeps its own versions at the source", "Job", "The declarative engine refuses both."],
        ["Has a critical rule that must stop the batch", "Job", "Only the job engine can hold a batch."],
        ["Must be in a native open format other than Delta", "Job", "The declarative engine writes Delta natively."],
        ["Must appear in the platform's run log, stage by stage", "Job", "The pipeline does not write it."],
        ["Is read downstream by its <code>_is_current</code> and <code>_is_deleted</code> columns", "Job", "The declarative table does not have them."],
        ["Is a change feed or an event stream, and the team wants the product to manage the writes", "Declarative",
         "Type 1, type 2 and append are what it supports, from the same manifest."],
        ["Is anything else, or you are unsure", "Job", "It is the default and takes every dataset."],
    ])
    out += "<h2>How it is tested</h2>"
    out += (f"<p>{src(T_ENGINES)} is four tables. They cover which engine a dataset gets, the history type read "
            "from a manifest, every refusal with the words of its message, and loading the real tyre manifests "
            "with the engine setting changed.</p>")
    out += code(T_ENGINES, r'\("case", "manifest", "settings", "platform", "says"\)', r"    assert all\(s in g",
                "The refusal table. The last column is the words each message must contain. An empty list means accepted.")
    out += code(T_ENGINES, r"^def test_a_binding_can_widen_an_engine\(", caption="A deployment's own capability table lifts a refusal.")
    out += (f"<p>The history contract is not run in that file. The job engine runs it in {src(T_STAGES)}, once "
            f"per case. The declarative engine runs it through the spike's runner, which needs the pipeline product.</p>")
    out += code(SPIKE + "lakeflow/run_contract.py", r"^for case, _, want in CASES:", r"    good = got == want",
                "The declarative side of the contract: read each case's table, bring it to the contract's shape, compare.")
    out += "<h2>What is verified and what is not</h2>"
    out += ("<p>Each row is one claim about the two engines. The second column says how it was checked for this "
            "page, or that it could not be.</p>")
    out += table(["Claim", "Status"], [
        ["Six of the nine tyre datasets are accepted by the declarative engine and three are refused",
         "Verified. <code>engine_problems</code> was run on each of the nine manifests with the engine set to "
         "declarative. Dealers, deliveries, sales order lines, lab inspection results, production records and "
         "press readings returned no problem. Materials and presses were refused for deletes by key compare. "
         "Dealer sell-out was refused for snapshot history."],
        ["The job engine passes the eight contract cases",
         "Verified by running the eight cases through the stage functions on DuckDB and reading the result. The "
         "test file was not run as a whole for this page. The design note records it passing on Spark."],
        ["The loader refuses a tyre dataset that asks for an engine it cannot use",
         "Verified in the code, where the loader adds the problems to its errors. The test with the real tyre "
         "manifests has two cases and both are accepted. Its refused branch is never taken."],
        ["The declarative engine places a late change in history and closes a deleted key",
         "Recorded, not repeated. The spike's notes say all five of its checks passed on the managed pipeline "
         "service on 13 and 14 November 2026."],
        ["The declarative engine passes the eight contract cases",
         "Not verified. The pipeline and the runner are in the repository. No recorded result is."],
        ["The declarative engine's tables equal the job engine's on the six datasets",
         "Not verified. A comparison script is in the repository beside the pipeline. It compares live rows, "
         "version counts and business columns for those six datasets. It needs a deployed workspace, and no "
         "output of it is recorded in the repository. The public snapshot's design notes do not mention the "
         "declarative engine at all."],
    ])
    out += not_built([
        "No test runs the two engines on the same data in one place. The contract is shared, but each engine runs it in its own harness.",
        "The contract covers type 2 only. Type 1 and append on the declarative engine have recipe tests and no shared cases.",
        "The comparison of the two engines' tables looks at live rows. It does not compare how a deleted key is stored, which is the known difference.",
        "The capability table lists what the declarative engine supports. It does not list what it lacks at run "
        "time: the hold on a critical rule and the run log are not refusals, so a dataset that needs them can "
        "still be moved to the declarative engine without a warning.",
    ])
    return out
