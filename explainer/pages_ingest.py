"""Ingestion: the hub, the run loop, the five pipelines, the entry steps, landing to bronze, and the sources."""
from lib import *  # noqa: F401,F403

RUNNER = "P/src/dataplatform/ingest/runner.py"
PIPES = "P/src/dataplatform/ingest/pipelines.py"
STEPS = "P/src/dataplatform/ingest/steps.py"
IPORTS = "P/src/dataplatform/ingest/ports.py"
DISC = "P/src/dataplatform/ingest/discovery.py"
DAYS = "P/src/dataplatform/ingest/daybyday.py"
SPORTS = "P/src/dataplatform/source_ports.py"
LANDER = "P/src/dataplatform/lander.py"
KEYS = "P/src/dataplatform/keys.py"
LEASES = "P/src/dataplatform/leases.py"
RESIL = "P/src/dataplatform/resilience.py"
SPLAN = "P/src/dataplatform/silver/plan.py"
A = "P/src/dataplatform/adapters/"
T = "P/tests/dataplatform/"
T_PIPE = T + "test_ingest_pipelines.py"
T_STEPS = T + "test_ingest_steps.py"
T_PORTS = T + "test_ingest_ports.py"
T_FORMATS = T + "test_ingest_formats.py"
T_WORLD = T + "test_ingest_world.py"
T_SRC = T + "test_source_ports.py"
T_DISC = T + "test_discovery.py"
TYRE = "C/slices/02-tyre/manifest/datasets/"


# ------------------------------------------------------------------------------------------------------------- hub
def hub_shape():
    d = box(10, 120, 150, 90, "Source systems", ["database, files,", "interface, stream"], "bxm", href="ingest-sources.html")
    d += box(200, 30, 130, 70, "Movers", "only copy", "bxm", href="ingest-sources.html")
    d += box(370, 30, 130, 70, "Landing", "as delivered", "bx", href="ingest-lander.html")
    d += box(550, 120, 150, 90, "Five pipelines", ["find what is new"], "bx", href="ingest-pipelines.html")
    d += box(740, 120, 130, 90, "Entry steps", ["check, stamp,", "write"], "solid", href="ingest-steps.html")
    d += box(895, 120, 90, 90, "Bronze", "", "bx", href="ingest-lander.html")
    d += box(550, 270, 150, 70, "Runner", "one run, in order", "bx", href="ingest-runner.html")
    d += box(740, 270, 245, 70, "Metadata store", "run log, file log, positions", "bxs", href="meta.html")
    d += arrow(160, 140, 198, 80) + arrow(330, 65, 368, 65) + arrow(500, 80, 548, 140, "push")
    d += arrow(160, 185, 548, 185, "pull and stream")
    d += arrow(700, 165, 738, 165) + arrow(870, 165, 893, 165)
    d += arrow(625, 270, 625, 212) + arrow(805, 210, 805, 268)
    return d


@page("ingest", "Ingestion: from a source system to bronze", parent="index", short="Ingestion",
      blurb="Three ways in: push, pull and stream. Five pipelines find what is new. Every batch passes the same entry steps.")
def ingest():
    out = ("<p class='lead'>Ingestion brings data from the systems that produce it into bronze, the first layer "
           "of the platform. Bronze holds every batch exactly as the source sent it, with a few columns added "
           "that say where each row came from.</p>"
           "<p>Sources differ a great deal. One sends files, one must be asked, one never stops talking. If each "
           "had its own loading code, each would have its own idea of a duplicate, of a broken file and of what "
           "to record. Here the sources differ only in how a batch is found. Once a batch is in hand, the same "
           "checks run for all of them.</p>")
    out += fig(990, 360, hub_shape(), "The parts of ingestion",
               "Read it left to right. The upper path is push, the lower path is pull and stream. The highlighted box is "
               "where every batch is checked. Click a box to zoom in.")
    out += "<h2>Three words used on every page</h2>"
    out += table(["Word", "Meaning"], [
        ["Landing zone", "A storage folder that holds delivered files exactly as they arrived, each with a small manifest file beside it. A delivery is written once and never changed."],
        ["Lander", "Our code that takes a batch, checks it and writes it to bronze. It is the same code whether the batch came from landing, from a database or from a stream."],
        ["Bronze", "One table per dataset that keeps every accepted batch as delivered. Nothing is cleaned or merged here."],
    ])
    out += "<h2>Push, pull and stream</h2>"
    out += table(["Way in", "Who starts it", "Where a batch comes from", "Why it exists"], [
        ["Push", "The source side. A mover, which is a service outside our code, copies files into landing.", "A delivery in landing.", "Change capture services and file transfers already exist as products. The platform only asks that they write each delivery once with a manifest."],
        ["Pull", "Our run. It asks the source.", "Rows read from a database copy or from pages of an interface. There is no landing hop.", "Some sources only answer questions."],
        ["Stream", "Our run. It reads each part of the stream from where it stopped to the end, then stops.", "Records from a stream. The stream itself plays the part of landing.", "Sensor readings arrive all day. Reading them in scheduled runs keeps one loading model for everything."],
    ])
    out += "<h2>The nine tyre datasets and the way each comes in</h2>"
    out += ("<p>Each dataset manifest of the tyre company states its way in with two fields, "
            "<code>ingest.mode</code> and <code>ingest.read</code>. The table lists them as the manifests have "
            "them.</p>")
    out += table(["Dataset", "Mode", "Read", "What the source is"], [
        ["<code>erp.sales_order_line</code>", "push", "change_feed", "Order lines from the database of the ERP, the company's system for orders and stock, as a feed of changes."],
        ["<code>erp.delivery</code>", "push", "change_feed", "Deliveries from the ERP database."],
        ["<code>mes.production</code>", "push", "change_feed", "Production records from the factory system."],
        ["<code>lims.inspection_result</code>", "push", "files", "CSV files from the laboratory system."],
        ["<code>partner.dealer_sellout</code>", "push", "files", "Monthly CSV files from dealers."],
        ["<code>erp.material</code>", "pull", "full_copy", "The material master, as a nightly full copy."],
        ["<code>mes.press</code>", "pull", "full_copy", "The press master, as a nightly full copy."],
        ["<code>dealer_api.dealer</code>", "pull", "pages", "Dealers from an interface, read page by page."],
        ["<code>press_stream.press_reading</code>", "stream", "events", "Press sensor readings."],
    ])
    out += "<h2>The pages under this one</h2>"
    out += table(["Page", "What it answers"], [
        [link("ingest-runner"), "What one run does, step by step, and every call it makes to the metadata store."],
        [link("ingest-pipelines"), "How each of the five pipelines finds what is new and what it remembers for next time."],
        [link("ingest-steps"), "The checks every batch passes before it may enter bronze."],
        [link("ingest-lander"), "What landing looks like, what a bronze row looks like, and why writing a batch twice changes nothing."],
        [link("ingest-sources"), "The source ports and the adapters behind them, and which of them are real connectors."],
        [link("ingest-families"), "The eight families of source routes, the products and their menus of routes, and how far each route is proven."],
    ])
    out += "<h2>The design decisions</h2>"
    out += table(["Decision", "What it means", "Why"], [
        ["One set of entry steps for every way in", "A pushed file, a pulled copy, a page and a stream chunk all go through the same function.", "A rule such as the duplicate check is written and tested once."],
        ["A batch is identified by its content", "The batch key is a hash of the batch's bytes. The file name and the arrival time are not part of it.", "A file that is sent again under a new name is still recognised."],
        ["A held batch is never a success", "A batch that fails a check is not written. Its dataset run is recorded as failed, with the reason.", "A broken delivery must be seen by a person, not skipped quietly."],
        ["Positions live in the metadata store", "How far a dataset has been read is kept in the store and moves only when the dataset run succeeds.", "A failed run is taken again from the same place."],
        ["Bronze is written once per dataset per run", "The new batches of a run are written in one commit, and only then recorded as loaded.", "The record never says loaded before the rows are there."],
    ])
    out += code("P/src/dataplatform/ingest/__init__.py", r'^"""Ingestion \(W1b', r'^"""$', "The package's own summary of its four modules.", cap=16)
    out += "<h2>How we know it works</h2>"
    out += (f"<p>{src(T_WORLD)} runs the simulated tyre company for 14 days at a small scale through ingestion "
            "with local adapters. For every day, the number of rows that reached bronze per dataset must equal "
            "the simulated company's own count. The company also plants eleven incidents, one on each of days 3 "
            "to 13, and each must get the verdict ingestion owes it.</p>")
    out += code(T_WORLD, r"^INCIDENTS = \[", r"^\]", "The planted incidents: name, day, the rule it tests, and the verdict expected from ingestion.")
    out += ("<p>A verdict of handled means ingestion did what the rule asks, such as skipping a file sent "
            "twice. Flagged means ingestion counted the problem and left the fix to silver. Deferred means the "
            f"problem is not ingestion's to catch. {link('tests')} explains the simulated company.</p>")
    out += not_built([
        "The connectors to real source systems are thin. Most adapters behind the source ports are local "
        f"stand-ins built for the simulated company. {link('ingest-sources')} says which is which.",
        "Datasets are run one after another inside a run. There is no parallel loading.",
        "Streams are read in scheduled runs that stop when they reach the end. There is no continuously "
        "running reader.",
        "A source discovery helper exists and is tested, but no runner calls it.",
    ])
    return out


# ---------------------------------------------------------------------------------------------------------- runner
@page("ingest-runner", "The run loop: one ingestion run over a domain", parent="ingest", short="Run loop",
      blurb="Open the run, take each dataset in turn, record every outcome, close the run. Every store call in order.")
def ingest_runner():
    out = ("<p class='lead'>The runner is the function that performs one ingestion run. It asks the metadata "
           "store which datasets to load, hands each one to its pipeline, and records what happened.</p>"
           "<p>It holds no knowledge of any dataset. Everything it needs comes from the store's merged view of "
           "the dataset, and everything it learns goes back into the store. A run that is not recorded did not "
           "happen as far as the platform is concerned.</p>")
    d = box(10, 100, 120, 70, "start_run", "", "bxs")
    d += box(180, 20, 620, 230, "", "", "bxm")
    d += text(490, 45, "for each enabled dataset", "tx", "middle")
    d += box(200, 100, 130, 70, "start", "dataset run", "bx")
    d += box(350, 100, 110, 70, "lease", "take it", "bx")
    d += box(480, 100, 130, 70, "pipeline", "then flush", "solid", href="ingest-pipelines.html")
    d += box(630, 100, 150, 70, "log, finish", "dataset run", "bx")
    d += box(850, 100, 120, 70, "finish_run", "", "bxs")
    d += arrow(130, 135, 198, 135) + arrow(330, 135, 348, 135) + arrow(460, 135, 478, 135)
    d += arrow(610, 135, 628, 135) + arrow(780, 135, 848, 135)
    d += text(490, 215, "an error in one dataset is recorded and the loop goes on", "sm", "middle")
    out += fig(980, 270, d, "The run loop", "The dashed frame is repeated per dataset. The grey boxes happen once per run.")
    out += "<h2>The steps, with every metadata store call in order</h2>"
    out += ladder([
        ("Open the run", "<code>start_run</code> writes the run with its id, domain, environment, trigger and run type."),
        ("List the datasets", "<code>datasets</code> returns the domain's datasets. The loop skips one that is switched off, one that is a gold table, and one that is not in the optional list of names to run."),
        ("Read the configuration", "<code>dataset_view</code> returns the dataset's merged configuration as one document."),
        ("Open the dataset run", "<code>start_dataset_run</code> stores the configuration snapshot under the dataset's fingerprint."),
        ("Take the lease", "<code>acquire_lease</code> on the name <code>bronze:source.dataset</code>. If another run holds it, a failed step is logged, the dataset run is closed as skipped, and the loop moves on."),
        ("Choose the pipeline", "The pair of ingest mode and ingest read picks one of five functions. An unknown pair is logged as an error and the dataset run fails."),
        ("Run the pipeline", "The pipeline finds the new batches. For each it calls <code>dataset_files</code>, <code>bookmarks</code> or <code>watermark</code> to know where it stands, and the entry steps call <code>batch_status</code> to spot a batch already loaded."),
        ("Flush", "The new batches are written to bronze in one commit. Then <code>log_file</code> is called once per batch, held and skipped ones included."),
        ("Record errors", "<code>log_error</code> once per held batch, with an error class that names the kind of hold."),
        ("Record the steps", "<code>log_step</code> for the pipeline itself, then one per detector that met something, then one for the profile, which also calls <code>last_profile</code> and, in a normal run, <code>save_profile</code>."),
        ("Close the dataset run", "<code>finish_dataset_run</code> with the status, the counts and, in a normal run only, the new watermark and bookmarks."),
        ("Give the lease back", "<code>release_lease</code>, always, even after an error."),
        ("Close the run", "<code>finish_run</code> works out the run's status from its dataset runs and returns it."),
    ])
    out += "<h2>The code</h2>"
    out += code(RUNNER, r"^def ingest\(", caption="The outer loop. Three calls to the store and one helper per dataset.")
    out += ("<p>The <code>plan</code> argument carries the run type and the business date. The "
            "<code>silver</code> argument is optional and gives the mass-delete guard a way to ask silver for "
            f"its current keys. {link('ingest-steps')} explains the guard.</p>")
    out += code(RUNNER, r"^def _one\(", caption="One dataset: open its run, take the lease, and make sure the lease is given back.")
    out += ("<p>Notice the order. The dataset run is opened before the lease is asked for. That is deliberate: "
            "a step can only be logged for a dataset that was started in the run, and a busy table must leave a "
            "step behind that says who held it.</p>")
    out += code(LEASES, r"^def busy\(", caption="What happens when the table is busy.")
    out += code(RUNNER, r"^def _one_leased\(", caption="One dataset with the lease held: choose, run, flush, record, close.", cap=64)
    out += ("<p>Walk through it. The lookup in <code>PIPELINES</code> comes first. The pipeline then runs inside "
            "a <code>try</code>. Any exception, such as a source that cannot be reached, is turned into one "
            "error row and a failed dataset run, and the function returns so the loop can go on to the next "
            "dataset. The context object is created with an empty <code>pending</code> list, which tells the "
            "entry steps to hold the batches back until <code>flush</code> writes them together.</p>"
            "<p>After the pipeline, the counts and the status are worked out from the outcomes. The step for "
            "the pipeline is named after the mode and the read, for example "
            "<code>ingest.push.change_feed</code>. Its status is <code>warned</code> when the dataset succeeded "
            "but something was noticed, such as delete rows in a change feed.</p>"
            "<p>The last call matters most. The watermark and the bookmarks are passed only when the run type "
            "is normal. A reload or a backfill goes over old ground and must not move the dataset's position.</p>")
    out += code(RUNNER, r"^def status_of\(", caption="The status of a dataset run, from its batches.")
    out += table(["Situation", "Dataset run status"], [
        ["At least one batch was held", "<code>failed</code>"],
        ["Batches were handled and none was held", "<code>succeeded</code>"],
        ["No batch at all", "<code>skipped</code>"],
        ["The bronze table was busy", "<code>skipped</code>, with a failed <code>orch.lease</code> step"],
        ["The pipeline raised an error", "<code>failed</code>"],
    ])
    out += code(RUNNER, r"^HELD = ", r"^\}", "The three statuses that count as held, and the rule each outcome is reported under.")
    out += code(RUNNER, r"^def detect_rows\(", caption="One step row per rule that met something in this run.")
    out += ("<p>A held batch gives a step with status <code>failed</code>. A skipped duplicate gives a step with "
            "status <code>warned</code>. Anything a pipeline noticed, such as readings that arrived out of "
            "order, also gives a <code>warned</code> step with the count.</p>")
    out += "<h2>A worked example: the day the material copy came short</h2>"
    out += ("<p>On day 9 of the simulated tyre company the nightly copy of the material master is missing a "
            "large share of its rows. The table shows what the run records that day. "
            "The first column is the dataset, the second its status, the third what the store holds.</p>")
    out += table(["Dataset", "Status", "What is recorded"], [
        ["<code>erp.material</code>", "failed", "A file log row with status <code>held_mass_delete</code>, an error row of class <code>MassDelete</code>, a failed step <code>detect.DEL-04</code>. The bookmarks do not move. Bronze is unchanged."],
        ["Every other dataset", "Its own status", "Each is loaded and recorded on its own. One dataset's hold does not stop another."],
    ])
    out += ("<p>The run as a whole ends as <code>partial</code>, because some datasets failed and some did not. "
            "The 14-day test asserts exactly this for that day.</p>")
    out += "<h2>The settings the runner reads</h2>"
    out += table(["Name", "Where it comes from", "Meaning", "Default"], [
        ["<code>is_enabled</code>", "The dataset row", "A dataset that is switched off is not run.", "Required in the manifest"],
        ["<code>kind</code>", "The dataset row", "Gold tables are skipped.", "<code>dataset</code>"],
        ["<code>ingest_mode</code>, <code>ingest_read</code>", "The dataset row", "Together they pick the pipeline.", "None. A missing pair fails the dataset."],
        ["<code>run_type</code>", "The plan passed by the caller", "normal, reload or backfill.", "normal"],
        ["<code>window</code>", "The plan passed by the caller", "The business date stamped on pulled batches.", "Empty"],
        ["<code>only</code>", "An argument", "A list of dataset names to limit the run to.", "All enabled datasets"],
        ["Lease length", "<code>LEASE_SECONDS</code>", "How long the bronze lease lasts.", "4 hours"],
    ])
    out += "<h2>How it is tested</h2>"
    out += (f"<p>{src(T_PIPE)} drives the runner against in-memory sources, an in-memory metadata store and an "
            "in-memory bronze. Each test is a short story: what the sources do, which runs happen, and what the "
            "store and bronze must show afterwards.</p>")
    out += code(T_PIPE, r"^def test_every_run_goes_through_the_run_log", caption="One run over five small datasets must leave a complete record: every dataset, a fingerprint each, one step per pipeline, a profile each, six files.")
    out += not_built([
        "Datasets are processed in sequence, in the order the store lists them. A slow source delays the ones "
        "after it.",
        "There is no retry of a whole dataset inside a run. A source call can be retried by the source guard. "
        f"See {link('ingest-pipelines')}.",
        "The lease is asked for once and not renewed during a long load.",
        "If the process dies in the middle of a dataset, its dataset run and the run stay recorded as running. "
        "The bronze write and the file log are still consistent, because a batch is logged as loaded only "
        "after the bronze commit. The stale run rows have to be dealt with by hand.",
        "When a pipeline raises an error, batches that had already passed the entry steps in that dataset are "
        "not written. They are taken again by the next run.",
    ])
    return out


# ------------------------------------------------------------------------------------------------------- pipelines
def pick_shape():
    d = box(10, 110, 170, 80, "Dataset row", ["ingest_mode,", "ingest_read"], "bxs", href="meta-model.html")
    d += box(260, 110, 150, 80, "PIPELINES", "lookup by the pair", "solid")
    names = [("push, change_feed", "push"), ("push, files", "push"), ("pull, full_copy", "pull_copy"),
             ("pull, pages", "pull_pages"), ("stream, events", "stream_events")]
    for i, (a, b) in enumerate(names):
        d += box(520, 5 + i * 62, 200, 52, a, b, "bx")
        d += arrow(410, 150, 518, 31 + i * 62)
    d += box(800, 110, 170, 80, "enter", "the entry steps", "bx", href="ingest-steps.html")
    for i in range(5):
        d += arrow(720, 31 + i * 62, 798, 150)
    return d


@page("ingest-pipelines", "The five pipelines and how one is chosen", parent="ingest", short="Five pipelines",
      blurb="Change feed, files, full copy, pages, stream. Each finds what is new in its own way and hands it to the same steps.")
def ingest_pipelines():
    out = ("<p class='lead'>A pipeline is a function that answers one question for one dataset: which batches "
           "are new since last time. It hands each new batch to the entry steps and reports the position to "
           "remember.</p>"
           "<p>There are five because sources offer five different things to hold on to: a list of files, a "
           "list of copies, a page cursor, a position in a stream. The pipelines are short because all the "
           "checking and writing is somewhere else.</p>")
    out += fig(980, 320, pick_shape(), "From two fields on the dataset to one function",
               "The pair of mode and read picks the function. The first two pairs share one function.")
    out += code(PIPES, r"^PIPELINES: dict", r"^\}", "The whole chooser.")
    out += "<h2>The five at a glance</h2>"
    out += table(["Pipeline", "What the source gives", "How new data is found", "What is remembered", "How deletes are seen", "Tyre datasets"], [
        ["Push, change feed", "Files in landing, each a set of change rows", "Deliveries in landing that the file log has not recorded", "Bookmark <code>feed_pos</code>, the watermark", "A change row with operation D", "Sales order lines, deliveries, production"],
        ["Push, files", "Files in landing, bytes as the partner sent them", "The same, limited to names that match <code>file_pattern</code>", "Bookmark <code>last_file</code>, one bookmark per feed", "Not seen", "Inspection results, dealer sell-out"],
        ["Pull, full copy", "A whole copy of a table, published each night", "The newest copy later than the bookmark", "Bookmarks <code>copy_extracted_at</code> and <code>copy_batch</code>", "A key that is no longer in the copy. Silver marks it. The guard here holds a copy that loses too many.", "Materials, presses"],
        ["Pull, pages", "Records changed after a cursor, a page at a time", "The stored cursor", "Bookmark <code>cursor</code>, the watermark", "Not seen", "Dealers"],
        ["Stream, events", "Records per shard, each with a sequence number", "The stored sequence number per shard", "One bookmark per shard, the watermark", "Not seen", "Press readings"],
    ])
    out += ("<p>A shard is one of the parallel parts a stream is split into. A cursor is a marker an interface "
            "returns that means the caller has seen everything up to here.</p>")
    out += "<h2>What every pipeline receives and returns</h2>"
    out += code(PIPES, r"^class Plan:", caption="The run type and, for a reload or a backfill, the windows to go over.")
    out += code(PIPES, r"^class Result:", caption="What a pipeline returns: the outcome of each batch, the positions to store, and counts of what it noticed.", cap=12)
    out += "<h2>1. Push, change feed</h2>"
    out += ("<p>A change feed is a list of the inserts, updates and deletes made in a source table, in the order "
            "they were committed. A change capture mover writes pieces of the feed into landing as files. The "
            "tyre company's sales order lines, deliveries and production records arrive this way.</p>")
    out += code(PIPES, r"^def push\(", caption="The push pipeline. It serves both change feed deliveries and plain files.", cap=45)
    out += ("<p>Walk through it. The file log of the dataset is read once and turned into two sets: the "
            "deliveries recorded at all, and the deliveries loaded. The deliveries in landing are then taken in "
            "window and name order. In a normal run a delivery the file log already has is passed over. That "
            "list is how new data is found. The pipeline does not use the watermark to choose.</p>"
            "<p>For a change feed the bytes are parsed here, because three columns have to be renamed first. "
            "The source's operation, feed position and commit time become <code>_op</code>, "
            "<code>_feed_pos</code> and <code>_committed_at</code>. The batch then goes to "
            "<code>enter</code>. When it loads, the pipeline works out the highest feed position and the highest "
            "value of the watermark column in the rows, and reports both.</p>")
    out += code(PIPES, r"^def _feed_notices\(", caption="What the pipeline counts in a change delivery: deletes, and changes that arrived out of order.")
    out += ("<p>Deletes are seen as rows whose operation is D. They are counted and written to bronze like any "
            f"other row. Applying the delete is silver's job. See {link('silver-deletes')}. A change is counted "
            "as out of order when its sequence is lower than one already seen for the same key in the same "
            "delivery.</p>")
    out += code(PIPES, r"^def feed_columns\(", caption="A change capture service may name its three columns differently. Three settings map them.")
    out += "<h2>2. Push, files</h2>"
    out += ("<p>Partners and the laboratory drop CSV files. A file mover copies each file into landing byte for "
            "byte. The same <code>push</code> function handles them, with three differences. A delivery whose "
            "name does not match the dataset's <code>file_pattern</code> is left alone. The bytes are not parsed "
            "in the pipeline. And the bookmark is the name of the last file loaded.</p>")
    out += code(PIPES, r"^def _feed_bookmarks\(", caption="One bookmark per feed. For the dealer sell-out files, a feed is a dealer.")
    out += ("<p>A dealer sends one file a month. If every row of a file shares one dealer code, the pipeline "
            "keeps a bookmark named after that dealer with the newest month in the file. A dealer that has not "
            "sent its file can then be seen as behind the others. Files have no way to say that a row was "
            "deleted, and the two tyre manifests say so with <code>deletes.detect: none</code>.</p>")
    out += "<h2>3. Pull, full copy</h2>"
    out += ("<p>Small master tables are published whole each night. The run takes only the newest copy. An "
            "older copy that was never taken is of no use once a newer one exists.</p>")
    out += code(PIPES, r"^def pull_copy\(", caption="The full copy pipeline.", cap=34)
    out += ("<p>Walk through it. The source lists the extraction times of its copies. A copy is new when the "
            "file log has no record of it and its time is later than the bookmark "
            "<code>copy_extracted_at</code>. Of the new ones only the last is kept. Before the copy is read, "
            "the pipeline fetches the keys to compare it with and puts them on the context for the mass-delete "
            "guard. The batch is named <code>copy:</code> followed by the extraction time. When it loads, two "
            "bookmarks are reported: the extraction time and the batch key.</p>")
    out += code(PIPES, r"^def _guard_keys\(", caption="What the guard compares a copy with: silver's current keys if silver exists, else the last accepted copy in bronze.")
    out += ("<p>A full copy shows a delete by absence. A material that was in yesterday's copy and is not in "
            "today's was deleted. Ingestion does not mark anything. It writes the copy to bronze, and silver "
            "compares keys. Ingestion's part is the guard that refuses a copy which would delete too much at "
            "once.</p>")
    out += "<h2>4. Pull, pages</h2>"
    out += ("<p>The dealer system is an interface that returns the records changed after a cursor, oldest "
            "first, 200 at a time.</p>")
    out += code(PIPES, r"^def pull_pages\(", caption="The page pipeline. One page is one batch.", cap=36)
    out += ("<p>Walk through the normal path at the bottom. The starting cursor is the stored bookmark, moved "
            "forward if the file log shows pages loaded after it. The loop asks for a page, enters it as a "
            "batch named <code>page:</code> with the cursor range, and moves the cursor. It stops on an empty "
            "page. It also stops when a page is held, so the next run starts again at that page.</p>"
            "<p>A reload does nothing here and says why. The interface only serves records as they are now, so "
            "a page cannot be read again as it was. Bronze is the only place the old page still exists.</p>")
    out += code(PIPES, r"^def _resume_cursor\(", caption="Why a failed run does not cause a page to be read twice.")
    out += "<h2>5. Stream, events</h2>"
    out += ("<p>Press readings are appended to a stream all day. A run reads each shard from the last sequence "
            "number it loaded to the end of the shard, makes one batch per shard, and stops.</p>")
    out += code(PIPES, r"^def stream_events\(", caption="The stream pipeline.", cap=46)
    out += ("<p>Walk through the normal path. The stored bookmarks hold one sequence number per shard. The file "
            "log is also scanned for the highest sequence loaded per shard, and the larger of the two is where "
            "reading starts. Records are read in chunks of 200,000 until the shard is empty. The batch is "
            "named with the shard and the first and last sequence number, which is what lets a reload read the "
            "same range again. Every reading is written to bronze, replays included.</p>")
    out += code(PIPES, r"^def _stream_notices\(", caption="Replayed readings and late readings are counted, not removed.")
    out += "<h2>Where a run starts after a failure</h2>"
    out += ("<p>Bookmarks move only when a dataset run succeeds. But a batch is committed by its file log "
            "record, and a run can fail after some batches loaded. Each pipeline therefore reads both the "
            "bookmark and the file log and starts from whichever is further on. The duplicate check in the "
            "entry steps is the second line of defence.</p>")
    out += "<h2>Reload and backfill</h2>"
    out += code(PIPES, r"^def _replay_names\(", caption="Which recorded batches a reload or a backfill goes over again.")
    out += table(["Run type", "What it takes", "Does it move positions"], [
        ["normal", "Batches not seen before.", "Yes, on success."],
        ["reload", "Every batch recorded for the given windows, loaded or not. The write replaces the rows of the batch key, so nothing changes if the data is the same.", "No."],
        ["backfill", "The batches of the given windows that are not loaded yet. A batch that was held is checked again.", "No."],
    ])
    out += "<h2>A worked example: three runs of the sales order lines</h2>"
    out += ("<p>The values are small and made up to show the mechanics. The first column is the run, the second "
            "what is in landing, the third what the pipeline does, the fourth what the store remembers.</p>")
    out += table(["Run", "Landing holds", "Pipeline", "Stored afterwards"], [
        ["Monday", "File A with feed positions 1 to 250", "A is not in the file log. It is entered and loads.", "File log: A loaded. Bookmark <code>feed_pos</code> 250."],
        ["Tuesday", "A, and file B with positions 251 to 300", "A is in the file log and is passed over. B loads.", "File log: A, B loaded. Bookmark 300."],
        ["Tuesday again", "A and B", "Both are in the file log. Nothing is entered.", "The dataset run is skipped. Nothing moves."],
    ])
    out += "<h2>The settings the pipelines read</h2>"
    out += table(["Key", "Read by", "Meaning", "Allowed values and default"], [
        ["<code>ingest.mode</code>, <code>ingest.read</code>", "The chooser", "Which pipeline runs.", "The five pairs above. No default."],
        ["<code>file_pattern</code>", "Push, files", "Only deliveries whose name matches are taken.", "A file name pattern. No default, which means every delivery."],
        ["<code>watermark_column</code>", "Push, pages, stream", "The column whose highest loaded value is recorded as the watermark.", "A column name. No default, which means no watermark is recorded."],
        ["<code>cdc_op_column</code>, <code>cdc_position_column</code>, <code>cdc_commit_column</code>", "Push, change feed", "The names of the operation, position and commit time columns in the delivery.", "Text. The dictionary has no default. The code falls back to <code>op</code>, <code>feed_pos</code> and <code>committed_at</code>."],
        ["<code>load.history_order_by</code>", "Push, change feed", "The sequence column used to count changes that arrived out of order.", "A contract column. From the manifest's <code>load</code> block."],
        ["<code>retry_count</code>", "Pull", "How many times a failed source call is tried again.", "Whole number. Default 2."],
        ["<code>retry_interval_seconds</code>", "Pull", "The wait before the first retry. Each later wait is twice the one before, up to 600 seconds.", "Whole number. Default 60."],
    ])
    out += code(PIPES, r"^def _read\(", caption="Every pull goes through the source guard when the binding supplies one.")
    out += code(RESIL, r"    def from_settings\(cls", caption="The two retry settings become a retry policy.")
    out += "<h2>How it is tested</h2>"
    out += code(T_PIPE, r"^def test_change_feed_lands_and_notices", caption="A change feed with an out-of-order change and a delete: one batch, five rows, both noticed, bookmark and watermark set, every change kept in bronze.")
    out += code(T_PIPE, r"^def test_a_failed_run_never_makes", caption="A stream run that fails after loading. The bookmarks did not move, yet the next run reads nothing twice.")
    out += not_built([
        "The watermark is recorded but no pipeline uses it to decide what to read. New data is found from the "
        "file log, the copy list, the cursor or the stream position.",
        "The dealer manifest sets <code>rest_endpoint_path</code>, <code>rest_method</code> and the "
        "<code>pagination</code> keys. The page pipeline does not read them. It calls the interface port with "
        "the dataset name and a page size of 200 that is fixed in the code.",
        "The circuit breaker's threshold and reset time are registered as proposed settings and the source "
        "connection cap as a source setting. The guard is built with its code defaults, three failures and 30 "
        "minutes, and reads none of them.",
        "The push pipeline is not wrapped by the source guard. Only the pulls are. The stream reads are not "
        "wrapped either.",
        "The stream pipeline reads a whole shard's new records into memory before it enters them as one batch.",
        "A reload of a page dataset is a no-op by design.",
        "The stream pipeline is tested with the in-memory stream. By reading the code I see three things that "
        "would need checking against a managed stream whose shard names and sequence numbers look different: "
        "the scan of the file log looks for names that begin with <code>shard-</code>, a reload works out how "
        "many records to read by subtracting two sequence numbers, and the sequence number is stored in an "
        "integer column. I did not run this and could not verify it either way.",
    ])
    return out


# ----------------------------------------------------------------------------------------------------------- steps
def steps_shape():
    names = [("1 Seen before", "batch key"), ("2 Parse", "bytes to rows"), ("3 Columns", "against contract"),
             ("4 Derive", "file name"), ("5 Types", "cast values")]
    names2 = [("6 Volume", "row counts"), ("7 Mass delete", "full copies"), ("8 Protect", "tokens"),
              ("9 Stamp, write", "bronze"), ("10 Log", "file log")]
    d = ""
    for i, (a, b) in enumerate(names):
        d += box(10 + i * 194, 30, 170, 64, a, b, "bx")
        if i:
            d += arrow(10 + i * 194 - 24, 62, 10 + i * 194 - 2, 62)
    for i, (a, b) in enumerate(names2):
        d += box(10 + i * 194, 150, 170, 64, a, b, "solid" if i == 3 else "bx")
        if i:
            d += arrow(10 + i * 194 - 24, 182, 10 + i * 194 - 2, 182)
    d += arrow(871, 94, 100, 148)
    d += text(490, 250, "A batch that fails a step from 1 to 7 skips straight to step 10 with the reason.", "tx", "middle")
    return d


@page("ingest-steps", "The entry steps every batch passes before bronze", parent="ingest", short="Entry steps",
      blurb="Ten steps in a fixed order: seen before, parse, columns, derive, types, volume, mass delete, protect, stamp and write, log.")
def ingest_steps():
    out = ("<p class='lead'>The entry steps are the gate in front of bronze. Every batch, from any of the five "
           "pipelines, is handed to one function called <code>enter</code>, which runs ten steps in a fixed "
           "order and records the outcome.</p>"
           "<p>The gate exists so that a broken delivery stops at the door. A file with a renamed column, a "
           "half-empty copy or a file sent twice should never reach the tables people query. And when a batch "
           "is turned away, the record must say which batch and why.</p>")
    out += fig(990, 270, steps_shape(), "The ten entry steps",
               "Read the top row, then the bottom row. The highlighted box is the only step that writes data.")
    out += "<h2>Goes in, what happens, comes out</h2>"
    out += flow3("One batch: a business date, a name, the bytes, and for pulled batches the rows already parsed. "
                 "Also the dataset's specification, built from the metadata store's view.",
                 "Ten steps. The first seven can each stop the batch. A stopped batch is not an error in the "
                 "code. It is an outcome with a status and a reason.",
                 "One outcome with one of five statuses. For a loaded batch, rows in bronze. In every case, one "
                 "row in the file log.")
    out += table(["Status", "Set by", "Meaning"], [
        ["<code>loaded</code>", "Step 9", "The batch is in bronze."],
        ["<code>skipped_duplicate</code>", "Step 1", "The same bytes were loaded before, or were loaded and later voided."],
        ["<code>rejected_contract</code>", "Steps 2, 3 and 5", "The batch cannot be read, lacks a contract column, has a forbidden new column, or holds a value of the wrong type."],
        ["<code>rejected_volume</code>", "Step 6", "The row count is not what was declared or is outside the expected band."],
        ["<code>held_mass_delete</code>", "Step 7", "A full copy is missing too many keys."],
    ])
    out += "<h2>The function that runs them</h2>"
    out += code(STEPS, r"^def enter\(", caption="All ten steps in order. Each early return is one way a batch is turned away.", cap=46)
    out += ("<p>The function never raises an error for a bad batch. Every exit goes through <code>_log</code>, "
            "so every batch leaves a record. The sections below take the steps one at a time.</p>")
    out += "<h2>The specification the steps work from</h2>"
    out += code(STEPS, r"    def from_view\(cls", caption="Everything the steps know about a dataset, taken from the store's view and never from a file.")
    out += ("<p>The settings come from the merged settings of the view. The <code>load</code> block and the "
            "volume band come from the manifest JSON that the view carries. The band's lower bound is 1 when "
            "the manifest gives none.</p>")
    out += "<h2>Step 1: has this batch been seen before</h2>"
    out += code(STEPS, r"^def content_key\(", caption="The batch key: the first 32 hex characters of a hash of the bytes.")
    out += ("<p>The key depends only on the content. The first lines of <code>enter</code> ask the metadata "
            "store for the latest status of that key. If it is <code>loaded</code> the batch is skipped as a "
            "duplicate. If it is <code>void</code>, meaning a person withdrew the batch, it is skipped too, so a "
            "bad file sent again does not come back. The check also looks at batches waiting in the current "
            "run, in case the same bytes arrive twice in one run. A reload sets <code>rewrite</code> and passes "
            "this step on purpose.</p>"
            "<p>For a pushed file the bytes are the file. For a pulled batch there is no file, so the pipeline "
            "passes the table name and the batch name as the bytes, for example the name of the copy or the "
            "page range. A pulled batch is therefore a duplicate when it has the same source position, not the "
            "same content.</p>")
    out += "<h2>Step 2: parse</h2>"
    out += code(STEPS, r"^def parse\(", caption="Bytes to rows, by the dataset's file format setting.")
    out += ("<p>Three families are read: JSON lines, which is the default, delimited text with a header row, "
            "and Parquet. A file that cannot be decoded or parsed makes the batch "
            "<code>rejected_contract</code> with the parser's message. Pulled batches arrive as rows and skip "
            "this step. The rows are copied first, because later steps change values in place and must not "
            "change what a source handed over.</p>")
    out += "<h2>Step 3: columns against the contract</h2>"
    out += code(STEPS, r"^def check_columns\(", caption="Missing columns stop the batch. New columns stop it or are kept, as the manifest says.")
    out += ("<p>The observed columns are the keys of the rows, leaving out names that start with an underscore, "
            "which are the platform's own. A contract column that is not observed stops the batch, and the "
            "reason names it. One exception is a column the lander fills itself. A column the contract does not "
            "have is new. If the manifest says <code>new_columns: stop</code> the batch stops. Otherwise the "
            f"new column is carried along and kept in bronze. {link('ingest-lander')} shows where it goes. "
            "Required columns are checked for presence only. A missing value in a required column is silver's "
            "to judge.</p>")
    out += "<h2>Step 4: derive</h2>"
    out += ("<p>One contract column can be filled by the lander: <code>file_name</code>. The dealer sell-out "
            "contract has it, because the file a row came from decides which version of a month is the latest. "
            "If the contract has that column, every row that lacks it gets the batch's name. This is the three "
            "lines in <code>enter</code> that start with the test for <code>file_name</code>.</p>")
    out += "<h2>Step 5: types</h2>"
    out += code(STEPS, r"^CASTS: dict", r"^\}", "One cast per contract type.")
    out += code(STEPS, r"^def cast_rows\(", caption="Every value is cast in place. The first value that does not cast stops the whole batch.")
    out += ("<p>An empty text is treated as missing for every type except string, because CSV has no way to "
            "write a missing value. A timestamp without a time zone is taken as UTC. A number with a fraction "
            "is not accepted as an integer. The reason names the column, the value and the row, so the person "
            "who gets the alert knows where to look. For change feeds and streams, the transport columns are "
            "cast as well.</p>")
    out += "<h2>Step 6: volume</h2>"
    out += code(STEPS, r"^def volume_problem\(", caption="Two checks: the count the delivery declared, and the band the manifest expects.")
    out += ("<p>A push delivery has a manifest that states how many rows it holds. If the batch has a different "
            "number, the file was cut short or grew on the way. The second check uses the manifest's "
            "<code>volume.expected_rows_per_delivery</code>. An empty batch is always held, even when the "
            "band's minimum is zero.</p>")
    out += "<h2>Step 7: the mass-delete guard</h2>"
    out += code(STEPS, r"^def mass_delete_problem\(", caption="A full copy that would delete more than the allowed share of keys is held.")
    out += ("<p>The guard applies only to full copies, only when the manifest sets "
            "<code>load.mass_delete_max_share</code>, and only when there is something to compare with. It "
            "builds the set of keys in the new copy, counts how many of the previous keys are gone, and holds "
            "the batch when that share is above the limit. Nothing is written. The previous keys are silver's "
            "current keys when silver exists, and otherwise the keys of the last accepted copy in bronze.</p>")
    out += "<h2>Step 8: protect personal data</h2>"
    out += code(STEPS, r"^def protect\(", caption="Values of restricted columns are replaced by tokens before anything is written.")
    out += ("<p>A token is a stand-in value that cannot be turned back into the original without a secret key. "
            "A column is restricted when the contract classes it so. The real value never reaches bronze. The "
            "same value always gives the same token, so joins and duplicate checks still work. The batch's "
            "profile, a summary of its rows and columns, is taken after this step so it never sees a real "
            "value either.</p>")
    out += code(A + "keyed_tokens.py", r"    def token\(self, column_class", caption="The tokenizer in use: a keyed hash, cut to 24 hex characters after a fixed prefix.")
    out += "<h2>Step 9: stamp and write</h2>"
    out += code(STEPS, r"^def stamp\(", caption="Each row gets the load columns. New columns are folded into one JSON text.")
    out += ("<p>The row is rebuilt from the list of bronze columns, so its shape is the same for every batch of "
            "the dataset. Inside a normal run the stamped rows are not written at once. They are put on the "
            "<code>pending</code> list and written with the run's other new batches of the dataset.</p>")
    out += code(STEPS, r"^def flush\(", caption="One bronze commit for the run's new batches, then the file log.")
    out += ("<p>The order inside <code>flush</code> is the point. The write comes first. Only after it returns "
            "are the batches recorded. If the write fails, nothing is recorded and the next run finds the same "
            "batches again. If the process dies between the write and the record, the next run writes the same "
            f"batch keys again, and the bronze writer replaces by batch key. {link('ingest-lander')} explains "
            "that.</p>")
    out += "<h2>Step 10: log</h2>"
    out += code(STEPS, r"^def _record\(", caption="One file log row per batch, whatever the outcome.")
    out += ("<p>The row holds the batch key, the name, the business date, the full checksum, the declared row "
            "count, the rows loaded, the status and the reason. The load type is <code>full</code> for a full "
            "copy and <code>incremental</code> for everything else.</p>")
    out += "<h2>A worked example: three inspection files</h2>"
    out += ("<p>The laboratory system sends CSV files of inspection results, separated by a character the "
            "manifest names. The manifest says <code>new_columns: stop</code>. Three files arrive. The file names are "
            "made up for this example. The first "
            "column is the file, the second what is in it, the third the outcome.</p>")
    out += table(["File", "Content", "Outcome"], [
        ["<code>inspection_P-CHN_1.csv</code>", "Two rows with every contract column", "<code>loaded</code>, 2 rows in bronze"],
        ["The same bytes again under the name <code>inspection_P-CHN_1_resend.csv</code>", "Identical", "<code>skipped_duplicate</code>, because the batch key is the same"],
        ["<code>inspection_P-HUN_1.csv</code>", "The column <code>defect_code</code> has been renamed", "<code>rejected_contract</code>. The reason names <code>defect_code</code> as missing and names the new column seen."],
    ])
    out += ("<p>The dataset run fails because one batch was held. The two good outcomes stay as they are. The "
            "14-day test plants this incident on day 7 and checks that the reason names the column and that the "
            "other plant's file keeps loading.</p>")
    out += "<h2>The settings the steps read</h2>"
    out += table(["Key", "Step", "Meaning", "Allowed values and default"], [
        ["<code>file_format_data</code>", "Parse", "The format of the delivered file.", "The code reads <code>json_lines</code>, <code>csv</code>, <code>tsv</code>, <code>pipe</code> and <code>parquet</code>. The dictionary has no default. The code falls back to JSON lines."],
        ["<code>file_encoding</code>", "Parse", "The text encoding.", "Default <code>utf-8</code>."],
        ["<code>csv_delimiter</code>", "Parse", "The separator for <code>csv</code>.", "Default a comma."],
        ["<code>csv_has_header</code>", "Parse", "Whether the first line names the columns.", "Default <code>true</code>. A file without a header is refused."],
        ["<code>load.new_columns</code>", "Columns", "What to do with a column the contract does not have.", "<code>hold_in_bronze</code> or <code>stop</code>. The code's default is <code>hold_in_bronze</code>."],
        ["<code>volume.expected_rows_per_delivery</code>", "Volume", "The smallest and largest row count expected.", "<code>min</code> defaults to 1. <code>max</code> may be absent."],
        ["<code>load.mass_delete_max_share</code>", "Mass delete", "The largest share of keys a full copy may lose.", "A fraction. No default, which switches the guard off. The two tyre master tables use 0.05."],
        ["The column's <code>classification</code>", "Protect", "Which columns are tokenised.", "<code>restricted</code> is the only class tokenised."],
    ])
    out += ("<p>The first four are settings in the dictionary. The three with a dot in front come from blocks of "
            "the dataset manifest, which the steps read from the manifest JSON in the view. The last comes from "
            "the contract.</p>")
    out += "<h2>How it is tested</h2>"
    out += (f"<p>{src(T_STEPS)} has one table of cases per step, with no database and no engine. Each row is one "
            "input and what the step must say.</p>")
    out += code(T_STEPS, r'        \("first copy", "full_copy"', 6, "The guard's cases: read type, previous keys, keys now, the limit, and whether the batch is held.")
    out += code(T_STEPS, r'        \("a good file loads"', 5, "The whole gate: a list of batches, the statuses expected, and the rows that must be in bronze.")
    out += code(T_PIPE, r"        \(\"silver's keys are the comparison", 7, "The guard through the runner: which keys it compares with in four situations.")
    out += not_built([
        "The dictionary allows more file formats than the parser reads. Fixed width, XML, spreadsheets and Avro "
        "are allowed values of <code>file_format_data</code>, but any value the parser does not know is read as "
        "JSON lines and the batch is then rejected as unreadable.",
        "The CSV keys for the quote character, the escape character and rows to skip are in the dictionary and "
        "are not read.",
        "The manifest field <code>load.batch_key</code> lists what the batch key should be made of, and it "
        "differs between the tyre datasets. The code does not read it. The key is always the hash of the bytes "
        "the pipeline passes.",
        "The manifest field <code>load.quarantine_stop_share</code> is not read by ingestion.",
        "The volume check compares one batch with a fixed band. It does not compare with the dataset's own "
        "history. The profile step in the runner looks at drift and only warns.",
        "A batch is all or nothing. One value of the wrong type stops every row of the file.",
        "Each batch is held in memory as a list of rows. There is no streaming of a large file.",
    ])
    return out


# ---------------------------------------------------------------------------------------------------------- lander
def lander_shape():
    d = box(10, 40, 200, 80, "Delivery in landing", ["data file, then", "its manifest"], "bx")
    d += box(10, 170, 200, 80, "Pulled rows", ["copy, page,", "stream chunk"], "bx")
    d += box(300, 100, 170, 90, "Entry steps", ["checks, then stamp"], "bx", href="ingest-steps.html")
    d += box(560, 100, 180, 90, "Bronze writer", ["replace by", "batch key"], "solid")
    d += box(830, 40, 150, 80, "Bronze table", ["one per dataset"], "bx")
    d += box(830, 170, 150, 80, "File log", ["one row per batch"], "bxs", href="meta-runtime.html")
    d += arrow(210, 80, 298, 130) + arrow(210, 210, 298, 160) + arrow(470, 145, 558, 145)
    d += arrow(740, 130, 828, 80, "1 write") + arrow(740, 160, 828, 210, "2 record")
    return d


@page("ingest-lander", "Landing to bronze: the lander, the bronze columns and the formats", parent="ingest",
      short="Landing to bronze",
      blurb="A delivery is written once. A bronze row carries where it came from. Writing the same batch twice changes nothing.")
def ingest_lander():
    out = ("<p class='lead'>This page is about the two ends of the lander: what a delivery in landing looks "
           "like, and what a row in bronze looks like once the lander has written it.</p>"
           "<p>Both ends are built so that a mistake can be undone. Landing keeps the bytes as they arrived, so "
           "a bad read can be done again. Bronze keeps the batch key on every row, so a batch can be found, "
           "replaced or removed as a unit.</p>")
    out += fig(990, 290, lander_shape(), "From a batch to bronze and the file log",
               "The highlighted box is the port that writes. The bronze write always comes before the file log record.")
    out += "<h2>Two landers in the code</h2>"
    out += (f"<p>The repository has two. {src(LANDER)} is the first one, written for the first slice of the "
            "platform with a single dataset. It still runs in that slice's test. The lander that loads the nine "
            f"tyre datasets is the function <code>enter</code> in {src(STEPS)}, described in "
            f"{link('ingest-steps')}. The two share ideas and differ in details, which the table lists.</p>")
    out += table(["", "First lander, <code>lander.py</code>", "Current lander, <code>ingest/steps.py</code>"], [
        ["Reads from", "Landing only", "Landing, pulled rows and stream chunks"],
        ["Batch key", "The business date and the first 16 characters of the checksum", "The first 32 characters of the checksum. The date is not part of it."],
        ["Columns stamped", "Four", "Six, plus transport columns for change feeds and streams"],
        ["Write", "Append", "Replace by batch key"],
        ["Checks", "Columns and volume", "Columns, types, volume, mass delete, tokens"],
    ])
    out += code(LANDER, r"^def land\(", r"        outcomes\.append\(outcome\)", "The first lander's loop, for comparison. The shape is the same: skip what was handled, check, stamp, write, record.", cap=32)
    out += "<h2>Landing: a delivery is written once</h2>"
    out += code(IPORTS, r"^class Landing\(Protocol\)", caption="The landing port. A mover writes through put. The lander reads through the other three.")
    out += code(A + "files.py", r"    def put\(self, source: str, dataset: str, window: str, name: str, data: bytes, rows: int,$",
                caption="Landing on a plain folder. The data file is written first and its manifest last.")
    out += ("<p>The layout is one folder per source, dataset and business date. A delivery is two files: the "
            "data exactly as delivered, and a small JSON manifest beside it with the row count and anything "
            "the mover wants to add. Two rules make landing safe. The manifest is written last, and a data file "
            "without a manifest is not a delivery, so a half-written file is never picked up. A second write of "
            "the same date and name is refused, so a replay always reads the bytes the first run read.</p>")
    out += "<h2>File formats</h2>"
    out += table(["Format", "Setting value", "How it is read"], [
        ["JSON lines", "<code>json_lines</code>, also the fallback", "One JSON object per line. Empty lines are skipped."],
        ["Comma separated", "<code>csv</code>", "A header row is required. The separator is <code>csv_delimiter</code>."],
        ["Tab or pipe separated", "<code>tsv</code>, <code>pipe</code>", "A header row is required."],
        ["Parquet", "<code>parquet</code>", "Read with a columnar library that must be installed. Types and missing values are kept."],
    ])
    out += ("<p>Text formats give every value as text. The type step of the lander casts each value to the "
            "contract's type afterwards, so bronze has typed columns whatever the format was.</p>")
    out += "<h2>The columns stamped on every bronze row</h2>"
    out += code(STEPS, r"^LOAD_COLUMNS: Columns", r"^RESTRICTED", "The load columns, the transport columns by read type, and the two small sets used by the derive and protect steps.")
    out += table(["Column", "Type", "What it holds"], [
        ["<code>_batch_key</code>", "string", "The key of the batch the row came in. Every row of a batch has the same one."],
        ["<code>_run_id</code>", "string", "The ingestion run that wrote the row."],
        ["<code>_source_file</code>", "string", "The batch's name: a file name, or a source position such as a copy time, a page range or a shard range."],
        ["<code>_window</code>", "string", "The business date the batch belongs to."],
        ["<code>_extra</code>", "string", "New columns the contract does not have, as one JSON text. Empty when there are none."],
        ["<code>_bronze_at</code>", "timestamp", "When the lander stamped the row."],
    ])
    out += table(["Transport column", "For", "What it holds"], [
        ["<code>_op</code>", "Change feeds", "The operation: I for insert, U for update, D for delete."],
        ["<code>_feed_pos</code>", "Change feeds", "The row's position in the feed."],
        ["<code>_committed_at</code>", "Change feeds", "When the source committed the change."],
        ["<code>_shard</code>", "Streams", "The shard the record was read from."],
        ["<code>_seq</code>", "Streams", "The record's sequence number in its shard."],
        ["<code>_arrived_at</code>", "Streams", "When the record arrived at the stream."],
    ])
    out += code(STEPS, r"    def bronze_columns\(self\)", caption="A bronze table's columns: the contract's, then the transport columns for its read type, then the load columns.")
    out += code(SPLAN, r"^# Columns the lander stamps on every bronze row", 3,
                "Silver's side of the same agreement. The two lists of names are written again here so silver knows which bronze columns are not business columns.")
    out += ("<p>The names are listed in two places, once in the ingestion steps and once in the silver plan. "
            "They agree today. Nothing in the code derives one from the other.</p>")
    out += "<h2>New columns are held in _extra</h2>"
    out += ("<p>When a source starts sending a column the contract does not have, and the manifest allows it, "
            "the batch still loads. The contract columns go to their own bronze columns. The new ones are "
            "written together as one JSON text in <code>_extra</code>. Nothing is lost and the bronze table's "
            "shape does not change. The outcome carries a note naming the new columns, which reaches the file "
            "log as the reason. Promoting such a column into the contract is a change to the contract file.</p>")
    out += "<h2>The bronze writer and why a rerun changes nothing</h2>"
    out += code(IPORTS, r"^class BronzeWriter\(Protocol\)", caption="The port. The first two methods write. The last two answer questions for reconciliation and the mass-delete guard.")
    out += ("<p>A write that can be repeated safely is called idempotent. The port states it as a rule: a write "
            "replaces whatever the table holds for its batch key. Since the batch key is a hash of the content, "
            "the same content written twice replaces itself with itself.</p>")
    out += code(A + "delta_bronze.py", r"    def write\(self, table: str, batch_key: str, rows: list\[Row\]", caption="Bronze on Delta tables in a folder, without a cluster. One commit that overwrites only the rows of this batch key.")
    out += code(A + "delta_bronze.py", r"    def write_many\(self, table: str, batches", caption="Several new batches in one commit. The overwrite names exactly these batch keys.")
    out += code(IPORTS, r"^SAFE_BATCH_KEY = ", 7, "A batch key ends up inside a filter text, so every writer checks its shape first.")
    out += table(["Adapter", "File", "Where it writes", "Used by"], [
        ["<code>MemoryBronze</code>", src(A + "memory_bronze.py"), "A list in memory", "Tests. It is the reference behaviour."],
        ["<code>DeltaBronze</code>", src(A + "delta_bronze.py"), "Delta tables in a local folder, through a library that needs no cluster", "The local platform script and the 14-day test."],
        ["<code>StoreBronzeWriter</code>", src(A + "table_store.py"), "Delta or Iceberg tables through the engine's table port", "The engine binding."],
        ["<code>SparkBronzeWriter</code>", src(A + "spark_delta.py"), "Delta tables in a catalogue through Spark", "The port's contract test, when Spark is switched on for tests. I found no binding that uses it."],
    ])
    out += code(A + "table_store.py", r"    def write\(self, table: str, batch_key: str, rows: list\[dict", r"        return len\(rows\)",
                "The engine's writer. A new batch is appended with a write mark. Only a reload takes the replace path.")
    out += ("<p>The comment in this method records a measured lesson: replacing on every write scanned the "
            "whole bronze table per batch and took most of a run's time. So on one table format a new batch is "
            "appended, and the format's own write mark, made of an application name and the batch key, stops "
            "the same batch from being appended twice.</p>")
    out += "<h2>The file log entry</h2>"
    out += code(STEPS, r"^def _record\(", caption="The record of a batch. It is written after the bronze commit.")
    out += ("<p>The file log is what the rest of the platform trusts. Silver decides what to read from it. A "
            f"reload finds a window's batches in it. {link('silver-reader')} and {link('meta-runtime')} show "
            "both.</p>")
    out += "<h2>A worked example: one bronze row</h2>"
    out += ("<p>A change file for the tyre company's sales order lines arrives on 4 November 2026. The source "
            "has started to send a column named <code>sales_channel</code> that the contract does not have. "
            "The table shows one row as bronze holds it, with made-up values. The left column is the bronze "
            "column, the right column its value.</p>")
    out += table(["Bronze column", "Value"], [
        ["<code>order_id</code>, <code>line_no</code>", "<code>SO-1001</code>, <code>10</code>"],
        ["<code>promised_date</code>, <code>ordered_qty</code>", "<code>2026-11-12</code>, <code>40</code>"],
        ["<code>change_seq</code>", "<code>4321</code>"],
        ["<code>_op</code>, <code>_feed_pos</code>", "<code>U</code>, <code>251</code>"],
        ["<code>_batch_key</code>", "32 hex characters, the same on every row of the file"],
        ["<code>_source_file</code>", "The delivery's file name"],
        ["<code>_window</code>", "<code>2026-11-04</code>"],
        ["<code>_extra</code>", "<code>{\"sales_channel\": \"web\"}</code>"],
    ])
    out += "<h2>The settings this part reads</h2>"
    out += table(["Key", "Meaning", "Default"], [
        ["<code>file_format_data</code>, <code>file_encoding</code>, <code>csv_delimiter</code>, <code>csv_has_header</code>", "How a delivered file is parsed.", "JSON lines, UTF-8, a comma, true"],
        ["<code>load.new_columns</code>", "Whether a new column is held in <code>_extra</code> or stops the batch.", "<code>hold_in_bronze</code>"],
        ["<code>ingest.read</code>", "Which transport columns the bronze table has.", "None for files, copies and pages"],
        ["<code>table_format</code>", "A proposed dataset setting for the table format. The engine binding chooses the format for the writer.", "No default in the dictionary"],
    ])
    out += "<h2>How it is tested</h2>"
    out += (f"<p>{src(T_PORTS)} is the contract test for the ports. Every bronze writer must pass the same "
            "table of cases, and every landing adapter must pass its own table. The cloud object storage "
            "landing runs against a local stand-in for the cloud service when that library is installed.</p>")
    out += code(T_PORTS, r"^BRONZE_CASES = \[", 9, "The first cases every bronze writer must pass, among them the same batch written twice.")
    out += code(T_STEPS, r"^def test_enter_stamps_and_protects", caption="The stamp: after one file, every row has the run id, the file name, the batch key and the business date, and the restricted column holds a token.")
    out += (f"<p>{src(T_FORMATS)} covers the delivered formats and the change feed column names.</p>")
    out += not_built([
        "<code>SparkBronzeWriter.write_many</code> appends without a replace and without a write mark. If a "
        "process died after that commit and before the file log was written, the next run would append the "
        "same batches again. The other three writers guard against this. I found this writer used only by the "
        "contract test.",
        "The folder landing adapter reads every delivery's bytes to compute its checksum each time it lists "
        "the deliveries of a dataset. The cost grows with the number of files kept in landing.",
        "The load column names are written in two modules that must be kept in step by hand.",
        "A value in <code>_extra</code> is text inside JSON. It is not typed and silver does not read it.",
        "Compressed and archive files are recognised by the discovery helper and cannot be parsed by the "
        "lander.",
        "The first lander is still in the repository beside the current one. It is not on the path of the nine "
        "tyre datasets.",
    ])
    return out


# --------------------------------------------------------------------------------------------------------- sources
def sources_shape():
    d = box(10, 20, 200, 64, "SourceDatabase", "feed and full copy", "bx")
    d += box(10, 100, 200, 64, "FileDrop", "files as dropped", "bx")
    d += box(10, 180, 200, 64, "RecordInterface", "pages since a cursor", "bx")
    d += box(10, 260, 200, 64, "EventStream", "shards and sequence", "bx")
    d += box(320, 20, 180, 64, "Change feed mover", "feed to landing", "bxm")
    d += box(320, 100, 180, 64, "Drop mover", "drop to landing", "bxm")
    d += box(600, 60, 150, 64, "Landing", "", "bx", href="ingest-lander.html")
    d += box(830, 140, 150, 90, "Pipelines", ["the five readers"], "solid", href="ingest-pipelines.html")
    d += arrow(210, 52, 318, 52) + arrow(210, 132, 318, 132)
    d += arrow(500, 52, 598, 85) + arrow(500, 132, 598, 100) + arrow(750, 92, 828, 160, "push")
    d += arrow(210, 212, 828, 190, "pull pages")
    d += arrow(210, 292, 828, 215, "stream")
    d += text(420, 205, "a full copy is pulled from the database", "sm", "middle")
    return d


@page("ingest-sources", "Source ports and movers: database, change feed, files, interface, stream", parent="ingest",
      short="Sources and movers",
      blurb="Four source ports, two movers, and the adapters behind them. Which are real connectors and which are local stand-ins.")
def ingest_sources():
    out = ("<p class='lead'>Ingestion never names a product. It talks to four small interfaces, called ports, "
           "that describe a source by what it is: a database, a file drop, a record interface, an event "
           "stream. An adapter is the code that puts a real system, or a stand-in for one, behind a port.</p>"
           "<p>This page lists every port and every adapter, and says plainly what each adapter is. That "
           "matters here, because most of them were built to let a simulated tyre company run on a laptop, and "
           "a reader should not mistake them for connectors to production systems.</p>")
    out += fig(990, 340, sources_shape(), "The four source ports, the two movers and where each leads",
               "The four boxes on the left are ports. The dashed boxes are movers, which only copy. Push goes "
               "through landing. Pull and stream are read directly.")
    out += "<h2>The honest summary</h2>"
    out += table(["Kind of source", "Adapters", "What it really is"], [
        ["Database with a change feed and full copies", "<code>MemorySourceDb</code>, <code>SqliteSource</code>, <code>PostgresSource</code>", "A stand-in. All three read tables that the simulated company itself writes in a layout of our own: the live table, a table of changes and a table of copies. None connects to a real ERP or reads a database's own change log."],
        ["Change feed mover", "<code>ChangeFeedMover</code>, <code>DmsMover</code>", "A stand-in for a change capture service. The second one imitates the file layout such a managed service writes, with Parquet files and its column names. No managed service is called."],
        ["File drop", "<code>MemoryFileDrop</code>, <code>LocalFileDrop</code>, <code>S3FileDrop</code>", "The first two are local. The third makes real calls to a cloud object storage service through the vendor's library."],
        ["Drop mover", "<code>DropMover</code>", "Real but simple code: it copies new files from a drop to landing. It stands in for a file transfer service."],
        ["Record interface", "<code>MemoryRecords</code>, <code>LocalRecordStore</code> with <code>serve</code>, <code>HttpRecordClient</code>", "A stand-in. A small HTTP server on the same machine serves the simulated dealer system. The client refuses any address that is not the local machine."],
        ["Event stream", "<code>MemoryStream</code>, <code>LocalStream</code>, <code>KinesisStream</code>", "The first two are local. The third makes real calls to a managed streaming service through the vendor's library."],
        ["Landing", "<code>FileLanding</code>, <code>S3Landing</code>", "A folder, which is also how a storage volume of the engine is reached, and cloud object storage."],
    ])
    out += ("<p>For the two cloud adapters that make real calls, the tests and the recorded run in the cloud "
            "realm's README use a local stand-in for the cloud service. I found no record of a run against the "
            "real services and could not verify one.</p>")
    out += "<h2>Why the ports have write methods</h2>"
    out += code(SPORTS, r'^"""The four mock-source ports', r'^"""$', "The module's own opening. It calls them mock-source ports.", cap=10)
    out += ("<p>A connector to a real system would only read. These ports also have methods that write: "
            "<code>apply</code>, <code>publish_copy</code>, <code>put</code>, <code>upsert</code> and "
            "<code>append</code>. They are there because the simulated company uses the same ports to act as "
            "the source systems. Ingestion calls only the reading methods.</p>")
    out += "<h2>Port 1: the source database</h2>"
    out += code(SPORTS, r"^class SourceDatabase\(Protocol\)", caption="A database with a change feed and a nightly full copy.")
    out += table(["Method", "Used by", "What it returns"], [
        ["<code>feed</code>", "The change feed mover", "The change rows after a feed position, in commit order, each with its position, operation and commit time."],
        ["<code>copies</code>, <code>copy</code>", "The full copy pipeline", "The extraction times of the copies published so far, and the rows of one copy."],
        ["<code>rows</code>", "The platform script, when it compares the live source with silver", "The live table as it stands."],
        ["<code>ensure</code>, <code>apply</code>, <code>publish_copy</code>", "The simulated company", "Nothing. They create tables and write changes and copies."],
    ])
    out += ("<p>The docstring of <code>feed</code> states a rule worth keeping: a reader keeps a feed position, "
            "not a change sequence, because an old change can be committed late.</p>")
    out += code(A + "sql_source.py", r"    def feed\(self, system: str, table: TableSpec, after_pos", caption="The feed on a SQL database: a plain query on a changes table that the simulated company fills.")
    out += ("<p>This is the clearest sign of what the adapter is. The feed is a table named after the source "
            "table with <code>__changes</code> added, and the full copies are a table with <code>__copy</code> "
            "added. A real database does not have these. A real connector would read the database's own change "
            "log, or a managed change capture service would, and write files into landing.</p>")
    out += "<h2>The movers: from a source to landing</h2>"
    out += code(A + "local_movers.py", r"^class ChangeFeedMover:", caption="The change feed mover. Its checkpoint is the highest feed position among the deliveries it has already landed.", cap=30)
    out += ("<p>A mover copies and never changes data. This one asks the database for feed rows after its "
            "checkpoint and lands them as one JSON lines delivery, named with the first and last feed position. "
            "It writes no empty file. If landing refuses the write because the delivery exists, the mover "
            "treats it as already done.</p>")
    out += code(A + "local_movers.py", r"^class DropMover:", caption="The drop mover: every file not yet in landing is copied byte for byte, under the business date of its arrival.")
    out += code(A + "local_movers.py", r"^DMS_SETTINGS = ", 2, "The settings a dataset needs when its deliveries have the managed service's shape.")
    out += "<h2>Port 2: the file drop</h2>"
    out += code(SPORTS, r"^class FileDrop\(Protocol\)", caption="A folder that partners and the laboratory drop files into.")
    out += ("<p>The local adapter keeps a file's arrival time as its modified time. The object storage adapter "
            "keeps it as a piece of metadata on the object, because the storage service stamps its own upload "
            "time. Both refuse a name that contains a path.</p>")
    out += "<h2>Port 3: the record interface</h2>"
    out += code(SPORTS, r"^class RecordInterface\(Protocol\)", caption="An interface that returns records changed after a cursor, in pages.")
    out += code(A + "http_api.py", r"^class HttpRecordClient:", caption="The HTTP client. Read the check on the address in the page method.", cap=30)
    out += ("<p>The cursor is the record's update time and its key joined by a bar. Records come back ordered "
            "by that pair, so a page never skips or repeats a record even when many share a time. The client "
            "has no authentication, no rate limit and no way to describe another interface's paths or paging "
            "style. It is the simulated dealer system's client and nothing more.</p>")
    out += "<h2>Port 4: the event stream</h2>"
    out += code(SPORTS, r"^class EventStream\(Protocol\)", caption="An append-only stream split into shards. The reader keeps its own position per shard.")
    out += code(A + "kinesis.py", r"    def read\(self, stream: str, shard: str, after_seq", caption="Reading a shard of the managed stream from after a sequence number.")
    out += ("<p>The local stream is a set of compressed files, one per shard. A record's shard is chosen by a "
            "hash of its key, the way managed streams spread records. The managed adapter lets the service "
            "assign shard names and sequence numbers.</p>")
    out += "<h2>The guard around source calls</h2>"
    out += code(RESIL, r"    def call\(self, source: str, fn", caption="Three protections in one call: a circuit breaker per source, an optional cap on parallel calls, and retries with growing waits.")
    out += ("<p>A circuit breaker stops calling a source that has failed several times in a row, and tries "
            "again after a pause. Only errors an adapter declares as temporary are retried. The pull pipelines "
            "send every source call through this guard when the binding supplies one.</p>")
    out += "<h2>Discovery: looking at what arrives before onboarding a feed</h2>"
    out += code(DISC, r"^def sniff\(", caption="The format of a file from its first bytes, not from its name.")
    out += code(DISC, r"^def discover\(", caption="Formats, files and rows per day, and the days with no file.")
    out += ("<p>Discovery is read-only and uses only the standard library. It answers the questions asked "
            "before a feed is set up: which formats come, how much per day, and which days are missing.</p>")
    out += "<h2>A worked example: one order line through the stand-in database</h2>"
    out += ("<p>The simulated company changes one sales order line three times. The table shows what the "
            "source database stand-in holds afterwards. The first column is the change, the second the live "
            "table, the third the feed.</p>")
    out += table(["Change applied", "Live table", "Feed rows"], [
        ["Insert line 10 with change sequence 1", "Line 10, sequence 1", "Position 1: I, sequence 1"],
        ["Update the promised date with sequence 3", "Line 10, sequence 3", "Position 2: U, sequence 3"],
        ["An older update with sequence 2 is committed late", "Line 10, still sequence 3. The older change does not overwrite.", "Position 3: U, sequence 2"],
    ])
    out += ("<p>The feed keeps the late change at position 3, after the newer one. That is why the mover and "
            "the pipeline keep the feed position as their checkpoint, and why silver orders history by the "
            "change sequence and not by arrival.</p>")
    out += "<h2>The settings this part reads</h2>"
    out += table(["Name", "Meaning", "Default"], [
        ["<code>WORLD_PG_DSN</code>", "Environment variable with the connection string of the stand-in source database on PostgreSQL.", "<code>dbname=tyre_world</code>"],
        ["<code>systems</code> in <code>Sources</code>", "A map from a manifest's source name to the system that holds it, passed by the binding.", "None"],
        ["<code>business_tz</code> in <code>Sources</code>", "The business time zone, used to turn an arrival time into a business date.", "None. The binding passes it."],
        ["<code>retry_count</code>, <code>retry_interval_seconds</code>", "The retry policy for pulls.", "2 and 60"],
        ["Shard count of the local stream", "How many shards the local stream has.", "4"],
        ["Page size limit of the local interface", "The most records one page returns.", "200"],
    ])
    out += ("<p>The dictionary registers 43 source-scope settings, such as a server name or an authentication "
            f"method. None of the adapters here reads them, and the store has no place to keep them. "
            f"{link('meta-settings')} explains that gap.</p>")
    out += "<h2>How it is tested</h2>"
    out += (f"<p>{src(T_SRC)} is the contract test for the four source ports. Every adapter of a port must pass "
            "the same cases. The PostgreSQL adapter runs when a local server answers. The two cloud adapters "
            "run against a local stand-in for the cloud when its library is installed. Otherwise they are "
            "skipped.</p>")
    out += code(T_SRC, r"^DB_CASES = \{", r"^\}", "The source database cases: changes applied, the live rows expected, and the feed expected.", cap=24)
    out += code(T_DISC, r"^def test_discover_finds_formats", caption="Discovery on four files: two formats, three days with files, two missing days.")
    out += not_built([
        "There is no connector that reads a real database's change log. Change capture is expected to come "
        "from a managed service that writes into landing. Only its file shape is imitated.",
        "There is no general HTTP connector. The client reads only the local stand-in and has no "
        "authentication, rate limit or configurable paging.",
        "There is no connector for a message queue product other than the one managed stream, and none for "
        "file transfer protocols or other databases.",
        "The two cloud adapters are tested against a local stand-in for the cloud service. I could not verify "
        "a run against the real services.",
        "The source database port mixes reading and writing because the simulated company shares it. A "
        "production binding would need a read-only adapter.",
        "Discovery is not wired into any runner or command. It is a function with tests.",
        "The object storage landing adapter downloads every delivery to compute its checksum when it lists "
        "deliveries, like the folder adapter.",
    ])
    return out
