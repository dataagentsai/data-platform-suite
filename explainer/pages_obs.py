"""Operations: the run log, freshness, alerts, lineage, the operations page, reconciliation, recovery, resilience."""
from lib import *  # noqa: F401,F403

OBS = "P/src/dataplatform/obs/"
RUNLOG = OBS + "runlog.py"
RECORDER = OBS + "recorder.py"
STEPS = OBS + "steps.py"
TRACED = OBS + "traced_store.py"
TRACE = OBS + "trace.py"
FRESH = OBS + "freshness.py"
SUCCESS = OBS + "success.py"
ALERTS = OBS + "alerts.py"
RULES = OBS + "rules/alerts.yaml"
MONITOR = OBS + "monitor.py"
LINEAGE = OBS + "lineage.py"
PAGE = OBS + "page.py"
OPS = OBS + "ops.py"
EXPORT = OBS + "ops_export.py"
VIEWS = OBS + "views.py"
DASH = OBS + "dashboards.py"
RECON = "P/src/dataplatform/reconcile.py"
LIVE = "P/src/dataplatform/silver/live_recon.py"
HOPS = "P/src/dataplatform/hop_reports.py"
PROFILING = "P/src/dataplatform/profiling.py"
RECOVERY = "P/src/dataplatform/recovery.py"
RETENTION = "P/src/dataplatform/retention.py"
HOUSE = "P/src/dataplatform/housekeeping.py"
RESIL = "P/src/dataplatform/resilience.py"
RUN = "P/scripts/run_platform.py"
T = "P/tests/dataplatform/"
BOARDS = "P/realms/databricks/dashboards/"


# ------------------------------------------------------------------------------------------------------ the hub
def hub_picture():
    d = box(10, 130, 150, 90, "Every run", ["ingestion, silver,", "gold, housekeeping"], "bx")
    d += box(230, 130, 190, 90, "Run log", ["in the metadata", "store"], "solid", href="obs-runlog.html")
    d += box(500, 20, 200, 70, "Freshness", "how late is too late", "bx", href="obs-freshness.html")
    d += box(500, 110, 200, 70, "Alerts", "who is told, and when", "bx", href="obs-alerts.html")
    d += box(500, 200, 200, 70, "Lineage", "source to bronze", "bx", href="obs-lineage.html")
    d += box(500, 290, 200, 70, "Operations tables", "fifteen small tables", "bx", href="obs-page.html")
    d += box(780, 250, 200, 70, "Operations page", "one file per run", "bxs", href="obs-page.html")
    d += box(780, 340, 200, 70, "Dashboards", "published on Databricks", "bxm", href="obs-page.html")
    d += arrow(160, 175, 228, 175, "writes")
    d += arrow(420, 150, 498, 60) + arrow(420, 165, 498, 145) + arrow(420, 185, 498, 235) + arrow(420, 200, 498, 320)
    d += arrow(700, 315, 778, 290) + arrow(700, 335, 778, 370)
    d += text(380, 400, "Everything on the right is read from the run log. Nothing on the right writes data.", "sm", "middle")
    return d


@page("obs", "Operations: how the platform is watched", parent="index", short="Operations",
      blurb="Every run writes to one run log. Freshness, alerts, lineage, the operations page and the dashboards "
            "are all read from it.")
def obs():
    out = ("<p class='lead'>Operations answers four questions about the platform every day. Did the loads run? "
           "Is the data as fresh as promised? Was anything held back or wrong, and who was told? Where did the "
           "time go?</p>"
           "<p>The design has one rule. Everything a run does is written to the <b>run log</b>, a set of tables in "
           "the metadata store. Every screen, every alert and every freshness number is then worked out from the "
           "run log by pure functions. A pure function takes rows in and gives rows back, and touches nothing "
           "else. So there is one record of what happened, and no screen can disagree with another.</p>")
    out += fig(990, 420, hub_picture(), "The run log in the middle, with everything that reads it",
               "Start at the run log in the middle. Runs write to it on the left. Everything on the right only reads. "
               "Click a box to zoom in.")
    out += "<h2>The pages under this one</h2>"
    out += table(["Page", "What it explains"], [
        [link("obs-runlog"), "What is recorded for a run, for each dataset in the run and for each step, how the "
                             "records are read back, and how tracing is wrapped around the store."],
        [link("obs-freshness"), "How the platform decides that a dataset, or one dealer's file, is late. Also the "
                                "twenty success criteria a run is scored against."],
        [link("obs-alerts"), "Which conditions raise an alert, how the severity is chosen, who is told, and the "
                             "four rules that keep alerts few enough to be read."],
        [link("obs-lineage"), "What lineage the platform records itself, and at what grain."],
        [link("obs-page"), "The operations tables, the page each run writes, and the metric views and dashboards "
                           "published on Databricks."],
        [link("obs-reconcile"), "Reconciliation between layers, the live source compared with silver, the "
                                "report each hop hands over, and profiling."],
        [link("obs-recovery"), "Withdrawing a bad batch and rebuilding by rerun, how long each copy is kept, and "
                               "the housekeeping job that removes old bronze."],
        [link("obs-resilience"), "Retry, the circuit breaker and the connection limit that protect calls to a "
                                 "source."],
    ])
    out += "<h2>The design decisions that shape operations</h2>"
    out += table(["Decision", "What it means", "Why"], [
        ["One run log, read through a port",
         "Runs write through the metadata store. Operations reads through a small read-only interface that "
         "returns every run in a time window.",
         "A port is an interface with more than one implementation behind it. The same screens work on the "
         "in-memory store in tests and on PostgreSQL in a deployment."],
        ["Push, not poll",
         "When a run finishes, the store tells the monitor. The monitor evaluates that run's datasets at once. "
         "No job wakes on a fixed interval to look for trouble.",
         "An alert goes out when the run ends, not minutes later, and nothing runs when nothing happened."],
        ["One format for every check result",
         "A detector, a quality rule, a reconciliation and a profile all write the same kind of step row: a "
         "name, a status, counts and a message.",
         "Operations can alert on any check and show it on a screen without knowing what the check does."],
        ["Operations never judges the data",
         "The part that owns a check decides whether it passed. Operations reads the status and reports it.",
         "One place decides. An alert and its owner can never disagree."],
        ["Adopt what the platform already shows",
         "Job runs, job failure messages, lineage from bronze onward and table usage are the platform's own "
         "screens. Operations builds only what the platform cannot know.",
         f"Less to build and keep correct. The adopted list is in {src(OBS + 'native.py')}."],
        ["Cost is not here",
         "No billing data is read. Every job carries tags with the domain, the data product and the run id.",
         "Spend belongs to a separate cost tool. The tags let that tool attribute spend to a run."],
    ])
    out += "<h2>How it is wired in a run</h2>"
    out += ("<p>The local run script builds the wiring in four lines. It wraps the metadata store twice. The "
            "inner wrapper stamps a time on every run log write. The outer wrapper opens a trace span for each "
            "run, dataset and step, and calls the monitor when a run finishes. The ingestion, silver and gold "
            "runners receive the wrapped store and do not know it is wrapped.</p>")
    out += code(RUN, r"    stamped = StampingRunLog\(raw_store, clock\)", r"    store = TracedStore\(",
                "The wiring: a stamping wrapper, then a tracing wrapper that pushes each finished run to the monitor.")
    out += not_built([
        "The alert state and the alert history are kept in memory on the monitor. A deployment needs a table for "
        "them, and none exists yet.",
        "Alerts are delivered to an in-memory notifier in every run this page describes. The adapter for a "
        "workspace's notification destinations exists and is tested with an injected transport, but sending "
        "through a real workspace has not been done.",
        "The data expectation services, which are meant to own quality rules and reconciliation results with "
        "their own tables and screen, are not built. Their screen exists and reads the shared step rows.",
        "The heartbeat rule is in the alert configuration, but no code produces a heartbeat signal.",
        "The run recorder that makes the run id equal to the trace id is used by the fixture and by tests. The "
        "ingestion, silver and gold runners record through the store directly with run ids such as "
        "<code>ingest-2026-11-09</code>.",
    ])
    return out


# ------------------------------------------------------------------------------------------------------ run log
def runlog_picture():
    d = box(20, 20, 300, 60, "run", "one per run of a domain", "bx")
    d += box(80, 110, 300, 60, "dataset run", "one per dataset in the run", "bx")
    d += box(140, 200, 240, 60, "step", "one per stage or check", "bxs")
    d += box(400, 200, 240, 60, "file", "one per batch handled", "bxs")
    d += box(660, 200, 240, 60, "error", "one per error caught", "bxs")
    d += arrow(110, 80, 110, 108) + arrow(200, 170, 240, 198) + arrow(330, 170, 480, 198) + arrow(380, 150, 740, 198)
    d += text(660, 55, "Every record carries the run id.", "tx", "middle")
    d += text(660, 80, "Join any two tables on it.", "tx", "middle")
    return d


@page("obs-runlog", "The run log: what every run writes down", parent="obs", short="Run log",
      blurb="One record per run, per dataset in the run and per step, plus files and errors, all keyed by the run id.")
def obs_runlog():
    out = ("<p class='lead'>The run log is the platform's diary. Every run of every layer writes what it did: "
           "which datasets it touched, which steps ran, how many rows, which files were loaded or held, and "
           "which errors it met.</p>"
           "<p>Without it, a question such as why yesterday's order lines are missing can only be answered by "
           "reading job output. With it, the answer is one query, and the same records feed freshness, alerts "
           "and every screen.</p>")
    out += fig(930, 280, runlog_picture(), "The five kinds of record in the run log",
               "A run has dataset runs. A dataset run has steps, files and errors. All five share the run id.")
    out += "<h2>What is recorded, level by level</h2>"
    out += ("<p>The table lists the five record kinds. The first column is the record. The second is what one "
            "row of it describes. The third lists its fields as the read side returns them.</p>")
    out += table(["Record", "One row is", "Fields"], [
        ["Run", "One run of one domain in one environment",
         "run id, domain, environment, trigger, run type, status, start time, end time"],
        ["Dataset run", "One dataset handled inside a run",
         "run id, source, dataset, status, configuration fingerprint, watermark before and after, counts, "
         "message, start time, end time"],
        ["Step", "One stage of the pipeline, or one check, for one dataset",
         "run id, source, dataset, step name, status, counts, message, time"],
        ["File", "One batch that ingestion handled",
         "run id, source, dataset, batch key, file name, window, rows, status, reason, time"],
        ["Error", "One error that was caught",
         "run id, source and dataset when known, step, error class, message, time"],
    ])
    out += ("<p>Three terms in that table need a definition. A <b>configuration fingerprint</b> is a hash of "
            "the dataset's settings at the time of the run, so a later reader can tell which settings a run "
            "used. A <b>watermark</b> is the highest change position or time already taken from the source. "
            "A <b>batch key</b> is the identity of one delivery, used to recognise the same delivery sent twice.</p>")
    out += code(RUNLOG, r"^class DatasetRunRow:", caption="The dataset run as operations reads it back. It is frozen: a reader cannot change it.")
    out += "<h2>Reading the log back</h2>"
    out += ("<p>Writing happens through the metadata store's own functions, which the "
            f"{link('meta-runtime', 'runtime tables page')} explains. Reading for operations happens through a "
            "separate, read-only port with a single method. It returns every run of a domain that started in a "
            "time window, with everything recorded for those runs.</p>")
    out += code(RUNLOG, r"^class RunLogReader\(Protocol\):", caption="The whole read side: one method.")
    out += table(["Adapter", "File", "How it answers"], [
        ["In memory", src("P/src/dataplatform/adapters/memory_runlog.py"),
         "Wraps any metadata store, passes every call through, and stamps each run log write with the time "
         "from the clock port. A simulated week therefore runs on simulated time."],
        ["PostgreSQL", src("P/src/dataplatform/adapters/postgres_runlog.py"),
         "One read-only database function over the store's run tables, using the store's own timestamps."],
        ["No log", src("P/src/dataplatform/adapters/null_runlog.py"),
         "Returns nothing. It proves that every screen still renders when a customer has no run log connected."],
        ["A customer's own log", src("P/src/dataplatform/adapters/customer_runlog.py"),
         "An example skeleton only. Its row in the contract test is skipped."],
    ])
    out += "<h2>A worked example</h2>"
    out += ("<p>One ingestion run loads the tyre company's sales order lines. One delivery arrives twice. A "
            "delete is seen in the change feed. The run log then holds these rows. The values are an example "
            "typed for this page, in the shape the code writes.</p>")
    out += raw("""run          run_id=ingest-2026-11-12  domain=tyre  env=dev  trigger=schedule  status=succeeded

dataset run  erp.sales_order_line  status=succeeded  counts={loaded: 1, skipped_duplicate: 1, rows_loaded: 412}

step         profile.batches           succeeded  {rows: 412, promised_date.nulls: 0}
step         detect.DUP-01             warned     {batches: 1}   "sol_2026-11-12_resent.jsonl: already loaded"
step         detect.DEL-01             warned     {rows: 5}      "5 noticed"
step         ingest.push.change_feed   warned     {loaded: 1, skipped_duplicate: 1, rows_loaded: 412, noticed.DEL-01: 5}

file         sol_2026-11-12.jsonl          loaded             rows=412
file         sol_2026-11-12_resent.jsonl   skipped_duplicate  rows=0    reason="already loaded\"""",
               "One run, one dataset run, four steps and two files. Every row carries the run id.")
    out += "<h2>The one step format</h2>"
    out += ("<p>A step is either a pipeline step, such as one ingestion read or one silver stage, or a <b>check "
            "result</b>. Check results follow one format so that operations can treat them all alike. The name "
            "starts with one of four words and a dot.</p>")
    out += table(["Name starts with", "Written by", "Meaning", "Screen with the detail"], [
        ["<code>detect.</code>", "The engine: ingestion and the gold harness today",
         "A detector met something in a load, for example <code>detect.DEL-01</code> for deletes at the source", "Sources and ingestion"],
        ["<code>quality.</code>", "Data expectation services", "The verdict of one quality rule", "Data expectations"],
        ["<code>recon.</code>", "Data expectation services, and the live source check after silver",
         "One reconciliation entry", "Data expectations"],
        ["<code>profile.</code>", "Ingestion's profiling step", "One profile and whether it drifted", "Data expectations"],
    ])
    out += ("<p>The status of a check row is the severity operations reads. <code>succeeded</code> reads as "
            "passed. <code>warned</code> means the condition was handled, or flagged for a later layer. "
            "<code>failed</code> reads as blocked: the load was stopped.</p>")
    out += code(STEPS, r"^DETECT = \"detect\"", r"^_NAME = ", "The four kinds, who owns each, which screen holds the detail, and how a status is read.")
    out += code(STEPS, r"^def violations\(", caption="The contract every check row must keep. It returns what is wrong, or an empty list.")
    out += ("<p>Walk through it. The name must be one of the four kinds, a dot, and an identifier. The status "
            "must be one of the three known words. Every count must be a whole number under a text name, and a "
            "true or false value is refused even though Python treats it as a number. The message is text of "
            "at most 4000 characters. The last rule is the one people forget: a blocked step must say why.</p>")
    out += "<h2>The recorder: one id from start to finish</h2>"
    out += ("<p>The recorder is a helper around the store for code that wants the run, the dataset and the step "
            "as nested blocks. Opening a run opens a root trace span. A trace is the record a tracing tool "
            "keeps of one piece of work, and a span is one timed part of it. The trace id of that root span "
            "becomes the run id.</p>")
    out += code(RECORDER, r"    def run\(self, domain: str, env: str, trigger", caption="Opening and closing a run. The run id is the root span's trace id.")
    out += ("<p>Walk through it. A root span starts, and its trace id is passed to the store as the run id. "
            "The block's body then runs. If the body raises, the error is logged against the run unless a "
            "dataset already logged it, every dataset still open is closed as failed, the run is finished, the "
            "hooks are called, and the error is raised again. If the body ends normally, the store works out "
            "the run's status from its dataset runs. A run with one failed dataset out of two comes back as "
            "<code>partial</code>. Either way the <code>on_finish</code> hooks receive the run id last.</p>")
    out += code(RECORDER, r"    def step\(self, name: str\)", caption="A step as a block. An exception inside it becomes a failed step row, then travels on.")
    out += code(RECORDER, r"    def check\(self, kind: str, name: str", r"        self\.span\.set\(\{f\"dp\.detect", "How a check and a detector become step rows in the one format.", cap=20)
    out += ("<p>A check that did not pass is <code>failed</code> unless the caller said it should only warn. A "
            "detector that fired is <code>warned</code> when the platform handled the condition and "
            "<code>failed</code> when it blocked the load.</p>")
    out += "<h2>Tracing wrapped around the store</h2>"
    out += ("<p>The ingestion, silver and gold runners were written before the recorder and call the store "
            "directly. To trace them without changing them, the store itself is wrapped. The wrapper forwards "
            "every call and adds a span for each run, dataset run and step.</p>")
    out += code(TRACED, r"    def __getattr__\(self, name", r"            span\.end\(\"error\" if step\.status == \"failed\" else \"ok\", step\.message\[:200\]\)",
                "Five calls are intercepted. Every other call goes straight through.", cap=30)
    out += code(TRACED, r"    def finish_run\(self, run_id", caption="Finishing a run ends its span and pushes the run id to the hooks.")
    out += ("<p>Two details matter. First, here the run id is the one the runner chose, and it is put on every "
            "span as the tag <code>dp_run_id</code>. The trace id is the tracer's own. Second, the hooks are "
            "called after the store has finished the run, so the monitor reads a complete record.</p>")
    out += code(TRACE, r"^def as_uuid\(", caption="The same 128 bits written as a UUID, the form lineage events want for a run id.")
    out += "<h2>The settings that drive it</h2>"
    out += ("<p>The run log reads no settings from the metadata store. What it records is fixed by the code. "
            "The only choices are made by the binding, which is the script that picks adapters: which store, "
            "which clock, and which tracer.</p>")
    out += "<h2>How it is tested</h2>"
    out += (f"<p>{src(T + 'test_obs_recorder.py')} is one table of cases. Each case plays a small run against "
            "the in-memory store and checks one fact about what was recorded.</p>")
    out += code(T + "test_obs_recorder.py", r"^CASES = \[", 14, "The start of the recorder's case table: a name, an action, and the expected result.")
    out += (f"<p>{src(T + 'test_obs_runlog.py')} is the contract table for the read port. The same cases run "
            "against the in-memory adapter and the PostgreSQL adapter. The PostgreSQL rows are skipped when no "
            "local server answers, and the customer adapter's rows are always skipped for now.</p>")
    out += code(T + "test_obs_runlog.py", r"^CASES = \[", r"^\]", "What every adapter of the read port must answer.", cap=24)
    out += (f"<p>{src(T + 'test_obs_steps.py')} runs the step contract over the rows of every owner: the "
            "recorder, ingestion, the fixture runner and the gold harness. The silver row and the data "
            f"expectation services row are skipped as pending. {src(T + 'test_obs_traced_store.py')} checks "
            f"that a traced run produces one trace and one push. {src(T + 'test_obs_trace.py')} checks the "
            "shape of the ids.</p>")
    out += code(T + "test_obs_steps.py", r'\("case", "row", "problem"\),', r"^\)", "The refusals of the step contract, one row each.")
    out += not_built([
        "The recorder, with its run id equal to the trace id, is not used by the ingestion, silver or gold "
        "runners. They record through the store with their own run ids, and the tracing wrapper links the two "
        "by a tag.",
        "The tracing wrapper's <code>start_dataset_run</code> does not hand back the watermark that the store "
        "returns. No runner reads that return value today, so nothing breaks, but the wrapper is not a perfect "
        "stand-in for the store.",
        "Silver does not yet write <code>detect.</code> rows for the conditions it handles. They are judged on "
        "what the silver tables hold.",
        "Silver records all its stages together after the run, so the time of a whole silver run shows on its "
        "first stage in the step timings.",
    ])
    return out


# ------------------------------------------------------------------------------------------------------ freshness
def fresh_picture():
    d = text(20, 30, "Hours since the dataset last loaded successfully", "tx")
    d += box(20, 50, 300, 60, "ok", "up to the warning target", "bx")
    d += box(320, 50, 300, 60, "warn", "past the warning target", "bxs")
    d += box(620, 50, 300, 60, "error", "past the error target", "solid")
    d += arrow(320, 150, 320, 114) + text(320, 170, "sla_freshness_hours", "sm", "middle")
    d += arrow(620, 150, 620, 114) + text(620, 170, "sla_freshness_error_hours", "sm", "middle")
    d += text(470, 210, "Only the error state raises an alert. A dataset that never loaded has the state never.", "sm", "middle")
    return d


@page("obs-freshness", "Freshness: how late is too late", parent="obs", short="Freshness",
      blurb="Each dataset states a warning and an error target in hours. Two measures are checked against them.")
def obs_freshness():
    out = ("<p class='lead'>Freshness says how old the newest data is, compared with what the dataset promised. "
           "A dataset that promises data no older than two hours is late when its last successful load is "
           "older than that.</p>"
           "<p>The promise is not written in code. Each dataset's manifest states it, and the freshness "
           "functions read it from the metadata store. A dataset with no target is not checked at all.</p>")
    out += fig(940, 230, fresh_picture(), "The three states along a line of hours",
               "Read left to right as time passes without a load. The two arrows are the two settings.")
    out += "<h2>Two measures, because one is not enough</h2>"
    out += table(["Measure", "The question", "What it is based on", "What it catches"], [
        ["Dataset freshness", "How long since this dataset last loaded successfully?",
         "The end time of the newest dataset run with status succeeded",
         "A source that stopped sending, or a job that stopped running"],
        ["Feed freshness", "Is one feed of this dataset behind the others?",
         "Each bookmark of the dataset whose value is a time, compared with the newest such bookmark",
         "One dealer's monthly file missing while every other dealer's arrived"],
    ])
    out += ("<p>A <b>bookmark</b> is a named position the metadata store keeps for a dataset. Ingestion keeps "
            "one bookmark per dealer for the dealer sell-out files, holding the newest data time in that "
            "dealer's file. A run that found nothing new still counts as a successful load for dataset "
            "freshness: the platform looked, and it is current with its source.</p>")
    out += flow3("The dataset's targets from the metadata store, the dataset runs from the run log, and the "
                 "dataset's bookmarks.",
                 "For each dataset with a target, the age of the last success is compared with the two "
                 "targets. Then each time-valued bookmark is compared with its newest sibling.",
                 "One result per dataset and one per bookmark, each with a state: ok, warn, error or never.")
    out += "<h2>A worked example</h2>"
    out += ("<p>The targets below are the tyre company's, as the test reads them from the loaded manifest. The "
            "first column is the dataset. The next two are its targets in hours. The tier and the rota follow "
            "from them and are explained further down.</p>")
    out += table(["Dataset", "Warning after", "Error after", "Tier", "Alerts go to"], [
        ["<code>erp.sales_order_line</code>", "1 hour", "2 hours", "1", "erp-data-rota"],
        ["<code>erp.material</code>", "24 hours", "48 hours", "2", "the rota named in its manifest"],
        ["<code>partner.dealer_sellout</code>", "72 hours", "120 hours", "3", "partner-data-rota"],
    ])
    out += ("<p>Now a check with example values. The order lines last loaded 1.5 hours ago, so the state is "
            "warn and nobody is told. The dealer sell-out dataset loaded 0.2 hours ago, so the dataset is ok. "
            "But its bookmarks tell a second story.</p>")
    out += raw("""dataset                   key     basis          age in hours   state
erp.sales_order_line              last_success     1.5          warn
partner.dealer_sellout            last_success     0.2          ok
partner.dealer_sellout    D-1     bookmark_lag     0.0          ok
partner.dealer_sellout    D-2     bookmark_lag     0.0          ok
partner.dealer_sellout    D-42    bookmark_lag   744.0          error""",
               "Dealer D-42's bookmark stands at the end of September while the others stand at the end of October. 744 hours is 31 days.")
    out += ("<p>The dataset loaded today, yet one dealer is a month behind. Dataset freshness alone would have "
            "said all is well. This is the late dealer file that the simulated company plants, and the "
            f"{link('tests-incidents', 'planted incidents page')} follows it through the layers.</p>")
    out += "<h2>The settings that drive it</h2>"
    out += ("<p>The table lists every setting freshness reads. The registry is the list of known settings in "
            f"{src('P/src/dataplatform/meta/settings_registry.json')}.</p>")
    out += table(["Setting", "Where it is set", "Meaning", "Default"], [
        ["<code>sla_freshness_hours</code>", "Dataset settings. Registered in the registry as a whole number.",
         "Hours after which the dataset is in the warn state.", "None. Without it the dataset has no target."],
        ["<code>sla_freshness_error_hours</code>", "Dataset settings. In the registry with status proposed.",
         "Hours after which the dataset is in the error state.", "Twice the warning target."],
        ["<code>owners.alerts</code>", "The dataset manifest", "The rota that alerts go to.", "The dataset owner."],
        ["<code>owners.dataset</code>", "The dataset manifest", "The dataset's owner.", "Empty."],
        ["<code>tiers</code>", f"The alert configuration, {src(RULES)}",
         "A tier forced for a named dataset.", "Empty: the tier follows the error target."],
    ])
    out += code("C/slices/02-tyre/manifest/datasets/partner.dealer_sellout.yaml", r"sla_freshness_hours", 2,
                "The dealer sell-out dataset's two targets, as its manifest states them.")
    out += "<h2>The code</h2>"
    out += code(FRESH, r"^def target_from_view\(", caption="A dataset's targets, read from the merged view the metadata store gives.")
    out += ("<p>The function returns nothing when the dataset has no warning target, and that dataset is then "
            "left out of every freshness check. The error target falls back to twice the warning target. The "
            "rota falls back to the dataset owner.</p>")
    out += code(FRESH, r"^def tier_for\(", caption="The tier from the error target, unless the alert configuration forces one.")
    out += ("<p>A <b>tier</b> says how urgent a dataset is. An error target of 4 hours or less is tier 1. An "
            "error target of 48 hours or less is tier 2. Anything longer is tier 3. The alerts page shows what "
            "each tier means for who is told.</p>")
    out += code(FRESH, r"^def state_of\(", caption="The whole rule for a state.")
    out += code(FRESH, r"^def check\(", caption="Both measures. It returns one result per dataset, then one per time-valued bookmark.")
    out += ("<p>Walk through it. The helper <code>last_success</code> finds, for each dataset, the newest run "
            "that succeeded, has an end time, and ended before now. A run still open does not count, and a "
            "failed run does not count. The age is the hours from that end time to now. Then the bookmarks: "
            "only values that parse as a time with a time zone are kept, so a shard position such as "
            "<code>1234</code> is ignored. With fewer than two such bookmarks there is nothing to lag behind, "
            "and the loop moves on. Otherwise each bookmark's lag is its distance from the newest one, and the "
            "same two targets decide its state.</p>")
    out += code(FRESH, r"^def next_due\(", caption="The one moment at which a lateness check is worth running.")
    out += ("<p>Lateness has no event. Nothing happens when data fails to arrive. So instead of checking on a "
            "fixed interval, the monitor asks this function when the next dataset would pass its error target "
            "if nothing loads, and checks only then. A dataset that is already late is skipped, because "
            "checking it again changes nothing.</p>")
    out += "<h2>The success criteria and the scorecard</h2>"
    out += ("<p>Freshness is one promise. The platform also keeps one list of twenty criteria that say what a "
            "good run means, each with a single metric and a target. After a run, a scorecard gives each "
            "criterion one of three states: met, not met, or not measured. A criterion the run cannot see is "
            "never counted as met.</p>")
    out += code(SUCCESS, r"^CRITERIA = \(", r"^\)", "The twenty criteria. Timeliness is C7 and C8.", cap=24)
    out += ("<p>Eight of the twenty are measured by a run from its own evidence: C1 to C5, C8, C9 and C15. The "
            "other twelve are reported as not measured, with the note that they are measured outside the run. "
            "The two timeliness numbers in the code are these. The hop from bronze to silver must take 30 "
            "minutes or less at the 95th percentile. One simulated day through all layers at one tenth of full "
            "volume must take 6 minutes or less.</p>")
    out += code(SUCCESS, r"^def _c8\(", caption="The hop latency criterion. The hop from source commit to bronze is shown as context and not judged.")
    out += ("<p>The scorecard rows travel with the operations tables as a sixteenth table, "
            "<code>ops_success</code>, so the run's score can be published beside its other tables.</p>")
    out += "<h2>How it is tested</h2>"
    out += (f"<p>{src(T + 'test_obs_freshness.py')} is table-driven. One table covers the states, one the "
            "tiers, one the full check, one the targets read from a view, and one the next due time. A "
            "further test loads the tyre manifest into the in-memory store and checks that nine datasets have "
            f"targets with the values shown above. {src(T + 'test_obs_success.py')} has one case per measured "
            "criterion for met, not met and not seen.</p>")
    out += code(T + "test_obs_freshness.py", r"^DS = \[", r"^\]", "The cases for the full check: runs, bookmarks, and the results expected.")
    out += not_built([
        "Criterion C7, the share of days whose gold is complete by 06:00 business time, is in the list but no "
        "code measures it.",
        "The freshness target is measured from the end of the last successful load, not from the age of the "
        "newest row in gold. A freshness target measured end to end is an obligation in the catalog that no "
        "test cites.",
        "In a local simulated run the platform loads once per simulated day, so datasets that promise hourly "
        "freshness show breaches between days. The operations page says so in its subtitle.",
        "Criterion C20's target text says 89 of 89 rules. The catalog snapshot beside this code holds 105 "
        f"obligations, as {link('tests-by-layer')} reports.",
        "Hop latency from a simulated run uses the simulated clock, so it describes the schedule and not the "
        "speed of a real network.",
    ])
    return out


# ------------------------------------------------------------------------------------------------------ alerts
def alerts_picture():
    d = box(10, 40, 170, 70, "A run finishes", "pushed by the store", "bx")
    d += box(10, 150, 170, 70, "Lateness is due", "the monitor wakes", "bx")
    d += box(250, 95, 170, 70, "Signals", "one per subject", "bxs")
    d += box(490, 95, 190, 70, "Evaluate", "state and noise rules", "solid")
    d += box(750, 20, 220, 60, "Tier 1: page", "the rota", "bx")
    d += box(750, 100, 220, 60, "Tier 2: ticket", "the rota", "bx")
    d += box(750, 180, 220, 60, "Tier 3: digest", "the dataset owner", "bx")
    d += arrow(180, 75, 248, 115) + arrow(180, 185, 248, 145) + arrow(420, 130, 488, 130)
    d += arrow(680, 115, 748, 50) + arrow(680, 130, 748, 130) + arrow(680, 145, 748, 210)
    return d


@page("obs-alerts", "Alerts: what raises one, how severe it is, and who is told", parent="obs", short="Alerts",
      blurb="Five rules as configuration, routing by tier, and four controls that keep the number of alerts low.")
def obs_alerts():
    out = ("<p class='lead'>An alert is a message to a person that something needs attention. The platform "
           "raises one when a dataset run fails, when data is older than its error target, when a detector "
           "fires, or when a quality or reconciliation check fails.</p>"
           "<p>The hard part of alerting is not sending. It is sending few enough that people still read them. "
           "So the rules are configuration reviewed like any other, and four controls decide whether a failing "
           "condition becomes a new message.</p>")
    out += fig(980, 260, alerts_picture(), "From a finished run or a due lateness check to a routed message",
               "Two things start an evaluation, on the left. The Evaluate box decides what is sent. The tier picks the channel.")
    out += "<h2>Which conditions raise an alert</h2>"
    out += ("<p>A <b>signal</b> is one subject's verdict at one check: failing or not. A <b>subject</b> is the "
            "thing the verdict is about. The table lists the five signal kinds, the subject of each, and the "
            "exact condition that makes it failing.</p>")
    out += table(["Rule id", "Signal", "Subject", "Failing when"], [
        ["<code>run-failed</code>", "<code>run_failed</code>", "The dataset, such as <code>erp.material</code>",
         "The dataset run ended with status failed."],
        ["<code>freshness-breach</code>", "<code>freshness</code>",
         "The dataset, or the dataset and a bookmark key, such as <code>partner.dealer_sellout/D-0042</code>",
         "The freshness state is error. The warn state does not alert."],
        ["<code>detector-fired</code>", "<code>detector</code>",
         "The dataset and the detector, such as <code>erp.sales_order_line/DEL-01</code>",
         "A <code>detect.</code> step has any status other than succeeded. Both a handled condition and a "
         "blocked load alert."],
        ["<code>check-failed</code>", "<code>check_failed</code>", "The dataset and the step name",
         "A <code>quality.</code> or <code>recon.</code> step has status failed. A warned check does not alert, "
         "and a profile never does."],
        ["<code>heartbeat-missing</code>", "<code>heartbeat</code>", "Not defined in code",
         "Nothing produces this signal yet."],
    ])
    out += code(RULES, r"^rules:", r"^snoozes:", "The shipped rules, routes and limits. A domain may replace this file.")
    out += "<h2>Severity, and who is told</h2>"
    out += ("<p>Severity is the dataset's tier. The tier picks a channel, and the dataset's manifest says who "
            "receives on that channel. The tier comes from the dataset's freshness error target, as the "
            f"{link('obs-freshness', 'freshness page')} explains, unless the configuration forces one.</p>")
    out += table(["Tier", "Error target", "Channel", "Goes to"], [
        ["1", "4 hours or less", "page", "The rota in <code>owners.alerts</code>"],
        ["2", "48 hours or less", "ticket", "The rota in <code>owners.alerts</code>"],
        ["3", "Longer, or the dataset has no freshness target", "digest", "The owner in <code>owners.dataset</code>"],
    ])
    out += code(ALERTS, r"^def route_for\(", caption="Turning a tier into a channel and a recipient.")
    out += ("<p>When the route names the rota but the manifest has none, the dataset owner is used. When the "
            "manifest names nobody at all, the recipient is the word <code>unrouted</code>, so the gap is "
            "visible in the alert log and the message is not lost. A dataset with no freshness target has no "
            "owners attached to its signals, so its run failures are tier 3 and unrouted.</p>")
    out += "<h2>The four controls that keep alerts few</h2>"
    out += table(["Control", "Rule", "Example with the tyre company"], [
        ["Change of state", "A subject alerts when it starts failing and resolves when it recovers. Still "
                            "failing at the next check sends nothing.",
         "Material copies stay held for six hours. One alert, not one per check."],
        ["One per rule, route and business day", "A second subject failing the same day under the same rule "
                                                 "and route joins the day's alert.",
         "Two dealers' files are late on the same day. One message lists both."],
        ["Routed by tier", "The channel and recipient come from the tier and the manifest.",
         "Order lines page the rota. Dealer sell-out goes into the owner's digest."],
        ["Snoozes expire", "A snooze needs a rule, a subject pattern, a reason of at least 10 characters, an "
                           "owner, and an end time with a time zone no more than 30 days ahead.",
         "The lab system is down for a planned upgrade until Friday. After Friday a subject still failing "
         "alerts again."],
    ])
    out += ("<p>A business day is counted in the domain's business time zone, which the shipped rules set to "
            "Asia/Kolkata. A resolution message is sent once per recovery, and only for a subject that was "
            "actually alerted.</p>")
    out += "<h2>A worked example</h2>"
    out += ("<p>The freshness rule is checked every two hours for dealer D-0042's file and dealer D-0107's "
            "file, both tier 3. The first column is the hour, the second is which subjects are failing, and the "
            "third is what is sent.</p>")
    out += table(["Hour", "Failing", "Sent"], [
        ["0", "D-0042", "One alert for <code>freshness-breach</code>, digest, to the dataset owner"],
        ["2", "D-0042 and D-0107", "Nothing. D-0107 joins the day's alert and is recorded as grouped."],
        ["4", "D-0042 and D-0107", "Nothing. Both are still failing."],
        ["6", "D-0107", "One resolution for D-0042"],
        ["24", "D-0107", "Nothing. It never stopped failing, so there is no change of state."],
    ])
    out += "<h2>The code</h2>"
    out += code(ALERTS, r"^def evaluate\(", caption="One evaluation: the signals of this check against the state kept from the last one.")
    out += ("<p>Walk through it. The state has two parts. <code>subjects</code> holds every subject known to "
            "be failing, keyed by rule and subject. <code>daily</code> holds the messages already sent today, "
            "and entries from earlier business days are dropped on the first line that builds it. The signals "
            "are sorted into rules. For each rule, every failing subject goes to <code>_fail</code>. Then every "
            "subject the state knew as failing, which is not failing now and which the scope covers, goes to "
            "<code>_recover</code>.</p>"
            "<p>The <b>scope</b> is what makes pushed evaluation safe. A finished run speaks only for the "
            "datasets it ran. Without a scope, a run of the order lines would find no signal for the material "
            "dataset and wrongly resolve its alert.</p>")
    out += code(ALERTS, r"^def _fail\(", caption="What happens to one failing subject. The four controls are four early returns.")
    out += ladder([
        ("Already notified", "The subject was alerted and is still failing. Return. This is change of state."),
        ("Find the route", "The tier gives the channel and the recipient, and both are stored on the subject."),
        ("Snoozed", "A snooze that has not expired matches the rule and subject. Record a snoozed event and "
                    "return. The subject is not marked notified, so it alerts once the snooze ends."),
        ("Join today's alert", "A message for this rule, day, channel and recipient already went out. Add the "
                               "subject to it, record a grouped event, and return."),
        ("Send", "Otherwise create the day's alert, add it to the list to send, and record a fired event."),
    ])
    out += code(OPS, r"^def run_signals\(", caption="How a finished run's records become signals.")
    out += ("<p>Every dataset the run touched produces a <code>run_failed</code> signal, failing or not. The "
            "not-failing ones matter: they are how a dataset that failed yesterday resolves today. Detector "
            "and check signals come only from step rows in the one format, and each message carries the name "
            "of the screen where the owner keeps the detail.</p>")
    out += code(MONITOR, r"    def run_finished\(self, run_id", caption="The monitor's entry point, called as each run ends.")
    out += ("<p>The monitor reads the run log, keeps only this run's records, and evaluates them inside a scope "
            "naming this run's datasets and the three run signal kinds. It then runs a lateness check, because "
            "a run that loaded data can end a lateness. The lateness check also stores the next due time, and "
            "<code>wake</code> does nothing until that time has come.</p>")
    out += "<h2>Delivery</h2>"
    out += ("<p>Sending is behind a port with one method, <code>send</code>. Two adapters exist. The in-memory "
            "one keeps the alerts in a list and is what every run and test on this site uses. The other, in "
            f"{src('P/src/dataplatform/adapters/databricks_notify.py')}, maps each channel to one of the "
            "workspace's notification destinations and posts one payload shape. A channel with no destination "
            "bound is refused with an error, never dropped silently.</p>")
    out += "<h2>How it is tested</h2>"
    out += (f"<p>{src(T + 'test_obs_alerts.py')} drives the evaluator with timelines. Each case is a list of "
            "checks, each an hour and the set of failing subjects, and the expected result is the list of "
            "messages sent with their hour, rule, subjects, channel and recipient.</p>")
    out += code(T + "test_obs_alerts.py", r"^TIMELINES = \[", r"^\]", "The noise controls as timelines. The first two rows are the core promise.", cap=30)
    out += (f"<p>The same file holds eleven configuration refusals, the routing table by tier, and the scope "
            f"cases. {src(T + 'test_obs_notify.py')} is the contract table for the notifier port, run against "
            "both adapters.</p>")
    out += not_built([
        "No code produces the heartbeat signal. The rule is configuration only.",
        "Alert state lives in memory on the monitor. Restarting the process forgets which subjects were already "
        "notified.",
        "Sending through a real workspace destination is not done. The transport is injected in tests.",
        "The platform's own job failure notification, meant to be set on every deployed job, is written as a "
        "snippet but not merged into the bundle generator.",
        "A dataset with no freshness target has no owners on its signals, so its alerts go to "
        "<code>unrouted</code>. Nothing warns about that at load time.",
    ])
    return out


# ------------------------------------------------------------------------------------------------------ lineage
def lineage_picture():
    d = box(10, 60, 190, 80, "Source dataset", ["erp_db:", "sales_order_line"], "bxm")
    d += box(290, 60, 190, 80, "Bronze table", ["dev_bronze.erp.", "sales_order_line"], "bx")
    d += box(570, 60, 170, 80, "Silver", "", "bxs")
    d += box(820, 60, 150, 80, "Gold", "", "bxs")
    d += arrow(200, 100, 288, 100) + arrow(480, 100, 568, 100) + arrow(740, 100, 818, 100)
    d += text(245, 40, "ours: one event per batch", "sm", "middle")
    d += text(700, 40, "the engine's catalog: table to table", "sm", "middle")
    d += text(490, 180, "The platform's run id is on both halves, so they join into one path.", "tx", "middle")
    return d


@page("obs-lineage", "Lineage: where a table's rows came from", parent="obs", short="Lineage",
      blurb="The platform records the hop the engine's catalog cannot see: source to bronze, one event per batch.")
def obs_lineage():
    out = ("<p class='lead'>Lineage is the record of which data fed which table. It answers two questions: "
           "where did this number come from, and what breaks downstream if this source changes.</p>"
           "<p>From bronze onward, the engine's own catalog records lineage as jobs read and write tables, and "
           "the platform adopts that screen. The catalog cannot see the hops before bronze: source system, "
           "mover, landing, bronze. Those exist only in the platform's file log. This part turns the file log "
           "into lineage events for that first hop.</p>")
    out += fig(980, 200, lineage_picture(), "Which half of the lineage path each side records",
               "The dashed box is outside the lakehouse. The first arrow is the only one this code produces.")
    out += "<h2>What is recorded, and at what grain</h2>"
    out += table(["Question", "Answer"], [
        ["What is one lineage record?", "One event for one batch, which is one record of the file log."],
        ["What does it connect?", "One source dataset to one bronze table."],
        ["Is there column lineage?", "No. The grain is dataset to table. Columns are not traced by this code."],
        ["Which batches produce an event?",
         "A loaded batch produces a <code>COMPLETE</code> event with an output. A batch rejected for breaking "
         "its contract, rejected for volume, or held as a mass delete produces a <code>FAIL</code> event with "
         "no output, because nothing reached bronze."],
        ["Which produce none?", "A duplicate that the lander skipped. Nothing moved."],
        ["What shape is the event?", "The run event of OpenLineage, an open standard for lineage events."],
        ["What ties it to the rest?", "The run id of the run that handled the batch."],
    ])
    out += flow3("File log records for a set of batches, a map from source name to source system, the "
                 "environment and an event time.",
                 "Each record with a known status becomes one event. The status decides the event type and "
                 "whether there is an output.",
                 "A list of events, ready for whichever emitter the binding provides.")
    out += "<h2>A worked example</h2>"
    out += ("<p>Three deliveries of order lines are in the file log. The first column is the file, the second "
            "its status, and the third the event that comes out.</p>")
    out += table(["File", "Status in the file log", "Event"], [
        ["<code>sol_2026-11-09.parquet</code>", "loaded",
         "<code>COMPLETE</code>, input <code>erp_db</code> / <code>sales_order_line</code>, output "
         "<code>dev_bronze.erp.sales_order_line</code>"],
        ["<code>sol_2026-11-09_resent.parquet</code>", "skipped_duplicate", "None"],
        ["<code>sol_2026-11-10.parquet</code>", "rejected_contract", "<code>FAIL</code>, the same input, no output"],
    ])
    out += "<h2>The code</h2>"
    out += code(LINEAGE, r"^def events_from_files\(", caption="The whole of it: one loop over file log records.", cap=32)
    out += ("<p>Walk through it. A record whose status is neither loaded nor one of the three held statuses is "
            "skipped. The event's run carries the batch's own facts as a facet, which is OpenLineage's word "
            "for an extra block of detail: the batch key, the window, the file, the rows counted and declared, "
            "the status and the reason. The job is named after the landing step for the dataset. The input's "
            "namespace is the source system, found through the map, so <code>erp</code> becomes "
            "<code>erp_db</code>. The output is the bronze table's name for the environment, and it is present "
            "only for a loaded batch.</p>")
    out += "<h2>Lineage in the operations tables</h2>"
    out += ("<p>There is a second, separate source. The platform's own telemetry, copied once per business "
            "day, includes table-to-table edges with the run that wrote them. The operations builder turns "
            "those into the table <code>ops_lineage</code> with four columns: day, run id, source table and "
            "target table. No screen of ours reads it now, because browsing lineage is the platform's own "
            "screen.</p>")
    out += code(OPS, r"^def lineage_table\(", caption="The table-to-table edges from the platform's telemetry.")
    out += "<h2>The settings that drive it</h2>"
    out += ("<p>No dataset setting is read. The caller passes the environment, the name of the bronze "
            "namespace, which defaults to <code>lakehouse</code>, and the map from source to system.</p>")
    out += "<h2>How it is tested</h2>"
    out += (f"<p>{src(T + 'test_obs_lineage.py')} has three cases in a table and one more test. Each case "
            "gives a file status and expects an event type and a number of outputs.</p>")
    out += code(T + "test_obs_lineage.py", r"^CASES = \[", r"^\]", "The three cases. A fourth test checks that a skipped duplicate produces no event.")
    out += not_built([
        "Nothing calls this function outside its test. No emitter adapter sends the events to a lineage "
        "service, and the local run does not write them anywhere.",
        "The event's run id is the file record's run id as stored, such as <code>ingest-2026-11-09</code>. "
        "OpenLineage expects a UUID there. The conversion helper exists on the run recorder, which the "
        "ingestion runner does not use.",
        "There is no column-level lineage from this code.",
        "The mover and the landing area do not appear as separate steps. The event goes from the source "
        "dataset straight to the bronze table.",
        "The table <code>ops_lineage</code> is still built although no screen reads it. The code comments call "
        "removing it a follow-up.",
    ])
    return out


# ------------------------------------------------------------------------------------------------------ ops page
def page_picture():
    d = box(10, 20, 170, 60, "Run log", "", "bx", href="obs-runlog.html")
    d += box(10, 100, 170, 60, "Freshness results", "", "bx", href="obs-freshness.html")
    d += box(10, 180, 170, 60, "Alert events", "", "bx", href="obs-alerts.html")
    d += box(10, 260, 170, 60, "Platform telemetry", "daily copy", "bxm")
    d += box(260, 130, 190, 80, "ops.build", "pure: rows in, rows out", "solid")
    d += box(530, 130, 190, 80, "15 ops tables", "plain rows", "bxs")
    d += box(800, 20, 180, 70, "ops.html", "one file per run", "bx")
    d += box(800, 130, 180, 80, "ops_tables.json", "typed, portable", "bx")
    d += box(800, 250, 180, 70, "Catalog tables,", ["views, dashboards"], "bxm")
    d += arrow(180, 50, 258, 150) + arrow(180, 130, 258, 165) + arrow(180, 210, 258, 180) + arrow(180, 290, 258, 195)
    d += arrow(450, 170, 528, 170) + arrow(720, 150, 798, 60) + arrow(720, 170, 798, 170) + arrow(890, 210, 890, 248)
    return d


@page("obs-page", "The operations page, the operations tables and the dashboards", parent="obs",
      short="Operations page and dashboards",
      blurb="Fifteen small tables built from the run log, one HTML page per run, and the views and dashboards on Databricks.")
def obs_page():
    out = ("<p class='lead'>After every run the platform writes one HTML page that shows what happened: the "
           "runs, the files held back, the checks that warned, the freshness results and the alerts. The same "
           "rows are published as tables on Databricks, where dashboards read them.</p>"
           "<p>The page and the dashboards never compute anything themselves. One builder turns the run log "
           "into fifteen small tables, and everything shown is a row of one of them. That is why the page on a "
           "laptop and the dashboard in the workspace show the same numbers.</p>")
    out += fig(990, 340, page_picture(), "From four inputs to the page, the file and the published tables",
               "The builder in the middle is the only place where numbers are worked out. Follow its output to the three places it is shown.")
    out += "<h2>The fifteen operations tables</h2>"
    out += ("<p>Each table is a list of plain rows. Every table has a <code>day</code> column holding the "
            "business day, and the tables about runs carry the run id so they join.</p>")
    out += table(["Table", "One row is"], [
        ["<code>ops_runs</code>", "One run: status, duration, number of datasets, number failed"],
        ["<code>ops_dataset_runs</code>", "One dataset run: status, duration, rows read and written, attempt number, whether it was slow"],
        ["<code>ops_run_status_daily</code>", "A day and a status, with the number of dataset runs"],
        ["<code>ops_errors_daily</code>", "A day and an error class, with the number of errors"],
        ["<code>ops_steps</code>", "One check result in the one step format, with its owner, severity and the owner's screen"],
        ["<code>ops_step_durations</code>", "One step of one dataset run, with the seconds it took"],
        ["<code>ops_ingestion_daily</code>", "A day and a dataset: files loaded, duplicates skipped, files rejected, rows loaded"],
        ["<code>ops_files_held</code>", "One file that was not loaded, with its reason"],
        ["<code>ops_restatements</code>", "One published gold window that changed, per measure: the number before and after"],
        ["<code>ops_freshness</code>", "One freshness result"],
        ["<code>ops_slo_daily</code>", "A day and a dataset: was the freshness target met at every check"],
        ["<code>ops_usage_daily</code>", "A day and a table: reads and distinct readers"],
        ["<code>ops_lineage</code>", "One table-to-table edge with the run that wrote it"],
        ["<code>ops_alerts</code>", "One alert event: fired, grouped, snoozed or resolved"],
        ["<code>ops_alert_counts</code>", "A rule and a day, with the number of alerts sent"],
    ])
    out += code(OPS, r"^def build\(", caption="The builder. Its inputs are the four boxes on the left of the picture.")
    out += "<h2>Two derived columns worth knowing</h2>"
    out += code(OPS, r"^def _performance\(", caption="Attempts and slow-downs, worked out per dataset from the run log alone.")
    out += ("<p>An <b>attempt</b> counts the runs of one dataset on one business day, and any attempt after "
            "the first is a retry. A run is <b>slow</b> when it took at least twice the median time of that "
            "dataset's own earlier successful runs. The median is the middle value, so one odd run does not "
            "move it much. The first run of a dataset has nothing to compare with and is never slow.</p>")
    out += "<h2>A worked example</h2>"
    out += ("<p>Three rows of <code>ops_dataset_runs</code> for the material dataset, with example values. The "
            "third run takes more than twice the usual time.</p>")
    out += raw("""day         dataset        status     duration_s  attempt  retry  duration_ratio  slow
2026-11-09  erp.material   succeeded    12.0        1        0                      false
2026-11-10  erp.material   succeeded    14.0        1        0      1.167           false
2026-11-11  erp.material   succeeded    30.0        1        0      2.308           true""",
               "The ratio on the third day is 30 divided by the median of 12 and 14, which is 13.")
    out += "<h2>The page each run writes</h2>"
    out += ("<p>The page is one self-contained file: white, no scripts, no external files. It opens from disk. "
            "At the top are eight tiles, then one section per table.</p>")
    out += code(PAGE, r"    tiles = \"\"\.join\(\[", r"^    \]\)$", "The eight tiles at the top of the page.")
    out += table(["Section of the page", "Where its rows come from"], [
        ["Every layer against the world's own totals", "The day-by-day comparison with the oracle, passed in by the run script"],
        ["Planted incidents and whether the platform caught them", "The incident verdicts, passed in by the run script"],
        ["Runs, and dataset runs", "<code>ops_runs</code>, <code>ops_dataset_runs</code>"],
        ["Where the time goes", "<code>ops_step_durations</code>, summed per step, the twelve largest"],
        ["Delay per hop", "Percentiles from the time stamps silver keeps, passed in by the run script"],
        ["Files held or skipped, with the reason", "<code>ops_files_held</code>"],
        ["Steps that warned or failed", "<code>ops_steps</code>"],
        ["Ingestion per day, freshness checks, alerts", "<code>ops_ingestion_daily</code>, <code>ops_freshness</code>, <code>ops_alerts</code>"],
    ])
    out += ("<p>The first two sections exist because the local run uses the simulated company. They show "
            f"whether each layer equalled the truth on each day, which {link('tests-oracle')} explains.</p>")
    out += code(RUN, r"^def write_ops_page\(", caption="The run script gathers the inputs, renders the page, and writes the tables as a file.", cap=32)
    out += "<h2>The portable file</h2>"
    out += ("<p>Plain rows show a column's type only through their values. A catalog table needs each column's "
            "type stated. The export gives every table its columns with one of six types, and values a file "
            "can carry.</p>")
    out += code(EXPORT, r"^def export\(", caption="Declared columns win, so a table with no rows today still has its shape.")
    out += ("<p>The declared columns matter on a quiet day. If no file was held back, "
            "<code>ops_files_held</code> has no rows. Without declared columns it would be published with no "
            "columns, and the dashboard reading it would break.</p>")
    out += "<h2>Certified views over the tables</h2>"
    out += ("<p>A dashboard that aggregates must not invent its own definition of a number. Twelve metric "
            "views sit over the tables. A <b>metric view</b> is a view that names its dimensions and its "
            "measures, so every reader computes a measure the same way. Each dashboard query that aggregates "
            "must name a measure its view certifies, with the same aggregate and the same column, and may group "
            "or filter only on the view's dimensions.</p>")
    out += code(VIEWS, r'    OpsView\("mv_ops_freshness"', 3, "One view definition: its table, its dimensions and its one measure.")
    out += code(VIEWS, r"^def problem\(", caption="Why a dashboard query is refused.")
    out += code(BOARDS + "views/mv_ops_freshness.sql", r"^CREATE OR REPLACE VIEW", r"^\$\$;", 
                "The same view as generated for Databricks. The file is generated and checked for drift.")
    out += "<h2>The dashboards</h2>"
    out += ("<p>Dashboards are defined as data: queries and widgets with no product named. An adapter compiles "
            "each one to a dashboard file for Databricks. The table lists what is in the dashboards folder.</p>")
    out += table(["Dashboard file", "Owner", "The question it answers"], [
        ["<code>platform_health.lvdash.json</code>", "Operations", "Did the loads run, and did they succeed?"],
        ["<code>freshness_slos.lvdash.json</code>", "Operations", "Is the data as fresh as each dataset promises?"],
        ["<code>performance.lvdash.json</code>", "Operations", "Where does each load spend its time, and is it getting slower?"],
        ["<code>sources_ingestion.lvdash.json</code>", "Operations", "What arrived from each source, and what was held back?"],
        ["<code>des_data_expectations.lvdash.json</code>", "Data expectation services", "Is the data right?"],
        ["<code>native/lakeflow_job_health.lvdash.json</code>", "The platform", "Job runs and their health. Adopted unchanged."],
    ])
    out += code(DASH, r"^FRESHNESS = Dashboard\(", r"^\)", "One dashboard definition in full: three queries and three widgets.", cap=24)
    out += ("<p>Because a query is so small, the same definition can be evaluated in memory. That is how a "
            "test proves that every widget has data before anything is deployed.</p>")
    out += "<h2>Publishing on Databricks</h2>"
    out += ("<p>On Databricks the run's task copies the page, the report and the tables file to a volume, "
            "then publishes the tables. Each table is written whole, replacing the last publish, because the "
            "tables are rebuilt from the whole run log on every run. The metric views are created after the "
            f"tables. The generator {src(BOARDS + 'generate.py')} writes the dashboard files, the view files and "
            "the bundle resources, with the catalog <code>dev_ops</code> and the schema <code>ops</code> as "
            "defaults.</p>")
    out += code("P/src/dataplatform/adapters/spark_ops_tables.py", r"^def publish\(", caption="Tables first, then the views. A view the session refuses is reported, not raised.", cap=34)
    out += "<h2>The settings that drive it</h2>"
    out += ("<p>No dataset setting is read here. The business time zone comes from the alert configuration and "
            "decides which day a row belongs to. The catalog and schema of the published tables are arguments "
            "of the generator and of the publish function.</p>")
    out += "<h2>How it is tested</h2>"
    out += (f"<p>{src(T + 'test_obs_page.py')} checks the page's rendering with a small table of cases and one "
            f"test of the tiles. {src(T + 'test_ops_export.py')} has three tables for the column types, the "
            f"round trip and the declared columns. {src(T + 'test_ops_restatements.py')} covers the "
            f"restatement table. {src(T + 'test_obs_dashboards.py')} is the largest: it plays a week and a "
            "fortnight of the simulated company on a fixture run log and checks that every widget of every "
            "dashboard has rows, that the declared columns match what the builder produces, that every "
            "aggregate reads a certified view, and that the generated files are up to date.</p>")
    out += code(T + "test_obs_page.py", r"^CASES = \[", r"^\]", "The page's rendering cases.")
    out += not_built([
        "The dashboard tests run on a fixture run log, which is a run log written by a stand-in runner that "
        "follows the simulated company's calendar. They do not run on the run log the real engine writes.",
        "The operations folder's own notes still say five dashboards and fourteen tables. The code defines four "
        "operations dashboards and fifteen tables. The usage dashboard was dropped in favour of the platform's "
        "own.",
        "<code>ops_usage_daily</code> and <code>ops_lineage</code> are built but no screen reads them.",
        "Each publish replaces the tables. There is no history of earlier publishes beyond what the run log "
        "itself holds.",
        "The bundle resources file must be included from the deployment bundle by hand. I could not verify "
        "from this repository that the dashboards themselves have been deployed to a workspace.",
    ])
    return out


# ------------------------------------------------------------------------------------------------------ reconcile
def recon_picture():
    d = box(10, 60, 170, 80, "Live source table", "all rows now", "bxm")
    d += box(10, 190, 170, 80, "Silver", "current rows", "bx")
    d += box(260, 60, 190, 80, "16 buckets", "rows per bucket", "bxs")
    d += box(260, 190, 190, 80, "16 buckets", "rows per bucket", "bxs")
    d += box(530, 125, 190, 80, "Compare totals", "bucket by bucket", "solid")
    d += box(800, 125, 180, 80, "Differing buckets", ["only: list the keys"], "bx")
    d += arrow(180, 100, 258, 100) + arrow(180, 230, 258, 230) + arrow(450, 100, 528, 150) + arrow(450, 230, 528, 185)
    d += arrow(720, 165, 798, 165)
    return d


@page("obs-reconcile", "Reconciliation and profiling: is the data the same at every hop", parent="obs",
      short="Reconciliation and profiling",
      blurb="Measures compared between hops, the live source compared with silver by buckets, hop reports, and profiles.")
def obs_reconcile():
    out = ("<p class='lead'>Reconciliation means counting the same thing in two places and checking that the "
           "two counts agree. If the source holds 2,410 deliveries and silver holds 2,403, seven rows were lost "
           "somewhere, and no job failed to say so.</p>"
           "<p>A pipeline can lose rows silently. A change feed can skip changes during maintenance. A filter "
           "can drop more than intended. Reconciliation is the only check that finds a loss nobody raised an "
           "error for. Profiling is its companion: it notices when the shape of the data moves.</p>")
    out += "<h2>The four parts on this page</h2>"
    out += table(["Part", "File", "What it does", "When it runs"], [
        ["Measures between hops", src(RECON), "Compares named measures counted at each hop with the first hop.",
         "In the first example domain's check task"],
        ["Live source against silver", src(LIVE), "Compares the source table as it is now with silver's "
                                                 "current rows, and names the missing keys.",
         "After each day's silver run, for database sources"],
        ["Hop reports", src(HOPS), "Turns each layer's run report into plain data and back.",
         "When ingestion, silver and gold run as separate processes or jobs"],
        ["Profiling", src(PROFILING), "Profiles every loaded batch and compares the run's profile with the last one.",
         "In every ingestion run, at the lander"],
    ])
    out += ("<p>Gold has its own checks that no row or quantity is lost between its build and its write. They "
            f"are part of the gold harness, which {link('gold-harness', 'the gold harness page')} explains.</p>")
    out += "<h2>Measures between hops</h2>"
    out += ("<p>A <b>hop</b> is one step of the path from source to gold. Each dataset's manifest lists its "
            "reconciliation measures: a row count, and sums of named columns.</p>")
    out += code("C/slices/02-tyre/manifest/datasets/erp.sales_order_line.yaml", r"^reconciliation:", 6,
                "The order line dataset's reconciliation block in its manifest.")
    out += code(RECON, r"^def compare\(", caption="Every hop is compared with the first hop, measure by measure.")
    out += ("<p>The line to notice is the one that sets <code>ok</code>. A hop with no value for a measure "
            "fails. Having nothing to compare is never treated as a pass.</p>")
    out += "<h2>The live source against silver</h2>"
    out += fig(990, 290, recon_picture(), "Comparing two tables by buckets before comparing keys",
               "Both sides are cut into the same 16 buckets. Only buckets whose totals differ are opened.")
    out += ("<p>Comparing two large tables key by key is expensive. So both sides first add up their rows per "
            "<b>bucket</b>. A bucket is a group of keys chosen by a hash of the business key, so the same key "
            "falls in the same bucket on both sides. Sixteen totals are compared. Only for buckets that differ "
            "are the keys listed on both sides, and the difference is the exact set of keys missing from "
            "silver or extra in it.</p>")
    out += code(RECON, r"^def bucket_of\(", caption="The bucket of a key. The engine computes the same number on silver's side.")
    out += code(LIVE, r"^def reconcile_live\(", caption="Buckets first, then keys only where buckets differ.")
    out += ("<p>Walk through it. The left side is the source's rows, bucketed in memory. The right side is "
            "asked of silver through a port, and only sixteen totals come back. When no bucket differs the "
            "result is returned with the two row counts. Otherwise the source's keys in the differing buckets "
            "are collected, silver is asked for its keys in the same buckets, and the two set differences are "
            "the missing and the extra keys.</p>")
    out += "<h3>A worked example</h3>"
    out += ("<p>Forty deliveries are in the source. The change feed skipped three of them. The values are the "
            "test's.</p>")
    out += table(["Side", "Rows", "Result"], [
        ["Live source", "40", ""],
        ["Silver", "37", "Three buckets differ at most. Missing from silver: DL-0003, DL-0017, DL-0031. Extra: none."],
    ])
    out += ("<p>When keys are missing and none are extra, the message names the hop as sources to landing. The "
            "result is written to the run log as the step <code>recon.live_source_vs_silver</code>, with "
            "status failed when any bucket differs, so the alert rule for failed checks picks it up.</p>")
    out += code(LIVE, r"    def message\(self\)", caption="The message a failed reconciliation carries. It names the hop and up to ten keys.")
    out += "<h2>Hop reports</h2>"
    out += ("<p>On a laptop one process runs ingestion, silver and gold and passes each report along as an "
            "object. Deployed, the three are separate jobs. Each one writes its report as plain data next to "
            "the run, and whoever judges the day reads it back.</p>")
    out += code(HOPS, r"^def load\(", caption="Reading a report back. The kind asked for must be the kind that was written.")
    out += ("<p>The promise is exact: loading what was dumped gives back an equal object of the same type. A "
            "report of another kind, of another format version, or with a missing or unknown field is refused "
            "with a message.</p>")
    out += "<h2>Profiling</h2>"
    out += ("<p>A <b>profile</b> is a summary of a batch: the number of rows, and for each column the number of "
            "empty values and of distinct values. For numbers, dates and times it also keeps the smallest and "
            "the largest. For text columns it keeps counts only, so a profile never holds a value from a text "
            "column. <b>Drift</b> is a profile that moved too far from the last one.</p>")
    out += code(PROFILING, r"^def drift\(", caption="What counts as drift. Each finding is one plain sentence.")
    out += table(["Finding", "Rule in the code"], [
        ["Volume moved", "The row count is at least 3 times the last one, or at most one third of it."],
        ["A column went empty", "The share of empty values in a column rose by 20 percentage points or more."],
        ["A column is new", "The column is in this profile and was not in the last."],
        ["A column stopped coming", "The column was in the last profile and is not in this one."],
    ])
    out += code("P/src/dataplatform/ingest/runner.py", r"^def _profile_step\(", caption="Where ingestion logs the profile and keeps it for next time.")
    out += ("<p>The step is named <code>profile.batches</code>. It is warned when drift was found and names "
            "what moved. Only a normal run's profile is kept as the next one to compare with. A reload or a "
            "backfill does not have the dataset's usual shape.</p>")
    out += "<h3>A worked example</h3>"
    out += table(["Last profile", "This profile", "Finding"], [
        ["100 rows, <code>defect_code</code> empty in 5", "100 rows, <code>defect_code</code> empty in 60",
         "defect_code empty in 60% of rows, was 5%"],
        ["100 rows", "10 rows", "volume moved from 100 to 10 rows"],
        ["100 rows, 110 rows before", "100 rows", "Nothing"],
    ])
    out += "<h2>The settings that drive it</h2>"
    out += table(["Setting", "Where", "Meaning", "Default"], [
        ["<code>reconciliation.measures</code>", "The dataset manifest", "The measures to count: a name, and a "
                                                                        "column to sum or none for a row count.", "None"],
        ["<code>reconciliation.tolerance</code>", "The dataset manifest",
         "How far a measure may differ. The gold plan and the release gate read it. The live comparison "
         "counts rows and applies no tolerance.", "0"],
        ["Buckets", "A constant in the code", "How many buckets the live comparison uses.", "16"],
        ["Drift limits", "Arguments of <code>drift</code>", "The jump in empty share, and the volume ratio.", "0.2 and 3.0"],
    ])
    out += "<h2>How it is tested</h2>"
    out += code(T + "test_reconcile_buckets.py", r"^CASES = \[", r"^\]", "The bucket comparison's cases: what silver holds, the tolerance, and what must be found.")
    out += code(T + "test_profiling.py", r"^DRIFT_CASES = \[", r"^\]", "The drift cases.")
    out += (f"<p>{src(T + 'test_live_recon.py')} covers the live comparison with four cases and checks that "
            f"only differing buckets are opened. {src(T + 'test_reconcile_engine.py')} checks that the engine "
            f"computes the same buckets as Python. {src(T + 'test_hop_reports.py')} has a round trip table and "
            "a refusal table.</p>")
    out += code(T + "test_hop_reports.py", r"^REFUSED = \[", r"^\]", "What a hop report loader refuses.")
    out += not_built([
        "The live comparison counts rows only. It passes one measure, the row count, so a value that changed "
        "without a row going missing is not found by it.",
        "The live comparison runs only for database sources, where the run script can read the live table. "
        "Files, the dealer interface and the stream are not compared this way.",
        "The simulated company plants seven delivery rows that the change feed skips. The code that judges "
        "this exists, but neither recorded design note shows its verdict, and neither slow test passes the "
        f"live reader. {link('tests-incidents')} says more.",
        "Reconciliation entries with their own tables, tolerances signed off by a person, and a screen with "
        "the hop and the keys belong to the data expectation services, which are not built.",
        "The drift limits are arguments with defaults in code. No setting changes them per dataset.",
    ])
    return out


# ------------------------------------------------------------------------------------------------------ recovery
def recovery_picture():
    d = box(10, 60, 180, 80, "1 Withdraw", ["void the batch,", "record a plan"], "solid")
    d += box(270, 60, 180, 80, "2 Silver", ["rebuilt from", "bronze"], "bx", href="silver-reader.html")
    d += box(530, 60, 180, 80, "3 Gold", ["every window", "recomputed"], "bx", href="gold-windows.html")
    d += box(790, 60, 190, 80, "Restatements", ["changed windows", "logged"], "bxs")
    d += arrow(190, 100, 268, 100) + arrow(450, 100, 528, 100) + arrow(710, 100, 788, 100)
    d += text(495, 180, "No row is deleted by hand. Each layer rebuilds from the layer below.", "tx", "middle")
    return d


@page("obs-recovery", "Recovery, retention and housekeeping", parent="obs", short="Recovery and housekeeping",
      blurb="Withdrawing a bad batch by rerun, how long each copy is kept, and the job that removes old bronze safely.")
def obs_recovery():
    out = ("<p class='lead'>Sometimes a batch that was loaded and published turns out to be wrong, a week "
           "later. Recovery is how it is taken out of every layer. Retention is how long each copy of the data "
           "is kept, so that a correct copy is still there to rebuild from. Housekeeping is the job that "
           "removes what retention no longer needs.</p>"
           "<p>The three belong together. A platform can only recover by rerun if the older copy it reruns "
           "from still exists, and it can only delete old data safely if it knows what a rerun would need.</p>")
    out += "<h2>Recovery: withdrawing a bad batch</h2>"
    out += fig(990, 200, recovery_picture(), "The three steps of a withdrawal",
               "The first box is the only manual act. The rest are ordinary runs.")
    out += ("<p>To <b>void</b> a batch is to mark its record in the file log as withdrawn, with a reason and "
            "the name of who approved it. Every reader skips a void batch from then on. A <b>backfill plan</b> "
            "is a record in the metadata store that names windows gold must rebuild regardless of its usual "
            "lookback. A <b>window</b> is the slice of a fact that is rebuilt as a unit, such as one promised "
            "date.</p>")
    out += code(RECOVERY, r"^def withdraw\(", caption="The whole of recovery's own code.")
    out += ladder([
        ("Void the batch", "The store marks the batch void with the reason and the approver. Silver's reader "
                           "never takes a void batch."),
        ("Record a plan for every window", "A backfill plan is recorded whose window list is the special value "
                                           "meaning every window. It carries no corrections."),
        ("Rebuild silver", "The next silver run is started with this dataset named for rebuild. It drops the "
                           "silver table and replays bronze from the first batch, without the void one."),
        ("Run gold as usual", "Gold sees the plan, recomputes every window of the facts that read the dataset, "
                              "replaces only the windows whose numbers changed, and logs each as a restatement."),
    ])
    out += "<h3>A worked example</h3>"
    out += ("<p>The values are from the test. Two orders are loaded for 9 November. One has a quantity of 1. "
            "The other, in a corrupt batch, has a quantity of 9. A week later the batch is withdrawn. The "
            "window is by then outside gold's lookback of five days.</p>")
    out += table(["Case", "Quantity gold shows for 9 November", "Windows restated"], [
        ["The batch is withdrawn", "1", "2026-11-09"],
        ["No withdrawal", "10", "None. A window outside the lookback is left alone."],
    ])
    out += code(T + "test_gold_harness.py", r'@pytest\.mark\.parametrize\(\("case", "withdrawn"', 4,
                "The two cases of the withdrawal test.")
    out += "<h2>Retention: how long each copy is kept</h2>"
    out += ("<p>Three copies of the data exist before silver. <b>Landing</b> holds each delivery as it "
            "arrived. The <b>landing archive</b> is colder storage for older deliveries. <b>Bronze</b> holds "
            "every batch as a table. The table lists the settings with their defaults from the code.</p>")
    out += table(["Setting", "Meaning", "Default"], [
        ["<code>landing_hot_days</code>", "Days a delivery stays in landing before it moves to the archive.", "30"],
        ["<code>landing_archive_days</code>", "Days a delivery stays in the archive after that.",
         "The same as bronze. 90 when the dataset holds personal data."],
        ["<code>bronze_retention_days</code>", "Days bronze keeps a batch.", "1095"],
        ["<code>bronze_baseline</code>", "Whether a baseline batch is written before old bronze is removed.", "true"],
        ["<code>version_history_days</code>", "Days of table versions kept for undo.", "7"],
    ])
    out += ("<p>All five are in the settings registry with status proposed. The personal data limit is a "
            "ceiling, not a default: landing holds personal data in clear, so the archive is never kept longer "
            "than 90 days for such a dataset whatever the setting says. A dataset holds personal data when any "
            "contract column is classified as restricted, personal, pii or sensitive.</p>")
    out += code(RETENTION, r"    def from_settings\(cls, settings", caption="Reading the five settings, with the ceiling for personal data.")
    out += code(RETENTION, r"^def landing_actions\(", caption="Keep, archive or delete, decided for each delivery.")
    out += ("<p>The first branch is the safety rule. A delivery the lander never finished with is always kept, "
            "however old, because it may be the only copy. The lander is finished with a delivery when its "
            "status is loaded, skipped as a duplicate, or void. A held or rejected delivery waits for a "
            "fix.</p>")
    out += code(T + "test_retention.py", r"^ACTION_CASES = \[", r"^\]", "The landing cases, with today fixed at 31 December 2026, 30 hot days and 90 archive days.")
    out += "<h2>Housekeeping: removing old bronze safely</h2>"
    out += ("<p>Bronze cannot simply drop batches older than a cut-off. For a change feed, the old batches are "
            "the only record of rows that have not changed since. So housekeeping needs a <b>baseline</b>: a "
            "complete picture of the table at a point in time. A baseline is either a full copy from the "
            "source, or a batch the platform writes from silver and checks against silver. The <b>correction "
            "window</b> is how far back a rerun must still be possible.</p>")
    out += ("<p>The keep rule in one sentence: keep everything from the newest checked baseline that is "
            "older than the correction window, and remove what is older than that baseline.</p>")
    out += code(HOUSE, r"^def plan_removals\(", caption="The decision for one dataset. It is pure: batches and dates in, a plan out.")
    out += ladder([
        ("Nothing old enough", "If the oldest batch is inside the correction window, nothing is removed."),
        ("No baseline yet", "If no checked baseline exists on or before the cut-off, the plan says a baseline "
                            "is needed and removes nothing in this run."),
        ("Remove what is older than the baseline", "Otherwise the newest qualifying baseline is the keep-from "
                                                   "point. Every batch delivered before it is listed for "
                                                   "removal, oldest first. The baseline itself stays."),
    ])
    out += "<h3>A worked example</h3>"
    out += ("<p>The values are from the test. Today is 15 November 2026 and the correction window is 90 days. "
            "The first column is the batch, the second how many days ago it was delivered, the third its "
            "kind.</p>")
    out += table(["Batch", "Days ago", "Kind", "Decision"], [
        ["b1", "200", "incremental", "Removed"],
        ["b2", "150", "incremental", "Removed"],
        ["base", "120", "baseline, checked", "Kept. It is the keep-from point."],
        ["b3", "100", "incremental", "Kept"],
        ["b4", "5", "incremental", "Kept"],
    ])
    out += code(T + "test_housekeeping.py", r'\("case", "batches", "window", "remove", "needs_baseline"\),', r"^    \],",
                "The decision's case table.", cap=26)
    out += code(HOUSE, r"^def _sweep_leased\(", caption="The job for one dataset, run while holding the dataset's bronze lease.")
    out += ("<p>Walk through it. The plan is made from the file log. If a baseline is needed and a baseliner "
            "was given, it writes one, checks it, and logs the step <code>housekeeping.baseline</code>. If the "
            "baseline was written, the plan is made again. The listed batches are removed from bronze in one "
            "transaction, and each is then voided in the file log with the reason housekeeping. The file log "
            "stays append-only, and every later reader, a rebuild included, skips the removed batches. The "
            "step <code>housekeeping.bronze</code> records the plan, and is warned when the dataset still "
            "waits for a baseline.</p>"
            "<p>The job takes the same lease as the lander, so it never removes while a load is writing. A "
            "lease is a short lock with a time limit, kept in the metadata store.</p>")
    out += ("<p>The run script can prove a rebuild afterwards. With the option <code>--prove-rebuild</code> "
            "it rebuilds silver for every dataset that received a baseline and compares the result with silver "
            "as it stood.</p>")
    out += "<h2>How it is tested</h2>"
    out += (f"<p>{src(T + 'test_housekeeping.py')} has the decision table shown above, a refusal table, and a "
            "table for the whole job run against in-memory bronze. "
            f"{src(T + 'test_retention.py')} has a table for the settings, the table for landing actions, and "
            "one test that moves and deletes real files in a temporary landing folder. "
            f"{src(T + 'test_baseline_stage.py')} covers writing and checking a baseline. The withdrawal is "
            f"tested in {src(T + 'test_gold_harness.py')}.</p>")
    out += not_built([
        "The function that carries out landing retention is called only by its test. No job or run script "
        "applies it, and on a cloud the same rules would be lifecycle rules that are not generated from these "
        "settings.",
        "Withdrawing a batch is a function with no command or screen around it. It is called only by its "
        "test. The order of the three steps is a convention that the caller must follow.",
        "Expiring old table versions is left to the table format's own upkeep. "
        "<code>version_history_days</code> is read into the retention object but nothing in this code acts on it.",
        "The five retention settings are proposed in the registry, not registered.",
        "A loaded full copy counts as a checked baseline without a separate check, on the reasoning that "
        "silver already took it through the key comparison and the mass delete guard.",
    ])
    return out


# ------------------------------------------------------------------------------------------------------ resilience
def resil_picture():
    d = box(10, 70, 170, 80, "Pipeline", "wants to read a source", "bx")
    d += box(260, 20, 190, 60, "Circuit breaker", "open: refuse at once", "bxs")
    d += box(260, 100, 190, 60, "Connection cap", "wait for a free slot", "bxs")
    d += box(260, 180, 190, 60, "Retry", "wait, then try again", "bxs")
    d += box(540, 70, 180, 80, "guard.call", "all three together", "solid")
    d += box(800, 70, 170, 80, "Source", "", "bxm")
    d += arrow(180, 110, 258, 50) + arrow(180, 110, 258, 130) + arrow(180, 110, 258, 210)
    d += arrow(450, 50, 538, 95) + arrow(450, 130, 538, 110) + arrow(450, 210, 538, 130) + arrow(720, 110, 798, 110)
    return d


@page("obs-resilience", "Resilience: retry, circuit breaker and connection limit", parent="obs", short="Resilience",
      blurb="Three protections around every call to a source, so a passing failure is retried and a broken source is left alone.")
def obs_resilience():
    out = ("<p class='lead'>Sources fail in passing ways. A database times out. An interface answers that it "
           "is busy. Resilience is the small part that decides what to do then: try again, wait, or stop "
           "asking.</p>"
           "<p>Without it, one slow minute at a source fails a whole run. With too much of it, a broken source "
           "is hammered by retries. Three protections together handle both.</p>")
    out += fig(980, 260, resil_picture(), "The three protections around one call to a source",
               "Every pull from a source goes through the guard. The three boxes before it are what it applies.")
    out += table(["Protection", "What it does", "Numbers from the code"], [
        ["Retry with backoff", "On an error declared temporary, waits and tries again. Each wait is longer "
                               "than the last. Any other error is raised at once.",
         "2 retries, first wait 60 seconds, each wait twice the last, no wait longer than 600 seconds"],
        ["Circuit breaker", "After several failed calls in a row, the source is not called at all for a while. "
                            "Then one trial call is let through.",
         "Opens after 3 failures, stays open 1800 seconds"],
        ["Connection cap", "At most a set number of calls to one source run at the same time.",
         "No cap unless one is given for the source"],
    ])
    out += ("<p>A <b>temporary error</b> is a timeout, a connection error, or the platform's own "
            "<code>TransientError</code>, which an adapter raises for failures it knows will pass. A wrong "
            "column name is a bug, and a bug is never retried.</p>")
    out += "<h2>A worked example</h2>"
    out += ("<p>The dealer interface is read every six hours. On one read it fails twice with passing errors, "
            "then answers. With a first wait of 10 seconds, as in the test:</p>")
    out += table(["Try", "Result", "Wait before the next try"], [
        ["1", "A temporary error", "10 seconds"],
        ["2", "Connection error", "20 seconds"],
        ["3", "The page of dealers", "None"],
    ])
    out += ("<p>The breaker counts this as one successful call. It counts calls that failed after all their "
            "retries, not each failed try.</p>")
    out += "<h2>The settings that drive it</h2>"
    out += table(["Setting", "Scope", "Meaning", "Default", "Read by the code?"], [
        ["<code>retry_count</code>", "Dataset", "Tries after the first.", "2", "Yes"],
        ["<code>retry_interval_seconds</code>", "Dataset", "Wait before the first retry.", "60", "Yes"],
        ["<code>max_concurrent_datasets</code>", "Source", "The connection cap.", "None",
         "Named in the module's notes as the cap. The run scripts build the guard without caps."],
        ["<code>circuit_failure_threshold</code>", "Dataset, proposed", "Failures in a row before the circuit opens.", "3",
         "No. The guard uses its own default."],
        ["<code>circuit_reset_seconds</code>", "Dataset, proposed", "How long the circuit stays open.", "1800",
         "No. The guard uses its own default."],
    ])
    out += "<h2>The code</h2>"
    out += code(RESIL, r"^def call_with_retry\(", caption="The retry loop. Sleeping is a function passed in, so tests do not wait.")
    out += ("<p>The waits are worked out once from the policy. The loop tries one more time than there are "
            "waits. Only the error types in the policy are caught. On the last try the error is raised as it "
            "is. Because <code>sleep</code> is an argument, a test passes a function that records the wait "
            "and returns at once.</p>")
    out += code(RESIL, r"^class CircuitBreaker:", caption="The breaker's three states, worked out from two numbers.")
    out += ("<p>The breaker stores only a count of failures and the time it opened. Closed means it never "
            "opened or was reset by a success. Open means the reset time has not passed. Half-open means it "
            "has: one call is let through, a success closes the breaker, and a failure opens it again from "
            "now.</p>")
    out += code(RESIL, r"    def call\(self, source: str, fn", caption="The three together. This is what an ingestion pipeline calls.")
    out += code("P/src/dataplatform/ingest/pipelines.py", r"^def _read\(ctx: Ctx", caption="Where ingestion uses it: every read of a source goes through the run's guard.")
    out += "<h2>How it is tested</h2>"
    out += (f"<p>{src(T + 'test_resilience.py')} uses a fake clock and a recorded sleep. The retry table gives "
            "the errors a call raises in order, the policy, the expected result, the number of calls made and "
            "the waits.</p>")
    out += code(T + "test_resilience.py", r"^RETRY_CASES = \[", r"^\]", "The retry cases.")
    out += not_built([
        "The breaker's state lives in the guard for one run. A source that fails in every run is tried again "
        "in each new run. The module's notes say a breaker that must outlive runs belongs in the metadata "
        "store, and that is not built.",
        "The two circuit settings are proposed in the registry and are not read from a dataset's settings.",
        "The run scripts create the guard with no connection caps, so the cap is tested but not in effect in "
        "the runs this site describes.",
        "Retries are kept in a list on the guard for the run log. I did not find the code that writes them to "
        "the run log.",
        "Push sources, where a mover delivers files to landing, do not go through the guard. It protects pulls.",
    ])
    return out
